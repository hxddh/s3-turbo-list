use dashmap::DashMap;
use log::info;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

use crate::config::OutputConfig;
use crate::core::{self, DataMapContext, MatchResult, ObjectKey, ObjectName, ObjectProps};

// ── Output direction flags ─────────────────────────────────

const OUTPUT_FLAG_EQUAL: u8 = 0;
const OUTPUT_FLAG_PLUS: u8 = 1;
const OUTPUT_FLAG_MINUS: u8 = 2;
const OUTPUT_FLAG_ASTRISK: u8 = 3;

#[derive(Debug, Clone, Copy)]
pub struct DumpStats {
    pub parquet_rows: usize,
    pub ks_entries: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct OutputWriteStats {
    pub parquet_rows: usize,
    pub ks_entries: usize,
    pub elapsed_secs: f64,
}

#[derive(Debug, Clone, Copy)]
struct ListStreamingStats {
    received_batches: usize,
    received_objects: usize,
    streamed_rows: usize,
    bytes_total: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PrefixAggregate {
    objects: usize,
    bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListTextOutputFormat {
    Tsv,
    Ndjson,
}

#[derive(Serialize)]
struct NdjsonRow<'a> {
    k: &'a str,
    s: u64,
    m: u64,
}

// ── PrefixMap: dashmap-based concurrent object store ───────

pub struct PrefixMap {
    /// dashmap<prefix, ObjectMap> — sharded lock-free.
    inner: DashMap<String, ObjectMap>,
    count: Arc<AtomicUsize>,
}

impl PrefixMap {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn inc_count(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Get or create the ObjectMap for a prefix.
    pub fn get_object_map(&self, prefix: &str) -> ObjectMap {
        if let Some(entry) = self.inner.get(prefix) {
            return entry.clone();
        }
        let new_map = ObjectMap::new();
        match self.inner.entry(prefix.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let map = new_map.clone();
                entry.insert(map.clone());
                self.inc_count();
                map
            }
        }
    }

    /// Bulk insert a batch of objects for a prefix.
    pub fn bulk_insert(&self, prefix: &str, items: Vec<(ObjectName, ObjectProps)>) {
        let obj_map = self.get_object_map(prefix);
        obj_map.bulk_insert(prefix, items);
    }

    pub fn get_stats(&self) -> (usize, usize) {
        let prefix_count = self.get_count();
        let obj_count: usize = self.inner.iter().map(|e| e.value().get_count()).sum();
        (prefix_count, obj_count)
    }

    /// Dump all objects to Parquet. KS file written separately.
    pub async fn dump<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut crate::utils::AsyncParquetOutput<W>,
        include_equal: bool,
    ) -> Result<DumpStats, String> {
        let mut ks_entries: Vec<(String, usize)> = Vec::new();

        for entry in self.inner.iter() {
            let prefix = entry.key().clone();
            let obj_map = entry.value();

            let mut plus: Vec<(ObjectKey, ObjectProps)> = Vec::new();
            let mut minus: Vec<(ObjectKey, ObjectProps)> = Vec::new();
            let mut astrisk: Vec<(ObjectKey, ObjectProps)> = Vec::new();
            let mut equal: Vec<(ObjectKey, ObjectProps)> = Vec::new();

            for obj_entry in obj_map.inner.iter() {
                let name = obj_entry.key().clone();
                let props = obj_entry.value().clone();
                let key = ObjectKey::encode(&prefix, &name);

                match props.final_status_check() {
                    MatchResult::Plus => plus.push((key, props)),
                    MatchResult::Minus => minus.push((key, props)),
                    MatchResult::Astrisk => astrisk.push((key, props)),
                    MatchResult::Equal if include_equal => equal.push((key, props)),
                    MatchResult::Ignore => {}
                    _ => {}
                }
            }

            ks_entries.push((prefix, obj_map.get_count()));

            writer.write_batch(plus, OUTPUT_FLAG_PLUS).await?;
            writer.write_batch(minus, OUTPUT_FLAG_MINUS).await?;
            writer.write_batch(astrisk, OUTPUT_FLAG_ASTRISK).await?;
            if include_equal {
                writer.write_batch(equal, OUTPUT_FLAG_EQUAL).await?;
            }
        }

        ks_entries.sort_by(|a, b| a.0.cmp(&b.0));
        let ks_count = write_ks_entries(writer.ks_path(), &ks_entries).await?;

        Ok(DumpStats {
            parquet_rows: writer.total_rows(),
            ks_entries: ks_count,
        })
    }
}

impl std::fmt::Display for PrefixMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (prefix, object) = self.get_stats();
        write!(f, "prefix count {}, object count {}", prefix, object)
    }
}

// ── ObjectMap: per-prefix object store ─────────────────────

#[derive(Clone)]
pub struct ObjectMap {
    inner: Arc<DashMap<ObjectName, ObjectProps>>,
    count: Arc<AtomicUsize>,
}

impl ObjectMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn inc_count(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Bulk insert with concurrent dedup/matching.
    pub fn bulk_insert(&self, _prefix: &str, items: Vec<(ObjectName, ObjectProps)>) {
        for (name, props) in items {
            if let Some(mut existing) = self.inner.get_mut(&name) {
                existing.r#match(&props);
            } else {
                self.inner.insert(name, props);
                self.inc_count();
            }
        }
    }
}

// ── Data map task (consumer) ───────────────────────────────

pub async fn data_map_task(
    mut ctx: DataMapContext,
    filename_ks: &str,
    filename_output: &str,
    include_equal: bool,
    output_config: OutputConfig,
) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — started");

    let map = PrefixMap::new();
    let mut last_ts = epoch_secs();
    let started_at = Instant::now();
    let mut received_batches = 0usize;
    let mut received_objects = 0usize;

    loop {
        let recv_result = ctx.data_map_channel.recv().await;

        match recv_result {
            Some(batch) => {
                received_batches += 1;
                received_objects += batch.len();
                insert_batch_grouped(&map, batch);
            }
            None => {
                // Channel disconnected — flush accumulated data before exit.
                info!("Data Map Task — channel disconnected, writing output");
                let stats = write_output(
                    &map,
                    filename_ks,
                    filename_output,
                    include_equal,
                    &output_config,
                )
                .await;
                log_data_map_final(
                    &ctx.g_state,
                    &map,
                    received_batches,
                    received_objects,
                    started_at,
                    stats,
                );
                ctx.complete();
                return;
            }
        }

        if ctx.is_quit() {
            info!("Data Map Task — force quit, dumping");
            let stats = write_output(
                &map,
                filename_ks,
                filename_output,
                include_equal,
                &output_config,
            )
            .await;
            log_data_map_final(
                &ctx.g_state,
                &map,
                received_batches,
                received_objects,
                started_at,
                stats,
            );
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            // All list tasks done — drain any remaining pending data, then write output.
            while let Ok(batch) = ctx.data_map_channel.try_recv() {
                received_batches += 1;
                received_objects += batch.len();
                insert_batch_grouped(&map, batch);
            }
            info!("Data Map Task — all list tasks done, writing output");
            let stats = write_output(
                &map,
                filename_ks,
                filename_output,
                include_equal,
                &output_config,
            )
            .await;
            log_data_map_final(
                &ctx.g_state,
                &map,
                received_batches,
                received_objects,
                started_at,
                stats,
            );
            ctx.complete();
            ctx.quit();
            return;
        }

        let now = epoch_secs();
        if now - last_ts > core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
            let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
            info!(
                "Data Map Task — {}, received batches {}, received objects {}, {:.0} objects/sec",
                map,
                received_batches,
                received_objects,
                received_objects as f64 / elapsed
            );
            last_ts = now;
        }
    }
}

pub async fn data_map_task_list_streaming(
    mut ctx: DataMapContext,
    filename_ks: &str,
    filename_output: &str,
    output_config: OutputConfig,
) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — list streaming started");

    let output_file = match tokio::fs::File::create(filename_output).await {
        Ok(f) => f,
        Err(e) => {
            log::error!("Failed to create output file {}: {}", filename_output, e);
            ctx.g_state.inc_output_error();
            ctx.complete();
            ctx.quit();
            return;
        }
    };
    let buf_writer = tokio::io::BufWriter::with_capacity(100 * core::MB, output_file);
    let mut parquet = crate::utils::AsyncParquetOutput::new_with_options(
        buf_writer,
        filename_ks,
        output_config.row_group_size,
        &output_config.compression,
        output_config.compression_level,
    );

    let mut prefix_stats: BTreeMap<String, PrefixAggregate> = BTreeMap::new();
    let started_at = Instant::now();
    let write_started_at = Instant::now();
    let mut last_ts = epoch_secs();
    let mut stats = ListStreamingStats {
        received_batches: 0,
        received_objects: 0,
        streamed_rows: 0,
        bytes_total: 0,
    };

    loop {
        let recv_result = ctx.data_map_channel.recv().await;

        match recv_result {
            Some(batch) => {
                if let Err(e) =
                    ingest_list_streaming_batch(&mut parquet, &mut prefix_stats, &mut stats, batch)
                        .await
                {
                    log::error!("{}", e);
                    ctx.g_state.inc_output_error();
                    ctx.complete();
                    ctx.quit();
                    return;
                }
            }
            None => {
                info!("Data Map Task — list streaming channel disconnected, finalizing output");
                finalize_list_streaming_output(
                    parquet,
                    &ctx.g_state,
                    filename_ks,
                    filename_output,
                    &prefix_stats,
                    stats,
                    started_at,
                    write_started_at,
                )
                .await;
                ctx.complete();
                return;
            }
        }

        if ctx.is_quit() {
            info!("Data Map Task — list streaming force quit, finalizing output");
            finalize_list_streaming_output(
                parquet,
                &ctx.g_state,
                filename_ks,
                filename_output,
                &prefix_stats,
                stats,
                started_at,
                write_started_at,
            )
            .await;
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            while let Ok(batch) = ctx.data_map_channel.try_recv() {
                if let Err(e) =
                    ingest_list_streaming_batch(&mut parquet, &mut prefix_stats, &mut stats, batch)
                        .await
                {
                    log::error!("{}", e);
                    ctx.g_state.inc_output_error();
                    ctx.complete();
                    ctx.quit();
                    return;
                }
            }
            info!("Data Map Task — list streaming all list tasks done, finalizing output");
            finalize_list_streaming_output(
                parquet,
                &ctx.g_state,
                filename_ks,
                filename_output,
                &prefix_stats,
                stats,
                started_at,
                write_started_at,
            )
            .await;
            ctx.complete();
            ctx.quit();
            return;
        }

        let now = epoch_secs();
        if now - last_ts > core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
            let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
            info!(
                "Data Map Task — list streaming prefixes {}, received batches {}, received objects {}, streamed rows {}, {:.0} objects/sec",
                prefix_stats.len(),
                stats.received_batches,
                stats.received_objects,
                stats.streamed_rows,
                stats.received_objects as f64 / elapsed
            );
            last_ts = now;
        }
    }
}

pub async fn data_map_task_list_summary_only(mut ctx: DataMapContext) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — list summary-only started");

    let mut prefix_stats: BTreeMap<String, PrefixAggregate> = BTreeMap::new();
    let started_at = Instant::now();
    let mut last_ts = epoch_secs();
    let mut stats = ListStreamingStats {
        received_batches: 0,
        received_objects: 0,
        streamed_rows: 0,
        bytes_total: 0,
    };

    loop {
        let recv_result = ctx.data_map_channel.recv().await;

        match recv_result {
            Some(batch) => {
                ingest_list_summary_batch(&mut prefix_stats, &mut stats, batch);
            }
            None => {
                info!("Data Map Task — list summary-only channel disconnected, finalizing");
                finalize_list_summary_only(&ctx.g_state, &prefix_stats, stats, started_at);
                ctx.complete();
                return;
            }
        }

        if ctx.is_quit() {
            info!("Data Map Task — list summary-only force quit, finalizing");
            finalize_list_summary_only(&ctx.g_state, &prefix_stats, stats, started_at);
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            while let Ok(batch) = ctx.data_map_channel.try_recv() {
                ingest_list_summary_batch(&mut prefix_stats, &mut stats, batch);
            }
            info!("Data Map Task — list summary-only all list tasks done, finalizing");
            finalize_list_summary_only(&ctx.g_state, &prefix_stats, stats, started_at);
            ctx.complete();
            ctx.quit();
            return;
        }

        let now = epoch_secs();
        if now - last_ts > core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
            let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
            info!(
                "Data Map Task — list summary-only prefixes {}, received batches {}, received objects {}, streamed rows {}, bytes {}, {:.0} objects/sec",
                prefix_stats.len(),
                stats.received_batches,
                stats.received_objects,
                stats.streamed_rows,
                stats.bytes_total,
                stats.received_objects as f64 / elapsed
            );
            last_ts = now;
        }
    }
}

pub async fn data_map_task_list_stdout(mut ctx: DataMapContext, format: ListTextOutputFormat) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — list stdout {:?} started", format);

    let stdout = tokio::io::stdout();
    let mut writer = tokio::io::BufWriter::new(stdout);
    let mut prefix_stats: BTreeMap<String, PrefixAggregate> = BTreeMap::new();
    let started_at = Instant::now();
    let mut last_ts = epoch_secs();
    let mut stats = ListStreamingStats {
        received_batches: 0,
        received_objects: 0,
        streamed_rows: 0,
        bytes_total: 0,
    };

    loop {
        let recv_result = ctx.data_map_channel.recv().await;

        match recv_result {
            Some(batch) => {
                if !ingest_list_stdout_batch(
                    &mut writer,
                    format,
                    &mut prefix_stats,
                    &mut stats,
                    batch,
                )
                .await
                {
                    ctx.g_state.inc_output_error();
                    ctx.complete();
                    ctx.quit();
                    return;
                }
            }
            None => {
                info!("Data Map Task — list stdout channel disconnected, finalizing");
                finalize_list_stdout(&ctx.g_state, &mut writer, &prefix_stats, stats, started_at)
                    .await;
                ctx.complete();
                return;
            }
        }

        if ctx.is_quit() {
            info!("Data Map Task — list stdout force quit, finalizing");
            finalize_list_stdout(&ctx.g_state, &mut writer, &prefix_stats, stats, started_at).await;
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            while let Ok(batch) = ctx.data_map_channel.try_recv() {
                if !ingest_list_stdout_batch(
                    &mut writer,
                    format,
                    &mut prefix_stats,
                    &mut stats,
                    batch,
                )
                .await
                {
                    ctx.g_state.inc_output_error();
                    ctx.complete();
                    ctx.quit();
                    return;
                }
            }
            info!("Data Map Task — list stdout all list tasks done, finalizing");
            finalize_list_stdout(&ctx.g_state, &mut writer, &prefix_stats, stats, started_at).await;
            ctx.complete();
            ctx.quit();
            return;
        }

        let now = epoch_secs();
        if now - last_ts > core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
            let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
            info!(
                "Data Map Task — list stdout prefixes {}, received batches {}, received objects {}, streamed rows {}, bytes {}, {:.0} objects/sec",
                prefix_stats.len(),
                stats.received_batches,
                stats.received_objects,
                stats.streamed_rows,
                stats.bytes_total,
                stats.received_objects as f64 / elapsed
            );
            last_ts = now;
        }
    }
}

async fn ingest_list_streaming_batch<W: tokio::io::AsyncWrite + Unpin + Send>(
    parquet: &mut crate::utils::AsyncParquetOutput<W>,
    prefix_stats: &mut BTreeMap<String, PrefixAggregate>,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) -> Result<(), String> {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    let mut rows = Vec::with_capacity(batch.len());
    for (key, props) in batch {
        let (prefix, _) = key.decode();
        record_prefix_stat(prefix_stats, prefix, props.size());
        stats.bytes_total = stats.bytes_total.saturating_add(props.size());

        if props.final_status_check() != MatchResult::Ignore {
            rows.push((key, props));
        }
    }

    stats.streamed_rows += rows.len();
    parquet.write_batch(rows, OUTPUT_FLAG_EQUAL).await
}

async fn ingest_list_stdout_batch<W: tokio::io::AsyncWrite + Unpin + Send>(
    writer: &mut W,
    format: ListTextOutputFormat,
    prefix_stats: &mut BTreeMap<String, PrefixAggregate>,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) -> bool {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    for (key, props) in batch {
        if props.final_status_check() == MatchResult::Ignore {
            continue;
        }

        let (prefix, _) = key.decode();
        record_prefix_stat(prefix_stats, prefix, props.size());
        stats.streamed_rows += 1;
        stats.bytes_total = stats.bytes_total.saturating_add(props.size());

        let line = match format {
            ListTextOutputFormat::Tsv => {
                let key = tsv_escape(key.as_str());
                if writer.write_all(key.as_bytes()).await.is_err()
                    || writer.write_all(b"\t").await.is_err()
                    || writer
                        .write_all(props.size().to_string().as_bytes())
                        .await
                        .is_err()
                    || writer.write_all(b"\t").await.is_err()
                    || writer
                        .write_all(props.last_modified().to_string().as_bytes())
                        .await
                        .is_err()
                    || writer.write_all(b"\n").await.is_err()
                {
                    log::error!("Stdout write error");
                    return false;
                }
                continue;
            }
            ListTextOutputFormat::Ndjson => {
                let row = NdjsonRow {
                    k: key.as_str(),
                    s: props.size(),
                    m: props.last_modified(),
                };
                match serde_json::to_string(&row) {
                    Ok(rendered) => rendered,
                    Err(e) => {
                        log::error!("NDJSON serialization error for '{}': {}", key, e);
                        return false;
                    }
                }
            }
        };

        if let Err(e) = writer.write_all(line.as_bytes()).await {
            log::error!("Stdout write error: {}", e);
            return false;
        }
        if format == ListTextOutputFormat::Ndjson {
            if let Err(e) = writer.write_all(b"\n").await {
                log::error!("Stdout write error: {}", e);
                return false;
            }
        }
    }

    true
}

fn ingest_list_summary_batch(
    prefix_stats: &mut BTreeMap<String, PrefixAggregate>,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    for (key, props) in batch {
        if props.final_status_check() != MatchResult::Ignore {
            let (prefix, _) = key.decode();
            record_prefix_stat(prefix_stats, prefix, props.size());
            stats.streamed_rows += 1;
            stats.bytes_total = stats.bytes_total.saturating_add(props.size());
        }
    }
}

fn tsv_escape(value: &str) -> Cow<'_, str> {
    if !value
        .bytes()
        .any(|b| matches!(b, b'\\' | b'\t' | b'\n' | b'\r'))
    {
        return Cow::Borrowed(value);
    }

    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            _ => escaped.push(ch),
        }
    }
    Cow::Owned(escaped)
}

fn record_prefix_stat(
    prefix_stats: &mut BTreeMap<String, PrefixAggregate>,
    prefix: String,
    size: u64,
) {
    let entry = prefix_stats.entry(prefix).or_default();
    entry.objects += 1;
    entry.bytes = entry.bytes.saturating_add(size);
}

async fn finalize_list_stdout<W: tokio::io::AsyncWrite + Unpin + Send>(
    g_state: &core::GlobalState,
    writer: &mut W,
    prefix_stats: &BTreeMap<String, PrefixAggregate>,
    stats: ListStreamingStats,
    started_at: Instant,
) {
    if let Err(e) = writer.flush().await {
        log::error!("Stdout flush error: {}", e);
        g_state.inc_output_error();
    }
    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    g_state.record_data_metrics(
        stats.received_batches,
        stats.received_objects,
        stats.streamed_rows,
        prefix_stats.len(),
        0,
        0,
        stats.bytes_total,
        top_prefixes(prefix_stats, 32),
        false,
    );
    info!(
        "Data Map Task — list stdout complete: streamed rows {}, received batches {}, received objects {}, unique prefixes {}, bytes {}, elapsed {:.3}s, {:.0} objects/sec",
        stats.streamed_rows,
        stats.received_batches,
        stats.received_objects,
        prefix_stats.len(),
        stats.bytes_total,
        elapsed,
        stats.received_objects as f64 / elapsed,
    );
}

async fn finalize_list_streaming_output<W: tokio::io::AsyncWrite + Unpin + Send>(
    parquet: crate::utils::AsyncParquetOutput<W>,
    g_state: &core::GlobalState,
    filename_ks: &str,
    filename_output: &str,
    prefix_stats: &BTreeMap<String, PrefixAggregate>,
    stats: ListStreamingStats,
    started_at: Instant,
    write_started_at: Instant,
) {
    let parquet_rows = parquet.total_rows();
    let mut output_ok = true;
    let ks_entries = match write_ks_counts(filename_ks, prefix_stats).await {
        Ok(count) => count,
        Err(e) => {
            log::error!("{}", e);
            output_ok = false;
            0
        }
    };
    if let Err(e) = parquet.close().await {
        log::error!("{}", e);
        output_ok = false;
    }
    if !output_ok {
        g_state.inc_output_error();
    }

    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    let write_elapsed = write_started_at.elapsed().as_secs_f64();
    g_state.record_data_metrics(
        stats.received_batches,
        stats.received_objects,
        stats.streamed_rows,
        prefix_stats.len(),
        parquet_rows,
        ks_entries,
        stats.bytes_total,
        top_prefixes(prefix_stats, 32),
        false,
    );
    info!(
        "Data Map Task — list streaming complete: streamed rows {}, received batches {}, received objects {}, unique prefixes {}, elapsed {:.3}s, {:.0} objects/sec",
        stats.streamed_rows,
        stats.received_batches,
        stats.received_objects,
        prefix_stats.len(),
        elapsed,
        stats.received_objects as f64 / elapsed,
    );
    info!(
        "Data Map Task — list streaming output metrics: Parquet rows {} to '{}', KS write entries {} to '{}', write elapsed {:.3}s",
        parquet_rows,
        filename_output,
        ks_entries,
        filename_ks,
        write_elapsed,
    );
}

fn finalize_list_summary_only(
    g_state: &core::GlobalState,
    prefix_stats: &BTreeMap<String, PrefixAggregate>,
    stats: ListStreamingStats,
    started_at: Instant,
) {
    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    g_state.record_data_metrics(
        stats.received_batches,
        stats.received_objects,
        stats.streamed_rows,
        prefix_stats.len(),
        0,
        0,
        stats.bytes_total,
        top_prefixes(prefix_stats, 32),
        true,
    );
    info!(
        "Data Map Task — list summary-only complete: streamed rows {}, received batches {}, received objects {}, unique prefixes {}, bytes {}, elapsed {:.3}s, {:.0} objects/sec",
        stats.streamed_rows,
        stats.received_batches,
        stats.received_objects,
        prefix_stats.len(),
        stats.bytes_total,
        elapsed,
        stats.received_objects as f64 / elapsed,
    );
}

fn insert_batch_grouped(map: &PrefixMap, batch: Vec<(ObjectKey, ObjectProps)>) {
    let mut grouped: HashMap<String, Vec<(ObjectName, ObjectProps)>> = HashMap::new();
    for (key, props) in batch {
        let (prefix, name) = key.decode();
        grouped.entry(prefix).or_default().push((name, props));
    }
    for (prefix, items) in grouped {
        map.bulk_insert(&prefix, items);
    }
}

async fn write_output(
    map: &PrefixMap,
    ks: &str,
    output: &str,
    include_equal: bool,
    output_config: &OutputConfig,
) -> Option<OutputWriteStats> {
    let started_at = Instant::now();
    let output_file = match tokio::fs::File::create(output).await {
        Ok(f) => f,
        Err(e) => {
            log::error!("Failed to create output file {}: {}", output, e);
            return None;
        }
    };
    let buf_writer = tokio::io::BufWriter::with_capacity(100 * core::MB, output_file);
    let mut parquet = crate::utils::AsyncParquetOutput::new_with_options(
        buf_writer,
        ks,
        output_config.row_group_size,
        &output_config.compression,
        output_config.compression_level,
    );
    let dump_stats = match map.dump(&mut parquet, include_equal).await {
        Ok(stats) => stats,
        Err(e) => {
            log::error!("{}", e);
            return None;
        }
    };
    if let Err(e) = parquet.close().await {
        log::error!("{}", e);
        return None;
    }
    let stats = OutputWriteStats {
        parquet_rows: dump_stats.parquet_rows,
        ks_entries: dump_stats.ks_entries,
        elapsed_secs: started_at.elapsed().as_secs_f64(),
    };
    info!(
        "Data Map Task — wrote {} Parquet rows to '{}' and {} KS entries to '{}' in {:.3}s",
        stats.parquet_rows, output, stats.ks_entries, ks, stats.elapsed_secs
    );
    Some(stats)
}

async fn write_ks_counts(
    path: &str,
    counts: &BTreeMap<String, PrefixAggregate>,
) -> Result<usize, String> {
    let entries: Vec<(String, usize)> = counts
        .iter()
        .map(|(p, stats)| (p.clone(), stats.objects))
        .collect();
    write_ks_entries(path, &entries).await
}

async fn write_ks_entries(path: &str, entries: &[(String, usize)]) -> Result<usize, String> {
    match tokio::fs::File::create(path).await {
        Ok(ks_file) => {
            let mut buf = tokio::io::BufWriter::new(ks_file);
            for (prefix, count) in entries {
                let line = format!("\"{}\",\"{}\"\n", prefix, count);
                if let Err(e) = buf.write_all(line.as_bytes()).await {
                    return Err(format!("Failed to write KS file {}: {}", path, e));
                }
            }
            if let Err(e) = buf.flush().await {
                return Err(format!("Failed to flush KS file {}: {}", path, e));
            }
            Ok(entries.len())
        }
        Err(e) => Err(format!("Failed to create KS file {}: {}", path, e)),
    }
}

fn log_data_map_final(
    g_state: &core::GlobalState,
    map: &PrefixMap,
    received_batches: usize,
    received_objects: usize,
    started_at: Instant,
    write_stats: Option<OutputWriteStats>,
) {
    let (prefix_count, object_count) = map.get_stats();
    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    info!(
        "Data Map Task — complete: received batches {}, received objects {}, prefixes {}, objects {}, elapsed {:.3}s, {:.0} objects/sec",
        received_batches,
        received_objects,
        prefix_count,
        object_count,
        elapsed,
        received_objects as f64 / elapsed,
    );
    if let Some(stats) = write_stats {
        g_state.record_data_metrics(
            received_batches,
            received_objects,
            stats.parquet_rows,
            prefix_count,
            stats.parquet_rows,
            stats.ks_entries,
            0,
            Vec::new(),
            false,
        );
        info!(
            "Data Map Task — output metrics: Parquet rows {}, KS entries {}, write elapsed {:.3}s",
            stats.parquet_rows, stats.ks_entries, stats.elapsed_secs
        );
    } else {
        g_state.inc_output_error();
    }
}

fn top_prefixes(
    prefix_stats: &BTreeMap<String, PrefixAggregate>,
    limit: usize,
) -> Vec<core::PrefixMetric> {
    let mut entries: Vec<_> = prefix_stats
        .iter()
        .map(|(prefix, stats)| core::PrefixMetric {
            prefix: prefix.clone(),
            objects: stats.objects,
            bytes: stats.bytes,
        })
        .collect();
    entries.sort_by(|a, b| {
        b.objects
            .cmp(&a.objects)
            .then_with(|| b.bytes.cmp(&a.bytes))
            .then_with(|| a.prefix.cmp(&b.prefix))
    });
    entries.truncate(limit);
    entries
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
