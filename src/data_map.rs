use log::info;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write as IoWrite;
use std::sync::{Arc, Mutex, MutexGuard};
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

type PrefixStats = HashMap<String, PrefixAggregate>;

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

#[derive(Default)]
struct DiffDumpBatches {
    plus: Vec<(ObjectKey, ObjectProps)>,
    minus: Vec<(ObjectKey, ObjectProps)>,
    astrisk: Vec<(ObjectKey, ObjectProps)>,
    equal: Vec<(ObjectKey, ObjectProps)>,
}

// ── PrefixMap: single-consumer diff object store ───────────

pub struct PrefixMap {
    inner: Mutex<HashMap<String, ObjectMap>>,
}

impl PrefixMap {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_count(&self) -> usize {
        self.inner().len()
    }

    /// Get or create the ObjectMap for a prefix.
    pub fn get_object_map(&self, prefix: &str) -> ObjectMap {
        self.inner().entry(prefix.to_string()).or_default().clone()
    }

    /// Bulk insert a batch of objects for a prefix.
    pub fn bulk_insert(&self, prefix: &str, items: Vec<(ObjectName, ObjectProps)>) {
        let obj_map = self.get_object_map(prefix);
        obj_map.bulk_insert(prefix, items);
    }

    pub fn get_stats(&self) -> (usize, usize) {
        let inner = self.inner();
        let prefix_count = inner.len();
        let obj_count: usize = inner.values().map(ObjectMap::get_count).sum();
        (prefix_count, obj_count)
    }

    /// Dump all objects to Parquet. KS file written separately.
    pub async fn dump<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut crate::utils::AsyncParquetOutput<W>,
        include_equal: bool,
    ) -> Result<DumpStats, String> {
        let mut ks_entries: Vec<(String, usize)> = Vec::new();
        let entries: Vec<(String, ObjectMap)> = self
            .inner()
            .iter()
            .map(|(prefix, obj_map)| (prefix.clone(), obj_map.clone()))
            .collect();

        for (prefix, obj_map) in entries {
            let (object_count, batches) = obj_map.classify_for_dump(&prefix, include_equal);
            ks_entries.push((prefix, object_count));

            writer.write_batch(batches.plus, OUTPUT_FLAG_PLUS).await?;
            writer.write_batch(batches.minus, OUTPUT_FLAG_MINUS).await?;
            writer
                .write_batch(batches.astrisk, OUTPUT_FLAG_ASTRISK)
                .await?;
            if include_equal {
                writer.write_batch(batches.equal, OUTPUT_FLAG_EQUAL).await?;
            }
        }

        ks_entries.sort_by(|a, b| a.0.cmp(&b.0));
        let ks_count = write_ks_entries(writer.ks_path(), &ks_entries).await?;

        Ok(DumpStats {
            parquet_rows: writer.total_rows(),
            ks_entries: ks_count,
        })
    }

    fn inner(&self) -> MutexGuard<'_, HashMap<String, ObjectMap>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    inner: Arc<Mutex<HashMap<ObjectName, ObjectProps>>>,
}

impl ObjectMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_count(&self) -> usize {
        self.inner().len()
    }

    /// Bulk insert with dedup/matching.
    pub fn bulk_insert(&self, _prefix: &str, items: Vec<(ObjectName, ObjectProps)>) {
        let mut inner = self.inner();
        for (name, props) in items {
            if let Some(existing) = inner.get_mut(&name) {
                existing.r#match(&props);
            } else {
                inner.insert(name, props);
            }
        }
    }

    fn classify_for_dump(&self, prefix: &String, include_equal: bool) -> (usize, DiffDumpBatches) {
        let inner = self.inner();
        let mut batches = DiffDumpBatches::default();
        for (name, props) in inner.iter() {
            let key = ObjectKey::encode(prefix, name);
            match props.final_status_check() {
                MatchResult::Plus => batches.plus.push((key, props.clone())),
                MatchResult::Minus => batches.minus.push((key, props.clone())),
                MatchResult::Astrisk => batches.astrisk.push((key, props.clone())),
                MatchResult::Equal if include_equal => batches.equal.push((key, props.clone())),
                MatchResult::Ignore => {}
                _ => {}
            }
        }
        (inner.len(), batches)
    }

    fn inner(&self) -> MutexGuard<'_, HashMap<ObjectName, ObjectProps>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for ObjectMap {
    fn default() -> Self {
        Self::new()
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

    let mut prefix_stats: PrefixStats = HashMap::new();
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

    let mut prefix_stats: PrefixStats = HashMap::new();
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

pub async fn data_map_task_list_stdout(ctx: DataMapContext, format: ListTextOutputFormat) {
    let stdout = tokio::io::stdout();
    let writer = tokio::io::BufWriter::new(stdout);
    data_map_task_list_text_writer(ctx, format, writer).await;
}

pub async fn data_map_task_list_text_writer<W>(
    mut ctx: DataMapContext,
    format: ListTextOutputFormat,
    mut writer: W,
) where
    W: tokio::io::AsyncWrite + Unpin + Send,
{
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — list stdout {:?} started", format);

    let mut prefix_stats: PrefixStats = HashMap::new();
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
    prefix_stats: &mut PrefixStats,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) -> Result<(), String> {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    let written = parquet
        .write_list_batch_filtered(batch, OUTPUT_FLAG_EQUAL, |key, props| {
            record_prefix_stat(prefix_stats, key.prefix(), props.size());
            stats.bytes_total = stats.bytes_total.saturating_add(props.size());
            props.include_in_list_output()
        })
        .await?;
    stats.streamed_rows += written;
    Ok(())
}

async fn ingest_list_stdout_batch<W: tokio::io::AsyncWrite + Unpin + Send>(
    writer: &mut W,
    format: ListTextOutputFormat,
    prefix_stats: &mut PrefixStats,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) -> bool {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    let mut out = Vec::with_capacity(batch.len().saturating_mul(96).min(1024 * 1024));

    for (key, props) in batch {
        if !props.include_in_list_output() {
            continue;
        }

        record_prefix_stat(prefix_stats, key.prefix(), props.size());
        stats.streamed_rows += 1;
        stats.bytes_total = stats.bytes_total.saturating_add(props.size());

        match format {
            ListTextOutputFormat::Tsv => {
                let key = tsv_escape(key.as_str());
                if writeln!(
                    &mut out,
                    "{}\t{}\t{}",
                    key,
                    props.size(),
                    props.last_modified()
                )
                .is_err()
                {
                    log::error!("TSV rendering error");
                    return false;
                }
            }
            ListTextOutputFormat::Ndjson => {
                let row = NdjsonRow {
                    k: key.as_str(),
                    s: props.size(),
                    m: props.last_modified(),
                };
                if let Err(e) = serde_json::to_writer(&mut out, &row) {
                    log::error!("NDJSON serialization error for '{}': {}", key, e);
                    return false;
                }
                out.push(b'\n');
            }
        }
    }

    if !out.is_empty() {
        if let Err(e) = writer.write_all(&out).await {
            log::error!("Stdout write error: {}", e);
            return false;
        }
    }

    true
}

fn ingest_list_summary_batch(
    prefix_stats: &mut PrefixStats,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    for (key, props) in batch {
        if props.include_in_list_output() {
            record_prefix_stat(prefix_stats, key.prefix(), props.size());
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

fn record_prefix_stat(prefix_stats: &mut PrefixStats, prefix: &str, size: u64) {
    let entry = match prefix_stats.get_mut(prefix) {
        Some(entry) => entry,
        None => prefix_stats.entry(prefix.to_string()).or_default(),
    };
    entry.objects += 1;
    entry.bytes = entry.bytes.saturating_add(size);
}

async fn finalize_list_stdout<W: tokio::io::AsyncWrite + Unpin + Send>(
    g_state: &core::GlobalState,
    writer: &mut W,
    prefix_stats: &PrefixStats,
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
    prefix_stats: &PrefixStats,
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
    prefix_stats: &PrefixStats,
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

pub fn insert_batch_grouped(map: &PrefixMap, batch: Vec<(ObjectKey, ObjectProps)>) {
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

async fn write_ks_counts(path: &str, counts: &PrefixStats) -> Result<usize, String> {
    let mut entries: Vec<(String, usize)> = counts
        .iter()
        .map(|(p, stats)| (p.clone(), stats.objects))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
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

fn top_prefixes(prefix_stats: &PrefixStats, limit: usize) -> Vec<core::PrefixMetric> {
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
