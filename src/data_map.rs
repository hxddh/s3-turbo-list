use dashmap::DashMap;
use log::info;
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
    ) -> DumpStats {
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

            writer.write_batch(plus, OUTPUT_FLAG_PLUS).await;
            writer.write_batch(minus, OUTPUT_FLAG_MINUS).await;
            writer.write_batch(astrisk, OUTPUT_FLAG_ASTRISK).await;
            if include_equal {
                writer.write_batch(equal, OUTPUT_FLAG_EQUAL).await;
            }
        }

        ks_entries.sort_by(|a, b| a.0.cmp(&b.0));
        let _ = write_ks_entries(writer.ks_path(), &ks_entries).await;

        DumpStats {
            parquet_rows: writer.total_rows(),
            ks_entries: ks_entries.len(),
        }
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

    let mut prefix_counts: BTreeMap<String, usize> = BTreeMap::new();
    let started_at = Instant::now();
    let write_started_at = Instant::now();
    let mut last_ts = epoch_secs();
    let mut stats = ListStreamingStats {
        received_batches: 0,
        received_objects: 0,
        streamed_rows: 0,
    };

    loop {
        let recv_result = ctx.data_map_channel.recv().await;

        match recv_result {
            Some(batch) => {
                ingest_list_streaming_batch(&mut parquet, &mut prefix_counts, &mut stats, batch)
                    .await;
            }
            None => {
                info!("Data Map Task — list streaming channel disconnected, finalizing output");
                finalize_list_streaming_output(
                    parquet,
                    &ctx.g_state,
                    filename_ks,
                    filename_output,
                    &prefix_counts,
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
                &prefix_counts,
                stats,
                started_at,
                write_started_at,
            )
            .await;
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            while let Ok(batch) = ctx.data_map_channel.try_recv() {
                ingest_list_streaming_batch(&mut parquet, &mut prefix_counts, &mut stats, batch)
                    .await;
            }
            info!("Data Map Task — list streaming all list tasks done, finalizing output");
            finalize_list_streaming_output(
                parquet,
                &ctx.g_state,
                filename_ks,
                filename_output,
                &prefix_counts,
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
                prefix_counts.len(),
                stats.received_batches,
                stats.received_objects,
                stats.streamed_rows,
                stats.received_objects as f64 / elapsed
            );
            last_ts = now;
        }
    }
}

async fn ingest_list_streaming_batch<W: tokio::io::AsyncWrite + Unpin + Send>(
    parquet: &mut crate::utils::AsyncParquetOutput<W>,
    prefix_counts: &mut BTreeMap<String, usize>,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    let mut rows = Vec::with_capacity(batch.len());
    for (key, props) in batch {
        let (prefix, _) = key.decode();
        *prefix_counts.entry(prefix).or_insert(0) += 1;

        if props.final_status_check() != MatchResult::Ignore {
            rows.push((key, props));
        }
    }

    stats.streamed_rows += rows.len();
    parquet.write_batch(rows, OUTPUT_FLAG_EQUAL).await;
}

async fn finalize_list_streaming_output<W: tokio::io::AsyncWrite + Unpin + Send>(
    parquet: crate::utils::AsyncParquetOutput<W>,
    g_state: &core::GlobalState,
    filename_ks: &str,
    filename_output: &str,
    prefix_counts: &BTreeMap<String, usize>,
    stats: ListStreamingStats,
    started_at: Instant,
    write_started_at: Instant,
) {
    let parquet_rows = parquet.total_rows();
    let ks_entries = write_ks_counts(filename_ks, prefix_counts).await;
    parquet.close().await;

    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    let write_elapsed = write_started_at.elapsed().as_secs_f64();
    g_state.record_data_metrics(
        stats.received_batches,
        stats.received_objects,
        stats.streamed_rows,
        prefix_counts.len(),
        parquet_rows,
        ks_entries,
    );
    info!(
        "Data Map Task — list streaming complete: streamed rows {}, received batches {}, received objects {}, unique prefixes {}, elapsed {:.3}s, {:.0} objects/sec",
        stats.streamed_rows,
        stats.received_batches,
        stats.received_objects,
        prefix_counts.len(),
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
    let dump_stats = map.dump(&mut parquet, include_equal).await;
    parquet.close().await;
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

async fn write_ks_counts(path: &str, counts: &BTreeMap<String, usize>) -> usize {
    let entries: Vec<(String, usize)> = counts.iter().map(|(p, c)| (p.clone(), *c)).collect();
    write_ks_entries(path, &entries).await
}

async fn write_ks_entries(path: &str, entries: &[(String, usize)]) -> usize {
    match tokio::fs::File::create(path).await {
        Ok(ks_file) => {
            let mut buf = tokio::io::BufWriter::new(ks_file);
            for (prefix, count) in entries {
                let line = format!("\"{}\",\"{}\"\n", prefix, count);
                if let Err(e) = buf.write_all(line.as_bytes()).await {
                    log::error!("Failed to write KS file {}: {}", path, e);
                    break;
                }
            }
            if let Err(e) = buf.flush().await {
                log::error!("Failed to flush KS file {}: {}", path, e);
            }
            entries.len()
        }
        Err(e) => {
            log::error!("Failed to create KS file {}: {}", path, e);
            0
        }
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
        );
        info!(
            "Data Map Task — output metrics: Parquet rows {}, KS entries {}, write elapsed {:.3}s",
            stats.parquet_rows, stats.ks_entries, stats.elapsed_secs
        );
    } else {
        g_state.inc_output_error();
    }
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
