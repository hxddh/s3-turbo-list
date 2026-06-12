use log::info;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write as IoWrite;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

use crate::config::OutputConfig;
use crate::core::{self, DataMapContext, MatchResult, ObjectKey, ObjectProps};

// ── Output direction flags ─────────────────────────────────

const OUTPUT_FLAG_EQUAL: u8 = 0;
const OUTPUT_FLAG_PLUS: u8 = 1;
const OUTPUT_FLAG_MINUS: u8 = 2;
const OUTPUT_FLAG_ASTRISK: u8 = 3;

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

// ── Streaming diff (merge-join) ────────────────────────────
//
// Diff is authoritative single-segment per side, so each side's batches
// arrive in strict ascending key order.  Merging the two ordered streams
// classifies every key on the fly — left-only (Plus), right-only (Minus),
// or present on both sides (Equal/Astrisk via ObjectProps::classify_pair)
// — and streams rows straight to Parquet.  Memory is bounded by the
// channel buffers instead of the combined object count.

/// Receiver pair for the two diff sides.
pub struct DiffStreamSides {
    pub left: tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
    pub right: tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
}

struct DiffSideStream {
    rx: tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
    buf: std::collections::VecDeque<(ObjectKey, ObjectProps)>,
    closed: bool,
    last_key: Option<String>,
    name: &'static str,
    received_batches: usize,
    received_objects: usize,
}

impl DiffSideStream {
    fn new(
        rx: tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
        name: &'static str,
    ) -> Self {
        Self {
            rx,
            buf: std::collections::VecDeque::new(),
            closed: false,
            last_key: None,
            name,
            received_batches: 0,
            received_objects: 0,
        }
    }

    /// Fill the buffer until it has an item or the side is exhausted.
    /// Enforces ascending key order (the merge's correctness contract) and
    /// keeps the latest entry for same-key duplicates, matching the legacy
    /// map-based behavior.
    async fn ensure_filled(&mut self) -> Result<bool, String> {
        while self.buf.is_empty() && !self.closed {
            match self.rx.recv().await {
                Some(batch) => {
                    self.received_batches += 1;
                    self.received_objects += batch.len();
                    for (key, props) in batch {
                        match self.last_key.as_deref() {
                            Some(prev) if key.as_str() < prev => {
                                return Err(format!(
                                    "{} listing returned keys out of order ('{}' after '{}'); \
                                     diff requires S3 lexicographic ordering",
                                    self.name, key, prev
                                ));
                            }
                            Some(prev) if key.as_str() == prev => {
                                log::warn!(
                                    "{} listing repeated key '{}'; keeping the latest entry",
                                    self.name,
                                    key
                                );
                                if let Some(back) = self.buf.back_mut() {
                                    *back = (key, props);
                                } // else: predecessor already merged; drop dup
                                continue;
                            }
                            _ => {}
                        }
                        self.last_key = Some(key.as_str().to_string());
                        self.buf.push_back((key, props));
                    }
                }
                None => self.closed = true,
            }
        }
        Ok(!self.buf.is_empty())
    }
}

/// Buffered streaming row sink: rows accumulate per DiffFlag and flush as
/// Parquet batches; per-prefix counts feed the KS file.
struct DiffRowSink {
    bufs: [Vec<(ObjectKey, ObjectProps)>; 4],
    prefix_stats: PrefixStats,
    rows: usize,
    plus: usize,
    minus: usize,
    astrisk: usize,
    equal: usize,
    ignored: usize,
}

const DIFF_SINK_FLUSH_ROWS: usize = 8192;

impl DiffRowSink {
    fn new() -> Self {
        Self {
            bufs: [Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            prefix_stats: PrefixStats::new(),
            rows: 0,
            plus: 0,
            minus: 0,
            astrisk: 0,
            equal: 0,
            ignored: 0,
        }
    }

    /// Record a merged key in the KS prefix counts (every merged key counts
    /// once, including filter-ignored pairs, matching legacy KS output).
    fn record_key(&mut self, key: &ObjectKey, size: u64) {
        record_prefix_stat(&mut self.prefix_stats, key.prefix(), size);
    }

    async fn push<W: tokio::io::AsyncWrite + Unpin + Send>(
        &mut self,
        parquet: &mut crate::utils::AsyncParquetOutput<W>,
        flag: u8,
        key: ObjectKey,
        props: ObjectProps,
    ) -> Result<(), String> {
        match flag {
            OUTPUT_FLAG_PLUS => self.plus += 1,
            OUTPUT_FLAG_MINUS => self.minus += 1,
            OUTPUT_FLAG_ASTRISK => self.astrisk += 1,
            _ => self.equal += 1,
        }
        self.rows += 1;
        let buf = &mut self.bufs[flag as usize];
        buf.push((key, props));
        if buf.len() >= DIFF_SINK_FLUSH_ROWS {
            let batch = std::mem::take(buf);
            parquet.write_batch(batch, flag).await?;
        }
        Ok(())
    }

    async fn flush_all<W: tokio::io::AsyncWrite + Unpin + Send>(
        &mut self,
        parquet: &mut crate::utils::AsyncParquetOutput<W>,
    ) -> Result<(), String> {
        for flag in [
            OUTPUT_FLAG_PLUS,
            OUTPUT_FLAG_MINUS,
            OUTPUT_FLAG_ASTRISK,
            OUTPUT_FLAG_EQUAL,
        ] {
            let buf = &mut self.bufs[flag as usize];
            if !buf.is_empty() {
                let batch = std::mem::take(buf);
                parquet.write_batch(batch, flag).await?;
            }
        }
        Ok(())
    }
}

/// Merge the two ordered diff streams into the sink. Returns Err on
/// ordering violations or write failures.
async fn merge_diff_streams<W: tokio::io::AsyncWrite + Unpin + Send>(
    left: &mut DiffSideStream,
    right: &mut DiffSideStream,
    parquet: &mut crate::utils::AsyncParquetOutput<W>,
    sink: &mut DiffRowSink,
) -> Result<(), String> {
    loop {
        let has_left = left.ensure_filled().await?;
        let has_right = right.ensure_filled().await?;

        let order = match (has_left, has_right) {
            (false, false) => break,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (true, true) => {
                let lk = &left.buf.front().expect("filled").0;
                let rk = &right.buf.front().expect("filled").0;
                lk.as_str().cmp(rk.as_str())
            }
        };

        match order {
            std::cmp::Ordering::Less => {
                let (key, props) = left.buf.pop_front().expect("filled");
                sink.record_key(&key, props.size());
                sink.push(parquet, OUTPUT_FLAG_PLUS, key, props).await?;
            }
            std::cmp::Ordering::Greater => {
                let (key, props) = right.buf.pop_front().expect("filled");
                sink.record_key(&key, props.size());
                sink.push(parquet, OUTPUT_FLAG_MINUS, key, props).await?;
            }
            std::cmp::Ordering::Equal => {
                let (key, left_props) = left.buf.pop_front().expect("filled");
                let (_rkey, right_props) = right.buf.pop_front().expect("filled");
                sink.record_key(&key, left_props.size());
                match core::ObjectProps::classify_pair(&left_props, &right_props) {
                    Some(MatchResult::Astrisk) => {
                        sink.push(parquet, OUTPUT_FLAG_ASTRISK, key, left_props)
                            .await?;
                    }
                    Some(_) => {
                        sink.push(parquet, OUTPUT_FLAG_EQUAL, key, left_props)
                            .await?;
                    }
                    None => sink.ignored += 1,
                }
            }
        }
    }
    Ok(())
}

/// Outcome of one diff merge run. Counter fields are public for callers
/// (benchmarks, reports); prefix stats stay internal and feed the KS file.
#[derive(Debug)]
pub struct DiffMergeOutcome {
    pub rows: usize,
    pub plus: usize,
    pub minus: usize,
    pub astrisk: usize,
    pub equal: usize,
    pub ignored: usize,
    pub received_batches: usize,
    pub received_objects: usize,
    pub bytes_total: u64,
    prefix_stats: PrefixStats,
}

impl DiffMergeOutcome {
    pub fn unique_prefixes(&self) -> usize {
        self.prefix_stats.len()
    }

    /// Write the per-prefix KS counts file for this merge.
    pub async fn write_ks(&self, path: &str) -> Result<usize, String> {
        write_ks_counts(path, &self.prefix_stats).await
    }
}

/// Merge two ordered diff streams into the Parquet writer and return the
/// classification counters. The caller owns writer lifecycle (close) and
/// KS output.
pub async fn run_diff_merge<W: tokio::io::AsyncWrite + Unpin + Send>(
    sides: DiffStreamSides,
    parquet: &mut crate::utils::AsyncParquetOutput<W>,
) -> Result<DiffMergeOutcome, String> {
    let mut left = DiffSideStream::new(sides.left, "left");
    let mut right = DiffSideStream::new(sides.right, "right");
    let mut sink = DiffRowSink::new();

    merge_diff_streams(&mut left, &mut right, parquet, &mut sink).await?;
    sink.flush_all(parquet).await?;

    let bytes_total = sink
        .prefix_stats
        .values()
        .map(|aggregate| aggregate.bytes)
        .fold(0u64, u64::saturating_add);
    Ok(DiffMergeOutcome {
        rows: sink.rows,
        plus: sink.plus,
        minus: sink.minus,
        astrisk: sink.astrisk,
        equal: sink.equal,
        ignored: sink.ignored,
        received_batches: left.received_batches + right.received_batches,
        received_objects: left.received_objects + right.received_objects,
        bytes_total,
        prefix_stats: sink.prefix_stats,
    })
}

pub async fn data_map_task_diff_streaming(
    g_state: core::GlobalState,
    sides: DiffStreamSides,
    filename_ks: &str,
    filename_output: &str,
    output_config: OutputConfig,
) {
    g_state.data_map_task_start();
    g_state.wait_to_start().await;

    info!("Data Map Task — diff streaming started");

    let output_file = match tokio::fs::File::create(filename_output).await {
        Ok(f) => f,
        Err(e) => {
            log::error!("Failed to create output file {}: {}", filename_output, e);
            g_state.inc_output_error();
            g_state.data_map_task_complete();
            g_state.quit();
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

    let started_at = Instant::now();
    let mut output_ok = true;
    let outcome = match run_diff_merge(sides, &mut parquet).await {
        Ok(outcome) => Some(outcome),
        Err(e) => {
            log::error!("Diff merge failed: {}", e);
            output_ok = false;
            None
        }
    };

    let parquet_rows = parquet.total_rows();
    let ks_entries = if let Some(ref outcome) = outcome {
        match outcome.write_ks(filename_ks).await {
            Ok(count) => count,
            Err(e) => {
                log::error!("{}", e);
                output_ok = false;
                0
            }
        }
    } else {
        0
    };
    if let Err(e) = parquet.close().await {
        log::error!("{}", e);
        output_ok = false;
    }
    if !output_ok {
        g_state.inc_output_error();
    }

    if let Some(outcome) = outcome {
        let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
        g_state.record_data_metrics(
            outcome.received_batches,
            outcome.received_objects,
            outcome.rows,
            outcome.unique_prefixes(),
            parquet_rows,
            ks_entries,
            outcome.bytes_total,
            top_prefixes(&outcome.prefix_stats, 32),
            false,
        );
        info!(
            "Data Map Task — diff streaming complete: rows {} (+{} -{} *{} ={} ignored {}), received objects {}, unique prefixes {}, elapsed {:.3}s, {:.0} objects/sec",
            outcome.rows,
            outcome.plus,
            outcome.minus,
            outcome.astrisk,
            outcome.equal,
            outcome.ignored,
            outcome.received_objects,
            outcome.unique_prefixes(),
            elapsed,
            outcome.received_objects as f64 / elapsed,
        );
    }

    g_state.data_map_task_complete();
    g_state.quit();
}
