use log::info;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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

// foldhash: the map is keyed by per-object directory prefixes and hashed on
// every run of rows, so a fast non-DoS-resistant hasher is appropriate (keys
// come from our own listing, not from an adversarial client).
type PrefixStats = HashMap<String, PrefixAggregate, foldhash::fast::RandomState>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListTextOutputFormat {
    Tsv,
    Ndjson,
}

/// Capacity for the coordinator→worker channels. A small bound matches the
/// incoming batch cadence; when it stays full the output governor reads that as
/// saturation and adds a writer.
const LIST_WORKER_CHANNEL_CAPACITY: usize = 64;

/// Parquet output workers the coordinator starts with; the governor grows this
/// on demand up to the worker cap.
const INITIAL_LIST_WORKERS: usize = 1;

/// Hard cap on output workers regardless of core count (bounds part-file count
/// on very large machines).
pub const MAX_LIST_OUTPUT_WORKERS: usize = 32;

/// Window over which the coordinator judges output saturation before adding a
/// writer. Short enough to ramp quickly on a fast store, long enough to ignore
/// momentary bursts.
const OUTPUT_GROW_SAMPLE: Duration = Duration::from_millis(250);

/// Minimum batches observed in a window before a grow decision is trusted.
const OUTPUT_GROW_MIN_BATCHES: usize = 8;

/// Per-writer busy fraction above which the writers are the bottleneck (they
/// spent most of the window encoding+compressing rather than waiting for input),
/// so adding a writer should raise throughput. A channel-full signal is too
/// brittle here — the bounded prefetch buffer masks the bottleneck — so the
/// governor measures the writers' actual CPU-busy time directly.
const OUTPUT_GROW_BUSY_FRACTION: f64 = 0.85;

/// Per-writer output buffer between the Parquet encoder and the file. The
/// `AsyncArrowWriter` already buffers and flushes whole row groups, so this only
/// coalesces those large, sequential writes into fewer syscalls — past a few MB
/// it stops affecting throughput (the page cache absorbs the writes, and on the
/// fast-store path the writers are CPU-bound on encode+compress, not on file
/// I/O). Sized so the pool's *aggregate* buffer stays bounded as the governor
/// scales out: `MAX_LIST_OUTPUT_WORKERS` × this is ~256 MB, sane on any machine
/// with enough cores to reach the cap. (Was a fixed 100 MB per writer, i.e. up
/// to 3.2 GB once the pool fanned out on a many-core host.)
const LIST_OUTPUT_WRITER_BUF_BYTES: usize = 8 * core::MB;

/// Upper bound on output writers: the machine's parallelism, hard-capped. More
/// writers than cores only thrash, since each does CPU-bound encode+compress.
fn list_output_worker_cap() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(INITIAL_LIST_WORKERS, MAX_LIST_OUTPUT_WORKERS)
}

/// Grow the writer pool when the writers were CPU-busy at least
/// `OUTPUT_GROW_BUSY_FRACTION` of the window (so they, not the input, are the
/// bottleneck) and we are still below the cap. On a rate-limited store the
/// writers idle waiting for input, busy fraction stays low, and the pool stays
/// at one writer.
fn output_should_grow(busy_fraction: f64, batches: usize, workers: usize, cap: usize) -> bool {
    workers < cap
        && batches >= OUTPUT_GROW_MIN_BATCHES
        && busy_fraction >= OUTPUT_GROW_BUSY_FRACTION
}

/// Route one batch to a worker, round-robin. Prefers a worker with buffer space
/// (non-blocking `try_send`); if all are full, blocks on the round-robin target.
/// Returns `Err` only if a worker is gone.
async fn route_batch(
    senders: &[tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>],
    rr: &mut usize,
    prefix_stats: &mut PrefixStats,
    mut batch: Vec<(ObjectKey, ObjectProps)>,
) -> Result<(), ()> {
    account_list_batch_prefixes(prefix_stats, &batch);
    let k = senders.len();
    for off in 0..k {
        let i = (*rr + off) % k;
        match senders[i].try_send(batch) {
            Ok(()) => {
                *rr = (i + 1) % k;
                return Ok(());
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(b)) => batch = b,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return Err(()),
        }
    }
    let i = *rr % k;
    *rr = (i + 1) % k;
    senders[i].send(batch).await.map_err(|_| ())
}

/// Per-prefix KS accounting for a Parquet list batch, done once in the
/// coordinator rather than in each writer: every writer used to keep its own
/// full prefix map (the same hot prefixes duplicated N times) and finalize
/// merged them. Counts only rows the writers will emit — the same
/// `include_in_list_output` predicate they apply — so the KS file describes the
/// Parquet artifact next to it. A writer failure fails the run, so counting
/// before the write cannot publish a KS file for rows that never landed.
fn account_list_batch_prefixes(prefix_stats: &mut PrefixStats, batch: &[(ObjectKey, ObjectProps)]) {
    let mut folder = PrefixRunFolder::default();
    for (key, props) in batch {
        if props.include_in_list_output() && !props.is_common_prefix() {
            folder.add(prefix_stats, key.prefix(), props.size());
        }
    }
    folder.flush(prefix_stats);
}

/// Forward every batch the channel has already buffered to the workers.
/// Returns `false` if a worker is gone. Called before finalizing on quit or
/// completion: batches of segments the checkpoint records as completed are
/// already queued here, and dropping them would silently lose their objects
/// (an interrupt followed by `--resume` skips those segments forever).
/// Close the listing channel ahead of a final drain.  Draining with
/// `try_recv` left the receiver open while finalize ran (seconds, for Parquet),
/// so a segment finishing in that window could still *send* its last batch —
/// successfully, into a channel no one would read again — and then report
/// itself complete.  The final checkpoint recorded it and a later `--resume`
/// skipped it: a silent hole.  Once closed, a late send fails, the segment sees
/// the quit and reports itself incomplete, and `recv` still yields everything
/// already buffered before returning `None`.
fn close_and_drain_start(rx: &mut tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>) {
    rx.close();
}

async fn drain_buffered_batches(
    rx: &mut tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
    senders: &[tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>],
    rr: &mut usize,
    prefix_stats: &mut PrefixStats,
) -> bool {
    close_and_drain_start(rx);
    while let Some(batch) = rx.recv().await {
        if route_batch(senders, rr, prefix_stats, batch).await.is_err() {
            return false;
        }
    }
    true
}

/// Derive the output path for a given part index.  Public so the run
/// manifest can enumerate the part-files a pooled run wrote.
///
/// Index 0 returns `base` unchanged. For index > 0, `.partN` is inserted before
/// the final extension of the basename (e.g. `out/a_ts.parquet` + 2 ->
/// `out/a_ts.part2.parquet`); if the basename has no extension, `.partN` is appended.
pub fn part_path(base: &str, index: usize) -> String {
    if index == 0 {
        return base.to_string();
    }

    // Split into directory prefix and basename so we only inspect the basename's
    // extension (a `.` in a parent directory must not be treated as the extension).
    let slash = base.rfind('/');
    let (dir, name) = match slash {
        Some(pos) => (&base[..=pos], &base[pos + 1..]),
        None => ("", base),
    };

    match name.rfind('.') {
        Some(dot) if dot > 0 => {
            let (stem, ext) = name.split_at(dot);
            format!("{}{}.part{}{}", dir, stem, index, ext)
        }
        _ => format!("{}.part{}", base, index),
    }
}

/// Result returned by a single Parquet output worker.
struct ListWorkerResult {
    stats: ListStreamingStats,
    parquet_rows: usize,
    output_ok: bool,
}

/// A single Parquet output worker: owns one output file and its
/// `AsyncParquetOutput`, ingests batches forwarded by the coordinator, and on
/// channel close finalizes (closes) its own Parquet writer.
async fn list_output_worker(
    mut rx: tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
    part_path: String,
    output_config: OutputConfig,
    g_state: core::GlobalState,
    busy_nanos: Arc<AtomicU64>,
) -> ListWorkerResult {
    let mut stats = ListStreamingStats {
        received_batches: 0,
        received_objects: 0,
        streamed_rows: 0,
        bytes_total: 0,
    };
    let mut output_ok = true;

    let output_file = match tokio::fs::File::create(&part_path).await {
        Ok(f) => f,
        Err(e) => {
            g_state.note_output_error(format!("cannot create output file {}: {}", part_path, e));
            g_state.inc_output_error();
            return ListWorkerResult {
                stats,
                parquet_rows: 0,
                output_ok: false,
            };
        }
    };
    let buf_writer = tokio::io::BufWriter::with_capacity(LIST_OUTPUT_WRITER_BUF_BYTES, output_file);
    let mut parquet = crate::utils::AsyncParquetOutput::new_with_options(
        buf_writer,
        &part_path,
        output_config.row_group_size,
        &output_config.compression,
        output_config.compression_level,
    );

    while let Some(batch) = rx.recv().await {
        // Time the encode+compress so the coordinator can see when writers are
        // the bottleneck and add another.
        let work_start = Instant::now();
        let result = ingest_list_streaming_batch(&mut parquet, &mut stats, batch).await;
        busy_nanos.fetch_add(work_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if let Err(e) = result {
            g_state.note_output_error(format!("{}: {}", part_path, e));
            output_ok = false;
            g_state.inc_output_error();
            break;
        }
    }

    // Capture the row count before close() consumes the writer.
    let parquet_rows = parquet.total_rows();
    if let Err(e) = parquet.close().await {
        g_state.note_output_error(format!("{}: {}", part_path, e));
        output_ok = false;
        g_state.inc_output_error();
    }

    ListWorkerResult {
        stats,
        parquet_rows,
        output_ok,
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

    // Start the worker pool; the output governor grows it on demand.
    let worker_cap = list_output_worker_cap();
    // Shared CPU-busy accumulator across all writers (encode+compress nanos).
    let busy_nanos = Arc::new(AtomicU64::new(0));
    let mut senders: Vec<tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>> =
        Vec::with_capacity(worker_cap);
    let mut handles: Vec<tokio::task::JoinHandle<ListWorkerResult>> =
        Vec::with_capacity(worker_cap);
    for index in 0..INITIAL_LIST_WORKERS {
        let (w_tx, w_rx) = tokio::sync::mpsc::channel(LIST_WORKER_CHANNEL_CAPACITY);
        let handle = tokio::spawn(list_output_worker(
            w_rx,
            part_path(filename_output, index),
            output_config.clone(),
            ctx.g_state.clone(),
            Arc::clone(&busy_nanos),
        ));
        senders.push(w_tx);
        handles.push(handle);
    }

    let started_at = Instant::now();
    let write_started_at = Instant::now();
    let mut last_ts = epoch_secs();
    // Coordinator-side counters for heartbeat logging only; authoritative stats
    // come from the merged worker results at finalize time.
    let mut routed_batches: usize = 0;
    let mut routed_objects: usize = 0;
    // Output governor state: round-robin cursor, per-window batch count, and the
    // writers' cumulative busy-nanos at the last evaluation.
    let mut rr: usize = 0;
    let mut batch_window: usize = 0;
    let mut last_busy_nanos: u64 = 0;
    let mut last_grow_eval = Instant::now();
    // KS prefix accounting lives here, not in the writers (see
    // `account_list_batch_prefixes`).
    let mut prefix_stats = PrefixStats::default();

    loop {
        let recv_result = ctx.data_map_channel.recv().await;

        match recv_result {
            Some(batch) => {
                routed_batches += 1;
                routed_objects += batch.len();
                if route_batch(&senders, &mut rr, &mut prefix_stats, batch)
                    .await
                    .is_err()
                {
                    log::error!("Data Map Task — list streaming worker gone, finalizing output");
                    ctx.g_state.inc_output_error();
                    coordinator_finalize(
                        senders,
                        handles,
                        prefix_stats,
                        &ctx.g_state,
                        filename_ks,
                        filename_output,
                        started_at,
                        write_started_at,
                    )
                    .await;
                    ctx.complete();
                    ctx.quit();
                    return;
                }
                batch_window += 1;

                // Output governor: if the writers were CPU-busy most of the
                // window (they, not the input, are the bottleneck) and we are
                // below the cap, add a writer with its own part-file.
                let now_i = Instant::now();
                let window = now_i.duration_since(last_grow_eval);
                if window >= OUTPUT_GROW_SAMPLE {
                    let busy_now = busy_nanos.load(Ordering::Relaxed);
                    let busy_delta = busy_now.saturating_sub(last_busy_nanos);
                    // Busy fraction = writer CPU time / (wall window × current writers).
                    let capacity_nanos = window.as_secs_f64() * senders.len() as f64 * 1e9;
                    let busy_fraction = busy_delta as f64 / capacity_nanos.max(1.0);
                    if output_should_grow(busy_fraction, batch_window, senders.len(), worker_cap) {
                        let index = senders.len();
                        let (w_tx, w_rx) = tokio::sync::mpsc::channel(LIST_WORKER_CHANNEL_CAPACITY);
                        handles.push(tokio::spawn(list_output_worker(
                            w_rx,
                            part_path(filename_output, index),
                            output_config.clone(),
                            ctx.g_state.clone(),
                            Arc::clone(&busy_nanos),
                        )));
                        senders.push(w_tx);
                        info!(
                            "Data Map Task — output scaled to {} Parquet writers (writers {:.0}% busy)",
                            senders.len(),
                            busy_fraction * 100.0
                        );
                    }
                    batch_window = 0;
                    last_busy_nanos = busy_now;
                    last_grow_eval = now_i;
                }
            }
            None => {
                info!("Data Map Task — list streaming channel disconnected, finalizing output");
                coordinator_finalize(
                    senders,
                    handles,
                    prefix_stats,
                    &ctx.g_state,
                    filename_ks,
                    filename_output,
                    started_at,
                    write_started_at,
                )
                .await;
                ctx.complete();
                return;
            }
        }

        if ctx.is_quit() {
            // Drain buffered batches before finalizing; without this, an
            // interrupt drops in-channel batches from segments the final
            // checkpoint save records as completed, and a later --resume
            // skips those segments — a silent hole in the combined output.
            if !drain_buffered_batches(
                &mut ctx.data_map_channel,
                &senders,
                &mut rr,
                &mut prefix_stats,
            )
            .await
            {
                log::error!("Data Map Task — list streaming worker gone, finalizing output");
                ctx.g_state.inc_output_error();
            }
            info!("Data Map Task — list streaming force quit, finalizing output");
            coordinator_finalize(
                senders,
                handles,
                prefix_stats,
                &ctx.g_state,
                filename_ks,
                filename_output,
                started_at,
                write_started_at,
            )
            .await;
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            // Drained batches are forwarded straight to finalize; no further
            // pool growth on this path.
            if !drain_buffered_batches(
                &mut ctx.data_map_channel,
                &senders,
                &mut rr,
                &mut prefix_stats,
            )
            .await
            {
                log::error!("Data Map Task — list streaming worker gone, finalizing output");
                ctx.g_state.inc_output_error();
            }
            info!("Data Map Task — list streaming all list tasks done, finalizing output");
            coordinator_finalize(
                senders,
                handles,
                prefix_stats,
                &ctx.g_state,
                filename_ks,
                filename_output,
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
                "Data Map Task — list streaming received batches {}, received objects {}, {:.0} objects/sec",
                routed_batches,
                routed_objects,
                routed_objects as f64 / elapsed
            );
            last_ts = now;
        }
    }
}

/// Drain the worker pool: drop all senders (so workers observe channel close and
/// finalize their own Parquet outputs), await all workers, merge their results,
/// then write the aggregated KS counts and record run metrics.
async fn coordinator_finalize(
    senders: Vec<tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>>,
    handles: Vec<tokio::task::JoinHandle<ListWorkerResult>>,
    merged_prefix_stats: PrefixStats,
    g_state: &core::GlobalState,
    filename_ks: &str,
    filename_output: &str,
    started_at: Instant,
    write_started_at: Instant,
) {
    // Drop senders so each worker's rx.recv() returns None and it finalizes.
    drop(senders);

    let mut merged_stats = ListStreamingStats {
        received_batches: 0,
        received_objects: 0,
        streamed_rows: 0,
        bytes_total: 0,
    };
    let mut parquet_rows: usize = 0;
    let mut output_ok = true;
    // One output file per writer: the base path plus one `.partN` each. The
    // manifest records exactly these, so a reused output path cannot pick up
    // part-files left behind by an earlier, wider run.
    let output_files = handles.len();

    for handle in handles {
        match handle.await {
            Ok(result) => {
                merged_stats.received_batches += result.stats.received_batches;
                merged_stats.received_objects += result.stats.received_objects;
                merged_stats.streamed_rows += result.stats.streamed_rows;
                merged_stats.bytes_total = merged_stats
                    .bytes_total
                    .saturating_add(result.stats.bytes_total);
                parquet_rows += result.parquet_rows;
                output_ok &= result.output_ok;
            }
            Err(e) => {
                log::error!("Data Map Task — list streaming worker join error: {}", e);
                output_ok = false;
            }
        }
    }

    let ks_entries = match write_ks_counts(filename_ks, &merged_prefix_stats).await {
        Ok(count) => count,
        Err(e) => {
            g_state.note_output_error(format!("{}: {}", filename_ks, e));
            output_ok = false;
            0
        }
    };
    if !output_ok {
        g_state.inc_output_error();
    }

    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    let write_elapsed = write_started_at.elapsed().as_secs_f64();
    g_state.record_data_metrics(
        merged_stats.received_batches,
        merged_stats.received_objects,
        merged_stats.streamed_rows,
        merged_prefix_stats.len(),
        parquet_rows,
        ks_entries,
        merged_stats.bytes_total,
        top_prefixes(&merged_prefix_stats, 32),
        false,
        output_files,
    );
    info!(
        "Data Map Task — list streaming complete: streamed rows {}, received batches {}, received objects {}, unique prefixes {}, elapsed {:.3}s, {:.0} objects/sec",
        merged_stats.streamed_rows,
        merged_stats.received_batches,
        merged_stats.received_objects,
        merged_prefix_stats.len(),
        elapsed,
        merged_stats.received_objects as f64 / elapsed,
    );
    info!(
        "Data Map Task — list streaming output metrics: Parquet rows {} to '{}', KS write entries {} to '{}', write elapsed {:.3}s",
        parquet_rows, filename_output, ks_entries, filename_ks, write_elapsed,
    );
}

pub async fn data_map_task_list_summary_only(mut ctx: DataMapContext) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — list summary-only started");

    let mut prefix_stats = PrefixStats::default();
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
            // Count batches the channel already buffered so interrupt metrics
            // (and any checkpointed-completed segments) match received data.
            close_and_drain_start(&mut ctx.data_map_channel);
            while let Some(batch) = ctx.data_map_channel.recv().await {
                ingest_list_summary_batch(&mut prefix_stats, &mut stats, batch);
            }
            info!("Data Map Task — list summary-only force quit, finalizing");
            finalize_list_summary_only(&ctx.g_state, &prefix_stats, stats, started_at);
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            close_and_drain_start(&mut ctx.data_map_channel);
            while let Some(batch) = ctx.data_map_channel.recv().await {
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

    let mut prefix_stats = PrefixStats::default();
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
                    // Record what was streamed before stopping: the manifest
                    // otherwise reported zero rows for a run that wrote
                    // millions before the reader went away (`| head`).
                    record_list_stdout_metrics(&ctx.g_state, &prefix_stats, &stats);
                    ctx.g_state.note_output_error(
                        "writing rows to stdout failed (a closed pipe, e.g. `| head`, or a \
                         full disk); the rows streamed before it are complete"
                            .to_string(),
                    );
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
            // Write out batches the channel already buffered: they were
            // received before the interrupt and dropping them would silently
            // truncate the streamed rows.
            close_and_drain_start(&mut ctx.data_map_channel);
            while let Some(batch) = ctx.data_map_channel.recv().await {
                if !ingest_list_stdout_batch(
                    &mut writer,
                    format,
                    &mut prefix_stats,
                    &mut stats,
                    batch,
                )
                .await
                {
                    // Record what was streamed before stopping: the manifest
                    // otherwise reported zero rows for a run that wrote
                    // millions before the reader went away (`| head`).
                    record_list_stdout_metrics(&ctx.g_state, &prefix_stats, &stats);
                    ctx.g_state.note_output_error(
                        "writing rows to stdout failed (a closed pipe, e.g. `| head`, or a \
                         full disk); the rows streamed before it are complete"
                            .to_string(),
                    );
                    ctx.g_state.inc_output_error();
                    ctx.complete();
                    ctx.quit();
                    return;
                }
            }
            info!("Data Map Task — list stdout force quit, finalizing");
            finalize_list_stdout(&ctx.g_state, &mut writer, &prefix_stats, stats, started_at).await;
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            close_and_drain_start(&mut ctx.data_map_channel);
            while let Some(batch) = ctx.data_map_channel.recv().await {
                if !ingest_list_stdout_batch(
                    &mut writer,
                    format,
                    &mut prefix_stats,
                    &mut stats,
                    batch,
                )
                .await
                {
                    // Record what was streamed before stopping: the manifest
                    // otherwise reported zero rows for a run that wrote
                    // millions before the reader went away (`| head`).
                    record_list_stdout_metrics(&ctx.g_state, &prefix_stats, &stats);
                    ctx.g_state.note_output_error(
                        "writing rows to stdout failed (a closed pipe, e.g. `| head`, or a \
                         full disk); the rows streamed before it are complete"
                            .to_string(),
                    );
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
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) -> Result<(), String> {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    // Byte accounting covers only objects included in the output so the
    // manifest metrics agree across parquet/tsv/ndjson/summary-only runs; the
    // matching KS prefix counts are kept by the coordinator.
    let written = parquet
        .write_list_batch_filtered(batch, OUTPUT_FLAG_EQUAL, |_key, props| {
            let include = props.include_in_list_output();
            if include {
                stats.bytes_total = stats.bytes_total.saturating_add(props.size());
            }
            include
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
    let mut folder = PrefixRunFolder::default();
    // Counted into `stats` only once the batch is written: a reader that
    // closed the pipe (`| head`) used to leave the manifest claiming the
    // rows of the batch that failed to write.
    let mut rows = 0usize;
    let mut bytes = 0u64;

    for (key, props) in batch {
        if !props.include_in_list_output() {
            continue;
        }

        if !props.is_common_prefix() {
            folder.add(prefix_stats, key.prefix(), props.size());
        }
        rows += 1;
        bytes = bytes.saturating_add(props.size());

        if let Err(e) = render_text_row(
            &mut out,
            format,
            key.as_str(),
            props.size(),
            props.last_modified(),
        ) {
            log::error!("NDJSON serialization error for '{}': {}", key, e);
            return false;
        }
    }

    folder.flush(prefix_stats);
    if !out.is_empty() {
        if let Err(e) = writer.write_all(&out).await {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                log::error!(
                    "Stdout closed by its reader (e.g. `| head`); stopping the listing. \
                     The output is partial and the run is reported as failed."
                );
            } else {
                log::error!("Stdout write error: {}", e);
            }
            return false;
        }
    }

    stats.streamed_rows += rows;
    stats.bytes_total = stats.bytes_total.saturating_add(bytes);
    true
}

/// Render one stdout list row. Hand-rolled (no `fmt` machinery or per-row
/// struct serialization) because it runs once per object on the stdout hot
/// path; the bytes are identical to the former `writeln!("{}\t{}\t{}")` TSV
/// and `serde_json` `{"k":..,"s":..,"m":..}` NDJSON renderings.
fn render_text_row(
    out: &mut Vec<u8>,
    format: ListTextOutputFormat,
    key: &str,
    size: u64,
    last_modified: u64,
) -> Result<(), serde_json::Error> {
    let mut ints = itoa::Buffer::new();
    match format {
        ListTextOutputFormat::Tsv => {
            out.extend_from_slice(tsv_escape(key).as_bytes());
            out.push(b'\t');
            out.extend_from_slice(ints.format(size).as_bytes());
            out.push(b'\t');
            out.extend_from_slice(ints.format(last_modified).as_bytes());
            out.push(b'\n');
        }
        ListTextOutputFormat::Ndjson => {
            // Only the key needs JSON string escaping.
            out.extend_from_slice(b"{\"k\":");
            serde_json::to_writer(&mut *out, key)?;
            out.extend_from_slice(b",\"s\":");
            out.extend_from_slice(ints.format(size).as_bytes());
            out.extend_from_slice(b",\"m\":");
            out.extend_from_slice(ints.format(last_modified).as_bytes());
            out.extend_from_slice(b"}\n");
        }
    }
    Ok(())
}

fn ingest_list_summary_batch(
    prefix_stats: &mut PrefixStats,
    stats: &mut ListStreamingStats,
    batch: Vec<(ObjectKey, ObjectProps)>,
) {
    stats.received_batches += 1;
    stats.received_objects += batch.len();

    let mut folder = PrefixRunFolder::default();
    for (key, props) in batch {
        if props.include_in_list_output() {
            if !props.is_common_prefix() {
                folder.add(prefix_stats, key.prefix(), props.size());
            }
            stats.streamed_rows += 1;
            stats.bytes_total = stats.bytes_total.saturating_add(props.size());
        }
    }
    folder.flush(prefix_stats);
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

/// Prefix accounting that coalesces runs of equal prefixes. Listing batches
/// arrive in S3 key order, so consecutive objects overwhelmingly share a
/// prefix; folding a whole run into one map update replaces the per-object
/// hash lookup with a per-object string compare against a reused buffer.
/// Callers must `flush` after their batch loop.
#[derive(Default)]
struct PrefixRunFolder {
    prefix: String,
    objects: usize,
    bytes: u64,
}

impl PrefixRunFolder {
    fn add(&mut self, prefix_stats: &mut PrefixStats, prefix: &str, size: u64) {
        if self.objects > 0 && self.prefix == prefix {
            self.objects += 1;
            self.bytes = self.bytes.saturating_add(size);
            return;
        }
        self.flush(prefix_stats);
        self.prefix.clear();
        self.prefix.push_str(prefix);
        self.objects = 1;
        self.bytes = size;
    }

    fn flush(&mut self, prefix_stats: &mut PrefixStats) {
        if self.objects == 0 {
            return;
        }
        let entry = match prefix_stats.get_mut(self.prefix.as_str()) {
            Some(entry) => entry,
            None => prefix_stats.entry(self.prefix.clone()).or_default(),
        };
        entry.objects += self.objects;
        entry.bytes = entry.bytes.saturating_add(self.bytes);
        self.objects = 0;
        self.bytes = 0;
    }
}

fn record_list_stdout_metrics(
    g_state: &core::GlobalState,
    prefix_stats: &PrefixStats,
    stats: &ListStreamingStats,
) {
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
        0,
    );
}

async fn finalize_list_stdout<W: tokio::io::AsyncWrite + Unpin + Send>(
    g_state: &core::GlobalState,
    writer: &mut W,
    prefix_stats: &PrefixStats,
    stats: ListStreamingStats,
    started_at: Instant,
) {
    if let Err(e) = writer.flush().await {
        g_state.note_output_error(format!("stdout flush failed: {}", e));
        g_state.inc_output_error();
    }
    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    record_list_stdout_metrics(g_state, prefix_stats, &stats);
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
        0,
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

/// Bytes of encoded KS rows buffered before a write, so a bucket with a
/// million distinct prefixes costs a few hundred writes rather than one per
/// row.
const KS_WRITE_CHUNK_BYTES: usize = 256 * 1024;

async fn write_ks_counts(path: &str, counts: &PrefixStats) -> Result<usize, String> {
    // Borrow the prefixes rather than cloning them: on a deep-hierarchy bucket
    // the map already holds one String per distinct prefix, and copying the
    // whole set again just to sort it doubled that for the length of the write.
    let mut entries: Vec<(&str, usize)> = counts
        .iter()
        .map(|(prefix, stats)| (prefix.as_str(), stats.objects))
        .collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
    write_ks_entries(path, &entries).await
}

/// Append one RFC 4180 field: quoted, with embedded quotes doubled. Object
/// keys may contain `"`, `,` and newlines — all legal S3 key bytes, all of
/// which reach this column through the key's prefix, and none of which the
/// previous bare `format!` escaped.
fn push_csv_field(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    for byte in value.as_bytes() {
        if *byte == b'"' {
            out.push(b'"');
        }
        out.push(*byte);
    }
    out.push(b'"');
}

async fn write_ks_entries(path: &str, entries: &[(&str, usize)]) -> Result<usize, String> {
    match tokio::fs::File::create(path).await {
        Ok(ks_file) => {
            let mut buf = tokio::io::BufWriter::new(ks_file);
            // One reused encode buffer and one reused number buffer: the old
            // per-row `format!` allocated once per distinct prefix, and wrote
            // one await per row.
            let mut chunk: Vec<u8> = Vec::with_capacity(KS_WRITE_CHUNK_BYTES + 1024);
            let mut number = String::new();
            for (prefix, count) in entries {
                push_csv_field(&mut chunk, prefix);
                chunk.push(b',');
                number.clear();
                let _ = std::fmt::Write::write_fmt(&mut number, format_args!("{}", count));
                push_csv_field(&mut chunk, &number);
                chunk.push(b'\n');
                if chunk.len() >= KS_WRITE_CHUNK_BYTES {
                    if let Err(e) = buf.write_all(&chunk).await {
                        return Err(format!("Failed to write KS file {}: {}", path, e));
                    }
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                if let Err(e) = buf.write_all(&chunk).await {
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
    // Borrow and select rather than clone-and-sort: this runs once per run,
    // but a deep-hierarchy bucket has one entry per distinct prefix, and
    // cloning every key to fully sort them for the top 32 was the second
    // largest cost in the run's accounting layer.
    let mut entries: Vec<(&str, usize, u64)> = prefix_stats
        .iter()
        .map(|(prefix, stats)| (prefix.as_str(), stats.objects, stats.bytes))
        .collect();
    let rank = |a: &(&str, usize, u64), b: &(&str, usize, u64)| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.cmp(b.0))
    };
    let limit = limit.min(entries.len());
    if limit < entries.len() {
        entries.select_nth_unstable_by(limit, rank);
        entries.truncate(limit);
    }
    entries.sort_unstable_by(rank);
    entries
        .into_iter()
        .map(|(prefix, objects, bytes)| core::PrefixMetric {
            prefix: prefix.to_string(),
            objects,
            bytes,
        })
        .collect()
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Streaming diff (merge-join) ────────────────────────────
//
// Each side is listed as a fixed set of key-ordered segments, so its batches
// arrive in strict ascending key order.  Merging the two ordered streams
// classifies every key on the fly — left-only (Plus), right-only (Minus),
// or present on both sides (Equal/Astrisk via ObjectProps::classify_pair)
// — and streams rows straight to Parquet.  Memory is bounded by the
// channel buffers instead of the combined object count.

/// Per-side, segment-ordered receivers for the two diff sides.  Segments
/// are static key-space partitions, so draining the receivers in index
/// order yields one globally ordered stream per side while the segments
/// themselves list in parallel (bounded by each channel's small capacity,
/// which acts as the prefetch window).
pub struct DiffStreamSides {
    pub left: Vec<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
    pub right: Vec<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
}

struct DiffSideStream {
    rx: Option<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
    pending: std::vec::IntoIter<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
    buf: std::collections::VecDeque<(ObjectKey, ObjectProps)>,
    closed: bool,
    last_key: Option<String>,
    name: &'static str,
    received_batches: usize,
    received_objects: usize,
}

impl DiffSideStream {
    fn new(
        receivers: Vec<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
        name: &'static str,
    ) -> Self {
        let mut pending = receivers.into_iter();
        let rx = pending.next();
        Self {
            rx,
            pending,
            buf: std::collections::VecDeque::new(),
            closed: false,
            last_key: None,
            name,
            received_batches: 0,
            received_objects: 0,
        }
    }

    /// Receive the next batch, advancing through the segment receivers in
    /// index order as each segment's channel closes.
    async fn recv_next(&mut self) -> Option<Vec<(ObjectKey, ObjectProps)>> {
        loop {
            let rx = self.rx.as_mut()?;
            match rx.recv().await {
                Some(batch) => return Some(batch),
                None => self.rx = self.pending.next(),
            }
        }
    }

    /// Fill the buffer until it has an item or the side is exhausted.
    /// Enforces ascending key order (the merge's correctness contract,
    /// including across segment transitions) and keeps the latest entry for
    /// same-key duplicates, matching the legacy map-based behavior.
    async fn ensure_filled(&mut self) -> Result<bool, String> {
        while self.buf.is_empty() && !self.closed {
            match self.recv_next().await {
                Some(batch) => {
                    self.received_batches += 1;
                    self.received_objects += batch.len();
                    for (key, props) in batch {
                        // The ordering-guard cursor reuses one String buffer
                        // instead of allocating per object.
                        match self.last_key.as_mut() {
                            Some(prev) if key.as_str() < prev.as_str() => {
                                return Err(format!(
                                    "{} listing returned keys out of order ('{}' after '{}'); \
                                     diff requires S3 lexicographic ordering",
                                    self.name, key, prev
                                ));
                            }
                            Some(prev) if key.as_str() == prev.as_str() => {
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
                            Some(prev) => {
                                prev.clear();
                                prev.push_str(key.as_str());
                            }
                            None => self.last_key = Some(key.as_str().to_string()),
                        }
                        self.buf.push_back((key, props));
                    }
                }
                None => self.closed = true,
            }
        }
        Ok(!self.buf.is_empty())
    }
}

/// Buffered streaming row sink: rows accumulate in merge (key) order, each
/// with its DiffFlag, and flush as Parquet batches; per-prefix counts feed the
/// KS file.  (Buffering per flag instead wrote each flag's rows in clumps, so
/// a diff file was not in key order and its Key statistics could not prune.)
struct DiffRowSink {
    buf: Vec<(ObjectKey, ObjectProps, u8)>,
    prefix_stats: PrefixStats,
    // Merged rows arrive in key order, so consecutive rows share a prefix:
    // fold runs instead of a hash lookup per row, as the list paths do.
    prefix_run: PrefixRunFolder,
    rows: usize,
    plus: usize,
    minus: usize,
    astrisk: usize,
    equal: usize,
    ignored: usize,
}

const DIFF_SINK_FLUSH_ROWS: usize = 8192;

/// Batches queued between the merge task and the Parquet writer loop — the
/// pipeline depth that lets classification run ahead of encode+compress.
const DIFF_WRITE_PIPELINE_CAP: usize = 4;

/// A flushed row batch on its way to the Parquet writer loop.
type DiffWriteBatch = Vec<(ObjectKey, ObjectProps, u8)>;

impl DiffRowSink {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            prefix_stats: PrefixStats::default(),
            prefix_run: PrefixRunFolder::default(),
            rows: 0,
            plus: 0,
            minus: 0,
            astrisk: 0,
            equal: 0,
            ignored: 0,
        }
    }

    async fn push(
        &mut self,
        writer_tx: &tokio::sync::mpsc::Sender<DiffWriteBatch>,
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
        // Prefix stats (KS file, bytes_total, unique/top prefixes) describe
        // the rows written — as in list mode — not filter-ignored pairs, so
        // the manifest's metrics agree with the Parquet artifact.
        self.prefix_run
            .add(&mut self.prefix_stats, key.prefix(), props.size());
        self.rows += 1;
        self.buf.push((key, props, flag));
        if self.buf.len() >= DIFF_SINK_FLUSH_ROWS {
            let batch = std::mem::replace(&mut self.buf, Vec::with_capacity(DIFF_SINK_FLUSH_ROWS));
            send_to_writer(writer_tx, batch).await?;
        }
        Ok(())
    }

    async fn flush_all(
        &mut self,
        writer_tx: &tokio::sync::mpsc::Sender<DiffWriteBatch>,
    ) -> Result<(), String> {
        self.prefix_run.flush(&mut self.prefix_stats);
        if !self.buf.is_empty() {
            let batch = std::mem::take(&mut self.buf);
            send_to_writer(writer_tx, batch).await?;
        }
        Ok(())
    }
}

/// Hand a flushed batch to the Parquet writer loop. A closed channel means
/// the writer stopped on a write error; that error is what the caller of
/// `run_diff_merge` reports, this message only unblocks the merge.
async fn send_to_writer(
    writer_tx: &tokio::sync::mpsc::Sender<DiffWriteBatch>,
    batch: DiffWriteBatch,
) -> Result<(), String> {
    writer_tx
        .send(batch)
        .await
        .map_err(|_| "diff Parquet writer stopped (write error)".to_string())
}

/// Merge the two ordered diff streams into the sink, handing flushed row
/// batches to the Parquet writer loop. Returns Err on ordering violations
/// or when the writer stopped.
async fn merge_diff_streams(
    left: &mut DiffSideStream,
    right: &mut DiffSideStream,
    writer_tx: &tokio::sync::mpsc::Sender<DiffWriteBatch>,
    sink: &mut DiffRowSink,
    aborted: &(dyn Fn() -> bool + Send + Sync),
) -> Result<(), String> {
    loop {
        // A side that died mid-listing closes its channels, which is
        // indistinguishable here from that side having ended: the merge would
        // classify every remaining key of the *other* side as one-sided,
        // producing a structurally valid diff that asserts something never
        // observed. Stop instead — the caller discards the partial output.
        if aborted() {
            return Err(
                "run aborted before both sides finished listing; a partial diff would \
                 classify unread keys as one-sided"
                    .to_string(),
            );
        }
        // A dropped receiver means the writer stopped on a write error. Bail
        // out here, per iteration, instead of waiting for the next flushed
        // batch to fail its send: rows only reach the writer every
        // DIFF_SINK_FLUSH_ROWS, and a diff filter that ignores the remaining
        // pairs never sends at all — the merge would keep draining the S3
        // listing to exhaustion after the output had already failed.
        if writer_tx.is_closed() {
            return Err("diff Parquet writer stopped (write error)".to_string());
        }
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
                if core::ObjectProps::include_one_sided(&props) {
                    sink.push(writer_tx, OUTPUT_FLAG_PLUS, key, props).await?;
                } else {
                    sink.ignored += 1;
                }
            }
            std::cmp::Ordering::Greater => {
                let (key, props) = right.buf.pop_front().expect("filled");
                if core::ObjectProps::include_one_sided(&props) {
                    sink.push(writer_tx, OUTPUT_FLAG_MINUS, key, props).await?;
                } else {
                    sink.ignored += 1;
                }
            }
            std::cmp::Ordering::Equal => {
                let (key, left_props) = left.buf.pop_front().expect("filled");
                let (_rkey, right_props) = right.buf.pop_front().expect("filled");
                match core::ObjectProps::classify_pair(&left_props, &right_props) {
                    Some(MatchResult::Astrisk) => {
                        sink.push(writer_tx, OUTPUT_FLAG_ASTRISK, key, left_props)
                            .await?;
                    }
                    Some(_) => {
                        sink.push(writer_tx, OUTPUT_FLAG_EQUAL, key, left_props)
                            .await?;
                    }
                    None => sink.ignored += 1,
                }
            }
        }
    }
    // Both streams ended — but a side whose *last* segment failed also ends
    // its stream (the failing task drops its sender after setting quit), so
    // exhaustion is not proof of completion. Check once more, or the merge
    // reports success over a side it never finished reading.
    if aborted() {
        return Err(
            "run aborted while the last segment of a side was listing; a partial diff \
             would classify unread keys as one-sided"
                .to_string(),
        );
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
///
/// The merge runs as its own task while this task drives the Parquet writes,
/// so classification/ingest overlaps encode+compress instead of serializing
/// with it in one loop; the bounded channel between them is the pipeline
/// depth. The `&mut parquet` contract is unchanged — all rows are written
/// before this returns.
pub async fn run_diff_merge<W: tokio::io::AsyncWrite + Unpin + Send>(
    sides: DiffStreamSides,
    parquet: &mut crate::utils::AsyncParquetOutput<W>,
    aborted: impl Fn() -> bool + Send + Sync + 'static,
) -> Result<DiffMergeOutcome, String> {
    let (writer_tx, mut writer_rx) =
        tokio::sync::mpsc::channel::<DiffWriteBatch>(DIFF_WRITE_PIPELINE_CAP);

    let merge_task = tokio::spawn(async move {
        let mut left = DiffSideStream::new(sides.left, "left");
        let mut right = DiffSideStream::new(sides.right, "right");
        let mut sink = DiffRowSink::new();

        merge_diff_streams(&mut left, &mut right, &writer_tx, &mut sink, &aborted).await?;
        sink.flush_all(&writer_tx).await?;

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
    });

    // Writer loop: drains until the merge task drops its sender (success) or
    // a write fails (dropping the receiver unblocks the merge, whose sends
    // then fail fast).
    let mut write_error: Option<String> = None;
    while let Some(batch) = writer_rx.recv().await {
        if let Err(e) = parquet.write_flagged_rows(batch).await {
            write_error = Some(e);
            break;
        }
    }
    drop(writer_rx);

    let merged = merge_task
        .await
        .map_err(|e| format!("diff merge task failed: {}", e))?;
    match write_error {
        // The write error is the root cause; the merge error it induced
        // ("writer stopped") is secondary.
        Some(e) => Err(e),
        None => merged,
    }
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
            g_state.note_output_error(format!(
                "cannot create output file {}: {}",
                filename_output, e
            ));
            g_state.inc_output_error();
            g_state.data_map_task_complete();
            g_state.quit();
            return;
        }
    };
    let buf_writer = tokio::io::BufWriter::with_capacity(LIST_OUTPUT_WRITER_BUF_BYTES, output_file);
    let mut parquet = crate::utils::AsyncParquetOutput::new_with_options(
        buf_writer,
        filename_output,
        output_config.row_group_size,
        &output_config.compression,
        output_config.compression_level,
    );

    let started_at = Instant::now();
    let mut output_ok = true;
    let abort_state = g_state.clone();
    let outcome = match run_diff_merge(sides, &mut parquet, move || abort_state.is_quit()).await {
        Ok(outcome) => Some(outcome),
        Err(e) => {
            log::error!("Diff merge failed: {}", e);
            // A merge stopped because the run is quitting — a side failed
            // (AccessDenied, retries exhausted) or the run was interrupted —
            // is explained by that fatal error or interrupt; counting it as an
            // output failure made a side's AccessDenied exit 5 "output write
            // failed" instead of 3 with the S3 reason. Anything else (an
            // ordering violation, a write error) is an output failure with
            // this message as its reason.
            if !g_state.is_quit() {
                g_state.note_output_error(format!("diff merge failed: {}", e));
                output_ok = false;
            }
            None
        }
    };

    let parquet_rows = parquet.total_rows();
    let ks_entries = if let Some(ref outcome) = outcome {
        match outcome.write_ks(filename_ks).await {
            Ok(count) => count,
            Err(e) => {
                log::error!("{}", e);
                g_state.note_output_error(format!("{}: {}", filename_ks, e));
                output_ok = false;
                0
            }
        }
    } else {
        0
    };
    if let Err(e) = parquet.close().await {
        g_state.note_output_error(format!("{}: {}", filename_output, e));
        output_ok = false;
    }
    if !output_ok {
        g_state.inc_output_error();
    }
    if outcome.is_none() {
        // The merge did not run to completion, so whatever reached the file
        // describes part of one side against an unfinished other side. It
        // parses, which is exactly what makes it dangerous — remove it rather
        // than leave a valid-looking diff behind. (The KS file is only written
        // for a completed merge, so there is nothing of this run's to remove
        // there.)
        match tokio::fs::remove_file(filename_output).await {
            Ok(()) => log::error!(
                "Removed incomplete diff output {} — a partial merge cannot describe both sides",
                filename_output
            ),
            Err(e) => log::error!(
                "Incomplete diff output {} could not be removed ({}); do not consume it",
                filename_output,
                e
            ),
        }
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
            1,
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

#[cfg(test)]
mod tests {
    use super::{
        ListTextOutputFormat, OUTPUT_GROW_MIN_BATCHES, output_should_grow, part_path,
        render_text_row,
    };

    const AWKWARD_KEYS: &[&str] = &[
        "a/plain",
        "b/tab\there",
        "c/quote\"q",
        "d/back\\slash",
        "e/uni-中文-é",
        "f/nl\nx\rx",
        "h/ctl\u{1}x\u{7f}",
        "i/emoji-😀",
        "k/\u{2028}ls",
        "",
    ];

    #[test]
    fn text_rows_match_the_formatted_renderings_byte_for_byte() {
        #[derive(serde::Serialize)]
        struct Row<'a> {
            k: &'a str,
            s: u64,
            m: u64,
        }
        for key in AWKWARD_KEYS {
            for (size, mtime) in [(0, 0), (1, 1_767_225_600), (u64::MAX, u64::MAX)] {
                let mut ndjson = Vec::new();
                render_text_row(&mut ndjson, ListTextOutputFormat::Ndjson, key, size, mtime)
                    .unwrap();
                let mut want = serde_json::to_vec(&Row {
                    k: key,
                    s: size,
                    m: mtime,
                })
                .unwrap();
                want.push(b'\n');
                assert_eq!(ndjson, want, "ndjson row for {key:?}");

                let mut tsv = Vec::new();
                render_text_row(&mut tsv, ListTextOutputFormat::Tsv, key, size, mtime).unwrap();
                let want = format!("{}\t{}\t{}\n", super::tsv_escape(key), size, mtime);
                assert_eq!(tsv, want.as_bytes(), "tsv row for {key:?}");
            }
        }
    }

    #[test]
    fn output_governor_grows_only_when_writers_busy_and_below_cap() {
        let cap = 4;
        let batches = 100;
        // Writers CPU-busy above threshold, below cap -> grow.
        assert!(output_should_grow(0.95, batches, 1, cap));
        assert!(output_should_grow(0.85, batches, 3, cap));
        // At the cap: never grow, however busy.
        assert!(!output_should_grow(1.0, batches, cap, cap));
        // Writers have idle time (input-bound, not writer-bound) -> hold.
        assert!(!output_should_grow(0.84, batches, 1, cap));
        assert!(!output_should_grow(0.1, batches, 1, cap));
        // Too few batches to judge -> hold even if fully busy.
        assert!(!output_should_grow(
            1.0,
            OUTPUT_GROW_MIN_BATCHES - 1,
            1,
            cap
        ));
    }

    #[test]
    fn part_path_index_zero_is_unchanged() {
        assert_eq!(part_path("out/a_b_ts.parquet", 0), "out/a_b_ts.parquet");
        assert_eq!(part_path("plain", 0), "plain");
    }

    #[test]
    fn part_path_inserts_before_extension() {
        assert_eq!(
            part_path("out/a_b_ts.parquet", 2),
            "out/a_b_ts.part2.parquet"
        );
        assert_eq!(part_path("file.parquet", 1), "file.part1.parquet");
    }

    #[test]
    fn part_path_no_extension_appends() {
        assert_eq!(part_path("out/datafile", 3), "out/datafile.part3");
        assert_eq!(part_path("noext", 5), "noext.part5");
    }

    #[test]
    fn part_path_ignores_dot_in_parent_dir() {
        // The dot is in the directory component, not the basename.
        assert_eq!(part_path("a.b/datafile", 2), "a.b/datafile.part2");
    }

    #[test]
    fn part_path_leading_dot_basename_appends() {
        // A dotfile with no further extension should append, not split the leading dot.
        assert_eq!(part_path("dir/.hidden", 2), "dir/.hidden.part2");
    }
}
