use log::info;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

use crate::config::OutputConfig;
use crate::core::{self, DataMapContext, MatchResult, ObjectKey, ObjectProps};
use crate::s3_output::PartSink;
use crate::segment_order::SegmentOrder;
use crate::utils::{DiffRow, DiffStatus};

// ── Output direction flags ─────────────────────────────────

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

/// File name of committed part `index` in an output directory.
pub fn part_file_name(index: usize) -> String {
    format!("part-{:05}.parquet", index)
}

/// Where part `index` is written before the run commits it: a hidden temp
/// file next to its final name, so a reader globbing `part-*.parquet` never
/// sees a half-written file.
pub fn tmp_part_path(dir: &Path, index: usize) -> PathBuf {
    dir.join(format!(".{}.tmp", part_file_name(index)))
}

/// Result returned by a single Parquet output worker.
struct ListWorkerResult {
    index: usize,
    stats: ListStreamingStats,
    parquet_rows: usize,
    output_ok: bool,
}

/// A single Parquet output worker: owns one output file and its
/// `AsyncParquetOutput`, ingests batches forwarded by the coordinator, and on
/// channel close finalizes (closes) its own Parquet writer.
async fn list_output_worker(
    mut rx: tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
    index: usize,
    sink: PartSink,
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
    let part_path = sink.describe(index);

    let writer = match sink.open(index).await {
        Ok(w) => w,
        Err(e) => {
            g_state.note_output_error(e);
            g_state.inc_output_error();
            return ListWorkerResult {
                index,
                stats,
                parquet_rows: 0,
                output_ok: false,
            };
        }
    };
    let mut parquet = crate::utils::AsyncParquetOutput::with_settings(
        writer,
        if output_config.versions {
            crate::utils::Layout::ListVersions
        } else {
            crate::utils::Layout::List
        },
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
        index,
        stats,
        parquet_rows,
        output_ok,
    }
}

/// Stream a list run into Parquet parts `first_index..` of `sink`
/// (uncommitted until the run commits them).
pub async fn data_map_task_list_streaming(
    mut ctx: DataMapContext,
    sink: PartSink,
    first_index: usize,
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
            first_index + index,
            sink.clone(),
            output_config.clone(),
            ctx.g_state.clone(),
            Arc::clone(&busy_nanos),
        ));
        senders.push(w_tx);
        handles.push(handle);
    }

    let started_at = Instant::now();
    let write_started_at = Instant::now();
    let mut last_ts = core::epoch_secs();
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
                            first_index + index,
                            sink.clone(),
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
                started_at,
                write_started_at,
            )
            .await;
            ctx.complete();
            ctx.quit();
            return;
        }

        let now = core::epoch_secs();
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
/// finalize their own Parquet outputs), await all workers, merge their results
/// and record run metrics.
async fn coordinator_finalize(
    senders: Vec<tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>>,
    handles: Vec<tokio::task::JoinHandle<ListWorkerResult>>,
    merged_prefix_stats: PrefixStats,
    g_state: &core::GlobalState,
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
    let mut parts: Vec<core::OutputPart> = Vec::with_capacity(handles.len());

    for handle in handles {
        match handle.await {
            Ok(result) => {
                parts.push(core::OutputPart {
                    index: result.index,
                    rows: result.parquet_rows,
                });
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

    if !output_ok {
        g_state.inc_output_error();
    }

    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    let write_elapsed = write_started_at.elapsed().as_secs_f64();
    parts.sort_by_key(|part| part.index);
    g_state.record_data(list_summary(
        &merged_prefix_stats,
        &merged_stats,
        parquet_rows,
        parts,
    ));
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
        "Data Map Task — list streaming output metrics: Parquet rows {}, write elapsed {:.3}s",
        parquet_rows, write_elapsed,
    );
}

pub async fn data_map_task_list_summary_only(mut ctx: DataMapContext) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — list summary-only started");

    let mut prefix_stats = PrefixStats::default();
    let started_at = Instant::now();
    let mut last_ts = core::epoch_secs();
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

        let now = core::epoch_secs();
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

/// Stream a list run as NDJSON rows on stdout.
pub async fn data_map_task_list_stdout(ctx: DataMapContext) {
    let stdout = tokio::io::stdout();
    let writer = tokio::io::BufWriter::new(stdout);
    data_map_task_list_text_writer(ctx, writer).await;
}

/// Stream a list run as NDJSON rows into `writer`.
pub async fn data_map_task_list_text_writer<W>(mut ctx: DataMapContext, mut writer: W)
where
    W: tokio::io::AsyncWrite + Unpin + Send,
{
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — list stdout started");

    let mut prefix_stats = PrefixStats::default();
    let started_at = Instant::now();
    let mut last_ts = core::epoch_secs();
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
                if !ingest_list_stdout_batch(&mut writer, &mut prefix_stats, &mut stats, batch)
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
                if !ingest_list_stdout_batch(&mut writer, &mut prefix_stats, &mut stats, batch)
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
                if !ingest_list_stdout_batch(&mut writer, &mut prefix_stats, &mut stats, batch)
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

        let now = core::epoch_secs();
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

async fn ingest_list_streaming_batch<W: parquet::arrow::async_writer::AsyncFileWriter>(
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
        .write_list_batch_filtered(batch, |_key, props| {
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

        if let Err(e) = render_text_row(&mut out, key.as_str(), &props) {
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

/// Render one NDJSON list row:
/// `{"key":..,"size":..,"last_modified":"…Z"|null,"etag":..|null,"storage_class":..|null}`
/// — the Parquet list schema's columns. Hand-rolled (no per-row struct
/// serialization) because it runs once per object on the stdout hot path.
fn render_text_row(
    out: &mut Vec<u8>,
    key: &str,
    props: &ObjectProps,
) -> Result<(), serde_json::Error> {
    let mut ints = itoa::Buffer::new();
    // Only the key needs JSON string escaping; the other strings are ASCII
    // without quotes or backslashes (hex ETags, S3 storage class names).
    out.extend_from_slice(b"{\"key\":");
    serde_json::to_writer(&mut *out, key)?;
    out.extend_from_slice(b",\"size\":");
    out.extend_from_slice(ints.format(props.size()).as_bytes());
    out.extend_from_slice(b",\"last_modified\":");
    match props.last_modified_millis() {
        0 => out.extend_from_slice(b"null"),
        ms => {
            out.push(b'"');
            push_rfc3339_millis(out, ms);
            out.push(b'"');
        }
    }
    out.extend_from_slice(b",\"etag\":");
    let mut buf = [0u8; 43];
    match props.etag_text(&mut buf) {
        None => out.extend_from_slice(b"null"),
        // Hex is plain ASCII; another vendor's ETag may need escaping.
        Some(etag) if props.etag_other().is_some() => serde_json::to_writer(&mut *out, etag)?,
        Some(etag) => {
            out.push(b'"');
            out.extend_from_slice(etag.as_bytes());
            out.push(b'"');
        }
    }
    out.extend_from_slice(b",\"storage_class\":");
    match props.storage_class_name() {
        None => out.extend_from_slice(b"null"),
        Some(name) => serde_json::to_writer(&mut *out, name)?,
    }
    if props.is_versioned() {
        out.extend_from_slice(b",\"version_id\":");
        match props.version_id() {
            None => out.extend_from_slice(b"null"),
            Some(id) => serde_json::to_writer(&mut *out, id)?,
        }
        out.extend_from_slice(if props.is_latest() {
            b",\"is_latest\":true"
        } else {
            b",\"is_latest\":false"
        });
        out.extend_from_slice(if props.is_delete_marker() {
            b",\"is_delete_marker\":true"
        } else {
            b",\"is_delete_marker\":false"
        });
    }
    out.extend_from_slice(b"}\n");
    Ok(())
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ` of epoch milliseconds (civil-from-days,
/// Howard Hinnant's algorithm), without chrono's formatting machinery.
fn push_rfc3339_millis(out: &mut Vec<u8>, ms: u64) {
    let secs = ms / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let two = |v: u64, out: &mut Vec<u8>| {
        out.push(b'0' + (v / 10) as u8);
        out.push(b'0' + (v % 10) as u8);
    };
    let year = year.clamp(0, 9999) as u64;
    two(year / 100, out);
    two(year % 100, out);
    out.push(b'-');
    two(month as u64, out);
    out.push(b'-');
    two(day as u64, out);
    out.push(b'T');
    two(rem / 3600, out);
    out.push(b':');
    two(rem / 60 % 60, out);
    out.push(b':');
    two(rem % 60, out);
    out.push(b'.');
    let frac = ms % 1000;
    out.push(b'0' + (frac / 100) as u8);
    two(frac % 100, out);
    out.push(b'Z');
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

/// The data summary of a list run.
fn list_summary(
    prefix_stats: &PrefixStats,
    stats: &ListStreamingStats,
    rows_written: usize,
    parts: Vec<core::OutputPart>,
) -> core::DataSummary {
    let objects: usize = prefix_stats.values().map(|a| a.objects).sum();
    core::DataSummary {
        objects,
        folders: stats.streamed_rows.saturating_sub(objects),
        bytes: stats.bytes_total,
        rows_written,
        top_prefixes: top_prefixes(prefix_stats, TOP_PREFIXES),
        parts,
        diff: None,
    }
}

/// How many prefixes the result lists under `top_prefixes`.
const TOP_PREFIXES: usize = 10;

fn record_list_stdout_metrics(
    g_state: &core::GlobalState,
    prefix_stats: &PrefixStats,
    stats: &ListStreamingStats,
) {
    g_state.record_data(list_summary(
        prefix_stats,
        stats,
        stats.streamed_rows,
        Vec::new(),
    ));
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
    g_state.record_data(list_summary(prefix_stats, &stats, 0, Vec::new()));
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

// ── Streaming diff (merge-join) ────────────────────────────
//
// Each side is listed as a fixed set of key-ordered segments, so its batches
// arrive in strict ascending key order.  Merging the two ordered streams
// classifies every key on the fly — left-only (Plus), right-only (Minus),
// or present on both sides (Equal/Astrisk via ObjectProps::classify_pair)
// — and streams rows straight to Parquet.  Memory is bounded by the
// channel buffers instead of the combined object count.

/// The two diff sides: each side's segments in key order (see
/// `segment_order`), read one after another as one ascending stream while the
/// segments themselves list in parallel, each into a small bounded channel
/// (the prefetch window).
pub struct DiffStreamSides {
    pub left: Arc<SegmentOrder>,
    pub right: Arc<SegmentOrder>,
    /// Each side's listing prefix: keys are compared, and written, relative
    /// to it, so `s3://a/x/` diffs against `s3://b/y/` key by key.
    pub left_prefix: String,
    pub right_prefix: String,
}

impl DiffStreamSides {
    /// Sides over fixed receivers, in order (tests, benchmarks).
    pub fn from_receivers(
        left: Vec<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
        right: Vec<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
    ) -> Self {
        Self {
            left: Arc::new(SegmentOrder::from_receivers(left)),
            right: Arc::new(SegmentOrder::from_receivers(right)),
            left_prefix: String::new(),
            right_prefix: String::new(),
        }
    }
}

/// Where the merge publishes the static segment it is reading on each side
/// (for a split child, the static segment it descends from), so the listing
/// can hold back segments too far ahead of it.
pub struct DiffMergeHeads {
    pub left: tokio::sync::watch::Sender<usize>,
    pub right: tokio::sync::watch::Sender<usize>,
}

struct DiffSideStream {
    order: Arc<SegmentOrder>,
    /// The segment being read (`None`: the side is exhausted).
    current: Option<usize>,
    rx: Option<tokio::sync::mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>>,
    buf: std::collections::VecDeque<(ObjectKey, ObjectProps)>,
    closed: bool,
    last_key: Option<String>,
    name: &'static str,
    received_batches: usize,
    received_objects: usize,
    head: Option<tokio::sync::watch::Sender<usize>>,
}

impl DiffSideStream {
    fn new(
        order: Arc<SegmentOrder>,
        name: &'static str,
        head: Option<tokio::sync::watch::Sender<usize>>,
    ) -> Self {
        let current = order.first();
        let rx = current.and_then(|id| order.take_receiver(id));
        Self {
            order,
            current,
            rx,
            buf: std::collections::VecDeque::new(),
            closed: false,
            last_key: None,
            name,
            received_batches: 0,
            received_objects: 0,
            head,
        }
    }

    /// Receive the next batch, moving to the next segment in key order as
    /// each segment's channel closes.
    async fn recv_next(&mut self) -> Option<Vec<(ObjectKey, ObjectProps)>> {
        loop {
            let id = self.current?;
            if let Some(rx) = self.rx.as_mut()
                && let Some(batch) = rx.recv().await
            {
                return Some(batch);
            }
            self.current = self.order.successor(id).await;
            self.rx = self.current.and_then(|next| self.order.take_receiver(next));
            if let (Some(head), Some(next)) = (&self.head, self.current) {
                head.send_replace(self.order.root(next));
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
                                    "{} listing returned keys {} ('{}' after '{}')",
                                    self.name, DIFF_ORDER_VIOLATION, key, prev
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

/// Buffered streaming row sink: differing rows accumulate in merge (key)
/// order and flush as Parquet batches, so the file stays in key order.
struct DiffRowSink {
    buf: Vec<DiffRow>,
    plus: usize,
    minus: usize,
    astrisk: usize,
    equal: usize,
}

const DIFF_SINK_FLUSH_ROWS: usize = 8192;

/// Marks a merge error caused by a side's keys arriving out of order.
const DIFF_ORDER_VIOLATION: &str = "out of order";

/// Batches queued between the merge task and the Parquet writer loop — the
/// pipeline depth that lets classification run ahead of encode+compress.
const DIFF_WRITE_PIPELINE_CAP: usize = 4;

/// A flushed row batch on its way to the Parquet writer loop.
type DiffWriteBatch = Vec<DiffRow>;

impl DiffRowSink {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            plus: 0,
            minus: 0,
            astrisk: 0,
            equal: 0,
        }
    }

    /// Count an equal key; it is never written.
    fn push_equal(&mut self) {
        self.equal += 1;
    }

    /// Count a differing key and queue its row.
    async fn push(
        &mut self,
        writer_tx: &tokio::sync::mpsc::Sender<DiffWriteBatch>,
        row: DiffRow,
    ) -> Result<(), String> {
        match row.status {
            DiffStatus::LeftOnly => self.plus += 1,
            DiffStatus::RightOnly => self.minus += 1,
            DiffStatus::Changed => self.astrisk += 1,
        }
        // Rows always go on to the writer stage, which verifies checksums
        // (`--compare checksum`), keeps the statistics and writes them.
        self.buf.push(row);
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

/// `key` without its side's listing prefix (every listed key starts with it).
fn relative(key: &str, strip: usize) -> &str {
    key.get(strip..).unwrap_or(key)
}

fn relative_key(key: ObjectKey, strip: usize) -> ObjectKey {
    if strip == 0 {
        key
    } else {
        ObjectKey::from(relative(key.as_str(), strip).to_string())
    }
}

/// Merge the two ordered diff streams into the sink, handing flushed row
/// batches to the Parquet writer loop. Returns Err on ordering violations
/// or when the writer stopped.
async fn merge_diff_streams(
    left: &mut DiffSideStream,
    right: &mut DiffSideStream,
    // Bytes of each side's listing prefix (keys are compared without it).
    strip: (usize, usize),
    compare: core::CompareMode,
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
                relative(lk.as_str(), strip.0).cmp(relative(rk.as_str(), strip.1))
            }
        };

        match order {
            std::cmp::Ordering::Less => {
                let (key, props) = left.buf.pop_front().expect("filled");
                let key = relative_key(key, strip.0);
                let row = DiffRow {
                    key,
                    status: DiffStatus::LeftOnly,
                    left: Some(props),
                    right: None,
                };
                sink.push(writer_tx, row).await?;
            }
            std::cmp::Ordering::Greater => {
                let (key, props) = right.buf.pop_front().expect("filled");
                let key = relative_key(key, strip.1);
                let row = DiffRow {
                    key,
                    status: DiffStatus::RightOnly,
                    left: None,
                    right: Some(props),
                };
                sink.push(writer_tx, row).await?;
            }
            std::cmp::Ordering::Equal => {
                let (key, left_props) = left.buf.pop_front().expect("filled");
                let key = relative_key(key, strip.0);
                let (_rkey, right_props) = right.buf.pop_front().expect("filled");
                match core::ObjectProps::classify_pair_with(compare, &left_props, &right_props) {
                    MatchResult::Astrisk => {
                        let row = DiffRow {
                            key,
                            status: DiffStatus::Changed,
                            left: Some(left_props),
                            right: Some(right_props),
                        };
                        sink.push(writer_tx, row).await?;
                    }
                    _ => sink.push_equal(),
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
    /// Changed pairs proven equal by their checksums (counted in `equal`).
    pub verified_equal: usize,
    /// Changed pairs the checksums could not settle.
    pub unverified: usize,
    pub received_batches: usize,
    pub received_objects: usize,
    pub bytes_total: u64,
    prefix_stats: PrefixStats,
}

impl DiffMergeOutcome {
    pub fn unique_prefixes(&self) -> usize {
        self.prefix_stats.len()
    }
}

/// Merge two ordered diff streams into the Parquet writer (`None`: count
/// only) and return the classification counters. Only differing keys are
/// written. The caller owns the writer's lifecycle (close).
///
/// The merge runs as its own task while this task drives the Parquet writes,
/// so classification/ingest overlaps encode+compress instead of serializing
/// with it in one loop; the bounded channel between them is the pipeline
/// depth. The `&mut parquet` contract is unchanged — all rows are written
/// before this returns.
pub async fn run_diff_merge<W: parquet::arrow::async_writer::AsyncFileWriter>(
    sides: DiffStreamSides,
    parquet: Option<&mut crate::utils::AsyncParquetOutput<W>>,
    aborted: impl Fn() -> bool + Send + Sync + 'static,
) -> Result<DiffMergeOutcome, String> {
    run_diff_merge_with(sides, None, parquet, DiffOptions::default(), aborted).await
}

/// Settles a changed pair by its stored checksums, given the key relative
/// to both prefixes: `Some(true)` equal, `Some(false)` different, `None`
/// inconclusive.
pub type ChecksumVerifier =
    Arc<dyn Fn(String) -> futures::future::BoxFuture<'static, Option<bool>> + Send + Sync>;

/// How the diff compares keys present on both sides.
#[derive(Clone, Default)]
pub struct DiffOptions {
    pub compare: core::CompareMode,
    /// Required for `CompareMode::Checksum`.
    pub verifier: Option<ChecksumVerifier>,
}

/// Concurrent checksum verifications per batch.
const CHECKSUM_VERIFY_CONCURRENCY: usize = 32;

/// Drop the changed pairs of `batch` whose checksums prove them equal;
/// returns the kept rows (in order), and the proven-equal and unverified
/// counts. Only pairs of equal size are candidates. Once `aborted` turns
/// true no further verification starts (the run is quitting; at most the
/// requests already in flight finish).
async fn verify_batch(
    batch: DiffWriteBatch,
    verifier: &ChecksumVerifier,
    aborted: &(dyn Fn() -> bool + Send + Sync),
) -> (DiffWriteBatch, usize, usize) {
    use futures::StreamExt;
    let candidates: Vec<usize> = batch
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            row.status == DiffStatus::Changed
                && matches!((&row.left, &row.right), (Some(l), Some(r)) if l.size() == r.size())
        })
        .map(|(i, _)| i)
        .collect();
    if candidates.is_empty() {
        return (batch, 0, 0);
    }
    let verdicts: Vec<(usize, Option<bool>)> = futures::stream::iter(candidates)
        .map(|i| {
            let fut = (!aborted()).then(|| verifier(batch[i].key.as_str().to_string()));
            async move {
                match fut {
                    Some(fut) => (i, fut.await),
                    None => (i, None),
                }
            }
        })
        .buffer_unordered(CHECKSUM_VERIFY_CONCURRENCY)
        .collect()
        .await;
    let mut equal = vec![false; batch.len()];
    let (mut proven, mut unverified) = (0, 0);
    for (i, verdict) in verdicts {
        match verdict {
            Some(true) => {
                equal[i] = true;
                proven += 1;
            }
            Some(false) => {}
            None => unverified += 1,
        }
    }
    let kept = batch
        .into_iter()
        .zip(equal)
        .filter_map(|(row, eq)| (!eq).then_some(row))
        .collect();
    (kept, proven, unverified)
}

/// [`run_diff_merge`] with the merge's segment position published and the
/// compare mode set.
pub async fn run_diff_merge_with<W: parquet::arrow::async_writer::AsyncFileWriter>(
    sides: DiffStreamSides,
    heads: Option<DiffMergeHeads>,
    mut parquet: Option<&mut crate::utils::AsyncParquetOutput<W>>,
    options: DiffOptions,
    aborted: impl Fn() -> bool + Send + Sync + 'static,
) -> Result<DiffMergeOutcome, String> {
    let (left_head, right_head) = match heads {
        Some(heads) => (Some(heads.left), Some(heads.right)),
        None => (None, None),
    };
    let (writer_tx, mut writer_rx) =
        tokio::sync::mpsc::channel::<DiffWriteBatch>(DIFF_WRITE_PIPELINE_CAP);

    let strip = (sides.left_prefix.len(), sides.right_prefix.len());
    let compare = options.compare;
    let aborted: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(aborted);
    let writer_aborted = Arc::clone(&aborted);
    let merge_task = tokio::spawn(async move {
        let mut left = DiffSideStream::new(sides.left, "left", left_head);
        let mut right = DiffSideStream::new(sides.right, "right", right_head);
        let mut sink = DiffRowSink::new();

        merge_diff_streams(
            &mut left, &mut right, strip, compare, &writer_tx, &mut sink, &*aborted,
        )
        .await?;
        sink.flush_all(&writer_tx).await?;
        Ok::<_, String>((
            sink,
            left.received_batches + right.received_batches,
            left.received_objects + right.received_objects,
        ))
    });

    // Writer stage: verifies checksums, keeps the statistics of the rows
    // that remain different, and writes them. Drains until the merge task
    // drops its sender, or a write fails (dropping the receiver unblocks the
    // merge, whose sends then fail fast).
    let mut write_error: Option<String> = None;
    let mut prefix_stats = PrefixStats::default();
    let mut prefix_run = PrefixRunFolder::default();
    let (mut rows, mut verified_equal, mut unverified) = (0usize, 0usize, 0usize);
    while let Some(mut batch) = writer_rx.recv().await {
        if let Some(verifier) = &options.verifier {
            // A quitting run fails anyway: stop verifying (and writing).
            if writer_aborted() {
                write_error = Some("diff aborted".to_string());
                break;
            }
            let (kept, proven, unsettled) = verify_batch(batch, verifier, &*writer_aborted).await;
            batch = kept;
            verified_equal += proven;
            unverified += unsettled;
        }
        for row in &batch {
            let size = row
                .left
                .as_ref()
                .or(row.right.as_ref())
                .map_or(0, |p| p.size());
            prefix_run.add(&mut prefix_stats, row.key.prefix(), size);
        }
        rows += batch.len();
        let Some(parquet) = parquet.as_deref_mut() else {
            continue;
        };
        if let Err(e) = parquet.write_diff_rows(batch).await {
            write_error = Some(e);
            break;
        }
    }
    drop(writer_rx);
    prefix_run.flush(&mut prefix_stats);

    let merged = merge_task
        .await
        .map_err(|e| format!("diff merge task failed: {}", e))?;
    if let Some(e) = write_error {
        // The write error is the root cause; the merge error it induced
        // ("writer stopped") is secondary.
        return Err(e);
    }
    let (sink, received_batches, received_objects) = merged?;
    let bytes_total = prefix_stats
        .values()
        .map(|aggregate| aggregate.bytes)
        .fold(0u64, u64::saturating_add);
    Ok(DiffMergeOutcome {
        rows,
        plus: sink.plus,
        minus: sink.minus,
        astrisk: sink.astrisk - verified_equal,
        equal: sink.equal + verified_equal,
        verified_equal,
        unverified,
        received_batches,
        received_objects,
        bytes_total,
        prefix_stats,
    })
}

/// The diff data map: merge both sides and write the differing keys to
/// Parquet part 0 of `sink` (uncommitted until the run commits it), or only
/// count them when `sink` is `None`.
pub async fn data_map_task_diff_streaming(
    g_state: core::GlobalState,
    sides: DiffStreamSides,
    heads: Option<DiffMergeHeads>,
    sink: Option<PartSink>,
    output_config: OutputConfig,
    options: DiffOptions,
) {
    g_state.data_map_task_start();
    g_state.wait_to_start().await;

    info!("Data Map Task — diff streaming started");

    let mut parquet = match &sink {
        None => None,
        Some(sink) => match sink.open(0).await {
            Ok(writer) => Some(crate::utils::AsyncParquetOutput::with_settings(
                writer,
                crate::utils::Layout::Diff,
                output_config.row_group_size,
                &output_config.compression,
                output_config.compression_level,
            )),
            Err(e) => {
                g_state.note_output_error(e);
                g_state.inc_output_error();
                g_state.data_map_task_complete();
                g_state.quit();
                return;
            }
        },
    };

    let started_at = Instant::now();
    let mut output_ok = true;
    let abort_state = g_state.clone();
    let outcome = match run_diff_merge_with(sides, heads, parquet.as_mut(), options, move || {
        abort_state.is_quit()
    })
    .await
    {
        Ok(outcome) => Some(outcome),
        Err(e) => {
            log::debug!("Diff merge failed: {}", e);
            // A merge stopped because the run is quitting (a side failed
            // or the run was interrupted) is explained by that error. An
            // ordering violation is the endpoint breaking the listing
            // contract; anything else (a write error) is an output failure.
            if e.contains(DIFF_ORDER_VIOLATION) {
                g_state.record_fatal_error(crate::error::ERROR_INCONSISTENT_LISTING, e);
                g_state.quit();
            } else if !g_state.is_quit() {
                g_state.note_output_error(format!("diff merge failed: {}", e));
                output_ok = false;
            }
            None
        }
    };

    let parquet_rows = parquet.as_ref().map_or(0, |p| p.total_rows());
    if let Some(parquet) = parquet
        && let Err(e) = parquet.close().await
    {
        g_state.note_output_error(format!("diff output: {}", e));
        output_ok = false;
    }
    if !output_ok {
        g_state.inc_output_error();
    } else if outcome.is_some() {
        g_state.set_diff_merge_complete();
    }
    if outcome.is_none()
        && let Some(sink) = &sink
    {
        // A partial merge describes part of one side against an unfinished
        // other side; it parses, which is what makes it dangerous.
        sink.discard(0).await;
    }

    if let Some(outcome) = outcome {
        let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
        g_state.record_data(core::DataSummary {
            objects: outcome.plus + outcome.minus + outcome.astrisk + outcome.equal,
            folders: 0,
            bytes: outcome.bytes_total,
            rows_written: parquet_rows,
            top_prefixes: top_prefixes(&outcome.prefix_stats, TOP_PREFIXES),
            parts: if sink.is_some() {
                vec![core::OutputPart {
                    index: 0,
                    rows: parquet_rows,
                }]
            } else {
                Vec::new()
            },
            diff: Some(core::DiffCounts {
                left_only: outcome.plus,
                right_only: outcome.minus,
                changed: outcome.astrisk,
                equal: outcome.equal,
                verified_equal: outcome.verified_equal,
                unverified: outcome.unverified,
            }),
        });
        info!(
            "Data Map Task — diff streaming complete: +{} -{} *{} ={}, received objects {}, elapsed {:.3}s, {:.0} objects/sec",
            outcome.plus,
            outcome.minus,
            outcome.astrisk,
            outcome.equal,
            outcome.received_objects,
            elapsed,
            outcome.received_objects as f64 / elapsed,
        );
    }

    g_state.data_map_task_complete();
    g_state.quit();
}

#[cfg(test)]
mod verify_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn changed(key: &str) -> DiffRow {
        let props = ObjectProps::new_open(0, 7, [1; 16]);
        DiffRow {
            key: ObjectKey::from(key),
            status: DiffStatus::Changed,
            left: Some(props.clone()),
            right: Some(props),
        }
    }

    fn counting_verifier(calls: Arc<AtomicUsize>) -> ChecksumVerifier {
        Arc::new(move |_key: String| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Some(true) })
        })
    }

    #[tokio::test]
    async fn verify_batch_proves_equal_pairs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let verifier = counting_verifier(Arc::clone(&calls));
        let (kept, proven, unverified) =
            verify_batch(vec![changed("a"), changed("b")], &verifier, &|| false).await;
        assert!(kept.is_empty());
        assert_eq!((proven, unverified), (2, 0));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn verify_batch_starts_nothing_once_aborted() {
        let calls = Arc::new(AtomicUsize::new(0));
        let verifier = counting_verifier(Arc::clone(&calls));
        let (kept, proven, _) =
            verify_batch(vec![changed("a"), changed("b")], &verifier, &|| true).await;
        assert_eq!(kept.len(), 2);
        assert_eq!(proven, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod tests {
    use super::{OUTPUT_GROW_MIN_BATCHES, output_should_grow, part_file_name, render_text_row};

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
    fn text_rows_match_serde_renderings_byte_for_byte() {
        #[derive(serde::Serialize)]
        struct Row<'a> {
            key: &'a str,
            size: u64,
            last_modified: Option<String>,
            etag: Option<String>,
            storage_class: Option<&'a str>,
        }
        let dir = crate::core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE;
        for key in AWKWARD_KEYS {
            for (size, ms, class) in [
                (0, 0, ""),
                (1, 1_767_225_600_123, "STANDARD"),
                (u64::MAX, 253_402_300_799_999, "GLACIER"),
            ] {
                let mut props = crate::core::ObjectProps::new_open(dir, size, [0xab; 16]);
                props.set_last_modified_millis(ms);
                props.set_storage_class(class);
                let mut ndjson = Vec::new();
                render_text_row(&mut ndjson, key, &props).unwrap();
                let mut want = serde_json::to_vec(&Row {
                    key,
                    size,
                    last_modified: (ms != 0).then(|| {
                        chrono::DateTime::from_timestamp_millis(ms as i64)
                            .unwrap()
                            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                            .to_string()
                    }),
                    etag: Some("ab".repeat(16)),
                    storage_class: (!class.is_empty()).then_some(class),
                })
                .unwrap();
                want.push(b'\n');
                assert_eq!(
                    String::from_utf8_lossy(&ndjson),
                    String::from_utf8_lossy(&want),
                    "ndjson row for {key:?}"
                );
            }
        }
    }

    #[test]
    fn rfc3339_formatting_matches_chrono_across_the_calendar() {
        let mut ms: u64 = 0;
        while ms < 4_102_444_800_000 {
            let mut out = Vec::new();
            super::push_rfc3339_millis(&mut out, ms);
            let want = chrono::DateTime::from_timestamp_millis(ms as i64)
                .unwrap()
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string();
            assert_eq!(String::from_utf8(out).unwrap(), want);
            ms += 86_399_997; // walks every day boundary and leap year
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
    fn part_names_are_zero_padded_and_temp_paths_hidden() {
        assert_eq!(part_file_name(0), "part-00000.parquet");
        assert_eq!(part_file_name(123), "part-00123.parquet");
        assert_eq!(
            super::tmp_part_path(std::path::Path::new("out"), 7),
            std::path::Path::new("out/.part-00007.parquet.tmp")
        );
    }
}
