//! The reactor: runs a side's segments under the concurrency limit, fans
//! out with runtime splits, and (list mode) records the resume ranges on
//! exit. A diff side runs the same reactor with an ordered output: every
//! segment writes to its own channel, linked into the side's
//! `SegmentOrder` so the merge reads the side in key order.

use super::chain::flat_list_run_to_complete;
use super::split::{
    FanOutGovernor, FlatHigh, SPLIT_CHECK_INTERVAL_MS, SegmentControl, SplitRange,
    maybe_start_split_probes,
};
use super::{JoinFailure, classify_join_failure};
use crate::core;
use crate::core::{KeySpaceHints, S3TaskContext};
use crate::segment_order::{SealOnDrop, SegmentOrder};
use log::{error, info};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentOutcome {
    index: usize,
    completed: bool,
    /// An original hint segment (counted in the heartbeat's done/remaining),
    /// not a runtime-split child.
    from_hints: bool,
}

// ── Public entry point ─────────────────────────────────────

pub async fn flat_list_main_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    flat_concurrency: usize,
    hints: KeySpaceHints,
) {
    flat_reactor_task(ctx, start_prefix, flat_concurrency, hints, None, None).await
}

/// Upper bound on concurrently listing segments per diff side.
pub const DIFF_SIDE_MAX_CONCURRENCY: usize = 32;
/// Static segments a diff side may start ahead of the one the merge is
/// reading. With the channel capacity this bounds a side's buffered batches
/// whatever the segment count.
const DIFF_SIDE_LOOKAHEAD_SEGMENTS: usize = 16;

/// List one diff side: its static segments (`hints`), split at runtime like
/// list mode, each segment writing to its channel in `order`. Any segment
/// failure fails the run.
pub async fn diff_side_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    concurrency: usize,
    hints: KeySpaceHints,
    order: Arc<SegmentOrder>,
    merge_head: Option<tokio::sync::watch::Receiver<usize>>,
) {
    flat_reactor_task(
        ctx,
        start_prefix,
        concurrency.clamp(1, DIFF_SIDE_MAX_CONCURRENCY),
        hints,
        Some(order),
        merge_head,
    )
    .await
}

// ── Reactor: controls concurrency via JoinSet ──────────────

async fn flat_reactor_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    flat_concurrency: usize,
    mut hints: KeySpaceHints,
    order: Option<Arc<SegmentOrder>>,
    mut merge_head: Option<tokio::sync::watch::Receiver<usize>>,
) {
    // However the reactor ends, a merge waiting on this side is released.
    let _seal = order.as_deref().map(SealOnDrop);
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Flat List S3 Task — {} — started", ctx.s3_bucket_name);
    tokio::task::yield_now().await;

    // A --delimiter run is never split: a page's CommonPrefixes are not
    // range-bounded, so a split parent would keep paging past its cut,
    // re-listing the child's prefixes.
    let allow_split = ctx.delimiter.as_deref().unwrap_or("").is_empty();
    // A static segment held back until the merge catches up (diff).
    let mut held: Option<core::KeySpacePair> = None;

    let (split_tx, mut split_rx) = tokio::sync::mpsc::unbounded_channel::<SplitRange>();
    let mut set = tokio::task::JoinSet::new();
    let mut controls: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
    // Where each in-flight segment started, for the resume ranges below.
    let mut starts: HashMap<usize, String> = HashMap::new();
    let mut unfinished: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
    let mut completed_pieces = 0usize;
    // Split children not yet started, with the index each was given when it
    // was received (and, for a diff side, linked after its parent).
    let mut pending_children: Vec<(usize, SplitRange)> = Vec::new();
    // Children need indices no original segment uses: on a resume the set is
    // sparse, and a reused index would let a child's control replace its
    // namesake's in `controls` — then the split parent completes looking
    // unsplit and gets checkpointed while the child's range is still unlisted.
    let mut next_child_index = hints.index_end();
    let mut link_child = |range: SplitRange, pending: &mut Vec<(usize, SplitRange)>| {
        let index = next_child_index;
        next_child_index += 1;
        if let Some(order) = &order {
            order.insert_after(range.parent, index);
        }
        pending.push((index, range));
    };
    let mut split_count = 0usize;
    let flat_high: FlatHigh = Arc::new(Mutex::new(None));
    let mut retired_pages = 0u64;
    let mut gov = FanOutGovernor::new(flat_concurrency);
    let mut last_ts = core::epoch_secs();
    // A persistent interval — unlike a fresh sleep per loop iteration, it
    // still fires when join/split events keep the select! busy.
    // In-flight split probes. Each holds a context clone — and with it a
    // data-map sender — so a detached probe kept the output open, delaying
    // the end of the run (and Ctrl-C) by up to its request timeouts. They
    // are aborted when the reactor exits.
    let mut probes = tokio::task::JoinSet::new();
    let mut remaining_held: Vec<core::KeySpacePair> = Vec::new();
    let mut split_check = tokio::time::interval(Duration::from_millis(SPLIT_CHECK_INTERVAL_MS));
    split_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Fill up to the concurrency limit: split children first, then hints.
        // Nothing new starts once the run is asked to stop — an interrupt
        // during startup discovery used to be followed by a full first fill.
        while set.len() < flat_concurrency && !ctx.is_quit() {
            let (index, start, end, from_hints) =
                if let Some((index, child)) = pending_children.pop() {
                    (index, child.start, child.end, false)
                } else if let Some(pair) = held.take().or_else(|| hints.next()) {
                    if let Some(head) = &merge_head
                        && pair.index >= *head.borrow() + DIFF_SIDE_LOOKAHEAD_SEGMENTS
                    {
                        held = Some(pair);
                        break;
                    }
                    (pair.index, pair.start, pair.end, true)
                } else {
                    break;
                };

            let mut task_ctx = ctx.clone();
            if let Some(order) = &order {
                match order.take_sender(index) {
                    Some(sender) => task_ctx.data_map_channel = sender,
                    None => {
                        error!("diff segment {} has no channel", index);
                        ctx.g_state.inc_fatal_error();
                        ctx.g_state.quit();
                        break;
                    }
                }
            }
            let control = Arc::new(if from_hints {
                SegmentControl::new(end)
            } else {
                SegmentControl::new_child(end)
            });
            controls.insert(index, Arc::clone(&control));
            starts.insert(index, start.clone());
            let start_prefix = start_prefix.to_string();
            let task_split_tx = allow_split.then(|| split_tx.clone());

            set.spawn(async move {
                let completed = flat_list_run_to_complete(
                    &task_ctx,
                    index,
                    &start_prefix,
                    &start,
                    &control,
                    task_split_tx.as_ref(),
                )
                .await;
                SegmentOutcome {
                    index,
                    completed,
                    from_hints,
                }
            });
        }

        while probes.try_join_next().is_some() {}

        // A segment that accepted a split sent its child range before it
        // finished; when `join_next` won the race below, that child is still
        // queued here. Leaving it there dropped its keys from the listing —
        // and from the resume ranges — while the run reported success.
        let mut drained = false;
        while let Ok(range) = split_rx.try_recv() {
            split_count += 1;
            link_child(range, &mut pending_children);
            drained = true;
        }
        if drained && !ctx.is_quit() {
            continue;
        }

        if set.is_empty() && pending_children.is_empty() && held.is_none() {
            // Every segment task has joined, so every cloned sender is dropped
            // and every `send` has completed — the batches are in the channel.
            // Returning drops the last sender, and the data map receives all of
            // them before `recv` reports the close, so there is nothing to wait
            // for here. (This used to sleep a second before returning, which
            // only delayed that close: the listing was over, the data map was
            // parked, and the run paid the second at the very end.)
            ctx.complete();
            info!("Flat List S3 Task — {} — completed", ctx.s3_bucket_name);
            break;
        }

        tokio::select! {
            joined = set.join_next() => match joined {
                Some(Ok(outcome)) => {
                    let control = controls.remove(&outcome.index);
                    retired_pages += control
                        .as_ref()
                        .map_or(0, |c| c.pages.load(Ordering::Relaxed) as u64);
                    if !outcome.completed {
                        // An unfinished segment's range is not done; keep its
                        // control for the resume ranges (not in `controls`,
                        // which the split prober and governor treat as live).
                        if let Some(control) = control.clone() {
                            unfinished.insert(outcome.index, control);
                        }
                    } else {
                        completed_pieces += 1;
                        starts.remove(&outcome.index);
                    }
                    if outcome.completed && outcome.from_hints {
                        hints.finish(outcome.index);
                    }
                    // Every split this segment sent is queued by now: link
                    // them, then let the merge move past it.
                    if let Some(order) = &order {
                        while let Ok(range) = split_rx.try_recv() {
                            split_count += 1;
                            link_child(range, &mut pending_children);
                        }
                        order.seal(outcome.index);
                    }
                }
                Some(Err(e)) => {
                    match classify_join_failure(e.is_cancelled(), ctx.is_quit()) {
                        JoinFailure::ShutdownCancel => {
                            info!("Segment task cancelled during shutdown");
                        }
                        JoinFailure::LostSegment => {
                            error!(
                                "Flat List S3 Task — {} — segment task failed to join, its key range is missing from the output: {:?}",
                                ctx.s3_bucket_name, e
                            );
                            ctx.g_state.inc_fatal_error();
                            ctx.g_state.quit();
                        }
                    }
                }
                None => {}
            },
            child = split_rx.recv() => {
                if let Some(range) = child {
                    split_count += 1;
                    info!(
                        "Segment split: new child segment from '{}' to '{}'",
                        range.start,
                        range.end.as_deref().unwrap_or("<end>"),
                    );
                    link_child(range, &mut pending_children);
                }
            },
            changed = async {
                match merge_head.as_mut() {
                    Some(head) => head.changed().await,
                    None => std::future::pending().await,
                }
            }, if held.is_some() => {
                if changed.is_err() {
                    // The merge is gone; stop gating (the sends will fail).
                    merge_head = None;
                }
            },
            _ = split_check.tick() => {
                let now = core::epoch_secs();
                if now - last_ts >= core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
                    info!(
                        "Flat List S3 Task — {} — heartbeat, {} segments in-flight, {} done, {} remaining, {} runtime splits",
                        ctx.s3_bucket_name,
                        set.len(),
                        hints.done_count(),
                        hints.len(),
                        split_count,
                    );
                    last_ts = now;
                }

                // Track whether added concurrency is still buying throughput;
                // the governor caps fan-out at the saturating level so a single
                // QPS-limited bucket is not oversubscribed.
                gov.observe(retired_pages, &controls, set.len(), flat_concurrency);

                // Idle capacity and nothing queued: probe the longest-running
                // candidates for structural split points, up to the governor's
                // proven-useful concurrency cap, in one pass instead of one
                // segment per tick.
                let cap = gov.effective_cap();
                if allow_split
                    && set.len() < cap
                    && pending_children.is_empty()
                    && held.is_none()
                    && hints.is_empty()
                {
                    let idle = cap - set.len();
                    maybe_start_split_probes(
                        ctx,
                        start_prefix,
                        &controls,
                        idle,
                        &flat_high,
                        &mut probes,
                    );
                }
            },
        }

        // Handle global quit.
        if ctx.is_quit() {
            set.abort_all();
            // Collect the aborted tasks so every segment's sent cursor is
            // final before the resume ranges are computed; one that finished
            // in the meantime has nothing left to list.
            while let Some(joined) = set.join_next().await {
                if let Ok(outcome) = joined
                    && outcome.completed
                {
                    controls.remove(&outcome.index);
                    starts.remove(&outcome.index);
                    completed_pieces += 1;
                }
            }
            // Children split off before or during the abort are unlisted
            // ranges: they belong in the resume ranges.
            while let Ok(range) = split_rx.try_recv() {
                link_child(range, &mut pending_children);
            }
            if let Some(pair) = held.take() {
                remaining_held.push(pair);
            }
            info!("Flat List S3 Task — {} — aborted", ctx.s3_bucket_name);
            break;
        }
    }

    probes.abort_all();
    controls.extend(unfinished);
    let mut unstarted: Vec<SplitRange> = pending_children.into_iter().map(|(_, r)| r).collect();
    unstarted.extend(remaining_held.into_iter().map(|pair| SplitRange {
        start: pair.start,
        end: pair.end,
        parent: pair.index,
    }));
    *ctx.resume_progress.lock().unwrap() = Some(resume_progress(
        &controls,
        &starts,
        unstarted,
        &mut hints,
        completed_pieces,
    ));
    info!("Flat List S3 Task — {} — quit", ctx.s3_bucket_name);
}

/// The key ranges an exiting reactor leaves unwritten: in-flight or
/// unfinished segments from their last sent key, split children not yet
/// started, and hint segments never started. Empty when the listing finished.
fn resume_progress(
    controls: &HashMap<usize, Arc<SegmentControl>>,
    starts: &HashMap<usize, String>,
    pending_children: Vec<SplitRange>,
    hints: &mut core::KeySpaceHints,
    completed_pieces: usize,
) -> crate::checkpoint::ResumeProgress {
    use crate::checkpoint::ResumeRange;
    let mut remaining = Vec::new();
    let mut ranges_with_progress = completed_pieces;
    let mut indices: Vec<usize> = controls.keys().copied().collect();
    indices.sort_unstable();
    for index in indices {
        let control = &controls[&index];
        let sent = control.sent.lock().unwrap().clone();
        if sent.is_some() {
            ranges_with_progress += 1;
        }
        let start_after = sent.unwrap_or_else(|| starts.get(&index).cloned().unwrap_or_default());
        let end = control.current_end();
        // A segment whose last sent key reached its end has nothing left.
        if end
            .as_deref()
            .is_some_and(|end| start_after.as_str() >= end)
        {
            continue;
        }
        remaining.push(ResumeRange { start_after, end });
    }
    for child in pending_children {
        remaining.push(ResumeRange {
            start_after: child.start,
            end: child.end,
        });
    }
    while let Some(pair) = hints.next() {
        remaining.push(ResumeRange {
            start_after: pair.start,
            end: pair.end,
        });
    }
    remaining.sort_by(|a, b| a.start_after.cmp(&b.start_after));
    crate::checkpoint::ResumeProgress {
        remaining,
        ranges_with_progress,
    }
}
