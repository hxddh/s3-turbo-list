//! The reactor: runs a side's segments under the concurrency limit, fans
//! out with runtime splits, and (list mode) records the resume ranges on
//! exit. A diff side runs the same reactor with an ordered output: every
//! segment writes to its own channel, linked into the side's
//! `SegmentOrder` so the merge reads the side in key order.

use super::chain::flat_list_run_to_complete;
use super::split::{
    FlatHigh, SPLIT_CHECK_INTERVAL_MS, SegmentControl, SplitRange, maybe_start_split_probes,
};
use super::{JoinFailure, classify_join_failure};
use crate::core;
use crate::core::{KeySpaceHints, S3TaskContext};
use crate::segment_order::{SealOnDrop, SegmentOrder};
use log::{error, info};
use std::collections::HashMap;
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

/// Startup boundaries still being discovered while the listing runs. Each
/// round of discovery hands what it found so far to `early` (the root
/// probe's CommonPrefixes arrive a round trip in, long before the flat runs
/// under them are cut); `done` resolves to all of them. Every batch becomes
/// cuts of the running segments as it arrives.
pub struct LateBoundaries {
    early: tokio::sync::mpsc::UnboundedReceiver<Vec<String>>,
    done: std::pin::Pin<Box<dyn std::future::Future<Output = Vec<String>> + Send>>,
}

/// Where a discovery hands its batches (see `LateBoundaries`).
pub type EarlyBoundaries = tokio::sync::mpsc::UnboundedSender<Vec<String>>;

/// What a running discovery produced next.
enum Discovered {
    /// A round's boundaries so far.
    Round(Vec<String>),
    /// Every boundary: the discovery is over.
    Done(Vec<String>),
}

impl LateBoundaries {
    /// The next round's boundaries, or all of them once discovery is done.
    async fn next(&mut self) -> Discovered {
        if !self.early.is_closed() || !self.early.is_empty() {
            tokio::select! {
                biased;
                Some(bounds) = self.early.recv() => return Discovered::Round(bounds),
                bounds = &mut self.done => return Discovered::Done(bounds),
            }
        }
        Discovered::Done((&mut self.done).await)
    }

    /// A discovery that sends its batches to the sender it is given and
    /// resolves to every boundary.
    pub fn new<F, Fut>(discovery: F) -> Self
    where
        F: FnOnce(EarlyBoundaries) -> Fut,
        Fut: std::future::Future<Output = Vec<String>> + Send + 'static,
    {
        let (tx, early) = tokio::sync::mpsc::unbounded_channel();
        Self {
            early,
            done: Box::pin(discovery(tx)),
        }
    }
}

pub async fn flat_list_main_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    flat_concurrency: usize,
    hints: KeySpaceHints,
    late: Option<LateBoundaries>,
) {
    flat_reactor_task(ctx, start_prefix, flat_concurrency, hints, None, None, late).await
}

/// Longest runtime splitting waits for startup discovery's boundaries.
const DISCOVERY_SPLIT_GATE: Duration = Duration::from_secs(2);

/// Upper bound on concurrently listing segments per diff side.
pub const DIFF_SIDE_MAX_CONCURRENCY: usize = 64;
/// How far ahead of the merge, in segments of the side's key order, a diff
/// side starts segments: with `DIFF_SEGMENT_CHANNEL_CAP` pages each, it
/// buffers at most 128 x 8 = 1,024 pages (as 32 x 32 did). Segments near
/// the merge are short (cut as it reaches them), so the reach is counted in
/// many of them: 64 left cut ranges waiting while slots sat idle.
const DIFF_SIDE_AHEAD_SEGMENTS: usize = 128;
/// Static segments a diff side may start ahead of the one the merge is
/// reading. With the channel capacity this bounds a side's buffered batches
/// whatever the segment count (32 segments x 32 pages). It used to be half
/// the side's concurrency, so a diff side ran at most 16 segments at once
/// whatever `-c` allowed.
const DIFF_SIDE_LOOKAHEAD_SEGMENTS: usize = DIFF_SIDE_MAX_CONCURRENCY;

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
    late: Option<LateBoundaries>,
) {
    flat_reactor_task(
        ctx,
        start_prefix,
        concurrency.clamp(1, DIFF_SIDE_MAX_CONCURRENCY),
        hints,
        Some(order),
        merge_head,
        late,
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
    mut late: Option<LateBoundaries>,
) {
    // However the reactor ends, a merge waiting on this side is released.
    // Startup discovery still running (`late`) is dropped with the reactor:
    // a listing that finishes first does not wait for it.
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
    let window = &ctx.window;
    // Startup discovery is waited for (no runtime probes cut the same range
    // blind) at most this long; past it, runtime splitting starts anyway.
    let discovery_gate = tokio::time::Instant::now() + DISCOVERY_SPLIT_GATE;
    // Probe requests the window had seen when runtime splitting began: the
    // runtime probe budget counts from there.
    let mut probes_at_start: Option<u64> = None;
    let mut last_ts = core::epoch_secs();
    // A persistent interval — unlike a fresh sleep per loop iteration, it
    // still fires when join/split events keep the select! busy.
    // In-flight split probes. Each holds a context clone — and with it a
    // data-map sender — so a detached probe kept the output open, delaying
    // the end of the run (and Ctrl-C) by up to its request timeouts. They
    // are aborted when the reactor exits.
    let mut probes = tokio::task::JoinSet::new();
    let mut remaining_held: Vec<core::KeySpacePair> = Vec::new();
    // A diff side checks four times as often: the segment the merge waits on
    // is cut as soon as it is reached, and its cuts are read off its own
    // pages, without probes.
    let check_every = if order.is_some() {
        SPLIT_CHECK_INTERVAL_MS / 4
    } else {
        SPLIT_CHECK_INTERVAL_MS
    };
    let mut split_check = tokio::time::interval(Duration::from_millis(check_every));
    split_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Segments that send requests. A diff segment waiting for room in its
    // output channel (ahead of the merge, its buffer full) sends none: it
    // does not count, so the segments the merge waits on are started and
    // cut while those ahead wait. (Counted, they held every slot while the
    // merge's own segment listed one page per round trip: a 5M-key diff
    // took 56 s at 30 ms.) A diff side starts only segments within
    // `DIFF_SIDE_AHEAD_SEGMENTS` of where the merge reads, which bounds what
    // it buffers.
    let in_key_order = order.is_some();
    let busy = |set_len: usize, controls: &HashMap<usize, Arc<SegmentControl>>| {
        if in_key_order {
            set_len
                - controls
                    .values()
                    .filter(|c| c.is_waiting())
                    .count()
                    .min(set_len)
        } else {
            set_len
        }
    };
    let admit = |index: usize| {
        order
            .as_ref()
            .is_none_or(|order| order.within_reach(index, DIFF_SIDE_AHEAD_SEGMENTS))
    };

    loop {
        // Fill up to the concurrency limit: split children first, then hints.
        // Nothing new starts once the run is asked to stop — an interrupt
        // during startup discovery used to be followed by a full first fill.
        while busy(set.len(), &controls) < flat_concurrency && !ctx.is_quit() {
            let (index, start, end, from_hints) = if let Some((index, child)) =
                next_child(&mut pending_children, in_key_order, admit)
            {
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
            let control = Arc::new(
                if from_hints {
                    SegmentControl::new(end)
                } else {
                    SegmentControl::new_child(end)
                }
                .starting_at(&start)
                .handing_off(in_key_order),
            );
            controls.insert(index, Arc::clone(&control));
            starts.insert(index, start.clone());
            ctx.g_state.inc_segments_started();
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
            event = async {
                match late.as_mut() {
                    Some(discovery) => discovery.next().await,
                    None => std::future::pending().await,
                }
            }, if late.is_some() => {
                let bounds = match event {
                    Discovered::Round(bounds) => bounds,
                    Discovered::Done(bounds) => {
                        late = None;
                        bounds
                    }
                };
                for child in hand_over(&bounds, &controls, allow_split) {
                    split_count += 1;
                    link_child(child, &mut pending_children);
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

                // Idle capacity and nothing queued: probe the longest-running
                // candidates for split points, up to the request window (the
                // concurrency the endpoint is currently taking), in one pass.
                let cap = window.limit().min(flat_concurrency);
                let discovering = late.is_some() && tokio::time::Instant::now() < discovery_gate;
                if !discovering && probes_at_start.is_none() {
                    probes_at_start = Some(window.stats().probes);
                }
                // Runtime probes stay a small share of the listing's own
                // requests: about one per eight pages, past a base of two
                // per slot (a segment stuck on a slow page must still be
                // cut late in a run).
                let within_budget = probes_at_start.is_none_or(|start| {
                    let stats = window.stats();
                    stats.probes.saturating_sub(start)
                        <= 2 * flat_concurrency as u64 + stats.list_requests / 8
                });
                // A diff side held back by the lookahead also splits: the
                // segments it runs then include the one the merge waits on,
                // and its children list that range in parallel.
                // While startup discovery runs, its boundaries are the cuts
                // to wait for: runtime probes would cut the same range blind.
                let busy = busy(set.len(), &controls);
                if allow_split
                    && !discovering
                    && within_budget
                    && busy < cap
                    && pending_children.is_empty()
                    && (held.is_some() || hints.is_empty())
                {
                    let idle = cap - busy;
                    maybe_start_split_probes(
                        ctx,
                        start_prefix,
                        &controls,
                        idle,
                        in_key_order,
                        &flat_high,
                        &split_tx,
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

/// The waiting split child to start next. A diff merges each side in key
/// order, so it starts the lowest: when one round of cuts splits many
/// segments at once, starting their children in any other order let
/// segments far ahead of the merge fill their buffers and stall while the
/// one the merge waited for queued; and only when `admit` lets it (within
/// reach of the merge). A listing starts the latest split's first child
/// (the straggler just cut).
fn next_child(
    pending: &mut Vec<(usize, SplitRange)>,
    in_key_order: bool,
    admit: impl Fn(usize) -> bool,
) -> Option<(usize, SplitRange)> {
    if !in_key_order {
        return pending.pop();
    }
    let lowest = pending
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| a.1.start.cmp(&b.1.start))?
        .0;
    admit(pending[lowest].0).then(|| pending.remove(lowest))
}

/// Startup boundaries become cuts of whatever is running (at first the one
/// segment started at the beginning of the range; later batches cut the
/// segments earlier batches made). Accepted at once, as runtime cuts are: a
/// segment waiting on a slow first page hands the rest of its range over
/// now (its page in flight is trimmed at its new end). A boundary already
/// cut, or outside every running segment, changes nothing.
fn hand_over(
    bounds: &[String],
    controls: &HashMap<usize, Arc<SegmentControl>>,
    allow_split: bool,
) -> Vec<SplitRange> {
    let mut cut = 0usize;
    let mut children = Vec::new();
    if allow_split {
        for (index, control) in controls {
            if control.propose_cuts(bounds) {
                cut += 1;
                for mut child in control.accept_split_now() {
                    child.parent = *index;
                    children.push(child);
                }
            }
        }
    }
    info!(
        "Startup discovery: {} boundaries handed to {} running segment(s)",
        bounds.len(),
        cut
    );
    children
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
