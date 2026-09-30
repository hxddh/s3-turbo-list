use crate::core;
use crate::core::{KeySpaceHints, ObjectKey, ObjectProps, S3TaskContext};
use crate::error::*;
use crate::list_page::{FastContentsInterceptor, ParsedPageSlot};
use crate::trace::S3CompatEvent;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, timeout_at};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentOutcome {
    index: usize,
    completed: bool,
    /// An original hint segment (counted in the heartbeat's done/remaining),
    /// not a runtime-split child.
    from_hints: bool,
}

// ── Adaptive long-tail splitting ───────────────────────────
//
// When the reactor has idle concurrency and a running segment has paged
// long enough to prove it is a long tail, a single delimiter probe on the
// segment's remaining range finds a real CommonPrefix boundary.  The
// segment task itself accepts the split at a page boundary (comparing the
// proposed cut against its authoritative cursor), shrinks its own
// end_before, and hands the right half back to the reactor as a child
// segment.  Anything uncertain — no structure, stale cut, probe error —
// results in no split.

/// Pages a segment must have processed before it is a split candidate.
const SPLIT_MIN_PAGES: u32 = 5;
/// Reactor split-probe cadence. Sub-second so cold-start fan-out on flat
/// namespaces — where startup discovery finds no structure and runtime
/// splitting is the only mechanism that fans out — ramps toward full
/// concurrency in a few page-latencies instead of one segment per second.
const SPLIT_CHECK_INTERVAL_MS: u64 = 200;
/// Maximum ancestor-directory probe rungs per split attempt.
const SPLIT_PROBE_MAX_RUNGS: usize = 4;
/// Consecutive failed split probes (errors or timeouts, not "no boundary")
/// before a segment stops being probed.  A throttled or briefly unreachable
/// endpoint says nothing about the range's structure, so one failure must not
/// retire the segment; repeated failures stop the probes adding to the load.
const SPLIT_PROBE_MAX_FAILURES: u32 = 3;
/// Minimum throughput gain (ratio) for newly added concurrency to count as
/// "helping".  Below this, the extra segments are oversubscribing a saturated
/// provider and fan-out is capped at the last useful level.
const FANOUT_IMPROVE_RATIO: f64 = 1.05;
/// Throughput sampling window for the fan-out governor.  The split tick runs
/// at 200ms, far too jittery to read a trend from; rates are judged over ~1s.
const FANOUT_SAMPLE: Duration = Duration::from_secs(1);
/// Largest exponent applied to `initial_backoff_secs` between consecutive
/// retries of one segment.
const RETRY_BACKOFF_MAX_SHIFT: u32 = 5;
/// Ceiling on a single inter-retry pause.
const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// Right half of a split, sent from the segment task to the reactor.
#[derive(Debug, Clone)]
struct SplitRange {
    start: String,
    end: Option<String>,
}

type SplitSender = tokio::sync::mpsc::UnboundedSender<SplitRange>;

/// Shared state between the reactor and one running segment.
pub(crate) struct SegmentControl {
    /// Last fully processed key; written by the segment task once per page.
    cursor: Mutex<String>,
    /// Upper boundary (exclusive start of the next segment); shrunk on split.
    end_before: Mutex<Option<String>>,
    /// Cut proposed by the reactor's probe, awaiting the segment's decision.
    pending_split: Mutex<Option<String>>,
    pages: AtomicU32,
    /// A probe or pending decision is in flight.
    splitting: AtomicBool,
    /// No structural boundary exists in the remaining range; do not re-probe.
    unsplittable: AtomicBool,
    /// Page count the segment must reach before its next probe.  Pushed ahead
    /// after a failed probe, so retries back off with the segment's progress.
    next_probe_page: AtomicU32,
    /// Consecutive probes that failed (as opposed to finding no boundary).
    probe_failures: AtomicU32,
    /// Last key whose page reached the output channel (`None`: nothing yet).
    /// Unlike `cursor` — recorded before the send, for split decisions — it
    /// only ever covers rows the data map will write, so an interrupted run
    /// can checkpoint "resume after this key" without losing a page.
    sent: Mutex<Option<String>>,
}

impl SegmentControl {
    fn new(end: Option<String>) -> Self {
        Self {
            cursor: Mutex::new(String::new()),
            end_before: Mutex::new(end),
            pending_split: Mutex::new(None),
            pages: AtomicU32::new(0),
            splitting: AtomicBool::new(false),
            unsplittable: AtomicBool::new(false),
            next_probe_page: AtomicU32::new(SPLIT_MIN_PAGES),
            probe_failures: AtomicU32::new(0),
            sent: Mutex::new(None),
        }
    }

    /// Every key up to `cursor` in this segment has been handed to the data
    /// map (or filtered out): a resume may start after it.
    fn record_sent(&self, cursor: &str) {
        let mut guard = self.sent.lock().unwrap();
        match guard.as_mut() {
            Some(sent) => {
                sent.clear();
                sent.push_str(cursor);
            }
            None => *guard = Some(cursor.to_string()),
        }
    }

    /// A probe errored or timed out.  Retry later rather than retiring the
    /// segment — until failures repeat, which is a reason to stop probing.
    fn record_probe_failure(&self) {
        let failures = self.probe_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= SPLIT_PROBE_MAX_FAILURES {
            self.unsplittable.store(true, Ordering::Relaxed);
        } else {
            let pages = self.pages.load(Ordering::Relaxed);
            self.next_probe_page
                .store(pages.saturating_add(SPLIT_MIN_PAGES), Ordering::Relaxed);
        }
        self.splitting.store(false, Ordering::Relaxed);
    }

    fn current_end(&self) -> Option<String> {
        self.end_before.lock().unwrap().clone()
    }

    fn record_page(&self, cursor: &str) {
        let mut guard = self.cursor.lock().unwrap();
        guard.clear();
        guard.push_str(cursor);
        drop(guard);
        self.pages.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (String, Option<String>) {
        (self.cursor.lock().unwrap().clone(), self.current_end())
    }

    fn is_split_candidate(&self) -> bool {
        self.pages.load(Ordering::Relaxed) >= self.next_probe_page.load(Ordering::Relaxed)
            && !self.splitting.load(Ordering::Relaxed)
            && !self.unsplittable.load(Ordering::Relaxed)
    }

    /// Called at a page boundary by the segment task.  Accepts the pending
    /// cut only if it is still strictly ahead of the cursor and inside the
    /// current range; otherwise the proposal is discarded.
    fn try_accept_split(&self) -> Option<SplitRange> {
        let proposed = self.pending_split.lock().unwrap().take()?;
        let cursor = self.cursor.lock().unwrap();
        let mut end = self.end_before.lock().unwrap();
        let in_range = proposed.as_str() > cursor.as_str()
            && end.as_deref().is_none_or(|e| proposed.as_str() < e);
        if !in_range {
            self.splitting.store(false, Ordering::Relaxed);
            return None;
        }
        let old_end = end.replace(proposed.clone());
        self.splitting.store(false, Ordering::Relaxed);
        Some(SplitRange {
            start: proposed,
            end: old_end,
        })
    }
}

/// What a segment task's `JoinError` means for the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinFailure {
    /// The run is already shutting down and aborted this task on purpose.
    ShutdownCancel,
    /// The task died on its own — its key range is missing from the output.
    LostSegment,
}

/// Classify a segment task's join failure.
///
/// Only a cancellation raised *while the run is already quitting* is benign:
/// that is `abort_all` reaping siblings after some other failure. Anything
/// else means a segment stopped early, and the keys it owned are simply absent
/// from the output — indistinguishable downstream from a range that was empty.
/// Treating that as non-fatal is what let a panicking segment produce a short
/// Parquet under `status: success` with `fatal_errors: 0`.
pub(crate) fn classify_join_failure(is_cancelled: bool, run_is_quitting: bool) -> JoinFailure {
    if is_cancelled && run_is_quitting {
        JoinFailure::ShutdownCancel
    } else {
        JoinFailure::LostSegment
    }
}

/// Ancestor directories of `cursor`, deepest first, ending at the listing
/// prefix: `big/a/0005` → `["big/a/", "big/", <listing_prefix>]`.
fn ancestor_dirs(cursor: &str, listing_prefix: &str) -> Vec<String> {
    let mut dirs = Vec::new();
    let mut idx = cursor.len();
    while let Some(pos) = cursor[..idx].rfind('/') {
        let dir = &cursor[..pos + 1];
        if dir.len() <= listing_prefix.len() {
            break;
        }
        dirs.push(dir.to_string());
        idx = pos;
    }
    if dirs.last().map(String::as_str) != Some(listing_prefix) {
        dirs.push(listing_prefix.to_string());
    }
    dirs
}

/// What a split probe learned about a segment's remaining range.
enum SplitProbe {
    /// A real boundary strictly inside the range.
    Cut(String),
    /// The probes completed and found no boundary to cut at.
    NoBoundary,
    /// A probe request errored or timed out: nothing was learned.
    Failed,
}

/// One delimiter probe per ancestor rung; returns the middle CommonPrefix
/// strictly inside `(cursor, end)`. When the range has no prefix structure,
/// falls back to a flat-range cut near the middle of the remaining keys.
async fn probe_split_candidate(
    ctx: &S3TaskContext,
    listing_prefix: &str,
    cursor: &str,
    end: Option<&str>,
    flat_high: &FlatHigh,
) -> SplitProbe {
    for dir in ancestor_dirs(cursor, listing_prefix)
        .into_iter()
        .take(SPLIT_PROBE_MAX_RUNGS)
    {
        let timeout_dur = Duration::from_secs(ctx.operation_timeout_secs);
        let send = ctx
            .s3_client
            .list_objects_v2()
            .bucket(&ctx.s3_bucket_name)
            .prefix(&dir)
            .start_after(cursor)
            .delimiter("/")
            // Only CommonPrefixes matter here, but at the leaf rung the page
            // is up to 1000 `<Contents>`: let the fast parser strip them (the
            // SDK's per-object deserializer made these probes ~15% of a
            // listing's CPU). The parsed rows are simply dropped.
            .customize()
            .interceptor(FastContentsInterceptor::new(ParsedPageSlot::default()))
            .send();
        let response = match timeout_at(Instant::now() + timeout_dur, send).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                debug!("Split probe failed for prefix '{}': {:?}", dir, e);
                return SplitProbe::Failed;
            }
            Err(_elapsed) => {
                debug!("Split probe timed out for prefix '{}'", dir);
                return SplitProbe::Failed;
            }
        };
        let mut candidates: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix())
            .filter(|c| *c > cursor && end.is_none_or(|e| *c < e))
            .map(str::to_string)
            .collect();
        if !candidates.is_empty() {
            candidates.sort();
            return SplitProbe::Cut(candidates.swap_remove(candidates.len() / 2));
        }
    }

    probe_flat_cut(ctx, listing_prefix, cursor, end, flat_high).await
}

/// Near-maximal real key of the listing, shared by a run's split probes: an
/// open-ended segment (the last one) needs an upper end to cut toward, and
/// estimating it costs several probe rounds, so the estimate is kept.
type FlatHigh = Arc<Mutex<Option<String>>>;

/// Flat-range split: no CommonPrefix structure exists, so cut near the middle
/// of the segment's remaining keys with max_keys=1 probes (see `flat_cut`):
/// a candidate between the cursor and the segment's upper end — its end
/// bound, or the listing's estimated high key when it is open-ended — whose
/// probe returns the real key that becomes the cut. The boundary is always an
/// observed key, never a synthetic guess, and children are themselves
/// splittable, so fan-out continues recursively.
async fn probe_flat_cut(
    ctx: &S3TaskContext,
    listing_prefix: &str,
    cursor: &str,
    end: Option<&str>,
    flat_high: &FlatHigh,
) -> SplitProbe {
    let probe = |candidate: String| async move {
        let timeout_dur = Duration::from_secs(ctx.operation_timeout_secs);
        let send = ctx
            .s3_client
            .list_objects_v2()
            .bucket(&ctx.s3_bucket_name)
            .prefix(listing_prefix)
            .start_after(&candidate)
            .max_keys(1)
            .send();
        match timeout_at(Instant::now() + timeout_dur, send).await {
            Ok(Ok(r)) => Ok(r
                .contents()
                .first()
                .and_then(|o| o.key())
                .map(str::to_string)),
            Ok(Err(e)) => Err(format!("probe at '{}' failed: {:?}", candidate, e)),
            Err(_elapsed) => Err(format!("probe at '{}' timed out", candidate)),
        }
    };
    let known_high = flat_high.lock().unwrap().clone();
    match crate::flat_cut::find_flat_cut(listing_prefix, cursor, end, known_high.as_deref(), &probe)
        .await
    {
        Ok(found) => {
            if let Some(high) = found.high {
                let mut shared = flat_high.lock().unwrap();
                if shared.as_ref().is_none_or(|k| high > *k) {
                    *shared = Some(high);
                }
            }
            match found.cut {
                Some(key) => SplitProbe::Cut(key),
                None => SplitProbe::NoBoundary,
            }
        }
        Err(e) => {
            debug!("Flat cut {}", e);
            SplitProbe::Failed
        }
    }
}

// ── Throughput-aware fan-out governor ──────────────────────
//
// Runtime splitting fans out to use idle concurrency, but a single bucket
// has a request-rate ceiling (e.g. ~50 req/s on OSS): past the point where
// segments saturate it, adding more in-flight segments only raises per-request
// latency.  The governor watches run-wide page throughput and caps fan-out at
// the highest concurrency that was still buying throughput.  It distinguishes
// the two split regimes by construction: while ramping up, added segments that
// stop raising throughput lower the cap; in the long-tail tail, segments finish
// and `set.len()` falls below the cap, so splitting resumes to refill.

struct FanOutGovernor {
    last_at: Instant,
    last_pages: u64,
    last_setlen: usize,
    last_rate: f64,
    /// Highest concurrency proven to still raise throughput; the split gate
    /// never fans out past it.  Starts open at `flat_concurrency`.
    cap: usize,
    samples: u32,
}

impl FanOutGovernor {
    fn new(max: usize) -> Self {
        Self {
            last_at: Instant::now(),
            last_pages: 0,
            last_setlen: 0,
            last_rate: 0.0,
            cap: max,
            samples: 0,
        }
    }

    fn effective_cap(&self) -> usize {
        self.cap
    }

    /// Sample run-wide page throughput once per `FANOUT_SAMPLE` and adjust the
    /// cap.  `retired` is the page count of completed-and-removed segments;
    /// live pages are summed from `controls`.  `max` is `flat_concurrency`.
    fn observe(
        &mut self,
        retired: u64,
        controls: &HashMap<usize, Arc<SegmentControl>>,
        setlen: usize,
        max: usize,
    ) {
        // Skip the whole-map page scan when a sample window hasn't elapsed; the
        // reactor calls this every tick but it only acts once per FANOUT_SAMPLE.
        let now = Instant::now();
        if now.duration_since(self.last_at) < FANOUT_SAMPLE {
            return;
        }
        self.observe_at(now, retired, live_pages(controls), setlen, max)
    }

    /// Pure core of `observe`, split out so tests can drive the clock and page
    /// totals directly without spawning segments.
    fn observe_at(&mut self, now: Instant, retired: u64, live: u64, setlen: usize, max: usize) {
        let elapsed = now.duration_since(self.last_at);
        if elapsed < FANOUT_SAMPLE {
            return;
        }
        let total = retired + live;
        let rate = (total.saturating_sub(self.last_pages)) as f64 / elapsed.as_secs_f64();

        self.samples += 1;
        // The first window only establishes a baseline; need two to judge.
        if self.samples >= 2 {
            if rate >= self.last_rate * FANOUT_IMPROVE_RATIO {
                // Throughput still climbing: reopen the ceiling and keep probing.
                self.cap = max;
            } else if setlen > self.last_setlen {
                // Added segments since last window without a throughput gain:
                // we are at/above useful concurrency.  Cap at the prior level.
                self.cap = self.last_setlen.max(1);
            }
            // Otherwise (no new segments, flat rate): long-tail region — hold
            // the cap so the split gate can refill as segments finish.
        }
        self.last_at = now;
        self.last_pages = total;
        self.last_setlen = setlen;
        self.last_rate = rate;
    }
}

fn live_pages(controls: &HashMap<usize, Arc<SegmentControl>>) -> u64 {
    controls
        .values()
        .map(|c| c.pages.load(Ordering::Relaxed) as u64)
        .sum()
}

// ── Public entry point ─────────────────────────────────────

pub async fn flat_list_main_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    flat_concurrency: usize,
    hints: KeySpaceHints,
) {
    flat_reactor_task(ctx, start_prefix, flat_concurrency, hints).await
}

// ── Reactor: controls concurrency via JoinSet ──────────────

async fn flat_reactor_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    flat_concurrency: usize,
    mut hints: KeySpaceHints,
) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Flat List S3 Task — {} — started", ctx.s3_bucket_name);
    tokio::task::yield_now().await;

    // Adaptive splitting only applies to plain list runs: diff uses a fixed,
    // key-ordered segment set per side (the merge needs it static), and
    // --start-after is a single-chain mode. A
    // --delimiter run is excluded for the reason hints are: a page's
    // CommonPrefixes are not range-bounded, so a split parent would keep
    // paging past its cut, re-listing the child's prefixes.
    let allow_split = ctx.dir & core::OBJECT_PROPS_FLAG_DIFF_MODE == 0
        && ctx.start_after.is_none()
        && ctx.delimiter.as_deref().unwrap_or("").is_empty();

    let (split_tx, mut split_rx) = tokio::sync::mpsc::unbounded_channel::<SplitRange>();
    let mut set = tokio::task::JoinSet::new();
    let mut controls: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
    // Where each in-flight segment started, for the resume ranges below.
    let mut starts: HashMap<usize, String> = HashMap::new();
    let mut unfinished: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
    let mut completed_pieces = 0usize;
    let mut pending_children: Vec<SplitRange> = Vec::new();
    // Children need indices no original segment uses: on a resume the set is
    // sparse, and a reused index would let a child's control replace its
    // namesake's in `controls` — then the split parent completes looking
    // unsplit and gets checkpointed while the child's range is still unlisted.
    let mut next_child_index = hints.index_end();
    let mut split_count = 0usize;
    let flat_high: FlatHigh = Arc::new(Mutex::new(None));
    let mut retired_pages = 0u64;
    let mut gov = FanOutGovernor::new(flat_concurrency);
    let mut last_ts = epoch_secs();
    // A persistent interval — unlike a fresh sleep per loop iteration, it
    // still fires when join/split events keep the select! busy.
    // In-flight split probes. Each holds a context clone — and with it a
    // data-map sender — so a detached probe kept the output open, delaying
    // the end of the run (and Ctrl-C) by up to its request timeouts. They
    // are aborted when the reactor exits.
    let mut probes = tokio::task::JoinSet::new();
    let mut split_check = tokio::time::interval(Duration::from_millis(SPLIT_CHECK_INTERVAL_MS));
    split_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Fill up to the concurrency limit: split children first, then hints.
        // Nothing new starts once the run is asked to stop — an interrupt
        // during startup discovery used to be followed by a full first fill.
        while set.len() < flat_concurrency && !ctx.is_quit() {
            let (index, start, end, from_hints) = if let Some(child) = pending_children.pop() {
                let index = next_child_index;
                next_child_index += 1;
                (index, child.start, child.end, false)
            } else if let Some(pair) = hints.next() {
                (pair.index, pair.start, pair.end, true)
            } else {
                break;
            };

            let control = Arc::new(SegmentControl::new(end));
            controls.insert(index, Arc::clone(&control));
            starts.insert(index, start.clone());
            let task_ctx = ctx.clone();
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
            pending_children.push(range);
            drained = true;
        }
        if drained && !ctx.is_quit() {
            continue;
        }

        if set.is_empty() && pending_children.is_empty() {
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
                    pending_children.push(range);
                }
            },
            _ = split_check.tick() => {
                let now = epoch_secs();
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
                pending_children.push(range);
            }
            info!("Flat List S3 Task — {} — aborted", ctx.s3_bucket_name);
            break;
        }
    }

    probes.abort_all();
    controls.extend(unfinished);
    *ctx.resume_progress.lock().unwrap() = Some(resume_progress(
        &controls,
        &starts,
        pending_children,
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

/// Choose which in-flight segments to probe for a split this tick.  Returns
/// segment indices busiest-first (most pages = most remaining range), bounded
/// so that total outstanding probes never exceed the idle slots: probes
/// already in flight (segments marked `splitting`, including ones with a
/// proposal pending acceptance) count against the budget.  This fills all
/// idle slots in a single pass while keeping probe-request cost bounded on
/// rate-limited providers.
fn select_split_targets(
    controls: &HashMap<usize, Arc<SegmentControl>>,
    idle_capacity: usize,
) -> Vec<usize> {
    let in_flight = controls
        .values()
        .filter(|c| c.splitting.load(Ordering::Relaxed))
        .count();
    let budget = idle_capacity.saturating_sub(in_flight);
    if budget == 0 {
        return Vec::new();
    }

    let mut candidates: Vec<(usize, u32)> = controls
        .iter()
        .filter(|(_, c)| c.is_split_candidate())
        .map(|(index, c)| (*index, c.pages.load(Ordering::Relaxed)))
        .collect();
    // Busiest first; index breaks ties for deterministic selection.
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    candidates
        .into_iter()
        .take(budget)
        .map(|(index, _)| index)
        .collect()
}

/// Probe the busiest splittable in-flight segments in the background, up to
/// the idle-slot budget.  Each probe only proposes a cut; the segment task
/// decides at its next page boundary.
fn maybe_start_split_probes(
    ctx: &S3TaskContext,
    start_prefix: &str,
    controls: &HashMap<usize, Arc<SegmentControl>>,
    idle_capacity: usize,
    flat_high: &FlatHigh,
    probes: &mut tokio::task::JoinSet<()>,
) {
    for index in select_split_targets(controls, idle_capacity) {
        let control = Arc::clone(&controls[&index]);
        control.splitting.store(true, Ordering::Relaxed);
        let probe_ctx = ctx.clone();
        let listing_prefix = start_prefix.to_string();
        let flat_high = Arc::clone(flat_high);
        probes.spawn(async move {
            let (cursor, end) = control.snapshot();
            if cursor.is_empty() {
                control.splitting.store(false, Ordering::Relaxed);
                return;
            }
            match probe_split_candidate(
                &probe_ctx,
                &listing_prefix,
                &cursor,
                end.as_deref(),
                &flat_high,
            )
            .await
            {
                SplitProbe::Cut(mid) => {
                    debug!("Split probe for segment {}: proposing cut '{}'", index, mid);
                    control.probe_failures.store(0, Ordering::Relaxed);
                    *control.pending_split.lock().unwrap() = Some(mid);
                    // The segment task clears `splitting` when it accepts or
                    // rejects the proposal at its next page boundary.
                }
                SplitProbe::Failed => {
                    debug!(
                        "Split probe for segment {} failed; will retry after more pages",
                        index
                    );
                    control.record_probe_failure();
                }
                SplitProbe::NoBoundary => {
                    debug!(
                        "Split probe for segment {}: no structural boundary in remaining range",
                        index
                    );
                    control.unsplittable.store(true, Ordering::Relaxed);
                    control.splitting.store(false, Ordering::Relaxed);
                }
            }
        });
    }
}

// ── Run one segment to completion (with retry) ─────────────

async fn flat_list_run_to_complete(
    ctx: &S3TaskContext,
    segment_index: usize,
    prefix: &str,
    start: &str,
    control: &SegmentControl,
    split_tx: Option<&SplitSender>,
) -> bool {
    // If the CLI provided --start-after, it overrides the segment's start.
    let mut start_after = ctx.start_after.as_deref().unwrap_or(start).to_string();
    let mut retry_attempt: u32 = 0;
    loop {
        match flat_list(
            ctx,
            segment_index,
            prefix,
            &start_after,
            control,
            split_tx,
            retry_attempt,
        )
        .await
        {
            Ok(()) => return true,
            Err(err) => {
                // Errors induced by shutdown (aborted siblings, the data-map
                // channel closing) are not segment failures: counting or
                // retrying them would inflate fatal_errors and burn S3
                // requests against a run that is already ending.
                if ctx.is_quit() {
                    return false;
                }
                // `max_attempts` bounds *consecutive* failures, not the
                // segment's lifetime. An attempt that listed part of its range
                // before failing leaves the segment strictly further along, so
                // it refunds the budget. Charging it instead made a run's
                // failure rate scale with its length rather than with how sick
                // the endpoint was: ten isolated hiccups, each retried
                // successfully, killed a listing that was advancing the whole
                // way — and long runs over big buckets accumulate ten of
                // anything. Keying the refund on the resume point (rather than
                // on a page count) keeps this monotonic: a refund is only ever
                // granted for ground the segment will not cover again, so a
                // segment that cannot advance still exhausts the budget instead
                // of retrying forever.
                let advanced = err.next_start() != start_after;
                let next_retry_attempt = if advanced {
                    0
                } else {
                    retry_attempt.saturating_add(1)
                };
                if err.continue_on_error() && next_retry_attempt < ctx.max_attempts {
                    start_after = err.next_start_owned();
                    retry_attempt = next_retry_attempt;
                    // Space consecutive failures. Re-issuing immediately is
                    // the wrong answer to `SlowDown` in particular: the
                    // endpoint has just asked for less load, and an
                    // undelayed retry answers with more of it. An attempt
                    // that advanced resets the budget and starts the delay
                    // over, so a healthy listing that hiccups once does not
                    // inherit a long pause.
                    // A throttle always pauses, even right after progress:
                    // an endpoint that lets one page through and then says
                    // `SlowDown` would otherwise be re-hit at once, forever.
                    let backoff_step = if err.is_throttle() {
                        retry_attempt.max(1)
                    } else {
                        retry_attempt
                    };
                    let delay = retry_backoff(ctx.initial_backoff_secs, backoff_step);
                    debug!(
                        "Retrying from '{}' (attempt {}{}) after {:?}: {}",
                        start_after,
                        retry_attempt,
                        if advanced { ", budget refunded" } else { "" },
                        delay,
                        err
                    );
                    if !delay.is_zero() {
                        // Raced against Ctrl-C: the backoff reaches 30 s,
                        // and nothing else wakes a segment sleeping in it.
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = quit_signalled(ctx) => {}
                        }
                        if ctx.is_quit() {
                            return false;
                        }
                    }
                    continue;
                }
                // Fatal error: fail the whole run fast via the global quit
                // signal. (Clearing this side's lifecycle bit here instead —
                // as this path once did — lied while sibling segments were
                // still running: the data map finalized early and the
                // siblings died on channel errors, inflating fatal_errors.)
                error!(
                    "Flat List S3 Task — {} — fatal after {} consecutive failed attempt(s) at '{}': {}",
                    ctx.s3_bucket_name,
                    retry_attempt.saturating_add(1),
                    start_after,
                    err
                );
                ctx.g_state.record_fatal_error(
                    err.errno(),
                    format!("bucket '{}': {}", ctx.s3_bucket_name, err.summary()),
                );
                ctx.g_state.quit();
                return false;
            }
        }
    }
}

/// Delay before the Nth consecutive retry of one segment: exponential from
/// `initial_backoff_secs`, capped so a long-lived segment never parks for
/// minutes. `attempt` is 1 for the first retry.
///
/// The cap matters more than the growth rate here. The budget counts
/// *consecutive* failures and is refunded whenever the segment advances, so a
/// segment only reaches the high attempts when it is making no progress at
/// all — at which point waiting longer neither helps it nor hurts a run that
/// is going to fail anyway, while unbounded growth would stall the reactor's
/// view of a segment that is still nominally alive.
/// Resolves once the run has been asked to stop (Ctrl-C / SIGTERM).
async fn quit_signalled(ctx: &S3TaskContext) {
    while !ctx.is_quit() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn retry_backoff(initial_backoff_secs: u64, attempt: u32) -> Duration {
    if attempt == 0 || initial_backoff_secs == 0 {
        return Duration::ZERO;
    }
    let factor = 1u64 << attempt.min(RETRY_BACKOFF_MAX_SHIFT);
    Duration::from_secs(initial_backoff_secs.saturating_mul(factor)).min(RETRY_BACKOFF_CAP)
}

// ── Single ListObjectsV2 continuation chain ────────────────

async fn flat_list(
    ctx: &S3TaskContext,
    segment_index: usize,
    prefix: &str,
    start_after: &str,
    control: &SegmentControl,
    split_tx: Option<&SplitSender>,
    retry_attempt: u32,
) -> Result<(), FlatRuntimeError> {
    let mut request = ctx
        .s3_client
        .list_objects_v2()
        .bucket(&ctx.s3_bucket_name)
        .prefix(prefix);

    // Only set start_after when non-empty.
    if !start_after.is_empty() {
        request = request.start_after(start_after);
    }
    if let Some(delim) = ctx
        .delimiter
        .as_deref()
        .filter(|delimiter| !delimiter.is_empty())
    {
        request = request.delimiter(delim);
    }
    if let Some(mk) = ctx.max_keys {
        request = request.max_keys(mk);
    }

    debug!(
        "S3 request: bucket={}, prefix={}, start_after={}",
        ctx.s3_bucket_name, prefix, start_after
    );

    // Pages are requested one at a time rather than through the SDK
    // paginator, for two reasons.  The paginator ends the stream — which read
    // here as "segment complete" — whenever a page carries no
    // NextContinuationToken or repeats the one it was sent, even when the page
    // says IsTruncated=true; a non-compliant endpoint then ended the run with
    // exit 0 and silently short output.  And each request carries its own
    // `FastContentsInterceptor`, which parses the page's `<Contents>` directly
    // (see `list_page`) instead of through the SDK's per-object deserializer.
    // Later pages keep `start_after` alongside the token, as the paginator did.
    let mut page_token: Option<String> = None;
    let mut next_start = start_after.to_string();
    let emit_common_prefixes = ctx.dir & core::OBJECT_PROPS_FLAG_DIFF_MODE == 0
        && !ctx.delimiter.as_deref().unwrap_or("").is_empty();
    let mut is_ended = false;
    let mut page_count: u32 = 0;
    let mut object_count: usize = 0;
    let mut common_prefixes_count: usize = 0;
    let segment_start = Instant::now();

    loop {
        let timeout_dur = Duration::from_secs(ctx.operation_timeout_secs);
        let page_start = Instant::now();
        let mut page_request = request.clone();
        if let Some(token) = page_token.as_deref() {
            page_request = page_request.continuation_token(token);
        }
        let parsed_slot = ParsedPageSlot::default();
        let send = page_request
            .customize()
            .interceptor(FastContentsInterceptor::new(parsed_slot.clone()))
            .send();
        let res = timeout_at(Instant::now() + timeout_dur, send).await;
        let latency_ms = page_start.elapsed().as_millis() as u64;
        let traced = TracedRequest {
            prefix,
            start_after,
            continuation_token: page_token.as_deref(),
            retry_attempt,
            latency_ms,
        };

        match res {
            Err(_elapsed) => {
                debug!("flat_list timeout at next_start: {}", next_start);
                ctx.g_state.inc_task_next_stream_timeout();

                // Emit trace event for timeout.
                write_trace(
                    ctx,
                    traced.failure(
                        ctx,
                        0,
                        Some("StreamTimeout".into()),
                        Some("ListObjectsV2 stream timeout".into()),
                        true,
                    ),
                );

                return Err(FlatRuntimeError::new(
                    ERROR_S3_NEXT_STREAM_TIMEOUT,
                    "ListObjectsV2 stream timeout".into(),
                    next_start,
                ));
            }
            Ok(Err(sdk_err)) => {
                error!("S3 API error: {:?}", sdk_err);
                return handle_sdk_error(sdk_err, &next_start, ctx, traced);
            }
            Ok(Ok(response)) => {
                let mut objects = response;
                // Rows the fast path parsed; `None` means this page fell back
                // to the SDK's own `Contents`.
                let parsed = parsed_slot.take();

                // Extract pagination metadata for trace.
                let is_truncated_reported = objects.is_truncated();
                let is_truncated = is_truncated_reported.unwrap_or(false);
                let next_token = objects.next_continuation_token().map(|t| t.to_string());
                let key_count_opt = objects.key_count();
                let cp_count = objects.common_prefixes().len() as i32;
                common_prefixes_count =
                    common_prefixes_count.saturating_add(objects.common_prefixes().len());
                let last_common_prefix = objects
                    .common_prefixes()
                    .last()
                    .and_then(|cp| cp.prefix())
                    .map(str::to_string);

                // Emit trace event for this page.
                let emit_page = |contents_count: usize, first: Option<&str>, last: Option<&str>| {
                    write_trace(
                        ctx,
                        traced.event(ctx, 200).map(|mut event| {
                            event.is_truncated = is_truncated;
                            event.next_continuation_token = next_token.clone();
                            event.next_continuation_token_present = Some(next_token.is_some());
                            event.key_count = key_count_opt;
                            event.contents_count = Some(contents_count as i32);
                            event.common_prefixes_count = Some(cp_count);
                            event.first_key = first.map(str::to_string);
                            event.last_key = last.map(str::to_string);
                            event
                        }),
                    )
                };
                match &parsed {
                    Some(rows) => emit_page(
                        rows.len(),
                        rows.first().map(|(key, _)| key.as_str()),
                        rows.last().map(|(key, _)| key.as_str()),
                    ),
                    None => emit_page(
                        objects.contents().len(),
                        objects.contents().first().and_then(|o| o.key()),
                        objects.contents().last().and_then(|o| o.key()),
                    ),
                }

                // The segment boundary is re-read each page so a runtime
                // split (which shrinks end_before) takes effect immediately.
                let until = control.current_end();

                // Either path moves each key's String into the batch; the
                // fast path's rows already are the batch.
                let mut batch: Vec<(ObjectKey, ObjectProps)> = match parsed {
                    Some(rows) => rows,
                    None => objects
                        .contents
                        .take()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|mut obj| {
                            let key = obj.key.take()?;
                            let props: ObjectProps = (&obj).into();
                            Some((ObjectKey::from(key), props))
                        })
                        .collect(),
                };

                // Segment ranges are (start_after, end_before].  The next
                // segment starts with start_after=end_before, so excluding
                // equality here would drop a real object whose key equals
                // the boundary.
                if let Some(end) = until.as_deref() {
                    if let Some(cut) = batch.iter().position(|(key, _)| end < key.as_str()) {
                        debug!("Segment boundary reached at key: {}", batch[cut].0);
                        batch.truncate(cut);
                        is_ended = true;
                    }
                }
                for (_, props) in batch.iter_mut() {
                    props.set_dir(ctx.dir);
                }
                object_count = object_count.saturating_add(batch.len());

                // A hierarchical (--delimiter) list run exists to show what
                // is at this level, "folders" included: emit each
                // CommonPrefix as a row (Key = the prefix, Size 0, no ETag),
                // merged into key order with the page's objects. They used
                // to be counted for the trace and dropped, so a bucket of
                // only folders listed as empty. (Diff compares objects, so it
                // does not emit them.)
                if emit_common_prefixes {
                    let prefixes: Vec<(ObjectKey, ObjectProps)> = objects
                        .common_prefixes
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|cp| cp.prefix)
                        // A chain restarted at a CommonPrefix (retry, resume)
                        // sends start-after=<prefix>; the keys under it sort
                        // after it and roll up into the same prefix, so the
                        // server returns it again. It was emitted already.
                        // Only that one prefix: a --start-after inside a
                        // folder (a/b/c) sorts after the folder's prefix
                        // (a/), which the server rightly returns and which
                        // was never emitted.
                        .filter(|cp| cp.as_str() != start_after)
                        .filter(|cp| {
                            !is_ended && until.as_deref().is_none_or(|end| cp.as_str() <= end)
                        })
                        .map(|cp| (cp.into(), ObjectProps::new_common_prefix(ctx.dir)))
                        .collect();
                    if !prefixes.is_empty() {
                        let objects_part = std::mem::take(&mut batch);
                        batch = merge_by_key(objects_part, prefixes);
                    }
                }

                // Remember the last processed key for resume-on-error.
                // Some S3-compatible providers omit KeyCount, so this must
                // not depend on provider pagination metadata.
                if let Some((key, _)) = batch.last() {
                    next_start.clear();
                    next_start.push_str(key.as_str());
                }
                // Delimiter pages can hold only CommonPrefixes; advance the
                // resume cursor over them too, or a retry restarts from many
                // pages back and re-lists them. Safe: a prefix string sorts
                // before every key it covers, and its keys are rolled up into
                // the prefix (never emitted), so nothing is skipped.
                if !is_ended {
                    if let Some(cp) = &last_common_prefix {
                        if cp.as_str() > next_start.as_str() {
                            next_start.clear();
                            next_start.push_str(cp);
                        }
                    }
                }
                control.record_page(&next_start);

                // A split proposal is decided here, at a page boundary,
                // against the authoritative cursor. A segment that already
                // crossed its boundary must not accept one: S3 order means
                // no keys remain in its range, so the child would be an
                // empty segment (a wasted request).
                if let (Some(tx), false) = (split_tx, is_ended || ctx.is_quit()) {
                    if let Some(child) = control.try_accept_split() {
                        info!(
                            "Segment {} accepted runtime split at '{}'",
                            segment_index, child.start
                        );
                        let _ = tx.send(child);
                    }
                }

                // Send batch to data_map via bounded channel.
                if !batch.is_empty() {
                    if let Err(e) = ctx.data_map_channel.send(batch).await {
                        if !ctx.is_quit() {
                            error!("Failed to send data to data_map channel: {}", e);
                        }
                        return Err(FlatRuntimeError::new(
                            ERROR_S3_CLIENT_GENERIC,
                            format!("Data map channel closed: {}", e),
                            next_start,
                        ));
                    }
                }
                if !next_start.is_empty() {
                    control.record_sent(&next_start);
                }

                page_count = page_count.saturating_add(1);

                if is_ended {
                    break;
                }

                // Next page, or the end of the listing.  A fresh, non-empty
                // token continues the chain (as the paginator did, whatever
                // IsTruncated says).  Without one the listing ends — unless
                // the endpoint said there is more: IsTruncated=true with no
                // usable token, or the token it was just sent handed back
                // (not explicitly IsTruncated=false), is a truncated page that
                // cannot be followed.  That must not pass for completion: fail
                // the attempt at the last key seen, so the retry loop resumes
                // with start-after — refunding the budget while it advances,
                // failing the run if the endpoint is stuck.
                let next_token = next_token.filter(|token| !token.is_empty());
                let repeated = next_token.is_some() && next_token == page_token;
                if !repeated && next_token.is_some() {
                    page_token = next_token;
                    continue;
                }
                let unfollowable = if repeated {
                    is_truncated_reported != Some(false)
                } else {
                    is_truncated_reported == Some(true)
                };
                if unfollowable {
                    let reason = if repeated {
                        "repeated the continuation token it was sent"
                    } else {
                        "reported IsTruncated=true without a NextContinuationToken"
                    };
                    warn!(
                        "Segment {}: ListObjectsV2 {}; resuming after '{}' with start-after",
                        segment_index, reason, next_start
                    );
                    return Err(FlatRuntimeError::new(
                        ERROR_S3_CLIENT_GENERIC,
                        format!("truncated ListObjectsV2 page: endpoint {}", reason),
                        next_start,
                    ));
                }

                // Pagination complete — emit final trace event (the record
                // the paginator's end of stream used to produce).
                write_trace(
                    ctx,
                    TracedRequest {
                        latency_ms: 0,
                        ..traced
                    }
                    .event(ctx, 200)
                    .map(|mut event| {
                        event.key_count = Some(0);
                        event.contents_count = Some(0);
                        event.common_prefixes_count = Some(0);
                        event
                    }),
                );
                break;
            }
        }
    }

    let ended_by = if is_ended { "boundary" } else { "pagination" };
    let final_end = control.current_end();
    emit_segment_summary(
        ctx,
        segment_index,
        prefix,
        start_after,
        final_end.as_deref(),
        retry_attempt,
        page_count,
        object_count,
        common_prefixes_count,
        segment_start.elapsed().as_millis() as u64,
        ended_by,
    );

    debug!(
        "Segment complete: start={}, end={:?}, pages={}",
        start_after, final_end, page_count
    );
    Ok(())
}

fn emit_segment_summary(
    ctx: &S3TaskContext,
    segment_index: usize,
    prefix: &str,
    start_after: &str,
    until: Option<&str>,
    retry_attempt: u32,
    page_count: u32,
    object_count: usize,
    common_prefixes_count: usize,
    elapsed_ms: u64,
    ended_by: &str,
) {
    let writer = match &ctx.trace_writer {
        Some(w) => w,
        None => return,
    };

    let mut event = S3CompatEvent::new(
        "ListObjectsV2SegmentSummary",
        &ctx.endpoint_url,
        &ctx.s3_bucket_name,
        prefix,
    );
    event.region = ctx.region.clone();
    event.set_provider(ctx.provider.as_deref());
    event.addressing_style = ctx.addressing_style.clone();
    event.start_after = if start_after.is_empty() {
        None
    } else {
        Some(start_after.to_string())
    };
    event.end_before = until.map(str::to_string);
    event.delimiter = ctx.delimiter.clone();
    event.max_keys = ctx.max_keys;
    event.retry_attempt = retry_attempt;
    event.latency_ms = elapsed_ms;
    event.http_status = 200;
    event.segment_index = Some(segment_index);
    event.segment_pages = Some(page_count);
    event.segment_objects = Some(object_count);
    event.segment_common_prefixes = Some(common_prefixes_count);
    event.ended_by = Some(ended_by.to_string());

    writer.write_event(event);
}

// ── Trace event emission ───────────────────────────────────

/// One ListObjectsV2 request of a segment's chain, as the compat trace
/// records it.
#[derive(Clone, Copy)]
struct TracedRequest<'a> {
    prefix: &'a str,
    start_after: &'a str,
    /// The continuation token this request sent (none on a chain's first page).
    continuation_token: Option<&'a str>,
    retry_attempt: u32,
    latency_ms: u64,
}

impl TracedRequest<'_> {
    /// This request's trace event with the run-level fields filled in, or
    /// `None` when tracing is off, so nothing is built for nothing.
    fn event(&self, ctx: &S3TaskContext, http_status: u16) -> Option<S3CompatEvent> {
        ctx.trace_writer.as_ref()?;
        let mut event = S3CompatEvent::new(
            "ListObjectsV2",
            &ctx.endpoint_url,
            &ctx.s3_bucket_name,
            self.prefix,
        );
        event.region = ctx.region.clone();
        event.set_provider(ctx.provider.as_deref());
        event.addressing_style = ctx.addressing_style.clone();
        event.start_after = (!self.start_after.is_empty()).then(|| self.start_after.to_string());
        event.continuation_token = self.continuation_token.map(str::to_string);
        event.delimiter = ctx.delimiter.clone();
        event.max_keys = ctx.max_keys;
        event.retry_attempt = self.retry_attempt;
        event.latency_ms = self.latency_ms;
        event.http_status = http_status;
        event.next_continuation_token_present = Some(false);
        Some(event)
    }

    /// The trace event of a failed request.
    fn failure(
        &self,
        ctx: &S3TaskContext,
        http_status: u16,
        code: Option<String>,
        message: Option<String>,
        retryable: bool,
    ) -> Option<S3CompatEvent> {
        let mut event = self.event(ctx, http_status)?;
        event.s3_error_code = code;
        event.s3_error_message = message;
        event.retryable = retryable;
        event.fatal = !retryable;
        Some(event)
    }
}

fn write_trace(ctx: &S3TaskContext, event: Option<S3CompatEvent>) {
    if let (Some(writer), Some(event)) = (&ctx.trace_writer, event) {
        writer.write_event(event);
    }
}

// ── SDK error classification ───────────────────────────────

fn handle_sdk_error(
    err: aws_sdk_s3::error::SdkError<ListObjectsV2Error>,
    next_start: &str,
    ctx: &S3TaskContext,
    traced: TracedRequest<'_>,
) -> Result<(), FlatRuntimeError> {
    let tracker = ctx.get_tracker();

    match &err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            let raw = service_err.raw();
            let http_code = raw.status().as_u16();
            let s3_err = service_err.err();
            let s3_code = s3_err.meta().code().map(|c| c.to_string());
            let s3_msg = s3_err.meta().message().map(|m| m.to_string());
            let errno = service_error_errno(s3_code.as_deref(), http_code);

            // Extract request ID from response headers (if available).
            let request_id = raw.headers().get("x-amz-request-id").map(|v| v.to_string());

            // Capture a bounded excerpt of the error response body.
            let body_excerpt: Option<String> = raw.body().bytes().map(|b| {
                let end = std::cmp::min(b.len(), 512);
                String::from_utf8_lossy(&b[..end]).into_owned()
            });

            let retryable = is_retryable(errno);

            if is_throttle(errno) {
                ctx.g_state.inc_throttled();
            }

            write_trace(
                ctx,
                traced
                    .failure(ctx, http_code, s3_code.clone(), s3_msg.clone(), retryable)
                    .map(|mut event| {
                        event.request_id = request_id.clone();
                        event.truncated_raw_body = body_excerpt.clone();
                        event
                    }),
            );

            error!(
                "Service error: code={:?}, msg={:?}, http={}",
                s3_code, s3_msg, http_code
            );

            Err(FlatRuntimeError::new(
                errno,
                s3_msg.unwrap_or_else(|| "Unknown S3 error".into()),
                next_start.into(),
            )
            .with_s3_error_details(
                http_code,
                s3_code,
                request_id,
                body_excerpt,
                tracker,
            ))
        }
        aws_sdk_s3::error::SdkError::DispatchFailure(dispatch_err) => {
            error!("Dispatch failure: {:?}", dispatch_err);

            let is_timeout = dispatch_err.is_timeout();
            let errno = if is_timeout {
                ctx.g_state.inc_s3_client_timeout();
                ERROR_S3_CLIENT_CONNECTION_TIMEOUT
            } else if let Some(conn_err) = dispatch_err.as_connector_error() {
                let err_str = conn_err.to_string();
                if err_str.contains("region must be set") {
                    ERROR_S3_MISSING_REGION
                } else if crate::error::is_missing_credentials(conn_err) {
                    ERROR_S3_MISSING_CREDENTIALS
                } else {
                    ctx.g_state.inc_s3_client_generic_error();
                    ERROR_S3_CLIENT_GENERIC
                }
            } else {
                ctx.g_state.inc_s3_client_generic_error();
                ERROR_S3_CLIENT_GENERIC
            };

            let retryable = is_retryable(errno);
            let (code, message) = match errno {
                ERROR_S3_MISSING_CREDENTIALS => (
                    "MissingCredentials",
                    MISSING_CREDENTIALS_MESSAGE.to_string(),
                ),
                _ if is_timeout => ("ConnectionTimeout", format!("{:?}", dispatch_err)),
                _ => ("DispatchFailure", format!("{:?}", dispatch_err)),
            };

            write_trace(
                ctx,
                traced.failure(ctx, 0, Some(code.into()), Some(message.clone()), retryable),
            );

            Err(FlatRuntimeError::new(errno, message, next_start.into()))
        }
        other => {
            error!("Unhandled SDK error: {:?}", other);
            ctx.g_state.inc_s3_client_generic_error();

            // Classified as ERROR_S3_CLIENT_GENERIC below, which the segment
            // loop retries; the trace must say the same.
            write_trace(
                ctx,
                traced.failure(
                    ctx,
                    0,
                    Some("Unknown".into()),
                    Some(format!("{:?}", other)),
                    is_retryable(ERROR_S3_CLIENT_GENERIC),
                ),
            );

            Err(FlatRuntimeError::new(
                ERROR_S3_CLIENT_GENERIC,
                format!("{:?}", other),
                next_start.into(),
            ))
        }
    }
}

// ── Helpers ────────────────────────────────────────────────

/// Merge two key-ordered row lists into one key-ordered list.
fn merge_by_key(
    left: Vec<(ObjectKey, ObjectProps)>,
    right: Vec<(ObjectKey, ObjectProps)>,
) -> Vec<(ObjectKey, ObjectProps)> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    loop {
        let take_left = match (left.peek(), right.peek()) {
            (Some(l), Some(r)) => l.0.as_str() <= r.0.as_str(),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        let next = if take_left { left.next() } else { right.next() };
        merged.extend(next);
    }
    merged
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ancestor_dirs_ladder() {
        assert_eq!(ancestor_dirs("big/a/0005", ""), vec!["big/a/", "big/", ""]);
        assert_eq!(ancestor_dirs("logs/a/b", "logs/"), vec!["logs/a/", "logs/"]);
        assert_eq!(ancestor_dirs("toplevel", ""), vec![""]);
        assert_eq!(ancestor_dirs("a/b", "a/"), vec!["a/"]);
    }

    #[test]
    fn test_failed_split_probe_backs_off_instead_of_retiring() {
        let control = SegmentControl::new(None);
        for _ in 0..SPLIT_MIN_PAGES {
            control.record_page("k");
        }
        assert!(control.is_split_candidate());

        // A transient probe failure defers the next probe; it does not mark
        // the segment unsplittable.
        control.splitting.store(true, Ordering::Relaxed);
        control.record_probe_failure();
        assert!(!control.unsplittable.load(Ordering::Relaxed));
        assert!(!control.splitting.load(Ordering::Relaxed));
        assert!(!control.is_split_candidate(), "must wait for more pages");
        for _ in 0..SPLIT_MIN_PAGES {
            control.record_page("k");
        }
        assert!(control.is_split_candidate());

        // Repeated failures do retire it.
        for _ in 1..SPLIT_PROBE_MAX_FAILURES {
            control.record_probe_failure();
        }
        assert!(control.unsplittable.load(Ordering::Relaxed));
        assert!(!control.is_split_candidate());
    }

    #[test]
    fn test_segment_control_accepts_valid_split() {
        let control = SegmentControl::new(Some("small/".to_string()));
        control.record_page("big/a/05");
        *control.pending_split.lock().unwrap() = Some("big/b/".to_string());
        control.splitting.store(true, Ordering::Relaxed);

        let child = control.try_accept_split().expect("split accepted");
        assert_eq!(child.start, "big/b/");
        assert_eq!(child.end.as_deref(), Some("small/"));
        assert_eq!(control.current_end().as_deref(), Some("big/b/"));
        assert!(!control.splitting.load(Ordering::Relaxed));
    }

    #[test]
    fn test_segment_control_rejects_stale_split() {
        // The cursor has already passed the proposed cut.
        let control = SegmentControl::new(None);
        control.record_page("big/c/99");
        *control.pending_split.lock().unwrap() = Some("big/b/".to_string());
        control.splitting.store(true, Ordering::Relaxed);

        assert!(control.try_accept_split().is_none());
        assert_eq!(control.current_end(), None);
        assert!(!control.splitting.load(Ordering::Relaxed));
    }

    #[test]
    fn test_segment_control_rejects_out_of_range_split() {
        // The proposed cut is at or beyond the current end boundary.
        let control = SegmentControl::new(Some("d/".to_string()));
        control.record_page("a/1");
        *control.pending_split.lock().unwrap() = Some("d/".to_string());
        assert!(control.try_accept_split().is_none());

        // Unbounded segment accepts any cut ahead of the cursor.
        let control = SegmentControl::new(None);
        control.record_page("a/1");
        *control.pending_split.lock().unwrap() = Some("z/".to_string());
        assert!(control.try_accept_split().is_some());
    }

    #[test]
    fn test_segment_control_split_candidate_gating() {
        let control = SegmentControl::new(None);
        assert!(!control.is_split_candidate(), "needs pages");
        for i in 0..SPLIT_MIN_PAGES {
            control.record_page(&format!("k/{}", i));
        }
        assert!(control.is_split_candidate());
        control.unsplittable.store(true, Ordering::Relaxed);
        assert!(!control.is_split_candidate(), "unsplittable is sticky");
    }

    fn control_with_pages(pages: u32) -> Arc<SegmentControl> {
        let control = SegmentControl::new(None);
        for i in 0..pages {
            control.record_page(&format!("k{}", i));
        }
        Arc::new(control)
    }

    #[test]
    fn test_select_split_targets_busiest_first_and_budget_bounded() {
        let mut controls: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
        controls.insert(0, control_with_pages(10)); // busiest
        controls.insert(1, control_with_pages(6));
        controls.insert(2, control_with_pages(SPLIT_MIN_PAGES));
        controls.insert(3, control_with_pages(SPLIT_MIN_PAGES - 1)); // below threshold

        // Two idle slots, nothing in flight: the two busiest candidates,
        // busiest first. The sub-threshold segment is never selected.
        assert_eq!(select_split_targets(&controls, 2), vec![0, 1]);
        // Ample idle capacity: every eligible candidate, still ordered.
        assert_eq!(select_split_targets(&controls, 10), vec![0, 1, 2]);
        // No idle capacity: no probes.
        assert!(select_split_targets(&controls, 0).is_empty());
    }

    #[test]
    fn test_select_split_targets_counts_in_flight_against_budget() {
        let mut controls: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
        controls.insert(0, control_with_pages(10));
        controls.insert(1, control_with_pages(8));
        // Segment 0 is already probing: excluded as a candidate and it spends
        // one unit of the idle budget so total outstanding probes stay bounded.
        controls[&0].splitting.store(true, Ordering::Relaxed);

        assert_eq!(select_split_targets(&controls, 2), vec![1]);
        assert!(select_split_targets(&controls, 1).is_empty());
    }

    #[test]
    fn test_retry_backoff_grows_then_caps() {
        // First retry waits the configured seed, then doubles.
        assert_eq!(
            retry_backoff(1, 0),
            Duration::ZERO,
            "no pause before the first attempt"
        );
        assert_eq!(retry_backoff(1, 1), Duration::from_secs(2));
        assert_eq!(retry_backoff(1, 2), Duration::from_secs(4));
        assert_eq!(retry_backoff(1, 3), Duration::from_secs(8));
        // A segment that cannot advance must not park indefinitely.
        assert_eq!(retry_backoff(1, 20), RETRY_BACKOFF_CAP);
        assert!(retry_backoff(60, 4) <= RETRY_BACKOFF_CAP);
    }

    #[test]
    fn test_retry_backoff_honours_a_zero_seed() {
        // `initial_backoff_secs = 0` is how the test config asks for no pause;
        // it must stay exactly that rather than becoming a one-second floor.
        for attempt in 0..6 {
            assert_eq!(retry_backoff(0, attempt), Duration::ZERO);
        }
    }

    #[test]
    fn test_fanout_governor_caps_when_added_concurrency_stops_helping() {
        let t0 = Instant::now();
        let win = FANOUT_SAMPLE + Duration::from_millis(10);
        let mut gov = FanOutGovernor::new(64);

        // Baseline window: 4 segments, 400 pages. Cap stays open.
        gov.observe_at(t0 + win, 0, 400, 4, 64);
        assert_eq!(gov.effective_cap(), 64);

        // Ramp helps: 8 segments, +800 pages (rate doubled). Cap reopened.
        gov.observe_at(t0 + win * 2, 0, 1200, 8, 64);
        assert_eq!(gov.effective_cap(), 64);

        // Ramp to 24 segments but throughput flat (+800 again): oversubscribed.
        // Cap drops back to the last useful level (8).
        gov.observe_at(t0 + win * 3, 0, 2000, 24, 64);
        assert_eq!(gov.effective_cap(), 8);
    }

    #[test]
    fn test_fanout_governor_reopens_when_throughput_climbs_again() {
        let t0 = Instant::now();
        let win = FANOUT_SAMPLE + Duration::from_millis(10);
        let mut gov = FanOutGovernor::new(32);

        gov.observe_at(t0 + win, 0, 500, 8, 32); // baseline
        gov.observe_at(t0 + win * 2, 0, 1000, 16, 32); // flat rate, added segs
        assert_eq!(gov.effective_cap(), 8, "capped at oversubscription");

        // Long-tail refill raises throughput again: ceiling reopens.
        gov.observe_at(t0 + win * 3, 0, 2000, 12, 32);
        assert_eq!(gov.effective_cap(), 32);
    }

    #[test]
    fn test_fanout_governor_ignores_sub_window_samples() {
        let t0 = Instant::now();
        let mut gov = FanOutGovernor::new(16);
        // Too soon: no sample taken, cap untouched, baseline not advanced.
        gov.observe_at(t0 + Duration::from_millis(100), 0, 999, 16, 16);
        assert_eq!(gov.effective_cap(), 16);
        assert_eq!(gov.samples, 0);
    }
}

#[cfg(test)]
mod join_failure_tests {
    use super::*;

    #[test]
    fn test_panicked_segment_is_a_lost_segment() {
        // A panic is never benign: the segment stopped mid-range and the keys
        // it owned are absent from the output, which nothing downstream can
        // distinguish from an empty range.
        assert_eq!(
            classify_join_failure(false, false),
            JoinFailure::LostSegment
        );
    }

    #[test]
    fn test_panicked_segment_stays_fatal_even_while_quitting() {
        // `is_quit()` alone must not excuse a join failure — the run may be
        // quitting *because* of this very panic, and the segment's keys are
        // missing either way.
        assert_eq!(classify_join_failure(false, true), JoinFailure::LostSegment);
    }

    #[test]
    fn test_cancellation_during_shutdown_is_benign() {
        // `abort_all` reaping siblings after some other failure already quit
        // the run: counting these would inflate fatal_errors on a clean stop.
        assert_eq!(
            classify_join_failure(true, true),
            JoinFailure::ShutdownCancel
        );
    }

    #[test]
    fn test_cancellation_without_shutdown_is_a_lost_segment() {
        // Nothing should cancel a segment while the run is healthy; if it
        // happens, the range is still missing and the run must not claim
        // success.
        assert_eq!(classify_join_failure(true, false), JoinFailure::LostSegment);
    }
}

#[cfg(test)]
mod flat_cut_tests {
    use crate::flat_cut::flat_cut_candidate;

    #[test]
    fn test_flat_cut_candidates_numeric_tail() {
        // Numeric tail: the cut lands at the numeric midpoint of the range,
        // strictly above the cursor.
        let candidate = flat_cut_candidate("obj-0014", "", "obj-0214").unwrap();
        assert!(candidate.as_str() > "obj-0014", "{}", candidate);
        assert_eq!(candidate, "obj-0114");
    }

    #[test]
    fn test_flat_cut_candidates_respect_end_bound() {
        let c = flat_cut_candidate("prefix-3/object-000123", "", "prefix-3/p").unwrap();
        assert!(c.as_str() > "prefix-3/object-000123", "{}", c);
        assert!(c.as_str() < "prefix-3/p", "{}", c);
    }

    #[test]
    fn test_flat_cut_candidates_listing_prefix_scopes_tail() {
        // The cut lies between two keys under the listing prefix, so the
        // candidate stays under the listing prefix scope.
        let c = flat_cut_candidate("logs/2026/abcdef", "logs/", "logs/2027/zz").unwrap();
        assert!(c.starts_with("logs/"), "{}", c);
    }

    #[test]
    fn test_flat_cut_candidates_empty_or_non_ascii_tail() {
        assert!(flat_cut_candidate("", "", "").is_none());
        // Multibyte keys yield valid UTF-8 candidates, never split characters.
        let c = flat_cut_candidate("中文键", "", "中文键键").unwrap();
        assert!(std::str::from_utf8(c.as_bytes()).is_ok());
        assert!(c.as_str() > "中文键" && c.as_str() < "中文键键", "{}", c);
    }
}

// ── Diff: parallel per-side listing ────────────────────────
//
// Diff sides list their static key-space segments in parallel; the merge
// consumes each side's segment channels in index order, which yields one
// globally ordered stream per side (segment k's keys all precede segment
// k+1's by boundary construction). Each segment's small channel acts as
// the prefetch window, bounding memory. Runtime splitting stays disabled
// for diff: the segment set must remain static for ordered consumption.

/// Per-segment channel capacity (batches): how far one segment may list
/// ahead of the merge.  At 4, a segment behind the merge head stalled after
/// four pages, so a side with a few large segments listed nearly serially
/// (one page per round trip).
pub const DIFF_SEGMENT_CHANNEL_CAP: usize = 32;
/// Upper bound on concurrently listing segments per diff side.
const DIFF_SIDE_MAX_CONCURRENCY: usize = 32;
/// Segments a side may start ahead of the one the merge is reading.  With
/// the channel capacity this bounds a side's buffered batches to
/// `DIFF_SIDE_LOOKAHEAD_SEGMENTS * DIFF_SEGMENT_CHANNEL_CAP` whatever the
/// segment count: finished segments used to keep their batches queued, so a
/// many-segment side could buffer most of the listing (RSS grew with bucket
/// size).
const DIFF_SIDE_LOOKAHEAD_SEGMENTS: usize = 16;

/// List one diff side across its static segments, writing each segment's
/// batches to the index-aligned sender. Any segment failure marks the run
/// fatal (non-zero exit), exactly like list mode.
pub async fn diff_list_side_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    concurrency: usize,
    boundaries: &[String],
    senders: Vec<tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>>,
    // Index of the segment the merge is reading (see `DiffSideStream`).
    mut merge_head: Option<tokio::sync::watch::Receiver<usize>>,
) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    let mut hints = KeySpaceHints::new_from(boundaries);
    assert_eq!(
        hints.total_count(),
        senders.len(),
        "diff side senders must align with segments"
    );
    info!(
        "Diff List S3 Task — {} — started, {} segments",
        ctx.s3_bucket_name,
        senders.len()
    );

    let concurrency = concurrency.clamp(1, DIFF_SIDE_MAX_CONCURRENCY);
    let mut senders = senders.into_iter();
    let mut set = tokio::task::JoinSet::new();

    let mut next_pair = hints.next();
    let mut aborted = false;
    loop {
        while set.len() < concurrency {
            // After Ctrl-C (possibly during startup discovery) start nothing
            // new: the merge aborts, and a fresh request would only delay
            // the exit by up to its timeout.
            if ctx.is_quit() {
                next_pair = None;
                break;
            }
            let Some(pair) = next_pair.take() else { break };
            if let Some(head) = &merge_head {
                if pair.index >= *head.borrow() + DIFF_SIDE_LOOKAHEAD_SEGMENTS {
                    next_pair = Some(pair);
                    break;
                }
            }
            next_pair = hints.next();
            let sender = senders.next().expect("sender per segment");
            let mut task_ctx = ctx.clone();
            task_ctx.data_map_channel = sender;
            let start_prefix = start_prefix.to_string();
            let control = Arc::new(SegmentControl::new(pair.end));
            let index = pair.index;
            let start = pair.start;
            set.spawn(async move {
                flat_list_run_to_complete(&task_ctx, index, &start_prefix, &start, &control, None)
                    .await
            });
        }

        if set.is_empty() && next_pair.is_none() {
            break;
        }
        // Wait for a segment to finish or, when the next segment is held
        // back by the lookahead window, for the merge to move on.
        let joined = tokio::select! {
            joined = set.join_next(), if !set.is_empty() => joined,
            // Ctrl-C: stop the in-flight segments now (list mode's reactor
            // does the same); a request or a retry backoff would otherwise
            // hold the exit for up to its timeout. The merge aborts.
            _ = quit_signalled(ctx), if !set.is_empty() && !aborted => {
                set.abort_all();
                aborted = true;
                continue;
            }
            changed = async {
                match merge_head.as_mut() {
                    Some(head) => head.changed().await,
                    None => std::future::pending().await,
                }
            }, if next_pair.is_some() => {
                if changed.is_err() {
                    // The merge is gone; stop gating (the sends will fail).
                    merge_head = None;
                }
                continue;
            }
        };
        match joined {
            Some(Ok(_completed)) => {}
            Some(Err(e)) => {
                // Losing a segment is worse here than in list mode: the merge
                // sees the missing keys as absent from this side and reports
                // every one of them as a one-sided difference. Two identical
                // buckets came out as a full-bucket delta, under
                // `status: success`.
                match classify_join_failure(e.is_cancelled(), ctx.is_quit()) {
                    JoinFailure::ShutdownCancel => {
                        info!("Diff segment task cancelled during shutdown");
                    }
                    JoinFailure::LostSegment => {
                        error!(
                            "Diff List S3 Task — {} — segment task failed to join, its key range is missing from this side of the comparison: {:?}",
                            ctx.s3_bucket_name, e
                        );
                        ctx.g_state.inc_fatal_error();
                        ctx.g_state.quit();
                    }
                }
            }
            None => break,
        }
        if ctx.is_quit() {
            set.abort_all();
            info!("Diff List S3 Task — {} — aborted", ctx.s3_bucket_name);
            break;
        }
    }

    ctx.complete();
    info!("Diff List S3 Task — {} — completed", ctx.s3_bucket_name);
}
