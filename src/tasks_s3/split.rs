//! Adaptive long-tail splitting of list-mode segments.

use crate::core::S3TaskContext;
use crate::list_page::{FastContentsInterceptor, ParsedPageSlot};
use log::debug;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

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
pub(super) const SPLIT_CHECK_INTERVAL_MS: u64 = 200;
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

/// Right half of a split, sent from the segment task to the reactor.
#[derive(Debug, Clone)]
pub(super) struct SplitRange {
    pub(super) start: String,
    pub(super) end: Option<String>,
}

pub(super) type SplitSender = tokio::sync::mpsc::UnboundedSender<SplitRange>;

/// Shared state between the reactor and one running segment.
pub(super) struct SegmentControl {
    /// Last fully processed key; written by the segment task once per page.
    cursor: Mutex<String>,
    /// Upper boundary (exclusive start of the next segment); shrunk on split.
    end_before: Mutex<Option<String>>,
    /// Cut proposed by the reactor's probe, awaiting the segment's decision.
    pending_split: Mutex<Option<String>>,
    pub(super) pages: AtomicU32,
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
    pub(super) sent: Mutex<Option<String>>,
}

impl SegmentControl {
    pub(super) fn new(end: Option<String>) -> Self {
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
    pub(super) fn record_sent(&self, cursor: &str) {
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

    pub(super) fn current_end(&self) -> Option<String> {
        self.end_before.lock().unwrap().clone()
    }

    pub(super) fn record_page(&self, cursor: &str) {
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
    pub(super) fn try_accept_split(&self) -> Option<SplitRange> {
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
/// strictly inside `(cursor, end)`. When no rung has one and the rung pages
/// together hold every remaining key of the range, cuts at their median;
/// otherwise falls back to a flat-range cut near the middle of the remaining
/// keys.
async fn probe_split_candidate(
    ctx: &S3TaskContext,
    listing_prefix: &str,
    cursor: &str,
    end: Option<&str>,
    flat_high: &FlatHigh,
) -> SplitProbe {
    let dirs = ancestor_dirs(cursor, listing_prefix);
    // When the ladder reaches the listing prefix, every key left in the range
    // is either under a CommonPrefix some rung returns or a file directly in a
    // rung (a `<Contents>` of its page): a key after the cursor under a prefix
    // that sorts before the cursor is under one of the cursor's own ancestors,
    // which a deeper rung lists.  So when no rung has a CommonPrefix inside
    // the range and no rung page is truncated, the rung pages' in-range keys
    // are all of them — no flat-cut probes needed.
    let mut complete = dirs.len() <= SPLIT_PROBE_MAX_RUNGS;
    let mut known: Vec<String> = Vec::new();
    for dir in dirs.into_iter().take(SPLIT_PROBE_MAX_RUNGS) {
        let slot = ParsedPageSlot::default();
        let timeout_dur = Duration::from_secs(ctx.operation_timeout_secs);
        let send = ctx
            .s3_client
            .list_objects_v2()
            .bucket(&ctx.s3_bucket_name)
            .prefix(&dir)
            .start_after(cursor)
            .delimiter("/")
            // At the leaf rung the page is up to 1000 `<Contents>`: let the
            // fast parser take them (the SDK's per-object deserializer made
            // these probes ~15% of a listing's CPU).  The rows only matter
            // when no rung has a CommonPrefix to cut at (see above).
            .customize()
            .interceptor(FastContentsInterceptor::new(slot.clone()))
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
        if complete {
            // A truncated page, or one the fast parser left to the SDK, does
            // not show every key: fall back to the flat cut.
            match (response.is_truncated(), slot.take()) {
                (Some(false), Some(rows)) => known.extend(
                    rows.into_iter()
                        .map(|(key, _)| String::from(key.as_str()))
                        .filter(|k| k.as_str() > cursor && end.is_none_or(|e| k.as_str() < e)),
                ),
                _ => complete = false,
            }
        }
    }
    if complete {
        // With fewer than two keys in hand, take the flat cut as before
        // rather than retire the segment on the rung pages' word.
        if let Some(cut) = median_cut(known) {
            return SplitProbe::Cut(cut);
        }
    }

    probe_flat_cut(ctx, listing_prefix, cursor, end, flat_high).await
}

/// The median of a range's remaining keys, all of them in hand (the lower
/// median, so both halves keep at least one key); `None` below two keys.
fn median_cut(mut keys: Vec<String>) -> Option<String> {
    keys.sort_unstable();
    keys.dedup();
    match keys.len() {
        0 | 1 => None,
        n => Some(keys.swap_remove((n - 1) / 2)),
    }
}

/// Near-maximal real key of the listing, shared by a run's split probes: an
/// open-ended segment (the last one) needs an upper end to cut toward, and
/// estimating it costs several probe rounds, so the estimate is kept.
pub(super) type FlatHigh = Arc<Mutex<Option<String>>>;

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

pub(super) struct FanOutGovernor {
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
    pub(super) fn new(max: usize) -> Self {
        Self {
            last_at: Instant::now(),
            last_pages: 0,
            last_setlen: 0,
            last_rate: 0.0,
            cap: max,
            samples: 0,
        }
    }

    pub(super) fn effective_cap(&self) -> usize {
        self.cap
    }

    /// Sample run-wide page throughput once per `FANOUT_SAMPLE` and adjust the
    /// cap.  `retired` is the page count of completed-and-removed segments;
    /// live pages are summed from `controls`.  `max` is `flat_concurrency`.
    pub(super) fn observe(
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
pub(super) fn maybe_start_split_probes(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_median_cut_splits_known_keys_in_half() {
        let keys = |v: &[&str]| v.iter().map(|k| k.to_string()).collect::<Vec<_>>();
        let cut = |v: &[&str]| median_cut(keys(v));
        assert_eq!(cut(&[]), None);
        assert_eq!(cut(&["a/1"]), None);
        assert_eq!(cut(&["a/1", "a/1"]), None);
        // The lower median: the parent keeps "a/1", the child gets "a/2".
        assert_eq!(cut(&["a/2", "a/1"]), Some("a/1".into()));
        assert_eq!(cut(&["a/3", "a/1", "b", "a/2"]), Some("a/2".into()));
        assert_eq!(
            cut(&["a/5", "a/1", "a/4", "a/2", "a/3"]),
            Some("a/3".into())
        );
    }

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
