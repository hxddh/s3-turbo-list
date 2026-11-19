//! Adaptive long-tail splitting of list-mode segments.

use crate::auto_hints::FlatRun;
use crate::core::S3TaskContext;
use crate::list_page::{FastContentsInterceptor, ParsedPageSlot};
use log::{debug, info};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

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
/// Pages a split child must list before it is a candidate itself: it was
/// cut from a range known to be large, so one page (which gives it a cursor)
/// is enough.  Waiting `SPLIT_MIN_PAGES` made fan-out double only every
/// half-second or so, and a straggler finished before it could be split.
const SPLIT_CHILD_MIN_PAGES: u32 = 1;
/// Least a runtime-split child of a flat run gets, in pages: a range of a
/// page or so is finished by its own next page as soon as by children.
const SPLIT_CHILD_MIN_SPAN_PAGES: f64 = 2.0;
/// Most cuts one split probe proposes (children one segment adds at once).
const SPLIT_MAX_CUTS: usize = 15;
/// Reactor split-probe cadence. Sub-second so cold-start fan-out on flat
/// namespaces — where startup discovery finds no structure and runtime
/// splitting is the only mechanism that fans out — ramps toward full
/// concurrency in a few page-latencies instead of one segment per second.
/// (Checking every 50 ms was measured: it probed segments that were about
/// to finish, multiplying probes for no gain.)
pub(super) const SPLIT_CHECK_INTERVAL_MS: u64 = 200;
/// Maximum ancestor-directory probe rungs per split attempt.
const SPLIT_PROBE_MAX_RUNGS: usize = 4;
/// Consecutive failed split probes (errors or timeouts, not "no boundary")
/// before a segment stops being probed.  A throttled or briefly unreachable
/// endpoint says nothing about the range's structure, so one failure must not
/// retire the segment; repeated failures stop the probes adding to the load.
const SPLIT_PROBE_MAX_FAILURES: u32 = 3;
/// Right half of a split, sent from the segment task to the reactor.
#[derive(Debug, Clone)]
pub(super) struct SplitRange {
    pub(super) start: String,
    pub(super) end: Option<String>,
    /// The segment this range was split off (its keys follow the parent's).
    pub(super) parent: usize,
}

pub(super) type SplitSender = tokio::sync::mpsc::UnboundedSender<SplitRange>;

/// Shared state between the reactor and one running segment.
pub(super) struct SegmentControl {
    /// Last fully processed key; written by the segment task once per page.
    /// Starts at the segment's start, so a cut can never land before it.
    cursor: Mutex<String>,
    /// Held by the segment task while it reads its end, trims a page to it
    /// and advances its cursor, and by anyone accepting a cut from outside
    /// the task (`accept_split_now`): a cut accepted in between would leave
    /// the page trimmed at the old end, its rows past the cut listed again
    /// by the child.
    shaping: Mutex<()>,
    /// Upper boundary (exclusive start of the next segment); shrunk on split.
    end_before: Mutex<Option<String>>,
    /// Cuts proposed by the reactor's probe (ascending), awaiting the
    /// segment's decision.
    pending_split: Mutex<Option<Vec<String>>>,
    pub(super) pages: AtomicU32,
    /// A probe or pending decision is in flight.
    splitting: AtomicBool,
    /// Child slots that in-flight probe may fill (its cut budget); counts
    /// against the idle budget while `splitting` is set.
    reserved: AtomicU32,
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
    /// Waiting for room in its output channel: a diff segment ahead of the
    /// merge, its buffer full. It sends no requests meanwhile, so it takes
    /// no concurrency slot; it handed the rest of its range off before it
    /// waited (`hand_off_rest`), so there is nothing left to cut.
    waiting: AtomicBool,
    /// The first and last key of the segment's latest page, and how many it
    /// held: where the keys run, read off for cuts without a probe.
    sample: Mutex<Option<(String, String, usize)>>,
    /// A diff segment: about to wait for the merge, it hands the rest of its
    /// range to a child (see `hand_off_rest`).
    hands_off: bool,
}

impl SegmentControl {
    pub(super) fn new(end: Option<String>) -> Self {
        Self::with_min_pages(end, SPLIT_MIN_PAGES)
    }

    /// The control of a segment whose range starts after `start`: its cursor
    /// starts there.
    pub(super) fn starting_at(self, start: &str) -> Self {
        start.clone_into(&mut self.cursor.lock().unwrap());
        self
    }

    /// The segment task's page-shaping section (see `shaping`).
    pub(super) fn shaping(&self) -> std::sync::MutexGuard<'_, ()> {
        self.shaping.lock().unwrap()
    }

    /// `try_accept_split` from outside the segment task: waits while the
    /// task is shaping a page, so the cut applies to the next one whole.
    pub(super) fn accept_split_now(&self) -> Vec<SplitRange> {
        let _shaping = self.shaping();
        self.try_accept_split()
    }

    /// The control of a runtime-split child (see `SPLIT_CHILD_MIN_PAGES`).
    pub(super) fn new_child(end: Option<String>) -> Self {
        Self::with_min_pages(end, SPLIT_CHILD_MIN_PAGES)
    }

    fn with_min_pages(end: Option<String>, min_pages: u32) -> Self {
        Self {
            cursor: Mutex::new(String::new()),
            shaping: Mutex::new(()),
            end_before: Mutex::new(end),
            pending_split: Mutex::new(None),
            pages: AtomicU32::new(0),
            splitting: AtomicBool::new(false),
            reserved: AtomicU32::new(0),
            unsplittable: AtomicBool::new(false),
            next_probe_page: AtomicU32::new(min_pages),
            probe_failures: AtomicU32::new(0),
            sent: Mutex::new(None),
            waiting: AtomicBool::new(false),
            sample: Mutex::new(None),
            hands_off: false,
        }
    }

    /// The segment of a diff side (see `hand_off_rest`).
    pub(super) fn handing_off(mut self, hands_off: bool) -> Self {
        self.hands_off = hands_off;
        self
    }

    pub(super) fn hands_off(&self) -> bool {
        self.hands_off
    }

    /// End the segment at its cursor and hand the rest of its range to a
    /// child. A diff segment whose output buffer is full would otherwise wait
    /// for the merge with part of its range unlisted, and list that part one
    /// page per round trip once the merge reached it — every segment of a
    /// side in turn. The child lists the rest at once, into its own buffer.
    /// The caller holds `shaping`. `None` when nothing is left.
    pub(super) fn hand_off_rest(&self) -> Option<SplitRange> {
        let cursor = self.cursor.lock().unwrap();
        let mut end = self.end_before.lock().unwrap();
        if cursor.is_empty() || end.as_deref().is_some_and(|e| e <= cursor.as_str()) {
            return None;
        }
        let old_end = end.replace(cursor.clone());
        Some(SplitRange {
            start: cursor.clone(),
            end: old_end,
            parent: 0,
        })
    }

    /// Keep the segment's latest page (two keys or more) as its sample.
    pub(super) fn record_sample(
        &self,
        rows: &[(crate::core::ObjectKey, crate::core::ObjectProps)],
    ) {
        if let (Some((first, _)), Some((last, _)), true) =
            (rows.first(), rows.last(), rows.len() >= 2)
        {
            *self.sample.lock().unwrap() = Some((
                first.as_str().to_string(),
                last.as_str().to_string(),
                rows.len(),
            ));
        }
    }

    /// Mark the segment as waiting for its output channel (or done waiting).
    pub(super) fn set_waiting(&self, waiting: bool) {
        self.waiting.store(waiting, Ordering::Relaxed);
    }

    /// Whether the segment is waiting for its output channel.
    pub(super) fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
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
            && !self.is_waiting()
    }

    /// Hand the segment cuts found outside its own probes (startup discovery
    /// that finished after the segment started): accepted at its next page
    /// boundary like a probe's, those behind its cursor dropped.  Returns
    /// whether any cut lies inside the current range.  A segment with a
    /// probe in flight keeps that probe's proposal.
    pub(super) fn propose_cuts(&self, cuts: &[String]) -> bool {
        let end = self.current_end();
        let inside: Vec<String> = cuts
            .iter()
            .filter(|c| end.as_deref().is_none_or(|e| c.as_str() < e))
            .cloned()
            .collect();
        if inside.is_empty() || self.splitting.swap(true, Ordering::Relaxed) {
            return false;
        }
        *self.pending_split.lock().unwrap() = Some(inside);
        true
    }

    /// Called at a page boundary by the segment task.  Accepts the pending
    /// cuts that are still strictly ahead of the cursor and inside the
    /// current range (the rest are discarded): the segment ends at the first,
    /// and each cut starts a child up to the next cut (the last child takes
    /// the old end).  The children come back last-first, so linking each
    /// right after the parent leaves them in key order.
    pub(super) fn try_accept_split(&self) -> Vec<SplitRange> {
        let Some(proposed) = self.pending_split.lock().unwrap().take() else {
            return Vec::new();
        };
        let cursor = self.cursor.lock().unwrap();
        let mut end = self.end_before.lock().unwrap();
        let mut cuts: Vec<String> = proposed
            .into_iter()
            .filter(|c| {
                c.as_str() > cursor.as_str() && end.as_deref().is_none_or(|e| c.as_str() < e)
            })
            .collect();
        cuts.sort();
        cuts.dedup();
        self.splitting.store(false, Ordering::Relaxed);
        let Some(first) = cuts.first().cloned() else {
            return Vec::new();
        };
        let old_end = end.replace(first);
        let mut children = Vec::with_capacity(cuts.len());
        for (i, start) in cuts.iter().enumerate() {
            children.push(SplitRange {
                start: start.clone(),
                end: cuts.get(i + 1).cloned().or_else(|| old_end.clone()),
                parent: 0, // set by the segment that accepts the split
            });
        }
        children.reverse();
        children
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
    /// Real boundaries strictly inside the range, ascending (at least one).
    Cut(Vec<String>),
    /// The probes completed and found no boundary to cut at.
    NoBoundary,
    /// A probe request errored or timed out: nothing was learned.
    Failed,
    /// The range is too short to cut now; asked again later at no cost
    /// (no back-off: a segment waiting for the merge lists no pages, and
    /// backing off by pages left it uncut until the merge reached it).
    TooShort,
}

/// One delimiter probe per ancestor rung; returns up to `k` CommonPrefixes
/// strictly inside `(cursor, end)`, evenly spread. When no rung has one and
/// the rung pages together hold every remaining key of the range, cuts at
/// their quantiles; otherwise falls back to flat-range cuts spread over the
/// remaining keys.
async fn probe_split_candidate(
    ctx: &S3TaskContext,
    listing_prefix: &str,
    cursor: &str,
    end: Option<&str>,
    flat_high: &FlatHigh,
    k: usize,
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
    // The deepest rung's page, when it is a full page of keys inside the
    // range with more to follow: a sample of the flat run the range lists.
    let mut leaf: Option<FlatRun> = None;
    for (rung, dir) in dirs.into_iter().take(SPLIT_PROBE_MAX_RUNGS).enumerate() {
        let slot = ParsedPageSlot::default();
        let response = match crate::probe::send(&ctx.window, ctx.operation_timeout_secs, || {
            ctx.s3_client
                .list_objects_v2()
                .encoding_type(aws_sdk_s3::types::EncodingType::Url)
                .bucket(&ctx.s3_bucket_name)
                .prefix(&dir)
                .start_after(cursor)
                .delimiter("/")
                // At the leaf rung the page is up to 1000 `<Contents>`: let
                // the fast parser take them (the SDK's per-object
                // deserializer made these probes ~15% of a listing's CPU).
                // The rows only matter when no rung has a CommonPrefix to
                // cut at (see above).
                .customize()
                .interceptor(FastContentsInterceptor::new(slot.clone()))
                .send()
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!("Split probe failed for prefix '{}': {}", dir, e);
                return SplitProbe::Failed;
            }
        };
        let url = response.encoding_type() == Some(&aws_sdk_s3::types::EncodingType::Url);
        let dec = |k: &str| crate::list_page::response_key(url, k);
        let mut candidates: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix())
            .map(dec)
            .filter(|c| c.as_str() > cursor && end.is_none_or(|e| c.as_str() < e))
            .collect();
        if !candidates.is_empty() {
            candidates.sort();
            candidates.dedup();
            return SplitProbe::Cut(spread_picks(candidates, k));
        }
        let rows: Option<Vec<String>> = slot
            .take()
            .map(|rows| rows.into_iter().map(|(key, _)| dec(key.as_str())).collect());
        let in_range = |k: &String| k.as_str() > cursor && end.is_none_or(|e| k.as_str() < e);
        // The deepest rung's page reaches past the segment's end: it holds
        // every key left in the range (no CommonPrefix of it is inside), so
        // the segment's own next page finishes the range.
        if rung == 0
            && let (Some(rows), Some(e)) = (&rows, end)
            && rows.iter().any(|k| k.as_str() >= e)
        {
            return SplitProbe::NoBoundary;
        }
        if rung == 0
            && response.is_truncated() == Some(true)
            && let Some(rows) = &rows
            && rows.len() >= 2
            && rows.iter().all(in_range)
        {
            leaf = Some(FlatRun {
                prefix: dir.clone(),
                first_key: rows[0].clone(),
                last_key: rows[rows.len() - 1].clone(),
                keys: rows.len(),
            });
        }
        if complete {
            // A truncated page, or one the fast parser left to the SDK, does
            // not show every key: fall back to the flat cut.
            match (response.is_truncated(), rows) {
                (Some(false), Some(rows)) => known.extend(rows.into_iter().filter(in_range)),
                _ => complete = false,
            }
        }
    }
    if complete {
        // Every remaining key is in hand. When one page holds them, the
        // segment's next page finishes the range: cutting it would only add
        // requests (a child per sliver). Otherwise each child gets at least
        // a page. With fewer than two keys in hand, take the flat cut as
        // before rather than retire the segment on the rung pages' word.
        let page = ctx.max_keys.filter(|m| *m > 0).map_or(1000, |m| m as usize);
        if known.len() >= 2 && known.len() <= page {
            return SplitProbe::NoBoundary;
        }
        let k = k.min(known.len() / page).max(1);
        let cuts = quantile_cuts(known, k);
        if !cuts.is_empty() {
            return SplitProbe::Cut(cuts);
        }
    }

    if let Some(run) = leaf {
        return cut_flat_run(ctx, run, cursor, end, k).await;
    }
    probe_flat_cut(ctx, listing_prefix, cursor, end, flat_high, k).await
}

/// Cut the rest of a flat run the segment lists, from a page of it (`run`,
/// the keys right after the cursor), the way startup cuts flat runs: along
/// the keys' number, scoped to the run's directory. (Cutting across the
/// listing prefix put the candidates of a directory's run in the key space
/// past the directory, where the probes found nothing in range.)
///
/// A range that ends inside the run is measured off the run's number: it is
/// cut so each child gets at least `SPLIT_CHILD_MIN_SPAN_PAGES` pages, and
/// not now when it measures shorter than two of them (its own pages finish
/// it as fast as children would; it is measured again after more pages, as
/// one page is only a sample of the run). An open-ended range, or one past
/// the run, is sized by probing, and each child gets at least four pages.
async fn cut_flat_run(
    ctx: &S3TaskContext,
    run: FlatRun,
    cursor: &str,
    end: Option<&str>,
    k: usize,
) -> SplitProbe {
    let in_range = |c: &str| c > cursor && end.is_none_or(|e| c < e);
    let probe =
        |prefix: String, start_after: Option<String>| first_key_after(ctx, prefix, start_after);
    let scale = crate::flat_cut::RunScale::new(&run.first_key, &run.last_key, &run.prefix);
    let bounded = scale
        .as_ref()
        .zip(end)
        .and_then(|(scale, end)| scale.spans_to(end).map(|spans| (scale, spans)));
    let cuts: Vec<String> = match bounded {
        Some((scale, spans)) => {
            let children = (spans / SPLIT_CHILD_MIN_SPAN_PAGES).floor() as usize;
            if children < 2 {
                return SplitProbe::Failed;
            }
            let k = k.min(children - 1);
            // Positions in thousandths of a page span, evenly over the range.
            let span = (spans * 1000.0) as u128;
            let candidates: Vec<String> = (1..=k as u128)
                .filter_map(|j| scale.at_fraction(span * j, 1000 * (k as u128 + 1)))
                .collect();
            let found = futures::future::join_all(
                candidates
                    .into_iter()
                    .map(|c| probe(run.prefix.clone(), Some(c))),
            )
            .await;
            found
                .into_iter()
                .filter_map(|key| key.ok().flatten())
                .collect()
        }
        None => crate::auto_hints::partition_flat_runs(std::slice::from_ref(&run), k, probe).await,
    };
    let mut cuts: Vec<String> = cuts.into_iter().filter(|c| in_range(c)).collect();
    cuts.sort();
    cuts.dedup();
    if cuts.is_empty() {
        SplitProbe::Failed
    } else {
        SplitProbe::Cut(cuts)
    }
}

/// Cuts of a segment's remaining range `(cursor, end)` read off its own
/// latest page (`sample`: first key, last key, keys), with no request: the
/// page's keys give the run's number scale (as startup sizes flat runs), the
/// range is measured along it, and the cuts are evenly spread positions on
/// it, each child at least `SPLIT_CHILD_MIN_SPAN_PAGES` pages. A cut need
/// not be a key: segments are contiguous ranges, so any cut keeps the
/// listing exact, and one that lands in a gap costs its child one empty
/// request — what a probe validating it would have cost, without the round
/// trip. `None` when the keys have no number to read or the range ends past
/// the run (the probes decide then); empty when the range is too short to
/// cut now.
fn local_cuts(
    sample: &(String, String, usize),
    listing_prefix: &str,
    cursor: &str,
    end: &str,
    k: usize,
    page: usize,
) -> Option<Vec<String>> {
    let (first, last, keys) = sample;
    let scale = crate::flat_cut::RunScale::new(first, last, listing_prefix)?;
    if !end.starts_with(&scale.scope) {
        return None;
    }
    let (from, to) = (scale.spans_to(cursor)?, scale.spans_to(end)?);
    if to <= from {
        return None;
    }
    // The sample's span held `keys` keys: the range holds about this many
    // pages.
    let pages = (to - from) * *keys as f64 / page.max(1) as f64;
    let children = (pages / SPLIT_CHILD_MIN_SPAN_PAGES).floor() as usize;
    if children < 2 {
        return Some(Vec::new());
    }
    let k = k.min(children - 1);
    let mut cuts: Vec<String> = (1..=k)
        .filter_map(|j| {
            let at = from + (to - from) * j as f64 / (k + 1) as f64;
            scale.at_fraction((at * 1000.0) as u128, 1000)
        })
        .filter(|c| c.as_str() > cursor && c.as_str() < end)
        .collect();
    cuts.sort();
    cuts.dedup();
    Some(cuts)
}

/// The first key under `prefix` after `start_after`: one `max-keys=1` probe.
async fn first_key_after(
    ctx: &S3TaskContext,
    prefix: String,
    start_after: Option<String>,
) -> Result<Option<String>, String> {
    let response = crate::probe::send(&ctx.window, ctx.operation_timeout_secs, || {
        let mut request = ctx
            .s3_client
            .list_objects_v2()
            .encoding_type(aws_sdk_s3::types::EncodingType::Url)
            .bucket(&ctx.s3_bucket_name)
            .prefix(&prefix)
            .max_keys(1);
        if let Some(after) = &start_after {
            request = request.start_after(after);
        }
        request.send()
    })
    .await?;
    let url = response.encoding_type() == Some(&aws_sdk_s3::types::EncodingType::Url);
    Ok(response
        .contents()
        .first()
        .and_then(|o| o.key())
        .map(|k| crate::list_page::response_key(url, k)))
}

/// `k` of `sorted` (distinct, ascending), evenly spread; the middle one for
/// `k == 1`; all of them when they fit.
fn spread_picks(sorted: Vec<String>, k: usize) -> Vec<String> {
    let n = sorted.len();
    let k = k.max(1);
    if n <= k {
        return sorted;
    }
    if k == 1 {
        return vec![sorted[n / 2].clone()];
    }
    (1..=k).map(|j| sorted[j * n / (k + 1)].clone()).collect()
}

/// Up to `k` cuts at the quantiles of a range's remaining keys, all of them
/// in hand: every piece keeps at least one key (the lower median for `k ==
/// 1`).  Empty below two keys.
fn quantile_cuts(mut keys: Vec<String>, k: usize) -> Vec<String> {
    keys.sort_unstable();
    keys.dedup();
    let n = keys.len();
    if n < 2 {
        return Vec::new();
    }
    let k = k.max(1);
    let mut idx: Vec<usize> = (1..=k).map(|j| j * (n - 1) / (k + 1)).collect();
    idx.dedup();
    idx.into_iter().map(|i| keys[i].clone()).collect()
}

/// The median of a range's remaining keys, all of them in hand (the lower
/// median, so both halves keep at least one key); `None` below two keys.
#[cfg(test)]
fn median_cut(keys: Vec<String>) -> Option<String> {
    quantile_cuts(keys, 1).into_iter().next()
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
    k: usize,
) -> SplitProbe {
    let probe = |candidate: String| async move {
        let response = crate::probe::send(&ctx.window, ctx.operation_timeout_secs, || {
            ctx.s3_client
                .list_objects_v2()
                .encoding_type(aws_sdk_s3::types::EncodingType::Url)
                .bucket(&ctx.s3_bucket_name)
                .prefix(listing_prefix)
                .start_after(&candidate)
                .max_keys(1)
                .send()
        })
        .await
        .map_err(|e| format!("probe at '{}' failed: {}", candidate, e))?;
        let url = response.encoding_type() == Some(&aws_sdk_s3::types::EncodingType::Url);
        Ok(response
            .contents()
            .first()
            .and_then(|o| o.key())
            .map(|k| crate::list_page::response_key(url, k)))
    };
    let known_high = flat_high.lock().unwrap().clone();
    match crate::flat_cut::find_flat_cuts(
        listing_prefix,
        cursor,
        end,
        known_high.as_deref(),
        k.max(1),
        &probe,
    )
    .await
    {
        Ok((cuts, high)) => {
            if let Some(high) = high {
                let mut shared = flat_high.lock().unwrap();
                if shared.as_ref().is_none_or(|k| high > *k) {
                    *shared = Some(high);
                }
            }
            if cuts.is_empty() {
                SplitProbe::NoBoundary
            } else {
                SplitProbe::Cut(cuts)
            }
        }
        Err(e) => {
            debug!("Flat cut {}", e);
            SplitProbe::Failed
        }
    }
}

/// Choose which in-flight segments to probe for a split this tick.  Returns
/// segment indices busiest-first (most pages = most remaining range), bounded
/// so that total outstanding probes never exceed the idle slots: probes
/// already in flight (segments marked `splitting`, including ones with a
/// proposal pending acceptance) count against the budget.  This fills all
/// idle slots in a single pass while keeping probe-request cost bounded on
/// rate-limited providers.
/// Idle slots not yet promised to an in-flight probe: each probe holds the
/// child slots it may fill (its cut budget, at least one).
fn unreserved_slots(controls: &HashMap<usize, Arc<SegmentControl>>, idle_capacity: usize) -> usize {
    let reserved: usize = controls
        .values()
        .filter(|c| c.splitting.load(Ordering::Relaxed))
        .map(|c| c.reserved.load(Ordering::Relaxed).max(1) as usize)
        .sum();
    idle_capacity.saturating_sub(reserved)
}

/// The segments to probe for cuts. A listing probes the busiest. A diff
/// side (`in_key_order`) probes the lowest in key order, a few at a time so
/// each gets up to `SPLIT_MAX_CUTS` cuts: the merge reads the side in key
/// order, so the segment it waits on is the one to cut, and cuts far ahead
/// of it only fill buffers that then wait. (Busiest first spent the probe
/// budget in a diff's first seconds on segments about to wait, and the
/// merge's segment listed one page per round trip for the rest of the run.)
fn select_split_targets(
    controls: &HashMap<usize, Arc<SegmentControl>>,
    idle_capacity: usize,
    in_key_order: bool,
) -> Vec<usize> {
    let budget = unreserved_slots(controls, idle_capacity);
    if budget == 0 {
        return Vec::new();
    }
    let candidates = controls.iter().filter(|(_, c)| c.is_split_candidate());
    if in_key_order {
        let mut lowest: Vec<(usize, String)> = candidates
            .map(|(index, c)| (*index, c.snapshot().0))
            .collect();
        lowest.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        return lowest
            .into_iter()
            .take(budget.div_ceil(SPLIT_MAX_CUTS))
            .map(|(index, _)| index)
            .collect();
    }
    let mut busiest: Vec<(usize, u32)> = candidates
        .map(|(index, c)| (*index, c.pages.load(Ordering::Relaxed)))
        .collect();
    // Busiest first; index breaks ties for deterministic selection.
    busiest.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    busiest
        .into_iter()
        .take(budget)
        .map(|(index, _)| index)
        .collect()
}

/// Probe splittable in-flight segments in the background (see
/// `select_split_targets`), up to the idle-slot budget.  Each probe only
/// proposes a cut; the segment task decides at its next page boundary.
#[allow(clippy::too_many_arguments)]
pub(super) fn maybe_start_split_probes(
    ctx: &S3TaskContext,
    start_prefix: &str,
    controls: &HashMap<usize, Arc<SegmentControl>>,
    idle_capacity: usize,
    in_key_order: bool,
    flat_high: &FlatHigh,
    split_tx: &SplitSender,
    probes: &mut tokio::task::JoinSet<()>,
) {
    let free = unreserved_slots(controls, idle_capacity);
    let targets = select_split_targets(controls, idle_capacity, in_key_order);
    // Idle slots are shared between the probed segments: one segment alone
    // with 31 idle slots is cut into up to 16 pieces in one probe, instead
    // of doubling once per probe and page.
    let share = (free / targets.len().max(1)).clamp(1, SPLIT_MAX_CUTS);
    for index in targets {
        let control = Arc::clone(&controls[&index]);
        control.reserved.store(share as u32, Ordering::Relaxed);
        control.splitting.store(true, Ordering::Relaxed);
        let probe_ctx = ctx.clone();
        let listing_prefix = start_prefix.to_string();
        let flat_high = Arc::clone(flat_high);
        let split_tx = split_tx.clone();
        probes.spawn(async move {
            let (cursor, end) = control.snapshot();
            if cursor.is_empty() {
                control.splitting.store(false, Ordering::Relaxed);
                return;
            }
            let sample = control.sample.lock().unwrap().clone();
            let page = probe_ctx
                .max_keys
                .filter(|m| *m > 0)
                .map_or(1000, |m| m as usize);
            let local = sample.and_then(|sample| {
                local_cuts(
                    &sample,
                    &listing_prefix,
                    &cursor,
                    end.as_deref()?,
                    share,
                    page,
                )
            });
            let found = match local {
                Some(cuts) if cuts.is_empty() => SplitProbe::TooShort,
                Some(cuts) => SplitProbe::Cut(cuts),
                None => {
                    probe_split_candidate(
                        &probe_ctx,
                        &listing_prefix,
                        &cursor,
                        end.as_deref(),
                        &flat_high,
                        share,
                    )
                    .await
                }
            };
            match found {
                SplitProbe::Cut(cuts) => {
                    debug!(
                        "Split probe for segment {}: proposing {} cut(s) from '{}'",
                        index,
                        cuts.len(),
                        cuts[0]
                    );
                    control.probe_failures.store(0, Ordering::Relaxed);
                    *control.pending_split.lock().unwrap() = Some(cuts);
                    // Accepted at once, not at the segment's next page
                    // boundary: a segment waiting on a slow page hands its
                    // remaining range to the children now. Its page in flight
                    // started at its cursor, before every accepted cut, and is
                    // trimmed at the new end when it arrives. (If the segment
                    // reaches a page boundary first, it takes the proposal
                    // itself and this finds nothing.)
                    for mut child in control.accept_split_now() {
                        child.parent = index;
                        info!(
                            "Segment {} accepted runtime split at '{}'",
                            index, child.start
                        );
                        let _ = split_tx.send(child);
                    }
                }
                SplitProbe::Failed => {
                    debug!(
                        "Split probe for segment {} failed; will retry after more pages",
                        index
                    );
                    control.record_probe_failure();
                }
                SplitProbe::TooShort => {
                    control.splitting.store(false, Ordering::Relaxed);
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
    fn test_a_waiting_diff_segment_hands_the_rest_of_its_range_off() {
        let control = SegmentControl::new_child(Some("k9".to_string()))
            .starting_at("k0")
            .handing_off(true);
        control.record_page("k4");
        let child = control.hand_off_rest().unwrap();
        assert_eq!(
            (child.start.as_str(), child.end.as_deref()),
            ("k4", Some("k9"))
        );
        assert_eq!(
            control.current_end().as_deref(),
            Some("k4"),
            "ends at its cursor"
        );
        assert!(
            control.hand_off_rest().is_none(),
            "nothing left the second time"
        );
    }

    #[test]
    fn test_local_cuts_read_the_range_off_the_latest_page() {
        // A page of 1000 keys spans obj-000001000..obj-000001999; the range
        // (obj-000001999, obj-000011000) holds about nine pages.
        let sample = (
            "obj-000001000".to_string(),
            "obj-000001999".to_string(),
            1000,
        );
        let cuts = local_cuts(&sample, "", "obj-000001999", "obj-000011000", 15, 1000).unwrap();
        // At least two pages a child: four cuts, evenly spread, inside.
        assert_eq!(cuts.len(), 3, "{cuts:?}");
        assert!(cuts.windows(2).all(|w| w[0] < w[1]));
        assert!(
            cuts.iter()
                .all(|c| c.as_str() > "obj-000001999" && c.as_str() < "obj-000011000")
        );
        // Too short to cut now: empty, not a failure.
        let short = local_cuts(&sample, "", "obj-000001999", "obj-000004000", 15, 1000).unwrap();
        assert!(short.is_empty());
        // The end past the run (outside its scope): the probes decide.
        assert!(local_cuts(&sample, "", "obj-000001999", "zzz", 15, 1000).is_none());
    }

    #[test]
    fn test_multi_way_split_yields_contiguous_children_last_first() {
        let control = SegmentControl::new(Some("z".to_string()));
        control.record_page("b");
        control.splitting.store(true, Ordering::Relaxed);
        // Unordered, with one cut behind the cursor and one past the end.
        *control.pending_split.lock().unwrap() = Some(vec![
            "m".into(),
            "a".into(),
            "f".into(),
            "zz".into(),
            "f".into(),
        ]);
        let children = control.try_accept_split();
        let ranges: Vec<(String, Option<String>)> = children
            .iter()
            .map(|c| (c.start.clone(), c.end.clone()))
            .collect();
        assert_eq!(
            ranges,
            vec![
                ("m".to_string(), Some("z".to_string())),
                ("f".to_string(), Some("m".to_string())),
            ]
        );
        assert_eq!(control.current_end().as_deref(), Some("f"));
        assert!(!control.splitting.load(Ordering::Relaxed));
    }

    #[test]
    fn test_in_flight_probes_hold_their_whole_cut_budget() {
        let mut controls = HashMap::new();
        let busy = control_with_pages(10);
        busy.reserved.store(6, Ordering::Relaxed);
        busy.splitting.store(true, Ordering::Relaxed);
        controls.insert(0, busy);
        controls.insert(1, control_with_pages(10));
        // Six of eight idle slots belong to segment 0's probe.
        assert_eq!(unreserved_slots(&controls, 8), 2);
        assert_eq!(select_split_targets(&controls, 8, false), vec![1]);
        assert!(select_split_targets(&controls, 6, false).is_empty());
        // A finished probe's reservation no longer counts.
        controls[&0].splitting.store(false, Ordering::Relaxed);
        assert_eq!(unreserved_slots(&controls, 8), 8);
    }

    #[test]
    fn test_cuts_before_a_segment_start_are_dropped_before_its_first_page() {
        // A child (m, z] that has not listed a page yet: startup cuts below
        // its start would re-list its parent's range.
        let control = SegmentControl::new_child(Some("z".into())).starting_at("m");
        control.splitting.store(true, Ordering::Relaxed);
        *control.pending_split.lock().unwrap() = Some(vec!["b".into(), "f".into(), "p".into()]);
        let children = control.accept_split_now();
        let starts: Vec<&str> = children.iter().map(|c| c.start.as_str()).collect();
        assert_eq!(starts, ["p"]);
        assert_eq!(control.current_end().as_deref(), Some("p"));
    }

    #[test]
    fn test_outside_cuts_wait_for_the_page_being_shaped() {
        let control = Arc::new(SegmentControl::new(Some("z".into())).starting_at("a"));
        control.splitting.store(true, Ordering::Relaxed);
        *control.pending_split.lock().unwrap() = Some(vec!["m".into()]);
        let shaping = control.shaping();
        let accepter = {
            let control = Arc::clone(&control);
            std::thread::spawn(move || control.accept_split_now().len())
        };
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!accepter.is_finished(), "the cut waits for the page");
        // The page is trimmed at the end it read, and the cursor moves on.
        assert_eq!(control.current_end().as_deref(), Some("z"));
        control.record_page("c");
        drop(shaping);
        assert_eq!(accepter.join().unwrap(), 1);
        assert_eq!(control.current_end().as_deref(), Some("m"));
    }

    #[test]
    fn test_split_children_are_candidates_after_one_page() {
        let child = SegmentControl::new_child(None);
        assert!(!child.is_split_candidate());
        child.record_page("k");
        assert!(child.is_split_candidate());
        let original = SegmentControl::new(None);
        original.record_page("k");
        assert!(!original.is_split_candidate());
    }

    #[test]
    fn test_quantile_cuts_and_spread_picks() {
        let keys = |n: usize| (0..n).map(|i| format!("k{i:02}")).collect::<Vec<_>>();
        // Every piece keeps a key: no cut is the last key.
        assert_eq!(quantile_cuts(keys(10), 3), vec!["k02", "k04", "k06"]);
        assert_eq!(quantile_cuts(keys(3), 5), vec!["k00", "k01"]);
        assert!(quantile_cuts(keys(1), 4).is_empty());
        assert_eq!(spread_picks(keys(9), 1), vec!["k04"]);
        assert_eq!(spread_picks(keys(9), 2), vec!["k03", "k06"]);
        assert_eq!(spread_picks(keys(2), 5), keys(2));
    }

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
        *control.pending_split.lock().unwrap() = Some(vec!["big/b/".to_string()]);
        control.splitting.store(true, Ordering::Relaxed);

        let child = control.try_accept_split().pop().expect("split accepted");
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
        *control.pending_split.lock().unwrap() = Some(vec!["big/b/".to_string()]);
        control.splitting.store(true, Ordering::Relaxed);

        assert!(control.try_accept_split().is_empty());
        assert_eq!(control.current_end(), None);
        assert!(!control.splitting.load(Ordering::Relaxed));
    }

    #[test]
    fn test_segment_control_rejects_out_of_range_split() {
        // The proposed cut is at or beyond the current end boundary.
        let control = SegmentControl::new(Some("d/".to_string()));
        control.record_page("a/1");
        *control.pending_split.lock().unwrap() = Some(vec!["d/".to_string()]);
        assert!(control.try_accept_split().is_empty());

        // Unbounded segment accepts any cut ahead of the cursor.
        let control = SegmentControl::new(None);
        control.record_page("a/1");
        *control.pending_split.lock().unwrap() = Some(vec!["z/".to_string()]);
        assert!(!control.try_accept_split().is_empty());
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
        assert_eq!(select_split_targets(&controls, 2, false), vec![0, 1]);
        // Ample idle capacity: every eligible candidate, still ordered.
        assert_eq!(select_split_targets(&controls, 10, false), vec![0, 1, 2]);
        // No idle capacity: no probes.
        assert!(select_split_targets(&controls, 0, false).is_empty());
    }

    #[test]
    fn test_select_split_targets_counts_in_flight_against_budget() {
        let mut controls: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
        controls.insert(0, control_with_pages(10));
        controls.insert(1, control_with_pages(8));
        // Segment 0 is already probing: excluded as a candidate and it spends
        // one unit of the idle budget so total outstanding probes stay bounded.
        controls[&0].splitting.store(true, Ordering::Relaxed);

        assert_eq!(select_split_targets(&controls, 2, false), vec![1]);
        assert!(select_split_targets(&controls, 1, false).is_empty());
    }
}
