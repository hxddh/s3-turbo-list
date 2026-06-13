use crate::core;
use crate::core::{KeySpaceHints, ObjectKey, ObjectProps, S3TaskContext};
use crate::error::*;
use crate::trace::S3CompatEvent;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{timeout_at, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentOutcome {
    index: usize,
    completed: bool,
    /// Original hint segments record checkpoint progress; runtime-split
    /// children (and split parents) conservatively do not.
    checkpointable: bool,
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
/// Reactor split/heartbeat tick.
const SPLIT_CHECK_INTERVAL_SECS: u64 = 1;
/// Maximum ancestor-directory probe rungs per split attempt.
const SPLIT_PROBE_MAX_RUNGS: usize = 4;

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
    /// Segment gave part of its range away; checkpoint must not record it.
    was_split: AtomicBool,
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
            was_split: AtomicBool::new(false),
        }
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

    fn was_split(&self) -> bool {
        self.was_split.load(Ordering::Relaxed)
    }

    fn is_split_candidate(&self) -> bool {
        self.pages.load(Ordering::Relaxed) >= SPLIT_MIN_PAGES
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
            && end.as_deref().map_or(true, |e| proposed.as_str() < e);
        if !in_range {
            self.splitting.store(false, Ordering::Relaxed);
            return None;
        }
        let old_end = end.replace(proposed.clone());
        self.was_split.store(true, Ordering::Relaxed);
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

/// One delimiter probe per ancestor rung; returns the middle CommonPrefix
/// strictly inside `(cursor, end)`. When the range has no prefix structure,
/// falls back to flat-range cuts derived from the cursor itself.
async fn probe_split_candidate(
    ctx: &S3TaskContext,
    listing_prefix: &str,
    cursor: &str,
    end: Option<&str>,
) -> Option<String> {
    for dir in ancestor_dirs(cursor, listing_prefix)
        .into_iter()
        .take(SPLIT_PROBE_MAX_RUNGS)
    {
        let response = ctx
            .s3_client
            .list_objects_v2()
            .bucket(&ctx.s3_bucket_name)
            .prefix(&dir)
            .start_after(cursor)
            .delimiter("/")
            .send()
            .await;
        let response = match response {
            Ok(r) => r,
            Err(e) => {
                debug!("Split probe failed for prefix '{}': {:?}", dir, e);
                return None;
            }
        };
        let mut candidates: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix())
            .filter(|c| *c > cursor && end.map_or(true, |e| *c < e))
            .map(str::to_string)
            .collect();
        if !candidates.is_empty() {
            candidates.sort();
            return Some(candidates.swap_remove(candidates.len() / 2));
        }
    }

    probe_flat_cut(ctx, listing_prefix, cursor, end).await
}

/// Flat-range split: no CommonPrefix structure exists, so derive candidate
/// cuts from the cursor — a real key inside the live region — by truncating
/// it at several depths and bumping one character (which keeps every
/// candidate strictly above the cursor while sharing its key-space shape;
/// for numeric tails a mid-depth bump lands near a power-of-ten boundary).
/// Each candidate costs one max_keys=1 request; the first real key returned
/// inside `(cursor, end)` becomes the cut, so the boundary is always an
/// observed key, never a synthetic guess. Unbalanced cuts are fine: children
/// are themselves splittable, so fan-out continues recursively.
async fn probe_flat_cut(
    ctx: &S3TaskContext,
    listing_prefix: &str,
    cursor: &str,
    end: Option<&str>,
) -> Option<String> {
    for candidate in flat_cut_candidates(cursor, listing_prefix, end) {
        let response = ctx
            .s3_client
            .list_objects_v2()
            .bucket(&ctx.s3_bucket_name)
            .prefix(listing_prefix)
            .start_after(&candidate)
            .max_keys(1)
            .send()
            .await;
        let response = match response {
            Ok(r) => r,
            Err(e) => {
                debug!("Flat cut probe failed at '{}': {:?}", candidate, e);
                return None;
            }
        };
        if let Some(key) = response.contents().first().and_then(|o| o.key()) {
            if key > cursor && end.map_or(true, |e| key < e) {
                return Some(key.to_string());
            }
        }
    }
    None
}

/// Candidate cuts for a flat range, mid-depth first (most balanced for
/// structured tails), then deeper (closer to the cursor, higher hit rate).
fn flat_cut_candidates(cursor: &str, listing_prefix: &str, end: Option<&str>) -> Vec<String> {
    let tail_start = listing_prefix.len().min(cursor.len());
    let tail_len = cursor.len() - tail_start;
    if tail_len == 0 {
        return Vec::new();
    }

    let bytes = cursor.as_bytes();
    let mut candidates: Vec<String> = Vec::new();
    // Tail depths to bump at: 1/2 first (most balanced), then deeper
    // (3/4, 7/8 — closer to the cursor, higher hit rate), then 1/4.
    for (numerator, denominator) in [(1usize, 2usize), (3, 4), (7, 8), (1, 4)] {
        let pos = tail_start + (tail_len * numerator / denominator).min(tail_len - 1);
        // Only bump printable ASCII at a char boundary; skip otherwise.
        let byte = bytes[pos];
        if !cursor.is_char_boundary(pos) || !(0x20..0x7e).contains(&byte) {
            continue;
        }
        let mut candidate = cursor[..pos].to_string();
        candidate.push((byte + 1) as char);
        if candidate.as_str() > cursor
            && end.map_or(true, |e| candidate.as_str() < e)
            && !candidates.contains(&candidate)
        {
            candidates.push(candidate);
        }
    }
    candidates
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

    // Adaptive splitting only applies to plain list runs: diff is
    // authoritative single-segment by design, and --start-after /
    // --continuation-token are single-chain modes.
    let allow_split = ctx.dir & core::OBJECT_PROPS_FLAG_DIFF_MODE == 0
        && ctx.start_after.is_none()
        && ctx.continuation_token.is_none();

    let (split_tx, mut split_rx) = tokio::sync::mpsc::unbounded_channel::<SplitRange>();
    let mut set = tokio::task::JoinSet::new();
    let mut controls: HashMap<usize, Arc<SegmentControl>> = HashMap::new();
    let mut pending_children: Vec<SplitRange> = Vec::new();
    let mut next_child_index = hints.total_count();
    let mut split_count = 0usize;
    let mut last_ts = epoch_secs();
    // A persistent interval — unlike a fresh sleep per loop iteration, it
    // still fires when join/split events keep the select! busy.
    let mut split_check = tokio::time::interval(Duration::from_secs(SPLIT_CHECK_INTERVAL_SECS));
    split_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Fill up to the concurrency limit: split children first, then hints.
        while set.len() < flat_concurrency {
            let (index, start, end, checkpointable) = if let Some(child) = pending_children.pop() {
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
                    checkpointable,
                }
            });
        }

        if set.is_empty() {
            ctx.complete();
            info!("Flat List S3 Task — {} — completed", ctx.s3_bucket_name);
            tokio::time::sleep(Duration::from_secs(
                core::DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS,
            ))
            .await;
            break;
        }

        tokio::select! {
            joined = set.join_next() => match joined {
                Some(Ok(outcome)) => {
                    let control = controls.remove(&outcome.index);
                    if outcome.completed {
                        if outcome.checkpointable {
                            hints.finish(outcome.index);
                            let split = control.is_some_and(|c| c.was_split());
                            if split {
                                debug!(
                                    "Segment {} was split at runtime; not marking checkpoint progress",
                                    outcome.index
                                );
                            } else {
                                ctx.checkpoint_completed.lock().unwrap().push(outcome.index);
                            }
                        }
                    } else {
                        debug!(
                            "Segment {} did not complete successfully; not marking checkpoint progress",
                            outcome.index
                        );
                    }
                }
                Some(Err(e)) => {
                    error!("Task join error: {:?}", e);
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

                // Idle capacity and nothing queued: probe the longest-running
                // candidate for a structural split point.
                if allow_split
                    && set.len() < flat_concurrency
                    && pending_children.is_empty()
                    && hints.is_empty()
                {
                    maybe_start_split_probe(ctx, start_prefix, &controls);
                }
            },
        }

        // Handle global quit.
        if ctx.is_quit() {
            set.abort_all();
            info!("Flat List S3 Task — {} — aborted", ctx.s3_bucket_name);
            break;
        }
    }

    info!("Flat List S3 Task — {} — quit", ctx.s3_bucket_name);
}

/// Pick the busiest splittable in-flight segment and probe it in the
/// background.  The probe only proposes a cut; the segment task decides.
fn maybe_start_split_probe(
    ctx: &S3TaskContext,
    start_prefix: &str,
    controls: &HashMap<usize, Arc<SegmentControl>>,
) {
    let candidate = controls
        .iter()
        .filter(|(_, c)| c.is_split_candidate())
        .max_by_key(|(_, c)| c.pages.load(Ordering::Relaxed));
    let Some((&index, control)) = candidate else {
        return;
    };

    control.splitting.store(true, Ordering::Relaxed);
    let control = Arc::clone(control);
    let probe_ctx = ctx.clone();
    let listing_prefix = start_prefix.to_string();
    tokio::spawn(async move {
        let (cursor, end) = control.snapshot();
        if cursor.is_empty() {
            control.splitting.store(false, Ordering::Relaxed);
            return;
        }
        match probe_split_candidate(&probe_ctx, &listing_prefix, &cursor, end.as_deref()).await {
            Some(mid) => {
                debug!("Split probe for segment {}: proposing cut '{}'", index, mid);
                *control.pending_split.lock().unwrap() = Some(mid);
                // The segment task clears `splitting` when it accepts or
                // rejects the proposal at its next page boundary.
            }
            None => {
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

// ── Run one segment to completion (with retry) ─────────────

async fn flat_list_run_to_complete(
    ctx: &S3TaskContext,
    segment_index: usize,
    prefix: &str,
    start: &str,
    control: &SegmentControl,
    split_tx: Option<&SplitSender>,
) -> bool {
    let mut continuation_token = ctx.continuation_token.clone();
    // If the CLI provided --start-after, it overrides the segment's start.
    // Continuation-token resume is a single-chain mode; do not also send
    // start_after on the initial token request.
    let mut start_after = if continuation_token.is_some() {
        String::new()
    } else {
        ctx.start_after.as_deref().unwrap_or(start).to_string()
    };
    let mut retry_attempt: u32 = 0;
    loop {
        match flat_list(
            ctx,
            segment_index,
            prefix,
            &start_after,
            control,
            split_tx,
            continuation_token.as_deref(),
            retry_attempt,
        )
        .await
        {
            Ok(()) => return true,
            Err(err) => {
                let next_retry_attempt = retry_attempt.saturating_add(1);
                if err.continue_on_error() && next_retry_attempt < ctx.max_attempts {
                    start_after = err.next_start_owned();
                    continuation_token = None;
                    retry_attempt = next_retry_attempt;
                    debug!(
                        "Retrying from '{}' (attempt {}): {}",
                        start_after, retry_attempt, err
                    );
                    continue;
                }
                // Fatal error.
                error!(
                    "Flat List S3 Task — {} — fatal after {} attempt(s): {}",
                    ctx.s3_bucket_name,
                    retry_attempt.saturating_add(1),
                    err
                );
                ctx.g_state.inc_fatal_error();
                if ctx.is_running() {
                    ctx.complete();
                }
                return false;
            }
        }
    }
}

// ── Single ListObjectsV2 paginator call ────────────────────

async fn flat_list(
    ctx: &S3TaskContext,
    segment_index: usize,
    prefix: &str,
    start_after: &str,
    control: &SegmentControl,
    split_tx: Option<&SplitSender>,
    continuation_token: Option<&str>,
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
    if let Some(token) = continuation_token {
        request = request.continuation_token(token);
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

    let mut stream = request.into_paginator().send();
    let mut next_start = start_after.to_string();
    let mut is_ended = false;
    let mut page_count: u32 = 0;
    let mut object_count: usize = 0;
    let mut common_prefixes_count: usize = 0;
    let segment_start = Instant::now();

    loop {
        let timeout_dur = Duration::from_secs(ctx.operation_timeout_secs);
        let page_start = Instant::now();
        let res = timeout_at(Instant::now() + timeout_dur, stream.next()).await;
        let latency_ms = page_start.elapsed().as_millis() as u64;

        match res {
            Err(_elapsed) => {
                debug!("flat_list timeout at next_start: {}", next_start);
                ctx.g_state.inc_task_next_stream_timeout();

                // Emit trace event for timeout.
                emit_trace_compat(
                    ctx,
                    "ListObjectsV2",
                    prefix,
                    start_after,
                    continuation_token,
                    retry_attempt,
                    latency_ms,
                    0,
                    Some("StreamTimeout".into()),
                    Some("ListObjectsV2 stream timeout".into()),
                    None,
                    false,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    true,
                    false,
                    None,
                );

                return Err(FlatRuntimeError::new(
                    ERROR_S3_NEXT_STREAM_TIMEOUT,
                    "ListObjectsV2 stream timeout".into(),
                    next_start,
                ));
            }
            Ok(None) => {
                // Pagination complete — emit final trace event.
                emit_trace_compat(
                    ctx,
                    "ListObjectsV2",
                    prefix,
                    start_after,
                    continuation_token,
                    retry_attempt,
                    latency_ms,
                    200,
                    None,
                    None,
                    None,
                    false,
                    None,
                    Some(0),
                    Some(0),
                    Some(0),
                    None,
                    None,
                    false,
                    false,
                    None,
                );
                break;
            }
            Ok(Some(Err(sdk_err))) => {
                error!("S3 API error: {:?}", sdk_err);
                return handle_sdk_error(
                    sdk_err,
                    &next_start,
                    ctx,
                    prefix,
                    start_after,
                    continuation_token,
                    retry_attempt,
                    latency_ms,
                );
            }
            Ok(Some(Ok(response))) => {
                let objects = response;

                // Extract pagination metadata for trace.
                let is_truncated = objects.is_truncated().unwrap_or(false);
                let next_token = objects.next_continuation_token().map(|t| t.to_string());
                let key_count_opt = objects.key_count();
                let contents_count = objects.contents().len() as i32;
                let cp_count = objects.common_prefixes().len() as i32;
                common_prefixes_count =
                    common_prefixes_count.saturating_add(objects.common_prefixes().len());

                // Emit trace event for this page.
                emit_trace_compat(
                    ctx,
                    "ListObjectsV2",
                    prefix,
                    start_after,
                    continuation_token,
                    retry_attempt,
                    latency_ms,
                    200,
                    None,
                    None,
                    None,
                    is_truncated,
                    next_token.as_deref(),
                    key_count_opt,
                    Some(contents_count),
                    Some(cp_count),
                    objects.contents().first().and_then(|o| o.key()),
                    objects.contents().last().and_then(|o| o.key()),
                    false,
                    false,
                    None,
                );

                // The segment boundary is re-read each page so a runtime
                // split (which shrinks end_before) takes effect immediately.
                let until = control.current_end();

                // Consume the page contents so each key's String moves into
                // the batch instead of being copied — zero per-object key
                // allocation on the ingest hot path.
                let contents = objects.contents.unwrap_or_default();
                let mut batch: Vec<(ObjectKey, ObjectProps)> = Vec::with_capacity(contents.len());

                for mut obj in contents {
                    let Some(obj_key) = obj.key.take() else {
                        continue;
                    };

                    // Segment ranges are (start_after, end_before].  The next
                    // segment starts with start_after=end_before, so excluding
                    // equality here would drop a real object whose key equals
                    // the boundary.
                    if let Some(end) = until.as_deref() {
                        if end < obj_key.as_str() {
                            debug!("Segment boundary reached at key: {}", obj_key);
                            is_ended = true;
                            break;
                        }
                    }

                    let mut props: ObjectProps = (&obj).into();
                    props.set_dir(ctx.dir);
                    batch.push((obj_key.into(), props));
                    object_count = object_count.saturating_add(1);
                }

                // Remember the last processed key for resume-on-error.
                // Some S3-compatible providers omit KeyCount, so this must
                // not depend on provider pagination metadata.
                if let Some((key, _)) = batch.last() {
                    next_start.clear();
                    next_start.push_str(key.as_str());
                }
                control.record_page(&next_start);

                // A split proposal is decided here, at a page boundary,
                // against the authoritative cursor.
                if let Some(tx) = split_tx {
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

                page_count = page_count.saturating_add(1);

                if is_ended {
                    break;
                }
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
    event.profile = ctx.profile.clone();
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

fn emit_trace_compat(
    ctx: &S3TaskContext,
    operation: &str,
    prefix: &str,
    start_after: &str,
    continuation_token: Option<&str>,
    retry_attempt: u32,
    latency_ms: u64,
    http_status: u16,
    s3_error_code: Option<String>,
    s3_error_message: Option<String>,
    request_id: Option<String>,
    is_truncated: bool,
    next_token: Option<&str>,
    key_count: Option<i32>,
    contents_count: Option<i32>,
    common_prefixes_count: Option<i32>,
    first_key: Option<&str>,
    last_key: Option<&str>,
    retryable: bool,
    fatal: bool,
    truncated_raw_body: Option<String>,
) {
    let writer = match &ctx.trace_writer {
        Some(w) => w,
        None => return,
    };

    let mut event = S3CompatEvent::new(operation, &ctx.endpoint_url, &ctx.s3_bucket_name, prefix);
    event.region = ctx.region.clone();
    event.profile = ctx.profile.clone();
    event.addressing_style = ctx.addressing_style.clone();
    event.start_after = if start_after.is_empty() {
        None
    } else {
        Some(start_after.to_string())
    };
    event.continuation_token = continuation_token.map(str::to_string);
    event.delimiter = ctx.delimiter.clone();
    event.max_keys = ctx.max_keys;
    event.retry_attempt = retry_attempt;
    event.latency_ms = latency_ms;
    event.http_status = http_status;
    event.s3_error_code = s3_error_code;
    event.s3_error_message = s3_error_message;
    event.request_id = request_id;
    event.retryable = retryable;
    event.fatal = fatal;
    event.is_truncated = is_truncated;
    event.next_continuation_token = next_token.map(|t| t.to_string());
    event.next_continuation_token_present = Some(next_token.is_some());
    event.key_count = key_count;
    event.contents_count = contents_count;
    event.common_prefixes_count = common_prefixes_count;
    event.first_key = first_key.map(str::to_string);
    event.last_key = last_key.map(str::to_string);
    event.truncated_raw_body = truncated_raw_body;

    writer.write_event(event);
}

// ── SDK error classification ───────────────────────────────

fn handle_sdk_error(
    err: aws_sdk_s3::error::SdkError<ListObjectsV2Error>,
    next_start: &str,
    ctx: &S3TaskContext,
    prefix: &str,
    start_after: &str,
    continuation_token: Option<&str>,
    retry_attempt: u32,
    latency_ms: u64,
) -> Result<(), FlatRuntimeError> {
    let tracker = ctx.get_tracker();

    match &err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            let raw = service_err.raw();
            let http_code = raw.status().as_u16();
            let s3_err = service_err.err();
            let s3_code = s3_err.meta().code().map(|c| c.to_string());
            let s3_msg = s3_err.meta().message().map(|m| m.to_string());
            let errno = s3_error_code_to_errno(s3_code.as_deref());

            // Extract request ID from response headers (if available).
            let request_id = raw.headers().get("x-amz-request-id").map(|v| v.to_string());

            // Capture a bounded excerpt of the error response body.
            let body_excerpt: Option<String> = raw.body().bytes().map(|b| {
                let end = std::cmp::min(b.len(), 512);
                String::from_utf8_lossy(&b[..end]).into_owned()
            });

            let retryable = errno < ERROR_NO_BUCKET;
            let fatal = errno >= ERROR_NO_BUCKET;

            // Emit trace event.
            emit_trace_compat(
                ctx,
                "ListObjectsV2",
                prefix,
                start_after,
                continuation_token,
                retry_attempt,
                latency_ms,
                http_code,
                s3_code.clone(),
                s3_msg.clone(),
                request_id.clone(),
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                retryable,
                fatal,
                body_excerpt.clone(),
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
                } else {
                    ctx.g_state.inc_s3_client_generic_error();
                    ERROR_S3_CLIENT_GENERIC
                }
            } else {
                ctx.g_state.inc_s3_client_generic_error();
                ERROR_S3_CLIENT_GENERIC
            };

            let retryable = errno < ERROR_NO_BUCKET;

            emit_trace_compat(
                ctx,
                "ListObjectsV2",
                prefix,
                start_after,
                continuation_token,
                retry_attempt,
                latency_ms,
                0,
                Some(if is_timeout {
                    "ConnectionTimeout".into()
                } else {
                    "DispatchFailure".into()
                }),
                Some(format!("{:?}", dispatch_err)),
                None,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                retryable,
                false,
                None,
            );

            Err(FlatRuntimeError::new(
                errno,
                format!("{:?}", dispatch_err),
                next_start.into(),
            ))
        }
        other => {
            error!("Unhandled SDK error: {:?}", other);
            ctx.g_state.inc_s3_client_generic_error();

            emit_trace_compat(
                ctx,
                "ListObjectsV2",
                prefix,
                start_after,
                continuation_token,
                retry_attempt,
                latency_ms,
                0,
                Some("Unknown".into()),
                Some(format!("{:?}", other)),
                None,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
                false,
                None,
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
    fn test_segment_control_accepts_valid_split() {
        let control = SegmentControl::new(Some("small/".to_string()));
        control.record_page("big/a/05");
        *control.pending_split.lock().unwrap() = Some("big/b/".to_string());
        control.splitting.store(true, Ordering::Relaxed);

        let child = control.try_accept_split().expect("split accepted");
        assert_eq!(child.start, "big/b/");
        assert_eq!(child.end.as_deref(), Some("small/"));
        assert_eq!(control.current_end().as_deref(), Some("big/b/"));
        assert!(control.was_split());
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
        assert!(!control.was_split());
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
}

#[cfg(test)]
mod flat_cut_tests {
    use super::*;

    #[test]
    fn test_flat_cut_candidates_numeric_tail() {
        // cursor tail "obj-0014" (8 chars): 1/2-depth bump first.
        let candidates = flat_cut_candidates("obj-0014", "", None);
        assert!(!candidates.is_empty());
        // Every candidate is strictly above the cursor.
        for c in &candidates {
            assert!(c.as_str() > "obj-0014", "{}", c);
        }
        // The mid-depth bump comes first: "obj-0014"[..4] + '1' = "obj-1".
        assert_eq!(candidates[0], "obj-1");
    }

    #[test]
    fn test_flat_cut_candidates_respect_end_bound() {
        let candidates = flat_cut_candidates("prefix-3/object-000123", "", Some("prefix-3/p"));
        for c in &candidates {
            assert!(c.as_str() > "prefix-3/object-000123", "{}", c);
            assert!(c.as_str() < "prefix-3/p", "{}", c);
        }
    }

    #[test]
    fn test_flat_cut_candidates_listing_prefix_scopes_tail() {
        // Bumps happen inside the tail after the listing prefix, so every
        // candidate stays under the listing prefix scope.
        let candidates = flat_cut_candidates("logs/2026/abcdef", "logs/", None);
        assert!(!candidates.is_empty());
        for c in &candidates {
            assert!(c.starts_with("logs/"), "{}", c);
        }
    }

    #[test]
    fn test_flat_cut_candidates_empty_or_non_ascii_tail() {
        assert!(flat_cut_candidates("", "", None).is_empty());
        // Multibyte tail positions are skipped rather than corrupting keys.
        let candidates = flat_cut_candidates("中文键", "", None);
        for c in &candidates {
            assert!(std::str::from_utf8(c.as_bytes()).is_ok());
        }
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

/// Per-segment channel capacity (batches) — the diff prefetch window.
pub const DIFF_SEGMENT_CHANNEL_CAP: usize = 4;
/// Upper bound on concurrently listing segments per diff side.
const DIFF_SIDE_MAX_CONCURRENCY: usize = 32;

/// List one diff side across its static segments, writing each segment's
/// batches to the index-aligned sender. Any segment failure marks the run
/// fatal (non-zero exit), exactly like list mode.
pub async fn diff_list_side_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    concurrency: usize,
    boundaries: &[String],
    senders: Vec<tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>>,
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

    loop {
        while set.len() < concurrency {
            let Some(pair) = hints.next() else { break };
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

        if set.is_empty() {
            break;
        }
        match set.join_next().await {
            Some(Ok(_completed)) => {}
            Some(Err(e)) => error!("Diff segment join error: {:?}", e),
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
