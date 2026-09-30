//! One segment's ListObjectsV2 continuation chain, run to completion with
//! retry.

use super::sdk_error::{TracedRequest, handle_sdk_error, write_trace};
use super::split::{SegmentControl, SplitSender};
use crate::core;
use crate::core::{ObjectKey, ObjectProps, S3TaskContext};
use crate::error::*;
use crate::list_page::{FastContentsInterceptor, ParsedPageSlot};
use crate::trace::S3CompatEvent;
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

/// Largest exponent applied to `initial_backoff_secs` between consecutive
/// retries of one segment.
const RETRY_BACKOFF_MAX_SHIFT: u32 = 5;
/// Ceiling on a single inter-retry pause.
const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(30);

// ── Run one segment to completion (with retry) ─────────────

pub(super) async fn flat_list_run_to_complete(
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

/// Resolves once the run has been asked to stop (Ctrl-C / SIGTERM).
pub(super) async fn quit_signalled(ctx: &S3TaskContext) {
    while !ctx.is_quit() {
        tokio::time::sleep(Duration::from_millis(100)).await;
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
