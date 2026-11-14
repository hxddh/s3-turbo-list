//! One segment's ListObjectsV2 continuation chain, run to completion with
//! retry.

use super::sdk_error::{TracedRequest, handle_sdk_error, write_trace};
use super::split::{SegmentControl, SplitSender};
use crate::core;
use crate::core::{ObjectKey, ObjectProps, S3TaskContext};
use crate::error::*;
use crate::list_page::{FastContentsInterceptor, ParsedPageSlot, url_decode_key};
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
    let mut start_after = start.to_string();
    let mut retry_attempt: u32 = 0;
    loop {
        let attempt = if ctx.versions {
            versions_list(ctx, prefix, &start_after, control, split_tx, retry_attempt).await
        } else {
            flat_list(
                ctx,
                segment_index,
                prefix,
                &start_after,
                control,
                split_tx,
                retry_attempt,
            )
            .await
        };
        match attempt {
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
    ctx.g_state.quit_signalled().await
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
    // `encoding-type=url`: keys come back percent-encoded, so a key holding
    // a character XML 1.0 cannot carry (U+0001..U+001F) still lists instead
    // of failing every page that contains it. Keys are decoded below
    // (`decode_url_keys`); an endpoint that ignores the parameter answers
    // without `<EncodingType>` and is read as-is.
    let mut request = ctx
        .s3_client
        .list_objects_v2()
        .bucket(&ctx.s3_bucket_name)
        .prefix(prefix)
        .encoding_type(aws_sdk_s3::types::EncodingType::Url);

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
    let mut is_ended;
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
                return handle_sdk_error(sdk_err, &next_start, ctx, traced);
            }
            Ok(Ok(response)) => {
                let mut objects = response;
                // Rows the fast path parsed; `None` means this page fell back
                // to the SDK's own `Contents`.
                let mut parsed = parsed_slot.take();
                let url_encoded =
                    objects.encoding_type() == Some(&aws_sdk_s3::types::EncodingType::Url);
                if url_encoded {
                    decode_page_keys(&mut objects, parsed.as_mut());
                }

                // Extract pagination metadata for trace.
                let is_truncated_reported = objects.is_truncated();
                let is_truncated = is_truncated_reported.unwrap_or(false);
                let next_token = objects.next_continuation_token().map(|t| t.to_string());
                let key_count_opt = objects.key_count();
                let cp_count = objects.common_prefixes().len() as i32;
                common_prefixes_count =
                    common_prefixes_count.saturating_add(objects.common_prefixes().len());

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
                let batch: Vec<(ObjectKey, ObjectProps)> = match parsed {
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

                let prefixes: Vec<String> = objects
                    .common_prefixes
                    .take()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|cp| cp.prefix)
                    .collect();
                let shaped = shape_page(
                    batch,
                    prefixes,
                    &mut next_start,
                    start_after,
                    until.as_deref(),
                    ctx.dir,
                    emit_common_prefixes,
                )
                .map_err(|violation| {
                    FlatRuntimeError::new(
                        ERROR_INCONSISTENT_LISTING,
                        format!(
                            "ListObjectsV2 returned keys out of order ({}); the endpoint \
                             breaks the S3 listing contract",
                            violation
                        ),
                        next_start.clone(),
                    )
                })?;
                let batch = shaped.batch;
                is_ended = shaped.ended;
                object_count = object_count.saturating_add(shaped.objects);
                control.record_page(&next_start);

                // A split proposal is decided here, at a page boundary,
                // against the authoritative cursor. A segment that already
                // crossed its boundary must not accept one: S3 order means
                // no keys remain in its range, so the child would be an
                // empty segment (a wasted request).
                if let (Some(tx), false) = (split_tx, is_ended || ctx.is_quit()) {
                    if let Some(mut child) = control.try_accept_split() {
                        child.parent = segment_index;
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
                // token continues the chain unless the page said
                // IsTruncated=false (an endpoint that hands out a token on
                // every final page would otherwise page forever).  Without
                // one the listing ends — unless
                // the endpoint said there is more: IsTruncated=true with no
                // usable token, or the token it was just sent handed back
                // (not explicitly IsTruncated=false), is a truncated page that
                // cannot be followed.  That must not pass for completion: fail
                // the attempt at the last key seen, so the retry loop resumes
                // with start-after — refunding the budget while it advances,
                // failing the run if the endpoint is stuck.
                let next_token = next_token.filter(|token| !token.is_empty());
                let repeated = next_token.is_some() && next_token == page_token;
                if !repeated && next_token.is_some() && is_truncated_reported != Some(false) {
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

// ── ListObjectVersions chain (`list --versions`) ───────────

/// Merge a page's versions and delete markers (two lists, each row with its
/// LastModified in ms): by key, newest first within a key; on a timestamp
/// tie the latest version (e.g. a delete marker written in the same
/// millisecond) goes first.
fn order_versions(rows: &mut [(ObjectKey, ObjectProps, i64)]) {
    rows.sort_by(|a, b| {
        a.0.as_str()
            .cmp(b.0.as_str())
            .then(b.2.cmp(&a.2))
            .then(b.1.is_latest().cmp(&a.1.is_latest()))
    });
}

/// One segment of a `--versions` listing: every version and delete marker
/// of the keys in `(start_after, end]`, newest version of a key first.
///
/// Pages follow `NextKeyMarker` / `NextVersionIdMarker`. A key's versions
/// can span pages, so the trailing key of a truncated page is held back
/// until the next page shows it ended: every batch sent ends on a key
/// boundary, and the resume cursor (the last key sent) never splits a key's
/// versions — a retry or `--resume` from it lists the next key whole.
async fn versions_list(
    ctx: &S3TaskContext,
    prefix: &str,
    start_after: &str,
    control: &SegmentControl,
    split_tx: Option<&SplitSender>,
    retry_attempt: u32,
) -> Result<(), FlatRuntimeError> {
    let mut next_start = start_after.to_string();
    let mut key_marker = start_after.to_string();
    let mut version_marker: Option<String> = None;
    let mut held: Vec<(ObjectKey, ObjectProps)> = Vec::new();
    loop {
        let mut request = ctx
            .s3_client
            .list_object_versions()
            .bucket(&ctx.s3_bucket_name)
            .prefix(prefix)
            .encoding_type(aws_sdk_s3::types::EncodingType::Url);
        if !key_marker.is_empty() {
            request = request.key_marker(&key_marker);
        }
        if let Some(v) = &version_marker {
            request = request.version_id_marker(v);
        }
        if let Some(mk) = ctx.max_keys {
            request = request.max_keys(mk);
        }
        let page_start = Instant::now();
        let res = timeout_at(
            Instant::now() + Duration::from_secs(ctx.operation_timeout_secs),
            request.send(),
        )
        .await;
        let traced = TracedRequest {
            prefix,
            start_after,
            continuation_token: version_marker.as_deref(),
            retry_attempt,
            latency_ms: page_start.elapsed().as_millis() as u64,
        };
        let page = match res {
            Err(_) => {
                ctx.g_state.inc_task_next_stream_timeout();
                return Err(FlatRuntimeError::new(
                    ERROR_S3_NEXT_STREAM_TIMEOUT,
                    "ListObjectVersions stream timeout".into(),
                    next_start,
                ));
            }
            Ok(Err(sdk_err)) => {
                return handle_sdk_error(sdk_err, &next_start, ctx, traced);
            }
            Ok(Ok(page)) => page,
        };
        let url = page.encoding_type() == Some(&aws_sdk_s3::types::EncodingType::Url);
        let decode = |k: &str| crate::list_page::response_key(url, k);
        let truncated = page.is_truncated() == Some(true);
        write_trace(
            ctx,
            traced.event(ctx, 200).map(|mut event| {
                event.is_truncated = truncated;
                event.contents_count =
                    Some((page.versions().len() + page.delete_markers().len()) as i32);
                event
            }),
        );

        let mut rows: Vec<(ObjectKey, ObjectProps, i64)> = Vec::new();
        for v in page.versions() {
            let Some(key) = v.key() else { continue };
            let mut props =
                ObjectProps::new_open(0, v.size().map_or(0, |s| s.max(0) as u64), [0; 16]);
            if let Some(etag) = v.e_tag() {
                props.set_etag(etag);
            }
            let ms = v
                .last_modified()
                .and_then(|t| t.to_millis().ok())
                .unwrap_or(0);
            props.set_last_modified_millis(core::epoch_millis_u64(ms));
            if let Some(class) = v.storage_class() {
                props.set_storage_class(class.as_str());
            }
            props.set_version(
                v.version_id().map(str::to_string),
                v.is_latest() == Some(true),
                false,
            );
            rows.push((ObjectKey::from(decode(key)), props, ms));
        }
        for m in page.delete_markers() {
            let Some(key) = m.key() else { continue };
            let mut props = ObjectProps::new_open(0, 0, [0; 16]);
            let ms = m
                .last_modified()
                .and_then(|t| t.to_millis().ok())
                .unwrap_or(0);
            props.set_last_modified_millis(core::epoch_millis_u64(ms));
            props.set_version(
                m.version_id().map(str::to_string),
                m.is_latest() == Some(true),
                true,
            );
            rows.push((ObjectKey::from(decode(key)), props, ms));
        }
        order_versions(&mut rows);
        let mut rows: Vec<(ObjectKey, ObjectProps)> =
            rows.into_iter().map(|(k, p, _)| (k, p)).collect();

        // Keys never go backwards, and a fresh key marker means keys after it.
        let floor = held.last().map_or(next_start.as_str(), |(k, _)| k.as_str());
        let fresh = version_marker.is_none() && held.is_empty();
        if let Some((first, _)) = rows.first()
            && (first.as_str() < floor || (fresh && first.as_str() == floor && !floor.is_empty()))
        {
            return Err(FlatRuntimeError::new(
                ERROR_INCONSISTENT_LISTING,
                format!(
                    "ListObjectVersions returned keys out of order ('{}' after '{}')",
                    first, floor
                ),
                next_start,
            ));
        }
        if rows.windows(2).any(|w| w[1].0.as_str() < w[0].0.as_str()) {
            return Err(FlatRuntimeError::new(
                ERROR_INCONSISTENT_LISTING,
                "ListObjectVersions returned keys out of order within a page".into(),
                next_start,
            ));
        }

        let mut batch = std::mem::take(&mut held);
        batch.append(&mut rows);
        let until = control.current_end();
        let mut ended = false;
        if let Some(end) = until.as_deref()
            && let Some(cut) = batch.iter().position(|(k, _)| end < k.as_str())
        {
            batch.truncate(cut);
            ended = true;
        }
        if truncated
            && !ended
            && let Some((last, _)) = batch.last()
        {
            let last = last.as_str().to_string();
            let split_at = batch
                .iter()
                .rposition(|(k, _)| k.as_str() != last)
                .map_or(0, |i| i + 1);
            held = batch.split_off(split_at);
        }
        for (_, props) in batch.iter_mut() {
            props.set_dir(ctx.dir);
        }
        for (_, props) in held.iter_mut() {
            props.set_dir(ctx.dir);
        }
        // Splits may cut only past every key seen, held ones included.
        let seen = held
            .last()
            .or(batch.last())
            .map_or(next_start.clone(), |(k, _)| k.as_str().to_string());
        if let Some((key, _)) = batch.last() {
            next_start = key.as_str().to_string();
        }
        control.record_page(&seen);
        if let (Some(tx), false) = (split_tx, ended || ctx.is_quit()) {
            if let Some(mut child) = control.try_accept_split() {
                child.parent = 0;
                let _ = tx.send(child);
            }
        }
        if !batch.is_empty() {
            if let Err(e) = ctx.data_map_channel.send(batch).await {
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
        if ended || !truncated {
            return Ok(());
        }
        key_marker = page.next_key_marker().map(decode).unwrap_or_default();
        version_marker = page.next_version_id_marker().map(str::to_string);
        if key_marker.is_empty() {
            return Err(FlatRuntimeError::new(
                ERROR_S3_CLIENT_GENERIC,
                "truncated ListObjectVersions page without NextKeyMarker".into(),
                next_start,
            ));
        }
    }
}

/// What one page contributes to its segment.
#[derive(Debug)]
struct ShapedPage {
    /// The rows to send: objects (and, for a `--delimiter` list, folder
    /// rows) in key order, cut at the segment's end.
    batch: Vec<(ObjectKey, ObjectProps)>,
    /// The page reached past the segment's end: the segment is complete.
    ended: bool,
    /// Objects among the rows (folder rows excluded).
    objects: usize,
}

/// Shape one page of a segment — no I/O, so every rule here is unit-tested
/// directly:
///
/// - keys must ascend, after `cursor` (the page's request position);
/// - segment ranges are `(start_after, until]`: the first key past `until`
///   ends the segment (a key equal to it belongs here, not to the next);
/// - a `--delimiter` list emits each CommonPrefix as a folder row merged
///   into key order — except the one equal to `start_after`, which a chain
///   restarted at a CommonPrefix gets back and already emitted;
/// - `cursor` advances to the last row, and over trailing CommonPrefixes
///   (a page can hold only prefixes; a retry must not re-list them).
fn shape_page(
    mut batch: Vec<(ObjectKey, ObjectProps)>,
    prefixes: Vec<String>,
    cursor: &mut String,
    start_after: &str,
    until: Option<&str>,
    dir: u8,
    emit_common_prefixes: bool,
) -> Result<ShapedPage, String> {
    if let Some(violation) = order_violation(&batch, cursor) {
        return Err(violation);
    }
    let mut ended = false;
    if let Some(end) = until
        && let Some(cut) = batch.iter().position(|(key, _)| end < key.as_str())
    {
        debug!("Segment boundary reached at key: {}", batch[cut].0);
        batch.truncate(cut);
        ended = true;
    }
    for (_, props) in batch.iter_mut() {
        props.set_dir(dir);
    }
    let objects = batch.len();
    let last_prefix = prefixes.last().cloned();
    if emit_common_prefixes {
        let folders: Vec<(ObjectKey, ObjectProps)> = prefixes
            .into_iter()
            .filter(|cp| cp.as_str() != start_after)
            .filter(|cp| !ended && until.is_none_or(|end| cp.as_str() <= end))
            .map(|cp| (cp.into(), ObjectProps::new_common_prefix(dir)))
            .collect();
        if !folders.is_empty() {
            batch = merge_by_key(std::mem::take(&mut batch), folders);
        }
    }
    if let Some((key, _)) = batch.last() {
        cursor.clear();
        cursor.push_str(key.as_str());
    }
    // Safe: a prefix sorts before every key it covers, and its keys are
    // rolled up into it (never emitted), so nothing is skipped.
    if !ended
        && let Some(cp) = last_prefix
        && cp.as_str() > cursor.as_str()
    {
        *cursor = cp;
    }
    Ok(ShapedPage {
        batch,
        ended,
        objects,
    })
}

/// The first ordering violation of a page: a key not after the cursor the
/// page was requested from, or not after its predecessor.
fn order_violation(batch: &[(ObjectKey, ObjectProps)], cursor: &str) -> Option<String> {
    let mut prev = cursor;
    for (key, _) in batch {
        if key.as_str() <= prev {
            return Some(format!("'{}' after '{}'", key.as_str(), prev));
        }
        prev = key.as_str();
    }
    None
}

/// Decode the `encoding-type=url` fields the run reads: object keys (fast
/// path rows or SDK contents) and CommonPrefixes.
fn decode_page_keys(
    objects: &mut aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
    parsed: Option<&mut Vec<(ObjectKey, ObjectProps)>>,
) {
    match parsed {
        Some(rows) => {
            for (key, _) in rows.iter_mut() {
                if let Some(decoded) = url_decode_key(key.as_str()) {
                    *key = ObjectKey::from(decoded);
                }
            }
        }
        None => {
            for object in objects.contents.iter_mut().flatten() {
                if let Some(decoded) = object.key.as_deref().and_then(url_decode_key) {
                    object.key = Some(decoded);
                }
            }
        }
    }
    for cp in objects.common_prefixes.iter_mut().flatten() {
        if let Some(decoded) = cp.prefix.as_deref().and_then(url_decode_key) {
            cp.prefix = Some(decoded);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(keys: &[&str]) -> Vec<(ObjectKey, ObjectProps)> {
        keys.iter()
            .map(|k| (ObjectKey::from(*k), ObjectProps::default()))
            .collect()
    }

    #[test]
    fn versions_order_newest_first_and_latest_wins_a_tie() {
        let row = |key: &str, id: &str, ms: i64, latest: bool, marker: bool| {
            let mut props = ObjectProps::default();
            props.set_version(Some(id.to_string()), latest, marker);
            (ObjectKey::from(key), props, ms)
        };
        // As parsed: every version first, then every delete marker.
        let mut rows = vec![
            row("a", "a-old", 100, false, false),
            row("b", "b-1", 50, false, false),
            row("a", "a-new", 200, false, false),
            row("a", "a-dm", 200, true, true),
        ];
        order_versions(&mut rows);
        let ids: Vec<_> = rows.iter().map(|r| r.1.version_id().unwrap()).collect();
        assert_eq!(ids, ["a-dm", "a-new", "a-old", "b-1"]);
    }

    fn keys(page: &ShapedPage) -> Vec<&str> {
        page.batch.iter().map(|(k, _)| k.as_str()).collect()
    }

    #[test]
    fn shape_page_cuts_after_the_segment_end_inclusively() {
        let mut cursor = String::new();
        let page = shape_page(
            rows(&["a", "b", "c"]),
            vec![],
            &mut cursor,
            "",
            Some("b"),
            0,
            false,
        )
        .unwrap();
        assert_eq!(
            keys(&page),
            ["a", "b"],
            "a key equal to the end belongs here"
        );
        assert!(page.ended);
        assert_eq!(cursor, "b");
    }

    #[test]
    fn shape_page_rejects_keys_not_after_the_cursor() {
        let mut cursor = "b".to_string();
        assert!(shape_page(rows(&["b", "c"]), vec![], &mut cursor, "", None, 0, false).is_err());
        assert_eq!(cursor, "b", "a rejected page does not move the cursor");
    }

    #[test]
    fn shape_page_merges_folders_and_skips_the_restart_prefix() {
        let mut cursor = "a/".to_string();
        let page = shape_page(
            rows(&["b.txt"]),
            vec!["a/".into(), "c/".into()],
            &mut cursor,
            "a/",
            None,
            0,
            true,
        )
        .unwrap();
        assert_eq!(keys(&page), ["b.txt", "c/"]);
        assert_eq!(page.objects, 1);
        assert_eq!(cursor, "c/");
    }

    #[test]
    fn shape_page_advances_over_a_page_of_only_prefixes() {
        let mut cursor = String::new();
        let page = shape_page(
            vec![],
            vec!["x/".into(), "y/".into()],
            &mut cursor,
            "",
            None,
            0,
            false,
        )
        .unwrap();
        assert!(
            page.batch.is_empty(),
            "diff and recursive lists emit no folder rows"
        );
        assert_eq!(cursor, "y/");
    }

    #[test]
    fn url_decode_key_follows_form_encoding() {
        assert_eq!(url_decode_key("plain/key.txt"), None);
        assert_eq!(url_decode_key("a+b").as_deref(), Some("a b"));
        assert_eq!(url_decode_key("a%2Bb").as_deref(), Some("a+b"));
        assert_eq!(url_decode_key("ctl%01x").as_deref(), Some("ctl\u{1}x"));
        assert_eq!(url_decode_key("%E4%B8%AD").as_deref(), Some("中"));
        assert_eq!(url_decode_key("100%").as_deref(), Some("100%"));
        assert_eq!(url_decode_key("%zz%4").as_deref(), Some("%zz%4"));
        assert_eq!(url_decode_key("%FF"), None, "not UTF-8: kept as sent");
    }

    #[test]
    fn order_violation_checks_cursor_and_succession() {
        let rows = |keys: &[&str]| -> Vec<(ObjectKey, ObjectProps)> {
            keys.iter()
                .map(|k| (ObjectKey::from(*k), ObjectProps::default()))
                .collect()
        };
        assert_eq!(order_violation(&rows(&["b", "c"]), "a"), None);
        assert_eq!(order_violation(&rows(&["a"]), ""), None);
        assert!(
            order_violation(&rows(&["a"]), "a").is_some(),
            "repeat of the cursor"
        );
        assert!(order_violation(&rows(&["c", "b"]), "a").is_some());
        assert!(order_violation(&rows(&["b", "b"]), "a").is_some());
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
}
