use crate::core;
use crate::core::{KeySpaceHints, ObjectKey, ObjectProps, S3TaskContext};
use crate::error::*;
use crate::trace::S3CompatEvent;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use log::{debug, error, info};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{timeout_at, Instant};

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

    let mut set = tokio::task::JoinSet::new();
    let mut last_ts = epoch_secs();

    loop {
        // Fill up to concurrency limit.
        while set.len() < flat_concurrency {
            if let Some(pair) = hints.next() {
                let task_ctx = ctx.clone();
                let start_prefix = start_prefix.to_string();
                let index = pair.index;
                let start = pair.start;
                let end = pair.end;

                set.spawn(async move {
                    let end_ref: Option<&str> = end.as_deref();
                    flat_list_run_to_complete(&task_ctx, &start_prefix, &start, end_ref).await;
                    index
                });
            } else {
                break;
            }
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

        // Await-driven polling: wait for the next segment to complete,
        // with a heartbeat timeout so logs stay fresh.
        let heartbeat_dur = Duration::from_secs(core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS);
        match tokio::time::timeout(heartbeat_dur, set.join_next()).await {
            Ok(Some(Ok(index))) => {
                hints.finish(index);
                ctx.checkpoint_completed.lock().unwrap().push(index);
            }
            Ok(Some(Err(e))) => {
                error!("Task join error: {:?}", e);
            }
            Ok(None) => {
                // All tasks drained — should not happen while !set.is_empty().
                break;
            }
            Err(_elapsed) => {
                // Timeout — emit heartbeat.
                let now = epoch_secs();
                if now - last_ts >= core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
                    info!(
                        "Flat List S3 Task — {} — heartbeat, {} segments in-flight, {} done, {} remaining",
                        ctx.s3_bucket_name,
                        set.len(),
                        hints.done_count(),
                        hints.len(),
                    );
                    last_ts = now;
                }
            }
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

// ── Run one segment to completion (with retry) ─────────────

async fn flat_list_run_to_complete(
    ctx: &S3TaskContext,
    prefix: &str,
    start: &str,
    until: Option<&str>,
) {
    // If the CLI provided --start-after, it overrides the segment's start.
    let mut start_after = ctx.start_after.as_deref().unwrap_or(start).to_string();
    let mut retry_attempt: u32 = 0;
    loop {
        match flat_list(ctx, prefix, &start_after, until, retry_attempt).await {
            Ok(()) => return,
            Err(err) => {
                let next_retry_attempt = retry_attempt.saturating_add(1);
                if err.continue_on_error() && next_retry_attempt < ctx.max_attempts {
                    start_after = err.next_start_owned();
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
                if ctx.is_running() {
                    ctx.complete();
                }
                return;
            }
        }
    }
}

// ── Single ListObjectsV2 paginator call ────────────────────

async fn flat_list(
    ctx: &S3TaskContext,
    prefix: &str,
    start_after: &str,
    until: Option<&str>,
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

    if let Some(ref delim) = ctx.delimiter {
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
                    None,
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
                    None,
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

                // Emit trace event for this page.
                emit_trace_compat(
                    ctx,
                    "ListObjectsV2",
                    prefix,
                    start_after,
                    next_token.as_deref(),
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
                    false,
                    false,
                    None,
                );

                let mut batch: Vec<(ObjectKey, ObjectProps)> = Vec::new();
                let mut remaining_keys = objects.key_count().unwrap_or(0) as usize;

                for obj in objects.contents() {
                    let obj_key = match obj.key() {
                        Some(k) => k,
                        None => {
                            remaining_keys = remaining_keys.saturating_sub(1);
                            continue;
                        }
                    };

                    // Check segment boundary.
                    if let Some(end) = until {
                        if end <= obj_key {
                            debug!("Segment boundary reached at key: {}", obj_key);
                            is_ended = true;
                            break;
                        }
                    }

                    // Remember last key for resume-on-error.
                    if remaining_keys == 1 {
                        next_start = obj_key.to_string();
                    }

                    let key: ObjectKey = obj_key.into();
                    let mut props: ObjectProps = obj.into();
                    props.set_dir(ctx.dir);
                    batch.push((key, props));

                    remaining_keys = remaining_keys.saturating_sub(1);
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

    debug!(
        "Segment complete: start={}, end={:?}, pages={}",
        start_after, until, page_count
    );
    Ok(())
}

// ── Trace event emission ───────────────────────────────────

fn emit_trace_compat(
    ctx: &S3TaskContext,
    operation: &str,
    prefix: &str,
    start_after: &str,
    _next_continuation_token: Option<&str>,
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
                None,
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
                None,
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
                None,
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
