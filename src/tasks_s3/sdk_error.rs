//! SDK error classification and the compat-trace events of a segment's
//! requests.

use crate::core::S3TaskContext;
use crate::error::*;
use crate::trace::S3CompatEvent;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use log::error;

/// One ListObjectsV2 request of a segment's chain, as the compat trace
/// records it.
#[derive(Clone, Copy)]
pub(super) struct TracedRequest<'a> {
    pub(super) prefix: &'a str,
    pub(super) start_after: &'a str,
    /// The continuation token this request sent (none on a chain's first page).
    pub(super) continuation_token: Option<&'a str>,
    pub(super) retry_attempt: u32,
    pub(super) latency_ms: u64,
}

impl TracedRequest<'_> {
    /// This request's trace event with the run-level fields filled in, or
    /// `None` when tracing is off, so nothing is built for nothing.
    pub(super) fn event(&self, ctx: &S3TaskContext, http_status: u16) -> Option<S3CompatEvent> {
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
    pub(super) fn failure(
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

pub(super) fn write_trace(ctx: &S3TaskContext, event: Option<S3CompatEvent>) {
    if let (Some(writer), Some(event)) = (&ctx.trace_writer, event) {
        writer.write_event(event);
    }
}

pub(super) fn handle_sdk_error(
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
