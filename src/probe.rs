//! Partitioning probes (startup discovery, bisection, runtime split probes):
//! one bounded ListObjectsV2 request each.
//!
//! Probes take their slot from their side's request window (`crate::window`),
//! like listing pages, so they count against the same budget and feed the
//! same signals. A probe is optional work: it gives up after a few typical
//! latencies (`Window::probe_deadline`) instead of holding a whole round of
//! discovery back, and its caller goes on without it. A throttled probe is
//! retried once after a short pause; any other failure is returned (callers
//! treat a failed probe as "no cut here", which only costs parallelism,
//! never correctness).

use crate::window::{Kind, Outcome};
use aws_sdk_s3::error::ProvideErrorMetadata;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// Pause before retrying a throttled probe: as short as a throttled page's
/// first retry, so a round of discovery does not stall on its throttled
/// probes (the window has already shrunk for them).
const THROTTLE_RETRY_DELAY: Duration = Duration::from_millis(100);

static TRACE: OnceLock<Arc<dyn crate::trace::S3TraceWriter>> = OnceLock::new();

/// Record probes in the run's trace (`--trace`) as `Probe` events.
pub fn set_trace(writer: Arc<dyn crate::trace::S3TraceWriter>) {
    let _ = TRACE.set(writer);
}

fn trace(latency: Duration, status: u16, error: Option<String>) {
    if let Some(writer) = TRACE.get() {
        let mut event = crate::trace::S3CompatEvent::new("Probe", "", "", "");
        event.addressing_style.clear();
        event.latency_ms = latency.as_millis() as u64;
        event.http_status = status;
        event.s3_error_code = error;
        writer.write_event(event);
    }
}

fn is_throttle<E: ProvideErrorMetadata, R>(
    err: &aws_smithy_runtime_api::client::result::SdkError<E, R>,
) -> bool {
    matches!(
        err.code(),
        Some("SlowDown" | "TooManyRequests" | "ThrottlingException" | "RequestLimitExceeded")
    )
}

/// Send one probe request (`make` builds and sends it) under the shared
/// limit, with a `timeout_secs` watchdog and one retry when throttled.
pub async fn send<T, E, F, Fut>(
    window: &crate::window::Window,
    timeout_secs: u64,
    mut make: F,
) -> Result<T, String>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    F: FnMut() -> Fut,
    Fut: Future<
        Output = Result<
            T,
            aws_smithy_runtime_api::client::result::SdkError<
                E,
                aws_smithy_runtime_api::client::orchestrator::HttpResponse,
            >,
        >,
    >,
{
    let floor = Duration::from_secs(timeout_secs.max(1));
    for attempt in 0..2 {
        let permit = window.acquire(Kind::Probe).await;
        let deadline = window.probe_deadline(floor);
        let timeout = window.timeout(floor);
        let started = Instant::now();
        let result = tokio::time::timeout(deadline, make()).await;
        let latency = started.elapsed();
        match result {
            Err(_) => {
                // Past the probe deadline: a congestion signal only when it
                // was the full request timeout too.
                permit.finish(if deadline >= timeout {
                    Outcome::TimedOut
                } else {
                    Outcome::Abandoned
                });
                trace(latency, 0, Some("ProbeDeadline".into()));
                return Err("probe gave up waiting".to_string());
            }
            Ok(Ok(value)) => {
                permit.finish(Outcome::Answered(latency));
                trace(latency, 200, None);
                return Ok(value);
            }
            Ok(Err(err)) if is_throttle(&err) => {
                permit.finish(Outcome::Throttled);
                trace(latency, 503, err.code().map(str::to_string));
                if attempt == 0 {
                    tokio::time::sleep(THROTTLE_RETRY_DELAY).await;
                    continue;
                }
                return Err(crate::error::concise_sdk_error(&err));
            }
            Ok(Err(err)) => {
                permit.finish(Outcome::Answered(latency));
                trace(latency, 0, err.code().map(str::to_string));
                return Err(crate::error::concise_sdk_error(&err));
            }
        }
    }
    Err("probe throttled".to_string())
}
