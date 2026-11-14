//! Partitioning probes (startup discovery, bisection, runtime split probes):
//! one bounded, watchdogged ListObjectsV2 request each.
//!
//! Probes share one process-wide limit, set from the run's concurrency, so a
//! QPS-limited bucket is not hit with hundreds of probes at startup on top of
//! the listing. A throttled probe is retried once after a short pause; any
//! other failure is returned (callers treat a failed probe as "no cut here",
//! which only costs parallelism, never correctness).

use aws_sdk_s3::error::ProvideErrorMetadata;
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Default limit when the run did not set one (tests, benchmarks).
const DEFAULT_PROBE_CONCURRENCY: usize = 64;
/// Pause before retrying a throttled probe.
const THROTTLE_RETRY_DELAY: Duration = Duration::from_millis(500);

static PROBE_PERMITS: OnceLock<Semaphore> = OnceLock::new();

/// Set the probe concurrency limit (once, before the first probe).
pub fn set_concurrency(limit: usize) {
    let _ = PROBE_PERMITS.set(Semaphore::new(limit.clamp(1, DEFAULT_PROBE_CONCURRENCY)));
}

fn permits() -> &'static Semaphore {
    PROBE_PERMITS.get_or_init(|| Semaphore::new(DEFAULT_PROBE_CONCURRENCY))
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
pub async fn send<T, E, F, Fut>(timeout_secs: u64, mut make: F) -> Result<T, String>
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
    let watchdog = Duration::from_secs(timeout_secs.max(1));
    for attempt in 0..2 {
        let result = {
            let _permit = permits().acquire().await.map_err(|e| e.to_string())?;
            tokio::time::timeout(watchdog, make()).await
        };
        match result {
            Err(_) => return Err("probe timed out".to_string()),
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(err)) if attempt == 0 && is_throttle(&err) => {
                tokio::time::sleep(THROTTLE_RETRY_DELAY).await;
            }
            Ok(Err(err)) => return Err(crate::error::concise_sdk_error(&err)),
        }
    }
    Err("probe throttled".to_string())
}
