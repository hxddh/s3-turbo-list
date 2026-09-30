//! Startup partitioning of a listing: the S3 side of `auto_hints`.
//!
//! `auto_hints` decides where to probe and how to turn the answers into
//! key-space boundaries; this module issues the probes (one ListObjectsV2
//! page each) and races the whole discovery against Ctrl-C.

use crate::auto_hints;
use crate::config::S3TurboConfig;
use crate::core;
use log::info;

/// Resolves once the run has been asked to stop (Ctrl-C / SIGTERM set the
/// quit flag). Startup discovery races against it: its probe rounds run
/// before any segment task exists, so nothing else would notice the signal.
pub async fn quit_requested(g_state: &core::GlobalState) {
    while !g_state.is_quit() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Key-space boundaries for one diff side from startup discovery, as in
/// list mode (diff takes no --hints-file). Empty means one segment.
///
/// `no_auto_hints`, `delimiter` and `max_keys` are the run's
/// `--no-auto-hints`, `--delimiter` and `--max-keys`.
#[allow(clippy::too_many_arguments)]
pub async fn diff_side_boundaries(
    bucket: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    prefix: &str,
    cfg: &S3TurboConfig,
    no_auto_hints: bool,
    delimiter: &str,
    max_keys: Option<i32>,
    sdk_config: &aws_config::SdkConfig,
    g_state: &core::GlobalState,
) -> Vec<String> {
    if no_auto_hints || !delimiter.is_empty() || cfg.s3.start_after.is_some() {
        return Vec::new();
    }

    let client = core::build_s3_client(sdk_config, region, endpoint, cfg.s3.force_path_style());
    let concurrency = cfg.runtime.max_concurrency;
    // Only `max_concurrency` segments run at once, so spare flat boundaries
    // would cost probes without adding parallelism; diff has no runtime
    // splitting to fall back on, so it keeps a floor of 8.
    startup_boundaries(
        &client,
        bucket,
        prefix,
        cfg,
        max_keys,
        g_state,
        concurrency.saturating_mul(2).clamp(16, 512),
        concurrency.clamp(8, 64),
    )
    .await
    .unwrap_or_default()
}

/// Startup partitioning of one listing: structural discovery (up to
/// `structural_target` CommonPrefix boundaries), flat leaves bisected when
/// that finds too few, and single-key bisection of a flat namespace (up to
/// `flat_target` boundaries). Raced against Ctrl-C / SIGTERM so an interrupt
/// stops it at once: `None` then.
#[allow(clippy::too_many_arguments)]
pub async fn startup_boundaries(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    cfg: &S3TurboConfig,
    max_keys: Option<i32>,
    g_state: &core::GlobalState,
    structural_target: usize,
    flat_target: usize,
) -> Option<Vec<String>> {
    let timeout_secs = cfg.s3.operation_timeout_secs;
    tokio::select! {
        boundaries = async {
            let discovery = auto_hints::discover_startup_boundaries(
                client,
                bucket,
                prefix,
                structural_target,
                timeout_secs,
            )
            .await;
            if !discovery.boundaries.is_empty() {
                refine_flat_leaves(client, bucket, discovery, flat_target, timeout_secs).await
            } else if discovery.is_single_page_listing(max_keys) {
                // The probe's page was not truncated and held no
                // CommonPrefixes, and this run's page size returns those keys
                // in one request too: bisecting would cost more requests than
                // the listing.
                info!("Startup discovery found a single-page listing — using single-segment listing");
                Vec::new()
            } else {
                info!("Startup discovery found no prefix structure — bisecting flat key space");
                let first = discovery.root_first_key;
                discover_flat_boundaries_via_client(client, bucket, prefix, flat_target, first, timeout_secs)
                    .await
            }
        } => Some(boundaries),
        _ = quit_requested(g_state) => None,
    }
}

/// Structural discovery found CommonPrefixes, but fewer boundaries than one
/// per worker, and some of what it probed is a flat run of keys more than a
/// page long — a flat directory (`data/part-…` under a single `data/`), or
/// files next to subdirectories (`obj-…` beside `logs/`): those segments
/// would list serially (diff) or wait for runtime splitting (list).  Bisect
/// the runs with the flat partitioner, sharing the remaining budget by their
/// estimated sizes, and merge their boundaries — all real keys inside each
/// run — into the structural set.
pub async fn refine_flat_leaves(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    discovery: auto_hints::StartupDiscovery,
    flat_target: usize,
    timeout_secs: u64,
) -> Vec<String> {
    let mut boundaries = discovery.boundaries;
    let runs = discovery.flat_runs;
    if runs.is_empty() || boundaries.len() >= flat_target {
        return boundaries;
    }
    info!(
        "Startup discovery found {} boundaries and {} flat run(s) — bisecting them",
        boundaries.len(),
        runs.len()
    );
    let budget = flat_target - boundaries.len();
    boundaries.extend(
        auto_hints::partition_flat_runs(&runs, budget, |prefix, start_after| {
            single_key_probe(client, bucket, prefix, start_after, timeout_secs)
        })
        .await,
    );
    boundaries.sort();
    boundaries.dedup();
    boundaries
}

/// Bisect a flat key range into boundaries via single-key ListObjectsV2
/// probes on the given client. Shared by list-mode startup discovery and the
/// per-side diff hints resolver.
pub async fn discover_flat_boundaries_via_client(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    target: usize,
    first_key: Option<String>,
    timeout_secs: u64,
) -> Vec<String> {
    auto_hints::discover_flat_boundaries(prefix, target, first_key, |start_after| {
        single_key_probe(
            client,
            bucket,
            prefix.to_string(),
            start_after,
            timeout_secs,
        )
    })
    .await
}

/// The first key under `prefix` strictly after `start_after` (the first of
/// all when `None`): one `max-keys=1` ListObjectsV2 request.
fn single_key_probe(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: String,
    start_after: Option<String>,
    timeout_secs: u64,
) -> impl std::future::Future<Output = Result<Option<String>, String>> + use<> {
    let mut req = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(1);
    if let Some(sa) = start_after {
        req = req.start_after(sa);
    }
    async move {
        // Same watchdog the runtime split probes use: startup runs before a
        // single object is listed, so a stalled endpoint must not hang the
        // run there.
        let resp =
            match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), req.send())
                .await
            {
                Ok(result) => result.map_err(|e| crate::error::concise_sdk_error(&e))?,
                Err(_) => return Err("probe timed out".to_string()),
            };
        Ok(resp
            .contents()
            .first()
            .and_then(|o| o.key())
            .map(str::to_string))
    }
}
