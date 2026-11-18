//! Startup partitioning of a listing: the S3 side of `auto_hints`.
//!
//! `auto_hints` decides where to probe and how to turn the answers into
//! key-space boundaries; this module issues the probes (one ListObjectsV2
//! page each) and races the whole discovery against Ctrl-C.

use crate::auto_hints;
use crate::config::EngineConfig;
use crate::core;
use crate::window::Window;
use log::info;
use std::sync::Arc;

/// Startup discovery's targets for a side with `slots` concurrent segments:
/// structural boundaries (about two segments per slot: CommonPrefixes come
/// with the probed pages, and uneven directories leave some short), and flat
/// boundaries (one segment per slot: one more would wait for a slot and then
/// list its whole range alone at the end; runtime splitting covers skew).
pub fn startup_targets(slots: usize) -> (usize, usize) {
    (
        slots.saturating_mul(2).clamp(16, 512),
        slots.saturating_sub(1).clamp(1, 255),
    )
}

/// Resolves once the run has been asked to stop (Ctrl-C / SIGTERM set the
/// quit flag). Startup discovery races against it: its probe rounds run
/// before any segment task exists, so nothing else would notice the signal.
pub async fn quit_requested(g_state: &core::GlobalState) {
    g_state.quit_signalled().await
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
    cfg: &EngineConfig,
    no_auto_hints: bool,
    delimiter: &str,
    max_keys: Option<i32>,
    sdk_config: &aws_config::SdkConfig,
    g_state: &core::GlobalState,
    window: &Arc<Window>,
    early: &crate::tasks_s3::EarlyBoundaries,
) -> Vec<String> {
    if no_auto_hints || !delimiter.is_empty() {
        return Vec::new();
    }

    let client = core::build_s3_client(sdk_config, region, endpoint, cfg.s3.force_path_style());
    // A side runs at most `DIFF_SIDE_MAX_CONCURRENCY` segments at once.
    let (structural, flat) = startup_targets(
        cfg.runtime
            .max_concurrency
            .clamp(1, crate::tasks_s3::DIFF_SIDE_MAX_CONCURRENCY),
    );
    startup_boundaries(
        &client,
        bucket,
        prefix,
        cfg,
        max_keys,
        g_state,
        window,
        Some(early),
        structural,
        flat,
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
    cfg: &EngineConfig,
    max_keys: Option<i32>,
    g_state: &core::GlobalState,
    window: &Arc<Window>,
    early: Option<&crate::tasks_s3::EarlyBoundaries>,
    structural_target: usize,
    flat_target: usize,
) -> Option<Vec<String>> {
    let timeout_secs = cfg.s3.operation_timeout_secs;
    // Each round's boundaries so far go to the listing at once (see
    // `LateBoundaries`); the full set is the return value.
    let emit = |bounds: &[String]| {
        if let Some(tx) = early
            && !bounds.is_empty()
        {
            let _ = tx.send(bounds.to_vec());
        }
    };
    tokio::select! {
        boundaries = async {
            let discovery = auto_hints::discover_startup_boundaries(
                client,
                bucket,
                prefix,
                structural_target,
                timeout_secs,
                window,
                &emit,
            )
            .await;
            if !discovery.boundaries.is_empty() {
                let tails = discovery.tails.clone();
                // Flat runs share the structural budget (about two segments
                // per slot), not the flat one (one per slot): structure
                // already used part of that, and 20 equal directories with 11
                // cuts between them left nine 50-page segments listing
                // serially while the rest of the run sat idle. Every run gets
                // at least one cut for the same reason. (One segment per slot
                // was measured: a diff, which runs at most 32 segments a side,
                // took twice as long on its larger, later segments.)
                let refine_target = structural_target
                    .max(flat_target)
                    .max(discovery.boundaries.len() + discovery.flat_runs.len());
                let boundaries =
                    refine_flat_leaves(client, bucket, window, discovery, refine_target, timeout_secs)
                        .await;
                if !tails.is_empty() {
                    emit(&boundaries);
                }
                bisect_tails(client, bucket, window, boundaries, &tails, flat_target, timeout_secs).await
            } else if discovery.is_single_page_listing(max_keys) {
                // The probe's page was not truncated and held no
                // CommonPrefixes, and this run's page size returns those keys
                // in one request too: bisecting would cost more requests than
                // the listing.
                info!("Startup discovery found a single-page listing — using single-segment listing");
                Vec::new()
            } else {
                info!("Startup discovery found no prefix structure — bisecting flat key space");
                match (discovery.root_first_key, discovery.root_last_key) {
                    // The root page is a page of the flat run the whole
                    // listing is: size and cut it in two rounds.
                    (Some(first_key), Some(last_key)) if discovery.root_page_keys >= 2 => {
                        let run = auto_hints::FlatRun {
                            prefix: prefix.to_string(),
                            first_key,
                            last_key,
                            keys: discovery.root_page_keys,
                        };
                        auto_hints::partition_flat_runs(&[run], flat_target, |p, start_after| {
                            single_key_probe(client, bucket, window, p, start_after, timeout_secs)
                        })
                        .await
                    }
                    (first, _) => {
                        discover_flat_boundaries_via_client(
                            client, bucket, window, prefix, flat_target, first, timeout_secs,
                        )
                        .await
                    }
                }
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
    window: &Arc<Window>,
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
            single_key_probe(client, bucket, window, prefix, start_after, timeout_secs)
        })
        .await,
    );
    boundaries.sort();
    boundaries.dedup();
    boundaries
}

/// Most tail runs bisected at startup (spread over those found).
const MAX_TAIL_RUNS: usize = 16;

/// Partition the tails of truncated delimiter probes: the key space after
/// the last CommonPrefix a probe saw would otherwise be one segment however
/// large (a bucket with 100,000 top-level prefixes listed as 1,000 segments
/// and one segment for the other 99%).  Each tail is bisected like a flat
/// namespace, anchored on its last seen entry, with an even share of
/// `flat_target`; the tails run concurrently.
pub async fn bisect_tails(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    window: &Arc<Window>,
    mut boundaries: Vec<String>,
    tails: &[auto_hints::TailRun],
    flat_target: usize,
    timeout_secs: u64,
) -> Vec<String> {
    if tails.is_empty() {
        return boundaries;
    }
    let step = tails.len().div_ceil(MAX_TAIL_RUNS);
    let tails: Vec<&auto_hints::TailRun> = tails.iter().step_by(step).collect();
    let share = (flat_target / tails.len()).max(1);
    info!(
        "Startup discovery: {} truncated prefix listing(s) — bisecting their tails",
        tails.len()
    );
    let found = futures::future::join_all(tails.iter().map(|tail| {
        auto_hints::discover_flat_boundaries(
            &tail.prefix,
            share,
            Some(tail.after.clone()),
            |start_after| {
                single_key_probe(
                    client,
                    bucket,
                    window,
                    tail.prefix.clone(),
                    start_after,
                    timeout_secs,
                )
            },
        )
    }))
    .await;
    boundaries.extend(found.into_iter().flatten());
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
    window: &Arc<Window>,
    prefix: &str,
    target: usize,
    first_key: Option<String>,
    timeout_secs: u64,
) -> Vec<String> {
    auto_hints::discover_flat_boundaries(prefix, target, first_key, |start_after| {
        single_key_probe(
            client,
            bucket,
            window,
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
    window: &Arc<Window>,
    prefix: String,
    start_after: Option<String>,
    timeout_secs: u64,
) -> impl std::future::Future<Output = Result<Option<String>, String>> + use<> {
    let client = client.clone();
    let bucket = bucket.to_string();
    let window = Arc::clone(window);
    async move {
        let resp = crate::probe::send(&window, timeout_secs, || {
            let mut req = client
                .list_objects_v2()
                .encoding_type(aws_sdk_s3::types::EncodingType::Url)
                .bucket(&bucket)
                .prefix(&prefix)
                .max_keys(1);
            if let Some(sa) = &start_after {
                req = req.start_after(sa);
            }
            req.send()
        })
        .await?;
        let url = resp.encoding_type() == Some(&aws_sdk_s3::types::EncodingType::Url);
        Ok(resp
            .contents()
            .first()
            .and_then(|o| o.key())
            .map(|k| crate::list_page::response_key(url, k)))
    }
}
