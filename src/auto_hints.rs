use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

// ── Cached hints format ────────────────────────────────────

// `#[serde(default)]` on the whole struct (no `deny_unknown_fields`) keeps older
// cache files readable: fields that earlier versions wrote (total_objects,
// scan_mode, estimate_mode, and the long-removed sampled-scan fields) are simply
// ignored on load.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HintsCache {
    pub bucket: String,
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    pub boundaries: Vec<String>,
    pub generated_at: String,
}

// ── Startup structural discovery ───────────────────────────
//
// First-run hints without any user action: a small BFS of delimiter
// probes (one ListObjectsV2 page each) discovers real CommonPrefix
// boundaries so the run starts with parallel segments instead of the
// single-segment fallback. Results are written to the conventional
// hints cache so later runs (including --resume) reload the exact same
// boundaries through the existing cache path.

/// Maximum BFS depth for startup discovery probes.
const STARTUP_DISCOVERY_MAX_DEPTH: usize = 3;
/// Maximum delimiter probes issued per BFS level.
const STARTUP_DISCOVERY_MAX_PROBES_PER_LEVEL: usize = 64;

/// Outcome of startup structural discovery.
pub struct StartupDiscovery {
    /// Sorted boundary list; empty means no structure was found (flat
    /// namespace) and the caller should partition or fall back to one segment.
    pub boundaries: Vec<String>,
    /// Whether the root probe's page was truncated.  An untruncated root page
    /// with no CommonPrefixes means the whole listing came back in that one
    /// page: there is nothing to partition, and probing for cut points would
    /// cost far more requests than the listing itself.
    pub root_page_truncated: bool,
    /// Keys the root probe's page returned.  The probe uses the provider's
    /// default page size, so a run with a smaller `--max-keys` paginates over
    /// the same keys and is *not* single-page — the caller compares this
    /// against its configured page size before taking that shortcut.
    pub root_page_keys: usize,
    /// Flat runs of keys more than a page long that CommonPrefix boundaries
    /// alone cannot split: flat directories below the root (`data/part-…`
    /// under a single top-level `data/`), and files listed next to
    /// subdirectories (`obj-…` beside `logs/`).  The caller bisects them
    /// when discovery found fewer boundaries than it wants.
    pub flat_runs: Vec<FlatRun>,
    /// First key of the root page when it held no CommonPrefixes (a flat
    /// namespace): flat bisection anchors on it instead of spending a
    /// `max-keys=1` round-trip to find it.
    pub root_first_key: Option<String>,
}

/// One page of a flat run of keys, as startup discovery saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatRun {
    /// Prefix the run is bisected under: the flat directory itself, or, for
    /// files listed next to CommonPrefixes, the text those files share
    /// before their number (`obj-`), so bisection stays among the files
    /// instead of cutting across the subdirectories as well.
    pub prefix: String,
    /// First `<Contents>` key of the page.
    pub first_key: String,
    /// Last `<Contents>` key of the page.
    pub last_key: String,
    /// `<Contents>` the page held.
    pub keys: usize,
}

impl StartupDiscovery {
    /// Whether the run will list everything in a single request, so there is
    /// nothing worth partitioning.  Requires both that the probe's page held
    /// the whole listing and that the run's own page size (`--max-keys`, when
    /// set below the provider default) is large enough to return it in one
    /// page — otherwise the run paginates over those same keys and does want
    /// segments.
    pub fn is_single_page_listing(&self, max_keys: Option<i32>) -> bool {
        if self.root_page_truncated || !self.boundaries.is_empty() {
            return false;
        }
        match max_keys {
            Some(limit) => limit >= 0 && (limit as usize) >= self.root_page_keys,
            None => true,
        }
    }
}

/// Discover key-space boundaries by probing real CommonPrefixes.
pub async fn discover_startup_boundaries(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    target_boundaries: usize,
    timeout_secs: u64,
) -> StartupDiscovery {
    discover_with_probe(prefix, target_boundaries, |p| {
        let client = client.clone();
        let bucket = bucket.to_string();
        async move {
            // A flat directory's page is 1,000 `<Contents>`: the fast parser
            // takes them, as it does for listing pages (through the SDK's
            // deserializer, a level of such pages cost ~100 ms of CPU before
            // the first object was listed).  Only the first and last key and
            // the count are kept.
            let slot = crate::list_page::ParsedPageSlot::default();
            let send = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&p)
                .delimiter("/")
                .customize()
                .interceptor(crate::list_page::FastContentsInterceptor::new(slot.clone()))
                .send();
            // Startup discovery runs before the first object is listed; a
            // stalled endpoint must not hang the run there. Same watchdog the
            // runtime split probes use.
            let response = match tokio::time::timeout(
                std::time::Duration::from_secs(timeout_secs),
                send,
            )
            .await
            {
                Ok(result) => result.map_err(|e| crate::error::concise_sdk_error(&e))?,
                Err(_) => return Err("probe timed out".to_string()),
            };
            // The parser's rows, or the SDK's when it left the page to them.
            let (keys, first_key, last_key) = match slot.take() {
                Some(rows) => (
                    rows.len(),
                    rows.first().map(|(k, _)| k.as_str().to_string()),
                    rows.last().map(|(k, _)| k.as_str().to_string()),
                ),
                None => {
                    let key = |o: Option<&aws_sdk_s3::types::Object>| {
                        o.and_then(|o| o.key()).map(str::to_string)
                    };
                    let contents = response.contents();
                    (contents.len(), key(contents.first()), key(contents.last()))
                }
            };
            Ok(ProbePage {
                prefixes: response
                    .common_prefixes()
                    .iter()
                    .filter_map(|cp| cp.prefix())
                    .map(str::to_string)
                    .collect(),
                truncated: response.is_truncated().unwrap_or(false),
                keys,
                first_key,
                last_key,
            })
        }
    })
    .await
}

/// One structural probe's page: the CommonPrefixes it found and whether more
/// pages follow.
pub struct ProbePage {
    pub prefixes: Vec<String>,
    pub truncated: bool,
    pub keys: usize,
    /// First `<Contents>` key of the page, if any.
    pub first_key: Option<String>,
    /// Last `<Contents>` key of the page, if any.
    pub last_key: Option<String>,
}

/// The flat run a probed page shows, if it is worth bisecting: the page was
/// truncated (more keys follow) and holds at least two keys to size the run
/// by.  A page without CommonPrefixes below the root is a flat directory.  A
/// page with them lists its own files among the subdirectories; those files
/// form a run only when they share text past the parent prefix (`obj-` in
/// `obj-000000123.snappy.parquet`), which is then what bisection stays under.
/// The root page of a flat namespace is not a run here: the caller bisects
/// the whole listing then.
fn flat_run(parent: &str, depth: usize, page: &ProbePage) -> Option<FlatRun> {
    if !page.truncated || page.keys < 2 {
        return None;
    }
    let (first_key, last_key) = (page.first_key.clone()?, page.last_key.clone()?);
    let prefix = if page.prefixes.is_empty() {
        if depth == 0 {
            return None;
        }
        parent.to_string()
    } else {
        let scope = crate::flat_cut::RunScale::new(&first_key, &last_key, parent)?.scope;
        if scope.len() <= parent.len() {
            return None;
        }
        scope
    };
    Some(FlatRun {
        prefix,
        first_key,
        last_key,
        keys: page.keys,
    })
}

/// BFS over CommonPrefixes via an injected probe (one request per call).
/// Bounded by depth and per-level probe count, so a worst-case run issues
/// at most 1 + 2×STARTUP_DISCOVERY_MAX_PROBES_PER_LEVEL requests.
async fn discover_with_probe<F, Fut>(
    prefix: &str,
    target_boundaries: usize,
    probe: F,
) -> StartupDiscovery
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<ProbePage, String>>,
{
    let mut boundaries: BTreeSet<String> = BTreeSet::new();
    let mut frontier: Vec<String> = vec![prefix.to_string()];
    // Assume more pages until the root probe says otherwise: a failed probe
    // must not be read as "this bucket is tiny".
    let mut root_page_truncated = true;
    let mut root_page_keys = 0usize;
    let mut flat_runs: Vec<FlatRun> = Vec::new();
    let mut root_first_key: Option<String> = None;

    for depth in 0..STARTUP_DISCOVERY_MAX_DEPTH {
        if frontier.is_empty() || boundaries.len() >= target_boundaries {
            break;
        }
        let level: Vec<String> = frontier
            .drain(..)
            .take(STARTUP_DISCOVERY_MAX_PROBES_PER_LEVEL)
            .collect();
        let results = futures::future::join_all(level.iter().map(|p| probe(p.clone()))).await;
        for (parent, result) in level.iter().zip(results) {
            match result {
                Ok(page) => {
                    if depth == 0 {
                        root_page_truncated = page.truncated;
                        root_page_keys = page.keys;
                        // Without CommonPrefixes, the page starts with the
                        // listing's first key (a CommonPrefix could sort
                        // before it).
                        if page.prefixes.is_empty() {
                            root_first_key = page.first_key.clone();
                        }
                    }
                    flat_runs.extend(flat_run(parent, depth, &page));
                    for child in &page.prefixes {
                        boundaries.insert(child.clone());
                    }
                    frontier.extend(page.prefixes);
                }
                Err(e) => {
                    log::warn!(
                        "Startup discovery probe failed at depth {} for prefix '{}': {}",
                        depth,
                        parent,
                        e
                    );
                }
            }
        }
    }

    StartupDiscovery {
        boundaries: cap_boundaries(boundaries.into_iter().collect(), target_boundaries),
        root_page_truncated,
        root_page_keys,
        flat_runs,
        root_first_key,
    }
}

/// A BFS level adds every CommonPrefix it finds, so a wide tree overshoots
/// the target by orders of magnitude (64 prefixes x 1,000 subdirectories =
/// 64,000 segments).  Every segment costs at least one page request, and its
/// last page is mostly keys past its end, so keep `target` of them, evenly
/// spaced.  Any subset of real boundaries is still a valid partition.
fn cap_boundaries(boundaries: Vec<String>, target: usize) -> Vec<String> {
    let n = boundaries.len();
    if target == 0 || n <= target {
        return boundaries;
    }
    (1..=target)
        .map(|i| boundaries[i * n / (target + 1)].clone())
        .collect()
}

// ── Flat-namespace partitioning ────────────────────────────
//
// Structural discovery returns nothing for a flat namespace (no
// CommonPrefixes), which leaves a diff side listing as one serial segment —
// the biggest remaining diff performance gap, since diff cannot use list
// mode's runtime splitting (its ordered merge needs a fixed, key-ordered
// segment set up front). This bisects the key range with single-key probes
// before listing, producing exactly that: a sorted, contiguous,
// non-overlapping partition whose boundaries are all real observed keys
// (the same flat-cut search runtime splitting uses), so the merge consumes
// it in key order with no changes.

/// Probes in flight at once across a bisection wave.  Each range's cut is
/// usually a single probe, but an estimate of a range's high key fans out
/// several per round.
const FLAT_BISECT_MAX_IN_FLIGHT: usize = 64;
/// Probes in flight at once while sizing flat runs: the whole sizing is one
/// round of seven probes per run, and the runs number fewer than the flat
/// target (at most 64), so this lets twenty runs size in one round-trip
/// while still bounding the burst.
const FLAT_SIZING_MAX_IN_FLIGHT: usize = 256;
/// Cuts one range takes per wave (one probe each, concurrently).
const FLAT_CUTS_PER_RANGE: usize = 7;

/// Discover key-space boundaries for a flat namespace by recursively
/// bisecting the key range. `probe(start_after)` returns the first key
/// strictly after `start_after` within the listing prefix (or the first key
/// of all when `None`).  `first_key`, when the caller already saw the
/// range's first key (startup discovery's page of the same prefix), saves
/// that probe; it only anchors the first range, so any boundary found is
/// still a real key after it.  Returns up to `target_boundaries` sorted
/// boundaries; empty means an empty range or no cuttable structure (single
/// segment).
///
/// Each cut lands near the middle of its range's keys (see `flat_cut`): the
/// open-ended root range first estimates the namespace's high key, which
/// later open-ended ranges reuse, and every bounded range cuts at the
/// midpoint of its two real-key ends with a single probe.  Each bisection
/// level probes all of its ranges concurrently: the ranges are disjoint, so
/// their cuts are independent, and a wave costs about one round-trip.
pub async fn discover_flat_boundaries<F, Fut>(
    prefix: &str,
    target_boundaries: usize,
    first_key: Option<String>,
    probe: F,
) -> Vec<String>
where
    F: Fn(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<Option<String>, String>>,
{
    if target_boundaries == 0 {
        return Vec::new();
    }
    let permits = tokio::sync::Semaphore::new(FLAT_BISECT_MAX_IN_FLIGHT);
    let probe = |start_after: Option<String>| {
        let request = probe(start_after);
        let permits = &permits;
        async move {
            let _permit = permits.acquire().await.ok();
            request.await
        }
    };
    // Anchor on the first real key (discovery's page already returned it, when
    // the caller has it); an empty range yields no boundaries.
    let first = match first_key {
        Some(key) => key,
        None => match probe(None).await {
            Ok(Some(key)) => key,
            _ => return Vec::new(),
        },
    };
    let cut_probe = |start_after: String| probe(Some(start_after));

    let mut boundaries: BTreeSet<String> = BTreeSet::new();
    // Near-maximal real key of the namespace, once estimated.
    let mut known_high: Option<String> = None;
    // Ranges still to bisect: (start_key, end_boundary). start_key is a real
    // observed key; end is an exclusive upper bound (None = open to the tail).
    let mut frontier: Vec<(String, Option<String>)> = vec![(first, None)];
    while boundaries.len() < target_boundaries && !frontier.is_empty() {
        // Each successful cut adds one boundary, so ranges beyond the
        // remaining budget are dropped — the same early-stop the serial
        // walk applied, decided before the wave instead of during it.
        let remaining = target_boundaries - boundaries.len();
        frontier.truncate(remaining);
        let n = frontier.len();
        // Spread the remaining budget over the ranges, up to
        // FLAT_CUTS_PER_RANGE each (a range splits into up to eight near-equal
        // parts per round-trip).  When the budget fits with at most one more
        // cut for some ranges, hand it out exactly so this wave reaches the
        // target; a range whose probes find fewer distinct keys than asked
        // (adjacent keys) leaves the rest to the next wave.
        let (even, extra) = (remaining / n, remaining % n);
        let budget = |i: usize| {
            if remaining <= n * (FLAT_CUTS_PER_RANGE + 1) {
                even + usize::from(i < extra)
            } else {
                FLAT_CUTS_PER_RANGE
            }
        };
        let cuts =
            futures::future::join_all(frontier.iter().enumerate().map(|(i, (start, end))| {
                crate::flat_cut::find_flat_cuts(
                    prefix,
                    start,
                    end.as_deref(),
                    known_high.as_deref(),
                    budget(i),
                    &cut_probe,
                )
            }))
            .await;
        let mut next: Vec<(String, Option<String>)> = Vec::new();
        for ((start, end), found) in frontier.drain(..).zip(cuts) {
            let (found, high) = match found {
                Ok(found) => found,
                Err(e) => {
                    log::debug!("Flat bisection probe failed after '{}': {}", start, e);
                    continue;
                }
            };
            if let Some(high) = high {
                if known_high.as_ref().is_none_or(|k| high > *k) {
                    known_high = Some(high);
                }
            }
            let mut lo = start.clone();
            for cut in found {
                if cut > lo && end.as_ref().is_none_or(|e| cut < *e) {
                    boundaries.insert(cut.clone());
                    next.push((lo, Some(cut.clone())));
                    lo = cut;
                }
            }
            if lo != start {
                next.push((lo, end));
            }
        }
        frontier = next;
    }
    boundaries.into_iter().take(target_boundaries).collect()
}

/// Bisect the flat runs startup discovery found, sharing `budget` boundaries
/// between them by their estimated size: a run holding 90% of the keys gets
/// about 90% of the boundaries, not `1/runs` of them.  `probe(prefix,
/// start_after)` is `discover_flat_boundaries`' probe under `prefix`.
/// Sizing costs one concurrent probe round (see
/// `flat_cut::estimate_run_keys`), skipped for a single run.  Every boundary
/// is a real key inside its run, so the result — sorted, distinct, at most
/// `budget` long — merges with the structural boundaries as is.
pub async fn partition_flat_runs<F, Fut>(runs: &[FlatRun], budget: usize, probe: F) -> Vec<String>
where
    F: Fn(String, Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<Option<String>, String>>,
{
    if budget == 0 || runs.is_empty() {
        return Vec::new();
    }
    let shares = if runs.len() == 1 {
        vec![budget]
    } else {
        let permits = tokio::sync::Semaphore::new(FLAT_SIZING_MAX_IN_FLIGHT);
        let sizes = futures::future::join_all(runs.iter().map(|run| {
            let (probe, permits) = (&probe, &permits);
            let scoped = move |start_after: String| {
                let request = probe(run.prefix.clone(), Some(start_after));
                async move {
                    let _permit = permits.acquire().await.ok();
                    request.await
                }
            };
            async move {
                crate::flat_cut::estimate_run_keys(
                    &run.first_key,
                    &run.last_key,
                    run.keys,
                    &run.prefix,
                    &scoped,
                )
                .await
                .unwrap_or_else(|e| {
                    log::debug!("Sizing flat run '{}' failed: {}", run.prefix, e);
                    None
                })
            }
        }))
        .await;
        log::debug!(
            "Flat run size estimates: {}",
            runs.iter()
                .zip(&sizes)
                .map(|(run, size)| format!("{}={:?}", run.prefix, size))
                .collect::<Vec<_>>()
                .join(" ")
        );
        let pages: Vec<usize> = runs.iter().map(|run| run.keys).collect();
        share_budget(&sizes, &pages, budget)
    };
    let found =
        futures::future::join_all(runs.iter().zip(shares).filter(|(_, share)| *share > 0).map(
            |(run, share)| {
                let probe = &probe;
                discover_flat_boundaries(
                    &run.prefix,
                    share,
                    Some(run.first_key.clone()),
                    move |start_after| probe(run.prefix.clone(), start_after),
                )
            },
        ))
        .await;
    let mut boundaries: Vec<String> = found.into_iter().flatten().collect();
    boundaries.sort();
    boundaries.dedup();
    boundaries
}

/// Split `budget` cuts between runs in proportion to their estimated sizes
/// (`None`: no estimate — the median of the others, or an even split when no
/// run has one), by largest remainder.  A run is never given more cuts than
/// it has pages (`page_keys` is the size of the page it was seen by): past
/// that its segments would be single requests anyway, so those cuts go to the
/// other runs, and when every run is capped the total stays below `budget`.
fn share_budget(sizes: &[Option<u64>], page_keys: &[usize], budget: usize) -> Vec<usize> {
    let n = sizes.len();
    let mut known: Vec<u64> = sizes.iter().flatten().copied().collect();
    known.sort_unstable();
    let fallback = known.get(known.len() / 2).copied().unwrap_or(1);
    let weight: Vec<u128> = sizes
        .iter()
        .map(|s| u128::from(s.unwrap_or(fallback).max(1)))
        .collect();
    let cap: Vec<usize> = sizes
        .iter()
        .zip(page_keys)
        .map(|(size, &page)| match size {
            Some(size) => usize::try_from(size.div_ceil(page.max(1) as u64))
                .unwrap_or(usize::MAX)
                .max(1),
            None => budget,
        })
        .collect();
    let mut share = vec![0usize; n];
    let mut active: Vec<usize> = (0..n).collect();
    let mut left = budget;
    while !active.is_empty() && left > 0 {
        let total: u128 = active.iter().map(|&i| weight[i]).sum();
        let mut alloc: Vec<(usize, usize, u128)> = active
            .iter()
            .map(|&i| {
                let scaled = left as u128 * weight[i];
                (i, (scaled / total) as usize, scaled % total)
            })
            .collect();
        let mut spare = left - alloc.iter().map(|a| a.1).sum::<usize>();
        let mut by_remainder: Vec<usize> = (0..alloc.len()).collect();
        by_remainder.sort_by(|&a, &b| alloc[b].2.cmp(&alloc[a].2));
        for j in by_remainder {
            if spare == 0 {
                break;
            }
            alloc[j].1 += 1;
            spare -= 1;
        }
        let over: Vec<usize> = alloc
            .iter()
            .filter(|(i, s, _)| *s > cap[*i])
            .map(|a| a.0)
            .collect();
        if over.is_empty() {
            for (i, s, _) in alloc {
                share[i] = s;
            }
            break;
        }
        // Pin the runs past their cap and share what is left again.
        for i in over {
            share[i] = cap[i];
            left -= cap[i];
            active.retain(|&a| a != i);
        }
    }
    share
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hints_cache_round_trip() {
        let cache = HintsCache {
            bucket: "bucket".to_string(),
            region: Some("us-east-1".to_string()),
            prefix: Some("logs/".to_string()),
            boundaries: vec!["a/".to_string(), "b/".to_string()],
            generated_at: "2026-05-16T00:00:00Z".to_string(),
        };

        let encoded = toml::to_string_pretty(&cache).unwrap();
        assert!(encoded.contains("generated_at = \"2026-05-16T00:00:00Z\""));

        let decoded: HintsCache = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded.bucket, "bucket");
        assert_eq!(decoded.region.as_deref(), Some("us-east-1"));
        assert_eq!(decoded.prefix.as_deref(), Some("logs/"));
        assert_eq!(decoded.boundaries, vec!["a/".to_string(), "b/".to_string()]);
    }

    #[test]
    fn test_hints_cache_ignores_legacy_fields() {
        // Files written by older versions carry total_objects/scan_mode/
        // estimate_mode (and older sampled-scan fields); they must still load.
        let legacy = r#"bucket = "bucket"
region = "us-east-1"
total_objects = 100
boundaries = ["a/", "b/"]
generated_at = "2026-05-16T00:00:00Z"
scan_mode = "structural"
estimate_mode = "structural"
"#;
        let decoded: HintsCache = toml::from_str(legacy).unwrap();
        assert_eq!(decoded.bucket, "bucket");
        assert_eq!(decoded.boundaries, vec!["a/".to_string(), "b/".to_string()]);
    }

    fn fake_tree(data: &[(&str, &[&str])]) -> std::collections::HashMap<String, Vec<String>> {
        data.iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    async fn run_discovery(
        tree: std::collections::HashMap<String, Vec<String>>,
        prefix: &str,
        target: usize,
    ) -> Vec<String> {
        run_discovery_full(tree, prefix, target, true)
            .await
            .boundaries
    }

    async fn run_discovery_full(
        tree: std::collections::HashMap<String, Vec<String>>,
        prefix: &str,
        target: usize,
        truncated: bool,
    ) -> StartupDiscovery {
        discover_with_probe(prefix, target, |p| {
            let children = tree.get(&p).cloned().unwrap_or_default();
            // A prefix without children is a directory of keys: a page of
            // `part-00000` .. `part-00999`.
            let leaf = children.is_empty();
            async move {
                Ok(ProbePage {
                    prefixes: children,
                    truncated,
                    keys: if leaf { 1000 } else { 0 },
                    first_key: leaf.then(|| format!("{p}part-00000")),
                    last_key: leaf.then(|| format!("{p}part-00999")),
                })
            }
        })
        .await
    }

    #[tokio::test]
    async fn test_startup_discovery_flat_namespace_yields_no_boundaries() {
        let boundaries = run_discovery(fake_tree(&[]), "", 16).await;
        assert!(boundaries.is_empty());
    }

    #[tokio::test]
    async fn test_startup_discovery_reports_flat_leaves() {
        // `data/` holds only keys (no CommonPrefixes) and more than a page.
        let tree = fake_tree(&[("", &["data/"])]);
        let discovery = run_discovery_full(tree, "", 16, true).await;
        assert_eq!(discovery.boundaries, vec!["data/"]);
        assert_eq!(
            discovery.flat_runs,
            vec![FlatRun {
                prefix: "data/".into(),
                first_key: "data/part-00000".into(),
                last_key: "data/part-00999".into(),
                keys: 1000,
            }]
        );
        // An untruncated leaf page is small: nothing to bisect.
        let tree = fake_tree(&[("", &["data/"])]);
        let discovery = run_discovery_full(tree, "", 16, false).await;
        assert!(discovery.flat_runs.is_empty());
    }

    #[tokio::test]
    async fn test_startup_discovery_reports_files_next_to_folders() {
        // The root lists `a/`, `z/` and a page of `obj-…` files: those files
        // are a run under `obj-`, not under the root (which would cut across
        // the folders as well).
        let page = |prefixes: Vec<String>, first: &str, last: &str| ProbePage {
            prefixes,
            truncated: true,
            keys: 998,
            first_key: Some(first.to_string()),
            last_key: Some(last.to_string()),
        };
        let discovery = discover_with_probe("", 16, |p| {
            let result = match p.as_str() {
                "" => page(
                    vec!["a/".into(), "z/".into()],
                    "obj-000000000.snappy.parquet",
                    "obj-000000997.snappy.parquet",
                ),
                // Folder pages mix files with no shared run text: `x.txt`
                // and `y.txt` differ in their first character.
                _ => page(
                    vec![format!("{p}sub/")],
                    &format!("{p}x.txt"),
                    &format!("{p}y.txt"),
                ),
            };
            async move { Ok(result) }
        })
        .await;
        assert_eq!(
            discovery.flat_runs,
            vec![FlatRun {
                prefix: "obj-".into(),
                first_key: "obj-000000000.snappy.parquet".into(),
                last_key: "obj-000000997.snappy.parquet".into(),
                keys: 998,
            }]
        );
        assert_eq!(discovery.root_first_key, None);
    }

    #[tokio::test]
    async fn test_flat_boundaries_multiway_waves_stay_balanced() {
        let keys: Vec<String> = (0..20_000)
            .map(|i| format!("obj-{:09}.snappy.parquet", i))
            .collect();
        let boundaries = run_flat(keys.clone(), "", 64).await;
        assert_eq!(boundaries.len(), 64);
        let mut edges: Vec<usize> = vec![0];
        edges.extend(boundaries.iter().map(|b| keys.partition_point(|k| k <= b)));
        edges.push(keys.len());
        let max = edges.windows(2).map(|w| w[1] - w[0]).max().unwrap();
        let mean = keys.len() / (edges.len() - 1);
        assert!(max <= mean * 2, "max segment {} vs mean {}", max, mean);
    }

    #[tokio::test]
    async fn test_flat_boundaries_multiway_waves_reach_exact_target() {
        // Multi-way waves must not stop short: every target a range can hold
        // is reached exactly, with strictly ascending real keys.
        let digits: Vec<String> = (0..20_000).map(|i| format!("k{:06}", i)).collect();
        let hex: Vec<String> = {
            let mut v: Vec<String> = (0..20_000u64)
                .map(|i| format!("{:016x}.bin", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
                .collect();
            v.sort();
            v
        };
        for keys in [&digits, &hex] {
            for target in [1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 57, 63, 64, 100] {
                let boundaries = run_flat(keys.clone(), "", target).await;
                assert_eq!(boundaries.len(), target, "target {target}: {boundaries:?}");
                assert!(boundaries.windows(2).all(|w| w[0] < w[1]));
                assert!(boundaries.iter().all(|b| keys.binary_search(b).is_ok()));
                assert!(boundaries[0] > keys[0], "the first key cannot be a cut");
            }
        }
    }

    #[tokio::test]
    async fn test_startup_discovery_single_level() {
        let tree = fake_tree(&[("", &["a/", "b/", "c/"])]);
        let boundaries = run_discovery(tree, "", 16).await;
        assert_eq!(boundaries, vec!["a/", "b/", "c/"]);
    }

    // Simulate S3 `max_keys=1`: the first key strictly after `start_after`
    // (or the very first key when `None`) from a sorted key set.
    async fn run_flat(keys: Vec<String>, prefix: &str, target: usize) -> Vec<String> {
        discover_flat_boundaries(prefix, target, None, |start_after| {
            let keys = keys.clone();
            async move {
                let next = match start_after {
                    None => keys.first().cloned(),
                    Some(sa) => keys.iter().find(|k| k.as_str() > sa.as_str()).cloned(),
                };
                Ok(next)
            }
        })
        .await
    }

    #[tokio::test]
    async fn test_flat_boundaries_empty_range_yields_none() {
        assert!(run_flat(Vec::new(), "", 16).await.is_empty());
    }

    #[tokio::test]
    async fn test_flat_boundaries_single_key_is_uncuttable() {
        assert!(run_flat(vec!["only".to_string()], "", 16).await.is_empty());
    }

    #[tokio::test]
    async fn test_flat_boundaries_partition_is_valid() {
        let keys: Vec<String> = (0..200).map(|i| format!("key{:04}", i)).collect();
        let target = 8;
        let boundaries = run_flat(keys.clone(), "key", target).await;

        assert!(!boundaries.is_empty(), "a 200-key flat range should split");
        assert!(boundaries.len() <= target);
        // Every boundary is a real observed key, and they are strictly sorted
        // (a contiguous, non-overlapping partition the diff merge can consume).
        let mut prev: Option<&String> = None;
        for b in &boundaries {
            assert!(keys.contains(b), "boundary '{}' is not a real key", b);
            if let Some(p) = prev {
                assert!(b > p, "boundaries must be strictly ascending");
            }
            prev = Some(b);
        }
    }

    #[tokio::test]
    async fn test_flat_boundaries_balance_suffix_heavy_keys() {
        // Long constant suffixes after a numeric run used to make every cut
        // land on the key right after the range start (boundaries at
        // obj-000000001, obj-000000002, … and one giant tail segment).
        for shape in ["obj-{}.snappy.parquet", "data/part-{}-c000.snappy.parquet"] {
            let keys: Vec<String> = (0..50_000)
                .map(|i| shape.replace("{}", &format!("{:09}", i)))
                .collect();
            let target = 16;
            let boundaries = run_flat(keys.clone(), "", target).await;
            assert_eq!(boundaries.len(), target, "{shape}");
            // Segment sizes: (start, b0], (b0, b1], …, (b_last, end].
            let mut positions: Vec<usize> = boundaries
                .iter()
                .map(|b| keys.binary_search(b).expect("boundary is a real key"))
                .collect();
            positions.push(keys.len() - 1);
            let mut prev = 0usize;
            let largest = positions
                .iter()
                .map(|&p| {
                    let size = p - prev;
                    prev = p;
                    size
                })
                .max()
                .unwrap();
            let ideal = keys.len() / (target + 1);
            assert!(
                largest <= ideal * 5 / 2,
                "{shape}: largest segment {largest} keys, ideal {ideal}: {boundaries:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_flat_boundaries_probe_waves_run_concurrently() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let keys: Vec<String> = (0..200).map(|i| format!("key{:04}", i)).collect();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let probe_in_flight = Arc::clone(&in_flight);
        let probe_max = Arc::clone(&max_in_flight);
        let boundaries = discover_flat_boundaries("key", 8, None, |start_after| {
            let keys = keys.clone();
            let in_flight = Arc::clone(&probe_in_flight);
            let max_in_flight = Arc::clone(&probe_max);
            async move {
                // Track overlap deterministically: yield_now parks this probe
                // once, so sibling probes of the same wave get polled while it
                // is "in flight" — no timing or sleeps involved.
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                let next = match start_after {
                    None => keys.first().cloned(),
                    Some(sa) => keys.iter().find(|k| k.as_str() > sa.as_str()).cloned(),
                };
                Ok(next)
            }
        })
        .await;

        assert!(!boundaries.is_empty());
        assert!(
            max_in_flight.load(Ordering::SeqCst) > 1,
            "bisection-wave probes must overlap, got max in-flight {}",
            max_in_flight.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_startup_discovery_recurses_until_target() {
        let tree = fake_tree(&[
            ("", &["a/", "b/"]),
            ("a/", &["a/x/", "a/y/"]),
            ("b/", &["b/z/"]),
        ]);
        let boundaries = run_discovery(tree, "", 16).await;
        assert_eq!(boundaries, vec!["a/", "a/x/", "a/y/", "b/", "b/z/"]);
    }

    #[tokio::test]
    async fn test_startup_discovery_stops_at_target() {
        let tree = fake_tree(&[
            ("", &["a/", "b/", "c/", "d/"]),
            ("a/", &["a/x/"]),
            ("b/", &["b/y/"]),
        ]);
        // Target satisfied by the first level — no recursion.
        let boundaries = run_discovery(tree, "", 4).await;
        assert_eq!(boundaries, vec!["a/", "b/", "c/", "d/"]);
    }

    #[tokio::test]
    async fn test_startup_discovery_caps_an_overshooting_level() {
        // Four prefixes of 250 subdirectories each: the second level alone
        // brings 1,000 boundaries for a target of 16.
        let subdirs: Vec<Vec<String>> = ["a/", "b/", "c/", "d/"]
            .iter()
            .map(|p| (0..250).map(|i| format!("{p}{i:03}/")).collect())
            .collect();
        let mut tree = std::collections::HashMap::new();
        tree.insert(
            String::new(),
            vec!["a/".into(), "b/".into(), "c/".into(), "d/".into()],
        );
        for (p, children) in ["a/", "b/", "c/", "d/"].iter().zip(&subdirs) {
            tree.insert(p.to_string(), children.clone());
        }
        let all: Vec<String> = {
            let mut v: Vec<String> = tree.values().flatten().cloned().collect();
            v.sort();
            v
        };
        let boundaries = run_discovery(tree, "", 16).await;
        assert_eq!(boundaries.len(), 16);
        assert!(boundaries.windows(2).all(|w| w[0] < w[1]));
        assert!(boundaries.iter().all(|b| all.binary_search(b).is_ok()));
        // Evenly spaced over the discovered set: no gap much wider than n/16.
        let idx: Vec<usize> = boundaries
            .iter()
            .map(|b| all.binary_search(b).unwrap())
            .collect();
        let widest = idx.windows(2).map(|w| w[1] - w[0]).max().unwrap();
        assert!(widest <= all.len() / 16 + 1, "widest gap {widest}");
    }

    #[tokio::test]
    async fn test_startup_discovery_keeps_first_key_of_pages_without_prefixes() {
        let probe = |root_prefixes: Vec<String>| {
            move |p: String| {
                let root_prefixes = root_prefixes.clone();
                async move {
                    Ok(ProbePage {
                        prefixes: if p.is_empty() {
                            root_prefixes
                        } else {
                            Vec::new()
                        },
                        truncated: true,
                        keys: 1000,
                        first_key: Some(format!("{p}first")),
                        last_key: Some(format!("{p}last")),
                    })
                }
            }
        };
        let discovery = discover_with_probe("", 16, probe(vec!["data/".to_string()])).await;
        assert_eq!(discovery.flat_runs.len(), 1);
        assert_eq!(discovery.flat_runs[0].prefix, "data/");
        assert_eq!(discovery.flat_runs[0].first_key, "data/first");
        // The root page had a CommonPrefix, which could sort before its first
        // key: the root's first key is not kept.
        assert_eq!(discovery.root_first_key, None);
        // A flat namespace's root page starts with the listing's first key.
        let discovery = discover_with_probe("", 16, probe(Vec::new())).await;
        assert_eq!(discovery.root_first_key.as_deref(), Some("first"));
        assert!(discovery.flat_runs.is_empty());
    }

    #[tokio::test]
    async fn test_flat_boundaries_reuse_a_known_first_key() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let keys: Vec<String> = (0..5_000).map(|i| format!("obj-{i:06}")).collect();
        let anchor_probes = Arc::new(AtomicUsize::new(0));
        let run = |first: Option<String>| {
            let keys = keys.clone();
            let anchor_probes = Arc::clone(&anchor_probes);
            async move {
                discover_flat_boundaries("", 16, first, |start_after| {
                    let keys = keys.clone();
                    let anchor_probes = Arc::clone(&anchor_probes);
                    async move {
                        Ok(match start_after {
                            None => {
                                anchor_probes.fetch_add(1, Ordering::SeqCst);
                                keys.first().cloned()
                            }
                            Some(sa) => keys.iter().find(|k| k.as_str() > sa.as_str()).cloned(),
                        })
                    }
                })
                .await
            }
        };
        let probed = run(None).await;
        assert_eq!(anchor_probes.load(Ordering::SeqCst), 1);
        let reused = run(Some(keys[0].clone())).await;
        assert_eq!(anchor_probes.load(Ordering::SeqCst), 1, "no anchor probe");
        assert_eq!(probed, reused);
        assert_eq!(reused.len(), 16);
    }

    #[test]
    fn test_share_budget_follows_run_sizes() {
        let pages = [1000usize; 4];
        // One run holds almost everything: it takes almost every cut, and the
        // two-page runs are capped at two cuts' worth of their pages.
        let share = share_budget(
            &[Some(2000), Some(256_000), Some(2000), Some(2000)],
            &pages,
            43,
        );
        assert_eq!(share.iter().sum::<usize>(), 43);
        assert!(share[1] >= 37, "{share:?}");
        assert!(share.iter().enumerate().all(|(i, &s)| i == 1 || s <= 2));
        // No estimates: an even split, the first runs taking the remainder.
        assert_eq!(share_budget(&[None; 4], &pages, 10), vec![3, 3, 2, 2]);
        // A run without an estimate counts as the median of the others.
        let share = share_budget(&[Some(64_000), None, Some(64_000), Some(8_000)], &pages, 20);
        assert_eq!(share.iter().sum::<usize>(), 20);
        assert!(share[1] >= 6 && share[3] <= 1, "{share:?}");
        // Every run capped at its pages: the total stays below the budget.
        assert_eq!(
            share_budget(&[Some(2000), Some(3000)], &pages[..2], 64),
            vec![2, 3]
        );
        assert_eq!(share_budget(&[Some(8000)], &pages[..1], 0), vec![0]);
    }

    /// `partition_flat_runs` over a sorted key set: `probe(prefix, sa)` is the
    /// first key under `prefix` after `sa`, as S3 `max-keys=1` returns it.
    async fn run_partition(keys: &[String], runs: &[FlatRun], budget: usize) -> Vec<String> {
        partition_flat_runs(runs, budget, |prefix, start_after| {
            let from = start_after.map_or(0, |sa| keys.partition_point(|k| *k <= sa));
            let next = keys[from..]
                .iter()
                .take_while(|k| k.as_str() >= prefix.as_str())
                .find(|k| k.starts_with(&prefix))
                .cloned();
            std::future::ready(Ok(next))
        })
        .await
    }

    fn page_of(keys: &[String], prefix: &str) -> FlatRun {
        let from = keys.partition_point(|k| k.as_str() < prefix);
        let page: Vec<&String> = keys[from..]
            .iter()
            .take_while(|k| k.starts_with(prefix))
            .take(1000)
            .collect();
        FlatRun {
            prefix: prefix.to_string(),
            first_key: page[0].clone(),
            last_key: page[page.len() - 1].clone(),
            keys: page.len(),
        }
    }

    #[tokio::test]
    async fn test_partition_flat_runs_puts_the_budget_where_the_keys_are() {
        // Twenty flat leaves, 90% of the keys in leaf 07.
        let mut keys: Vec<String> = (0..180_000)
            .map(|i| format!("data/leaf=07/part-{i:09}-c000.snappy.parquet"))
            .collect();
        for leaf in (0..20).filter(|&l| l != 7) {
            keys.extend(
                (0..1_050).map(|i| format!("data/leaf={leaf:02}/part-{i:09}-c000.snappy.parquet")),
            );
        }
        keys.sort();
        let runs: Vec<FlatRun> = (0..20)
            .map(|l| page_of(&keys, &format!("data/leaf={l:02}/")))
            .collect();
        let budget = 43;
        let boundaries = run_partition(&keys, &runs, budget).await;
        assert!(boundaries.len() <= budget);
        assert!(boundaries.windows(2).all(|w| w[0] < w[1]));
        assert!(boundaries.iter().all(|b| keys.binary_search(b).is_ok()));
        let big: Vec<&String> = boundaries
            .iter()
            .filter(|b| b.starts_with("data/leaf=07/"))
            .collect();
        assert!(big.len() >= 35, "big leaf got {} of {budget}", big.len());
        // Its segments stay near the ideal size (180k / (cuts + 1)).
        let lo = keys.partition_point(|k| k.as_str() < "data/leaf=07/");
        let mut edges = vec![lo];
        edges.extend(big.iter().map(|b| keys.partition_point(|k| k <= *b)));
        edges.push(lo + 180_000);
        let widest = edges.windows(2).map(|w| w[1] - w[0]).max().unwrap();
        assert!(
            widest <= 2 * 180_000 / (big.len() + 1),
            "widest segment {widest}"
        );
    }

    #[tokio::test]
    async fn test_partition_flat_runs_splits_files_next_to_folders() {
        // 90% of the keys are files at the root, next to ten small folders:
        // the run under `obj-` is cut inside the files only.
        let mut keys: Vec<String> = (0..90_000)
            .map(|i| format!("obj-{i:09}.snappy.parquet"))
            .collect();
        for d in ["a0", "a1", "a2", "z0", "z1"] {
            keys.extend((0..2_000).map(|i| format!("{d}/part-{i:06}")));
        }
        keys.sort();
        let run = FlatRun {
            prefix: "obj-".into(),
            ..page_of(&keys, "obj-")
        };
        let boundaries = run_partition(&keys, &[run], 16).await;
        assert_eq!(boundaries.len(), 16);
        assert!(boundaries.windows(2).all(|w| w[0] < w[1]));
        assert!(
            boundaries
                .iter()
                .all(|b| b.starts_with("obj-") && keys.binary_search(b).is_ok())
        );
    }

    #[tokio::test]
    async fn test_startup_discovery_respects_listing_prefix() {
        let tree = fake_tree(&[("logs/", &["logs/2025/", "logs/2026/"])]);
        let boundaries = run_discovery(tree, "logs/", 16).await;
        assert_eq!(boundaries, vec!["logs/2025/", "logs/2026/"]);
    }

    #[tokio::test]
    async fn test_startup_discovery_probe_errors_are_non_fatal() {
        let discovery = discover_with_probe("", 16, |p| async move {
            if p.is_empty() {
                Ok(ProbePage {
                    prefixes: vec!["a/".to_string(), "b/".to_string()],
                    truncated: true,
                    keys: 0,
                    first_key: None,
                    last_key: None,
                })
            } else {
                Err("probe failed".to_string())
            }
        })
        .await;
        assert_eq!(discovery.boundaries, vec!["a/", "b/"]);
    }

    #[tokio::test]
    async fn test_untruncated_root_page_reports_single_page_listing() {
        // No CommonPrefixes and no next page: the whole listing fits in one
        // request, so the caller must not pay bisection probes to split it.
        let discovery = run_discovery_full(fake_tree(&[]), "", 16, false).await;
        assert!(discovery.boundaries.is_empty());
        assert!(!discovery.root_page_truncated);
    }

    #[tokio::test]
    async fn test_single_page_decision_honors_max_keys() {
        let discovery = StartupDiscovery {
            boundaries: Vec::new(),
            root_page_truncated: false,
            root_page_keys: 500,
            flat_runs: Vec::new(),
            root_first_key: None,
        };
        // The probe returned the whole listing in one page, and the run's page
        // size can too.
        assert!(discovery.is_single_page_listing(None));
        assert!(discovery.is_single_page_listing(Some(1000)));
        assert!(discovery.is_single_page_listing(Some(500)));
        // A smaller page size paginates over those same keys — 50 pages at
        // --max-keys 10 — so the run does want segments.
        assert!(!discovery.is_single_page_listing(Some(10)));
        assert!(!discovery.is_single_page_listing(Some(499)));
    }

    #[tokio::test]
    async fn test_single_page_decision_requires_untruncated_page() {
        let truncated = StartupDiscovery {
            boundaries: Vec::new(),
            root_page_truncated: true,
            root_page_keys: 1000,
            flat_runs: Vec::new(),
            root_first_key: None,
        };
        assert!(!truncated.is_single_page_listing(None));
        // Structure found: partitioned by boundaries, not by this shortcut.
        let structured = StartupDiscovery {
            boundaries: vec!["a/".to_string()],
            root_page_truncated: false,
            root_page_keys: 3,
            flat_runs: Vec::new(),
            root_first_key: None,
        };
        assert!(!structured.is_single_page_listing(None));
    }

    #[tokio::test]
    async fn test_failed_root_probe_does_not_claim_single_page() {
        // A failed probe must not be read as "this bucket is tiny".
        let discovery = discover_with_probe("", 16, |_p| async move {
            Err::<ProbePage, String>("probe failed".to_string())
        })
        .await;
        assert!(discovery.boundaries.is_empty());
        assert!(discovery.root_page_truncated);
    }
}
