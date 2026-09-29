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
    /// Probed prefixes below the root whose page was truncated and held no
    /// CommonPrefixes: flat directories with more than one page of keys.
    /// Their boundaries alone cannot split them, so the caller bisects them
    /// when discovery found fewer boundaries than it wants (`data/part-…`
    /// under a single top-level `data/`).
    pub flat_leaves: Vec<String>,
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
            let send = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&p)
                .delimiter("/")
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
            Ok(ProbePage {
                prefixes: response
                    .common_prefixes()
                    .iter()
                    .filter_map(|cp| cp.prefix())
                    .map(str::to_string)
                    .collect(),
                truncated: response.is_truncated().unwrap_or(false),
                keys: response.contents().len(),
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
    let mut flat_leaves: Vec<String> = Vec::new();

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
                    } else if page.truncated && page.prefixes.is_empty() {
                        flat_leaves.push(parent.clone());
                    }
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
        boundaries: boundaries.into_iter().collect(),
        root_page_truncated,
        root_page_keys,
        flat_leaves,
    }
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
/// Cuts one range takes per wave (one probe each, concurrently).
const FLAT_CUTS_PER_RANGE: usize = 7;

/// Discover key-space boundaries for a flat namespace by recursively
/// bisecting the key range. `probe(start_after)` returns the first key
/// strictly after `start_after` within the listing prefix (or the first key
/// of all when `None`). Returns up to `target_boundaries` sorted boundaries;
/// empty means an empty range or no cuttable structure (single segment).
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
    // Anchor on the first real key; an empty range yields no boundaries.
    let first = match probe(None).await {
        Ok(Some(key)) => key,
        _ => return Vec::new(),
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
            async move {
                Ok(ProbePage {
                    prefixes: children,
                    truncated,
                    keys: 0,
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
        assert_eq!(discovery.flat_leaves, vec!["data/"]);
        // An untruncated leaf page is small: nothing to bisect.
        let tree = fake_tree(&[("", &["data/"])]);
        let discovery = run_discovery_full(tree, "", 16, false).await;
        assert!(discovery.flat_leaves.is_empty());
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
        discover_flat_boundaries(prefix, target, |start_after| {
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
        let boundaries = discover_flat_boundaries("key", 8, |start_after| {
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
            flat_leaves: Vec::new(),
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
            flat_leaves: Vec::new(),
        };
        assert!(!truncated.is_single_page_listing(None));
        // Structure found: partitioned by boundaries, not by this shortcut.
        let structured = StartupDiscovery {
            boundaries: vec!["a/".to_string()],
            root_page_truncated: false,
            root_page_keys: 3,
            flat_leaves: Vec::new(),
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
