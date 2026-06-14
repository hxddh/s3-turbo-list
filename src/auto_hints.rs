use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

// ── Cached hints format ────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentEstimate {
    pub start_after: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_before: Option<String>,
    pub estimated_objects: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HintsCache {
    pub bucket: String,
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    pub total_objects: usize,
    pub boundaries: Vec<String>,
    pub generated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_prefix_entries: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub prefix_counts_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampled_objects: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampled_pages: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pages: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimate_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segment_estimates: Vec<SegmentEstimate>,
}

fn is_false(value: &bool) -> bool {
    !*value
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

/// Discover key-space boundaries by probing real CommonPrefixes.
/// Returns a sorted boundary list; empty means no structure was found
/// (flat namespace) and the caller should fall back to a single segment.
pub async fn discover_startup_boundaries(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    target_boundaries: usize,
) -> Vec<String> {
    discover_with_probe(prefix, target_boundaries, |p| {
        let client = client.clone();
        let bucket = bucket.to_string();
        async move {
            let response = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&p)
                .delimiter("/")
                .send()
                .await
                .map_err(|e| format!("{:?}", e))?;
            Ok(response
                .common_prefixes()
                .iter()
                .filter_map(|cp| cp.prefix())
                .map(str::to_string)
                .collect())
        }
    })
    .await
}

/// BFS over CommonPrefixes via an injected probe (one request per call).
/// Bounded by depth and per-level probe count, so a worst-case run issues
/// at most 1 + 2×STARTUP_DISCOVERY_MAX_PROBES_PER_LEVEL requests.
async fn discover_with_probe<F, Fut>(
    prefix: &str,
    target_boundaries: usize,
    probe: F,
) -> Vec<String>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<String>, String>>,
{
    let mut boundaries: BTreeSet<String> = BTreeSet::new();
    let mut frontier: Vec<String> = vec![prefix.to_string()];

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
                Ok(children) => {
                    for child in &children {
                        boundaries.insert(child.clone());
                    }
                    frontier.extend(children);
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

    boundaries.into_iter().collect()
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
// (the same flat-cut logic runtime splitting uses), so the merge consumes
// it in key order with no changes.

/// Discover key-space boundaries for a flat namespace by recursively
/// bisecting the key range. `probe(start_after)` returns the first key
/// strictly after `start_after` within the listing prefix (or the first key
/// of all when `None`). Returns up to `target_boundaries` sorted boundaries;
/// empty means an empty range or no cuttable structure (single segment).
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
    // Anchor on the first real key; an empty range yields no boundaries.
    let first = match probe(None).await {
        Ok(Some(key)) => key,
        _ => return Vec::new(),
    };

    let mut boundaries: BTreeSet<String> = BTreeSet::new();
    // Ranges still to bisect: (start_key, end_boundary). start_key is a real
    // observed key; end is an exclusive upper bound (None = open to the tail).
    let mut frontier: Vec<(String, Option<String>)> = vec![(first, None)];
    while boundaries.len() < target_boundaries && !frontier.is_empty() {
        let mut next: Vec<(String, Option<String>)> = Vec::new();
        for (start, end) in frontier.drain(..) {
            if boundaries.len() >= target_boundaries {
                break;
            }
            if let Some(cut) = find_flat_cut(prefix, &start, end.as_deref(), &probe).await {
                boundaries.insert(cut.clone());
                next.push((start, Some(cut.clone())));
                next.push((cut, end));
            }
        }
        frontier = next;
    }
    boundaries.into_iter().take(target_boundaries).collect()
}

/// Find one real observed key strictly inside `(start, end)` using the same
/// flat-cut candidate logic runtime splitting uses. Each candidate costs one
/// single-key probe; the first observed key in range wins.
async fn find_flat_cut<F, Fut>(
    prefix: &str,
    start: &str,
    end: Option<&str>,
    probe: &F,
) -> Option<String>
where
    F: Fn(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<Option<String>, String>>,
{
    for candidate in crate::tasks_s3::flat_cut_candidates(start, prefix, end) {
        if let Ok(Some(key)) = probe(Some(candidate)).await {
            if key.as_str() > start && end.map_or(true, |e| key.as_str() < e) {
                return Some(key);
            }
        }
    }
    None
}

/// Persist startup-discovered boundaries to the conventional hints cache
/// so subsequent runs (and --resume) load identical segments.
pub fn write_startup_hints_cache(
    bucket: &str,
    region: Option<&str>,
    prefix: &str,
    boundaries: &[String],
) -> Result<String, String> {
    let cache = HintsCache {
        bucket: bucket.to_string(),
        region: region.map(|r| r.to_string()),
        prefix: (!prefix.is_empty()).then(|| prefix.to_string()),
        total_objects: 0,
        boundaries: boundaries.to_vec(),
        generated_at: chrono::Local::now().to_rfc3339(),
        source_count: None,
        source_files: Vec::new(),
        max_keys: None,
        max_prefix_entries: None,
        prefix_counts_truncated: false,
        scan_mode: Some("structural".to_string()),
        sampled_objects: None,
        sampled_pages: None,
        sample_limit: None,
        max_pages: None,
        estimate_mode: Some("structural".to_string()),
        segment_estimates: Vec::new(),
    };
    let path = crate::agent::conventional_hints_path(bucket, region);
    let toml_str = toml::to_string_pretty(&cache)
        .map_err(|e| format!("failed to serialize hints cache: {}", e))?;
    std::fs::write(&path, &toml_str)
        .map_err(|e| format!("failed to write hints cache '{}': {}", path, e))?;
    Ok(path)
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sampled_metadata_serializes() {
        let cache = HintsCache {
            bucket: "bucket".to_string(),
            region: Some("us-east-1".to_string()),
            prefix: Some("logs/".to_string()),
            total_objects: 100,
            boundaries: vec!["a/".to_string(), "b/".to_string()],
            generated_at: "2026-05-16T00:00:00Z".to_string(),
            source_count: None,
            source_files: Vec::new(),
            max_keys: Some(500),
            max_prefix_entries: Some(1_000_000),
            prefix_counts_truncated: false,
            scan_mode: Some("sampled".to_string()),
            sampled_objects: Some(100),
            sampled_pages: Some(2),
            sample_limit: Some(100),
            max_pages: Some(2),
            estimate_mode: Some("sampled".to_string()),
            segment_estimates: vec![SegmentEstimate {
                start_after: String::new(),
                end_before: Some("a/".to_string()),
                estimated_objects: 50,
            }],
        };

        let encoded = toml::to_string_pretty(&cache).unwrap();
        assert!(encoded.contains("scan_mode = \"sampled\""));
        assert!(encoded.contains("sampled_objects = 100"));

        let decoded: HintsCache = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded.scan_mode.as_deref(), Some("sampled"));
        assert_eq!(decoded.prefix.as_deref(), Some("logs/"));
        assert_eq!(decoded.max_keys, Some(500));
        assert_eq!(decoded.sampled_pages, Some(2));
        assert_eq!(decoded.segment_estimates.len(), 1);
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
        discover_with_probe(prefix, target, |p| {
            let children = tree.get(&p).cloned().unwrap_or_default();
            async move { Ok(children) }
        })
        .await
    }

    #[tokio::test]
    async fn test_startup_discovery_flat_namespace_yields_no_boundaries() {
        let boundaries = run_discovery(fake_tree(&[]), "", 16).await;
        assert!(boundaries.is_empty());
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
        let boundaries = discover_with_probe("", 16, |p| async move {
            if p.is_empty() {
                Ok(vec!["a/".to_string(), "b/".to_string()])
            } else {
                Err("probe failed".to_string())
            }
        })
        .await;
        assert_eq!(boundaries, vec!["a/", "b/"]);
    }

    #[test]
    fn test_startup_hints_cache_round_trip() {
        // conventional_hints_path is cwd-relative; use a unique bucket name
        // and remove the file afterwards.
        let bucket = format!("startup-hints-test-{}", std::process::id());
        let boundaries = vec!["a/".to_string(), "b/".to_string()];
        let path = write_startup_hints_cache(&bucket, Some("us-east-1"), "", &boundaries).unwrap();
        let loaded = crate::hints::parse_hints_file(&path);
        let _ = std::fs::remove_file(&path);
        assert_eq!(loaded.unwrap(), boundaries);
    }
}
