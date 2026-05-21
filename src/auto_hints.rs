use log::info;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

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

#[derive(Debug, Clone, Copy)]
pub struct GenerateHintsOptions<'a> {
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub output: Option<&'a str>,
    pub endpoint_url: Option<&'a str>,
    pub force_path_style: bool,
    pub prefix: &'a str,
    pub max_keys: Option<i32>,
    pub max_attempts: u32,
    pub initial_backoff_secs: u64,
    pub connect_timeout_secs: u64,
    pub operation_timeout_secs: u64,
    pub sample_threshold: usize,
    pub max_prefix_depth: usize,
    pub max_prefix_entries: usize,
    pub sample_limit: Option<usize>,
    pub max_pages: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub struct DiscoverPrefixesOptions<'a> {
    pub region: Option<&'a str>,
    pub bucket: &'a str,
    pub output: Option<&'a str>,
    pub endpoint_url: Option<&'a str>,
    pub force_path_style: bool,
    pub prefix: &'a str,
    pub delimiter: &'a str,
    pub max_keys: Option<i32>,
    pub max_attempts: u32,
    pub initial_backoff_secs: u64,
    pub connect_timeout_secs: u64,
    pub operation_timeout_secs: u64,
    pub max_pages: Option<usize>,
    pub toml: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixDiscoveryCache {
    pub bucket: String,
    pub region: Option<String>,
    pub prefix: String,
    pub delimiter: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<i32>,
    pub generated_at: String,
    pub scanned_pages: usize,
    pub common_prefixes_count: usize,
    pub common_prefixes: Vec<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

// ── Main entry point ───────────────────────────────────────

pub async fn generate_hints(options: GenerateHintsOptions<'_>) {
    let sampled_mode = options.sample_limit.is_some() || options.max_pages.is_some();
    info!(
        "Auto-hints: scanning bucket '{}' prefix '{}' (threshold={}, max_depth={}, sample_limit={:?}, max_pages={:?})",
        options.bucket,
        options.prefix,
        options.sample_threshold,
        options.max_prefix_depth,
        options.sample_limit,
        options.max_pages,
    );

    let client = build_client(
        options.region,
        options.endpoint_url,
        options.force_path_style,
        options.max_attempts,
        options.initial_backoff_secs,
        options.connect_timeout_secs,
        options.operation_timeout_secs,
    )
    .await;

    // Phase 1: sequential scan collecting prefix→count.
    let mut prefix_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total_objects = 0usize;
    let mut scanned_pages = 0usize;
    let mut stopped_by_limit = false;
    let mut prefix_counts_truncated = false;
    let mut request = client
        .list_objects_v2()
        .bucket(options.bucket)
        .prefix(options.prefix);
    if let Some(max_keys) = options.max_keys {
        request = request.max_keys(max_keys);
    }
    let mut paginator = request.into_paginator().send();
    let mut last_heartbeat = Instant::now();

    'scan: loop {
        match paginator.next().await {
            Some(Ok(response)) => {
                scanned_pages += 1;
                for obj in response.contents() {
                    if let Some(key) = obj.key() {
                        total_objects += 1;
                        let prefix = key
                            .rsplit_once('/')
                            .map(|(p, _)| p.to_string())
                            .unwrap_or_else(|| "/".to_string());
                        if let Some(count) = prefix_counts.get_mut(&prefix) {
                            *count += 1;
                        } else if prefix_counts.len() < options.max_prefix_entries {
                            prefix_counts.insert(prefix, 1);
                        } else {
                            prefix_counts_truncated = true;
                        }
                        if options
                            .sample_limit
                            .is_some_and(|limit| total_objects >= limit)
                        {
                            stopped_by_limit = true;
                            break 'scan;
                        }
                    }
                }
                if last_heartbeat.elapsed() >= Duration::from_secs(5) {
                    info!(
                        "Auto-hints: scanned {} objects across {} pages, {} unique prefixes{}",
                        total_objects,
                        scanned_pages,
                        prefix_counts.len(),
                        if prefix_counts_truncated {
                            " (prefix map bounded)"
                        } else {
                            ""
                        }
                    );
                    last_heartbeat = Instant::now();
                }
                if options
                    .max_pages
                    .is_some_and(|limit| scanned_pages >= limit)
                {
                    stopped_by_limit = true;
                    break;
                }
            }
            Some(Err(e)) => {
                log::warn!("Auto-hints scan error: {:?}", e);
                break;
            }
            None => break,
        }
    }

    info!(
        "Auto-hints: scanned {} objects across {} pages and {} unique prefixes{}",
        total_objects,
        scanned_pages,
        prefix_counts.len(),
        if prefix_counts_truncated {
            " (prefix map bounded)"
        } else {
            ""
        },
    );

    // Phase 2: split prefixes exceeding threshold.
    let boundaries = split_prefixes(
        &prefix_counts,
        options.sample_threshold,
        options.max_prefix_depth,
    );
    let segment_estimates = estimate_segments(&boundaries, &prefix_counts);

    info!(
        "Auto-hints: generated {} key-space boundaries",
        boundaries.len()
    );

    // Phase 3: write cache file.
    let cache = HintsCache {
        bucket: options.bucket.to_string(),
        region: options.region.map(|r| r.to_string()),
        prefix: (!options.prefix.is_empty()).then(|| options.prefix.to_string()),
        total_objects,
        boundaries: boundaries.clone(),
        generated_at: chrono::Local::now().to_rfc3339(),
        max_keys: options.max_keys,
        max_prefix_entries: Some(options.max_prefix_entries),
        prefix_counts_truncated,
        source_count: None,
        source_files: Vec::new(),
        scan_mode: Some(if sampled_mode {
            "sampled".to_string()
        } else {
            "full".to_string()
        }),
        sampled_objects: sampled_mode.then_some(total_objects),
        sampled_pages: sampled_mode.then_some(scanned_pages),
        sample_limit: options.sample_limit,
        max_pages: options.max_pages,
        estimate_mode: Some(if sampled_mode {
            "sampled".to_string()
        } else {
            "full".to_string()
        }),
        segment_estimates: segment_estimates.clone(),
    };

    let output_path = options
        .output
        .map(|o| o.to_string())
        .unwrap_or_else(|| crate::agent::conventional_hints_path(options.bucket, options.region));

    let toml_str = toml::to_string_pretty(&cache).expect("Failed to serialize hints cache");
    std::fs::write(&output_path, &toml_str).expect("Failed to write hints cache");
    info!("Auto-hints: cache written to {}", output_path);

    // Print summary for user.
    println!("Auto-hints generated for bucket '{}':", options.bucket);
    println!(
        "  Scan mode:             {}",
        if sampled_mode { "sampled" } else { "full" }
    );
    println!(
        "  Prefix:                {}",
        if options.prefix.is_empty() {
            "(bucket root)"
        } else {
            options.prefix
        }
    );
    println!("  Objects scanned:       {}", total_objects);
    println!("  Pages scanned:         {}", scanned_pages);
    println!("  Unique prefixes:       {}", prefix_counts.len());
    if prefix_counts_truncated {
        println!(
            "  Prefix map:            bounded at {} entries; estimates are partial",
            options.max_prefix_entries
        );
    }
    println!("  Key-space boundaries:  {}", boundaries.len());
    print_estimate_summary(&segment_estimates, sampled_mode);
    println!("  Cache file:            {}", output_path);
    if sampled_mode {
        println!(
            "  Sampling note:         boundaries are estimated from the scanned sample; total_objects is not the bucket total."
        );
        if stopped_by_limit {
            println!("  Stop reason:           sampling limit reached");
        }
    }
    if !boundaries.is_empty() {
        println!("  First 5 boundaries:");
        for b in boundaries.iter().take(5) {
            println!("    - {}", b);
        }
        if boundaries.len() > 5 {
            println!("    ... and {} more", boundaries.len() - 5);
        }
    }
}

pub async fn discover_prefixes(options: DiscoverPrefixesOptions<'_>) {
    info!(
        "Discover-prefixes: scanning bucket '{}' prefix '{}' delimiter '{}'",
        options.bucket, options.prefix, options.delimiter
    );

    let client = build_client(
        options.region,
        options.endpoint_url,
        options.force_path_style,
        options.max_attempts,
        options.initial_backoff_secs,
        options.connect_timeout_secs,
        options.operation_timeout_secs,
    )
    .await;

    let mut request = client
        .list_objects_v2()
        .bucket(options.bucket)
        .prefix(options.prefix)
        .delimiter(options.delimiter);
    if let Some(max_keys) = options.max_keys {
        request = request.max_keys(max_keys);
    }

    let mut paginator = request.into_paginator().send();
    let mut scanned_pages = 0usize;
    let mut common_prefixes = BTreeSet::new();
    let mut last_heartbeat = Instant::now();

    loop {
        match paginator.next().await {
            Some(Ok(response)) => {
                scanned_pages += 1;
                for cp in response.common_prefixes() {
                    if let Some(prefix) = cp.prefix() {
                        common_prefixes.insert(prefix.to_string());
                    }
                }
                if last_heartbeat.elapsed() >= Duration::from_secs(5) {
                    info!(
                        "Discover-prefixes: scanned {} pages, {} unique CommonPrefixes",
                        scanned_pages,
                        common_prefixes.len()
                    );
                    last_heartbeat = Instant::now();
                }
                if options
                    .max_pages
                    .is_some_and(|limit| scanned_pages >= limit)
                {
                    break;
                }
            }
            Some(Err(e)) => {
                log::warn!("Discover-prefixes scan error: {:?}", e);
                break;
            }
            None => break,
        }
    }

    let prefixes: Vec<String> = common_prefixes.into_iter().collect();
    let output_path = options
        .output
        .map(|o| o.to_string())
        .unwrap_or_else(|| prefixes_output_path(options.region, options.bucket, options.toml));

    if options.toml {
        let cache = PrefixDiscoveryCache {
            bucket: options.bucket.to_string(),
            region: options.region.map(|r| r.to_string()),
            prefix: options.prefix.to_string(),
            delimiter: options.delimiter.to_string(),
            max_keys: options.max_keys,
            generated_at: chrono::Local::now().to_rfc3339(),
            scanned_pages,
            common_prefixes_count: prefixes.len(),
            common_prefixes: prefixes.clone(),
        };
        let toml_str = toml::to_string_pretty(&cache).expect("Failed to serialize prefixes cache");
        std::fs::write(&output_path, toml_str).expect("Failed to write prefixes cache");
    } else {
        let body = if prefixes.is_empty() {
            String::new()
        } else {
            format!("{}\n", prefixes.join("\n"))
        };
        std::fs::write(&output_path, body).expect("Failed to write prefixes file");
    }

    println!("Discovered CommonPrefixes for bucket '{}':", options.bucket);
    println!(
        "  Prefix:                {}",
        if options.prefix.is_empty() {
            "(bucket root)"
        } else {
            options.prefix
        }
    );
    println!("  Delimiter:             {}", options.delimiter);
    println!("  Pages scanned:         {}", scanned_pages);
    println!("  CommonPrefixes:        {}", prefixes.len());
    println!("  Output file:           {}", output_path);
}

async fn build_client(
    region: Option<&str>,
    endpoint_url: Option<&str>,
    force_path_style: bool,
    max_attempts: u32,
    initial_backoff_secs: u64,
    connect_timeout_secs: u64,
    operation_timeout_secs: u64,
) -> aws_sdk_s3::Client {
    let loader = aws_config::from_env()
        .retry_config(
            aws_config::retry::RetryConfig::standard()
                .with_max_attempts(max_attempts)
                .with_initial_backoff(Duration::from_secs(initial_backoff_secs)),
        )
        .timeout_config(
            aws_config::timeout::TimeoutConfigBuilder::new()
                .connect_timeout(Duration::from_secs(connect_timeout_secs))
                .operation_timeout(Duration::from_secs(operation_timeout_secs))
                .read_timeout(Duration::from_secs(operation_timeout_secs))
                .operation_attempt_timeout(Duration::from_secs(operation_timeout_secs))
                .build(),
        );

    let config = loader.load().await;
    let mut s3_cfg = aws_sdk_s3::config::Builder::from(&config);
    if let Some(r) = region {
        s3_cfg = s3_cfg.region(aws_sdk_s3::config::Region::new(r.to_owned()));
    }
    if let Some(ep) = endpoint_url {
        s3_cfg = s3_cfg.endpoint_url(ep.to_string());
    }
    if force_path_style {
        s3_cfg = s3_cfg.force_path_style(true);
    }
    aws_sdk_s3::Client::from_conf(s3_cfg.build())
}

fn estimate_segments(
    boundaries: &[String],
    counts: &BTreeMap<String, usize>,
) -> Vec<SegmentEstimate> {
    let mut estimates: Vec<SegmentEstimate> = Vec::with_capacity(boundaries.len() + 1);
    let mut starts = Vec::with_capacity(boundaries.len() + 1);
    starts.push(String::new());
    starts.extend(boundaries.iter().cloned());

    for (i, start) in starts.iter().enumerate() {
        let end = boundaries.get(i).cloned();
        let estimated_objects = counts
            .iter()
            .filter(|(prefix, _)| {
                (start.is_empty() || prefix.as_str() >= start.as_str())
                    && end.as_ref().map_or(true, |e| prefix.as_str() < e.as_str())
            })
            .map(|(_, count)| *count)
            .sum();
        estimates.push(SegmentEstimate {
            start_after: start.clone(),
            end_before: end,
            estimated_objects,
        });
    }

    estimates
}

fn print_estimate_summary(estimates: &[SegmentEstimate], sampled_mode: bool) {
    if estimates.is_empty() {
        return;
    }

    let total: usize = estimates.iter().map(|e| e.estimated_objects).sum();
    let min = estimates
        .iter()
        .map(|e| e.estimated_objects)
        .min()
        .unwrap_or(0);
    let max = estimates
        .iter()
        .map(|e| e.estimated_objects)
        .max()
        .unwrap_or(0);
    println!(
        "  Segment estimates:     {} segments, min {}, max {}, total {} ({})",
        estimates.len(),
        min,
        max,
        total,
        if sampled_mode {
            "sampled/estimated"
        } else {
            "observed/full"
        }
    );
    println!("  First 5 estimates:");
    for estimate in estimates.iter().take(5) {
        println!(
            "    - start_after='{}', end_before='{}', estimated_objects={}",
            estimate.start_after,
            estimate.end_before.as_deref().unwrap_or(""),
            estimate.estimated_objects
        );
    }
    if estimates.len() > 5 {
        println!("    ... and {} more", estimates.len() - 5);
    }
}

// ── Prefix splitting algorithm ─────────────────────────────

/// Walk the prefix→count map and emit ordered boundaries. Prefixes observed
/// deeper than max_depth are coalesced to the configured depth so generated
/// hints never exceed the documented depth limit.
fn split_prefixes(
    counts: &BTreeMap<String, usize>,
    _threshold: usize,
    max_depth: usize,
) -> Vec<String> {
    let mut split_points: BTreeMap<String, usize> = BTreeMap::new();

    for (prefix, &count) in counts.iter() {
        if prefix == "/" || prefix.is_empty() {
            continue;
        }

        let boundary = clamp_prefix_depth(prefix, max_depth);
        if boundary.is_empty() {
            continue;
        }
        let entry = split_points.entry(boundary).or_insert(0);
        *entry = entry.saturating_add(count);
    }

    // Convert split points to ordered boundary list.
    let mut boundaries: Vec<String> = split_points.keys().cloned().collect();
    boundaries.sort();
    boundaries
}

fn clamp_prefix_depth(prefix: &str, max_depth: usize) -> String {
    if max_depth == 0 {
        return String::new();
    }
    let has_trailing_slash = prefix.ends_with('/');
    let segments: Vec<&str> = prefix
        .split('/')
        .filter(|segment| !segment.is_empty())
        .take(max_depth)
        .collect();
    let mut boundary = segments.join("/");
    if has_trailing_slash && !boundary.is_empty() {
        boundary.push('/');
    }
    boundary
}

fn prefixes_output_path(region: Option<&str>, bucket: &str, toml: bool) -> String {
    let extension = if toml { "toml" } else { "txt" };
    let mut parts = Vec::new();
    if let Some(region) = region.filter(|r| !r.is_empty()) {
        parts.push(crate::agent::sanitize_path_component(region));
    }
    parts.push(crate::agent::sanitize_path_component(bucket));
    format!("{}_prefixes.{}", parts.join("_"), extension)
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_counts(data: &[(&str, usize)]) -> BTreeMap<String, usize> {
        data.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn test_split_empty() {
        let counts = make_counts(&[]);
        let b = split_prefixes(&counts, 100, 5);
        assert!(b.is_empty());
    }

    #[test]
    fn test_split_below_threshold() {
        let counts = make_counts(&[("a/", 50), ("b/", 30)]);
        let b = split_prefixes(&counts, 100, 5);
        // Both appear as split points.
        assert_eq!(b.len(), 2);
        assert_eq!(b[0], "a/");
        assert_eq!(b[1], "b/");
    }

    #[test]
    fn test_split_above_threshold() {
        let counts = make_counts(&[("a/", 200), ("b/", 50)]);
        let b = split_prefixes(&counts, 100, 5);
        assert_eq!(b.len(), 2);
        assert!(b.contains(&"a/".to_string()));
        assert!(b.contains(&"b/".to_string()));
    }

    #[test]
    fn test_root_prefix_ignored() {
        let counts = make_counts(&[("/", 500), ("a/", 200)]);
        let b = split_prefixes(&counts, 100, 5);
        assert!(!b.contains(&"/".to_string()));
        assert!(b.contains(&"a/".to_string()));
    }

    #[test]
    fn test_split_sorted_output() {
        let counts = make_counts(&[("z/", 50), ("a/", 30), ("m/", 60)]);
        let b = split_prefixes(&counts, 100, 5);
        assert_eq!(b, vec!["a/", "m/", "z/"]);
    }

    #[test]
    fn test_split_respects_max_prefix_depth() {
        let counts = make_counts(&[("a/b/c/", 200), ("a/b/d/", 50), ("x/y/z", 20)]);
        let b = split_prefixes(&counts, 100, 2);
        assert_eq!(b, vec!["a/b/", "x/y"]);
    }

    #[test]
    fn test_default_output_paths_sanitize_bucket_and_region() {
        assert_eq!(
            crate::agent::conventional_hints_path("../evil/bucket", Some("us/east")),
            "us_east_.._evil_bucket_hints.toml"
        );
        assert_eq!(
            prefixes_output_path(Some("us/east"), "../evil/bucket", true),
            "us_east_.._evil_bucket_prefixes.toml"
        );
    }

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

    #[test]
    fn test_estimate_segments() {
        let counts = make_counts(&[("a/", 10), ("b/", 20), ("c/", 30)]);
        let boundaries = vec!["b/".to_string()];
        let estimates = estimate_segments(&boundaries, &counts);
        assert_eq!(estimates.len(), 2);
        assert_eq!(estimates[0].estimated_objects, 10);
        assert_eq!(estimates[1].estimated_objects, 50);
    }
}
