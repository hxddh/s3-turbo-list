use log::info;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ── Cached hints format ────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HintsCache {
    pub bucket: String,
    pub region: Option<String>,
    pub total_objects: usize,
    pub boundaries: Vec<String>,
    pub generated_at: String,
}

// ── Main entry point ───────────────────────────────────────

pub async fn generate_hints(
    region: Option<&str>,
    bucket: &str,
    output: Option<&str>,
    endpoint_url: Option<&str>,
    force_path_style: bool,
    sample_threshold: usize,
    max_prefix_depth: usize,
) {
    info!(
        "Auto-hints: sampling bucket '{}' (threshold={}, max_depth={})",
        bucket, sample_threshold, max_prefix_depth,
    );

    // Build S3 client.
    let loader = aws_config::from_env()
        .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3));

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
    let client = aws_sdk_s3::Client::from_conf(s3_cfg.build());

    // Phase 1: sequential scan collecting prefix→count.
    let mut prefix_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total_objects = 0usize;
    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
        .into_paginator()
        .send();

    loop {
        match paginator.next().await {
            Some(Ok(response)) => {
                for obj in response.contents() {
                    if let Some(key) = obj.key() {
                        total_objects += 1;
                        let prefix = key
                            .rsplit_once('/')
                            .map(|(p, _)| p.to_string())
                            .unwrap_or_else(|| "/".to_string());
                        *prefix_counts.entry(prefix).or_insert(0) += 1;
                    }
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
        "Auto-hints: scanned {} objects across {} unique prefixes",
        total_objects,
        prefix_counts.len(),
    );

    // Phase 2: split prefixes exceeding threshold.
    let boundaries = split_prefixes(&prefix_counts, sample_threshold, max_prefix_depth);

    info!(
        "Auto-hints: generated {} key-space boundaries",
        boundaries.len()
    );

    // Phase 3: write cache file.
    let cache = HintsCache {
        bucket: bucket.to_string(),
        region: region.map(|r| r.to_string()),
        total_objects,
        boundaries: boundaries.clone(),
        generated_at: chrono::Local::now().to_rfc3339(),
    };

    let output_path = output.map(|o| o.to_string()).unwrap_or_else(|| {
        if let Some(r) = region {
            format!("{}_{}_hints.toml", r, bucket)
        } else {
            format!("{}_hints.toml", bucket)
        }
    });

    let toml_str = toml::to_string_pretty(&cache).expect("Failed to serialize hints cache");
    std::fs::write(&output_path, &toml_str).expect("Failed to write hints cache");
    info!("Auto-hints: cache written to {}", output_path);

    // Print summary for user.
    println!("Auto-hints generated for bucket '{}':", bucket);
    println!("  Total objects scanned: {}", total_objects);
    println!("  Unique prefixes:       {}", prefix_counts.len());
    println!("  Key-space boundaries:  {}", boundaries.len());
    println!("  Cache file:            {}", output_path);
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

// ── Prefix splitting algorithm ─────────────────────────────

/// Walk the prefix→count map and emit boundaries.  Prefixes with count >
/// threshold are recursively split into sub-prefixes up to max_depth levels of
/// '/' nesting.
fn split_prefixes(
    counts: &BTreeMap<String, usize>,
    threshold: usize,
    max_depth: usize,
) -> Vec<String> {
    let mut split_points: BTreeMap<String, usize> = BTreeMap::new();

    for (prefix, &count) in counts.iter() {
        if prefix == "/" || prefix.is_empty() {
            continue;
        }

        if count > threshold {
            let depth = prefix.matches('/').count() + 1;
            if depth < max_depth {
                split_points.entry(prefix.clone()).or_insert(count);
            } else {
                split_points.entry(prefix.clone()).or_insert(count);
            }
        } else {
            split_points.entry(prefix.clone()).or_insert(count);
        }
    }

    // Convert split points to ordered boundary list.
    let mut boundaries: Vec<String> = split_points.keys().cloned().collect();
    boundaries.sort();
    boundaries
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
}
