use crate::auto_hints::{HintsCache, SegmentEstimate};
use crate::hints;
use crate::trace::S3CompatEvent;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactSummary {
    pub path: String,
    pub exists: bool,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalToolManifest {
    pub schema_version: u32,
    pub tool_version: String,
    pub command: String,
    pub generated_at: String,
    pub status: String,
    pub inputs: Vec<ArtifactSummary>,
    pub outputs: Vec<ArtifactSummary>,
    pub warnings: Vec<String>,
    pub summary: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct HintsMergeReport {
    pub status: String,
    pub input_files: Vec<String>,
    pub input_count: usize,
    pub boundary_count_before_dedup: usize,
    pub boundary_count: usize,
    pub duplicate_count: usize,
    pub output: Option<String>,
    pub output_written: bool,
    pub dry_run: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InitConfigReport {
    pub status: String,
    pub profile: String,
    pub output: String,
    pub output_written: bool,
    pub overwrite: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceSummaryReport {
    pub status: String,
    pub trace_file: String,
    pub events_total: usize,
    pub list_events: usize,
    pub segment_count: usize,
    pub total_pages: u64,
    pub total_objects: u64,
    pub total_common_prefixes: u64,
    pub retry_events: usize,
    pub error_events: usize,
    pub slowest_segments: Vec<SegmentSummary>,
    pub imbalance: Option<ImbalanceSummary>,
    pub warnings: Vec<String>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SegmentSummary {
    pub segment_index: usize,
    pub start_after: Option<String>,
    pub end_before: Option<String>,
    pub pages: u32,
    pub objects: usize,
    pub common_prefixes: usize,
    pub elapsed_ms: u64,
    pub retry_attempt: u32,
    pub ended_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImbalanceSummary {
    pub min_pages: u32,
    pub median_pages: u32,
    pub max_pages: u32,
    pub max_to_median_ratio: f64,
    pub max_to_min_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HintsRebalanceReport {
    pub status: String,
    pub trace_file: String,
    pub hints_file: String,
    pub output: Option<String>,
    pub output_written: bool,
    pub dry_run: bool,
    pub original_boundary_count: usize,
    pub new_boundary_count: usize,
    pub added_boundaries: Vec<String>,
    pub long_tail_segments: Vec<LongTailSegment>,
    pub warnings: Vec<String>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LongTailSegment {
    pub segment_index: usize,
    pub start_after: Option<String>,
    pub end_before: Option<String>,
    pub pages: u32,
    pub median_pages: u32,
    pub selected_boundary: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone)]
struct TraceAnalysis {
    report: TraceSummaryReport,
    segments: Vec<SegmentSummary>,
    page_samples_by_start: BTreeMap<String, Vec<String>>,
}

pub fn merge_hints_files(
    inputs: &[String],
    output: Option<&str>,
    dry_run: bool,
    overwrite: bool,
) -> Result<HintsMergeReport, String> {
    if inputs.is_empty() {
        return Err("hints-merge requires at least one input file".to_string());
    }

    let mut all = Vec::new();
    let mut warnings = Vec::new();
    for path in inputs {
        let boundaries = hints::parse_hints_file(path)?;
        if boundaries.is_empty() {
            warnings.push(format!("input '{}' contains no boundaries", path));
        }
        all.extend(boundaries);
    }

    let before = all.len();
    all.sort();
    all.dedup();
    let duplicate_count = before.saturating_sub(all.len());

    let output_written = if let Some(path) = output {
        if dry_run {
            false
        } else {
            write_hints_cache(path, "merged", None, all.clone(), Some(inputs), overwrite)?;
            true
        }
    } else {
        false
    };

    Ok(HintsMergeReport {
        status: "success".to_string(),
        input_files: inputs.to_vec(),
        input_count: inputs.len(),
        boundary_count_before_dedup: before,
        boundary_count: all.len(),
        duplicate_count,
        output: output.map(str::to_string),
        output_written,
        dry_run,
        warnings,
    })
}

pub fn trace_summary(path: &str) -> Result<TraceSummaryReport, String> {
    Ok(analyze_trace(path)?.report)
}

pub fn rebalance_hints(
    trace_path: &str,
    hints_path: &str,
    output: Option<&str>,
    dry_run: bool,
    max_new_boundaries: usize,
    long_tail_ratio: f64,
    min_pages: u32,
    overwrite: bool,
) -> Result<HintsRebalanceReport, String> {
    let analysis = analyze_trace(trace_path)?;
    let original = hints::parse_hints_file(hints_path)?;
    let original_set: BTreeSet<String> = original.iter().cloned().collect();
    let median_pages = analysis
        .report
        .imbalance
        .as_ref()
        .map(|i| i.median_pages.max(1))
        .unwrap_or(1);

    let mut added = Vec::new();
    let mut long_tails = Vec::new();
    let mut warnings = Vec::new();
    for segment in &analysis.segments {
        if added.len() >= max_new_boundaries {
            break;
        }
        let threshold = (median_pages as f64 * long_tail_ratio).ceil() as u32;
        if segment.pages < min_pages || segment.pages < threshold.max(1) {
            continue;
        }

        let start = segment.start_after.clone().unwrap_or_default();
        let candidate = analysis
            .page_samples_by_start
            .get(&start)
            .and_then(|samples| pick_candidate(samples, &start, segment.end_before.as_deref()));

        let reason = if candidate.is_some() {
            "selected midpoint page last_key from trace".to_string()
        } else {
            "trace lacks usable per-page key samples for this segment".to_string()
        };

        if let Some(boundary) = candidate.clone() {
            if original_set.contains(&boundary) || added.contains(&boundary) {
                warnings.push(format!(
                    "candidate boundary '{}' for segment {} already exists",
                    boundary, segment.segment_index
                ));
            } else {
                added.push(boundary);
            }
        }

        long_tails.push(LongTailSegment {
            segment_index: segment.segment_index,
            start_after: segment.start_after.clone(),
            end_before: segment.end_before.clone(),
            pages: segment.pages,
            median_pages,
            selected_boundary: candidate,
            reason,
        });
    }

    let mut new_boundaries = original.clone();
    new_boundaries.extend(added.iter().cloned());
    new_boundaries.sort();
    new_boundaries.dedup();

    let output_written = if let Some(path) = output {
        if dry_run {
            false
        } else {
            write_hints_cache(
                path,
                "rebalanced",
                None,
                new_boundaries.clone(),
                Some(&[hints_path.to_string(), trace_path.to_string()]),
                overwrite,
            )?;
            true
        }
    } else {
        false
    };

    let mut recommendations = Vec::new();
    if added.is_empty() {
        recommendations.push(
            "no new boundaries were generated; collect trace with first_key/last_key metadata or use auto-hints/discover-prefixes for more evidence"
                .to_string(),
        );
    } else {
        recommendations.push(format!(
            "rerun listing with the rebalanced hints file; {} boundary/boundaries were added",
            added.len()
        ));
    }
    if max_new_boundaries == 0 {
        recommendations.push("--max-new-boundaries=0 makes this an analysis-only run".to_string());
    }

    Ok(HintsRebalanceReport {
        status: "success".to_string(),
        trace_file: trace_path.to_string(),
        hints_file: hints_path.to_string(),
        output: output.map(str::to_string),
        output_written,
        dry_run,
        original_boundary_count: original.len(),
        new_boundary_count: new_boundaries.len(),
        added_boundaries: added,
        long_tail_segments: long_tails,
        warnings,
        recommendations,
    })
}

pub fn init_config(
    output: &str,
    profile: Option<&str>,
    overwrite: bool,
) -> Result<InitConfigReport, String> {
    ensure_can_write(output, overwrite)?;
    let profile_name = profile.unwrap_or("aws").to_lowercase();
    let warnings = init_config_warnings(&profile_name);
    let rendered = init_config_template(&profile_name);
    if let Some(parent) = Path::new(output)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "failed to create output directory '{}': {}",
                parent.display(),
                e
            )
        })?;
    }
    std::fs::write(output, rendered)
        .map_err(|e| format!("failed to write config '{}': {}", output, e))?;
    Ok(InitConfigReport {
        status: "success".to_string(),
        profile: profile_name,
        output: output.to_string(),
        output_written: true,
        overwrite,
        warnings,
    })
}

pub fn render_init_config_text(report: &InitConfigReport) -> String {
    let mut out = String::new();
    out.push_str("Config initialized:\n");
    out.push_str(&format!("  Profile:         {}\n", report.profile));
    out.push_str(&format!("  Output:          {}\n", report.output));
    out.push_str("Next:\n");
    out.push_str("  s3-turbo-list doctor --local-only --simple\n");
    out.push_str("  s3-turbo-list --dry-run --agent --config ");
    out.push_str(&report.output);
    out.push_str(" --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1\n");
    append_warnings_and_recommendations(&mut out, &report.warnings, &[]);
    out
}

pub fn render_recipe(name: Option<&str>) -> Result<String, String> {
    let name = name.unwrap_or("index");
    match name {
        "index" | "list" => Ok(
            r#"Available recipes:
  aws-basic      Minimal AWS S3 dry-run and list
  large-bucket   Hints, trace, and output-dir workflow
  local-minio    Local MinIO endpoint example
  agent-safe     Local-only agent/CI commands

Run: s3-turbo-list recipes <name>
"#
            .to_string(),
        ),
        "aws-basic" => Ok(
            r#"AWS basic:
  export AWS_PROFILE=default
  s3-turbo-list doctor --local-only --simple
  s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        "large-bucket" => Ok(
            r#"Large bucket:
  s3-turbo-list auto-hints --bucket my-bucket --region us-east-1 --sample-limit 1000000 -o hints.toml
  s3-turbo-list hints-validate --hints-file hints.toml --json
  s3-turbo-list --output-dir out --delimiter '' --trace-compat out/trace.jsonl -H hints.toml list --bucket my-bucket --region us-east-1
  s3-turbo-list trace-summary out/trace.jsonl --machine-readable
"#
            .to_string(),
        ),
        "local-minio" => Ok(
            r#"Local MinIO:
  export AWS_ACCESS_KEY_ID=minioadmin
  export AWS_SECRET_ACCESS_KEY=minioadmin
  s3-turbo-list --profile minio --endpoint-url http://127.0.0.1:9000 --force-path-style --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        "agent-safe" => Ok(
            r#"Agent-safe local commands:
  s3-turbo-list config-inspect --json
  s3-turbo-list doctor --local-only --json
  s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list trace-summary trace.jsonl --machine-readable
"#
            .to_string(),
        ),
        other => Err(format!(
            "unknown recipe '{}'. Run 's3-turbo-list recipes' to list recipes.",
            other
        )),
    }
}

pub fn render_cheatsheet() -> String {
    r#"s3-turbo-list cheatsheet

First run:
  s3-turbo-list doctor --local-only --simple
  s3-turbo-list init-config --output s3-turbo-list.toml
  s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1

Credentials vs endpoint profiles:
  export AWS_PROFILE=my-credentials-profile
  s3-turbo-list --profile r2 ...

Useful local commands:
  s3-turbo-list profiles list
  s3-turbo-list hints-validate --hints-file hints.toml --json
  s3-turbo-list trace-summary trace.jsonl --machine-readable
  s3-turbo-list recipes large-bucket
"#
    .to_string()
}

pub fn render_quickstart(provider: &str) -> Result<String, String> {
    match provider {
        "aws" => Ok(
            r#"AWS quickstart:
1. Set credentials:
   export AWS_PROFILE=default
2. Check local setup:
   s3-turbo-list doctor --local-only --simple
3. Dry-run:
   s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
4. First list:
   s3-turbo-list --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        "minio" => Ok(
            r#"MinIO quickstart:
1. Set local credentials:
   export AWS_ACCESS_KEY_ID=minioadmin
   export AWS_SECRET_ACCESS_KEY=minioadmin
2. First list:
   s3-turbo-list --profile minio --endpoint-url http://127.0.0.1:9000 --force-path-style --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        "r2" => Ok(
            r#"Cloudflare R2 quickstart:
1. Select credentials with AWS_PROFILE, not --profile:
   export AWS_PROFILE=my-r2-creds
2. Use --profile r2 for endpoint compatibility defaults:
   s3-turbo-list --profile r2 --endpoint-url https://<account-id>.r2.cloudflarestorage.com --output-dir out --delimiter '' list --bucket my-bucket --region auto
"#
            .to_string(),
        ),
        "bos" => Ok(
            r#"BOS quickstart:
1. Select credentials with AWS_PROFILE, not --profile:
   export AWS_PROFILE=my-bos-creds
2. Use virtual-hosted addressing:
   s3-turbo-list --profile bos --addressing-style virtual --output-dir out --delimiter '' list --bucket my-bucket --region bj
3. For authoritative BOS output, prefer single-segment listing when service-side start_after + continuation-token compatibility matters.
"#
            .to_string(),
        ),
        other => Err(format!(
            "unknown quickstart '{}'. Valid values: aws, minio, r2, bos.",
            other
        )),
    }
}

pub fn render_trace_summary_text(report: &TraceSummaryReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("Trace file: {}\n", report.trace_file));
    out.push_str(&format!("  Events:          {}\n", report.events_total));
    out.push_str(&format!("  List events:     {}\n", report.list_events));
    out.push_str(&format!("  Segments:        {}\n", report.segment_count));
    out.push_str(&format!("  Pages:           {}\n", report.total_pages));
    out.push_str(&format!("  Objects:         {}\n", report.total_objects));
    out.push_str(&format!(
        "  CommonPrefixes:  {}\n",
        report.total_common_prefixes
    ));
    out.push_str(&format!("  Retry events:    {}\n", report.retry_events));
    out.push_str(&format!("  Error events:    {}\n", report.error_events));
    if let Some(imbalance) = &report.imbalance {
        out.push_str(&format!(
            "  Page imbalance: min={}, median={}, max={}, max/median={:.2}\n",
            imbalance.min_pages,
            imbalance.median_pages,
            imbalance.max_pages,
            imbalance.max_to_median_ratio
        ));
    }
    if !report.slowest_segments.is_empty() {
        out.push_str("  Slowest segments:\n");
        for s in &report.slowest_segments {
            out.push_str(&format!(
                "    - #{} pages={} objects={} elapsed_ms={} start_after='{}' end_before='{}'\n",
                s.segment_index,
                s.pages,
                s.objects,
                s.elapsed_ms,
                s.start_after.as_deref().unwrap_or(""),
                s.end_before.as_deref().unwrap_or("")
            ));
        }
    }
    append_warnings_and_recommendations(&mut out, &report.warnings, &report.recommendations);
    out
}

pub fn render_trace_summary_markdown(report: &TraceSummaryReport) -> String {
    let mut out = String::new();
    out.push_str("# s3-turbo-list trace summary\n\n");
    out.push_str(&format!("- trace_file: `{}`\n", report.trace_file));
    out.push_str(&format!("- events_total: `{}`\n", report.events_total));
    out.push_str(&format!("- segment_count: `{}`\n", report.segment_count));
    out.push_str(&format!("- total_pages: `{}`\n", report.total_pages));
    out.push_str(&format!("- total_objects: `{}`\n", report.total_objects));
    if let Some(imbalance) = &report.imbalance {
        out.push_str(&format!(
            "- page_imbalance: min `{}`, median `{}`, max `{}`, max/median `{:.2}`\n",
            imbalance.min_pages,
            imbalance.median_pages,
            imbalance.max_pages,
            imbalance.max_to_median_ratio
        ));
    }
    if !report.slowest_segments.is_empty() {
        out.push_str("\n## Slowest segments\n\n");
        out.push_str("| segment | pages | objects | elapsed_ms | start_after | end_before |\n");
        out.push_str("|---:|---:|---:|---:|---|---|\n");
        for s in &report.slowest_segments {
            out.push_str(&format!(
                "| {} | {} | {} | {} | `{}` | `{}` |\n",
                s.segment_index,
                s.pages,
                s.objects,
                s.elapsed_ms,
                s.start_after.as_deref().unwrap_or(""),
                s.end_before.as_deref().unwrap_or("")
            ));
        }
    }
    out
}

pub fn render_merge_text(report: &HintsMergeReport) -> String {
    let mut out = String::new();
    out.push_str("Hints merge:\n");
    out.push_str(&format!("  Inputs:          {}\n", report.input_count));
    out.push_str(&format!(
        "  Boundaries:      {} -> {}\n",
        report.boundary_count_before_dedup, report.boundary_count
    ));
    out.push_str(&format!("  Duplicates:      {}\n", report.duplicate_count));
    out.push_str(&format!(
        "  Output:          {}\n",
        report.output.as_deref().unwrap_or("-")
    ));
    out.push_str(&format!("  Output written:  {}\n", report.output_written));
    append_warnings_and_recommendations(&mut out, &report.warnings, &[]);
    out
}

pub fn render_rebalance_text(report: &HintsRebalanceReport, explain: bool) -> String {
    let mut out = String::new();
    out.push_str("Hints rebalance:\n");
    out.push_str(&format!("  Trace:           {}\n", report.trace_file));
    out.push_str(&format!("  Hints:           {}\n", report.hints_file));
    out.push_str(&format!(
        "  Boundaries:      {} -> {}\n",
        report.original_boundary_count, report.new_boundary_count
    ));
    out.push_str(&format!(
        "  Added:           {}\n",
        report.added_boundaries.len()
    ));
    out.push_str(&format!(
        "  Output:          {}\n",
        report.output.as_deref().unwrap_or("-")
    ));
    out.push_str(&format!("  Output written:  {}\n", report.output_written));
    if explain && !report.long_tail_segments.is_empty() {
        out.push_str("  Long-tail segments:\n");
        for s in &report.long_tail_segments {
            out.push_str(&format!(
                "    - #{} pages={} median={} selected='{}' reason={}\n",
                s.segment_index,
                s.pages,
                s.median_pages,
                s.selected_boundary.as_deref().unwrap_or(""),
                s.reason
            ));
        }
    }
    append_warnings_and_recommendations(&mut out, &report.warnings, &report.recommendations);
    out
}

pub fn write_local_manifest<T: Serialize>(
    path: &str,
    command: &str,
    input_paths: &[String],
    output_paths: &[String],
    report: &T,
    warnings: &[String],
) -> Result<(), String> {
    let manifest = LocalToolManifest {
        schema_version: 1,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        command: command.to_string(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        status: "success".to_string(),
        inputs: input_paths.iter().map(|p| summarize_artifact(p)).collect(),
        outputs: output_paths.iter().map(|p| summarize_artifact(p)).collect(),
        warnings: warnings.to_vec(),
        summary: serde_json::to_value(report).map_err(|e| e.to_string())?,
    };
    let rendered = serde_json::to_string_pretty(&manifest).map_err(|e| e.to_string())?;
    std::fs::write(path, rendered)
        .map_err(|e| format!("failed to write manifest '{}': {}", path, e))
}

fn analyze_trace(path: &str) -> Result<TraceAnalysis, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read trace '{}': {}", path, e))?;
    let mut events_total = 0usize;
    let mut list_events = 0usize;
    let mut retry_events = 0usize;
    let mut error_events = 0usize;
    let mut segments = Vec::new();
    let mut page_samples_by_start: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (line_no, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        events_total += 1;
        let event: S3CompatEvent = serde_json::from_str(trimmed).map_err(|e| {
            format!(
                "trace '{}' line {} is invalid JSON: {}",
                path,
                line_no + 1,
                e
            )
        })?;
        if event.operation == "ListObjectsV2" {
            list_events += 1;
            if event.retry_attempt > 0 {
                retry_events += 1;
            }
            if event.http_status >= 400 || event.fatal || event.s3_error_code.is_some() {
                error_events += 1;
            }
            if let Some(last_key) = event.last_key.clone() {
                page_samples_by_start
                    .entry(event.start_after.clone().unwrap_or_default())
                    .or_default()
                    .push(last_key);
            }
        } else if event.operation == "ListObjectsV2SegmentSummary" {
            segments.push(SegmentSummary {
                segment_index: event.segment_index.unwrap_or(0),
                start_after: event.start_after,
                end_before: event.end_before,
                pages: event.segment_pages.unwrap_or(0),
                objects: event.segment_objects.unwrap_or(0),
                common_prefixes: event.segment_common_prefixes.unwrap_or(0),
                elapsed_ms: event.latency_ms,
                retry_attempt: event.retry_attempt,
                ended_by: event.ended_by,
            });
        }
    }

    segments.sort_by_key(|s| s.segment_index);
    let total_pages = segments.iter().map(|s| s.pages as u64).sum();
    let total_objects = segments.iter().map(|s| s.objects as u64).sum();
    let total_common_prefixes = segments.iter().map(|s| s.common_prefixes as u64).sum();
    let mut slowest_segments = segments.clone();
    slowest_segments.sort_by(|a, b| b.elapsed_ms.cmp(&a.elapsed_ms));
    slowest_segments.truncate(5);

    let imbalance = summarize_imbalance(&segments);
    let mut warnings = Vec::new();
    let mut recommendations = Vec::new();
    if segments.is_empty() {
        warnings.push("trace contains no ListObjectsV2SegmentSummary events".to_string());
        recommendations
            .push("collect a new trace with a v0.1.10+ binary and --trace-compat".to_string());
    }
    if !segments.is_empty() && page_samples_by_start.is_empty() {
        warnings.push("trace contains no per-page last_key samples".to_string());
        recommendations.push(
            "collect a new trace with v0.1.11+ if hints-rebalance should generate new boundaries"
                .to_string(),
        );
    }
    if let Some(i) = &imbalance {
        if i.max_to_median_ratio >= 5.0 {
            recommendations.push(
                "long-tail segment detected; consider hints-rebalance with this trace".to_string(),
            );
        }
    }

    let report = TraceSummaryReport {
        status: "success".to_string(),
        trace_file: path.to_string(),
        events_total,
        list_events,
        segment_count: segments.len(),
        total_pages,
        total_objects,
        total_common_prefixes,
        retry_events,
        error_events,
        slowest_segments,
        imbalance,
        warnings,
        recommendations,
    };

    Ok(TraceAnalysis {
        report,
        segments,
        page_samples_by_start,
    })
}

fn summarize_imbalance(segments: &[SegmentSummary]) -> Option<ImbalanceSummary> {
    if segments.is_empty() {
        return None;
    }
    let mut pages: Vec<u32> = segments.iter().map(|s| s.pages).collect();
    pages.sort_unstable();
    let min_pages = *pages.first().unwrap_or(&0);
    let max_pages = *pages.last().unwrap_or(&0);
    let median_pages = pages[(pages.len() - 1) / 2];
    let median_for_ratio = median_pages.max(1) as f64;
    let min_for_ratio = (min_pages > 0).then(|| max_pages as f64 / min_pages as f64);
    Some(ImbalanceSummary {
        min_pages,
        median_pages,
        max_pages,
        max_to_median_ratio: max_pages as f64 / median_for_ratio,
        max_to_min_ratio: min_for_ratio,
    })
}

fn pick_candidate(
    samples: &[String],
    start_after: &str,
    end_before: Option<&str>,
) -> Option<String> {
    if samples.is_empty() {
        return None;
    }
    let mut candidates: Vec<&String> = samples
        .iter()
        .filter(|key| key.as_str() > start_after)
        .filter(|key| end_before.map(|end| key.as_str() < end).unwrap_or(true))
        .collect();
    candidates.dedup();
    if candidates.is_empty() {
        return None;
    }
    let idx = candidates.len() / 2;
    Some(candidates[idx].clone())
}

fn write_hints_cache(
    path: &str,
    bucket: &str,
    region: Option<String>,
    boundaries: Vec<String>,
    sources: Option<&[String]>,
    overwrite: bool,
) -> Result<(), String> {
    ensure_can_write(path, overwrite)?;
    let cache = HintsCache {
        bucket: bucket.to_string(),
        region,
        prefix: None,
        total_objects: 0,
        boundaries,
        generated_at: chrono::Utc::now().to_rfc3339(),
        source_count: sources.map(|s| s.len()),
        source_files: sources.map(|s| s.to_vec()).unwrap_or_default(),
        max_keys: None,
        max_prefix_entries: None,
        prefix_counts_truncated: false,
        scan_mode: Some(bucket.to_string()),
        sampled_objects: None,
        sampled_pages: None,
        sample_limit: None,
        max_pages: None,
        estimate_mode: Some("local-tooling".to_string()),
        segment_estimates: Vec::<SegmentEstimate>::new(),
    };
    let rendered = toml::to_string_pretty(&cache)
        .map_err(|e| format!("failed to serialize hints TOML: {}", e))?;
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "failed to create output directory '{}': {}",
                parent.display(),
                e
            )
        })?;
    }
    std::fs::write(path, rendered).map_err(|e| format!("failed to write '{}': {}", path, e))
}

fn ensure_can_write(path: &str, overwrite: bool) -> Result<(), String> {
    if !overwrite && Path::new(path).exists() {
        return Err(format!(
            "Output file exists: {}\nNext step:\n  choose a different --output\n  or pass --overwrite",
            path
        ));
    }
    Ok(())
}

fn init_config_warnings(profile: &str) -> Vec<String> {
    match profile {
        "bos" => vec![
            "BOS profile is a compatibility preset; it does not enable BOS pagination workarounds"
                .to_string(),
        ],
        "oss" | "r2" | "b2" => vec![format!(
            "{} profile is documented but should be validated with compat-probe before production use",
            profile
        )],
        _ => Vec::new(),
    }
}

fn init_config_template(profile: &str) -> String {
    let (endpoint, addressing, force_path) = match profile {
        "minio" => ("http://127.0.0.1:9000", "path", "true"),
        "r2" => (
            "https://<account-id>.r2.cloudflarestorage.com",
            "virtual",
            "false",
        ),
        "b2" => ("https://s3.<region>.backblazeb2.com", "virtual", "false"),
        "oss" => ("https://oss-<region>.aliyuncs.com", "virtual", "false"),
        "bos" => ("https://s3.<region>.bcebos.com", "virtual", "false"),
        _ => ("", "auto", "false"),
    };
    let endpoint_line = if endpoint.is_empty() {
        "# endpoint_url = \"https://s3.amazonaws.com\"".to_string()
    } else {
        format!("endpoint_url = \"{}\"", endpoint)
    };
    let profile_line = if profile == "aws" {
        "# profile = \"aws\"".to_string()
    } else {
        format!("profile = \"{}\"", profile)
    };
    format!(
        r#"# s3-turbo-list local config
# AWS credentials are selected with AWS_PROFILE or the standard AWS SDK chain.
# The s3-turbo-list `profile` below is an endpoint compatibility profile.

[runtime]
worker_threads = 10
max_concurrency = 100

[s3]
{endpoint_line}
{profile_line}
addressing_style = "{addressing}"
force_path_style = {force_path}
max_attempts = 10
initial_backoff_secs = 30
connect_timeout_secs = 60
operation_timeout_secs = 5

[output]
row_group_size = 10000
compression = "gzip"
compression_level = 6

[auto_hints]
sample_threshold = 10000
max_prefix_depth = 5
min_segment_size = 1000
max_prefix_entries = 1000000

[channel]
capacity = 64
"#
    )
}

fn append_warnings_and_recommendations(out: &mut String, warnings: &[String], recs: &[String]) {
    if !warnings.is_empty() {
        out.push_str("  Warnings:\n");
        for warning in warnings {
            out.push_str(&format!("    - {}\n", warning));
        }
    }
    if !recs.is_empty() {
        out.push_str("  Recommendations:\n");
        for rec in recs {
            out.push_str(&format!("    - {}\n", rec));
        }
    }
}

fn summarize_artifact(path: &str) -> ArtifactSummary {
    let meta = std::fs::metadata(path);
    let exists = meta.is_ok();
    let size_bytes = meta.ok().map(|m| m.len());
    let sha256 = exists.then(|| sha256_file(path).ok()).flatten();
    ArtifactSummary {
        path: path.to_string(),
        exists,
        size_bytes,
        sha256,
    }
}

fn sha256_file(path: &str) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| format!("failed to open '{}' for hashing: {}", path, e))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("failed to read '{}' for hashing: {}", path, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
