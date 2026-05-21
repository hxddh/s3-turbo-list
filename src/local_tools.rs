use crate::auto_hints::{HintsCache, SegmentEstimate};
use crate::hints;
use crate::profiles;
use crate::trace::S3CompatEvent;
use parquet::file::reader::{FileReader, SerializedFileReader};
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

#[derive(Debug, Clone, Serialize)]
pub struct ManifestSummaryReport {
    pub status: String,
    pub manifest_file: String,
    pub tool_version: Option<String>,
    pub run_status: String,
    pub exit_code: Option<i64>,
    pub elapsed_secs: Option<f64>,
    pub command: Vec<String>,
    pub output_format: Option<String>,
    pub summary_only: bool,
    pub received_objects: u64,
    pub streamed_rows: u64,
    pub parquet_rows: u64,
    pub ks_entries: u64,
    pub bytes_total: u64,
    pub unique_prefixes: u64,
    pub parquet_rows_match_streamed_rows: Option<bool>,
    pub top_prefixes: Vec<ManifestPrefixSummary>,
    pub outputs: ManifestOutputSummary,
    pub artifacts: Vec<ManifestArtifactSummary>,
    pub warnings: Vec<String>,
    pub check_passed: bool,
    pub checks: Vec<ManifestCheck>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManifestPrefixSummary {
    pub prefix: String,
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManifestOutputSummary {
    pub parquet_file: Option<String>,
    pub ks_file: Option<String>,
    pub hints_file: Option<String>,
    pub trace_compat: Option<String>,
    pub log_file: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManifestArtifactSummary {
    pub kind: String,
    pub path: String,
    pub exists: bool,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub parquet_row_count: Option<i64>,
    pub parquet_schema_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManifestCheck {
    pub name: String,
    pub status: String,
    pub message: String,
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

pub fn manifest_summary(
    path: &str,
    verify_artifacts: bool,
) -> Result<ManifestSummaryReport, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read manifest '{}': {}", path, e))?;
    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("failed to parse manifest '{}': {}", path, e))?;
    let metrics = value
        .get("metrics")
        .ok_or_else(|| format!("manifest '{}' does not contain metrics", path))?;

    let streamed_rows = json_u64(metrics, "streamed_rows");
    let parquet_rows = json_u64(metrics, "parquet_rows");
    let output_format = value
        .get("inputs")
        .and_then(|v| json_string(v, "output_format"));
    let summary_only = metrics
        .get("summary_only")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let row_check_applies = manifest_row_check_applies(output_format.as_deref(), summary_only);
    let parquet_rows_match_streamed_rows =
        row_check_applies.then_some(streamed_rows == parquet_rows);
    let run_status = json_string(&value, "status").unwrap_or_else(|| "unknown".to_string());
    let exit_code = value.get("exit_code").and_then(|v| v.as_i64());
    let fatal_errors = json_u64(metrics, "fatal_errors");
    let output_errors = json_u64(metrics, "output_errors");
    let artifacts: Vec<ManifestArtifactSummary> = value
        .get("artifacts")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .map(|item| ManifestArtifactSummary {
                    kind: json_string(item, "kind").unwrap_or_default(),
                    path: json_string(item, "path").unwrap_or_default(),
                    exists: item
                        .get("exists")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    size_bytes: item.get("size_bytes").and_then(|v| v.as_u64()),
                    sha256: json_string(item, "sha256"),
                    parquet_row_count: item
                        .get("parquet")
                        .and_then(|v| v.get("row_count"))
                        .and_then(|v| v.as_i64()),
                    parquet_schema_fields: item
                        .get("parquet")
                        .and_then(|v| v.get("schema_fields"))
                        .and_then(|v| v.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default();
    let checks = manifest_checks(
        &run_status,
        exit_code,
        fatal_errors,
        output_errors,
        output_format.as_deref(),
        summary_only,
        streamed_rows,
        parquet_rows,
        &artifacts,
        verify_artifacts,
    );
    let check_passed = checks.iter().all(|check| check.status != "fail");

    Ok(ManifestSummaryReport {
        status: "success".to_string(),
        manifest_file: path.to_string(),
        tool_version: json_string(&value, "tool_version"),
        run_status,
        exit_code,
        elapsed_secs: value.get("elapsed_secs").and_then(|v| v.as_f64()),
        command: value
            .get("command")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        output_format,
        summary_only,
        received_objects: json_u64(metrics, "received_objects"),
        streamed_rows,
        parquet_rows,
        ks_entries: json_u64(metrics, "ks_entries"),
        bytes_total: json_u64(metrics, "bytes_total"),
        unique_prefixes: json_u64(metrics, "unique_prefixes"),
        parquet_rows_match_streamed_rows,
        top_prefixes: metrics
            .get("top_prefixes")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .map(|item| ManifestPrefixSummary {
                        prefix: json_string(item, "prefix").unwrap_or_default(),
                        objects: json_u64(item, "objects"),
                        bytes: json_u64(item, "bytes"),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        outputs: ManifestOutputSummary {
            parquet_file: value
                .get("outputs")
                .and_then(|v| json_string(v, "parquet_file")),
            ks_file: value.get("outputs").and_then(|v| json_string(v, "ks_file")),
            hints_file: value
                .get("outputs")
                .and_then(|v| json_string(v, "hints_file")),
            trace_compat: value
                .get("outputs")
                .and_then(|v| json_string(v, "trace_compat")),
            log_file: value
                .get("outputs")
                .and_then(|v| json_string(v, "log_file")),
        },
        artifacts,
        warnings: value
            .get("warnings")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        check_passed,
        checks,
    })
}

fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn json_u64(value: &serde_json::Value, key: &str) -> u64 {
    value.get(key).and_then(|v| v.as_u64()).unwrap_or(0)
}

fn manifest_row_check_applies(output_format: Option<&str>, summary_only: bool) -> bool {
    if summary_only {
        return false;
    }
    matches!(output_format.unwrap_or("parquet"), "parquet")
}

fn manifest_checks(
    run_status: &str,
    exit_code: Option<i64>,
    fatal_errors: u64,
    output_errors: u64,
    output_format: Option<&str>,
    summary_only: bool,
    streamed_rows: u64,
    parquet_rows: u64,
    artifacts: &[ManifestArtifactSummary],
    verify_artifacts: bool,
) -> Vec<ManifestCheck> {
    let mut checks = Vec::new();
    checks.push(ManifestCheck {
        name: "run_status".to_string(),
        status: if run_status == "success" {
            "ok"
        } else {
            "fail"
        }
        .to_string(),
        message: format!("manifest status is {}", run_status),
    });
    checks.push(ManifestCheck {
        name: "exit_code".to_string(),
        status: if exit_code == Some(0) { "ok" } else { "fail" }.to_string(),
        message: format!(
            "manifest exit_code is {}",
            exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "missing".to_string())
        ),
    });
    checks.push(ManifestCheck {
        name: "fatal_errors".to_string(),
        status: if fatal_errors == 0 { "ok" } else { "fail" }.to_string(),
        message: format!("metrics.fatal_errors is {}", fatal_errors),
    });
    checks.push(ManifestCheck {
        name: "output_errors".to_string(),
        status: if output_errors == 0 { "ok" } else { "fail" }.to_string(),
        message: format!("metrics.output_errors is {}", output_errors),
    });

    if manifest_row_check_applies(output_format, summary_only) {
        checks.push(ManifestCheck {
            name: "parquet_rows_match_streamed_rows".to_string(),
            status: if parquet_rows == streamed_rows {
                "ok"
            } else {
                "fail"
            }
            .to_string(),
            message: format!(
                "parquet_rows={} streamed_rows={}",
                parquet_rows, streamed_rows
            ),
        });
    } else {
        checks.push(ManifestCheck {
            name: "parquet_rows_match_streamed_rows".to_string(),
            status: "skip".to_string(),
            message: format!(
                "row check is not applicable for {} output",
                if summary_only {
                    "summary-only".to_string()
                } else {
                    output_format.unwrap_or("unknown").to_string()
                }
            ),
        });
    }

    for artifact in artifacts {
        let current_exists = !artifact.path.is_empty() && Path::new(&artifact.path).exists();
        checks.push(ManifestCheck {
            name: format!("artifact_exists:{}", artifact.kind),
            status: if current_exists { "ok" } else { "fail" }.to_string(),
            message: format!(
                "{} recorded_exists={} current_exists={}",
                artifact.path, artifact.exists, current_exists
            ),
        });

        if !verify_artifacts || !current_exists {
            continue;
        }

        if let Some(recorded_size) = artifact.size_bytes {
            let current_size = std::fs::metadata(&artifact.path).ok().map(|m| m.len());
            checks.push(ManifestCheck {
                name: format!("artifact_size:{}", artifact.kind),
                status: if current_size == Some(recorded_size) {
                    "ok"
                } else {
                    "fail"
                }
                .to_string(),
                message: format!(
                    "{} recorded_size={} current_size={}",
                    artifact.path,
                    recorded_size,
                    current_size
                        .map(|size| size.to_string())
                        .unwrap_or_else(|| "missing".to_string())
                ),
            });
        }

        if let Some(recorded_sha256) = artifact.sha256.as_deref() {
            let current_sha256 = sha256_file(&artifact.path).ok();
            checks.push(ManifestCheck {
                name: format!("artifact_sha256:{}", artifact.kind),
                status: if current_sha256.as_deref() == Some(recorded_sha256) {
                    "ok"
                } else {
                    "fail"
                }
                .to_string(),
                message: format!(
                    "{} recorded_sha256={} current_sha256={}",
                    artifact.path,
                    recorded_sha256,
                    current_sha256.unwrap_or_else(|| "unavailable".to_string())
                ),
            });
        }

        if artifact.kind == "parquet"
            && (artifact.parquet_row_count.is_some() || !artifact.parquet_schema_fields.is_empty())
        {
            match current_parquet_summary(&artifact.path) {
                Ok(current) => {
                    if let Some(recorded_rows) = artifact.parquet_row_count {
                        checks.push(ManifestCheck {
                            name: "artifact_parquet_rows:parquet".to_string(),
                            status: if current.row_count == recorded_rows {
                                "ok"
                            } else {
                                "fail"
                            }
                            .to_string(),
                            message: format!(
                                "{} recorded_rows={} current_rows={}",
                                artifact.path, recorded_rows, current.row_count
                            ),
                        });
                    }
                    if !artifact.parquet_schema_fields.is_empty() {
                        checks.push(ManifestCheck {
                            name: "artifact_parquet_schema:parquet".to_string(),
                            status: if current.schema_fields == artifact.parquet_schema_fields {
                                "ok"
                            } else {
                                "fail"
                            }
                            .to_string(),
                            message: format!(
                                "{} recorded_schema={:?} current_schema={:?}",
                                artifact.path,
                                artifact.parquet_schema_fields,
                                current.schema_fields
                            ),
                        });
                    }
                }
                Err(e) => checks.push(ManifestCheck {
                    name: "artifact_parquet_metadata:parquet".to_string(),
                    status: "fail".to_string(),
                    message: format!("{} metadata read failed: {}", artifact.path, e),
                }),
            }
        }
    }

    checks
}

struct CurrentParquetSummary {
    row_count: i64,
    schema_fields: Vec<String>,
}

fn current_parquet_summary(path: &str) -> Result<CurrentParquetSummary, String> {
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    let metadata = reader.metadata();
    let schema_fields = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    Ok(CurrentParquetSummary {
        row_count: metadata.file_metadata().num_rows(),
        schema_fields,
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

pub fn render_manifest_summary_text(report: &ManifestSummaryReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("Manifest: {}\n", report.manifest_file));
    out.push_str(&format!("  Status:       {}\n", report.run_status));
    if let Some(code) = report.exit_code {
        out.push_str(&format!("  Exit code:    {}\n", code));
    }
    if let Some(elapsed) = report.elapsed_secs {
        out.push_str(&format!("  Elapsed:      {:.3}s\n", elapsed));
    }
    out.push_str(&format!("  Summary only: {}\n", report.summary_only));
    if let Some(format) = &report.output_format {
        out.push_str(&format!("  Output format: {}\n", format));
    }
    out.push_str(&format!("  Objects:      {}\n", report.streamed_rows));
    out.push_str(&format!(
        "  Bytes:        {} ({})\n",
        report.bytes_total,
        human_bytes(report.bytes_total)
    ));
    out.push_str(&format!("  Prefixes:     {}\n", report.unique_prefixes));
    out.push_str(&format!("  Parquet rows: {}\n", report.parquet_rows));
    if let Some(matches) = report.parquet_rows_match_streamed_rows {
        out.push_str(&format!(
            "  Row check:    parquet_rows == streamed_rows: {}\n",
            matches
        ));
    } else {
        out.push_str("  Row check:    parquet_rows == streamed_rows: not applicable\n");
    }
    out.push_str(&format!(
        "  Check:        {}\n",
        if report.check_passed { "PASS" } else { "FAIL" }
    ));
    if !report.top_prefixes.is_empty() {
        out.push_str("Top prefixes:\n");
        for prefix in report.top_prefixes.iter().take(10) {
            out.push_str(&format!(
                "  {}  objects={} bytes={} ({})\n",
                prefix.prefix,
                prefix.objects,
                prefix.bytes,
                human_bytes(prefix.bytes)
            ));
        }
    }
    if report.outputs.parquet_file.is_some()
        || report.outputs.ks_file.is_some()
        || report.outputs.trace_compat.is_some()
        || report.outputs.log_file.is_some()
    {
        out.push_str("Outputs:\n");
        if let Some(path) = &report.outputs.parquet_file {
            out.push_str(&format!("  Parquet:  {}\n", path));
        }
        if let Some(path) = &report.outputs.ks_file {
            out.push_str(&format!("  KeySpace: {}\n", path));
        }
        if let Some(path) = &report.outputs.trace_compat {
            out.push_str(&format!("  Trace:    {}\n", path));
        }
        if let Some(path) = &report.outputs.log_file {
            out.push_str(&format!("  Log:      {}\n", path));
        }
    }
    if !report.warnings.is_empty() {
        out.push_str("Warnings:\n");
        for warning in &report.warnings {
            out.push_str(&format!("  - {}\n", warning));
        }
    }
    if !report.checks.is_empty() && !report.check_passed {
        out.push_str("Failed checks:\n");
        for check in &report.checks {
            if check.status == "fail" {
                out.push_str(&format!("  - {}: {}\n", check.name, check.message));
            }
        }
    }
    out
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = UNITS[0];
    for next_unit in UNITS.iter().skip(1) {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next_unit;
    }
    if unit == "B" {
        format!("{} {}", bytes, unit)
    } else {
        format!("{:.2} {}", value, unit)
    }
}

pub fn render_recipe(name: Option<&str>) -> Result<String, String> {
    let name = name.unwrap_or("index");
    match name {
        "index" | "list" => Ok(
            r#"Available recipes:
  aws-basic      Minimal AWS S3 dry-run and list
  summary        Count objects and bytes without Parquet/KS outputs
  pipe           Stream list results to shell tools or agents
  verify         Validate a saved run manifest locally
  diff-safe      Authoritative single-segment diff workflow
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
        "summary" => Ok(
            r#"Summary only:
  export AWS_PROFILE=default
  s3-turbo-list doctor --local-only --simple
  s3-turbo-list --dry-run --agent --summary-only --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list --summary-only --run-manifest summary.json --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list manifest-summary summary.json
"#
            .to_string(),
        ),
        "pipe" => Ok(
            r#"Pipe-friendly list:
  export AWS_PROFILE=default
  s3-turbo-list --delimiter '' list --bucket my-bucket --region us-east-1 --output-format tsv | wc -l
  s3-turbo-list --delimiter '' list --bucket my-bucket --region us-east-1 --output-format tsv | awk -F '\t' '{bytes += $2} END {print bytes}'
  s3-turbo-list --delimiter '' list --bucket my-bucket --region us-east-1 --output-format ndjson | jq -r '.k'
  s3-turbo-list --delimiter '' --run-manifest run.json list --bucket my-bucket --region us-east-1 --output-format ndjson > objects.ndjson
  s3-turbo-list manifest-summary run.json --json
"#
            .to_string(),
        ),
        "verify" => Ok(
            r#"Verify a saved run:
  s3-turbo-list --run-manifest run.json --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list manifest-summary run.json
  s3-turbo-list manifest-summary run.json --check

Pipe output with a manifest:
  s3-turbo-list --run-manifest run.json --delimiter '' list --bucket my-bucket --region us-east-1 --output-format ndjson > objects.ndjson
  s3-turbo-list manifest-summary run.json --check
"#
            .to_string(),
        ),
        "diff-safe" => Ok(
            r#"Safe diff:
  export AWS_PROFILE=default
  s3-turbo-list --dry-run --agent --output-dir out --delimiter '' diff --bucket source-bucket --region us-east-1 --target-bucket target-bucket --target-region us-east-1
  s3-turbo-list --run-manifest diff-run.json --output-dir out --delimiter '' diff --bucket source-bucket --region us-east-1 --target-bucket target-bucket --target-region us-east-1
  s3-turbo-list manifest-summary diff-run.json --check
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
  s3-turbo-list manifest-summary run.json --check
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
        scan_mode: None,
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
    let endpoint = match profile {
        "minio" => "http://127.0.0.1:9000",
        "r2" => "https://<account-id>.r2.cloudflarestorage.com",
        "b2" => "https://s3.<region>.backblazeb2.com",
        "oss" => "https://oss-<region>.aliyuncs.com",
        "bos" => "https://s3.<region>.bcebos.com",
        _ => "",
    };
    let (addressing, force_path) = if let Some(profile) = profiles::get_profile(profile) {
        let addressing = profile.recommended_addressing_style.to_string();
        let force_path = matches!(
            profile.recommended_addressing_style,
            crate::config::AddressingStyle::Path
        );
        (addressing, force_path.to_string())
    } else {
        ("auto".to_string(), "false".to_string())
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
