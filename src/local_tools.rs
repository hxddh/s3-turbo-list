use crate::profiles;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

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
    pub check: ManifestCheckSummary,
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
    /// `path` resolved against the run's recorded `cwd` (older manifests:
    /// the current directory, then the manifest's directory).
    #[serde(skip)]
    pub resolved_path: String,
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

#[derive(Debug, Clone, Serialize)]
pub struct ManifestCheckSummary {
    pub ok: bool,
    pub errors: usize,
    pub warnings: usize,
    pub skipped: usize,
    pub artifacts_checked: usize,
    pub artifacts_missing: usize,
    pub row_check: String,
    pub parquet_schema_check: String,
    pub exit_code_check: String,
}

pub fn manifest_summary(
    path: &str,
    verify_artifacts: bool,
) -> Result<ManifestSummaryReport, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read manifest '{}': {}", path, e))?;
    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("failed to parse manifest '{}': {}", path, e))?;
    let metrics = value.get("metrics").ok_or_else(|| {
        if value.get("network").is_some() && value.get("hints").is_some() {
            format!(
                "'{}' is a --dry-run plan, not a run manifest; pass the file written by \
                 --run-manifest after a real run",
                path
            )
        } else {
            format!("manifest '{}' does not contain metrics", path)
        }
    })?;

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
    // Relative artifact paths are relative to the run's working directory,
    // not to wherever --check happens to be invoked.
    let run_cwd = json_string(&value, "cwd").filter(|dir| !dir.is_empty());
    let manifest_dir = Path::new(path).parent().map(Path::to_path_buf);
    // First existing candidate wins: the run's working directory, then the
    // current directory, then the manifest's own directory (a run directory
    // copied or moved elsewhere). The cwd candidate used to be returned
    // unconditionally, so a moved run failed every artifact check.
    let resolve = |artifact_path: &str| -> String {
        let p = Path::new(artifact_path);
        if artifact_path.is_empty() || p.is_absolute() {
            return artifact_path.to_string();
        }
        let mut candidates = Vec::new();
        if let Some(cwd) = run_cwd.as_deref() {
            candidates.push(Path::new(cwd).join(p));
        }
        candidates.push(p.to_path_buf());
        if let Some(dir) = manifest_dir
            .as_deref()
            .filter(|d| !d.as_os_str().is_empty())
        {
            candidates.push(dir.join(p));
        }
        candidates
            .iter()
            .find(|candidate| candidate.exists())
            .unwrap_or(&candidates[0])
            .display()
            .to_string()
    };
    let artifacts: Vec<ManifestArtifactSummary> = value
        .get("artifacts")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .map(|item| ManifestArtifactSummary {
                    kind: json_string(item, "kind").unwrap_or_default(),
                    path: json_string(item, "path").unwrap_or_default(),
                    resolved_path: resolve(&json_string(item, "path").unwrap_or_default()),
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
    let check_summary = manifest_check_summary(check_passed, &checks, &artifacts);

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
        check: check_summary,
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

fn manifest_check_summary(
    check_passed: bool,
    checks: &[ManifestCheck],
    artifacts: &[ManifestArtifactSummary],
) -> ManifestCheckSummary {
    ManifestCheckSummary {
        ok: check_passed,
        errors: checks.iter().filter(|check| check.status == "fail").count(),
        warnings: checks.iter().filter(|check| check.status == "warn").count(),
        skipped: checks.iter().filter(|check| check.status == "skip").count(),
        artifacts_checked: artifacts.len(),
        artifacts_missing: checks
            .iter()
            .filter(|check| check.name.starts_with("artifact_exists:") && check.status == "fail")
            .count(),
        row_check: manifest_check_status(checks, "parquet_rows_match_streamed_rows"),
        // One entry per Parquet artifact (a pooled run writes several), so the
        // summary reports the worst of them rather than whichever came first.
        // A missing or unreadable Parquet artifact has no schema to compare;
        // that is a failure of this check, not "not applicable".
        parquet_schema_check: worst_status(
            manifest_check_worst(checks, "artifact_parquet_schema:parquet"),
            manifest_check_worst(checks, "artifact_parquet_metadata:parquet"),
        ),
        exit_code_check: manifest_check_status(checks, "exit_code"),
    }
}

fn worst_status(a: String, b: String) -> String {
    let rank = |s: &str| match s {
        "fail" => 3,
        "warn" => 2,
        "ok" => 1,
        _ => 0,
    };
    if rank(&b) > rank(&a) { b } else { a }
}

/// Worst status across `name` and its `name#N` siblings: `fail` beats `warn`
/// beats `ok`, and `not_applicable` only when there is nothing to report.
fn manifest_check_worst(checks: &[ManifestCheck], name: &str) -> String {
    let matching = checks.iter().filter(|check| {
        check.name == name
            || check
                .name
                .strip_prefix(name)
                .is_some_and(|rest| rest.starts_with('#'))
    });
    let mut worst: Option<String> = None;
    for check in matching {
        let status = normalize_check_status(&check.status);
        let rank = |s: &str| match s {
            "fail" => 3,
            "warn" => 2,
            "ok" => 1,
            _ => 0,
        };
        if worst.as_deref().is_none_or(|w| rank(&status) > rank(w)) {
            worst = Some(status);
        }
    }
    worst.unwrap_or_else(|| "not_applicable".to_string())
}

fn manifest_check_status(checks: &[ManifestCheck], name: &str) -> String {
    checks
        .iter()
        .find(|check| check.name == name)
        .map(|check| normalize_check_status(&check.status))
        .unwrap_or_else(|| "not_applicable".to_string())
}

fn normalize_check_status(status: &str) -> String {
    match status {
        "ok" | "fail" | "warn" => status.to_string(),
        "skip" => "not_applicable".to_string(),
        other => other.to_string(),
    }
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
        // The artifacts must hold the rows the metrics claim. Comparing two
        // in-memory counters (below) cannot catch a short or missing file.
        let artifact_rows: i64 = artifacts
            .iter()
            .filter(|artifact| artifact.kind == "parquet")
            .filter_map(|artifact| artifact.parquet_row_count)
            .sum();
        checks.push(ManifestCheck {
            name: "artifact_parquet_rows_total".to_string(),
            status: if artifact_rows == parquet_rows as i64 {
                "ok"
            } else {
                "fail"
            }
            .to_string(),
            message: format!(
                "sum of recorded Parquet artifact rows={} metrics.parquet_rows={}",
                artifact_rows, parquet_rows
            ),
        });
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
        checks.push(ManifestCheck {
            name: "artifact_parquet_metadata:parquet".to_string(),
            status: "skip".to_string(),
            message: format!(
                "Parquet row and schema metadata checks are not applicable for {} output; recorded artifact existence, size, and sha256 checks still apply when artifacts are present",
                if summary_only {
                    "summary-only".to_string()
                } else {
                    output_format.unwrap_or("unknown").to_string()
                }
            ),
        });
    }

    // Several artifacts can share a kind — a pooled list run records one
    // `parquet` entry per writer. The first keeps the bare `:<kind>` check
    // name so existing consumers still find it; the rest are suffixed with
    // their index, so a failing part is attributable to its file instead of
    // hiding behind a duplicate name.
    let mut seen_kinds: HashMap<&str, usize> = HashMap::new();
    for artifact in artifacts {
        let occurrence = seen_kinds
            .entry(artifact.kind.as_str())
            .and_modify(|count| *count += 1)
            .or_insert(0);
        let label = if *occurrence == 0 {
            artifact.kind.clone()
        } else {
            format!("{}#{}", artifact.kind, occurrence)
        };
        let current_exists =
            !artifact.resolved_path.is_empty() && Path::new(&artifact.resolved_path).exists();
        checks.push(ManifestCheck {
            name: format!("artifact_exists:{}", label),
            status: if current_exists { "ok" } else { "fail" }.to_string(),
            message: format!(
                "{} recorded_exists={} current_exists={}",
                artifact.path, artifact.exists, current_exists
            ),
        });

        if verify_artifacts && !current_exists && artifact.kind == "parquet" {
            // Nothing to compare the recorded schema against.
            checks.push(ManifestCheck {
                name: format!("artifact_parquet_metadata:{}", label),
                status: "fail".to_string(),
                message: format!(
                    "{} is missing; its schema cannot be verified",
                    artifact.path
                ),
            });
        }
        if !verify_artifacts || !current_exists {
            continue;
        }

        if let Some(recorded_size) = artifact.size_bytes {
            let current_size = std::fs::metadata(&artifact.resolved_path)
                .ok()
                .map(|m| m.len());
            checks.push(ManifestCheck {
                name: format!("artifact_size:{}", label),
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
            let current_sha256 = crate::agent::sha256_file(&artifact.resolved_path).ok();
            checks.push(ManifestCheck {
                name: format!("artifact_sha256:{}", label),
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

        // The writer recorded no Parquet metadata for this file: its footer
        // was unreadable when the manifest was written. Skipping it — as
        // --check used to — passed a file that is not a listing at all.
        if artifact.kind == "parquet"
            && artifact.parquet_row_count.is_none()
            && artifact.parquet_schema_fields.is_empty()
        {
            checks.push(ManifestCheck {
                name: format!("artifact_parquet_metadata:{}", label),
                status: "fail".to_string(),
                message: format!(
                    "{} has no recorded Parquet metadata (unreadable footer when the run ended)",
                    artifact.path
                ),
            });
            continue;
        }

        if artifact.kind == "parquet"
            && (artifact.parquet_row_count.is_some() || !artifact.parquet_schema_fields.is_empty())
        {
            match current_parquet_summary(&artifact.resolved_path) {
                Ok(current) => {
                    if let Some(recorded_rows) = artifact.parquet_row_count {
                        checks.push(ManifestCheck {
                            name: format!("artifact_parquet_rows:{}", label),
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
                            name: format!("artifact_parquet_schema:{}", label),
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
                    name: format!("artifact_parquet_metadata:{}", label),
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
    // streamed_rows counts every row written, which for a --delimiter run
    // includes one row per folder (CommonPrefix) — not objects.
    let delimited = report
        .command
        .windows(2)
        .any(|pair| pair[0] == "--delimiter" && !pair[1].is_empty())
        || report.command.iter().any(|arg| {
            arg.strip_prefix("--delimiter=")
                .is_some_and(|v| !v.is_empty())
        });
    if delimited {
        out.push_str(&format!(
            "  Rows:         {} (objects and folders)\n",
            report.streamed_rows
        ));
    } else {
        out.push_str(&format!("  Objects:      {}\n", report.streamed_rows));
    }
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
        out.push_str(
            "  Artifact check: Parquet row/schema checks are not applicable; recorded artifact size/hash checks still apply\n",
        );
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

pub fn human_bytes(bytes: u64) -> String {
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

/// `guide`: the overview, or one provider's quickstart and facts.
pub fn render_guide(topic: Option<&str>) -> Result<String, String> {
    match topic {
        None => Ok(render_overview()),
        Some(name) => match profiles::get_profile(name) {
            Some(profile) => Ok(format!(
                "{}\n{}",
                render_quickstart(profile.name),
                render_profile_facts(profile)
            )),
            None => Err(format!(
                "unknown guide topic '{}': use one of {} (or no topic for the overview)",
                name,
                profile_names()
            )),
        },
    }
}

fn profile_names() -> String {
    profiles::all_profiles()
        .iter()
        .map(|p| p.name)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_overview() -> String {
    format!(
        r#"s3-turbo-list guide

First run:
  s3-turbo-list doctor
  s3-turbo-list list --bucket my-bucket --region us-east-1 --output-dir out --dry-run
  s3-turbo-list list --bucket my-bucket --region us-east-1 --output-dir out

Other shapes:
  list ... --output-format tsv | ndjson     rows on stdout, for pipes
  list ... --output-format summary          counts only
  list ... --resume                         continue after Ctrl-C
  diff --bucket a --target-bucket b ...     one Parquet file with a flag per key
  manifest-summary run.json --check         verify a run's outputs

Credentials come from the AWS SDK chain (AWS_PROFILE, env vars, ...);
--provider only selects an S3-compatible endpoint preset.

Provider quickstarts: s3-turbo-list guide <{}>
"#,
        profile_names().replace(", ", "|")
    )
}

fn render_quickstart(provider: &str) -> String {
    // (credentials, provider options, list options). compat-probe takes the
    // same endpoint options as the listing, so its line reuses them.
    let (setup, endpoint, list) = match provider {
        "minio" => (
            "export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin",
            "--provider minio --endpoint-url http://127.0.0.1:9000",
            "--bucket my-bucket --region us-east-1",
        ),
        "r2" => (
            "export AWS_PROFILE=my-r2-credentials",
            "--provider r2 --endpoint-url https://<account-id>.r2.cloudflarestorage.com",
            "--bucket my-bucket",
        ),
        "bos" => (
            "export AWS_PROFILE=my-bos-credentials",
            "--provider bos",
            "--bucket my-bucket --region bj",
        ),
        "b2" => (
            "export AWS_PROFILE=my-b2-credentials",
            "--provider b2",
            "--bucket my-bucket --region us-west-004",
        ),
        "oss" => (
            "export AWS_PROFILE=my-oss-credentials",
            "--provider oss",
            "--bucket my-bucket --region oss-cn-beijing",
        ),
        _ => (
            "export AWS_PROFILE=default",
            "",
            "--bucket my-bucket --region us-east-1",
        ),
    };
    let global = if endpoint.is_empty() {
        String::new()
    } else {
        format!("{} ", endpoint)
    };
    let probe = if endpoint.is_empty() {
        String::new()
    } else {
        format!(
            "  s3-turbo-list {}compat-probe {}   # check the endpoint first\n",
            global, list
        )
    };
    format!(
        "{} quickstart:\n  {}\n{}  s3-turbo-list {}list {} --output-dir out\n",
        provider, setup, probe, global, list
    )
}

fn render_profile_facts(profile: &profiles::EndpointProfile) -> String {
    let mut out = String::new();
    out.push_str(&format!("Provider preset '{}':\n", profile.name));
    out.push_str(&format!("  provider: {}\n", profile.provider));
    out.push_str(&format!("  status: {}\n", profile.status));
    out.push_str(&format!(
        "  addressing style: {}\n",
        profile.recommended_addressing_style
    ));
    if let Some(template) = profile.endpoint_template {
        out.push_str(&format!("  endpoint: {} (from --region)\n", template));
    } else if profile.requires_explicit_endpoint {
        out.push_str("  endpoint: pass --endpoint-url\n");
    }
    if let Some(region) = profile.default_region {
        out.push_str(&format!("  default region: {}\n", region));
    }
    for note in profile.notes {
        out.push_str(&format!("  - {}\n", note));
    }
    out
}
