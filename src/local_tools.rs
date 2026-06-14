use crate::profiles;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;

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
        parquet_schema_check: manifest_check_status(checks, "artifact_parquet_schema:parquet"),
        exit_code_check: manifest_check_status(checks, "exit_code"),
    }
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
    out.push_str("  s3-turbo-list doctor --simple\n");
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

/// Providers with a hand-written quickstart, dispatched ahead of named recipes
/// so that a bare provider name (e.g. `guide r2`) prints its quickstart.
fn is_quickstart_provider(topic: &str) -> bool {
    matches!(topic, "aws" | "minio" | "r2" | "bos")
}

/// Single guidance surface: bare `guide` prints the overview; a provider name
/// (any known endpoint profile, e.g. aws/minio/r2/b2/oss/bos) prints that
/// provider's quickstart followed by its endpoint-compatibility facts; anything
/// else is treated as a recipe name (including `index`/`list`).
pub fn render_guide(topic: Option<&str>) -> Result<String, String> {
    match topic {
        None => Ok(render_overview()),
        Some(provider) if profiles::get_profile(provider).is_some() => {
            // A provider name resolves to an endpoint profile. Print the
            // hand-written quickstart when one exists (aws/minio/r2/bos), then
            // append the profile's compatibility facts so providers without a
            // quickstart (b2/oss) still produce guidance.
            let mut out = String::new();
            if is_quickstart_provider(provider) {
                out.push_str(&render_quickstart(provider)?);
                out.push('\n');
            }
            out.push_str(&render_profile_facts(provider));
            Ok(out)
        }
        Some(name) => render_recipe(Some(name)),
    }
}

/// Plain-text endpoint-compatibility facts for a known profile, matching the
/// former `profiles show` output (doctor/guide absorbed that command).
fn render_profile_facts(name: &str) -> String {
    let Some(profile) = profiles::get_profile(name) else {
        return String::new();
    };
    let mut out = String::new();
    out.push_str("Endpoint compatibility profile:\n");
    out.push_str(&format!("  provider: {}\n", profile.provider));
    out.push_str(&format!("  status: {}\n", profile.status));
    out.push_str(&format!(
        "  recommended_addressing_style: {}\n",
        profile.recommended_addressing_style
    ));
    out.push_str(&format!(
        "  default_endpoint_url: {}\n",
        profile.default_endpoint_url.unwrap_or("-")
    ));
    out.push_str(&format!(
        "  requires_explicit_endpoint: {}\n",
        profile.requires_explicit_endpoint
    ));
    if !profile.notes.is_empty() {
        out.push_str("  notes:\n");
        for note in profile.notes {
            out.push_str(&format!("    - {}\n", note));
        }
    }
    out
}

fn render_recipe(name: Option<&str>) -> Result<String, String> {
    let name = name.unwrap_or("index");
    match name {
        "index" | "list" => Ok(
            r#"Available recipes:
  aws-basic      Minimal AWS S3 dry-run and list
  summary        Count objects and bytes without Parquet/KS outputs
  pipe           Stream list results to shell tools or agents
  filter         Local object filter examples and limits
  verify         Validate a saved run manifest locally
  release-check  Local pre-release checks without contacting S3
  diff-safe      Authoritative single-segment diff workflow
  large-bucket   Automatic partitioning and output-dir workflow
  local-minio    Local MinIO endpoint example
  agent-safe     Local-only agent/CI commands

Run: s3-turbo-list guide <name>
"#
            .to_string(),
        ),
        "aws-basic" => Ok(
            r#"AWS basic:
  export AWS_PROFILE=default
  s3-turbo-list doctor --simple
  s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        "summary" => Ok(
            r#"Summary only:
  export AWS_PROFILE=default
  s3-turbo-list doctor --simple
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
        "filter" => Ok(
            r#"Object filters:
  # Filters are local: they run after S3 listing and before output.
  # Use prefix/delimiter/max-keys for request-side shaping.

  # Keep objects larger than 1 GiB
  s3-turbo-list --filter 'SOURCE.size > 1073741824' --delimiter '' list --bucket my-bucket --region us-east-1

  # Keep recently modified objects by epoch seconds
  s3-turbo-list --filter 'SOURCE.last_modified >= 1715700000' --delimiter '' list --bucket my-bucket --region us-east-1

  # Diff-only: keep rows where source and target sizes differ
  s3-turbo-list --filter 'SOURCE.size != TARGET.size' diff --bucket left-bucket --target-bucket right-bucket

Allowed: SOURCE/TARGET size and last_modified numeric comparisons, arithmetic, &&, ||, !.
Rejected before network: functions, methods, strings, arrays, maps, indexing, statements, large/deep expressions.
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
        "release-check" | "ci" => Ok(
            r#"Release check (local only):
  VERSION=$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
  ./scripts/check-release-env.sh
  cargo fmt --check
  cargo check
  cargo clippy --all-targets -- -D warnings
  cargo test
  cargo build
  for f in examples/*.sh; do bash -n "$f" || exit 1; done
  python3 -m py_compile examples/read-parquet.py
  python3 -m py_compile examples/inspect-trace.py

Benchmark smoke checks:
  BUILD_MODE=clang OBJECTS=1000 BATCH_SIZE=100 PREFIXES=16 ./scripts/benchmark-local.sh
  BIN=./target/release/s3-turbo-list OBJECTS=1000 BATCH_SIZE=100 PREFIXES=16 ./scripts/benchmark-local.sh

Release build on Ubuntu 20.04 arm64:
  BUILD_MODE=clang ./scripts/build-release.sh

Release publication checks:
  gh workflow run release-assets.yml --repo hxddh/s3-turbo-list -f tag="v${VERSION}"
  RUN_ID=$(gh run list --repo hxddh/s3-turbo-list --workflow release-assets.yml --limit 1 --json databaseId --jq '.[0].databaseId')
  gh run view "$RUN_ID" --repo hxddh/s3-turbo-list --json status,conclusion,jobs
  ./scripts/verify-release-assets.sh "v${VERSION}"
  git rev-parse main origin/main "v${VERSION}^{}"

These commands do not contact S3-compatible cloud endpoints.
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
  # Key-space partitioning is automatic: the first run probes the bucket
  # structure at startup, lists in parallel, and caches the boundaries.
  s3-turbo-list --output-dir out --delimiter '' -c 8 -T 4 list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        "local-minio" => Ok(
            r#"Local MinIO:
  export AWS_ACCESS_KEY_ID=minioadmin
  export AWS_SECRET_ACCESS_KEY=minioadmin
  s3-turbo-list --profile minio --endpoint-url http://127.0.0.1:9000 --addressing-style path --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        "agent-safe" => Ok(
            r#"Agent-safe local commands:
  s3-turbo-list doctor --json
  s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
"#
            .to_string(),
        ),
        other => Err(format!(
            "unknown recipe '{}'. Run 's3-turbo-list recipes' to list recipes.",
            other
        )),
    }
}

fn render_overview() -> String {
    r#"s3-turbo-list guide

First run:
  s3-turbo-list doctor --simple
  s3-turbo-list init-config --output s3-turbo-list.toml
  s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
  s3-turbo-list --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1

Credentials vs endpoint profiles:
  export AWS_PROFILE=my-credentials-profile
  s3-turbo-list --profile r2 ...

More guidance (s3-turbo-list guide <topic>):
  guide aws | minio | r2 | b2 | oss | bos   provider quickstarts and compat facts
  guide index                               list all recipes
  guide filter | release-check | large-bucket

Useful local commands:
  s3-turbo-list guide <provider>   endpoint-compatibility facts (aws/minio/r2/b2/oss/bos)
  s3-turbo-list doctor --hints-file hints.toml --json
  s3-turbo-list manifest-summary run.json --check
"#
    .to_string()
}

fn render_quickstart(provider: &str) -> Result<String, String> {
    match provider {
        "aws" => Ok(
            r#"AWS quickstart:
1. Set credentials:
   export AWS_PROFILE=default
2. Check local setup:
   s3-turbo-list doctor --simple
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
   s3-turbo-list --profile minio --endpoint-url http://127.0.0.1:9000 --addressing-style path --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
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
max_concurrency = 100

[s3]
{endpoint_line}
{profile_line}
addressing_style = "{addressing}"
force_path_style = {force_path}
max_attempts = 10
initial_backoff_secs = 1
connect_timeout_secs = 60
operation_timeout_secs = 5

[output]
row_group_size = 100000
compression = "zstd"
compression_level = 1

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
