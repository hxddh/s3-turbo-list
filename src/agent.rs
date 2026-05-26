use crate::checkpoint::{CheckpointIdentity, CheckpointJournal};
use crate::config::{ConfigLoadSummary, S3TurboConfig};
use crate::core::RunMetricsSnapshot;
use crate::hints::{self, HintsEstimateSummary};
use crate::profiles;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};

pub const AGENT_SCHEMA_VERSION: &str = "s3-turbo-list.agent.v1";
const REDACTED_ARG_VALUE: &str = "<redacted>";
const SENSITIVE_VALUE_FLAGS: &[&str] = &["--continuation-token", "--endpoint-url", "--endpoint"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    Success = 0,
    InternalError = 1,
    CliConfig = 2,
    ProviderSetup = 3,
    NetworkRetryExhausted = 4,
    OutputWrite = 5,
    DataValidation = 6,
    Interrupted = 7,
}

pub fn redacted_command_args() -> Vec<String> {
    redact_command_args(std::env::args())
}

pub fn redact_command_args<I, S>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut redacted = Vec::new();
    let mut redact_next = false;

    for arg in args {
        let arg = arg.into();
        if redact_next {
            redacted.push(REDACTED_ARG_VALUE.to_string());
            redact_next = false;
            continue;
        }

        if let Some((flag, _value)) = arg.split_once('=') {
            if is_sensitive_value_flag(flag) {
                redacted.push(format!("{}={}", flag, REDACTED_ARG_VALUE));
                continue;
            }
        }

        if is_sensitive_value_flag(&arg) {
            redacted.push(arg);
            redact_next = true;
            continue;
        }

        redacted.push(arg);
    }

    redacted
}

fn is_sensitive_value_flag(arg: &str) -> bool {
    SENSITIVE_VALUE_FLAGS.contains(&arg)
}

impl ExitCode {
    pub fn code(self) -> i32 {
        self as i32
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedConfigSummary {
    pub runtime: RuntimeSummary,
    pub s3: S3Summary,
    pub output: OutputSummary,
    pub auto_hints: AutoHintsSummary,
    pub channel: ChannelSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeSummary {
    pub worker_threads: usize,
    pub max_concurrency: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3Summary {
    pub max_attempts: u32,
    pub initial_backoff_secs: u64,
    pub connect_timeout_secs: u64,
    pub operation_timeout_secs: u64,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub addressing_style: String,
    pub profile: Option<String>,
    pub profile_known: bool,
    pub profile_warnings: Vec<String>,
    pub debug_s3: bool,
    pub trace_compat: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputSummary {
    pub row_group_size: usize,
    pub compression: String,
    pub compression_level: u32,
    pub log_file: Option<String>,
    pub ks_file: Option<String>,
    pub parquet_file: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutoHintsSummary {
    pub sample_threshold: usize,
    pub max_prefix_depth: usize,
    pub min_segment_size: usize,
    pub max_prefix_entries: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelSummary {
    pub capacity: usize,
}

impl From<&S3TurboConfig> for ResolvedConfigSummary {
    fn from(cfg: &S3TurboConfig) -> Self {
        Self {
            runtime: RuntimeSummary {
                worker_threads: cfg.runtime.worker_threads,
                max_concurrency: cfg.runtime.max_concurrency,
            },
            s3: S3Summary {
                profile_known: cfg
                    .s3
                    .profile
                    .as_deref()
                    .and_then(profiles::get_profile)
                    .is_some(),
                profile_warnings: cfg
                    .s3
                    .profile
                    .as_deref()
                    .and_then(profiles::get_profile)
                    .map(|profile| {
                        profile
                            .limitations
                            .iter()
                            .map(|item| (*item).to_string())
                            .collect()
                    })
                    .unwrap_or_default(),
                max_attempts: cfg.s3.max_attempts,
                initial_backoff_secs: cfg.s3.initial_backoff_secs,
                connect_timeout_secs: cfg.s3.connect_timeout_secs,
                operation_timeout_secs: cfg.s3.operation_timeout_secs,
                endpoint_url: cfg.s3.endpoint_url.clone(),
                force_path_style: cfg.s3.force_path_style,
                addressing_style: cfg.s3.addressing_style.to_string(),
                profile: cfg.s3.profile.clone(),
                debug_s3: cfg.s3.debug_s3,
                trace_compat: cfg.s3.trace_compat.clone(),
                start_after: cfg.s3.start_after.clone(),
            },
            output: OutputSummary {
                row_group_size: cfg.output.row_group_size,
                compression: cfg.output.compression.clone(),
                compression_level: cfg.output.compression_level,
                log_file: cfg.output.log_file.clone(),
                ks_file: cfg.output.ks_file.clone(),
                parquet_file: cfg.output.parquet_file.clone(),
            },
            auto_hints: AutoHintsSummary {
                sample_threshold: cfg.auto_hints.sample_threshold,
                max_prefix_depth: cfg.auto_hints.max_prefix_depth,
                min_segment_size: cfg.auto_hints.min_segment_size,
                max_prefix_entries: cfg.auto_hints.max_prefix_entries,
            },
            channel: ChannelSummary {
                capacity: cfg.channel.capacity,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandInputSummary {
    pub mode: String,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub target_bucket: Option<String>,
    pub target_region: Option<String>,
    pub output_format: Option<String>,
    pub prefix: String,
    pub delimiter: String,
    pub max_keys: Option<i32>,
    pub start_after: Option<String>,
    pub continuation_token: Option<String>,
    pub profile: Option<String>,
    pub addressing_style: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputPathSummary {
    pub parquet_file: Option<String>,
    pub ks_file: Option<String>,
    pub hints_file: Option<String>,
    pub trace_compat: Option<String>,
    pub log_file: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HintsPlan {
    pub source: String,
    pub path: Option<String>,
    pub exists: bool,
    pub valid: Option<bool>,
    pub format: Option<String>,
    pub boundary_count: Option<usize>,
    pub estimate_summary: Option<HintsEstimateSummary>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointPlan {
    pub enabled: bool,
    pub path: Option<String>,
    pub exists: bool,
    pub valid: Option<bool>,
    pub identity_matches: Option<bool>,
    pub identity_mismatches: Vec<String>,
    pub completed_segments: Option<usize>,
    pub total_segments: Option<usize>,
    pub identity_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileConflict {
    pub path: String,
    pub exists: bool,
    pub parent_path: Option<String>,
    pub parent_exists: bool,
    pub parent_writable: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanReport {
    pub schema_version: &'static str,
    pub tool_version: &'static str,
    pub status: String,
    pub command: Vec<String>,
    pub network: String,
    pub inputs: CommandInputSummary,
    pub outputs: OutputPathSummary,
    pub config_source: ConfigSourceSummary,
    pub resolved_config: ResolvedConfigSummary,
    pub hints: HintsPlan,
    pub checkpoint: CheckpointPlan,
    pub file_conflicts: Vec<FileConflict>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigSourceSummary {
    pub explicit_config: Option<String>,
    pub loaded_config: Option<String>,
    pub loaded_config_kind: String,
    pub searched: Vec<String>,
    pub cli_overrides: Vec<String>,
    pub warnings: Vec<String>,
}

impl ConfigSourceSummary {
    pub fn new(load: &ConfigLoadSummary, cli_overrides: Vec<String>) -> Self {
        Self {
            explicit_config: load.explicit_config.clone(),
            loaded_config: load.loaded_config.clone(),
            loaded_config_kind: load.loaded_config_kind.clone(),
            searched: load.searched.clone(),
            cli_overrides,
            warnings: load.warnings.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSummary {
    pub fatal_errors: usize,
    pub output_errors: usize,
    pub stream_timeouts: usize,
    pub s3_client_timeouts: usize,
    pub s3_client_generic_errors: usize,
    pub received_batches: usize,
    pub received_objects: usize,
    pub streamed_rows: usize,
    pub unique_prefixes: usize,
    pub parquet_rows: usize,
    pub ks_entries: usize,
    pub bytes_total: u64,
    pub top_prefixes: Vec<crate::core::PrefixMetric>,
    pub summary_only: bool,
}

impl From<RunMetricsSnapshot> for MetricsSummary {
    fn from(metrics: RunMetricsSnapshot) -> Self {
        Self {
            fatal_errors: metrics.fatal_errors,
            output_errors: metrics.output_errors,
            stream_timeouts: metrics.stream_timeouts,
            s3_client_timeouts: metrics.s3_client_timeouts,
            s3_client_generic_errors: metrics.s3_client_generic_errors,
            received_batches: metrics.data_received_batches,
            received_objects: metrics.data_received_objects,
            streamed_rows: metrics.data_streamed_rows,
            unique_prefixes: metrics.data_unique_prefixes,
            parquet_rows: metrics.data_parquet_rows,
            ks_entries: metrics.data_ks_entries,
            bytes_total: metrics.data_bytes_total,
            top_prefixes: metrics.data_top_prefixes,
            summary_only: metrics.data_summary_only,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RunManifest {
    pub schema_version: &'static str,
    pub tool_version: &'static str,
    pub status: String,
    pub exit_code: i32,
    pub started_at: String,
    pub finished_at: String,
    pub elapsed_secs: f64,
    pub command: Vec<String>,
    pub inputs: CommandInputSummary,
    pub outputs: OutputPathSummary,
    pub config_source: ConfigSourceSummary,
    pub artifacts: Vec<ArtifactSummary>,
    pub metrics: MetricsSummary,
    pub checkpoint: CheckpointPlan,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactSummary {
    pub kind: String,
    pub path: String,
    pub exists: bool,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub line_count: Option<usize>,
    pub parquet: Option<ParquetArtifactSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParquetArtifactSummary {
    pub row_count: i64,
    pub row_group_count: usize,
    pub schema_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigInspectReport {
    pub schema_version: &'static str,
    pub tool_version: &'static str,
    pub status: String,
    pub config_source: ConfigSourceSummary,
    pub resolved_config: ResolvedConfigSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    pub schema_version: &'static str,
    pub tool_version: &'static str,
    pub status: String,
    pub local_only: bool,
    pub cwd: String,
    pub checks: Vec<DoctorCheck>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorCheck {
    pub name: String,
    pub status: String,
    pub message: String,
}

pub fn conventional_hints_path(bucket: &str, region: Option<&str>) -> String {
    let bucket = sanitize_path_component(bucket);
    match region {
        Some(r) => format!("{}_{}_hints.toml", sanitize_path_component(r), bucket),
        None => format!("{}_hints.toml", bucket),
    }
}

pub fn sanitize_path_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "_".to_string()
    } else {
        sanitized
    }
}

pub fn detect_hints_plan(
    explicit_hints_file: Option<&str>,
    bucket: Option<&str>,
    region: Option<&str>,
    no_auto_hints: bool,
) -> HintsPlan {
    if let Some(path) = explicit_hints_file {
        let report = inspect_hints_for_plan(path);
        return HintsPlan {
            source: "explicit".to_string(),
            path: Some(path.to_string()),
            exists: Path::new(path).exists(),
            valid: report.as_ref().map(|r| r.valid),
            format: report
                .as_ref()
                .map(|r| format!("{:?}", r.format).to_lowercase()),
            boundary_count: report.as_ref().map(|r| r.boundary_count),
            estimate_summary: report.as_ref().and_then(|r| r.estimate_summary.clone()),
            warnings: report
                .map(|r| r.warnings)
                .unwrap_or_else(|| vec!["hints file does not exist or could not be parsed".into()]),
        };
    }

    if no_auto_hints {
        return HintsPlan {
            source: "disabled_single_segment_fallback".to_string(),
            path: None,
            exists: false,
            valid: None,
            format: None,
            boundary_count: None,
            estimate_summary: None,
            warnings: vec!["--no-auto-hints skips conventional hints cache loading".to_string()],
        };
    }

    if let Some(bucket) = bucket {
        let path = conventional_hints_path(bucket, region);
        let exists = Path::new(&path).exists();
        let report = exists.then(|| inspect_hints_for_plan(&path)).flatten();
        return HintsPlan {
            source: if exists {
                "auto_cache"
            } else {
                "single_segment_fallback"
            }
            .to_string(),
            path: Some(path),
            exists,
            valid: report.as_ref().map(|r| r.valid),
            format: report
                .as_ref()
                .map(|r| format!("{:?}", r.format).to_lowercase()),
            boundary_count: report.as_ref().map(|r| r.boundary_count),
            estimate_summary: report.as_ref().and_then(|r| r.estimate_summary.clone()),
            warnings: report.map(|r| r.warnings).unwrap_or_default(),
        };
    }

    HintsPlan {
        source: "not_applicable".to_string(),
        path: None,
        exists: false,
        valid: None,
        format: None,
        boundary_count: None,
        estimate_summary: None,
        warnings: Vec::new(),
    }
}

pub fn diff_single_segment_hints_plan(bucket: Option<&str>, region: Option<&str>) -> HintsPlan {
    let path = bucket.map(|bucket| conventional_hints_path(bucket, region));
    let exists = path
        .as_deref()
        .map(|path| Path::new(path).exists())
        .unwrap_or(false);
    let report = path
        .as_deref()
        .filter(|_| exists)
        .and_then(inspect_hints_for_plan);

    HintsPlan {
        source: "disabled_for_diff_single_segment".to_string(),
        path,
        exists,
        valid: report.as_ref().map(|r| r.valid),
        format: report
            .as_ref()
            .map(|r| format!("{:?}", r.format).to_lowercase()),
        boundary_count: report.as_ref().map(|r| r.boundary_count),
        estimate_summary: report.as_ref().and_then(|r| r.estimate_summary.clone()),
        warnings: vec![
            "diff uses single-segment authoritative mode; conventional hints cache is ignored until paired-segment diff coordination is implemented"
                .to_string(),
        ],
    }
}

fn inspect_hints_for_plan(path: &str) -> Option<hints::HintsValidationReport> {
    hints::inspect_hints_file(path, 3).ok()
}

pub fn default_checkpoint_plan(enabled: bool, path: Option<String>) -> CheckpointPlan {
    CheckpointPlan {
        enabled,
        path,
        exists: false,
        valid: None,
        identity_matches: None,
        identity_mismatches: Vec::new(),
        completed_segments: None,
        total_segments: None,
        identity_fields: vec![
            "bucket".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            "delimiter".to_string(),
            "max_keys".to_string(),
            "profile".to_string(),
            "addressing_style".to_string(),
            "mode".to_string(),
        ],
    }
}

pub fn checkpoint_plan(
    enabled: bool,
    path: Option<String>,
    current_identity: Option<&CheckpointIdentity>,
) -> CheckpointPlan {
    let mut plan = default_checkpoint_plan(enabled, path.clone());
    let Some(path) = path else {
        return plan;
    };
    plan.exists = Path::new(&path).exists();
    if !plan.exists {
        return plan;
    }

    match CheckpointJournal::load(&path) {
        Some(journal) => {
            plan.valid = Some(true);
            plan.completed_segments = Some(journal.completed_indices.len());
            plan.total_segments = Some(journal.total_segments);
            if let (Some(stored), Some(current)) = (journal.identity.as_ref(), current_identity) {
                let mismatches = stored.diff(current);
                plan.identity_matches = Some(mismatches.is_empty());
                plan.identity_mismatches = mismatches;
            } else if current_identity.is_some() {
                plan.identity_matches = Some(false);
                plan.identity_mismatches = vec!["identity".to_string()];
            }
        }
        None => {
            plan.valid = Some(false);
            if current_identity.is_some() {
                plan.identity_matches = Some(false);
            }
        }
    }
    plan
}

pub fn output_conflicts(outputs: &OutputPathSummary) -> Vec<FileConflict> {
    [
        outputs.parquet_file.as_ref(),
        outputs.ks_file.as_ref(),
        outputs.hints_file.as_ref(),
        outputs.trace_compat.as_ref(),
        outputs.log_file.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(|path| FileConflict {
        path: path.clone(),
        exists: Path::new(path).exists(),
        parent_path: output_parent(path).map(|p| p.display().to_string()),
        parent_exists: output_parent(path).map(|p| p.exists()).unwrap_or(true),
        parent_writable: output_parent(path).and_then(parent_writable),
    })
    .collect()
}

fn output_parent(path: &str) -> Option<PathBuf> {
    Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
}

fn parent_writable(path: PathBuf) -> Option<bool> {
    let metadata = std::fs::metadata(path).ok()?;
    Some(!metadata.permissions().readonly())
}

pub fn collect_artifacts(outputs: &OutputPathSummary) -> Vec<ArtifactSummary> {
    let mut artifacts = Vec::new();
    if let Some(path) = &outputs.parquet_file {
        artifacts.push(summarize_artifact("parquet", path));
    }
    if let Some(path) = &outputs.ks_file {
        artifacts.push(summarize_artifact("ks", path));
    }
    if let Some(path) = &outputs.hints_file {
        artifacts.push(summarize_artifact("hints", path));
    }
    if let Some(path) = &outputs.trace_compat {
        artifacts.push(summarize_artifact("trace", path));
    }
    if let Some(path) = &outputs.log_file {
        artifacts.push(summarize_artifact("log", path));
    }
    artifacts
}

fn summarize_artifact(kind: &str, path: &str) -> ArtifactSummary {
    let exists = Path::new(path).exists();
    if !exists {
        return ArtifactSummary {
            kind: kind.to_string(),
            path: path.to_string(),
            exists,
            size_bytes: None,
            sha256: None,
            line_count: None,
            parquet: None,
        };
    }

    ArtifactSummary {
        kind: kind.to_string(),
        path: path.to_string(),
        exists,
        size_bytes: std::fs::metadata(path).ok().map(|m| m.len()),
        sha256: sha256_file(path).ok(),
        line_count: matches!(kind, "ks" | "trace" | "log" | "hints")
            .then(|| line_count(path).ok())
            .flatten(),
        parquet: (kind == "parquet")
            .then(|| parquet_summary(path).ok())
            .flatten(),
    }
}

fn sha256_file(path: &str) -> Result<String, String> {
    let mut file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn line_count(path: &str) -> Result<usize, String> {
    let content = std::fs::read(path).map_err(|e| e.to_string())?;
    Ok(content.iter().filter(|b| **b == b'\n').count())
}

fn parquet_summary(path: &str) -> Result<ParquetArtifactSummary, String> {
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
    Ok(ParquetArtifactSummary {
        row_count: metadata.file_metadata().num_rows(),
        row_group_count: metadata.num_row_groups(),
        schema_fields,
    })
}

pub fn config_inspect_report(
    cfg: &S3TurboConfig,
    config_source: ConfigSourceSummary,
) -> ConfigInspectReport {
    ConfigInspectReport {
        schema_version: AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: "ok".to_string(),
        config_source,
        resolved_config: cfg.into(),
    }
}

pub fn doctor_report(local_only: bool, cfg: &S3TurboConfig) -> DoctorReport {
    let cwd = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .display()
        .to_string();
    let mut checks = Vec::new();
    checks.push(DoctorCheck {
        name: "binary_version".to_string(),
        status: "ok".to_string(),
        message: env!("CARGO_PKG_VERSION").to_string(),
    });
    checks.push(DoctorCheck {
        name: "working_directory".to_string(),
        status: "ok".to_string(),
        message: cwd.clone(),
    });
    checks.push(DoctorCheck {
        name: "config_parse".to_string(),
        status: "ok".to_string(),
        message: "resolved configuration is valid TOML/CLI state".to_string(),
    });
    checks.push(DoctorCheck {
        name: "config_file".to_string(),
        status: if Path::new("./s3-turbo-list.toml").exists() {
            "ok"
        } else {
            "warn"
        }
        .to_string(),
        message: if Path::new("./s3-turbo-list.toml").exists() {
            "local s3-turbo-list.toml exists".to_string()
        } else {
            "local s3-turbo-list.toml not found; run init-config if you want a starter config"
                .to_string()
        },
    });
    checks.push(DoctorCheck {
        name: "aws_profile".to_string(),
        status: if std::env::var("AWS_PROFILE")
            .ok()
            .filter(|v| !v.is_empty())
            .is_some()
        {
            "ok"
        } else {
            "warn"
        }
        .to_string(),
        message: std::env::var("AWS_PROFILE")
            .ok()
            .filter(|v| !v.is_empty())
            .map(|v| format!("AWS_PROFILE={}", v))
            .unwrap_or_else(|| {
                "AWS_PROFILE is not set; the AWS SDK will use its default credential chain"
                    .to_string()
            }),
    });
    if let Some(profile) = std::env::var("AWS_PROFILE")
        .ok()
        .filter(|value| profiles::is_endpoint_preset_name(value))
    {
        checks.push(DoctorCheck {
            name: "aws_profile_endpoint_preset_name".to_string(),
            status: "warn".to_string(),
            message: format!(
                "AWS_PROFILE={} also matches an endpoint compatibility preset name; verify this is your credentials profile, not a --profile value pasted into AWS_PROFILE",
                profile
            ),
        });
    }
    checks.push(DoctorCheck {
        name: "endpoint_profile".to_string(),
        status: match cfg.s3.profile.as_deref() {
            Some(name) if profiles::get_profile(name).is_some() => "ok",
            Some(_) => "warn",
            None => "ok",
        }
        .to_string(),
        message: match cfg.s3.profile.as_deref() {
            Some(name) if profiles::get_profile(name).is_some() => {
                format!("endpoint compatibility profile '{}' is known", name)
            }
            Some(name) => format!(
                "endpoint compatibility profile '{}' is unknown; this is not an AWS credentials profile",
                name
            ),
            None => "no endpoint compatibility profile selected".to_string(),
        },
    });
    checks.push(endpoint_url_check(cfg));
    checks.push(DoctorCheck {
        name: "network".to_string(),
        status: if local_only { "skipped" } else { "warn" }.to_string(),
        message: if local_only {
            "local-only doctor does not contact S3 endpoints".to_string()
        } else {
            "network probing is intentionally not implemented in doctor; use compat-probe explicitly"
                .to_string()
        },
    });

    for (name, path) in [
        ("output_parquet_parent", cfg.output.parquet_file.as_ref()),
        ("output_ks_parent", cfg.output.ks_file.as_ref()),
        ("log_parent", cfg.output.log_file.as_ref()),
        ("trace_parent", cfg.s3.trace_compat.as_ref()),
    ] {
        if let Some(path) = path {
            checks.push(parent_dir_check(name, path));
        }
    }

    let status = if checks.iter().any(|check| check.status == "error") {
        "error"
    } else {
        "ok"
    };

    DoctorReport {
        schema_version: AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: status.to_string(),
        local_only,
        cwd,
        checks,
    }
}

fn endpoint_url_check(cfg: &S3TurboConfig) -> DoctorCheck {
    if let Some(endpoint) = cfg.s3.endpoint_url.as_deref() {
        if profiles::endpoint_url_has_template_placeholder(endpoint) {
            return DoctorCheck {
                name: "endpoint_url".to_string(),
                status: "warn".to_string(),
                message: format!(
                    "endpoint_url contains template placeholders and must be edited before a real run: {}",
                    endpoint
                ),
            };
        }

        return DoctorCheck {
            name: "endpoint_url".to_string(),
            status: "ok".to_string(),
            message: format!("endpoint_url is configured: {}", endpoint),
        };
    }

    if let Some(profile_name) = cfg.s3.profile.as_deref() {
        if let Some(profile) = profiles::get_profile(profile_name) {
            if profile.requires_explicit_endpoint {
                return DoctorCheck {
                    name: "endpoint_url".to_string(),
                    status: "warn".to_string(),
                    message: format!(
                        "profile '{}' requires --endpoint-url or s3.endpoint_url in config",
                        profile.name
                    ),
                };
            }
        }
    }

    DoctorCheck {
        name: "endpoint_url".to_string(),
        status: "ok".to_string(),
        message: "no explicit endpoint URL required by the selected profile".to_string(),
    }
}

fn parent_dir_check(name: &str, path: &str) -> DoctorCheck {
    let parent = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty());
    match parent {
        Some(parent) if !parent.exists() => DoctorCheck {
            name: name.to_string(),
            status: "error".to_string(),
            message: format!("parent directory does not exist: {}", parent.display()),
        },
        Some(parent) => DoctorCheck {
            name: name.to_string(),
            status: "ok".to_string(),
            message: format!("parent directory exists: {}", parent.display()),
        },
        None => DoctorCheck {
            name: name.to_string(),
            status: "ok".to_string(),
            message: "path is relative to current directory".to_string(),
        },
    }
}

pub fn write_json_file<T: Serialize>(path: &str, value: &T) -> Result<(), String> {
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "failed to create parent directory {}: {}",
                parent.display(),
                e
            )
        })?;
    }
    let json = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    std::fs::write(path, json).map_err(|e| format!("failed to write {}: {}", path, e))
}

pub fn to_pretty_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::redact_command_args;

    #[test]
    fn redacts_sensitive_command_values() {
        let args = redact_command_args([
            "s3-turbo-list",
            "--endpoint-url",
            "https://account.example.com",
            "--continuation-token=token-123",
            "list",
            "--bucket",
            "bucket",
        ]);

        assert_eq!(
            args,
            vec![
                "s3-turbo-list",
                "--endpoint-url",
                "<redacted>",
                "--continuation-token=<redacted>",
                "list",
                "--bucket",
                "bucket",
            ]
        );
    }

    #[test]
    fn redacts_endpoint_alias_and_preserves_diagnostic_values() {
        let args = redact_command_args([
            "s3-turbo-list",
            "--endpoint=https://account.example.com",
            "--profile",
            "r2",
            "list",
            "--bucket",
            "public-diagnostic-bucket",
            "--region",
            "auto",
        ]);

        assert_eq!(
            args,
            vec![
                "s3-turbo-list",
                "--endpoint=<redacted>",
                "--profile",
                "r2",
                "list",
                "--bucket",
                "public-diagnostic-bucket",
                "--region",
                "auto",
            ]
        );
    }

    #[test]
    fn redacts_compat_probe_endpoint_value() {
        let args = redact_command_args([
            "s3-turbo-list",
            "compat-probe",
            "--endpoint",
            "https://account.example.com",
            "--bucket",
            "diagnostic-bucket",
        ]);

        assert_eq!(
            args,
            vec![
                "s3-turbo-list",
                "compat-probe",
                "--endpoint",
                "<redacted>",
                "--bucket",
                "diagnostic-bucket",
            ]
        );
    }

    #[test]
    fn redacts_value_after_sensitive_flag_at_end_safely() {
        let args = redact_command_args(["s3-turbo-list", "list", "--continuation-token"]);

        assert_eq!(args, vec!["s3-turbo-list", "list", "--continuation-token"]);
    }
}
