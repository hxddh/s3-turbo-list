use crate::config::S3TurboConfig;
use crate::core::RunMetricsSnapshot;
use serde::Serialize;
use std::path::{Path, PathBuf};

pub const AGENT_SCHEMA_VERSION: &str = "s3-turbo-list.agent.v1";

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
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointPlan {
    pub enabled: bool,
    pub path: Option<String>,
    pub identity_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileConflict {
    pub path: String,
    pub exists: bool,
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
    pub resolved_config: ResolvedConfigSummary,
    pub hints: HintsPlan,
    pub checkpoint: CheckpointPlan,
    pub file_conflicts: Vec<FileConflict>,
    pub warnings: Vec<String>,
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
    pub metrics: MetricsSummary,
    pub checkpoint: CheckpointPlan,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigInspectReport {
    pub schema_version: &'static str,
    pub tool_version: &'static str,
    pub status: String,
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
    match region {
        Some(r) => format!("{}_{}_hints.toml", r, bucket),
        None => format!("{}_hints.toml", bucket),
    }
}

pub fn detect_hints_plan(
    explicit_hints_file: Option<&str>,
    bucket: Option<&str>,
    region: Option<&str>,
) -> HintsPlan {
    if let Some(path) = explicit_hints_file {
        return HintsPlan {
            source: "explicit".to_string(),
            path: Some(path.to_string()),
            exists: Path::new(path).exists(),
        };
    }

    if let Some(bucket) = bucket {
        let path = conventional_hints_path(bucket, region);
        let exists = Path::new(&path).exists();
        return HintsPlan {
            source: if exists {
                "auto_cache"
            } else {
                "single_segment_fallback"
            }
            .to_string(),
            path: Some(path),
            exists,
        };
    }

    HintsPlan {
        source: "not_applicable".to_string(),
        path: None,
        exists: false,
    }
}

pub fn default_checkpoint_plan(enabled: bool, path: Option<String>) -> CheckpointPlan {
    CheckpointPlan {
        enabled,
        path,
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
    })
    .collect()
}

pub fn config_inspect_report(cfg: &S3TurboConfig) -> ConfigInspectReport {
    ConfigInspectReport {
        schema_version: AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: "ok".to_string(),
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
