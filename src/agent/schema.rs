//! The serialized plan, manifest and doctor types.

use super::*;

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedConfigSummary {
    pub runtime: RuntimeSummary,
    pub s3: S3Summary,
    pub output: OutputSummary,
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
    pub addressing_style: String,
    pub provider: Option<String>,
    /// `provider` names a known preset.
    pub provider_known: bool,
    /// The preset's known limitations.
    pub provider_warnings: Vec<String>,
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
pub struct ChannelSummary {
    pub capacity: usize,
}

impl From<&S3TurboConfig> for ResolvedConfigSummary {
    fn from(cfg: &S3TurboConfig) -> Self {
        let preset = cfg.s3.provider.as_deref().and_then(profiles::get_profile);
        let provider_warnings: Vec<String> = preset
            .map(|profile| {
                profile
                    .limitations
                    .iter()
                    .map(|item| (*item).to_string())
                    .collect()
            })
            .unwrap_or_default();
        Self {
            runtime: RuntimeSummary {
                worker_threads: cfg.runtime.worker_threads,
                max_concurrency: cfg.runtime.max_concurrency,
            },
            s3: S3Summary {
                provider_known: preset.is_some(),
                provider_warnings,
                max_attempts: cfg.s3.max_attempts,
                initial_backoff_secs: cfg.s3.initial_backoff_secs,
                connect_timeout_secs: cfg.s3.connect_timeout_secs,
                operation_timeout_secs: cfg.s3.operation_timeout_secs,
                endpoint_url: cfg.s3.endpoint_url.as_deref().map(redact_url_userinfo),
                addressing_style: cfg.s3.addressing_style.to_string(),
                provider: cfg.s3.provider.clone(),
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
    /// The provider preset, canonical lowercase name.
    pub provider: Option<String>,
    pub addressing_style: String,
    /// The `--filter` expression this run applied, or `None` for no filter.
    /// Two runs over one bucket under different filters produce different
    /// artifacts; without this the manifests that describe them are identical.
    pub filter: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputPathSummary {
    pub parquet_file: Option<String>,
    pub ks_file: Option<String>,
    pub hints_file: Option<String>,
    pub trace_compat: Option<String>,
    pub log_file: Option<String>,
    /// compat-probe's `-o` report file.
    pub report_file: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HintsPlan {
    pub source: String,
    pub path: Option<String>,
    pub exists: bool,
    pub valid: Option<bool>,
    pub format: Option<String>,
    pub boundary_count: Option<usize>,
    /// Problems with an explicit hints file; empty otherwise.
    pub warnings: Vec<String>,
    /// How this source partitions the run (informational, not a problem).
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointPlan {
    /// The run saves a checkpoint at `path` when it is interrupted.
    pub enabled: bool,
    /// The run resumes from the checkpoint at `path` (`--resume`).
    pub resume: bool,
    pub path: Option<String>,
    pub exists: bool,
    pub valid: Option<bool>,
    pub identity_matches: Option<bool>,
    pub identity_mismatches: Vec<String>,
    /// Key ranges a resume would list.
    pub remaining_ranges: Option<usize>,
    pub identity_fields: Vec<String>,
    /// Segments this run skipped because a checkpoint recorded them complete.
    /// `None` when the run did not resume; `Some(n)` with `n > 0` means the
    /// artifacts describe only the rest of the key space, and the earlier
    /// run's output is the other half. Read from what the run actually
    /// loaded, not from the file on disk — a completed run removes its
    /// checkpoint, so the disk state at manifest time says nothing about
    /// whether this run resumed.
    pub resumed_segments_skipped: Option<usize>,
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
    /// Working directory the plan's relative paths resolve against.
    pub cwd: String,
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
    /// Always empty since 0.39: it listed the deprecated config keys, which
    /// are now errors. Kept so readers of the field do not break.
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
            warnings: Vec::new(),
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
    /// Rate-limited responses (`SlowDown`, `TooManyRequests`,
    /// `ThrottlingException`) and the full error-status histogram.  These
    /// separate "slow because the bucket is large" from "slow because the
    /// endpoint was pushing back" without reading the heartbeat log.
    pub throttled_responses: usize,
    pub http_error_statuses: Vec<crate::core::HttpStatusMetric>,
    pub received_batches: usize,
    pub received_objects: usize,
    pub streamed_rows: usize,
    pub unique_prefixes: usize,
    pub parquet_rows: usize,
    pub ks_entries: usize,
    pub bytes_total: u64,
    pub top_prefixes: Vec<crate::core::PrefixMetric>,
    /// Deprecated (0.39; removed in 0.40): `inputs.output_format == "summary"`.
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
            throttled_responses: metrics.throttled_responses,
            http_error_statuses: metrics.http_error_statuses,
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
    /// Working directory of the run. Artifact paths are recorded as given,
    /// so relative ones are relative to this — `manifest-summary --check`
    /// resolves them against it from wherever it is invoked.
    pub cwd: String,
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
pub struct DoctorReport {
    pub schema_version: &'static str,
    pub tool_version: &'static str,
    pub status: String,
    pub cwd: String,
    pub checks: Vec<DoctorCheck>,
    /// Resolved configuration and its provenance — doctor is the single
    /// local-inspection command (it absorbed the former config-inspect).
    pub config_source: ConfigSourceSummary,
    pub resolved_config: ResolvedConfigSummary,
    /// Hints-file validation report when --hints-file is supplied — doctor
    /// absorbed the former hints-validate command.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hints: Option<hints::HintsValidationReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorCheck {
    pub name: String,
    pub status: String,
    pub message: String,
}
