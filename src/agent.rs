use crate::checkpoint::{CheckpointIdentity, CheckpointJournal};
use crate::config::{ConfigLoadSummary, S3TurboConfig};
use crate::core::RunMetricsSnapshot;
use crate::hints;
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

/// `url` with any `user:password@` userinfo replaced: the command line is
/// redacted, and the resolved config beside it must not print the same
/// secret in full.
pub fn redact_url_userinfo(url: &str) -> String {
    let Some(scheme_end) = url.find("://").map(|i| i + 3) else {
        return url.to_string();
    };
    let authority_end = url[scheme_end..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| scheme_end + i);
    match url[scheme_end..authority_end].rfind('@') {
        Some(at) => format!(
            "{}{}@{}",
            &url[..scheme_end],
            REDACTED_ARG_VALUE,
            &url[scheme_end + at + 1..]
        ),
        None => url.to_string(),
    }
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
    /// Deprecated (0.37): `addressing_style == "path"`.
    pub force_path_style: bool,
    pub addressing_style: String,
    pub provider: Option<String>,
    /// Deprecated (0.37): the same value as `provider`.
    pub profile: Option<String>,
    pub profile_known: bool,
    pub profile_warnings: Vec<String>,
    /// Deprecated (0.37): `trace_compat == "-"`.
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
                endpoint_url: cfg.s3.endpoint_url.as_deref().map(redact_url_userinfo),
                force_path_style: cfg.s3.force_path_style(),
                addressing_style: cfg.s3.addressing_style.to_string(),
                provider: cfg.s3.profile.clone(),
                profile: cfg.s3.profile.clone(),
                debug_s3: cfg.s3.trace_compat.as_deref() == Some("-"),
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
    pub continuation_token: Option<String>,
    pub profile: Option<String>,
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
}

#[derive(Debug, Clone, Serialize)]
pub struct HintsPlan {
    pub source: String,
    pub path: Option<String>,
    pub exists: bool,
    pub valid: Option<bool>,
    pub format: Option<String>,
    pub boundary_count: Option<usize>,
    pub warnings: Vec<String>,
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
    /// Deprecated (0.37): always null; checkpoints record key ranges.
    pub completed_segments: Option<usize>,
    /// Deprecated (0.37): always null.
    pub total_segments: Option<usize>,
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

/// How a list run will partition its key space, mirroring the run's own
/// order of decisions (see the hints/discovery block in `main`).
pub struct HintsPlanInputs<'a> {
    pub explicit_hints_file: Option<&'a str>,
    /// A run command with a bucket (not a local tool).
    pub listing: bool,
    pub no_auto_hints: bool,
    /// `--start-after` or `--continuation-token`: one sequential chain.
    pub single_chain: bool,
    /// A non-empty `--delimiter`: hierarchical, not partitioned.
    pub delimited: bool,
}

pub fn detect_hints_plan(inputs: HintsPlanInputs<'_>) -> HintsPlan {
    let HintsPlanInputs {
        explicit_hints_file,
        listing,
        no_auto_hints,
        single_chain,
        delimited,
    } = inputs;
    let plan_without_hints = |source: &str, warning: &str| HintsPlan {
        source: source.to_string(),
        path: None,
        exists: false,
        valid: None,
        format: None,
        boundary_count: None,
        warnings: vec![warning.to_string()],
    };
    if single_chain {
        return plan_without_hints(
            "single_chain",
            "--start-after / --continuation-token list one sequential ListObjectsV2 chain; \
             hints, startup discovery and runtime splitting are skipped",
        );
    }
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
            warnings: report
                .map(|r| r.warnings)
                .unwrap_or_else(|| vec!["hints file does not exist or could not be parsed".into()]),
        };
    }

    if delimited {
        return plan_without_hints(
            "delimiter_single_segment",
            "a --delimiter run lists one hierarchical segment: CommonPrefixes are not \
             range-bounded, so hints, startup discovery and runtime splitting are skipped",
        );
    }

    if no_auto_hints {
        return plan_without_hints(
            "disabled_single_segment_fallback",
            "--no-auto-hints skips startup discovery; the run starts as one segment and \
             relies on runtime splitting to fan out",
        );
    }

    if listing {
        // The run probes the bucket's structure at startup (every run: there
        // is no cache) and partitions from what it finds.
        return plan_without_hints(
            "startup_discovery",
            "startup discovery partitions the key space from the bucket's structure \
             (a few delimiter or single-key probes); runtime splitting covers skew",
        );
    }

    HintsPlan {
        source: "not_applicable".to_string(),
        path: None,
        exists: false,
        valid: None,
        format: None,
        boundary_count: None,
        warnings: Vec::new(),
    }
}

pub fn diff_per_side_hints_plan() -> HintsPlan {
    HintsPlan {
        source: "diff_per_side_automatic".to_string(),
        path: None,
        exists: false,
        valid: None,
        format: None,
        boundary_count: None,
        warnings: vec![
            "diff partitions each side with startup discovery; --hints-file and --resume \
             are list-only"
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
        resume: false,
        path,
        exists: false,
        valid: None,
        identity_matches: None,
        identity_mismatches: Vec::new(),
        completed_segments: None,
        total_segments: None,
        remaining_ranges: None,
        identity_fields: vec![
            "bucket".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            "delimiter".to_string(),
            "max_keys".to_string(),
            "profile".to_string(),
            "addressing_style".to_string(),
            "mode".to_string(),
            // Added to the identity in 0.30.0; this list is what the manifest
            // reports as the verified set, and it was left behind.
            "filter".to_string(),
            "endpoint_url".to_string(),
        ],
        resumed_segments_skipped: None,
    }
}

/// `resume`: whether the run reads the checkpoint at `path`; `path`: where
/// the run saves one on interrupt (`None` for runs that cannot resume).
pub fn checkpoint_plan(
    resume: bool,
    path: Option<String>,
    current_identity: Option<&CheckpointIdentity>,
    resumed_segments_skipped: Option<usize>,
) -> CheckpointPlan {
    let mut plan = default_checkpoint_plan(path.is_some(), path.clone());
    plan.resume = resume;
    plan.resumed_segments_skipped = resumed_segments_skipped;
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
            plan.remaining_ranges = journal.remaining.as_ref().map(Vec::len);
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

/// Why the real run could not create an output file at `path`, if it could
/// not: the path is a directory, or its nearest existing ancestor is a file or
/// a read-only directory (e.g. `--output-dir` pointing at an existing file, or
/// a path under /proc). Checked without touching the filesystem, so a dry run
/// predicts the exit-5 the run would hit when creating the file.
pub fn output_path_problem(path: &str) -> Option<String> {
    let target = Path::new(path);
    if target.is_dir() {
        return Some(format!("'{}' is a directory", path));
    }
    // An existing target is opened in place, so its own permissions decide,
    // not its directory's: `/dev/null` is writable although macOS's `/dev`
    // is mode 555.
    if target.exists() {
        return (!writable(target)).then(|| format!("'{}' is not writable", path));
    }
    let mut ancestor = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    loop {
        match std::fs::metadata(ancestor) {
            Ok(meta) if !meta.is_dir() => {
                return Some(format!(
                    "'{}' exists and is not a directory",
                    ancestor.display()
                ));
            }
            Ok(_) if !writable(ancestor) => {
                return Some(format!(
                    "directory '{}' is not writable",
                    ancestor.display()
                ));
            }
            Ok(_) => return None,
            Err(_) => ancestor = ancestor.parent().filter(|p| !p.as_os_str().is_empty())?,
        }
    }
}

fn output_parent(path: &str) -> Option<PathBuf> {
    Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
}

fn parent_writable(path: PathBuf) -> Option<bool> {
    path.exists().then(|| writable(&path))
}

/// Whether this process may write `path` (a file, or a directory to create
/// files in). Mode bits alone are wrong both ways: root writes a mode-444
/// file, and an ACL can grant or deny what the bits say.
#[cfg(unix)]
fn writable(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `c_path` is a valid NUL-terminated string for the call.
    unsafe { libc::access(c_path.as_ptr(), libc::W_OK) == 0 }
}

#[cfg(not(unix))]
fn writable(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|m| !m.permissions().readonly())
}

/// The `.partN` paths a pooled list run's extra writers streamed to, given the
/// number of output files the run reported writing (base plus one per extra
/// writer).  Derived from the run's own writer count rather than from what is
/// on disk: an explicit output path reused after a wider run still has that
/// run's higher-numbered parts sitting next to it, and reporting those as this
/// run's output would let verification pass over objects from an earlier
/// listing.
pub fn parquet_part_paths(base: &str, output_files: usize) -> Vec<String> {
    (1..output_files.min(crate::data_map::MAX_LIST_OUTPUT_WORKERS))
        .map(|index| crate::data_map::part_path(base, index))
        .collect()
}

pub fn collect_artifacts(outputs: &OutputPathSummary, output_files: usize) -> Vec<ArtifactSummary> {
    let mut artifacts = Vec::new();
    if let Some(path) = &outputs.parquet_file {
        artifacts.push(summarize_artifact("parquet", path));
        // A pooled list run scales to several writers, each with its own
        // part-file. Recording only the base path left most of the output
        // unlisted and unverifiable, and made the manifest's own
        // artifact row count disagree with metrics.parquet_rows.
        for part in parquet_part_paths(path, output_files) {
            artifacts.push(summarize_artifact("parquet", &part));
        }
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
        line_count: match kind {
            // KS is CSV: a quoted prefix may itself contain a newline, so
            // count records, not newline bytes — it must equal ks_entries.
            "ks" => csv_record_count(path).ok(),
            "trace" | "log" | "hints" => line_count(path).ok(),
            _ => None,
        },
        parquet: (kind == "parquet")
            .then(|| parquet_summary(path).ok())
            .flatten(),
    }
}

pub fn sha256_file(path: &str) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| format!("failed to open '{}' for hashing: {}", path, e))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Records in an RFC 4180 CSV file: newlines outside double quotes.
fn csv_record_count(path: &str) -> Result<usize, String> {
    let content = std::fs::read(path).map_err(|e| e.to_string())?;
    let mut in_quotes = false;
    let mut records = 0usize;
    for byte in content {
        match byte {
            b'"' => in_quotes = !in_quotes, // "" toggles twice: net no-op
            b'\n' if !in_quotes => records += 1,
            _ => {}
        }
    }
    Ok(records)
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

pub fn doctor_report(
    cfg: &S3TurboConfig,
    config_source: ConfigSourceSummary,
    hints: Option<hints::HintsValidationReport>,
) -> DoctorReport {
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
    // Report the config the command actually loaded (explicit --config,
    // workspace or home), not just whether ./s3-turbo-list.toml exists.
    checks.push(match config_source.loaded_config.as_deref() {
        Some(path) => DoctorCheck {
            name: "config_file".to_string(),
            status: "ok".to_string(),
            message: format!(
                "loaded {} config {}",
                config_source.loaded_config_kind, path
            ),
        },
        None => DoctorCheck {
            name: "config_file".to_string(),
            status: "ok".to_string(),
            message: "no config file (searched ./s3-turbo-list.toml and \
                      ~/.s3-turbo-list.toml); built-in defaults apply"
                .to_string(),
        },
    });
    // Credentials come from the SDK chain whether or not AWS_PROFILE is set:
    // an unset AWS_PROFILE is the normal case, not a warning.
    checks.push(DoctorCheck {
        name: "aws_profile".to_string(),
        status: "ok".to_string(),
        message: std::env::var("AWS_PROFILE")
            .ok()
            .filter(|v| !v.is_empty())
            .map(|v| format!("AWS_PROFILE={}", v))
            .unwrap_or_else(|| {
                "AWS_PROFILE is not set; the AWS SDK uses its default credential chain".to_string()
            }),
    });
    checks.push(DoctorCheck {
        name: "provider".to_string(),
        status: "ok".to_string(),
        message: match cfg.s3.profile.as_deref().and_then(profiles::get_profile) {
            Some(profile) => format!("provider preset '{}' ({})", profile.name, profile.provider),
            None => "no provider preset; plain S3 settings apply".to_string(),
        },
    });
    checks.push(endpoint_url_check(cfg));
    checks.push(proxy_check(cfg));
    checks.push(DoctorCheck {
        name: "network".to_string(),
        status: "skipped".to_string(),
        message: "doctor is a local preflight and does not contact S3 endpoints; use compat-probe to validate an endpoint".to_string(),
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
        cwd,
        checks,
        config_source,
        resolved_config: cfg.into(),
        hints,
    }
}

/// The URL the SDK will send a bucket's requests to: the S3 endpoint resolver
/// the client itself uses, given the same bucket, region, endpoint override and
/// addressing style (a virtual-hosted AWS request goes to
/// `<bucket>.s3.<region>.amazonaws.com`, not a fixed global host).
pub fn resolved_request_url(
    bucket: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
    force_path_style: bool,
) -> Option<String> {
    use aws_sdk_s3::config::endpoint::{DefaultResolver, Params, ResolveEndpoint};
    let mut params = Params::builder().force_path_style(force_path_style);
    if let Some(bucket) = bucket {
        params = params.bucket(bucket);
    }
    if let Some(region) = region {
        params = params.region(region);
    }
    if let Some(endpoint) = endpoint {
        params = params.endpoint(endpoint);
    }
    let params = params.build().ok()?;
    // The default resolver answers synchronously; its future is ready at once.
    let endpoint =
        futures::executor::block_on(DefaultResolver::new().resolve_endpoint(&params)).ok()?;
    Some(endpoint.url().to_string())
}

/// The proxy the SDK's HTTP client will route `url` through, if any.
///
/// The SDK (behavior version 2025-08-07 and later, which this binary uses)
/// builds its connector's proxy rules from `HTTP_PROXY` / `HTTPS_PROXY` /
/// `ALL_PROXY` / `NO_PROXY` with hyper-util's `Matcher::from_env`; asking the
/// same matcher about the same request URL gives the same answer the run
/// gets. Only the proxy's scheme, host and port are returned — never
/// credentials in its URL.
pub fn env_proxy_for_url(url: &str) -> Option<String> {
    let uri: http::Uri = url.parse().ok()?;
    let intercept = hyper_util::client::proxy::matcher::Matcher::from_env().intercept(&uri)?;
    let proxy = intercept.uri();
    let host = proxy.host()?;
    let scheme = proxy.scheme_str().unwrap_or("http");
    Some(match proxy.port_u16() {
        Some(port) => format!("{}://{}:{}", scheme, host, port),
        None => format!("{}://{}", scheme, host),
    })
}

/// Doctor has no bucket, so it can only answer exactly when the request host
/// does not depend on one: an explicit endpoint with path-style addressing.
/// Anywhere else a `NO_PROXY` entry could match the real (bucket-qualified,
/// regional) host and not a stand-in, so the check is skipped rather than
/// guessed; the run log names the decision for each resolved endpoint.
fn proxy_check(cfg: &S3TurboConfig) -> DoctorCheck {
    let skipped = |message: &str| DoctorCheck {
        name: "proxy".to_string(),
        status: "skipped".to_string(),
        message: message.to_string(),
    };
    let endpoint = match cfg.s3.endpoint_url.as_deref() {
        Some(e) if profiles::endpoint_url_has_template_placeholder(e) => {
            return skipped(
                "endpoint_url is still a template; proxy rules are checked once it is a real URL",
            );
        }
        Some(e) if cfg.s3.force_path_style() => e,
        _ => {
            return skipped(
                "the request host depends on the bucket and region (virtual-hosted or AWS \
                 endpoint), which doctor does not take; the run log names the proxy, if \
                 any, for each resolved endpoint",
            );
        }
    };
    let url = resolved_request_url(None, None, Some(endpoint), true)
        .unwrap_or_else(|| endpoint.to_string());
    DoctorCheck {
        name: "proxy".to_string(),
        status: "ok".to_string(),
        message: match env_proxy_for_url(&url) {
            Some(proxy) => format!(
                "requests to {} go through proxy {} (from HTTP(S)_PROXY / ALL_PROXY; \
                 add the host to NO_PROXY to connect directly)",
                url, proxy
            ),
            None => format!("requests to {} connect directly (no proxy applies)", url),
        },
    }
}

fn endpoint_url_check(cfg: &S3TurboConfig) -> DoctorCheck {
    if let Some(endpoint) = cfg.s3.endpoint_url.as_deref() {
        // Both endpoint problems below stop every real list/diff run with
        // exit 3, so doctor reports them as errors, not warnings: a preflight
        // that says "ok" before a guaranteed setup failure is worse than none.
        if profiles::endpoint_url_has_template_placeholder(endpoint) {
            return DoctorCheck {
                name: "endpoint_url".to_string(),
                status: "error".to_string(),
                message: format!(
                    "endpoint_url contains template placeholders and must be edited before a real run: {}",
                    redact_url_userinfo(endpoint)
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
                    status: "error".to_string(),
                    message: format!(
                        "profile '{}' requires --endpoint-url or s3.endpoint_url in config",
                        profile.name
                    ),
                };
            }
            // doctor takes no --region, so a region-derived endpoint cannot
            // be resolved here; the run supplies it.
            if profile.endpoint_template.is_some() && profile.default_region.is_none() {
                return DoctorCheck {
                    name: "endpoint_url".to_string(),
                    status: "warn".to_string(),
                    message: format!(
                        "profile '{}' derives its endpoint from the region; the run needs --region or --endpoint-url",
                        profile.name
                    ),
                };
            }
        }
    }

    DoctorCheck {
        name: "endpoint_url".to_string(),
        status: "ok".to_string(),
        message: "no explicit endpoint URL required by the provider preset".to_string(),
    }
}

/// The same test the dry run applies (`output_path_problem`): the run
/// creates missing parent directories itself, and fails on a path that is a
/// directory or under an unwritable or non-directory ancestor.
fn parent_dir_check(name: &str, path: &str) -> DoctorCheck {
    match output_path_problem(path) {
        Some(problem) => DoctorCheck {
            name: name.to_string(),
            status: "error".to_string(),
            message: format!("output '{}' cannot be created: {}", path, problem),
        },
        None => DoctorCheck {
            name: name.to_string(),
            status: "ok".to_string(),
            message: format!("output '{}' can be created", path),
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
    #[test]
    fn resolved_request_url_matches_the_sdk_request_host() {
        // Virtual-hosted AWS: bucket-qualified, regional host.
        let url =
            super::resolved_request_url(Some("my-bucket"), Some("us-west-2"), None, false).unwrap();
        assert!(
            url.starts_with("https://my-bucket.s3.us-west-2.amazonaws.com"),
            "{}",
            url
        );
        // Path-style custom endpoint: the endpoint plus the bucket path.
        let url = super::resolved_request_url(
            Some("my-bucket"),
            Some("us-east-1"),
            Some("http://127.0.0.1:9000"),
            true,
        )
        .unwrap();
        assert_eq!(url, "http://127.0.0.1:9000/my-bucket");
    }

    use super::redact_command_args;

    fn parquet_outputs(base: &str) -> super::OutputPathSummary {
        super::OutputPathSummary {
            parquet_file: Some(base.to_string()),
            ks_file: None,
            hints_file: None,
            trace_compat: None,
            log_file: None,
        }
    }

    #[test]
    fn manifest_enumerates_every_parquet_part_file() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("out.parquet");
        let base_str = base.to_str().unwrap().to_string();
        for name in ["out.parquet", "out.part1.parquet", "out.part2.parquet"] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }

        // Three writers: the base file plus two parts.
        let parts = super::parquet_part_paths(&base_str, 3);
        assert_eq!(parts.len(), 2, "{:?}", parts);
        assert!(parts[0].ends_with("out.part1.parquet"));
        assert!(parts[1].ends_with("out.part2.parquet"));

        let artifacts = super::collect_artifacts(&parquet_outputs(&base_str), 3);
        assert_eq!(artifacts.len(), 3, "{:?}", artifacts);
        assert!(artifacts.iter().all(|a| a.kind == "parquet" && a.exists));
        assert!(artifacts.iter().all(|a| a.sha256.is_some()));
    }

    #[test]
    fn manifest_ignores_part_files_this_run_did_not_write() {
        // An explicit output path reused after a wider run: the old run's
        // higher-numbered parts are still on disk, holding objects from an
        // earlier listing. Only what this run wrote may be reported.
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("out.parquet");
        let base_str = base.to_str().unwrap().to_string();
        for name in [
            "out.parquet",
            "out.part1.parquet",
            "out.part2.parquet",
            "out.part3.parquet",
        ] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }

        let artifacts = super::collect_artifacts(&parquet_outputs(&base_str), 2);
        assert_eq!(artifacts.len(), 2, "{:?}", artifacts);
        assert!(artifacts[1].path.ends_with("out.part1.parquet"));
    }

    #[test]
    fn manifest_parquet_artifacts_are_just_the_base_file_without_parts() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("out.parquet");
        std::fs::write(&base, b"x").unwrap();
        let base_str = base.to_str().unwrap().to_string();
        // One writer, and the degenerate "nothing recorded" case (a run that
        // failed before the data map reported): base file only either way.
        assert_eq!(
            super::collect_artifacts(&parquet_outputs(&base_str), 1).len(),
            1
        );
        assert_eq!(
            super::collect_artifacts(&parquet_outputs(&base_str), 0).len(),
            1
        );
    }

    #[test]
    fn parquet_part_paths_stay_within_the_writer_cap() {
        // Indices run 0..=cap-1, so the highest part is cap-1 — never `.part32`
        // with a 32-writer cap.
        let parts = super::parquet_part_paths("out.parquet", 10_000);
        assert_eq!(
            parts.len(),
            super::super::data_map::MAX_LIST_OUTPUT_WORKERS - 1
        );
        assert!(parts.last().unwrap().ends_with(&format!(
            "out.part{}.parquet",
            super::super::data_map::MAX_LIST_OUTPUT_WORKERS - 1
        )));
    }

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

    #[cfg(unix)]
    #[test]
    fn output_path_problem_judges_an_existing_target_by_its_own_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let ro = dir.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        let existing = ro.join("out.ks");
        std::fs::write(&existing, b"").unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o555)).unwrap();
        // Opened in place: the read-only directory does not matter (macOS /dev).
        assert_eq!(super::output_path_problem(existing.to_str().unwrap()), None);
        // Root writes regardless of mode bits, and so does the run: the check
        // must agree with it (it used to block a run that would succeed).
        let root = unsafe { libc::geteuid() } == 0;
        let fresh = ro.join("new.ks");
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o444)).unwrap();
        for path in [&fresh, &existing] {
            assert_eq!(
                super::output_path_problem(path.to_str().unwrap()).is_some(),
                !root,
                "{}",
                path.display()
            );
        }
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn redact_url_userinfo_hides_credentials_only() {
        use super::redact_url_userinfo as r;
        assert_eq!(r("http://u:pw@h:9000/x"), "http://<redacted>@h:9000/x");
        assert_eq!(r("https://h.example/a@b"), "https://h.example/a@b");
        assert_eq!(r("https://h.example"), "https://h.example");
        assert_eq!(r("not a url"), "not a url");
    }
}
