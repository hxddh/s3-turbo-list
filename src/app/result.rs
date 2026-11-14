//! The result contract: one JSON document per invocation (`s3tl.result/1`),
//! exit codes, and stable error codes.
//!
//! Every key is always present (`null` when it does not apply), so a caller
//! can index the document without checking for missing keys. Keys are only
//! ever added within a schema version.

use s3tl::core::{DiffCounts, PrefixMetric};
use s3tl::error as e;
use serde::Serialize;

pub const RESULT_SCHEMA: &str = "s3tl.result/1";

/// Process exit codes; each is a class an automated caller can branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    Success = 0,
    /// A bug: please report it.
    Internal = 1,
    /// The command line is wrong; rerunning it unchanged fails again.
    Usage = 2,
    /// Credentials, bucket, region or endpoint are wrong; fix the setup.
    Setup = 3,
    /// Network, throttling or a server fault; rerunning may succeed.
    Retryable = 4,
    /// Writing the output failed (disk full, closed pipe, permissions).
    Output = 5,
    /// Interrupted (Ctrl-C / SIGTERM); `--resume` continues a list run.
    Interrupted = 130,
}

impl ExitCode {
    pub fn code(self) -> i32 {
        self as i32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Success,
    Interrupted,
    Failed,
    /// A `--dry-run` plan.
    Planned,
}

/// Why a command did not succeed.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorInfo {
    /// Stable, dotted identifier, e.g. `auth.expired_token`.
    pub code: &'static str,
    pub message: String,
    /// Whether rerunning the same command unchanged may succeed.
    pub retryable: bool,
    /// What to do about it, when there is something specific to say.
    pub hint: Option<String>,
}

/// A failure together with its exit class.
#[derive(Debug, Clone)]
pub struct Failure {
    pub exit: ExitCode,
    pub error: ErrorInfo,
}

impl Failure {
    pub fn new(exit: ExitCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            exit,
            error: ErrorInfo {
                code,
                message: message.into(),
                retryable: matches!(exit, ExitCode::Retryable | ExitCode::Interrupted),
                hint: None,
            },
        }
    }

    pub fn usage(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ExitCode::Usage, code, message)
    }

    pub fn setup(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ExitCode::Setup, code, message)
    }

    pub fn output(message: impl Into<String>) -> Self {
        Self::new(ExitCode::Output, "output.write_failed", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ExitCode::Internal, "internal.error", message)
    }

    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.error.hint = Some(hint.into());
        self
    }

    /// A listing error, classified by the engine's errno.
    pub fn from_errno(errno: u8, message: impl Into<String>) -> Self {
        let (exit, code, hint): (ExitCode, &'static str, Option<&str>) = match errno {
            e::ERROR_S3_MISSING_CREDENTIALS => (
                ExitCode::Setup,
                "auth.missing_credentials",
                Some(
                    "configure AWS credentials: AWS_PROFILE, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or an instance role",
                ),
            ),
            e::ERROR_EXPIRED_TOKEN => (
                ExitCode::Setup,
                "auth.expired_token",
                Some("refresh the session credentials (e.g. `aws sso login`)"),
            ),
            e::ERROR_INVALID_ACCESS_KEY => (
                ExitCode::Setup,
                "auth.invalid_access_key",
                Some(
                    "the access key does not exist on this endpoint; check the credentials and --endpoint-url",
                ),
            ),
            e::ERROR_ACCESS_DENIED => (
                ExitCode::Setup,
                "auth.access_denied",
                Some("the credentials need s3:ListBucket on the bucket"),
            ),
            e::ERROR_SIGNATURE_DOES_NOT_MATCH => (
                ExitCode::Setup,
                "auth.signature_mismatch",
                Some("check the secret key, --region and --path-style"),
            ),
            e::ERROR_PERMANENT_REDIRECT | e::ERROR_AUTH_HEADER_MALFORMED => (
                ExitCode::Setup,
                "config.wrong_region",
                Some("omit --region to detect the bucket's region, or pass the right one"),
            ),
            e::ERROR_S3_MISSING_REGION => (
                ExitCode::Setup,
                "config.missing_region",
                Some("pass --region or set AWS_REGION"),
            ),
            e::ERROR_NO_BUCKET => (
                ExitCode::Setup,
                "config.no_such_bucket",
                Some("check the bucket name and --endpoint-url"),
            ),
            e::ERROR_INCONSISTENT_LISTING => (
                ExitCode::Setup,
                "endpoint.inconsistent_listing",
                Some(
                    "the endpoint broke the ListObjectsV2 ordering contract; run `s3tl check` against it",
                ),
            ),
            e::ERROR_SLOW_DOWN | e::ERROR_TOO_MANY_REQUESTS => (
                ExitCode::Retryable,
                "net.throttled",
                Some("the endpoint is rate limiting; retry later or lower -c"),
            ),
            e::ERROR_S3_CLIENT_CONNECTION_TIMEOUT | e::ERROR_S3_NEXT_STREAM_TIMEOUT => {
                (ExitCode::Retryable, "net.timeout", None)
            }
            e::ERROR_INTERNAL_ERROR | e::ERROR_SERVICE_UNAVAILABLE => {
                (ExitCode::Retryable, "net.unavailable", None)
            }
            _ => (ExitCode::Retryable, "net.error", None),
        };
        let failure = Self::new(exit, code, message);
        match hint {
            Some(h) => failure.hint(h),
            None => failure,
        }
    }

    pub fn interrupted(message: impl Into<String>) -> Self {
        Self::new(ExitCode::Interrupted, "interrupted", message)
    }
}

/// One side of a list or diff.
#[derive(Debug, Clone, Serialize)]
pub struct Side {
    pub url: String,
    pub bucket: String,
    pub prefix: String,
    /// The signing region (`null` in a plan until detected).
    pub region: Option<String>,
    /// The endpoint, credentials redacted (`null`: AWS S3).
    pub endpoint_url: Option<String>,
    pub path_style: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileInfo {
    pub path: String,
    pub rows: usize,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputInfo {
    /// `parquet`, `ndjson` or `none`.
    pub format: &'static str,
    /// The dataset directory (`parquet`), `-` (`ndjson`), or `null`.
    pub path: Option<String>,
    /// Committed part files of the dataset, including earlier resumed runs'.
    pub files: Vec<FileInfo>,
    /// Rows this run wrote.
    pub rows: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Stats {
    /// Objects listed (a diff: distinct keys compared).
    pub objects: usize,
    /// Folder rows of a `--delimiter` listing.
    pub folders: usize,
    /// Total size of the objects (a diff: of the differing keys).
    pub bytes: u64,
    pub elapsed_secs: f64,
    /// Requests the endpoint rejected for rate.
    pub throttled: usize,
    /// Requests that timed out (and were retried).
    pub timeouts: usize,
    pub top_prefixes: Vec<PrefixMetric>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResumeInfo {
    /// This run continued an earlier checkpoint.
    pub resumed: bool,
    /// The checkpoint left for `--resume` (`null`: nothing left to resume).
    pub checkpoint: Option<String>,
    /// Key ranges the checkpoint leaves to list.
    pub remaining_ranges: usize,
}

/// The result document.
#[derive(Debug, Clone, Serialize)]
pub struct RunResult {
    pub schema: &'static str,
    pub version: &'static str,
    pub command: &'static str,
    pub ok: bool,
    pub status: Status,
    pub exit_code: i32,
    pub error: Option<ErrorInfo>,
    pub source: Option<Side>,
    pub target: Option<Side>,
    pub output: Option<OutputInfo>,
    pub stats: Option<Stats>,
    pub diff: Option<DiffCounts>,
    pub resume: Option<ResumeInfo>,
    /// `--dry-run`: how the run would proceed.
    pub plan: Option<serde_json::Value>,
    /// `check`: its findings.
    pub check: Option<serde_json::Value>,
    pub warnings: Vec<String>,
}

impl RunResult {
    pub fn new(command: &'static str) -> Self {
        Self {
            schema: RESULT_SCHEMA,
            version: env!("CARGO_PKG_VERSION"),
            command,
            ok: true,
            status: Status::Success,
            exit_code: 0,
            error: None,
            source: None,
            target: None,
            output: None,
            stats: None,
            diff: None,
            resume: None,
            plan: None,
            check: None,
            warnings: Vec::new(),
        }
    }

    /// Mark the result failed with `failure`.
    pub fn fail(&mut self, failure: Failure) {
        self.ok = false;
        self.status = if failure.exit == ExitCode::Interrupted {
            Status::Interrupted
        } else {
            Status::Failed
        };
        self.exit_code = failure.exit.code();
        self.error = Some(failure.error);
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| {
            format!(
                "{{\"schema\":\"{}\",\"ok\":false,\"exit_code\":1,\"error\":{{\"code\":\"internal.error\",\"message\":\"result serialization failed: {}\"}}}}",
                RESULT_SCHEMA, e
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_is_present() {
        let result = RunResult::new("list");
        let value: serde_json::Value = serde_json::from_str(&result.to_json()).unwrap();
        for key in [
            "schema",
            "version",
            "command",
            "ok",
            "status",
            "exit_code",
            "error",
            "source",
            "target",
            "output",
            "stats",
            "diff",
            "resume",
            "plan",
            "check",
            "warnings",
        ] {
            assert!(value.get(key).is_some(), "missing key {key}");
        }
        assert_eq!(value["schema"], RESULT_SCHEMA);
        assert_eq!(value["status"], "success");
    }

    #[test]
    fn failures_carry_exit_class_and_retryability() {
        let mut result = RunResult::new("list");
        result.fail(Failure::from_errno(e::ERROR_EXPIRED_TOKEN, "expired"));
        assert_eq!(result.exit_code, 3);
        let error = result.error.as_ref().unwrap();
        assert_eq!(error.code, "auth.expired_token");
        assert!(!error.retryable);
        assert!(error.hint.is_some());

        let throttled = Failure::from_errno(e::ERROR_SLOW_DOWN, "slow");
        assert_eq!(throttled.exit, ExitCode::Retryable);
        assert!(throttled.error.retryable);

        let mut interrupted = RunResult::new("list");
        interrupted.fail(Failure::interrupted("stop"));
        assert_eq!(interrupted.status, Status::Interrupted);
        assert_eq!(interrupted.exit_code, 130);
        assert!(interrupted.error.unwrap().retryable);
    }

    #[test]
    fn every_setup_errno_maps_to_exit_3() {
        for errno in [
            e::ERROR_S3_MISSING_CREDENTIALS,
            e::ERROR_EXPIRED_TOKEN,
            e::ERROR_INVALID_ACCESS_KEY,
            e::ERROR_ACCESS_DENIED,
            e::ERROR_SIGNATURE_DOES_NOT_MATCH,
            e::ERROR_PERMANENT_REDIRECT,
            e::ERROR_AUTH_HEADER_MALFORMED,
            e::ERROR_S3_MISSING_REGION,
            e::ERROR_NO_BUCKET,
            e::ERROR_INCONSISTENT_LISTING,
        ] {
            assert!(e::is_setup_error(errno), "errno {errno:#x}");
            assert_eq!(Failure::from_errno(errno, "x").exit, ExitCode::Setup);
        }
    }
}
