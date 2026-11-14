//! Turning the command line into a validated job, without any network I/O.
//! Everything a dry run reports, and every usage error, comes from here.

use super::cli::{Cli, Command, S3Url};
use super::result::{Failure, FileInfo, OutputInfo, Side};
use s3tl::checkpoint::{Checkpoint, CheckpointError, CheckpointIdentity};
use s3tl::core::RunMode;
use std::path::{Path, PathBuf};

/// One side of a run: what to list and where.
#[derive(Debug, Clone)]
pub struct SideSpec {
    pub url: S3Url,
    /// Explicit endpoint (`None`: AWS S3).
    pub endpoint: Option<String>,
    pub path_style: bool,
    /// `--region`; otherwise resolved at run time.
    pub region: Option<String>,
}

impl SideSpec {
    pub fn describe(&self, region: Option<&str>) -> Side {
        Side {
            url: self.url.url(),
            bucket: self.url.bucket.clone(),
            prefix: self.url.prefix.clone(),
            region: region.map(str::to_string),
            endpoint_url: self.endpoint.as_deref().map(s3tl::redact::redact_url),
            path_style: self.path_style,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Output {
    /// Statistics only.
    None,
    /// NDJSON rows on stdout.
    Stdout,
    /// A Parquet dataset directory.
    Dir(PathBuf),
}

impl Output {
    pub fn dir(&self) -> Option<&Path> {
        match self {
            Output::Dir(dir) => Some(dir),
            _ => None,
        }
    }
}

/// A validated list or diff run.
#[derive(Debug)]
pub struct Job {
    pub mode: RunMode,
    pub source: SideSpec,
    pub target: Option<SideSpec>,
    pub output: Output,
    /// `--delimiter` ("" for a recursive listing).
    pub delimiter: String,
    pub concurrency: usize,
    pub max_keys: Option<i32>,
    pub no_auto_hints: bool,
    pub trace: Option<String>,
    /// The checkpoint a `--resume` continues.
    pub resume_from: Option<Checkpoint>,
    /// Index of the first part file this run writes.
    pub first_part: usize,
    /// Whether this run created the output directory (removed again when a
    /// failed run leaves it empty).
    pub creates_dir: bool,
}

impl Job {
    pub fn identity(&self) -> CheckpointIdentity {
        identity_of(&self.source, &self.delimiter, self.max_keys)
    }

    /// Whether the run uses startup discovery, and why not if it does not.
    pub fn partitioning(&self) -> (&'static str, Option<&'static str>) {
        if self.resume_from.is_some() {
            ("checkpoint", None)
        } else if !self.delimiter.is_empty() {
            (
                "single_segment",
                Some("a --delimiter listing is one ListObjectsV2 chain"),
            )
        } else if self.no_auto_hints {
            ("single_segment", Some("--no-auto-hints"))
        } else {
            ("startup_discovery", None)
        }
    }

    /// The output section of the result as known before the run.
    pub fn output_info(&self) -> OutputInfo {
        match &self.output {
            Output::None => OutputInfo {
                format: "none",
                path: None,
                files: Vec::new(),
                rows: 0,
            },
            Output::Stdout => OutputInfo {
                format: "ndjson",
                path: Some("-".to_string()),
                files: Vec::new(),
                rows: 0,
            },
            Output::Dir(dir) => OutputInfo {
                format: "parquet",
                path: Some(dir.display().to_string()),
                files: committed_parts(dir),
                rows: 0,
            },
        }
    }
}

fn identity_of(side: &SideSpec, delimiter: &str, max_keys: Option<i32>) -> CheckpointIdentity {
    CheckpointIdentity {
        bucket: side.url.bucket.clone(),
        prefix: side.url.prefix.clone(),
        endpoint_url: side.endpoint.clone(),
        delimiter: delimiter.to_string(),
        max_keys,
    }
}

/// The endpoint a side lists against: the explicit value, else the SDK's
/// standard environment variables (resolved here, so results, traces and
/// checkpoints name the endpoint actually used).
fn resolve_endpoint(explicit: Option<&str>) -> Result<Option<String>, Failure> {
    let env = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let endpoint = match explicit {
        Some(e) => Some(e.to_string()),
        None => env("AWS_ENDPOINT_URL_S3").or_else(|| env("AWS_ENDPOINT_URL")),
    };
    if let Some(e) = &endpoint
        && !(e.starts_with("http://") || e.starts_with("https://"))
    {
        return Err(Failure::usage(
            "usage.invalid_endpoint",
            format!(
                "endpoint '{}' must start with http:// or https://",
                s3tl::redact::redact_url(e)
            ),
        ));
    }
    Ok(endpoint)
}

/// Path-style addressing is automatic for endpoints a virtual-hosted name
/// can never resolve against: IP literals and localhost.
fn needs_path_style(endpoint: Option<&str>) -> bool {
    let Some(endpoint) = endpoint else {
        return false;
    };
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or("");
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = if let Some(rest) = host.strip_prefix('[') {
        // [v6]:port
        rest.split(']').next().unwrap_or(rest)
    } else {
        host.rsplit_once(':').map_or(host, |(h, _)| h)
    };
    host.eq_ignore_ascii_case("localhost") || host.parse::<std::net::IpAddr>().is_ok()
}

fn side(url: &str, endpoint: Option<&str>, cli: &Cli) -> Result<SideSpec, Failure> {
    let url = S3Url::parse(url).map_err(|m| Failure::usage("usage.invalid_url", m))?;
    let endpoint = resolve_endpoint(endpoint)?;
    Ok(SideSpec {
        path_style: cli.global.path_style || needs_path_style(endpoint.as_deref()),
        url,
        endpoint,
        region: cli.global.region.clone(),
    })
}

/// The side `check` inspects (an empty bucket when it has no S3 URL).
pub fn side_for_check(url: Option<S3Url>, cli: &Cli) -> Result<SideSpec, Failure> {
    let endpoint = resolve_endpoint(cli.global.endpoint_url.as_deref())?;
    Ok(SideSpec {
        url: url.unwrap_or(S3Url {
            bucket: String::new(),
            prefix: String::new(),
        }),
        path_style: cli.global.path_style || needs_path_style(endpoint.as_deref()),
        endpoint,
        region: cli.global.region.clone(),
    })
}

/// Committed part files of a dataset directory, in index order.
pub fn committed_parts(dir: &Path) -> Vec<FileInfo> {
    let mut parts: Vec<(usize, PathBuf)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let index = name
                .strip_prefix("part-")?
                .strip_suffix(".parquet")?
                .parse::<usize>()
                .ok()?;
            Some((index, entry.path()))
        })
        .collect();
    parts.sort();
    parts
        .into_iter()
        .map(|(_, path)| FileInfo {
            rows: parquet_rows(&path),
            bytes: std::fs::metadata(&path).map_or(0, |m| m.len()),
            path: path.display().to_string(),
        })
        .collect()
}

/// Row count from a Parquet footer (0 when unreadable).
fn parquet_rows(path: &Path) -> usize {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    std::fs::File::open(path)
        .ok()
        .and_then(|f| SerializedFileReader::new(f).ok())
        .map_or(0, |r| {
            r.metadata().file_metadata().num_rows().max(0) as usize
        })
}

fn next_part_index(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .into_string()
                .ok()?
                .strip_prefix("part-")?
                .strip_suffix(".parquet")?
                .parse::<usize>()
                .ok()
        })
        .max()
        .map_or(0, |max| max + 1)
}

/// Remove temp part files a crashed run left behind (never committed).
fn remove_stale_temp_parts(dir: &Path) {
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        if let Ok(name) = entry.file_name().into_string()
            && name.starts_with(".part-")
            && name.ends_with(".tmp")
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Validate a fresh output directory: absent, or an empty directory.
fn check_fresh_dir(dir: &Path) -> Result<bool, Failure> {
    match std::fs::read_dir(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotADirectory || dir.is_file() => {
            Err(Failure::usage(
                "usage.output_not_a_directory",
                format!("output '{}' exists and is not a directory", dir.display()),
            ))
        }
        Err(e) => Err(Failure::output(format!(
            "cannot read output directory '{}': {}",
            dir.display(),
            e
        ))),
        Ok(mut entries) => {
            if entries.next().is_some() {
                let has_checkpoint = Checkpoint::path(dir).exists();
                let failure = Failure::usage(
                    "usage.output_not_empty",
                    format!("output directory '{}' is not empty", dir.display()),
                );
                Err(if has_checkpoint {
                    failure.hint("it holds an interrupted run: add --resume to continue it")
                } else {
                    failure.hint("write to a new directory, or delete this one")
                })
            } else {
                Ok(false)
            }
        }
    }
}

/// Build the job for a list or diff command.
pub fn prepare(cli: &Cli) -> Result<Job, Failure> {
    let g = &cli.global;
    let (mode, source, target, output, delimiter, resume) = match &cli.cmd {
        Command::List {
            source,
            output,
            delimiter,
            resume,
        } => (
            RunMode::List,
            side(source, g.endpoint_url.as_deref(), cli)?,
            None,
            output.as_deref(),
            delimiter.clone().unwrap_or_default(),
            *resume,
        ),
        Command::Diff {
            source,
            target,
            output,
            target_endpoint_url,
        } => {
            if output.as_deref() == Some("-") {
                return Err(Failure::usage(
                    "usage.invalid_output",
                    "diff writes a Parquet dataset: -o takes a directory, not '-'",
                ));
            }
            let target_endpoint = target_endpoint_url.as_deref().or(g.endpoint_url.as_deref());
            (
                RunMode::BiDir,
                side(source, g.endpoint_url.as_deref(), cli)?,
                Some(side(target, target_endpoint, cli)?),
                output.as_deref(),
                String::new(),
                false,
            )
        }
        Command::Check { .. } => unreachable!("check is not a listing job"),
    };

    let output = match output {
        None => Output::None,
        Some("-") => Output::Stdout,
        Some("") => {
            return Err(Failure::usage(
                "usage.invalid_output",
                "-o needs a directory or '-'",
            ));
        }
        Some(dir) => Output::Dir(PathBuf::from(dir)),
    };

    let mut job = Job {
        mode,
        source,
        target,
        output,
        delimiter,
        concurrency: g.concurrency as usize,
        max_keys: g.max_keys,
        no_auto_hints: g.no_auto_hints,
        trace: g.trace.clone(),
        resume_from: None,
        first_part: 0,
        creates_dir: false,
    };

    if resume {
        let Some(dir) = job.output.dir().map(Path::to_path_buf) else {
            return Err(Failure::usage(
                "usage.resume_needs_output",
                "--resume continues a Parquet dataset: pass -o with the interrupted run's directory",
            ));
        };
        let checkpoint = Checkpoint::load(&dir, &job.identity()).map_err(|e| match e {
            CheckpointError::Missing => Failure::usage(
                "usage.no_checkpoint",
                format!("'{}' has no checkpoint to resume", dir.display()),
            )
            .hint("the run there finished or never started; drop --resume and use a new directory"),
            CheckpointError::Mismatch(_) => Failure::usage(
                "usage.checkpoint_mismatch",
                format!("'{}': {}", dir.display(), e),
            )
            .hint("resume with the same S3 URL, endpoint and options as the interrupted run"),
            CheckpointError::Unreadable(_) => Failure::usage(
                "usage.checkpoint_unreadable",
                format!("'{}': {}", dir.display(), e),
            ),
        })?;
        if !cli.global.dry_run {
            remove_stale_temp_parts(&dir);
        }
        job.first_part = next_part_index(&dir);
        job.resume_from = Some(checkpoint);
    } else if let Some(dir) = job.output.dir() {
        job.creates_dir = check_fresh_dir(dir)?;
    }
    Ok(job)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_style_is_automatic_for_ip_and_localhost() {
        assert!(needs_path_style(Some("http://localhost:9000")));
        assert!(needs_path_style(Some("http://127.0.0.1:9000/")));
        assert!(needs_path_style(Some("http://user:pw@10.0.0.1")));
        assert!(needs_path_style(Some("http://[::1]:9000")));
        assert!(!needs_path_style(Some("https://s3.example.com")));
        assert!(!needs_path_style(Some("https://minio.internal:9000")));
        assert!(!needs_path_style(None));
    }

    #[test]
    fn endpoint_must_be_a_url() {
        assert!(resolve_endpoint(Some("localhost:9000")).is_err());
        assert_eq!(
            resolve_endpoint(Some("http://h")).unwrap().as_deref(),
            Some("http://h")
        );
    }

    #[test]
    fn fresh_dir_rules() {
        let dir = tempfile::tempdir().unwrap();
        assert!(check_fresh_dir(&dir.path().join("new")).unwrap());
        assert!(!check_fresh_dir(dir.path()).unwrap());
        std::fs::write(dir.path().join("x"), "").unwrap();
        let err = check_fresh_dir(dir.path()).unwrap_err();
        assert_eq!(err.error.code, "usage.output_not_empty");
        let err = check_fresh_dir(&dir.path().join("x")).unwrap_err();
        assert_eq!(err.error.code, "usage.output_not_a_directory");
    }

    #[test]
    fn part_indexes_continue_after_committed_parts() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(next_part_index(dir.path()), 0);
        std::fs::write(dir.path().join("part-00000.parquet"), "").unwrap();
        std::fs::write(dir.path().join("part-00003.parquet"), "").unwrap();
        std::fs::write(dir.path().join(".part-00009.parquet.tmp"), "").unwrap();
        assert_eq!(next_part_index(dir.path()), 4);
        remove_stale_temp_parts(dir.path());
        assert!(!dir.path().join(".part-00009.parquet.tmp").exists());
        assert_eq!(committed_parts(dir.path()).len(), 2);
    }
}
