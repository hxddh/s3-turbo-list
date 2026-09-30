//! The dry-run plan: hints and checkpoint plans, output conflicts and
//! output path checks.

use super::*;

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
    /// `--start-after`: one sequential chain.
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
    let plan_without_hints = |source: &str, note: &str| HintsPlan {
        source: source.to_string(),
        path: None,
        exists: false,
        valid: None,
        format: None,
        boundary_count: None,
        warnings: Vec::new(),
        note: Some(note.to_string()),
    };
    if single_chain {
        return plan_without_hints(
            "single_chain",
            "--start-after lists one sequential ListObjectsV2 chain; hints, startup \
             discovery and runtime splitting are skipped",
        );
    }
    if let Some(path) = explicit_hints_file {
        let report = inspect_hints_for_plan(path);
        // The run loads the file with `parse_hints_file`: the plan's verdict
        // must be the one the run reaches (it exits 2 on a file it cannot load).
        let loads = hints::parse_hints_file(path).is_ok();
        return HintsPlan {
            source: "explicit".to_string(),
            path: Some(path.to_string()),
            exists: Path::new(path).exists(),
            valid: Some(loads && report.as_ref().is_none_or(|r| r.valid)),
            format: report
                .as_ref()
                .map(|r| format!("{:?}", r.format).to_lowercase()),
            boundary_count: report.as_ref().map(|r| r.boundary_count),
            warnings: report
                .map(|r| r.warnings)
                .unwrap_or_else(|| vec!["hints file does not exist or could not be parsed".into()]),
            note: None,
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
        note: None,
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
        warnings: Vec::new(),
        note: Some(
            "each side is partitioned from its own structure at startup and listed in \
             parallel; the ordered segment streams are merged"
                .to_string(),
        ),
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
        remaining_ranges: None,
        identity_fields: vec![
            "bucket".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            "delimiter".to_string(),
            "max_keys".to_string(),
            "provider".to_string(),
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
            // The run discards a checkpoint with no ranges left (or none
            // recorded, pre-0.36) and lists everything again; say so here.
            plan.valid = Some(journal.remaining.as_ref().is_some_and(|r| !r.is_empty()));
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
        outputs.report_file.as_ref(),
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

#[cfg(test)]
mod tests {
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
}
