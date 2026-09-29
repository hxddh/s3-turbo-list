//! Output paths: naming, reservation, and the checks shared by the plan
//! and the run.

use super::*;

/// Where this run keeps its checkpoint: `None` for runs that cannot resume
/// (diff, --start-after, --continuation-token). In --output-dir when one is
/// given — with the outputs it describes — else the working directory.
pub(crate) fn run_checkpoint_path(
    cli: &Cli,
    bucket: &str,
    region: Option<&str>,
    prefix: &str,
) -> Option<String> {
    if !matches!(cli.cmd, Commands::List { .. })
        || cli.start_after.is_some()
        || cli.continuation_token.is_some()
    {
        return None;
    }
    let name = checkpoint::checkpoint_path_for_prefix(bucket, region, prefix);
    Some(match cli.output_dir.as_deref() {
        Some(dir) => std::path::Path::new(dir).join(name).display().to_string(),
        None => name,
    })
}

/// Every file a run writes, resolved once — for the plan, the run and the
/// manifest alike:
///
/// - Parquet: `--output-parquet-file`, else `<dir>/<stem>.parquet` with the
///   auto-named stem (`--output-dir`, else the working directory).
/// - KeySpace: beside the Parquet file as `<name>.ks` (the deprecated
///   `--output-ks-file` still wins). It used to land in the working
///   directory under a timestamp whenever only the Parquet path was given.
/// - Log (`--log`): beside the outputs as `<name>.log`, uniquely named with
///   them (every run in one second used to share, and truncate, one
///   `turbo_list_<ts>.log`, so earlier manifests failed `--check`).
///
/// An auto-named stem that is taken gets a `_N` suffix, and a real run
/// reserves it by creating its first file exclusively, so runs started at
/// the same moment cannot pick the same name (they all did, and all
/// reported success over one set of files).
pub(crate) fn resolve_output_paths(cli: &Cli, cfg: &mut S3TurboConfig) {
    let artifacts = list_writes_artifacts(cli);
    let (region, bucket, target_region, target_bucket, stem_suffix) = match &cli.cmd {
        Commands::List { region, bucket, .. } => {
            (region.as_deref(), bucket.as_str(), None, None, "")
        }
        Commands::Diff {
            region,
            bucket,
            target_region,
            target_bucket,
            ..
        } => (
            region.as_deref(),
            bucket.as_str(),
            target_region.as_deref(),
            Some(target_bucket.as_str()),
            "",
        ),
        Commands::CompatProbe { region, bucket, .. } => (
            region.as_deref(),
            bucket.as_str(),
            None,
            None,
            "_compat-probe",
        ),
        _ => return,
    };
    if !artifacts {
        cfg.output.parquet_file = None;
        cfg.output.ks_file = None;
    }
    let wants_log = cli.log && cfg.output.log_file.is_none();
    // Explicit Parquet path: KS and log follow its name, nothing is reserved.
    if let Some(parquet) = cfg.output.parquet_file.clone() {
        let base = parquet.strip_suffix(".parquet").unwrap_or(&parquet);
        cfg.output
            .ks_file
            .get_or_insert_with(|| format!("{}.ks", base));
        if wants_log {
            cfg.output.log_file = Some(format!("{}.log", base));
        }
        return;
    }
    if !artifacts && !wants_log {
        return;
    }
    let base = format!(
        "{}{}",
        output_stem_with_timestamp(
            region,
            bucket,
            target_region,
            target_bucket,
            &listing_prefix(cli),
            &Local::now().format("%Y%m%d%H%M%S").to_string(),
        ),
        stem_suffix
    );
    let mut extensions: Vec<&str> = Vec::new();
    if artifacts {
        extensions.extend([".parquet", ".ks"]);
    }
    if wants_log {
        extensions.push(".log");
    }
    let path_of = |name: String| match cli.output_dir.as_deref() {
        Some(dir) => format!("{}/{}", dir, name),
        None => name,
    };
    let stem = pick_output_stem(&base, &extensions, &path_of, !cli.dry_run);
    if artifacts {
        cfg.output.parquet_file = Some(path_of(format!("{}.parquet", stem)));
        cfg.output
            .ks_file
            .get_or_insert_with(|| path_of(format!("{}.ks", stem)));
    }
    if wants_log {
        cfg.output.log_file = Some(path_of(format!("{}.log", stem)));
    }
}

/// The first of `base`, `base_1`, `base_2`, … none of whose files exist.
/// With `reserve`, the stem is claimed by creating its first file with
/// `create_new`, which only one process can win; the file is removed again
/// if the run stops before writing it (`exit_before_run`).
pub(crate) fn pick_output_stem(
    base: &str,
    extensions: &[&str],
    path_of: &dyn Fn(String) -> String,
    reserve: bool,
) -> String {
    for n in 0.. {
        let stem = if n == 0 {
            base.to_string()
        } else {
            format!("{}_{}", base, n)
        };
        let paths: Vec<String> = extensions
            .iter()
            .map(|ext| path_of(format!("{}{}", stem, ext)))
            .collect();
        if paths.iter().any(|path| std::path::Path::new(path).exists()) {
            continue;
        }
        if !reserve {
            return stem;
        }
        let anchor = &paths[0];
        if let Some(parent) = std::path::Path::new(anchor)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            // A parent that cannot be created is reported by the run's own
            // output checks; the name is still the right one.
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(anchor)
        {
            Ok(_) => {
                let _ = RESERVED_OUTPUT.set(anchor.clone());
                return stem;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return stem,
        }
    }
    unreachable!("an unused suffix exists")
}

/// The auto-generated output name for a run at `timestamp` (uniqueness is
/// `pick_output_stem`'s job).
pub(crate) fn output_stem_with_timestamp(
    region: Option<&str>,
    bucket: &str,
    target_region: Option<&str>,
    target_bucket: Option<&str>,
    prefix: &str,
    timestamp: &str,
) -> String {
    let mut parts = Vec::new();
    if let Some(region) = region.filter(|r| !r.is_empty()) {
        parts.push(sanitize_path_component(region));
    }
    parts.push(sanitize_path_component(bucket));
    if let Some(target_region) = target_region.filter(|r| !r.is_empty()) {
        parts.push(sanitize_path_component(target_region));
    }
    if let Some(target_bucket) = target_bucket {
        parts.push(sanitize_path_component(target_bucket));
    }
    // Runs over different prefixes of one bucket get different names (the
    // hints cache and checkpoint are keyed the same way): parallel per-prefix
    // runs started in the same second used to overwrite each other.
    if !prefix.is_empty() {
        let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(prefix.as_bytes()));
        parts.push(format!("p{}", &digest[..8]));
    }
    parts.push(timestamp.to_string());
    parts.join("_")
}

pub(crate) fn sanitize_path_component(value: &str) -> String {
    agent::sanitize_path_component(value)
}

/// `<base>.partN.parquet` files that exist for `base`, in index order.
pub(crate) fn stale_parquet_parts(base: &str) -> Vec<String> {
    (1..s3_turbo_list::data_map::MAX_LIST_OUTPUT_WORKERS)
        .map(|index| s3_turbo_list::data_map::part_path(base, index))
        .filter(|path| std::path::Path::new(path).is_file())
        .collect()
}

/// Every file a run writes must have its own path. Two outputs sharing one
/// silently destroy each other — the KS file, written after the Parquet
/// writers close, used to overwrite a Parquet file at the same path while the
/// run reported success and `manifest-summary --check` passed. Paths are
/// compared lexically after making them absolute; an existing non-regular
/// file (e.g. `/dev/null`) may be shared.
pub(crate) fn validate_distinct_output_paths(
    cli: &Cli,
    cfg: &S3TurboConfig,
    ks: Option<&str>,
    parquet: Option<&str>,
) {
    if !matches!(cli.cmd, Commands::List { .. } | Commands::Diff { .. }) {
        return;
    }
    let mut outputs: Vec<(String, String)> = Vec::new();
    if let Some(path) = parquet {
        outputs.push(("--output-parquet-file".to_string(), path.to_string()));
        if matches!(cli.cmd, Commands::List { .. }) {
            for index in 1..s3_turbo_list::data_map::MAX_LIST_OUTPUT_WORKERS {
                outputs.push((
                    format!("Parquet part file {}", index),
                    s3_turbo_list::data_map::part_path(path, index),
                ));
            }
        }
    }
    let named = [
        ("--output-ks-file", ks),
        ("--output-log-file", cfg.output.log_file.as_deref()),
        ("--trace-compat", cfg.s3.trace_compat.as_deref()),
        ("--run-manifest", cli.run_manifest.as_deref()),
        (
            "--plan-json",
            cli.plan_json.as_deref().filter(|_| cli.dry_run),
        ),
    ];
    for (label, path) in named {
        if let Some(path) = path {
            outputs.push((label.to_string(), path.to_string()));
        }
    }
    let key = |path: &str| -> Option<std::path::PathBuf> {
        let p = std::path::Path::new(path);
        if p.exists() && !p.is_file() {
            return None; // devices and the like may be shared
        }
        let absolute = std::path::absolute(p).ok()?;
        let mut normalized = std::path::PathBuf::new();
        for component in absolute.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                other => normalized.push(other),
            }
        }
        Some(normalized)
    };
    let mut seen: Vec<(std::path::PathBuf, &str, &str)> = Vec::new();
    for (label, path) in &outputs {
        let Some(k) = key(path) else { continue };
        if let Some((_, other_label, other_path)) =
            seen.iter().find(|(seen_key, _, _)| *seen_key == k)
        {
            exit_before_run(
                agent::ExitCode::CliConfig,
                format!(
                    "{} '{}' and {} '{}' are the same file; every output needs its own path",
                    other_label, other_path, label, path
                ),
            );
        }
        seen.push((k, label, path));
    }
}

/// Create the parent directories of explicit output paths, as `--output-dir`
/// already does for its own. A missing parent used to surface only once the
/// listing was done (exit 5, reason only in the log) — after the S3 requests
/// had been paid for.
pub(crate) fn create_output_parents(cli: &Cli, cfg: &S3TurboConfig) {
    if !matches!(
        cli.cmd,
        Commands::List { .. } | Commands::Diff { .. } | Commands::CompatProbe { .. }
    ) {
        return;
    }
    let paths = [
        cfg.output.parquet_file.as_deref(),
        cfg.output.ks_file.as_deref(),
        cfg.output.log_file.as_deref(),
        cfg.s3.trace_compat.as_deref(),
        cli.run_manifest.as_deref(),
    ];
    for path in paths.into_iter().flatten() {
        let Some(parent) = std::path::Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        else {
            continue;
        };
        if let Err(e) = std::fs::create_dir_all(parent) {
            exit_before_run(
                agent::ExitCode::OutputWrite,
                format!(
                    "Output error: cannot create directory '{}' for '{}': {}",
                    parent.display(),
                    path,
                    e
                ),
            );
        }
    }
}

pub(crate) fn ensure_output_dir(cli: &Cli) {
    if cli.dry_run {
        return;
    }
    if let Some(dir) = cli.output_dir.as_deref()
        && let Err(e) = std::fs::create_dir_all(dir)
    {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!("Output directory error: failed to create '{}': {}", dir, e),
        );
    }
}

/// Output files the run would fail to create (exit 5), with the reason.
pub(crate) fn planned_output_problems(
    outputs: &agent::OutputPathSummary,
    cli: &Cli,
) -> Vec<String> {
    [
        outputs.parquet_file.as_deref(),
        outputs.ks_file.as_deref(),
        outputs.log_file.as_deref(),
        outputs.trace_compat.as_deref(),
        cli.run_manifest.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter_map(|path| {
        agent::output_path_problem(path).map(|problem| {
            format!(
                "output '{}' cannot be created: {}; the run would exit 5",
                path, problem
            )
        })
    })
    .collect()
}

pub(crate) fn runtime_output_summary(
    cli: &Cli,
    cfg: &S3TurboConfig,
    ks_file: Option<&str>,
    parquet_file: Option<&str>,
) -> agent::OutputPathSummary {
    if !list_writes_artifacts(cli) {
        return agent::OutputPathSummary {
            parquet_file: None,
            ks_file: None,
            hints_file: None,
            trace_compat: cfg.s3.trace_compat.clone(),
            log_file: cfg.output.log_file.clone(),
        };
    }

    agent::OutputPathSummary {
        parquet_file: parquet_file.map(str::to_string),
        ks_file: ks_file.map(str::to_string),
        hints_file: None,
        trace_compat: cfg.s3.trace_compat.clone(),
        log_file: cfg.output.log_file.clone(),
    }
}

pub(crate) fn planned_output_paths(
    cli: &Cli,
    cfg: &S3TurboConfig,
) -> (Option<String>, Option<String>, Option<String>) {
    if !list_writes_artifacts(cli) {
        return (None, None, None);
    }
    (
        cfg.output.ks_file.clone(),
        cfg.output.parquet_file.clone(),
        None,
    )
}

// ── Unified hints loader ───────────────────────────────────
