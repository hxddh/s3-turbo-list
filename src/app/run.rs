//! The list and diff run: command dispatch, config resolution and the
//! pre-run steps. The listing itself is in `listing.rs`, its result (exit
//! code, manifest, summaries) in `finish.rs`.

use super::*;

pub(crate) fn run() {
    let cli = parse_and_record_cli();
    if run_local_command(&cli) {
        return;
    }
    let (cli, resolved) = resolve_config(cli);
    if let Commands::Doctor { json, .. } = &cli.cmd {
        run_doctor(&cli, &resolved, *json);
        return;
    }
    if cli.dry_run {
        run_dry_run(&cli, &resolved);
        return;
    }
    let Resolved {
        cfg,
        config_source,
        diff_target_endpoint,
    } = resolved;

    let mut run_warnings = preflight_outputs(&cli, &cfg);
    init_logging(&cli, &cfg);
    if matches!(cli.cmd, Commands::CompatProbe { .. }) {
        dispatch_compat_probe(&cli, &cfg, &run_warnings);
        return;
    }
    let target = RunTarget::new(&cli);
    install_filter(&cli, &target.mode);

    // ── Phase 3: orchestration wiring ───────────────────────
    let g_tasks_count = if target.mode == RunMode::BiDir { 4 } else { 3 }; // list + data_map + mon (+right list)

    // Resolved up front (`resolve_output_paths`); empty for runs that write
    // no files.
    let filename_ks = cfg.output.ks_file.clone().unwrap_or_default();
    let filename_output = cfg.output.parquet_file.clone().unwrap_or_default();
    let writes_artifacts = list_writes_artifacts(&cli);
    validate_distinct_output_paths(
        &cli,
        &cfg,
        writes_artifacts.then_some(filename_ks.as_str()),
        writes_artifacts.then_some(filename_output.as_str()),
    );
    ensure_output_dir(&cli);
    if writes_artifacts && target.mode == RunMode::List {
        remove_stale_parquet_parts(&cli, &filename_output, &mut run_warnings);
    }

    let (quit, interrupted) = install_interrupt_handler();
    let g_state = core::GlobalState::new(quit, g_tasks_count);
    let run_started_at = chrono::Utc::now();
    let run_timer = Instant::now();

    // Build runtime
    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);

    let spec = RunSpec {
        cli: &cli,
        cfg: &cfg,
        target: &target,
        diff_target_endpoint: diff_target_endpoint.as_deref(),
        filename_ks: &filename_ks,
        filename_output: &filename_output,
        list_output_format: list_output_format(&cli).unwrap_or(ListOutputFormat::Parquet),
    };
    let outcome = rt.block_on(execute_listing(
        &spec,
        &g_state,
        &interrupted,
        &mut run_warnings,
    ));

    rt.shutdown_background();

    finish_run(RunEnd {
        spec: &spec,
        config_source,
        g_state: &g_state,
        interrupted: interrupted.load(Ordering::SeqCst),
        outcome,
        run_warnings,
        run_started_at,
        run_timer,
    });
}

/// The resolved configuration of a run: the config file with the command
/// line and the provider preset applied.
pub(crate) struct Resolved {
    pub(crate) cfg: S3TurboConfig,
    pub(crate) config_source: agent::ConfigSourceSummary,
    /// The endpoint the diff target side lists against (`diff_target_endpoint`).
    pub(crate) diff_target_endpoint: Option<String>,
}

/// What a list or diff run lists: the mode, each side's bucket and region,
/// and the listing prefix.
pub(crate) struct RunTarget<'a> {
    pub(crate) mode: RunMode,
    pub(crate) bucket: &'a str,
    pub(crate) region: Option<&'a str>,
    /// `Some` for a diff: the target side's region (itself optional).
    pub(crate) target_region: Option<Option<&'a str>>,
    /// `Some` for a diff: the target side's bucket.
    pub(crate) target_bucket: Option<&'a str>,
    /// The listing prefix; `/` on the command line means the whole bucket.
    pub(crate) prefix: String,
}

impl<'a> RunTarget<'a> {
    /// The target of a `list` or `diff` command; the other commands never
    /// get this far.
    pub(crate) fn new(cli: &'a Cli) -> Self {
        let (mode, region, bucket, target_region, target_bucket) = match &cli.cmd {
            Commands::List { region, bucket, .. } => (
                RunMode::List,
                region.as_deref(),
                bucket.as_str(),
                None,
                None,
            ),
            Commands::Diff {
                region,
                bucket,
                target_region,
                target_bucket,
                ..
            } => (
                RunMode::BiDir,
                region.as_deref(),
                bucket.as_str(),
                Some(target_region.as_deref()),
                Some(target_bucket.as_str()),
            ),
            Commands::CompatProbe { .. } => {
                unreachable!("compat-probe is dispatched before the run target is built")
            }
            Commands::Doctor { .. } => {
                unreachable!("local-only commands are handled before runtime setup")
            }
            Commands::Completions { .. } | Commands::Man => {
                unreachable!("local-only commands are handled before config load")
            }
            Commands::ManifestSummary { .. } | Commands::Guide { .. } => {
                unreachable!("local tooling commands are handled before config load")
            }
        };
        let prefix = if cli.prefix == "/" {
            String::new()
        } else {
            cli.prefix.clone()
        };
        Self {
            mode,
            bucket,
            region,
            target_region,
            target_bucket,
            prefix,
        }
    }
}

/// Everything the listing and its epilogue read about the run.
pub(crate) struct RunSpec<'a> {
    pub(crate) cli: &'a Cli,
    pub(crate) cfg: &'a S3TurboConfig,
    pub(crate) target: &'a RunTarget<'a>,
    pub(crate) diff_target_endpoint: Option<&'a str>,
    /// KeySpace and Parquet output paths; empty for runs that write no files.
    pub(crate) filename_ks: &'a str,
    pub(crate) filename_output: &'a str,
    pub(crate) list_output_format: ListOutputFormat,
}

/// The run's identity for checkpoint verification.
pub(crate) fn run_identity(
    cli: &Cli,
    cfg: &S3TurboConfig,
    target: &RunTarget<'_>,
) -> checkpoint::CheckpointIdentity {
    checkpoint::CheckpointIdentity::new(
        target.bucket,
        target.region,
        &target.prefix,
        Some(&cli.delimiter),
        cli.max_keys,
        cfg.s3.provider.as_deref(),
        Some(&cfg.s3.addressing_style.to_string()),
        Some(if target.mode == RunMode::BiDir {
            "bidir"
        } else {
            "list"
        }),
        cli.filter.as_deref(),
    )
    .with_endpoint(cfg.s3.endpoint_url.as_deref())
}

/// Parse the command line and record what the exit paths need to know
/// about the command.
fn parse_and_record_cli() -> Cli {
    let mut cli = parse_cli();
    // A diff without --target-region lists the target in --region. It used to
    // fall through to the SDK's ambient region (AWS_REGION / profile), so the
    // target was signed for another region than the plan showed (it showed
    // none), and a region-templated profile kept the source's endpoint.
    if let Commands::Diff {
        region,
        target_region,
        ..
    } = &mut cli.cmd
        && target_region.is_none()
    {
        *target_region = region.clone();
    }
    // Dry runs included: every non-zero list/diff/compat-probe exit prints
    // the `s3-turbo-list: run <status> (exit N): <reason>` line.
    let run_command = matches!(
        cli.cmd,
        Commands::List { .. } | Commands::Diff { .. } | Commands::CompatProbe { .. }
    );
    let _ = RUN_COMMAND.set(run_command);
    let _ = AGENT_RUN.set(run_command && cli.agent);
    let _ = DOCTOR_JSON.set(matches!(cli.cmd, Commands::Doctor { json: true, .. }));
    cli
}

/// Run a command that needs no config (completions, man, manifest-summary,
/// guide); `false` for every other command.
fn run_local_command(cli: &Cli) -> bool {
    match &cli.cmd {
        Commands::Completions { shell } => generate_completions(*shell),
        Commands::Man => generate_man_page(),
        Commands::ManifestSummary {
            manifest_file,
            json,
            check,
            ..
        } => run_manifest_summary(manifest_file, *json, *check),
        Commands::Guide { topic } => run_guide(topic.as_deref()),
        _ => return false,
    }
    true
}

/// Load the config file, apply the command line and the provider preset,
/// resolve the output paths, and validate the result (exit 2 on a problem).
fn resolve_config(mut cli: Cli) -> (Cli, Resolved) {
    let (mut cfg, config_load) = S3TurboConfig::load_with_summary(cli.config.as_deref())
        .unwrap_or_else(|e| exit_config_error(&format!("Config error: {}", e)));

    let config_source = agent::ConfigSourceSummary::new(&config_load, cli_config_overrides(&cli));
    set_doctor_context(&config_source, &cfg);
    validate_addressing_style_command(&cli);
    cfg.apply_cli_overrides(config::CliOverrides {
        threads: cli.threads,
        concurrency: cli.concurrency,
        endpoint: cli.endpoint.as_deref(),
        addressing_style: cli.addressing_style.as_deref(),
        provider: cli.provider.as_deref(),
        trace_compat: cli.trace_compat.as_deref(),
        start_after: cli.start_after.as_deref(),
        parquet_file: cli.output_parquet_file.as_deref(),
        compression: cli.compression.as_deref(),
        compression_level: cli.compression_level,
    });
    set_doctor_context(&config_source, &cfg);
    // A misspelled provider (`mino`) applied no preset at all — no endpoint,
    // no addressing style — and the run went to AWS. Like an unknown config
    // key, it is a configuration error.
    if let Some(name) = cfg.s3.provider.as_deref()
        && profiles::get_profile(name).is_none()
    {
        let source = match (&cli.provider, config_load.loaded_config.as_deref()) {
            (None, Some(path)) => format!("s3.provider in {}", path),
            _ => "--provider".to_string(),
        };
        exit_config_error(&format!(
            "{} '{}' is not a provider preset; use one of: {} \
                 (credentials profiles go in AWS_PROFILE)",
            source,
            name,
            profiles::all_profiles()
                .iter()
                .map(|profile| profile.name)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    // `BOS` and `bos` are one preset: the canonical name goes into the plan,
    // the manifest and the checkpoint identity (a resume under the other
    // spelling was an identity mismatch).
    cfg.normalize_provider();
    // Recorded before the preset fills it in: a diff derives the target
    // side's endpoint from its own region only when the user gave none.
    let endpoint_was_explicit = cfg.s3.endpoint_url.is_some();
    // A provider whose endpoint is built from the region (bos, oss, b2)
    // fills in its default region when none is given — and the request must
    // then be signed for that region, not the ambient AWS_REGION (it was:
    // `--provider bos` alone signed for us-east-1 against the bj host).
    // A provider whose region is not tied to the endpoint (r2: "auto") gets
    // its default whenever --region is omitted.
    if let Some(profile) = cfg.s3.provider.as_deref().and_then(profiles::get_profile)
        && (profile.endpoint_template.is_none() || !endpoint_was_explicit)
        && let Some(default_region) = profile.default_region
    {
        fill_default_region(&mut cli.cmd, default_region);
    }
    let cli = cli;
    cfg.apply_profile_preset(command_region(&cli.cmd));
    let diff_target_endpoint = diff_target_endpoint(&cli, &cfg, endpoint_was_explicit);
    resolve_output_paths(&cli, &mut cfg);
    set_doctor_context(&config_source, &cfg);
    validate_runtime_values(&cfg);
    validate_output_format_command(&cli);
    validate_start_after_command(&cli, &cfg);
    validate_delimiter_hints_command(&cli);
    (
        cli,
        Resolved {
            cfg,
            config_source,
            diff_target_endpoint,
        },
    )
}

/// The provider-setup and output checks before any request, then the
/// runtime warnings (printed unless --agent); returns those warnings.
fn preflight_outputs(cli: &Cli, cfg: &S3TurboConfig) -> Vec<String> {
    validate_provider_setup_or_exit(cli, cfg);
    // The dry run's output check, before any request: an output the run
    // cannot create (a directory, an unwritable path) used to surface only
    // after the whole listing had been paid for. It runs before the missing
    // parents are created, so it names the ancestor that blocks them.
    let planned = runtime_output_summary(
        cli,
        cfg,
        cfg.output.ks_file.as_deref(),
        cfg.output.parquet_file.as_deref(),
    );
    if let Some(problem) = planned_output_problems(&planned, cli).first() {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!(
                "Output error: {}",
                problem.trim_end_matches("; the run would exit 5")
            ),
        );
    }
    create_output_parents(cli, cfg);

    let run_warnings = runtime_guardrail_warnings(cli, cfg);
    if !cli.agent {
        print_runtime_warnings(&run_warnings);
    }
    run_warnings
}

/// Setup logging. `--agent` keeps stderr quiet: its default stderr filter
/// is off (RUST_LOG still wins); a `--log` file keeps the normal level.
fn init_logging(cli: &Cli, cfg: &S3TurboConfig) {
    let opt_log = cli.log || cfg.output.log_file.is_some();
    let loglevel = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        if cli.agent && !opt_log {
            "off".to_string()
        } else {
            "s3_turbo_list=info".to_string()
        }
    });

    if opt_log {
        let logfile_s =
            cfg.output.log_file.clone().unwrap_or_else(|| {
                format!("turbo_list_{}.log", Local::now().format("%Y%m%d%H%M%S"))
            });
        let logfile = match std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&logfile_s)
        {
            Ok(file) => file,
            Err(e) => {
                // An unwritable log path is an output failure, reported
                // through the documented exit codes like every other one.
                exit_before_run(
                    agent::ExitCode::OutputWrite,
                    format!("Failed to open log file '{}': {}", logfile_s, e),
                );
            }
        };
        env_logger::Builder::new()
            .parse_filters(&loglevel)
            .target(env_logger::Target::Pipe(Box::new(logfile)))
            .init();
    } else {
        env_logger::Builder::new().parse_filters(&loglevel).init();
    }
}

/// Run `compat-probe` against the endpoint the listing would use.
fn dispatch_compat_probe(cli: &Cli, cfg: &S3TurboConfig, run_warnings: &[String]) {
    let Commands::CompatProbe {
        region,
        bucket,
        output,
        ..
    } = &cli.cmd
    else {
        unreachable!("dispatch_compat_probe is called for compat-probe only")
    };
    // Same resolution as a listing run: the probe must exercise the
    // endpoint and addressing style the run would use, including
    // values that come from the config file or the provider.
    let endpoint_url = cfg.s3.endpoint_url.clone().unwrap_or_else(|| {
        exit_before_run(
            agent::ExitCode::ProviderSetup,
            format!(
                "Provider setup error: {}",
                compat_probe_endpoint_problem(cli, cfg)
            ),
        );
    });
    run_compat_probe(
        &endpoint_url,
        region.as_deref(),
        bucket,
        &listing_prefix(cli),
        &cfg.s3.addressing_style.to_string(),
        output.as_deref(),
        cfg,
        cli.agent,
        run_warnings.to_vec(),
    );
}

/// Install --filter if provided (exit 2 when it does not compile).
fn install_filter(cli: &Cli, mode: &RunMode) {
    if let Some(ref filter_expr) = cli.filter {
        if let Err(e) = config::install_filter(filter_expr, mode) {
            exit_before_run(agent::ExitCode::CliConfig, format!("Filter error: {}", e));
        }
        info!("Filter installed: \"{}\"", filter_expr);
    }
}

/// A pooled run writes `<name>.partN.parquet` beside the base file. A part
/// file left by an earlier, wider run of the same output path would be read
/// with this run's set by anything that globs the stem (and the base file is
/// being overwritten anyway), so remove it.
fn remove_stale_parquet_parts(cli: &Cli, filename_output: &str, run_warnings: &mut Vec<String>) {
    let stale = stale_parquet_parts(filename_output);
    let mut removed = 0usize;
    for path in &stale {
        match std::fs::remove_file(path) {
            Ok(()) => removed += 1,
            Err(e) => {
                exit_before_run(
                    agent::ExitCode::OutputWrite,
                    format!(
                        "Output error: cannot remove stale part file '{}': {}",
                        path, e
                    ),
                );
            }
        }
    }
    if removed > 0 {
        let warning = format!(
            "removed {} stale Parquet part file(s) left by an earlier run of '{}' (e.g. '{}')",
            removed, filename_output, stale[0]
        );
        if !cli.agent {
            eprintln!("Warning: {}", warning);
        }
        run_warnings.push(warning);
    }
}

/// Setup Ctrl-C handler: returns the run's quit flag and the flag that
/// records an interrupt.
fn install_interrupt_handler() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
    let quit = Arc::new(AtomicBool::new(false));
    let interrupted = Arc::new(AtomicBool::new(false));
    let q = quit.clone();
    let i = interrupted.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        q.store(true, Ordering::SeqCst);
        i.store(true, Ordering::SeqCst);
    }) {
        exit_before_run(
            agent::ExitCode::InternalError,
            format!("Failed to set ctrl-c signal handler: {}", e),
        );
    }
    (quit, interrupted)
}

/// Boundaries from an explicit --hints-file; empty otherwise (the run then
/// uses startup discovery, or lists one segment when it is disabled).
pub(crate) fn load_hints(hints_file: Option<&str>) -> Vec<String> {
    let Some(path) = hints_file else {
        return Vec::new();
    };
    hints::parse_hints_file(path).unwrap_or_else(|e| {
        // Through the standard failure epilogue: this exit used to print
        // nothing on stderr (only the log file had it) and no --agent JSON.
        exit_before_run(
            agent::ExitCode::CliConfig,
            format!("Hints file error: failed to load '{}': {}", path, e),
        )
    })
}
