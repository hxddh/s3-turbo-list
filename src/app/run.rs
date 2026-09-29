//! The list and diff run.

use super::*;

pub(crate) fn run() {
    let mut cli = parse_cli();
    // Deprecations go to stderr and into the plan / manifest / doctor
    // report; `--agent` keeps stderr quiet, so there they are JSON only.
    if !cli.agent {
        for warning in cli.deprecation_warnings() {
            eprintln!("warning: {}", warning);
        }
    }
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

    match &cli.cmd {
        Commands::Completions { shell } => {
            generate_completions(*shell);
            return;
        }
        Commands::Man => {
            generate_man_page();
            return;
        }
        Commands::ManifestSummary {
            manifest_file,
            json,
            check,
            ..
        } => {
            run_manifest_summary(manifest_file, *json, *check);
            return;
        }
        Commands::Guide { topic } => {
            run_guide(topic.as_deref());
            return;
        }
        _ => {}
    }

    // Load config.
    let (mut cfg, config_load) = S3TurboConfig::load_with_summary(cli.config.as_deref())
        .unwrap_or_else(|e| exit_config_error(&format!("Config error: {}", e)));
    if !cli.agent {
        for warning in config_load
            .warnings
            .iter()
            .filter(|warning| warning.starts_with("deprecated"))
        {
            eprintln!("warning: {}", warning);
        }
    }

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
    let config_source_warnings = config_source.warnings.clone();

    if let Commands::Doctor { json, .. } = &cli.cmd {
        // doctor absorbed the former hints-validate command: when a hints
        // file is supplied it is linted and embedded in the report.
        let hints = cli.hints_file.as_deref().map(|path| {
            hints::inspect_hints_file(path, 5).unwrap_or_else(|e| {
                exit_doctor_check_error("hints", &format!("Hints validation failed: {}", e))
            })
        });
        let mut report = agent::doctor_report(&cfg, config_source.clone(), hints);
        for warning in cli.deprecation_warnings() {
            report.checks.push(agent::DoctorCheck {
                name: "deprecated".to_string(),
                status: "warn".to_string(),
                message: warning,
            });
        }
        // A filter that would fail the real run (exit 2) fails doctor too.
        if let Some(filter_expr) = cli.filter.as_deref() {
            let check = match config::compile_filter_with_mode(filter_expr, &RunMode::List)
                .or_else(|_| config::compile_filter_with_mode(filter_expr, &RunMode::BiDir))
            {
                Ok(_) => agent::DoctorCheck {
                    name: "filter".to_string(),
                    status: "ok".to_string(),
                    message: format!("filter compiles: {}", filter_expr),
                },
                Err(e) => agent::DoctorCheck {
                    name: "filter".to_string(),
                    status: "error".to_string(),
                    message: format!("filter does not compile: {}", e),
                },
            };
            if check.status == "error" {
                report.status = "error".to_string();
            }
            report.checks.push(check);
        }
        if *json {
            println!("{}", agent::to_pretty_json(&report));
        } else {
            print_doctor_report(&report);
        }
        if report.status == "error" {
            // An endpoint/provider error is the same setup failure a real
            // run exits 3 on; any other error is a local config problem.
            let setup_error = report
                .checks
                .iter()
                .any(|check| check.name == "endpoint_url" && check.status == "error");
            std::process::exit(if setup_error {
                agent::ExitCode::ProviderSetup.code()
            } else {
                agent::ExitCode::CliConfig.code()
            });
        }
        return;
    }

    if cli.dry_run {
        // Validate --filter exactly as a real run would, so a bad expression
        // fails with exit code 2 at plan time instead of at run time.
        if let Some(ref filter_expr) = cli.filter {
            let mode = if matches!(cli.cmd, Commands::Diff { .. }) {
                RunMode::BiDir
            } else {
                RunMode::List
            };
            if let Err(e) = config::compile_filter_with_mode(filter_expr, &mode) {
                exit_before_run(agent::ExitCode::CliConfig, format!("Filter error: {}", e));
            }
        }
        let (planned_ks, planned_parquet, _) = planned_output_paths(&cli, &cfg);
        validate_distinct_output_paths(
            &cli,
            &cfg,
            planned_ks.as_deref(),
            planned_parquet.as_deref(),
        );
        let report = build_plan_report(
            &cli,
            &cfg,
            config_source.clone(),
            diff_target_endpoint.as_deref(),
        );
        println!("{}", agent::to_pretty_json(&report));
        // The plan is the JSON result: a blocked dry run exits with the
        // run's code and its run line, but prints no second JSON document.
        let _ = PLAN_PRINTED.set(true);
        // An explicit hints file the run cannot load stops it with exit 2;
        // so does the plan (status `blocked`, `hints.valid: false`).
        if let Some(path) = cli.hints_file.as_deref()
            && let Err(e) = hints::parse_hints_file(path)
        {
            exit_before_run(
                agent::ExitCode::CliConfig,
                format!("Hints file error: {}", e),
            );
        }
        // The plan must predict the run: a setup problem the run would stop
        // on with exit 3 fails the dry run the same way (plan still written).
        if let Some(error) = provider_setup_guardrail_warnings(&cli, &cfg).first() {
            exit_before_run(
                agent::ExitCode::ProviderSetup,
                format!("Provider setup error: {}", error),
            );
        }
        if let Some(problem) = planned_output_problems(&report.outputs, &cli).first() {
            exit_before_run(
                agent::ExitCode::OutputWrite,
                format!("Output error: {}", problem),
            );
        }
        return;
    }

    validate_provider_setup_or_exit(&cli, &cfg);
    create_output_parents(&cli, &cfg);
    // The dry run's output check, before any request: an output the run
    // cannot create (a directory, an unwritable path) used to surface only
    // after the whole listing had been paid for.
    let planned = runtime_output_summary(
        &cli,
        &cfg,
        cfg.output.ks_file.as_deref(),
        cfg.output.parquet_file.as_deref(),
    );
    if let Some(problem) = planned_output_problems(&planned, &cli).first() {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!(
                "Output error: {}",
                problem.trim_end_matches("; the run would exit 5")
            ),
        );
    }

    let mut run_warnings = config_source_warnings;
    run_warnings.extend(cli.deprecation_warnings());
    run_warnings.extend(runtime_guardrail_warnings(&cli, &cfg));
    if !cli.agent {
        // Deprecations were printed as `warning: deprecated …` already.
        let fresh: Vec<String> = run_warnings
            .iter()
            .filter(|warning| !warning.starts_with("deprecated"))
            .cloned()
            .collect();
        print_runtime_warnings(&fresh);
    }

    // Setup logging. `--agent` keeps stderr quiet: its default stderr filter
    // is off (RUST_LOG still wins); a `--log` file keeps the normal level.
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

    // Parse subcommand.
    let (mode, opt_region, opt_bucket, opt_target_region, opt_target_bucket) = match &cli.cmd {
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
        Commands::CompatProbe {
            region,
            bucket,
            output,
            ..
        } => {
            // Same resolution as a listing run: the probe must exercise the
            // endpoint and addressing style the run would use, including
            // values that come from the config file or the provider.
            let endpoint_url = cfg.s3.endpoint_url.clone().unwrap_or_else(|| {
                exit_before_run(
                    agent::ExitCode::ProviderSetup,
                    format!(
                        "Provider setup error: {}",
                        compat_probe_endpoint_problem(&cli, &cfg)
                    ),
                );
            });
            run_compat_probe(
                &endpoint_url,
                region.as_deref(),
                bucket,
                &listing_prefix(&cli),
                &cfg.s3.addressing_style.to_string(),
                output.as_deref(),
                &cfg,
                cli.agent,
            );
            return;
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
    let opt_prefix = if cli.prefix == "/" {
        String::new()
    } else {
        cli.prefix.clone()
    };

    // Install filter if provided.
    if let Some(ref filter_expr) = cli.filter {
        if let Err(e) = config::install_filter(filter_expr, &mode) {
            exit_before_run(agent::ExitCode::CliConfig, format!("Filter error: {}", e));
        }
        info!("Filter installed: \"{}\"", filter_expr);
    }

    // ── Phase 3: orchestration wiring ───────────────────────
    let g_tasks_count = if mode == RunMode::BiDir { 4 } else { 3 }; // list + data_map + mon (+right list)

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
    if writes_artifacts && mode == RunMode::List {
        // A pooled run writes `<name>.partN.parquet` beside the base file. A
        // part file left by an earlier, wider run of the same output path
        // would be read with this run's set by anything that globs the stem
        // (and the base file is being overwritten anyway), so remove it.
        let stale = stale_parquet_parts(&filename_output);
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

    // Setup Ctrl-C handler
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

    let g_state = core::GlobalState::new(quit, g_tasks_count);
    let run_started_at = chrono::Utc::now();
    let run_timer = Instant::now();

    // Build runtime
    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);

    let list_output_format = list_output_format(&cli).unwrap_or(ListOutputFormat::Parquet);

    // The async block yields what the run learned about resuming: the manifest
    // is built after it, and a completed run has already removed its
    // checkpoint, so the file on disk can no longer answer this.
    let (resumed_segments_skipped, checkpoint_note): (Option<usize>, Option<String>) = rt
        .block_on(async {
        // ── Checkpoint ───────────────────────────────────────
        // Every list run that can resume saves a checkpoint when it is
        // interrupted; --resume only decides whether one is read. (A run
        // had to pass --resume from the start to get one, so the first
        // Ctrl-C on a large bucket threw the progress away.)
        let checkpoint_path_opt = run_checkpoint_path(&cli, opt_bucket, opt_region, &opt_prefix);
        let resume_from = checkpoint_path_opt.as_deref().filter(|_| cli.resume);

        // Build the current run identity for checkpoint verification.
        let current_identity = checkpoint::CheckpointIdentity::new(
            opt_bucket,
            opt_region,
            &opt_prefix,
            Some(&cli.delimiter),
            cli.max_keys,
            cfg.s3.provider.as_deref(),
            Some(&cfg.s3.addressing_style.to_string()),
            Some(if mode == RunMode::BiDir {
                "bidir"
            } else {
                "list"
            }),
            cli.filter.as_deref(),
        )
        .with_endpoint(cfg.s3.endpoint_url.as_deref());

        let checkpoint_journal = resume_from
            .and_then(|p| checkpoint::CheckpointJournal::load_and_verify(p, &current_identity));

        // A checkpoint is resumed by listing exactly its ranges; hints and
        // startup discovery play no part.
        let resume_ranges: Option<Vec<checkpoint::ResumeRange>> = checkpoint_journal
            .as_ref()
            .and_then(|cj| cj.remaining.clone());
        // What this run skipped because a checkpoint records it listed —
        // computed below, once the checkpoint has survived verification.
        let mut resumed_segments_skipped: Option<usize> = None;

        let g_state = g_state.clone();
        let mut set = tokio::task::JoinSet::new();
        let concurrency = cfg.runtime.max_concurrency;
        let channel_capacity = cfg.channel.capacity;
        let sdk_config = core::S3TaskContext::load_sdk_config(&cfg.s3).await;

        // Without a region the SDK cannot sign a single request. Left to the
        // listing, that surfaced as a retryable client error on every segment
        // and the run spent its whole retry budget (minutes) before exiting as
        // a network failure — the wrong exit class for a setup mistake.
        let side_without_region =
            opt_region.is_none() || opt_target_region.is_some_and(|r| r.is_none());
        if side_without_region && sdk_config.region().is_none() {
            exit_before_run(agent::ExitCode::ProviderSetup, format!("No AWS region resolved{}: pass --region{} or set AWS_REGION \
                 (or a region in the AWS profile).",
                if opt_region.is_none() { "" } else { " for the diff target" },
                if opt_region.is_none() { "" } else { " / --target-region" },));
        }

        // List mode streams over one channel; diff builds per-segment
        // channels for each side further below.
        let (tx, rx) = if mode != RunMode::BiDir {
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(core::ObjectKey, core::ObjectProps)>>(
                channel_capacity,
            );
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // ── Proxy routing, before the first S3 request ───────
        // Startup discovery below already talks to the endpoint, so an
        // unexpected proxy must be named before it; a diff names both sides,
        // whose endpoints (and NO_PROXY matches) can differ.
        let sdk_region = sdk_config.region().map(|r| r.as_ref().to_string());
        let mut sides = vec![(
            "source",
            opt_bucket,
            opt_region.or(sdk_region.as_deref()),
            cfg.s3.endpoint_url.as_deref(),
        )];
        if let Some(target_bucket) = opt_target_bucket {
            sides.push((
                "target",
                target_bucket,
                opt_target_region
                    .flatten()
                    .or(sdk_region.as_deref()),
                diff_target_endpoint.as_deref(),
            ));
        }
        for (side, bucket, region, endpoint) in sides {
            if let Some(url) = agent::resolved_request_url(
                Some(bucket),
                region,
                endpoint,
                cfg.s3.force_path_style(),
            ) {
                match agent::env_proxy_for_url(&url) {
                    Some(proxy) => info!(
                        "  {} requests to {} go through proxy {} (from the proxy environment variables)",
                        side, url, proxy
                    ),
                    None => log::debug!("  {} requests to {} connect directly", side, url),
                }
            }
        }

        // ── Create trace writer ──────────────────────────────
        use s3_turbo_list::trace::S3TraceWriter;
        let trace_writer: Option<Arc<dyn S3TraceWriter>> =
            match trace::trace_writer_for_target(cfg.s3.trace_compat.as_deref()) {
                Ok(writer) => writer.map(Arc::from),
                // Through the standard failure epilogue: this exit used to
                // print nothing (the reason went only to the log file).
                Err(e) => exit_before_run(agent::ExitCode::OutputWrite, e),
            };

        // ── Load or generate KeySpace hints ─────────────────
        // --start-after is single-chain: hint segments would each override
        // their start with the CLI key and list overlapping ranges, so
        // startup discovery is skipped. (--hints-file plus --start-after is
        // rejected at CLI validation.)
        let ks_list: Vec<String> = if resume_ranges.is_some() {
            Vec::new()
        } else if cfg.s3.start_after.is_some() {
            info!(
                "--start-after is single-chain: listing as one segment"
            );
            Vec::new()
        } else {
            load_hints(cli.hints_file.as_deref())
        };
        // ── Startup structural discovery ─────────────────────
        // When no hints exist for a flat (delimiter='') list run, probe the
        // bucket's CommonPrefix structure once at startup so the first run
        // lists in parallel with no prior step. The
        // boundaries are persisted to the conventional hints cache, so
        // subsequent runs (including --resume) reload identical segments
        // through the existing cache path.
        let mut ks_list = ks_list;
        if ks_list.is_empty()
            && resume_ranges.is_none()
            && mode == RunMode::List
            && !cli.no_auto_hints
            && cli.hints_file.is_none()
            && cli.delimiter.is_empty()
            && cfg.s3.start_after.is_none()
        {
            info!("Probing bucket structure for startup key-space boundaries");
            let probe_client = core::build_s3_client(
                &sdk_config,
                opt_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style(),
            );
            let target_boundaries = concurrency.saturating_mul(2).clamp(16, 512);
            // Discovery can take many probe rounds on a slow endpoint; race it
            // against Ctrl-C / SIGTERM so an interrupt stops it at once (the
            // run then exits 7 without caching half-discovered boundaries).
            let discovered = tokio::select! {
                boundaries = async {
                    let discovery = auto_hints::discover_startup_boundaries(
                        &probe_client,
                        opt_bucket,
                        &opt_prefix,
                        target_boundaries,
                        cfg.s3.operation_timeout_secs,
                    )
                    .await;
                    // Flat namespace: no CommonPrefix structure, which previously
                    // meant starting as a single segment and relying on runtime
                    // splitting to ramp up (SPLIT_MIN_PAGES pages per generation).
                    // Bisect the key range with single-key probes instead — the same
                    // partitioner diff sides use — so the first run starts at full
                    // concurrency. The boundaries land in the same cache below.
                    if !discovery.boundaries.is_empty() {
                        discovery.boundaries
                    } else if discovery.is_single_page_listing(cli.max_keys) {
                        // The discovery probe's page was not truncated and held no
                        // CommonPrefixes, and this run's page size returns those keys
                        // in one request too: the whole listing is a single page.
                        // Bisecting it would cost several probes per cut to partition
                        // work the single segment finishes in one request, and would
                        // cache boundaries that pin that shape for later runs.
                        info!(
                            "Startup discovery found a single-page listing — using single-segment listing"
                        );
                        Vec::new()
                    } else {
                        info!("Startup discovery found no prefix structure — bisecting flat key space");
                        // Runtime splitting still covers this run, so one boundary per
                        // worker is enough; spare boundaries would only cost probes.
                        let flat_target = concurrency.clamp(1, 64);
                        discover_flat_boundaries_via_client(
                            &probe_client,
                            opt_bucket,
                            &opt_prefix,
                            flat_target,
                            cfg.s3.operation_timeout_secs,
                        )
                        .await
                    }
                } => Some(boundaries),
                _ = quit_requested(&g_state) => None,
            };
            let interrupted_discovery = discovered.is_none();
            let boundaries = discovered.unwrap_or_default();
            if interrupted_discovery {
                info!("Interrupted during startup discovery");
            } else if boundaries.is_empty() {
                info!(
                    "Startup discovery found no cuttable key space — using single-segment listing"
                );
            } else {
                info!(
                    "Startup discovery found {} key-space boundaries",
                    boundaries.len()
                );
                log::debug!("Startup boundaries: {}", boundaries.join("\t"));
                ks_list = boundaries;
            }
        }
        let ks_list = ks_list;
        // Ranges this run will not list because the checkpoint records them
        // listed. Captured here rather than re-read at manifest time: a run
        // that finishes removes its checkpoint, so the file on disk at the end
        // says nothing about whether this run resumed.
        if let (Some(cj), Some(ranges)) = (&checkpoint_journal, resume_ranges.as_deref()) {
            let listed = cj.listed_ranges.unwrap_or(0);
            info!(
                "Resuming checkpoint: {} key range(s) left to list",
                ranges.len()
            );
            resumed_segments_skipped = Some(listed);
            if listed > 0 {
                let warning = format!(
                    "Resuming from checkpoint: the key space earlier runs already wrote \
                     ({} range(s), in whole or in part) will not be listed again; this run \
                     lists the {} remaining range(s), so its output covers only the rest \
                     of the key space. Combine it with the output of the interrupted \
                     run(s); writing both to the same path leaves only this run's part.",
                    listed,
                    ranges.len()
                );
                // The runtime warnings were printed before the checkpoint was
                // read, so this one goes to stderr here or it never does.
                if !cli.agent {
                    print_runtime_warnings(std::slice::from_ref(&warning));
                }
                run_warnings.push(warning);
            }
        }

        let hints = match resume_ranges.as_deref() {
            Some(ranges) => core::KeySpaceHints::from_ranges(ranges),
            None => core::KeySpaceHints::new_from(&ks_list),
        };
        let hints_count = hints.total_count();

        info!("S3 Turbo List v{} starting:", env!("CARGO_PKG_VERSION"));
        info!(
            "  mode {:?}, threads {}, concurrency {}, channel cap {}",
            mode, cfg.runtime.worker_threads, concurrency, channel_capacity
        );
        info!(
            "  bucket {}, prefix '{}', {} key-space segments",
            opt_bucket, opt_prefix, hints_count
        );
        if let Some(ep) = &cfg.s3.endpoint_url {
            info!("  endpoint: {}", ep);
        }
        // ── Spawn list / diff side tasks ─────────────────────
        let is_diff = mode == RunMode::BiDir;
        // The list reactor's report of the key ranges it left unwritten.
        let mut resume_slot: Option<Arc<std::sync::Mutex<Option<checkpoint::ResumeProgress>>>> =
            None;
        let s3_cfg = cfg.s3.clone();
        let output_config = cfg.output.clone();
        let filename_ks_for_task = filename_ks.clone();
        let filename_output_for_task = filename_output.clone();

        if is_diff {
            drop(hints); // diff partitions each side independently below

            let target_region: Option<&str> =
                opt_target_region.and_then(|inner: Option<&str>| inner);
            let target_bucket: &str =
                opt_target_bucket.expect("target_bucket required for diff mode");

            // Per-side boundaries from cached hints or startup discovery —
            // the same automatic sources as list mode. Sides need not agree:
            // each side only has to be a complete ordered partition of its
            // own key space. The sides resolve concurrently — each can take
            // many network round-trips — except in the degenerate self-diff
            // case (same bucket and region), where both sides would race
            // writing the same hints-cache file.
            let (left_bounds, right_bounds) =
                if opt_bucket == target_bucket && opt_region == target_region {
                    let left = diff_side_boundaries(
                        opt_bucket,
                        opt_region,
                        cfg.s3.endpoint_url.as_deref(),
                        &opt_prefix,
                        &cfg,
                        &cli,
                        &sdk_config,
                        &g_state,
                    )
                    .await;
                    let right = diff_side_boundaries(
                        target_bucket,
                        target_region,
                        diff_target_endpoint.as_deref(),
                        &opt_prefix,
                        &cfg,
                        &cli,
                        &sdk_config,
                        &g_state,
                    )
                    .await;
                    (left, right)
                } else {
                    tokio::join!(
                        diff_side_boundaries(
                            opt_bucket,
                            opt_region,
                            cfg.s3.endpoint_url.as_deref(),
                            &opt_prefix,
                            &cfg,
                            &cli,
                            &sdk_config,
                            &g_state,
                        ),
                        diff_side_boundaries(
                            target_bucket,
                            target_region,
                            diff_target_endpoint.as_deref(),
                            &opt_prefix,
                            &cfg,
                            &cli,
                            &sdk_config,
                            &g_state,
                        ),
                    )
                };
            info!(
                "  diff segments: left {}, right {}",
                left_bounds.len() + 1,
                right_bounds.len() + 1
            );

            let (left_senders, left_receivers) = diff_segment_channels(left_bounds.len() + 1);
            let (right_senders, right_receivers) = diff_segment_channels(right_bounds.len() + 1);

            // Base contexts; each segment task swaps in its own sender.
            let (placeholder_tx, _) = tokio::sync::mpsc::channel(1);
            let left_ctx = core::S3TaskContext::new(core::TaskContextParams {
                bucket: opt_bucket,
                region: opt_region,
                endpoint: cfg.s3.endpoint_url.as_deref(),
                force_path_style: cfg.s3.force_path_style(),
                sdk_config: &sdk_config,
                s3_config: &s3_cfg,
                data_map_channel: placeholder_tx.clone(),
                dir: core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                g_state: g_state.clone(),
                trace_writer: trace_writer.clone(),
                addressing_style: &cfg.s3.addressing_style.to_string(),
                provider: cfg.s3.provider.as_deref(),
                delimiter: Some(&cli.delimiter),
                max_keys: cli.max_keys,
                start_after: cfg.s3.start_after.as_deref(),
            });
            let right_ctx = core::S3TaskContext::new(core::TaskContextParams {
                bucket: target_bucket,
                region: target_region,
                endpoint: diff_target_endpoint.as_deref(),
                force_path_style: cfg.s3.force_path_style(),
                sdk_config: &sdk_config,
                s3_config: &s3_cfg,
                data_map_channel: placeholder_tx,
                dir: core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                g_state: g_state.clone(),
                trace_writer: trace_writer.clone(),
                addressing_style: &cfg.s3.addressing_style.to_string(),
                provider: cfg.s3.provider.as_deref(),
                delimiter: Some(&cli.delimiter),
                max_keys: cli.max_keys,
                start_after: cfg.s3.start_after.as_deref(),
            });

            let (left_head_tx, left_head_rx) = tokio::sync::watch::channel(0usize);
            let (right_head_tx, right_head_rx) = tokio::sync::watch::channel(0usize);
            let prefix = opt_prefix.clone();
            set.spawn(async move {
                tasks_s3::diff_list_side_task(
                    &left_ctx,
                    &prefix,
                    concurrency,
                    &left_bounds,
                    left_senders,
                    Some(left_head_rx),
                )
                .await
            });
            let prefix = opt_prefix.clone();
            set.spawn(async move {
                tasks_s3::diff_list_side_task(
                    &right_ctx,
                    &prefix,
                    concurrency,
                    &right_bounds,
                    right_senders,
                    Some(right_head_rx),
                )
                .await
            });

            let sides = data_map::DiffStreamSides {
                left: left_receivers,
                right: right_receivers,
            };
            let heads = data_map::DiffMergeHeads {
                left: left_head_tx,
                right: right_head_tx,
            };
            let diff_g_state = g_state.clone();
            let diff_ks = filename_ks_for_task.clone();
            let diff_output = filename_output_for_task.clone();
            let diff_output_config = output_config.clone();
            set.spawn(async move {
                data_map::data_map_task_diff_streaming(
                    diff_g_state,
                    sides,
                    Some(heads),
                    &diff_ks,
                    &diff_output,
                    diff_output_config,
                )
                .await
            });
        } else {
            let prefix = opt_prefix.clone();
            let task_ctx = core::S3TaskContext::new(core::TaskContextParams {
                bucket: opt_bucket,
                region: opt_region,
                endpoint: cfg.s3.endpoint_url.as_deref(),
                force_path_style: cfg.s3.force_path_style(),
                sdk_config: &sdk_config,
                s3_config: &s3_cfg,
                data_map_channel: tx.expect("list mode allocates the streaming channel"),
                dir: core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE,
                g_state: g_state.clone(),
                trace_writer: trace_writer.clone(),
                addressing_style: &cfg.s3.addressing_style.to_string(),
                provider: cfg.s3.provider.as_deref(),
                delimiter: Some(&cli.delimiter),
                max_keys: cli.max_keys,
                start_after: cfg.s3.start_after.as_deref(),
            });
            resume_slot = Some(task_ctx.resume_progress.clone());
            set.spawn(async move {
                tasks_s3::flat_list_main_task(&task_ctx, &prefix, concurrency, hints).await
            });
        }

        // ── Spawn data map task (list modes) ─────────────────
        if is_diff {
            // spawned above alongside the side tasks
        } else if list_output_format == ListOutputFormat::Summary {
            let rx = rx.expect("list mode allocates the streaming channel");
            let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
            set.spawn(async move { data_map::data_map_task_list_summary_only(data_map_ctx).await });
        } else if list_output_format.writes_stdout_rows() {
            let rx = rx.expect("list mode allocates the streaming channel");
            let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
            let text_format = data_map::ListTextOutputFormat::from(list_output_format);
            set.spawn(async move {
                data_map::data_map_task_list_stdout(data_map_ctx, text_format).await
            });
        } else {
            let rx = rx.expect("list mode allocates the streaming channel");
            let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
            set.spawn(async move {
                data_map::data_map_task_list_streaming(
                    data_map_ctx,
                    &filename_ks_for_task,
                    &filename_output_for_task,
                    output_config,
                )
                .await
            });
        }

        // ── Spawn monitor task ──────────────────────────────
        let mon_ctx = core::MonContext::new(g_state.clone());
        set.spawn(async move { mon::mon_task(mon_ctx).await });

        // ── Diff mode lifecycle ───────────────────────────
        if mode == RunMode::BiDir {
            info!("Diff mode initialized — objects from both sides will be compared by data_map");
        }

        // The channel senders are moved into the list-task contexts, so each
        // channel closes when its side's task finishes.

        // Wait for all tasks.  There are deliberately no periodic checkpoint
        // saves: a segment counts as complete once its last batch is queued,
        // while its rows may still sit in the channel, a row-group buffer or
        // a BufWriter — and a Parquet file has no readable footer until it is
        // closed.  A mid-run save therefore recorded segments whose rows a
        // crash or a later write error would lose, and the next `--resume`
        // skipped them for good.  The checkpoint is written only on the
        // graceful-interrupt path below, after the outputs are finalized.
        while let Some(result) = set.join_next().await {
            if let Err(e) = result {
                if e.is_cancelled() {
                    // Cancellation is expected during shutdown (Ctrl-C aborts
                    // in-flight tasks); it is not a fatal error, so do not count
                    // it — counting it inflates the run manifest's fatal_errors
                    // on a clean interrupt and trips `manifest-summary --check`.
                    info!("Task was cancelled (abort or shutdown)");
                } else {
                    error!("Task panicked: {}", e);
                    if let Ok(panic_msg) = e.try_into_panic() {
                        let msg: String = panic_msg
                            .downcast_ref::<&str>()
                            .map(|s: &&str| s.to_string())
                            .or_else(|| panic_msg.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "<unknown panic>".to_string());
                        error!("Task panic message: {}", msg);
                    }
                    // A genuine panic is a fatal failure — record it.
                    g_state.inc_fatal_error();
                }
                g_state.quit();
            }
        }

        // ── Final checkpoint save / removal ────────────────
        // What the exit line should say about resuming; `None` for runs
        // that cannot resume (diff, --start-after).
        let mut checkpoint_note: Option<String> = None;
        if let Some(ref cp_path) = checkpoint_path_opt {
            let final_metrics = g_state.metrics_snapshot();
            let run_was_interrupted = interrupted.load(Ordering::SeqCst);
            // Another job's checkpoint that shares this file name (same
            // bucket, region and prefix; another endpoint, filter or page
            // size) is neither removed nor overwritten.
            let may_replace =
                checkpoint::CheckpointJournal::may_replace(cp_path, &current_identity);
            if final_metrics.fatal_errors > 0 || final_metrics.output_errors > 0 {
                info!(
                    "Skipping final checkpoint save because run failed before producing reliable output"
                );
            } else if !run_was_interrupted {
                // The run listed its whole key space, so there is no resume
                // point left. Saving one anyway was not merely redundant: the
                // next ordinary `--resume` invocation would skip the recorded
                // ranges and write an output covering only the remainder —
                // reported as success, because the manifest honestly
                // described its own short artifact.
                if !may_replace {
                    info!("Leaving checkpoint {} of another run in place", cp_path);
                } else {
                    match std::fs::remove_file(cp_path) {
                        Ok(()) => info!(
                            "Run completed the whole key space — removed checkpoint {}",
                            cp_path
                        ),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => warn!(
                            "Run completed but its checkpoint {} could not be removed ({}). \
                             A later --resume would skip the ranges it records; delete it \
                             before resuming.",
                            cp_path, e
                        ),
                    }
                }
            } else if !may_replace {
                let message = format!(
                    "Checkpoint {} belongs to another run (different endpoint, filter or \
                     page size) and was not overwritten; this run's progress was not saved, \
                     so a --resume would list it again from the start",
                    cp_path
                );
                warn!("{}", message);
                run_warnings.push(message.clone());
                checkpoint_note = Some(message);
            } else {
                let progress = resume_slot
                    .as_ref()
                    .and_then(|slot| slot.lock().unwrap().take());
                match progress {
                    // Interrupted after the last range was listed: nothing is
                    // left to resume. A checkpoint with no ranges would make
                    // the next --resume write an empty output and succeed.
                    Some(progress) if progress.remaining.is_empty() => {
                        let _ = std::fs::remove_file(cp_path);
                        checkpoint_note = Some(
                            "the listing had already finished; no checkpoint is needed"
                                .to_string(),
                        );
                    }
                    Some(progress) => {
                        let listed_before = checkpoint_journal
                            .as_ref()
                            .and_then(|cj| cj.listed_ranges)
                            .unwrap_or(0);
                        let remaining_count = progress.remaining.len();
                        let journal = checkpoint::CheckpointJournal {
                            bucket: opt_bucket.to_string(),
                            prefix: opt_prefix.clone(),
                            last_updated: chrono::Local::now().to_rfc3339(),
                            identity: Some(current_identity.clone()),
                            remaining: Some(progress.remaining),
                            listed_ranges: Some(listed_before + progress.ranges_with_progress),
                        };
                        match journal.save(cp_path) {
                            Ok(()) => {
                                info!(
                                    "Final checkpoint saved: {} key range(s) left to list",
                                    remaining_count
                                );
                                checkpoint_note = Some(format!(
                                    "checkpoint {} saved; rerun with --resume to list the {} \
                                     remaining range(s)",
                                    cp_path, remaining_count
                                ));
                            }
                            Err(e) => {
                                let message = format!(
                                    "{}; a --resume would list everything again",
                                    e
                                );
                                run_warnings.push(message.clone());
                                checkpoint_note = Some(message);
                            }
                        }
                    }
                    None => {
                        let message =
                            "no resume progress was recorded; a --resume would list everything \
                             again"
                                .to_string();
                        warn!("{}", message);
                        checkpoint_note = Some(message);
                    }
                }
            }
        }

        // ── Diff mode completion notice ────────────────────
        if mode == RunMode::BiDir {
            info!("diff mode comparison complete — data_map will finalize output");
        }

        info!("All tasks completed.");
        (resumed_segments_skipped, checkpoint_note)
    });

    rt.shutdown_background();

    let metrics = g_state.metrics_snapshot();
    // Parquet outputs this run wrote (base plus one per extra pooled writer);
    // read before the snapshot is folded into the manifest.
    let output_files = metrics.data_output_files;
    let interrupted = interrupted.load(Ordering::SeqCst);
    let first_fatal = g_state.first_fatal_error();
    let exit_code = if interrupted {
        agent::ExitCode::Interrupted
    } else if metrics.output_errors > 0 {
        agent::ExitCode::OutputWrite
    } else if first_fatal
        .as_ref()
        .is_some_and(|(errno, _)| s3_turbo_list::error::is_setup_error(*errno))
    {
        // Wrong bucket, credentials, or region: re-running unchanged cannot
        // succeed, so this must not read as a retryable network failure.
        agent::ExitCode::ProviderSetup
    } else if metrics.fatal_errors > 0 {
        agent::ExitCode::NetworkRetryExhausted
    } else {
        agent::ExitCode::Success
    };
    if let Some((_, summary)) = &first_fatal {
        run_warnings.push(format!("Listing failed: {}", summary));
    }
    let first_output_error = g_state.first_output_error();
    if let Some(message) = &first_output_error {
        run_warnings.push(format!("Output failed: {}", message));
    }
    let status = if exit_code == agent::ExitCode::Success {
        "success"
    } else if exit_code == agent::ExitCode::Interrupted {
        "interrupted"
    } else {
        "failed"
    };

    let manifest_outputs = runtime_output_summary(
        &cli,
        &cfg,
        list_writes_artifacts(&cli).then_some(filename_ks.as_str()),
        list_writes_artifacts(&cli).then_some(filename_output.as_str()),
    );
    // Artifact summaries re-read every output in full (SHA256, Parquet footer,
    // line counts) — minutes of tail latency on a multi-GB listing. Only pay
    // that when a manifest is actually emitted; the human "Wrote:" summary
    // needs just the paths.
    let manifest_emitted = cli.agent || cli.run_manifest.is_some();
    let artifacts = if manifest_emitted {
        agent::collect_artifacts(&manifest_outputs, output_files)
    } else {
        Vec::new()
    };
    let mut manifest_warnings = run_warnings.clone();
    // A Parquet artifact whose footer cannot be read is not a listing anyone
    // can consume; say so where agents look.
    for artifact in &artifacts {
        if artifact.kind == "parquet" && artifact.exists && artifact.parquet.is_none() {
            manifest_warnings.push(format!(
                "Parquet artifact '{}' has no readable footer; it is not a usable listing",
                artifact.path
            ));
        }
    }
    let mut manifest = agent::RunManifest {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: status.to_string(),
        exit_code: exit_code.code(),
        started_at: run_started_at.to_rfc3339(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        elapsed_secs: run_timer.elapsed().as_secs_f64(),
        command: agent::redacted_command_args(),
        cwd: std::env::current_dir()
            .map(|dir| dir.display().to_string())
            .unwrap_or_default(),
        inputs: command_input_summary(&cli, &cfg),
        artifacts,
        outputs: manifest_outputs,
        config_source,
        metrics: metrics.into(),
        checkpoint: agent::checkpoint_plan(
            cli.resume,
            run_checkpoint_path(&cli, opt_bucket, opt_region, &opt_prefix),
            cli.resume.then_some(
                &checkpoint::CheckpointIdentity::new(
                    opt_bucket,
                    opt_region,
                    &opt_prefix,
                    Some(&cli.delimiter),
                    cli.max_keys,
                    cfg.s3.provider.as_deref(),
                    Some(&cfg.s3.addressing_style.to_string()),
                    Some(if mode == RunMode::BiDir {
                        "bidir"
                    } else {
                        "list"
                    }),
                    cli.filter.as_deref(),
                )
                .with_endpoint(cfg.s3.endpoint_url.as_deref()),
            ),
            resumed_segments_skipped,
        ),
        warnings: manifest_warnings,
    };

    // A manifest that cannot be written fails the run (exit 5) — but the
    // listing is done: --agent still gets the manifest on stdout, marked
    // failed, and the exit line says what is missing. It used to take the
    // pre-run path: "Nothing was listed", and no manifest at all.
    if let Some(path) = cli.run_manifest.as_deref()
        && let Err(e) = agent::write_json_file(path, &manifest)
    {
        manifest
            .warnings
            .push(format!("manifest write error: {}", e));
        if exit_code == agent::ExitCode::Success {
            let code = agent::ExitCode::OutputWrite;
            manifest.status = "failed".to_string();
            manifest.exit_code = code.code();
            if cli.agent {
                println!("{}", agent::to_pretty_json(&manifest));
            }
            eprintln!(
                "s3-turbo-list: run failed (exit {}): manifest write error: {}. The listing \
                 completed and its outputs are complete; only the manifest is missing.",
                code.code(),
                e
            );
            std::process::exit(code.code());
        }
    }
    if cli.agent {
        println!("{}", agent::to_pretty_json(&manifest));
    } else if exit_code == agent::ExitCode::Success
        && list_output_format == ListOutputFormat::Summary
    {
        print_summary(&manifest.metrics, &cli.delimiter);
    } else if exit_code == agent::ExitCode::Success && list_output_format.writes_stdout_rows() {
        // stdout is reserved for TSV/NDJSON rows.
    } else if exit_code == agent::ExitCode::Success {
        print_wrote_summary(&manifest.outputs, output_files);
    }
    if exit_code != agent::ExitCode::Success {
        // One line on stderr for every non-success exit: without it a failed
        // run could end with empty stdout and stderr (the reason only in the
        // log file) while leaving partial artifacts behind.
        let reason = match (&first_fatal, exit_code) {
            (_, agent::ExitCode::Interrupted) => match &checkpoint_note {
                Some(note) => format!("interrupted; {}", note),
                None => "interrupted".to_string(),
            },
            (_, agent::ExitCode::OutputWrite) => first_output_error
                .clone()
                .map(|message| format!("output failed: {}", message))
                .unwrap_or_else(|| "an output write failed".to_string()),
            (Some((_, summary)), _) => summary.clone(),
            (None, _) => "a listing segment failed".to_string(),
        };
        eprintln!(
            "s3-turbo-list: run {} (exit {}): {}. Any outputs written are partial.",
            status,
            exit_code.code(),
            reason
        );
        std::process::exit(exit_code.code());
    }
}

/// Top-level hints loader: resolves hints from explicit file, the conventional
/// startup-discovery cache, or falls back to empty (single-segment).
///
/// Priority:
/// 1. `hints_file` (from `--hints-file` CLI flag) — always used first.
/// 2. Auto-hints cache at `{region}_{bucket}_hints.toml` in CWD.
/// 3. Single-segment fallback (empty vec).
pub(crate) type SegmentBatchSender =
    tokio::sync::mpsc::Sender<Vec<(core::ObjectKey, core::ObjectProps)>>;

pub(crate) type SegmentBatchReceiver =
    tokio::sync::mpsc::Receiver<Vec<(core::ObjectKey, core::ObjectProps)>>;

/// One small channel per diff segment; the capacity is the per-segment
/// prefetch window, keeping memory bounded while segments list in parallel.
pub(crate) fn diff_segment_channels(
    segments: usize,
) -> (Vec<SegmentBatchSender>, Vec<SegmentBatchReceiver>) {
    (0..segments)
        .map(|_| tokio::sync::mpsc::channel(tasks_s3::DIFF_SEGMENT_CHANNEL_CAP))
        .unzip()
}

/// Resolves once the run has been asked to stop (Ctrl-C / SIGTERM set the
/// quit flag). Startup discovery races against it: its probe rounds run
/// before any segment task exists, so nothing else would notice the signal.
pub(crate) async fn quit_requested(g_state: &core::GlobalState) {
    while !g_state.is_quit() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Key-space boundaries for one diff side from startup discovery, as in
/// list mode (diff takes no --hints-file). Empty means one segment.
pub(crate) async fn diff_side_boundaries(
    bucket: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    prefix: &str,
    cfg: &S3TurboConfig,
    cli: &Cli,
    sdk_config: &aws_config::SdkConfig,
    g_state: &core::GlobalState,
) -> Vec<String> {
    if cli.no_auto_hints || !cli.delimiter.is_empty() || cfg.s3.start_after.is_some() {
        return Vec::new();
    }

    let client = core::build_s3_client(sdk_config, region, endpoint, cfg.s3.force_path_style());
    let target = cfg.runtime.max_concurrency.saturating_mul(2).clamp(16, 512);
    // Race discovery against Ctrl-C / SIGTERM, as list mode does.
    let discovered = tokio::select! {
        boundaries = async {
            let discovery = auto_hints::discover_startup_boundaries(
                &client,
                bucket,
                prefix,
                target,
                cfg.s3.operation_timeout_secs,
            )
            .await;
        if !discovery.boundaries.is_empty() {
                discovery.boundaries
            } else if discovery.is_single_page_listing(cli.max_keys) {
                // Single-page side: nothing to partition, and the probes would cost
                // more requests than the listing.
                Vec::new()
            } else {
                // Flat namespace: structural discovery found no CommonPrefixes, so the
                // side would otherwise list as one serial segment. Bisect the key range
                // with single-key probes so it lists in parallel. The target is smaller
                // than structural discovery's: each cut is a one-time up-front probe,
                // and only `max_concurrency` segments run at once, so spare boundaries
                // beyond that would just cost probes without adding parallelism. Diff
                // has no runtime splitting to fall back on, so it keeps a floor.
                let flat_target = cfg.runtime.max_concurrency.clamp(8, 64);
                discover_flat_boundaries_via_client(
                    &client,
                    bucket,
                    prefix,
                    flat_target,
                    cfg.s3.operation_timeout_secs,
                )
                .await
            }
        } => Some(boundaries),
        _ = quit_requested(g_state) => None,
    };
    discovered.unwrap_or_default()
}

/// Bisect a flat key range into boundaries via single-key ListObjectsV2
/// probes on the given client. Shared by list-mode startup discovery and the
/// per-side diff hints resolver.
pub(crate) async fn discover_flat_boundaries_via_client(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    target: usize,
    timeout_secs: u64,
) -> Vec<String> {
    let probe_bucket = bucket.to_string();
    let probe_prefix = prefix.to_string();
    auto_hints::discover_flat_boundaries(prefix, target, |start_after| {
        let client = client.clone();
        let bucket = probe_bucket.clone();
        let prefix = probe_prefix.clone();
        async move {
            let mut req = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .max_keys(1);
            if let Some(sa) = start_after {
                req = req.start_after(sa);
            }
            // Same watchdog the runtime split probes use: startup runs before
            // a single object is listed, so a stalled endpoint must not hang
            // the run there.
            let resp = match tokio::time::timeout(
                std::time::Duration::from_secs(timeout_secs),
                req.send(),
            )
            .await
            {
                Ok(result) => result.map_err(|e| s3_turbo_list::error::concise_sdk_error(&e))?,
                Err(_) => return Err("probe timed out".to_string()),
            };
            Ok(resp
                .contents()
                .first()
                .and_then(|o| o.key())
                .map(str::to_string))
        }
    })
    .await
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

// ── Compat-probe ───────────────────────────────────────────
