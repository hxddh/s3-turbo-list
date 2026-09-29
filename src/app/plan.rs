//! The dry-run plan, the command summary, and the pre-run guardrails.

use super::*;

pub(crate) fn validate_provider_setup_or_exit(cli: &Cli, cfg: &S3TurboConfig) {
    let warnings = provider_setup_guardrail_warnings(cli, cfg);
    if let Some(error) = warnings.first() {
        exit_before_run(
            agent::ExitCode::ProviderSetup,
            format!("Provider setup error: {}", error),
        );
    }
}

/// Whether a region resolves without the network: `AWS_REGION`,
/// `AWS_DEFAULT_REGION`, or the AWS config file's profile. (The SDK's full
/// chain also asks EC2 instance metadata, which a preflight cannot.)
pub(crate) fn offline_region_resolves() -> bool {
    let env_set = |name: &str| std::env::var(name).is_ok_and(|v| !v.trim().is_empty());
    if env_set("AWS_REGION") || env_set("AWS_DEFAULT_REGION") {
        return true;
    }
    use aws_config::meta::region::ProvideRegion;
    tokio::runtime::Builder::new_current_thread()
        .build()
        .ok()
        .and_then(|rt| rt.block_on(aws_config::profile::ProfileFileRegionProvider::new().region()))
        .is_some()
}

pub(crate) fn imds_disabled() -> bool {
    std::env::var("AWS_EC2_METADATA_DISABLED").is_ok_and(|v| v.eq_ignore_ascii_case("true"))
}

/// The sides of a list/diff command that name no region of their own.
pub(crate) fn sides_without_region(cli: &Cli) -> Vec<&'static str> {
    match &cli.cmd {
        Commands::List { region: None, .. } => vec!["--region"],
        Commands::Diff {
            region,
            target_region,
            ..
        } => {
            let mut sides = Vec::new();
            if region.is_none() {
                sides.push("--region");
            }
            if target_region.is_none() {
                sides.push("--target-region");
            }
            sides
        }
        _ => Vec::new(),
    }
}

pub(crate) fn provider_setup_guardrail_warnings(cli: &Cli, cfg: &S3TurboConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    match &cli.cmd {
        Commands::List { .. } | Commands::Diff { .. } => {
            warnings.extend(profiles::endpoint_profile_guardrail_warnings(cfg));
            // With instance metadata disabled the SDK has no other region
            // source, so this is exactly the run's own "No AWS region
            // resolved" failure, reported before any work.
            let missing = sides_without_region(cli);
            if !missing.is_empty() && imds_disabled() && !offline_region_resolves() {
                warnings.push(format!(
                    "no AWS region resolved for {}: pass it, or set AWS_REGION (or a region in \
                     the AWS config profile)",
                    missing.join(" / ")
                ));
            }
        }
        Commands::CompatProbe { .. } => {
            if cfg.s3.endpoint_url.is_none() {
                warnings.push(
                    "compat-probe needs an endpoint: pass --endpoint-url or --provider, or set \
                     s3.endpoint_url in the config"
                        .to_string(),
                );
            }
            if let Some(endpoint) = cfg.s3.endpoint_url.as_deref()
                && profiles::endpoint_url_has_template_placeholder(endpoint)
            {
                warnings.push(format!(
                        "endpoint URL '{}' still contains template placeholders; replace values such as <account-id> or <region> before a real run",
                        endpoint
                    ));
            }
        }
        _ => {}
    }
    warnings
}

pub(crate) fn build_plan_report(
    cli: &Cli,
    cfg: &S3TurboConfig,
    config_source: agent::ConfigSourceSummary,
    diff_target_endpoint: Option<&str>,
) -> agent::PlanReport {
    let (planned_ks, planned_parquet, _) = planned_output_paths(cli, cfg);
    let outputs =
        runtime_output_summary(cli, cfg, planned_ks.as_deref(), planned_parquet.as_deref());
    let inputs = command_input_summary(cli, cfg);
    let checkpoint_path = inputs.bucket.as_deref().and_then(|bucket| {
        run_checkpoint_path(cli, bucket, inputs.region.as_deref(), &inputs.prefix)
    });
    let current_identity = inputs.bucket.as_deref().map(|bucket| {
        checkpoint::CheckpointIdentity::new(
            bucket,
            inputs.region.as_deref(),
            &inputs.prefix,
            Some(&inputs.delimiter),
            inputs.max_keys,
            inputs.profile.as_deref(),
            Some(&inputs.addressing_style),
            Some(if inputs.mode == "diff" {
                "bidir"
            } else {
                "list"
            }),
            inputs.filter.as_deref(),
        )
        .with_endpoint(cfg.s3.endpoint_url.as_deref())
    });
    let hints = if inputs.mode == "diff" {
        // Mirror diff_side_boundaries: these options leave each side as one
        // serial segment (diff never splits at runtime).
        let single = if cfg.s3.start_after.is_some() {
            Some("single_chain")
        } else if !cli.delimiter.is_empty() {
            Some("delimiter_single_segment")
        } else if cli.no_auto_hints {
            Some("disabled_single_segment_fallback")
        } else {
            None
        };
        match single {
            Some(source) => agent::HintsPlan {
                source: source.to_string(),
                path: None,
                exists: false,
                valid: None,
                format: None,
                boundary_count: None,
                warnings: vec![
                    "diff lists each side as one serial segment with these options; diff \
                     does not split segments at runtime"
                        .to_string(),
                ],
            },
            None => agent::diff_per_side_hints_plan(),
        }
    } else {
        agent::detect_hints_plan(agent::HintsPlanInputs {
            explicit_hints_file: cli.hints_file.as_deref(),
            listing: inputs.bucket.is_some(),
            no_auto_hints: cli.no_auto_hints,
            single_chain: cfg.s3.start_after.is_some() || cli.continuation_token.is_some(),
            delimited: !cli.delimiter.is_empty(),
        })
    };
    let file_conflicts = agent::output_conflicts(&outputs);
    let mut warnings = config_source.warnings.clone();
    let missing_region = sides_without_region(cli);
    if !missing_region.is_empty() && !imds_disabled() && !offline_region_resolves() {
        warnings.push(format!(
            "no region for {} from the command line, AWS_REGION or the AWS config profile; \
             the run fails with exit 3 unless EC2 instance metadata supplies one",
            missing_region.join(" / ")
        ));
    }
    if let (Commands::List { .. }, Some(parquet)) = (&cli.cmd, outputs.parquet_file.as_deref()) {
        let stale = stale_parquet_parts(parquet);
        if !stale.is_empty() {
            warnings.push(format!(
                "{} Parquet part file(s) from an earlier run of '{}' exist (e.g. '{}'); \
                 the run will remove them",
                stale.len(),
                parquet,
                stale[0]
            ));
        }
    }
    warnings.extend(runtime_guardrail_warnings(cli, cfg));
    if matches!(cli.cmd, Commands::CompatProbe { .. }) {
        warnings.push(
            "compat-probe will contact the configured endpoint when not run with --dry-run"
                .to_string(),
        );
    }
    if let Some(target_endpoint) =
        diff_target_endpoint.filter(|target| Some(*target) != cfg.s3.endpoint_url.as_deref())
    {
        warnings.push(format!(
            "diff target side lists against {} (the profile's endpoint for --target-region); \
             the source side uses resolved_config.s3.endpoint_url",
            target_endpoint
        ));
    }
    // Only runs that can never fan out get this warning. A run without cached
    // hints still partitions (startup discovery, then runtime splitting), and
    // so does --no-auto-hints (runtime splitting alone).
    let never_fans_out = matches!(
        hints.source.as_str(),
        "single_chain" | "delimiter_single_segment"
    ) || (inputs.mode == "diff"
        && hints.source == "disabled_single_segment_fallback");
    if never_fans_out {
        warnings.push(
            "list is planned as a single ListObjectsV2 chain; --concurrency does not add parallelism to it"
                .to_string(),
        );
    }
    if cli.delimiter.is_empty() && matches!(cli.cmd, Commands::List { .. }) {
        warnings.push(
            "--delimiter '' means recursive listing and is omitted from ListObjectsV2 requests for S3-compatible provider compatibility"
                .to_string(),
        );
    }
    if cli.summary_only {
        warnings.push(
            "summary-only will scan S3 ListObjectsV2 pages when not run with --dry-run, but it will not write Parquet or KeySpace outputs"
                .to_string(),
        );
    }
    if cli.continuation_token.is_some() {
        warnings.push(
            "continuation-token resumes one sequential ListObjectsV2 chain; hints and checkpoint resume are intentionally not combined with it"
                .to_string(),
        );
    }

    let output_problems = planned_output_problems(&outputs, cli);
    for path in [
        outputs.parquet_file.as_deref(),
        outputs.ks_file.as_deref(),
        outputs.log_file.as_deref(),
        outputs.trace_compat.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if std::path::Path::new(path).is_file() {
            warnings.push(format!("output '{}' exists and will be overwritten", path));
        }
    }
    let output_blocked = !output_problems.is_empty();
    warnings.extend(output_problems);

    agent::PlanReport {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        // `blocked`: a problem that stops the real run — a provider setup
        // problem (exit 3) or an output it cannot create (exit 5); the reason
        // is in `warnings` and the dry run exits with the same code.
        status: if provider_setup_guardrail_warnings(cli, cfg).is_empty() && !output_blocked {
            "ok"
        } else {
            "blocked"
        }
        .to_string(),
        command: agent::redacted_command_args(),
        network: "none: dry-run only resolves local configuration and planned paths".to_string(),
        inputs,
        outputs,
        config_source,
        resolved_config: cfg.into(),
        hints,
        // A dry run has not loaded a checkpoint, so nothing was skipped yet.
        checkpoint: agent::checkpoint_plan(
            cli.resume,
            checkpoint_path,
            current_identity.as_ref().filter(|_| cli.resume),
            None,
        ),
        file_conflicts,
        warnings,
    }
}

pub(crate) fn runtime_guardrail_warnings(cli: &Cli, cfg: &S3TurboConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    // Only the exact default "/" means the whole bucket. A path-style
    // "/logs/" is a literal prefix that S3 keys (which rarely start with a
    // slash) do not match, so the run listed nothing and exited 0.
    if cli.prefix != "/"
        && cli.prefix.starts_with('/')
        && matches!(cli.cmd, Commands::List { .. } | Commands::Diff { .. })
    {
        warnings.push(format!(
            "--prefix '{}' starts with '/', and S3 keys rarely do; this lists only keys that \
             literally begin with '/'. Did you mean '{}'?",
            cli.prefix,
            cli.prefix.trim_start_matches('/')
        ));
    }
    if matches!(
        cli.cmd,
        Commands::List { .. } | Commands::Diff { .. } | Commands::CompatProbe { .. }
    ) {
        warnings.extend(provider_setup_guardrail_warnings(cli, cfg));
        if let Some(endpoint) = cfg.s3.endpoint_url.as_deref()
            && !endpoint_url_looks_usable(endpoint)
        {
            warnings.push(format!(
                    "--endpoint-url '{}' has no scheme and host; the run will fail when it dispatches its first request",
                    endpoint
                ));
        }
    }
    if cli.summary_only
        && (cli.output_dir.is_some()
            || cli.output_parquet_file.is_some()
            || cli.output_ks_file.is_some())
    {
        warnings.push(
            "--summary-only does not write Parquet or KeySpace outputs; output path flags are ignored"
                .to_string(),
        );
    }
    if list_output_format(cli)
        .map(ListOutputFormat::writes_stdout_rows)
        .unwrap_or(false)
    {
        warnings.push(
            "--output-format tsv/ndjson streams list rows to stdout and does not write Parquet or KeySpace outputs"
                .to_string(),
        );
        if cli.output_dir.is_some()
            || cli.output_parquet_file.is_some()
            || cli.output_ks_file.is_some()
        {
            warnings.push(
                "output path flags are ignored when --output-format is tsv or ndjson".to_string(),
            );
        }
    }
    if matches!(cli.cmd, Commands::Diff { .. }) {
        warnings.push(
            "diff partitions each side automatically; explicit --hints-file and --resume are intentionally unsupported for diff"
                .to_string(),
        );
    }
    warnings
}

pub(crate) fn print_runtime_warnings(warnings: &[String]) {
    for warning in warnings {
        eprintln!("WARN {}", warning);
    }
}

pub(crate) fn command_input_summary(cli: &Cli, cfg: &S3TurboConfig) -> agent::CommandInputSummary {
    let prefix = if cli.prefix == "/" {
        String::new()
    } else {
        cli.prefix.clone()
    };
    let (mode, bucket, region, target_bucket, target_region, output_format) = match &cli.cmd {
        Commands::List {
            region,
            bucket,
            output_format,
            ..
        } => (
            "list".to_string(),
            Some(bucket.clone()),
            region.clone(),
            None,
            None,
            Some(output_format.as_str().to_string()),
        ),
        Commands::Diff {
            region,
            bucket,
            target_region,
            target_bucket,
            ..
        } => (
            "diff".to_string(),
            Some(bucket.clone()),
            region.clone(),
            Some(target_bucket.clone()),
            target_region.clone(),
            None,
        ),
        Commands::CompatProbe { region, bucket, .. } => (
            "compat-probe".to_string(),
            Some(bucket.clone()),
            region.clone(),
            None,
            None,
            None,
        ),
        Commands::ManifestSummary { .. } => {
            ("manifest-summary".to_string(), None, None, None, None, None)
        }
        Commands::InitConfig { .. } => ("init-config".to_string(), None, None, None, None, None),
        Commands::Guide { .. } => ("guide".to_string(), None, None, None, None, None),
        Commands::Doctor { .. } => ("doctor".to_string(), None, None, None, None, None),
        Commands::Completions { .. } => ("completions".to_string(), None, None, None, None, None),
        Commands::Man => ("man".to_string(), None, None, None, None, None),
    };

    agent::CommandInputSummary {
        mode,
        bucket,
        region,
        target_bucket,
        target_region,
        output_format,
        prefix,
        delimiter: cli.delimiter.clone(),
        max_keys: cli.max_keys,
        start_after: cfg.s3.start_after.clone(),
        // Redacted like the command line it came from.
        continuation_token: cli
            .continuation_token
            .as_ref()
            .map(|_| "<redacted>".to_string()),
        profile: cfg.s3.profile.clone(),
        addressing_style: cfg.s3.addressing_style.to_string(),
        filter: cli.filter.clone(),
    }
}

// The region supplied by the active subcommand, used for profile endpoint
// templating before the full command dispatch.
/// The endpoint the diff target side lists against. A region-templated
/// profile's preset was applied from the source region; when the endpoint
/// came from that template (not from the user) and the target has its own
/// region, the target gets its own region's host instead of the source's.
pub(crate) fn diff_target_endpoint(
    cli: &Cli,
    cfg: &S3TurboConfig,
    endpoint_was_explicit: bool,
) -> Option<String> {
    if let Commands::Diff {
        target_region: Some(target_region),
        ..
    } = &cli.cmd
        && !endpoint_was_explicit
        && let Some(endpoint) = cfg
            .s3
            .profile
            .as_deref()
            .and_then(|profile| profiles::region_endpoint(profile, target_region))
    {
        return Some(endpoint);
    }
    cfg.s3.endpoint_url.clone()
}
