//! The end of a list or diff run: exit code, run manifest, and what goes to
//! stdout and stderr.

use super::*;

/// Everything the end of a run reports on.
pub(crate) struct RunEnd<'a> {
    pub(crate) spec: &'a RunSpec<'a>,
    pub(crate) config_source: agent::ConfigSourceSummary,
    pub(crate) g_state: &'a core::GlobalState,
    /// Ctrl-C / SIGTERM arrived during the run.
    pub(crate) interrupted: bool,
    pub(crate) outcome: ListingOutcome,
    pub(crate) run_warnings: Vec<String>,
    pub(crate) run_started_at: chrono::DateTime<chrono::Utc>,
    pub(crate) run_timer: Instant,
}

/// Compute the exit code, build and emit the manifest and summaries, and
/// exit non-zero with the run line when the run did not succeed.
pub(crate) fn finish_run(end: RunEnd<'_>) {
    let RunEnd {
        spec,
        config_source,
        g_state,
        interrupted,
        outcome,
        mut run_warnings,
        run_started_at,
        run_timer,
    } = end;
    let cli = spec.cli;
    let metrics = g_state.metrics_snapshot();
    // Parquet outputs this run wrote (base plus one per extra pooled writer);
    // read before the snapshot is folded into the manifest.
    let output_files = metrics.data_output_files;
    let interrupted = interrupted && !outcome.listing_finished;
    let first_fatal = g_state.first_fatal_error();
    let exit_code = run_exit_code(interrupted, &metrics, first_fatal.as_ref());
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

    let mut manifest = build_manifest(ManifestInputs {
        spec,
        config_source,
        metrics,
        output_files,
        status,
        exit_code,
        run_warnings,
        run_started_at,
        run_timer,
        resumed_segments_skipped: outcome.resumed_segments_skipped,
    });

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
    let list_output_format = spec.list_output_format;
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
            (_, agent::ExitCode::Interrupted) => match &outcome.checkpoint_note {
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

/// The run's exit code: an interrupt first, then an output failure, then a
/// setup error (wrong bucket, credentials or region), then any other fatal
/// listing error.
fn run_exit_code(
    interrupted: bool,
    metrics: &core::RunMetricsSnapshot,
    first_fatal: Option<&(u8, String)>,
) -> agent::ExitCode {
    if interrupted {
        agent::ExitCode::Interrupted
    } else if metrics.output_errors > 0 {
        agent::ExitCode::OutputWrite
    } else if first_fatal.is_some_and(|(errno, _)| s3_turbo_list::error::is_setup_error(*errno)) {
        // Wrong bucket, credentials, or region: re-running unchanged cannot
        // succeed, so this must not read as a retryable network failure.
        agent::ExitCode::ProviderSetup
    } else if metrics.fatal_errors > 0 {
        agent::ExitCode::NetworkRetryExhausted
    } else {
        agent::ExitCode::Success
    }
}

/// What the run manifest is built from.
struct ManifestInputs<'a> {
    spec: &'a RunSpec<'a>,
    config_source: agent::ConfigSourceSummary,
    metrics: core::RunMetricsSnapshot,
    output_files: usize,
    status: &'a str,
    exit_code: agent::ExitCode,
    run_warnings: Vec<String>,
    run_started_at: chrono::DateTime<chrono::Utc>,
    run_timer: Instant,
    resumed_segments_skipped: Option<usize>,
}

fn build_manifest(inputs: ManifestInputs<'_>) -> agent::RunManifest {
    let ManifestInputs {
        spec,
        config_source,
        metrics,
        output_files,
        status,
        exit_code,
        run_warnings,
        run_started_at,
        run_timer,
        resumed_segments_skipped,
    } = inputs;
    let RunSpec {
        cli, cfg, target, ..
    } = *spec;
    let manifest_outputs = runtime_output_summary(
        cli,
        cfg,
        list_writes_artifacts(cli).then_some(spec.filename_ks),
        list_writes_artifacts(cli).then_some(spec.filename_output),
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
    agent::RunManifest {
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
        inputs: command_input_summary(cli, cfg),
        artifacts,
        outputs: manifest_outputs,
        config_source,
        metrics: metrics.into(),
        checkpoint: agent::checkpoint_plan(
            cli.resume,
            run_checkpoint_path(cli, target.bucket, target.region, &target.prefix),
            cli.resume.then_some(&run_identity(cli, cfg, target)),
            resumed_segments_skipped,
        ),
        warnings: manifest_warnings,
    }
}
