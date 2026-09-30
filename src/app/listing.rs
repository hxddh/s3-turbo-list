//! The listing itself, inside the runtime: checkpoint, startup
//! partitioning, the list / diff / data-map / monitor tasks, and the final
//! checkpoint save.

use super::*;

/// What the listing learned about resuming. The manifest is built after it,
/// and a completed run has already removed its checkpoint, so the file on
/// disk can no longer answer this.
pub(crate) struct ListingOutcome {
    /// Ranges this run skipped because a checkpoint records them listed.
    pub(crate) resumed_segments_skipped: Option<usize>,
    /// What the exit line should say about resuming; `None` for runs that
    /// cannot resume (diff, --start-after).
    pub(crate) checkpoint_note: Option<String>,
    /// Ctrl-C arrived after the last range was listed, so the outputs are
    /// complete and the run reports success.
    pub(crate) listing_finished: bool,
}

/// The list reactor's report of the key ranges it left unwritten.
type ResumeSlot = Arc<std::sync::Mutex<Option<checkpoint::ResumeProgress>>>;

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

/// The checkpoint this run reads and writes.
struct RunCheckpoint {
    /// `None` for runs that cannot resume (diff, --start-after).
    path: Option<String>,
    /// The current run identity for checkpoint verification.
    identity: checkpoint::CheckpointIdentity,
    /// The checkpoint read by --resume, when it survived verification.
    journal: Option<checkpoint::CheckpointJournal>,
    /// A checkpoint is resumed by listing exactly its ranges; hints and
    /// startup discovery play no part.
    resume_ranges: Option<Vec<checkpoint::ResumeRange>>,
}

/// Run the listing to the end (or the interrupt) and save or remove the
/// checkpoint.
pub(crate) async fn execute_listing(
    spec: &RunSpec<'_>,
    g_state: &core::GlobalState,
    interrupted: &AtomicBool,
    run_warnings: &mut Vec<String>,
) -> ListingOutcome {
    let RunSpec {
        cli, cfg, target, ..
    } = *spec;
    let checkpoint = open_checkpoint(spec);

    let mut set = tokio::task::JoinSet::new();
    let concurrency = cfg.runtime.max_concurrency;
    let channel_capacity = cfg.channel.capacity;
    let sdk_config = core::S3TaskContext::load_sdk_config(&cfg.s3).await;
    require_signing_region(target, &sdk_config);

    // List mode streams over one channel; diff builds per-segment
    // channels for each side further below.
    let (tx, rx) = if target.mode != RunMode::BiDir {
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(core::ObjectKey, core::ObjectProps)>>(
            channel_capacity,
        );
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    log_proxy_routing(spec, &sdk_config);
    let trace_writer = open_trace_writer(cfg);
    let ks_list = initial_boundaries(
        spec,
        checkpoint.resume_ranges.is_some(),
        &sdk_config,
        g_state,
    )
    .await;
    let resumed_segments_skipped = note_resume(cli, &checkpoint, run_warnings);

    let hints = match checkpoint.resume_ranges.as_deref() {
        Some(ranges) => core::KeySpaceHints::from_ranges(ranges),
        None => core::KeySpaceHints::new_from(&ks_list),
    };
    let hints_count = hints.total_count();

    info!("S3 Turbo List v{} starting:", env!("CARGO_PKG_VERSION"));
    info!(
        "  mode {:?}, threads {}, concurrency {}, channel cap {}",
        target.mode, cfg.runtime.worker_threads, concurrency, channel_capacity
    );
    info!(
        "  bucket {}, prefix '{}', {} key-space segments",
        target.bucket, target.prefix, hints_count
    );
    if let Some(ep) = &cfg.s3.endpoint_url {
        info!("  endpoint: {}", ep);
    }
    // ── Spawn list / diff side tasks ─────────────────────
    let is_diff = target.mode == RunMode::BiDir;
    let contexts = SideContexts {
        spec,
        sdk_config: &sdk_config,
        trace_writer,
        g_state,
        addressing_style: cfg.s3.addressing_style.to_string(),
    };
    let resume_slot = if is_diff {
        drop(hints); // diff partitions each side independently below
        spawn_diff(&mut set, &contexts, concurrency).await;
        None
    } else {
        let tx = tx.expect("list mode allocates the streaming channel");
        let rx = rx.expect("list mode allocates the streaming channel");
        Some(spawn_list(&mut set, &contexts, tx, rx, hints, concurrency))
    };

    // ── Spawn monitor task ──────────────────────────────
    let mon_ctx = core::MonContext::new(g_state.clone());
    set.spawn(async move { mon::mon_task(mon_ctx).await });

    // ── Diff mode lifecycle ───────────────────────────
    if target.mode == RunMode::BiDir {
        info!("Diff mode initialized — objects from both sides will be compared by data_map");
    }

    // The channel senders are moved into the list-task contexts, so each
    // channel closes when its side's task finishes.
    join_all(&mut set, g_state).await;

    let (checkpoint_note, listing_finished) = finalize_checkpoint(
        spec,
        &checkpoint,
        resume_slot.as_ref(),
        g_state,
        interrupted,
        run_warnings,
    );

    // ── Diff mode completion notice ────────────────────
    if target.mode == RunMode::BiDir {
        info!("diff mode comparison complete — data_map will finalize output");
    }

    info!("All tasks completed.");
    ListingOutcome {
        resumed_segments_skipped,
        checkpoint_note,
        listing_finished,
    }
}

/// The run's checkpoint. Every list run that can resume saves a checkpoint when it is
/// interrupted; --resume only decides whether one is read. (A run
/// had to pass --resume from the start to get one, so the first
/// Ctrl-C on a large bucket threw the progress away.)
fn open_checkpoint(spec: &RunSpec<'_>) -> RunCheckpoint {
    let RunSpec {
        cli, cfg, target, ..
    } = *spec;
    let path = run_checkpoint_path(cli, target.bucket, target.region, &target.prefix);
    let resume_from = path.as_deref().filter(|_| cli.resume);

    // Build the current run identity for checkpoint verification.
    let identity = run_identity(cli, cfg, target);

    let journal =
        resume_from.and_then(|p| checkpoint::CheckpointJournal::load_and_verify(p, &identity));

    // A checkpoint is resumed by listing exactly its ranges; hints and
    // startup discovery play no part.
    let resume_ranges = journal.as_ref().and_then(|cj| cj.remaining.clone());
    RunCheckpoint {
        path,
        identity,
        journal,
        resume_ranges,
    }
}

/// Without a region the SDK cannot sign a single request. Left to the
/// listing, that surfaced as a retryable client error on every segment
/// and the run spent its whole retry budget (minutes) before exiting as
/// a network failure — the wrong exit class for a setup mistake.
fn require_signing_region(target: &RunTarget<'_>, sdk_config: &aws_config::SdkConfig) {
    let side_without_region =
        target.region.is_none() || target.target_region.is_some_and(|r| r.is_none());
    if side_without_region && sdk_config.region().is_none() {
        exit_before_run(
            agent::ExitCode::ProviderSetup,
            format!(
                "No AWS region resolved{}: pass --region{} or set AWS_REGION \
                 (or a region in the AWS profile).",
                if target.region.is_none() {
                    ""
                } else {
                    " for the diff target"
                },
                if target.region.is_none() {
                    ""
                } else {
                    " / --target-region"
                },
            ),
        );
    }
}

/// Log how requests are routed, before the first S3 request. Startup discovery already talks to the endpoint, so an unexpected proxy
/// must be named before it; a diff names both sides, whose endpoints (and
/// NO_PROXY matches) can differ.
fn log_proxy_routing(spec: &RunSpec<'_>, sdk_config: &aws_config::SdkConfig) {
    let RunSpec { cfg, target, .. } = *spec;
    let sdk_region = sdk_config.region().map(|r| r.as_ref().to_string());
    let mut sides = vec![(
        "source",
        target.bucket,
        target.region.or(sdk_region.as_deref()),
        cfg.s3.endpoint_url.as_deref(),
    )];
    if let Some(target_bucket) = target.target_bucket {
        sides.push((
            "target",
            target_bucket,
            target.target_region.flatten().or(sdk_region.as_deref()),
            spec.diff_target_endpoint,
        ));
    }
    for (side, bucket, region, endpoint) in sides {
        if let Some(url) =
            agent::resolved_request_url(Some(bucket), region, endpoint, cfg.s3.force_path_style())
        {
            match agent::env_proxy_for_url(&url) {
                Some(proxy) => info!(
                    "  {} requests to {} go through proxy {} (from the proxy environment variables)",
                    side, url, proxy
                ),
                None => log::debug!("  {} requests to {} connect directly", side, url),
            }
        }
    }
}

/// The --trace-compat writer, if any (exit 5 when it cannot be opened).
fn open_trace_writer(cfg: &S3TurboConfig) -> Option<Arc<dyn trace::S3TraceWriter>> {
    match trace::trace_writer_for_target(cfg.s3.trace_compat.as_deref()) {
        Ok(writer) => writer.map(Arc::from),
        // Through the standard failure epilogue: this exit used to
        // print nothing (the reason went only to the log file).
        Err(e) => exit_before_run(agent::ExitCode::OutputWrite, e),
    }
}

/// The list run's key-space boundaries: an explicit --hints-file, else
/// startup structural discovery for a flat (delimiter='') list run; none
/// for a --resume run (its segments come from the checkpoint) and for
/// --start-after.
async fn initial_boundaries(
    spec: &RunSpec<'_>,
    resuming: bool,
    sdk_config: &aws_config::SdkConfig,
    g_state: &core::GlobalState,
) -> Vec<String> {
    let RunSpec {
        cli, cfg, target, ..
    } = *spec;
    // ── Load or generate KeySpace hints ─────────────────
    // --start-after is single-chain: hint segments would each override
    // their start with the CLI key and list overlapping ranges, so
    // startup discovery is skipped. (--hints-file plus --start-after is
    // rejected at CLI validation.)
    let mut ks_list: Vec<String> = if resuming {
        Vec::new()
    } else if cfg.s3.start_after.is_some() {
        info!("--start-after is single-chain: listing as one segment");
        Vec::new()
    } else {
        load_hints(cli.hints_file.as_deref())
    };
    // ── Startup structural discovery ─────────────────────
    // When no hints exist for a flat (delimiter='') list run, probe the
    // bucket's CommonPrefix structure once at startup so the first run
    // lists in parallel with no prior step. A --resume run reads its
    // segments from the checkpoint instead.
    if ks_list.is_empty()
        && !resuming
        && target.mode == RunMode::List
        && !cli.no_auto_hints
        && cli.hints_file.is_none()
        && cli.delimiter.is_empty()
        && cfg.s3.start_after.is_none()
    {
        info!("Probing bucket structure for startup key-space boundaries");
        let probe_client = core::build_s3_client(
            sdk_config,
            target.region,
            cfg.s3.endpoint_url.as_deref(),
            cfg.s3.force_path_style(),
        );
        let concurrency = cfg.runtime.max_concurrency;
        // Runtime splitting still covers this run, so one flat boundary
        // per worker is enough; spare boundaries would only cost probes.
        let discovered = startup::startup_boundaries(
            &probe_client,
            target.bucket,
            &target.prefix,
            cfg,
            cli.max_keys,
            g_state,
            concurrency.saturating_mul(2).clamp(16, 512),
            concurrency.clamp(1, 64),
        )
        .await;
        let interrupted_discovery = discovered.is_none();
        let boundaries = discovered.unwrap_or_default();
        if interrupted_discovery {
            info!("Interrupted during startup discovery");
        } else if boundaries.is_empty() {
            info!("Startup discovery found no cuttable key space — using single-segment listing");
        } else {
            info!(
                "Startup discovery found {} key-space boundaries",
                boundaries.len()
            );
            log::debug!("Startup boundaries: {}", boundaries.join("\t"));
            ks_list = boundaries;
        }
    }
    ks_list
}

/// Ranges this run will not list because the checkpoint records them
/// listed. Captured here rather than re-read at manifest time: a run
/// that finishes removes its checkpoint, so the file on disk at the end
/// says nothing about whether this run resumed.
fn note_resume(
    cli: &Cli,
    checkpoint: &RunCheckpoint,
    run_warnings: &mut Vec<String>,
) -> Option<usize> {
    let (Some(cj), Some(ranges)) = (&checkpoint.journal, checkpoint.resume_ranges.as_deref())
    else {
        return None;
    };
    let listed = cj.listed_ranges.unwrap_or(0);
    info!(
        "Resuming checkpoint: {} key range(s) left to list",
        ranges.len()
    );
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
    Some(listed)
}

/// What every listing side's task context shares; only the target and
/// direction differ between the list task and the two diff sides.
struct SideContexts<'a> {
    spec: &'a RunSpec<'a>,
    sdk_config: &'a aws_config::SdkConfig,
    trace_writer: Option<Arc<dyn trace::S3TraceWriter>>,
    g_state: &'a core::GlobalState,
    addressing_style: String,
}

impl SideContexts<'_> {
    /// One listing side's task context.
    fn side(
        &self,
        bucket: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
        data_map_channel: SegmentBatchSender,
        dir: u8,
    ) -> core::S3TaskContext {
        let RunSpec { cli, cfg, .. } = *self.spec;
        core::S3TaskContext::new(core::TaskContextParams {
            bucket,
            region,
            endpoint,
            force_path_style: cfg.s3.force_path_style(),
            sdk_config: self.sdk_config,
            s3_config: &cfg.s3,
            data_map_channel,
            dir,
            g_state: self.g_state.clone(),
            trace_writer: self.trace_writer.clone(),
            addressing_style: &self.addressing_style,
            provider: cfg.s3.provider.as_deref(),
            delimiter: Some(&cli.delimiter),
            max_keys: cli.max_keys,
            start_after: cfg.s3.start_after.as_deref(),
        })
    }
}

/// Spawn both diff sides (each partitioned by startup discovery) and the
/// merge that compares them.
async fn spawn_diff(
    set: &mut tokio::task::JoinSet<()>,
    contexts: &SideContexts<'_>,
    concurrency: usize,
) {
    let spec = contexts.spec;
    let RunSpec {
        cli, cfg, target, ..
    } = *spec;
    let target_region: Option<&str> = target.target_region.and_then(|inner| inner);
    let target_bucket: &str = target
        .target_bucket
        .expect("target_bucket required for diff mode");

    // Per-side boundaries from startup discovery — the same automatic
    // source as list mode. Sides need not agree: each side only has
    // to be a complete ordered partition of its own key space. The
    // sides resolve concurrently; each can take many round-trips.
    let (left_bounds, right_bounds) = tokio::join!(
        startup::diff_side_boundaries(
            target.bucket,
            target.region,
            cfg.s3.endpoint_url.as_deref(),
            &target.prefix,
            cfg,
            cli.no_auto_hints,
            &cli.delimiter,
            cli.max_keys,
            contexts.sdk_config,
            contexts.g_state,
        ),
        startup::diff_side_boundaries(
            target_bucket,
            target_region,
            spec.diff_target_endpoint,
            &target.prefix,
            cfg,
            cli.no_auto_hints,
            &cli.delimiter,
            cli.max_keys,
            contexts.sdk_config,
            contexts.g_state,
        ),
    );
    info!(
        "  diff segments: left {}, right {}",
        left_bounds.len() + 1,
        right_bounds.len() + 1
    );

    let (left_senders, left_receivers) = diff_segment_channels(left_bounds.len() + 1);
    let (right_senders, right_receivers) = diff_segment_channels(right_bounds.len() + 1);

    // Base contexts; each segment task swaps in its own sender.
    let (placeholder_tx, _) = tokio::sync::mpsc::channel(1);
    let left_ctx = contexts.side(
        target.bucket,
        target.region,
        cfg.s3.endpoint_url.as_deref(),
        placeholder_tx.clone(),
        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
    );
    let right_ctx = contexts.side(
        target_bucket,
        target_region,
        spec.diff_target_endpoint,
        placeholder_tx,
        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
    );

    let (left_head_tx, left_head_rx) = tokio::sync::watch::channel(0usize);
    let (right_head_tx, right_head_rx) = tokio::sync::watch::channel(0usize);
    let prefix = target.prefix.clone();
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
    let prefix = target.prefix.clone();
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
    let diff_g_state = contexts.g_state.clone();
    let diff_ks = spec.filename_ks.to_string();
    let diff_output = spec.filename_output.to_string();
    let diff_output_config = cfg.output.clone();
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
}

/// Spawn the list reactor and the data map that writes its output; returns
/// the reactor's resume-progress slot.
fn spawn_list(
    set: &mut tokio::task::JoinSet<()>,
    contexts: &SideContexts<'_>,
    tx: SegmentBatchSender,
    rx: SegmentBatchReceiver,
    hints: core::KeySpaceHints,
    concurrency: usize,
) -> ResumeSlot {
    let spec = contexts.spec;
    let RunSpec { cfg, target, .. } = *spec;
    let prefix = target.prefix.clone();
    let task_ctx = contexts.side(
        target.bucket,
        target.region,
        cfg.s3.endpoint_url.as_deref(),
        tx,
        core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE,
    );
    let resume_slot = task_ctx.resume_progress.clone();
    set.spawn(async move {
        tasks_s3::flat_list_main_task(&task_ctx, &prefix, concurrency, hints).await
    });

    // ── Spawn data map task (list modes) ─────────────────
    let g_state = contexts.g_state;
    if spec.list_output_format == ListOutputFormat::Summary {
        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        set.spawn(async move { data_map::data_map_task_list_summary_only(data_map_ctx).await });
    } else if spec.list_output_format.writes_stdout_rows() {
        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        let text_format = data_map::ListTextOutputFormat::from(spec.list_output_format);
        set.spawn(
            async move { data_map::data_map_task_list_stdout(data_map_ctx, text_format).await },
        );
    } else {
        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        let filename_ks = spec.filename_ks.to_string();
        let filename_output = spec.filename_output.to_string();
        let output_config = cfg.output.clone();
        set.spawn(async move {
            data_map::data_map_task_list_streaming(
                data_map_ctx,
                &filename_ks,
                &filename_output,
                output_config,
            )
            .await
        });
    }
    resume_slot
}

/// Wait for all tasks.  There are deliberately no periodic checkpoint
/// saves: a segment counts as complete once its last batch is queued,
/// while its rows may still sit in the channel, a row-group buffer or
/// a BufWriter — and a Parquet file has no readable footer until it is
/// closed.  A mid-run save therefore recorded segments whose rows a
/// crash or a later write error would lose, and the next `--resume`
/// skipped them for good.  The checkpoint is written only on the
/// graceful-interrupt path (`finalize_checkpoint`), after the outputs are
/// finalized.
async fn join_all(set: &mut tokio::task::JoinSet<()>, g_state: &core::GlobalState) {
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
}

/// Final checkpoint save / removal. Returns what the exit line should say about resuming (`None` for runs
/// that cannot resume: diff, --start-after) and whether the listing had
/// finished when Ctrl-C arrived.
fn finalize_checkpoint(
    spec: &RunSpec<'_>,
    checkpoint: &RunCheckpoint,
    resume_slot: Option<&ResumeSlot>,
    g_state: &core::GlobalState,
    interrupted: &AtomicBool,
    run_warnings: &mut Vec<String>,
) -> (Option<String>, bool) {
    let target = spec.target;
    let is_diff = target.mode == RunMode::BiDir;
    let mut checkpoint_note: Option<String> = None;
    // Ctrl-C after everything was listed and written: a list reactor
    // with no key range left, or a diff merge that ran to the end. The
    // outputs are complete, so the run reports success — in every mode,
    // with or without a checkpoint.
    let listing_finished = interrupted.load(Ordering::SeqCst) && {
        let metrics = g_state.metrics_snapshot();
        metrics.fatal_errors == 0
            && metrics.output_errors == 0
            && if is_diff {
                g_state.diff_merge_complete()
            } else {
                resume_slot.is_some_and(|slot| {
                    slot.lock()
                        .unwrap()
                        .as_ref()
                        .is_some_and(|progress| progress.remaining.is_empty())
                })
            }
    };
    if listing_finished {
        info!("Interrupted after the listing had finished; outputs are complete");
    }
    let Some(cp_path) = checkpoint.path.as_deref() else {
        return (checkpoint_note, listing_finished);
    };
    let final_metrics = g_state.metrics_snapshot();
    let run_was_interrupted = interrupted.load(Ordering::SeqCst) && !listing_finished;
    // Another job's checkpoint that shares this file name (same
    // bucket, region and prefix; another endpoint, filter or page
    // size) is neither removed nor overwritten.
    let may_replace = checkpoint::CheckpointJournal::may_replace(cp_path, &checkpoint.identity);
    if final_metrics.fatal_errors > 0 || final_metrics.output_errors > 0 {
        info!("Skipping final checkpoint save because run failed before producing reliable output");
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
        let progress = resume_slot.and_then(|slot| slot.lock().unwrap().take());
        match progress {
            Some(progress) => {
                let listed_before = checkpoint
                    .journal
                    .as_ref()
                    .and_then(|cj| cj.listed_ranges)
                    .unwrap_or(0);
                let remaining_count = progress.remaining.len();
                let journal = checkpoint::CheckpointJournal {
                    bucket: target.bucket.to_string(),
                    prefix: target.prefix.clone(),
                    last_updated: chrono::Local::now().to_rfc3339(),
                    identity: Some(checkpoint.identity.clone()),
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
                        let message = format!("{}; a --resume would list everything again", e);
                        run_warnings.push(message.clone());
                        checkpoint_note = Some(message);
                    }
                }
            }
            None => {
                let message = "no resume progress was recorded; a --resume would list everything \
                     again"
                    .to_string();
                warn!("{}", message);
                checkpoint_note = Some(message);
            }
        }
    }
    (checkpoint_note, listing_finished)
}
