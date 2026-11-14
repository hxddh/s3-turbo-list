//! Executing a list or diff job: region resolution, startup partitioning,
//! the listing and data-map tasks, then committing the output and the
//! checkpoint.

use super::Outcome;
use super::job::{Job, Output, SideSpec};
use super::result::{ExitCode, Failure, ResumeInfo, RunResult, Stats, Status};
use log::info;
use s3tl::checkpoint::{Checkpoint, ResumeProgress};
use s3tl::config::EngineConfig;
use s3tl::core::{self, ObjectKey, ObjectProps, RunMode};
use s3tl::segment_order::SegmentOrder;
use s3tl::{data_map, mon, startup, tasks_s3, trace};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

type Batch = Vec<(ObjectKey, ObjectProps)>;
type ResumeSlot = Arc<Mutex<Option<ResumeProgress>>>;

/// The `--dry-run` result: the job as it would run.
pub fn plan(job: &Job, result: &mut RunResult) {
    result.status = Status::Planned;
    result.source = Some(job.source.describe(job.source.region.as_deref()));
    result.target = job.target.as_ref().map(|t| t.describe(t.region.as_deref()));
    result.output = Some(job.output_info());
    let (partitioning, reason) = job.partitioning();
    result.plan = Some(serde_json::json!({
        "partitioning": partitioning,
        "partitioning_reason": reason,
        "concurrency": job.concurrency,
        "compare": job.target.as_ref().map(|_| format!("{:?}", job.compare).to_lowercase()),
        "versions": job.target.is_none().then_some(job.versions),
        "max_keys": job.max_keys,
        "resume_ranges": job.resume_from.as_ref().map(|c| c.remaining.len()),
        "first_part": job.first_part,
        "region": if job.source.region.is_some() {
            "explicit"
        } else if job.source.endpoint.is_some() {
            "AWS_REGION, else us-east-1"
        } else {
            "detected from the bucket"
        },
    }));
    if let Some(cp) = &job.resume_from {
        result.resume = Some(ResumeInfo {
            resumed: true,
            checkpoint: job
                .output
                .dir()
                .map(|d| Checkpoint::path(d).display().to_string()),
            remaining_ranges: cp.remaining.len(),
        });
    }
}

/// Ctrl-C / SIGTERM: stop the listing gracefully. Quitting through the
/// global state wakes every task waiting on it at once.
fn install_interrupt_handler(g_state: &core::GlobalState) -> Result<Arc<AtomicBool>, Failure> {
    let interrupted = Arc::new(AtomicBool::new(false));
    let (g, i) = (g_state.clone(), interrupted.clone());
    ctrlc::set_handler(move || {
        i.store(true, Ordering::SeqCst);
        g.quit();
    })
    .map_err(|e| Failure::internal(format!("cannot install the signal handler: {}", e)))?;
    Ok(interrupted)
}

/// What the listing reports back for the commit step.
struct Listing {
    source_region: String,
    target_region: Option<String>,
    resume_slot: Option<ResumeSlot>,
}

/// Run `job` to the end (or the interrupt) and build its result.
pub fn execute(job: Job, mut result: RunResult) -> Outcome {
    let started = Instant::now();
    let stdout_is_data = matches!(job.output, Output::Stdout);
    result.source = Some(job.source.describe(job.source.region.as_deref()));
    result.target = job.target.as_ref().map(|t| t.describe(t.region.as_deref()));
    let fail = |mut result: RunResult, failure: Failure| {
        result.fail(failure);
        Outcome {
            result,
            stdout_is_data,
            result_dir: None,
        }
    };

    let tasks = if job.mode == RunMode::BiDir { 4 } else { 3 };
    let g_state = core::GlobalState::new(Arc::new(AtomicBool::new(false)), tasks);
    let interrupted = match install_interrupt_handler(&g_state) {
        Ok(interrupted) => interrupted,
        Err(f) => return fail(result, f),
    };
    let mut cfg = EngineConfig::from_env();
    cfg.runtime.max_concurrency = job.concurrency;
    s3tl::probe::set_concurrency(job.concurrency);
    cfg.s3.force_path_style = job.source.path_style;
    cfg.s3.endpoint_url = job.source.endpoint.clone();

    if let Some(dir) = job.output.dir()
        && let Err(e) = std::fs::create_dir_all(dir)
    {
        return fail(
            result,
            Failure::output(format!(
                "cannot create output directory '{}': {}",
                dir.display(),
                e
            )),
        );
    }

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(cfg.runtime.worker_threads)
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(result, Failure::internal(format!("tokio runtime: {}", e))),
    };
    let listing = rt.block_on(run_listing(&job, &cfg, &g_state));
    rt.shutdown_background();

    let listing = match listing {
        Ok(listing) => listing,
        Err(f) => {
            cleanup_failed_output(&job, &[]);
            return fail(result, f);
        }
    };
    result.source = Some(job.source.describe(Some(&listing.source_region)));
    if let (Some(t), Some(region)) = (&job.target, &listing.target_region) {
        result.target = Some(t.describe(Some(region)));
    }
    finish(
        job,
        result,
        &g_state,
        &interrupted,
        listing,
        started,
        stdout_is_data,
    )
}

/// The signing region of a side: `--region`; for a custom endpoint the SDK's
/// region or us-east-1; on AWS the bucket's own region, read from the
/// `x-amz-bucket-region` header of one HeadBucket.
pub(super) async fn resolve_region(
    sdk: &aws_config::SdkConfig,
    side: &SideSpec,
    timeout_secs: u64,
) -> Result<String, Failure> {
    if let Some(region) = &side.region {
        return Ok(region.clone());
    }
    let fallback = sdk
        .region()
        .map(|r| r.to_string())
        .unwrap_or_else(|| "us-east-1".to_string());
    if side.endpoint.is_some() {
        return Ok(fallback);
    }
    let client = core::build_s3_client(sdk, Some(&fallback), None, side.path_style);
    let request = client.head_bucket().bucket(&side.url.bucket).send();
    let response =
        match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), request).await {
            Ok(response) => response,
            Err(_) => return Ok(fallback),
        };
    match response {
        Ok(out) => Ok(out.bucket_region().map_or(fallback, str::to_string)),
        Err(err) => {
            if let Some(region) = err
                .raw_response()
                .and_then(|r| r.headers().get("x-amz-bucket-region"))
            {
                return Ok(region.to_string());
            }
            if s3tl::error::is_missing_credentials(&err) {
                return Err(Failure::from_errno(
                    s3tl::error::ERROR_S3_MISSING_CREDENTIALS,
                    s3tl::error::MISSING_CREDENTIALS_MESSAGE,
                ));
            }
            if err.raw_response().map(|r| r.status().as_u16()) == Some(404) {
                return Err(Failure::from_errno(
                    s3tl::error::ERROR_NO_BUCKET,
                    format!("bucket '{}' does not exist", side.url.bucket),
                ));
            }
            // Anything else is reported precisely by the listing itself.
            Ok(fallback)
        }
    }
}

fn open_trace(job: &Job) -> Result<Option<Arc<dyn trace::S3TraceWriter>>, Failure> {
    trace::trace_writer_for_target(job.trace.as_deref())
        .map(|w| w.map(Arc::from))
        .map_err(Failure::output)
}

struct Contexts<'a> {
    job: &'a Job,
    cfg: &'a EngineConfig,
    sdk: &'a aws_config::SdkConfig,
    trace: Option<Arc<dyn trace::S3TraceWriter>>,
    g_state: &'a core::GlobalState,
}

impl Contexts<'_> {
    fn side(
        &self,
        side: &SideSpec,
        region: &str,
        channel: tokio::sync::mpsc::Sender<Batch>,
        dir: u8,
    ) -> core::S3TaskContext {
        core::S3TaskContext::new(core::TaskContextParams {
            bucket: &side.url.bucket,
            region: Some(region),
            endpoint: side.endpoint.as_deref(),
            force_path_style: side.path_style,
            sdk_config: self.sdk,
            s3_config: &self.cfg.s3,
            data_map_channel: channel,
            dir,
            g_state: self.g_state.clone(),
            trace_writer: self.trace.clone(),
            addressing_style: if side.path_style { "path" } else { "virtual" },
            delimiter: Some(&self.job.delimiter),
            max_keys: self.job.max_keys,
            versions: self.job.versions,
        })
    }
}

async fn run_listing(
    job: &Job,
    cfg: &EngineConfig,
    g_state: &core::GlobalState,
) -> Result<Listing, Failure> {
    let sdk = core::S3TaskContext::load_sdk_config(&cfg.s3).await;
    let timeout = cfg.s3.operation_timeout_secs;
    let (source_region, target_region) = match &job.target {
        None => (resolve_region(&sdk, &job.source, timeout).await?, None),
        Some(target) => {
            let (s, t) = tokio::join!(
                resolve_region(&sdk, &job.source, timeout),
                resolve_region(&sdk, target, timeout)
            );
            (s?, Some(t?))
        }
    };
    let contexts = Contexts {
        job,
        cfg,
        sdk: &sdk,
        trace: open_trace(job)?,
        g_state,
    };
    info!(
        "s3tl v{}: {} {} (region {}), concurrency {}",
        env!("CARGO_PKG_VERSION"),
        if job.mode == RunMode::BiDir {
            "diff"
        } else {
            "list"
        },
        job.source.url.url(),
        source_region,
        job.concurrency
    );

    let mut set = tokio::task::JoinSet::new();
    let resume_slot = match (&job.target, &target_region) {
        (Some(target), Some(t_region)) => {
            spawn_diff(&mut set, &contexts, &source_region, target, t_region).await;
            None
        }
        _ => Some(spawn_list(&mut set, &contexts, &source_region).await),
    };
    let mon_ctx = core::MonContext::new(g_state.clone());
    set.spawn(async move { mon::mon_task(mon_ctx).await });
    join_all(&mut set, g_state).await;
    Ok(Listing {
        source_region,
        target_region,
        resume_slot,
    })
}

/// Startup boundaries of a list run (none when resuming or not splitting).
async fn list_boundaries(contexts: &Contexts<'_>, region: &str) -> core::KeySpaceHints {
    let job = contexts.job;
    if let Some(cp) = &job.resume_from {
        info!("Resuming: {} key range(s) left to list", cp.remaining.len());
        return core::KeySpaceHints::from_ranges(&cp.remaining);
    }
    if job.partitioning().0 != "startup_discovery" {
        return core::KeySpaceHints::new_from(&[]);
    }
    let client = core::build_s3_client(
        contexts.sdk,
        Some(region),
        job.source.endpoint.as_deref(),
        job.source.path_style,
    );
    // Runtime splitting covers skew, so one flat boundary per worker is
    // enough; spare boundaries would only cost probes.
    let concurrency = job.concurrency;
    let boundaries = startup::startup_boundaries(
        &client,
        &job.source.url.bucket,
        &job.source.url.prefix,
        contexts.cfg,
        job.max_keys,
        contexts.g_state,
        concurrency.saturating_mul(2).clamp(16, 512),
        concurrency.clamp(1, 64),
    )
    .await
    .unwrap_or_default();
    info!("Startup discovery: {} boundaries", boundaries.len());
    core::KeySpaceHints::new_from(&boundaries)
}

async fn spawn_list(
    set: &mut tokio::task::JoinSet<()>,
    contexts: &Contexts<'_>,
    region: &str,
) -> ResumeSlot {
    let job = contexts.job;
    let hints = list_boundaries(contexts, region).await;
    let (tx, rx) = tokio::sync::mpsc::channel::<Batch>(contexts.cfg.channel.capacity);
    let task_ctx = contexts.side(
        &job.source,
        region,
        tx,
        core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE,
    );
    let resume_slot = task_ctx.resume_progress.clone();
    let prefix = job.source.url.prefix.clone();
    let concurrency = job.concurrency;
    set.spawn(async move {
        tasks_s3::flat_list_main_task(&task_ctx, &prefix, concurrency, hints).await
    });

    let data_ctx = core::DataMapContext::new(rx, contexts.g_state.clone());
    match &job.output {
        Output::None => {
            set.spawn(async move { data_map::data_map_task_list_summary_only(data_ctx).await });
        }
        Output::Stdout => {
            set.spawn(async move { data_map::data_map_task_list_stdout(data_ctx).await });
        }
        Output::Dir(dir) => {
            let (dir, first) = (dir.clone(), job.first_part);
            let mut output = contexts.cfg.output.clone();
            output.versions = job.versions;
            set.spawn(async move {
                data_map::data_map_task_list_streaming(data_ctx, dir, first, output).await
            });
        }
    }
    resume_slot
}

async fn spawn_diff(
    set: &mut tokio::task::JoinSet<()>,
    contexts: &Contexts<'_>,
    source_region: &str,
    target: &SideSpec,
    target_region: &str,
) {
    let job = contexts.job;
    let side_bounds = |side: &SideSpec, region: &str| {
        let side = side.clone();
        let region = region.to_string();
        async move {
            startup::diff_side_boundaries(
                &side.url.bucket,
                Some(&region),
                side.endpoint.as_deref(),
                &side.url.prefix,
                contexts.cfg,
                job.no_auto_hints,
                "",
                job.max_keys,
                contexts.sdk,
                contexts.g_state,
            )
            .await
        }
    };
    let (left_bounds, right_bounds) = tokio::join!(
        side_bounds(&job.source, source_region),
        side_bounds(target, target_region)
    );
    info!(
        "Diff segments: left {}, right {}",
        left_bounds.len() + 1,
        right_bounds.len() + 1
    );
    let left_order = Arc::new(SegmentOrder::new(
        left_bounds.len() + 1,
        tasks_s3::DIFF_SEGMENT_CHANNEL_CAP,
    ));
    let right_order = Arc::new(SegmentOrder::new(
        right_bounds.len() + 1,
        tasks_s3::DIFF_SEGMENT_CHANNEL_CAP,
    ));
    // Each segment task takes its own sender from the order.
    let (placeholder, _) = tokio::sync::mpsc::channel(1);
    let left_ctx = contexts.side(
        &job.source,
        source_region,
        placeholder.clone(),
        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
    );
    let right_ctx = contexts.side(
        target,
        target_region,
        placeholder,
        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
    );
    let (left_head_tx, left_head_rx) = tokio::sync::watch::channel(0usize);
    let (right_head_tx, right_head_rx) = tokio::sync::watch::channel(0usize);
    let concurrency = job.concurrency;
    let (left_prefix, right_prefix) = (job.source.url.prefix.clone(), target.url.prefix.clone());
    let order = left_order.clone();
    set.spawn(async move {
        let hints = core::KeySpaceHints::new_from(&left_bounds);
        tasks_s3::diff_side_task(
            &left_ctx,
            &left_prefix,
            concurrency,
            hints,
            order,
            Some(left_head_rx),
        )
        .await
    });
    let order = right_order.clone();
    set.spawn(async move {
        let hints = core::KeySpaceHints::new_from(&right_bounds);
        tasks_s3::diff_side_task(
            &right_ctx,
            &right_prefix,
            concurrency,
            hints,
            order,
            Some(right_head_rx),
        )
        .await
    });
    let sides = data_map::DiffStreamSides {
        left: left_order,
        right: right_order,
        left_prefix: job.source.url.prefix.clone(),
        right_prefix: target.url.prefix.clone(),
    };
    let heads = data_map::DiffMergeHeads {
        left: left_head_tx,
        right: right_head_tx,
    };
    let g_state = contexts.g_state.clone();
    let out_dir = job.output.dir().map(Path::to_path_buf);
    let output = contexts.cfg.output.clone();
    let verifier = (job.compare == core::CompareMode::Checksum).then(|| {
        let side = |spec: &SideSpec, region: &str| s3tl::checksum::Side {
            client: core::build_s3_client(
                contexts.sdk,
                Some(region),
                spec.endpoint.as_deref(),
                spec.path_style,
            ),
            bucket: spec.url.bucket.clone(),
            prefix: spec.url.prefix.clone(),
        };
        s3tl::checksum::verifier(
            side(&job.source, source_region),
            side(target, target_region),
            contexts.cfg.s3.operation_timeout_secs,
        )
    });
    let options = data_map::DiffOptions {
        compare: job.compare,
        verifier,
    };
    set.spawn(async move {
        data_map::data_map_task_diff_streaming(
            g_state,
            sides,
            Some(heads),
            out_dir,
            output,
            options,
        )
        .await
    });
}

/// Wait for every task. A panicking task is a fatal error (reported as an
/// internal error) and stops the run.
async fn join_all(set: &mut tokio::task::JoinSet<()>, g_state: &core::GlobalState) {
    while let Some(joined) = set.join_next().await {
        if let Err(e) = joined {
            if !e.is_cancelled() {
                log::error!("task panicked: {}", e);
                g_state.inc_fatal_error();
            }
            g_state.quit();
        }
    }
}

/// Make the temp parts of this run visible: fsync each, rename it to its
/// final name, fsync the directory. Empty parts are dropped, unless the
/// dataset would otherwise have no file at all (readers need one to see the
/// schema).
fn commit_parts(dir: &Path, parts: &[core::OutputPart]) -> Result<Vec<PathBuf>, String> {
    let has_committed = !super::job::committed_parts(dir).is_empty();
    let total: usize = parts.iter().map(|p| p.rows).sum();
    let mut published: Vec<PathBuf> = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let tmp = data_map::tmp_part_path(dir, part.index);
        let keep = part.rows > 0 || (total == 0 && !has_committed && i == 0);
        if !keep {
            let _ = std::fs::remove_file(&tmp);
            continue;
        }
        let target = dir.join(data_map::part_file_name(part.index));
        let commit = || -> std::io::Result<()> {
            std::fs::File::open(&tmp)?.sync_all()?;
            std::fs::rename(&tmp, &target)
        };
        if let Err(e) = commit() {
            // All or nothing: un-publish what this commit already renamed.
            unpublish(dir, &published);
            return Err(format!("cannot commit {}: {}", tmp.display(), e));
        }
        published.push(target);
    }
    if let Err(e) = s3tl::utils::sync_dir(dir) {
        unpublish(dir, &published);
        return Err(format!("cannot sync {}: {}", dir.display(), e));
    }
    Ok(published)
}

/// Remove parts this run published, restoring the dataset it started from.
fn unpublish(dir: &Path, published: &[PathBuf]) {
    for path in published {
        let _ = std::fs::remove_file(path);
    }
    let _ = s3tl::utils::sync_dir(dir);
}

/// A failed run leaves the dataset as it was before it: its temp parts are
/// removed, and a directory it created and left empty is removed too.
fn cleanup_failed_output(job: &Job, parts: &[core::OutputPart]) {
    let Some(dir) = job.output.dir() else {
        return;
    };
    for part in parts {
        let _ = std::fs::remove_file(data_map::tmp_part_path(dir, part.index));
    }
    if job.creates_dir {
        let _ = std::fs::remove_dir(dir); // only succeeds when empty
    }
}

/// The run's failure, if any: an output error, then a listing error, then
/// the interrupt (a real failure must not read as a resumable interrupt).
fn run_failure(
    g_state: &core::GlobalState,
    metrics: &core::RunMetricsSnapshot,
    interrupted: bool,
) -> Option<Failure> {
    if metrics.output_errors > 0 {
        let message = g_state
            .first_output_error()
            .unwrap_or_else(|| "an output write failed".to_string());
        return Some(Failure::output(message));
    }
    if metrics.fatal_errors > 0 {
        return Some(match g_state.first_fatal_error() {
            Some((errno, summary)) => Failure::from_errno(errno, summary),
            None => Failure::internal("a task panicked; please report this as a bug"),
        });
    }
    interrupted.then(|| Failure::interrupted("interrupted by a signal"))
}

/// Ctrl-C after everything was listed and written: the outputs are
/// complete, so the run reports success.
fn listing_finished(job: &Job, g_state: &core::GlobalState, listing: &Listing) -> bool {
    if job.mode == RunMode::BiDir {
        return g_state.diff_merge_complete();
    }
    listing.resume_slot.as_ref().is_some_and(|slot| {
        slot.lock()
            .unwrap()
            .as_ref()
            .is_some_and(|p| p.remaining.is_empty())
    })
}

fn finish(
    job: Job,
    mut result: RunResult,
    g_state: &core::GlobalState,
    interrupted: &AtomicBool,
    listing: Listing,
    started: Instant,
    stdout_is_data: bool,
) -> Outcome {
    let metrics = g_state.metrics_snapshot();
    let data = &metrics.data;
    let interrupted =
        interrupted.load(Ordering::SeqCst) && !listing_finished(&job, g_state, &listing);
    let mut failure = run_failure(g_state, &metrics, interrupted);
    let mut result_dir = None;

    // Commit (or discard) the dataset and the checkpoint.
    let mut checkpoint_saved: Option<(String, usize)> = None;
    if let Some(dir) = job.output.dir() {
        let keep = match &failure {
            None => true,
            Some(f) => f.exit == ExitCode::Interrupted && job.mode == RunMode::List,
        };
        if keep {
            match commit_parts(dir, &data.parts) {
                Err(e) => {
                    failure = Some(Failure::output(e));
                    cleanup_failed_output(&job, &data.parts);
                    if job.resume_from.is_some() {
                        result_dir = Some(dir.to_path_buf());
                    }
                }
                Ok(published) => {
                    result_dir = Some(dir.to_path_buf());
                    if failure.is_none() {
                        if job.mode == RunMode::List
                            && let Err(e) = Checkpoint::remove(dir)
                        {
                            // A checkpoint left behind would make a later
                            // --resume list its ranges again: take this run's
                            // parts back so the dataset still matches it.
                            unpublish(dir, &published);
                            failure = Some(
                                Failure::output(format!(
                                    "the listing finished but its checkpoint could not be \
                                     removed ({}); this run's parts were removed so the \
                                     dataset stays consistent",
                                    e
                                ))
                                .hint("--resume continues from the checkpoint"),
                            );
                        }
                    } else {
                        let progress = listing
                            .resume_slot
                            .as_ref()
                            .and_then(|slot| slot.lock().unwrap().take());
                        let saved = match progress {
                            Some(progress) => {
                                let remaining = progress.remaining.len();
                                Checkpoint::new(job.identity(), progress.remaining)
                                    .save(dir)
                                    .map(|()| remaining)
                            }
                            None => Err("no resume progress was recorded".to_string()),
                        };
                        match saved {
                            Ok(remaining) => {
                                checkpoint_saved =
                                    Some((Checkpoint::path(dir).display().to_string(), remaining))
                            }
                            Err(e) => {
                                // The checkpoint did not advance: published parts
                                // would be listed again by the next --resume, so
                                // the dataset goes back to what that checkpoint
                                // describes.
                                unpublish(dir, &published);
                                let resumable = job.resume_from.is_some();
                                if !resumable {
                                    result_dir = None;
                                    cleanup_failed_output(&job, &[]);
                                }
                                let f = Failure::output(format!(
                                    "interrupted, and the checkpoint could not be saved ({}); this \
                                 run's parts were removed so the dataset stays consistent",
                                    e
                                ));
                                failure = Some(if resumable {
                                    f.hint("--resume continues from the previous checkpoint")
                                } else {
                                    f.hint("rerun into an empty directory")
                                });
                            }
                        }
                    }
                }
            }
        } else {
            cleanup_failed_output(&job, &data.parts);
            // A failed resume leaves the earlier runs' parts and checkpoint.
            if job.resume_from.is_some() {
                result_dir = Some(dir.to_path_buf());
            }
        }
    }

    let mut output = job.output_info();
    output.rows = data.rows_written;
    result.output = Some(output);
    result.stats = Some(Stats {
        objects: data.objects,
        folders: data.folders,
        bytes: data.bytes,
        elapsed_secs: (started.elapsed().as_secs_f64() * 1000.0).round() / 1000.0,
        throttled: metrics.throttled_responses,
        timeouts: metrics.s3_client_timeouts + metrics.stream_timeouts,
        top_prefixes: data.top_prefixes.clone(),
    });
    result.diff = data.diff.clone();
    if job.mode == RunMode::List && job.output.dir().is_some() {
        let resumed = job.resume_from.is_some();
        result.resume = Some(match &checkpoint_saved {
            Some((path, remaining)) => ResumeInfo {
                resumed,
                checkpoint: Some(path.clone()),
                remaining_ranges: *remaining,
            },
            None => ResumeInfo {
                resumed,
                checkpoint: None,
                remaining_ranges: 0,
            },
        });
    }
    if let Some(mut f) = failure {
        if f.exit == ExitCode::Interrupted {
            f = match &checkpoint_saved {
                Some((_, remaining)) => Failure::interrupted(format!(
                    "interrupted; {} key range(s) left: rerun with --resume",
                    remaining
                ))
                .hint("rerun the same command with --resume"),
                None if job.mode == RunMode::List && job.output.dir().is_some() => {
                    Failure::interrupted("interrupted; no checkpoint was saved")
                }
                None => Failure::interrupted("interrupted; the output is partial"),
            };
        }
        result.fail(f);
    }
    Outcome {
        result,
        stdout_is_data,
        result_dir,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(index: usize, rows: usize) -> core::OutputPart {
        core::OutputPart { index, rows }
    }

    #[test]
    fn commit_publishes_all_parts_or_none() {
        let dir = tempfile::tempdir().unwrap();
        for i in [0, 1] {
            std::fs::write(data_map::tmp_part_path(dir.path(), i), b"x").unwrap();
        }
        // Part 2's temp file is missing: its commit fails after 0 and 1
        // were renamed, and both must be taken back.
        let err = commit_parts(dir.path(), &[part(0, 1), part(1, 1), part(2, 1)]).unwrap_err();
        assert!(err.contains("cannot commit"), "{}", err);
        for i in [0, 1, 2] {
            assert!(!dir.path().join(data_map::part_file_name(i)).exists());
        }

        for i in [0, 1] {
            std::fs::write(data_map::tmp_part_path(dir.path(), i), b"x").unwrap();
        }
        let published = commit_parts(dir.path(), &[part(0, 1), part(1, 1)]).unwrap();
        assert_eq!(published.len(), 2);
        assert!(published.iter().all(|p| p.exists()));
        unpublish(dir.path(), &published);
        assert!(published.iter().all(|p| !p.exists()));
    }

    #[test]
    fn empty_parts_are_dropped_unless_the_dataset_would_be_empty() {
        let dir = tempfile::tempdir().unwrap();
        for i in [0, 1] {
            std::fs::write(data_map::tmp_part_path(dir.path(), i), b"x").unwrap();
        }
        let published = commit_parts(dir.path(), &[part(0, 0), part(1, 0)]).unwrap();
        assert_eq!(
            published,
            vec![dir.path().join(data_map::part_file_name(0))]
        );
        assert!(!data_map::tmp_part_path(dir.path(), 1).exists());
    }
}
