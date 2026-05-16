// All modules exported from the library crate (src/lib.rs).
// The binary uses `s3_turbo_list::...` paths to avoid module duplication.
use s3_turbo_list::{
    auto_hints, checkpoint, config, core, data_map, diff, hints, mon, tasks_s3, trace,
};

use chrono::Local;
use clap::{Parser, Subcommand};
use config::S3TurboConfig;
use core::RunMode;
use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ── CLI definition ─────────────────────────────────────────

#[derive(Parser)]
#[command(name = "s3-turbo-list")]
#[command(
    author,
    version,
    about = "High-performance concurrent S3 bucket listing",
    long_about = None
)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,

    /// Path to TOML config file
    #[arg(long, global = true)]
    config: Option<String>,

    /// Prefix to start listing from
    #[arg(short, long, default_value = "/", global = true)]
    prefix: String,

    /// Worker threads for tokio runtime
    #[arg(short = 'T', long, global = true)]
    threads: Option<usize>,

    /// Max concurrent list operations
    #[arg(short, long, global = true)]
    concurrency: Option<usize>,

    /// Input key space hints file (overrides auto-hints)
    #[arg(short = 'H', long, global = true)]
    hints_file: Option<String>,

    /// Object filter expression (e.g. "SOURCE.size > 1000")
    #[arg(short, long, global = true)]
    filter: Option<String>,

    /// Log to file
    #[arg(short, long, global = true)]
    log: bool,

    /// Custom S3 endpoint URL
    #[arg(long = "endpoint-url", global = true)]
    endpoint: Option<String>,

    /// Force path-style addressing
    #[arg(long, global = true)]
    force_path_style: bool,

    /// Log file path (implies --log)
    #[arg(long, global = true)]
    output_log_file: Option<String>,

    /// KeySpace file output path
    #[arg(long, global = true)]
    output_ks_file: Option<String>,

    /// Parquet file output path
    #[arg(long, global = true)]
    output_parquet_file: Option<String>,

    /// Resume from checkpoint
    #[arg(long, global = true)]
    resume: bool,

    /// Disable auto-hints (forces manual hints or single-threaded)
    #[arg(long, global = true)]
    no_auto_hints: bool,

    // ── S3-compatible observability flags ─────────────────
    /// Delimiter for ListObjectsV2 (default: "/")
    #[arg(long, default_value = "/", global = true)]
    delimiter: String,

    /// Max keys per ListObjectsV2 page
    #[arg(long, global = true)]
    max_keys: Option<i32>,

    /// Start listing after this key
    #[arg(long, global = true)]
    start_after: Option<String>,

    /// Resume from a specific continuation token
    #[arg(long, global = true)]
    continuation_token: Option<String>,

    /// AWS named profile or vendor profile name (e.g. "bos", "minio")
    #[arg(long, global = true)]
    profile: Option<String>,

    /// S3 addressing style: path, virtual, or auto
    #[arg(long, global = true)]
    addressing_style: Option<String>,

    /// Emit S3 compat trace events to stderr
    #[arg(long, global = true)]
    debug_s3: bool,

    /// Write S3 compat trace events to this JSONL file
    #[arg(long, global = true)]
    trace_compat: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Fast list a single bucket and export results
    List {
        /// AWS region
        #[arg(long)]
        region: Option<String>,

        /// Source bucket to list
        #[arg(long)]
        bucket: String,
    },

    /// Bi-directional fast list and diff results
    Diff {
        /// Source AWS region
        #[arg(long)]
        region: Option<String>,

        /// Source bucket to list
        #[arg(long)]
        bucket: String,

        /// Target AWS region
        #[arg(long)]
        target_region: Option<String>,

        /// Target bucket to list
        #[arg(long)]
        target_bucket: String,
    },

    /// Validate S3-compatible provider compatibility before listing
    CompatProbe {
        /// Endpoint URL (required for compat-probe)
        #[arg(long = "endpoint")]
        endpoint_url: String,

        /// AWS region or vendor region
        #[arg(long)]
        region: String,

        /// Bucket to probe
        #[arg(long)]
        bucket: String,

        /// Addressing style: path, virtual, or auto
        #[arg(long, default_value = "auto")]
        addressing_style: String,

        /// Output JSON report file path (default: stdout)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Auto-discover KeySpace hints via adaptive sampling
    AutoHints {
        /// AWS region
        #[arg(long)]
        region: Option<String>,

        /// Bucket to sample
        #[arg(long)]
        bucket: String,

        /// Output hints file path
        #[arg(short, long)]
        output: Option<String>,

        /// Stop after scanning this many objects; default scans the full bucket
        #[arg(long)]
        sample_limit: Option<usize>,

        /// Stop after scanning this many ListObjectsV2 pages; default scans all pages
        #[arg(long)]
        max_pages: Option<usize>,
    },

    /// Validate a local hints file without contacting S3
    HintsValidate {
        /// Hints file path
        #[arg(short = 'H', long)]
        hints_file: String,

        /// Number of boundaries to preview
        #[arg(long, default_value_t = 5)]
        preview: usize,

        /// Emit JSON report
        #[arg(long)]
        json: bool,
    },
}

// ── Main ───────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    // This command is intentionally local-only and should not depend on S3
    // credentials, endpoint configuration, or a valid runtime config file.
    if let Commands::HintsValidate {
        hints_file,
        preview,
        json,
    } = &cli.cmd
    {
        run_hints_validate(hints_file, *preview, *json);
        return;
    }

    // Load config.
    let mut cfg = S3TurboConfig::load(cli.config.as_deref()).unwrap_or_else(|e| {
        eprintln!("Config error: {}", e);
        std::process::exit(1);
    });

    cfg.apply_cli_overrides(
        cli.threads,
        cli.concurrency,
        cli.endpoint.as_deref(),
        cli.force_path_style,
        cli.addressing_style.as_deref(),
        cli.profile.as_deref(),
        cli.debug_s3,
        cli.trace_compat.as_deref(),
        cli.start_after.as_deref(),
        cli.output_log_file.as_deref(),
        cli.output_ks_file.as_deref(),
        cli.output_parquet_file.as_deref(),
    );
    cfg.apply_profile_preset();
    cfg.normalize_addressing_style();

    // Setup logging.
    let opt_log = cli.log || cfg.output.log_file.is_some();
    let loglevel = std::env::var("RUST_LOG").unwrap_or_else(|_| "s3_turbo_list=info".to_string());

    if opt_log {
        let logfile_s =
            cfg.output.log_file.clone().unwrap_or_else(|| {
                format!("turbo_list_{}.log", Local::now().format("%Y%m%d%H%M%S"))
            });
        let logfile = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(logfile_s)
            .expect("unable to open log file");
        env_logger::Builder::new()
            .parse_filters(&loglevel)
            .target(env_logger::Target::Pipe(Box::new(logfile)))
            .init();
    } else {
        env_logger::Builder::new().parse_filters(&loglevel).init();
    }

    // Parse subcommand.
    let (mode, opt_region, opt_bucket, opt_target_region, opt_target_bucket) = match &cli.cmd {
        Commands::List { region, bucket } => (
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
        } => (
            RunMode::BiDir,
            region.as_deref(),
            bucket.as_str(),
            Some(target_region.as_deref()),
            Some(target_bucket.as_str()),
        ),
        Commands::CompatProbe {
            endpoint_url,
            region,
            bucket,
            addressing_style,
            output,
        } => {
            run_compat_probe(
                endpoint_url,
                region,
                bucket,
                addressing_style,
                output.as_deref(),
                &cfg,
            );
            return;
        }
        Commands::AutoHints {
            region,
            bucket,
            output,
            sample_limit,
            max_pages,
        } => {
            run_auto_hints(
                region.as_deref(),
                bucket,
                output.as_deref(),
                *sample_limit,
                *max_pages,
                &cfg,
            );
            return;
        }
        Commands::HintsValidate {
            hints_file,
            preview,
            json,
        } => {
            run_hints_validate(hints_file, *preview, *json);
            return;
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
            eprintln!("Filter error: {}", e);
            std::process::exit(1);
        }
        info!("Filter installed: \"{}\"", filter_expr);
    }

    // ── Phase 3: orchestration wiring ───────────────────────
    let g_tasks_count = if mode == RunMode::BiDir { 4 } else { 3 }; // list + data_map + mon (+right list)

    let dt_str = Local::now().format("%Y%m%d%H%M%S").to_string();
    let region_prefix: std::borrow::Cow<'_, str> = opt_region
        .map(|r: &str| format!("{}_", r).into())
        .unwrap_or_default();

    // Setup Ctrl-C handler
    let quit = Arc::new(AtomicBool::new(false));
    let q = quit.clone();
    ctrlc::set_handler(move || {
        q.store(true, Ordering::SeqCst);
    })
    .expect("failed to set ctrl-c signal handler");

    // Build runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(cfg.runtime.worker_threads)
        .build()
        .unwrap();

    rt.block_on(async {
        // ── Checkpoint journal (resume mode) ──────────────────
        let checkpoint_path_opt = if cli.resume {
            Some(checkpoint::checkpoint_path(opt_bucket, opt_region))
        } else {
            None
        };

        // Build the current run identity for checkpoint verification.
        let current_identity = checkpoint::CheckpointIdentity::new(
            opt_bucket,
            opt_region,
            &opt_prefix,
            Some(&cli.delimiter),
            cli.max_keys,
            cfg.s3.profile.as_deref(),
            Some(&cfg.s3.addressing_style.to_string()),
            Some(if mode == RunMode::BiDir {
                "bidir"
            } else {
                "list"
            }),
        );

        let checkpoint_journal = checkpoint_path_opt
            .as_deref()
            .and_then(|p| checkpoint::CheckpointJournal::load_and_verify(p, &current_identity));

        if let Some(ref cj) = checkpoint_journal {
            info!(
                "Resuming checkpoint: {} of {} segments completed",
                cj.completed_indices.len(),
                cj.total_segments
            );
        }

        let g_state = core::GlobalState::new(quit, g_tasks_count);
        let mut set = tokio::task::JoinSet::new();
        let concurrency = cfg.runtime.max_concurrency;
        let channel_capacity = cfg.channel.capacity;

        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(core::ObjectKey, core::ObjectProps)>>(
            channel_capacity,
        );

        // ── Create trace writer ──────────────────────────────
        use crate::trace::S3TraceWriter;
        let trace_writer: Option<Arc<dyn S3TraceWriter>> = {
            let writer =
                trace::create_trace_writer(cfg.s3.trace_compat.as_deref(), cfg.s3.debug_s3);
            Some(Arc::from(writer))
        };

        // ── Load or generate KeySpace hints ─────────────────
        let ks_list: Vec<String> = load_hints(
            cli.hints_file.as_deref(),
            opt_bucket,
            opt_region,
            cfg.s3.profile.as_deref(),
        );
        let hints = core::KeySpaceHints::new_from(&ks_list);

        // Filter out completed segments when resuming.
        let hints = if let Some(ref cj) = checkpoint_journal {
            let remaining_boundaries = cj.filter_uncompleted(&ks_list);
            let filtered = core::KeySpaceHints::new_from(&remaining_boundaries);
            info!(
                "Resume: {} segments filtered, {} remaining",
                ks_list.len() + 1 - remaining_boundaries.len(),
                filtered.total_count()
            );
            filtered
        } else {
            hints
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

        // ── Spawn list task (left) ──────────────────────────
        let prefix = opt_prefix.clone();
        let dir = if mode == RunMode::BiDir {
            core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE
        } else {
            core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE
        };
        let left_checkpoint: Arc<std::sync::Mutex<Vec<usize>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let s3_cfg = cfg.s3.clone();
        let task_ctx = core::S3TaskContext::new(
            opt_bucket,
            opt_region,
            cfg.s3.endpoint_url.as_deref(),
            cfg.s3.force_path_style,
            &s3_cfg,
            tx.clone(),
            dir,
            g_state.clone(),
            trace_writer.clone(),
            &cfg.s3.addressing_style.to_string(),
            cfg.s3.profile.as_deref(),
            Some(&cli.delimiter),
            cli.max_keys,
            cfg.s3.start_after.as_deref(),
            left_checkpoint.clone(),
        );
        set.spawn(async move {
            tasks_s3::flat_list_main_task(&task_ctx, &prefix, concurrency, hints).await
        });

        // ── Spawn list task (right) if diff mode ────────────
        let right_checkpoint: Option<Arc<std::sync::Mutex<Vec<usize>>>> = if mode == RunMode::BiDir
        {
            let prefix = opt_prefix.clone();
            let target_region: Option<&str> =
                opt_target_region.and_then(|inner: Option<&str>| inner);
            let target_bucket: &str =
                opt_target_bucket.expect("target_bucket required for diff mode");
            let target_ks: Vec<String> = vec![];
            let target_hints = core::KeySpaceHints::new_from(&target_ks);

            let right_cp: Arc<std::sync::Mutex<Vec<usize>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));

            let task_ctx = core::S3TaskContext::new(
                target_bucket,
                target_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style,
                &cfg.s3,
                tx.clone(),
                core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                g_state.clone(),
                trace_writer.clone(),
                &cfg.s3.addressing_style.to_string(),
                cfg.s3.profile.as_deref(),
                Some(&cli.delimiter),
                cli.max_keys,
                cfg.s3.start_after.as_deref(),
                right_cp.clone(),
            );
            set.spawn(async move {
                tasks_s3::flat_list_main_task(&task_ctx, &prefix, concurrency, target_hints).await
            });
            Some(right_cp)
        } else {
            None
        };

        // ── Spawn data map task ─────────────────────────────
        let filename_ks = cfg
            .output
            .ks_file
            .clone()
            .unwrap_or_else(|| format!("{}_{}_{}.ks", region_prefix, opt_bucket, dt_str));
        let filename_output = cfg.output.parquet_file.clone().unwrap_or_else(|| {
            if mode == RunMode::List {
                format!("{}_{}_{}.parquet", region_prefix, opt_bucket, dt_str)
            } else {
                let tr = opt_target_region.and_then(|r| r).unwrap_or("");
                let tb = opt_target_bucket.unwrap_or("");
                format!(
                    "{}_{}_{}_{}_{}.parquet",
                    region_prefix, opt_bucket, tr, tb, dt_str
                )
            }
        });

        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        let is_diff = mode == RunMode::BiDir;
        let output_config = cfg.output.clone();
        if is_diff {
            set.spawn(async move {
                data_map::data_map_task(
                    data_map_ctx,
                    &filename_ks,
                    &filename_output,
                    true,
                    output_config,
                )
                .await
            });
        } else {
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

        // ── Spawn monitor task ──────────────────────────────
        let mon_ctx = core::MonContext::new(g_state.clone());
        set.spawn(async move { mon::mon_task(mon_ctx).await });

        // ── Diff mode lifecycle ───────────────────────────
        if mode == RunMode::BiDir {
            diff::init_diff_state();
        }

        // Drop the original sender so the channel closes when all list tasks
        // finish (each list task clones tx; this drops the last reference).
        drop(tx);

        // Wait for all tasks — save checkpoints on segment completion.
        let mut last_checkpoint_save = std::time::Instant::now();
        while let Some(result) = set.join_next().await {
            if let Err(e) = result {
                error!("Task panicked: {}", e);
                if e.is_cancelled() {
                    info!("Task was cancelled (abort or shutdown)");
                } else if let Ok(panic_msg) = e.try_into_panic() {
                    let msg: String = panic_msg
                        .downcast_ref::<&str>()
                        .map(|s: &&str| s.to_string())
                        .or_else(|| panic_msg.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "<unknown panic>".to_string());
                    error!("Task panic message: {}", msg);
                }
                // Propagate panic as failure — exit with non-zero code.
                g_state.quit();
            }

            // Save checkpoint progress periodically (every 30s or on state change).
            if cli.resume
                && last_checkpoint_save.elapsed().as_secs() >= 30
                && g_state.all_list_tasks_is_running()
            {
                if let Some(ref cp_path) = checkpoint_path_opt {
                    let mut completed: Vec<usize> = left_checkpoint.lock().unwrap().clone();
                    if let Some(ref right_cp) = right_checkpoint {
                        completed.extend(right_cp.lock().unwrap().clone());
                    }
                    let journal = checkpoint::CheckpointJournal {
                        bucket: opt_bucket.to_string(),
                        prefix: opt_prefix.clone(),
                        total_segments: hints_count,
                        completed_indices: completed,
                        last_updated: chrono::Local::now().to_rfc3339(),
                        identity: Some(current_identity.clone()),
                    };
                    journal.save(cp_path);
                    last_checkpoint_save = std::time::Instant::now();
                }
            }
        }

        // ── Final checkpoint save on successful completion ─
        if cli.resume {
            if let Some(ref cp_path) = checkpoint_path_opt {
                let mut completed: Vec<usize> = left_checkpoint.lock().unwrap().clone();
                if let Some(ref right_cp) = right_checkpoint {
                    completed.extend(right_cp.lock().unwrap().clone());
                }
                if !completed.is_empty() {
                    let journal = checkpoint::CheckpointJournal {
                        bucket: opt_bucket.to_string(),
                        prefix: opt_prefix.clone(),
                        total_segments: hints_count,
                        completed_indices: completed,
                        last_updated: chrono::Local::now().to_rfc3339(),
                        identity: Some(current_identity.clone()),
                    };
                    journal.save(cp_path);
                    info!(
                        "Final checkpoint saved: {}/{} segments completed",
                        journal.completed_indices.len(),
                        journal.total_segments
                    );
                }
            }
        }

        // ── Diff mode completion notice ────────────────────
        if mode == RunMode::BiDir {
            info!("{}", diff::diff_complete_notice());
        }

        info!("All tasks completed.");
    });

    rt.shutdown_background();
}

// ── Unified hints loader ───────────────────────────────────

/// Top-level hints loader: resolves hints from explicit file, auto-hints cache,
/// or falls back to empty (single-segment).
///
/// Priority:
/// 1. `hints_file` (from `--hints-file` CLI flag) — always used first.
/// 2. Auto-hints cache at `{region}_{bucket}_hints.toml` in CWD.
/// 3. Single-segment fallback (empty vec).
fn load_hints(
    hints_file: Option<&str>,
    bucket: &str,
    region: Option<&str>,
    profile: Option<&str>,
) -> Vec<String> {
    // 1. Explicit --hints-file takes absolute precedence.
    if let Some(path) = hints_file {
        match hints::parse_hints_file(path) {
            Ok(boundaries) => {
                hints::warn_bos_hinted_segments(profile, &boundaries);
                return boundaries;
            }
            Err(e) => {
                error!("Failed to load hints file '{}': {}", path, e);
                error!("Aborting to avoid sending malformed S3 requests.");
                std::process::exit(1);
            }
        }
    }

    // 2. Try auto-hints cache at conventional path.
    let cache_filename = if let Some(r) = region {
        format!("{}_{}_hints.toml", r, bucket)
    } else {
        format!("{}_hints.toml", bucket)
    };

    if let Ok(boundaries) = hints::parse_hints_file(&cache_filename) {
        hints::warn_bos_hinted_segments(profile, &boundaries);
        return boundaries;
    }

    // 3. Single-segment fallback.
    info!(
        "No hints available. Run 's3-turbo-list auto-hints --bucket {}' first for \
         optimal performance. Using single-segment fallback.",
        bucket
    );
    vec![]
}

// ── Compat-probe ───────────────────────────────────────────

fn run_compat_probe(
    endpoint_url: &str,
    region: &str,
    bucket: &str,
    addressing_style: &str,
    output: Option<&str>,
    cfg: &S3TurboConfig,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();

    rt.block_on(async {
        compat_probe::run_compat_probe(endpoint_url, region, bucket, addressing_style, output, cfg)
            .await
    });
}

mod compat_probe {
    use crate::config::S3TurboConfig;
    use crate::trace::{S3CompatEvent, S3TraceWriter, StderrTraceWriter};
    use serde::Serialize;
    use std::time::Instant;

    #[derive(Debug, Serialize)]
    pub struct CompatProbeReport {
        pub endpoint_url: String,
        pub region: String,
        pub bucket: String,
        pub addressing_style: String,
        pub tests: Vec<ProbeTestResult>,
        pub overall_status: String, // "compatible", "partial", "incompatible"
    }

    #[derive(Debug, Serialize)]
    pub struct ProbeTestResult {
        pub test: String,
        pub status: String, // "ok", "error", "skipped"
        pub latency_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub http_status: Option<u16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub s3_error_code: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub error_message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub request_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub is_truncated: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub key_count: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub contents_count: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub next_continuation_token_present: Option<bool>,
    }

    pub async fn run_compat_probe(
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        addressing_style: &str,
        output: Option<&str>,
        cfg: &S3TurboConfig,
    ) {
        let trace_writer: Box<dyn S3TraceWriter> = Box::new(StderrTraceWriter);

        // Build S3 client.
        let loader = aws_config::from_env()
            .retry_config(
                aws_config::retry::RetryConfig::standard()
                    .with_max_attempts(cfg.s3.max_attempts)
                    .with_initial_backoff(std::time::Duration::from_secs(
                        cfg.s3.initial_backoff_secs,
                    )),
            )
            .timeout_config(
                aws_config::timeout::TimeoutConfigBuilder::new()
                    .connect_timeout(std::time::Duration::from_secs(cfg.s3.connect_timeout_secs))
                    .operation_timeout(std::time::Duration::from_secs(
                        cfg.s3.operation_timeout_secs,
                    ))
                    .read_timeout(std::time::Duration::from_secs(
                        cfg.s3.operation_timeout_secs,
                    ))
                    .operation_attempt_timeout(std::time::Duration::from_secs(
                        cfg.s3.operation_timeout_secs,
                    ))
                    .build(),
            );
        let config = loader.load().await;
        let mut s3_cfg = aws_sdk_s3::config::Builder::from(&config);
        s3_cfg = s3_cfg.region(aws_sdk_s3::config::Region::new(region.to_owned()));
        s3_cfg = s3_cfg.endpoint_url(endpoint_url.to_owned());
        if addressing_style == "path" {
            s3_cfg = s3_cfg.force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_cfg.build());
        let mut results: Vec<ProbeTestResult> = Vec::new();

        // ── Test 1: HeadBucket ────────────────────────────
        let (res, evt) = timed_s3_call(
            || async { client.head_bucket().bucket(bucket).send().await },
            "HeadBucket",
            endpoint_url,
            region,
            bucket,
            addressing_style,
            &trace_writer,
            None,
        )
        .await;
        results.push(probe_result_from("HeadBucket", res, evt));

        // ── Test 2: ListObjectsV2 max-keys=1 ──────────────
        let (res, evt) = timed_s3_call(
            || async {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .max_keys(1)
                    .send()
                    .await
            },
            "ListObjectsV2 (max-keys=1)",
            endpoint_url,
            region,
            bucket,
            addressing_style,
            &trace_writer,
            None,
        )
        .await;
        results.push(probe_result_from("ListObjectsV2 (max-keys=1)", res, evt));

        // ── Test 3: ListObjectsV2 with prefix ─────────────
        let (res, evt) = timed_s3_call(
            || async {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix("")
                    .max_keys(1)
                    .send()
                    .await
            },
            "ListObjectsV2 with prefix",
            endpoint_url,
            region,
            bucket,
            addressing_style,
            &trace_writer,
            None,
        )
        .await;
        results.push(probe_result_from("ListObjectsV2 with prefix", res, evt));

        // ── Test 4: ListObjectsV2 with delimiter ──────────
        let (res, evt) = timed_s3_call(
            || async {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .delimiter("/")
                    .max_keys(1)
                    .send()
                    .await
            },
            "ListObjectsV2 with delimiter",
            endpoint_url,
            region,
            bucket,
            addressing_style,
            &trace_writer,
            None,
        )
        .await;
        results.push(probe_result_from("ListObjectsV2 with delimiter", res, evt));

        // ── Test 5: Encoding-type url ─────────────────────
        let (res, evt) = timed_s3_call(
            || async {
                client
                    .list_objects_v2()
                    .bucket(bucket)
                    .encoding_type(aws_sdk_s3::types::EncodingType::Url)
                    .max_keys(1)
                    .send()
                    .await
            },
            "ListObjectsV2 (encoding-type=url)",
            endpoint_url,
            region,
            bucket,
            addressing_style,
            &trace_writer,
            None,
        )
        .await;
        results.push(probe_result_from(
            "ListObjectsV2 (encoding-type=url)",
            res,
            evt,
        ));

        // ── Test 6: Small pagination check ────────────────
        let (res, mut evt) = timed_s3_call(
            || async {
                // List up to 3 keys, verify continuation token if truncated.
                let resp = client
                    .list_objects_v2()
                    .bucket(bucket)
                    .max_keys(3)
                    .send()
                    .await?;
                Ok::<
                    _,
                    aws_sdk_s3::error::SdkError<
                        aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error,
                    >,
                >(resp)
            },
            "ListObjectsV2 pagination check",
            endpoint_url,
            region,
            bucket,
            addressing_style,
            &trace_writer,
            None,
        )
        .await;
        // Handle insufficient objects case: if the response has fewer than max_keys
        // objects and is_truncated is false, we can't meaningfully test pagination.
        let pagination_probe = match &res {
            Ok(resp) => {
                let key_count = resp.key_count().unwrap_or(0);
                let is_truncated = resp.is_truncated().unwrap_or(false);
                let content_count = resp.contents().len() as i32;
                let next_token = resp.next_continuation_token().map(|t| t.to_string());
                evt.key_count = Some(key_count);
                evt.contents_count = Some(content_count);
                evt.is_truncated = is_truncated;
                evt.next_continuation_token = next_token.clone();
                evt.next_continuation_token_present = Some(next_token.is_some());
                if !is_truncated && content_count < 3 {
                    ProbeTestResult {
                        test: "ListObjectsV2 pagination check".to_string(),
                        status: "skipped".to_string(),
                        latency_ms: evt.latency_ms,
                        http_status: Some(200),
                        s3_error_code: None,
                        error_message: Some(format!(
                            "insufficient_objects: only {} objects; need >= max_keys (3) to test pagination",
                            content_count
                        )),
                        request_id: evt.request_id.clone(),
                        is_truncated: Some(is_truncated),
                        key_count: Some(key_count),
                        contents_count: Some(content_count),
                        next_continuation_token_present: Some(false),
                    }
                } else if is_truncated {
                    match next_token {
                        Some(token) => {
                            let (second_res, second_evt) = timed_s3_call(
                                || {
                                    let token_for_request = token.clone();
                                    async move {
                                        client
                                            .list_objects_v2()
                                            .bucket(bucket)
                                            .max_keys(3)
                                            .continuation_token(token_for_request)
                                            .send()
                                            .await
                                    }
                                },
                                "ListObjectsV2 pagination check (page 2)",
                                endpoint_url,
                                region,
                                bucket,
                                addressing_style,
                                &trace_writer,
                                Some(&token),
                            )
                            .await;

                            match second_res {
                                Ok(second_resp) => {
                                    let second_key_count = second_resp.key_count().unwrap_or(0);
                                    let second_content_count = second_resp.contents().len() as i32;
                                    ProbeTestResult {
                                        test: "ListObjectsV2 pagination check".to_string(),
                                        status: "ok".to_string(),
                                        latency_ms: evt.latency_ms + second_evt.latency_ms,
                                        http_status: Some(200),
                                        s3_error_code: None,
                                        error_message: Some(format!(
                                            "page_1_keys={}, page_2_keys={}",
                                            content_count, second_content_count
                                        )),
                                        request_id: second_evt.request_id.clone(),
                                        is_truncated: Some(is_truncated),
                                        key_count: Some(key_count + second_key_count),
                                        contents_count: Some(content_count + second_content_count),
                                        next_continuation_token_present: Some(true),
                                    }
                                }
                                Err(e) => ProbeTestResult {
                                    test: "ListObjectsV2 pagination check".to_string(),
                                    status: "error".to_string(),
                                    latency_ms: evt.latency_ms + second_evt.latency_ms,
                                    http_status: if second_evt.http_status != 0 {
                                        Some(second_evt.http_status)
                                    } else {
                                        None
                                    },
                                    s3_error_code: second_evt.s3_error_code,
                                    error_message: Some(format!("page_2_error: {:?}", e)),
                                    request_id: second_evt.request_id,
                                    is_truncated: Some(is_truncated),
                                    key_count: Some(key_count),
                                    contents_count: Some(content_count),
                                    next_continuation_token_present: Some(true),
                                },
                            }
                        }
                        None => ProbeTestResult {
                            test: "ListObjectsV2 pagination check".to_string(),
                            status: "error".to_string(),
                            latency_ms: evt.latency_ms,
                            http_status: Some(200),
                            s3_error_code: None,
                            error_message: Some(
                                "is_truncated=true but next_continuation_token is absent"
                                    .to_string(),
                            ),
                            request_id: evt.request_id.clone(),
                            is_truncated: Some(is_truncated),
                            key_count: Some(key_count),
                            contents_count: Some(content_count),
                            next_continuation_token_present: Some(false),
                        },
                    }
                } else {
                    let mut result =
                        probe_result_from::<
                            (),
                            aws_sdk_s3::error::SdkError<
                                aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error,
                            >,
                        >("ListObjectsV2 pagination check", Ok(()), evt);
                    result.is_truncated = Some(is_truncated);
                    result.key_count = Some(key_count);
                    result.contents_count = Some(content_count);
                    result.next_continuation_token_present = Some(false);
                    result
                }
            }
            Err(_) => probe_result_from("ListObjectsV2 pagination check", res, evt),
        };
        results.push(pagination_probe);

        // Determine overall status.
        let error_count = results.iter().filter(|r| r.status == "error").count();
        let overall = if error_count == 0 {
            "compatible"
        } else if error_count < results.len() {
            "partial"
        } else {
            "incompatible"
        };

        let report = CompatProbeReport {
            endpoint_url: endpoint_url.to_string(),
            region: region.to_string(),
            bucket: bucket.to_string(),
            addressing_style: addressing_style.to_string(),
            tests: results,
            overall_status: overall.to_string(),
        };

        let json = serde_json::to_string_pretty(&report).unwrap();
        if let Some(out_path) = output {
            std::fs::write(out_path, &json).expect("Failed to write compat-probe report");
            println!("Compat-probe report written to {}", out_path);
        } else {
            println!("{}", json);
        }
    }

    /// Time an S3 call and emit a trace event.
    ///
    /// NOTE: request_id is not extracted from successful responses because
    /// typed AWS SDK outputs (HeadBucketOutput, ListObjectsV2Output) don't
    /// expose raw HTTP headers.  The field is populated from errors via
    /// `tasks_s3::handle_sdk_error` where the raw response is available.
    async fn timed_s3_call<F, Fut, T, E>(
        f: F,
        test_name: &str,
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        addressing_style: &str,
        trace_writer: &Box<dyn S3TraceWriter>,
        continuation_token: Option<&str>,
    ) -> (Result<T, E>, S3CompatEvent)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Debug,
    {
        let start = Instant::now();
        let result = f().await;
        let latency_ms = start.elapsed().as_millis() as u64;

        let mut event = S3CompatEvent::new(test_name, endpoint_url, bucket, "");
        event.region = Some(region.to_string());
        event.addressing_style = addressing_style.to_string();
        event.latency_ms = latency_ms;
        event.continuation_token = continuation_token.map(|token| token.to_string());

        match &result {
            Ok(_) => {
                event.http_status = 200;
            }
            Err(_) => {
                event.http_status = 0;
                event.fatal = true;
            }
        }

        trace_writer.write_event(event.clone());
        (result, event)
    }

    fn probe_result_from<T, E: std::fmt::Debug>(
        test_name: &str,
        result: Result<T, E>,
        event: S3CompatEvent,
    ) -> ProbeTestResult {
        match result {
            Ok(_) => ProbeTestResult {
                test: test_name.to_string(),
                status: "ok".to_string(),
                latency_ms: event.latency_ms,
                http_status: if event.http_status != 0 {
                    Some(event.http_status)
                } else {
                    Some(200)
                },
                s3_error_code: event.s3_error_code,
                error_message: event.s3_error_message,
                request_id: event.request_id,
                is_truncated: None,
                key_count: None,
                contents_count: None,
                next_continuation_token_present: None,
            },
            Err(e) => ProbeTestResult {
                test: test_name.to_string(),
                status: "error".to_string(),
                latency_ms: event.latency_ms,
                http_status: if event.http_status != 0 {
                    Some(event.http_status)
                } else {
                    None
                },
                s3_error_code: event.s3_error_code,
                error_message: Some(format!("{:?}", e)),
                request_id: event.request_id,
                is_truncated: None,
                key_count: None,
                contents_count: None,
                next_continuation_token_present: None,
            },
        }
    }
}

fn run_auto_hints(
    region: Option<&str>,
    bucket: &str,
    output: Option<&str>,
    sample_limit: Option<usize>,
    max_pages: Option<usize>,
    cfg: &S3TurboConfig,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();

    let endpoint = cfg.s3.endpoint_url.as_deref();
    let fps = cfg.s3.force_path_style;
    let threshold = cfg.auto_hints.sample_threshold;
    let depth = cfg.auto_hints.max_prefix_depth;

    if cfg.s3.profile.as_deref() == Some("bos") {
        log::warn!(
            "Generating hints for BOS is allowed, but hinted multi-segment BOS scans \
             are not authoritative until BOS fixes its ListObjectsV2 start_after + \
             continuation-token compatibility behavior."
        );
    }

    rt.block_on(async {
        auto_hints::generate_hints(
            region,
            bucket,
            output,
            endpoint,
            fps,
            threshold,
            depth,
            sample_limit,
            max_pages,
        )
        .await
    });
}

fn run_hints_validate(path: &str, preview: usize, json: bool) {
    match hints::inspect_hints_file(path, preview) {
        Ok(report) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).expect("serialize hints report")
                );
                return;
            }

            println!("Hints file: {}", report.path);
            println!("  Format:          {:?}", report.format);
            println!("  Boundary count:  {}", report.boundary_count);
            if let Some(metadata) = &report.metadata {
                println!(
                    "  Bucket:          {}",
                    metadata.bucket.as_deref().unwrap_or("-")
                );
                println!(
                    "  Region:          {}",
                    metadata.region.as_deref().unwrap_or("-")
                );
                println!(
                    "  Total objects:   {}",
                    metadata
                        .total_objects
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string())
                );
                println!(
                    "  Scan mode:       {}",
                    metadata.scan_mode.as_deref().unwrap_or("unknown")
                );
                if metadata.scan_mode.as_deref() == Some("sampled") {
                    println!(
                        "  Sampled objects: {}",
                        metadata
                            .sampled_objects
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".to_string())
                    );
                    println!(
                        "  Sampled pages:   {}",
                        metadata
                            .sampled_pages
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".to_string())
                    );
                }
                if let Some(summary) = &report.estimate_summary {
                    println!(
                        "  Estimates:       {} ({})",
                        summary.count,
                        if summary.sampled {
                            "sampled/estimated"
                        } else {
                            "observed/full"
                        }
                    );
                    println!(
                        "  Estimate min/max/sum: {}/{}/{}",
                        summary.min_estimated_objects,
                        summary.max_estimated_objects,
                        summary.total_estimated_objects
                    );
                }
            }
            if !report.first_estimates.is_empty() {
                println!("  First {} estimates:", report.first_estimates.len());
                for estimate in &report.first_estimates {
                    println!(
                        "    - start_after='{}', end_before='{}', estimated_objects={}",
                        estimate.start_after,
                        estimate.end_before.as_deref().unwrap_or(""),
                        estimate.estimated_objects
                    );
                }
            }
            if !report.first_boundaries.is_empty() {
                println!("  First {} boundaries:", report.first_boundaries.len());
                for boundary in &report.first_boundaries {
                    println!("    - {}", boundary);
                }
            }
            if !report.warnings.is_empty() {
                println!("  Warnings:");
                for warning in &report.warnings {
                    println!("    - {}", warning);
                }
            }
        }
        Err(e) => {
            eprintln!("Hints validation failed: {}", e);
            std::process::exit(1);
        }
    }
}
