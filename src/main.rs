// All modules exported from the library crate (src/lib.rs).
// The binary uses `s3_turbo_list::...` paths to avoid module duplication.
#![allow(
    clippy::borrowed_box,
    clippy::if_same_then_else,
    clippy::too_many_arguments
)]

use s3_turbo_list::{
    agent, auto_hints, checkpoint, compat_probe, config, core, data_map, hints, local_tools, mon,
    profiles, tasks_s3, trace,
};

use chrono::Local;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use config::S3TurboConfig;
use core::RunMode;
use log::{error, info};
use serde::Serialize;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

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

    /// Input key space hints file (overrides automatic discovery)
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

    /// Log file path (implies --log)
    #[arg(long, global = true)]
    output_log_file: Option<String>,

    /// KeySpace file output path
    #[arg(long, global = true)]
    output_ks_file: Option<String>,

    /// Parquet file output path
    #[arg(long, global = true)]
    output_parquet_file: Option<String>,

    /// Parquet compression codec: gzip, zstd, snappy, lz4, lz4_raw, brotli, or uncompressed
    #[arg(long, global = true)]
    compression: Option<String>,

    /// Parquet compression level for codecs that support levels
    #[arg(long, global = true)]
    compression_level: Option<u32>,

    /// Directory for default Parquet and KeySpace outputs
    #[arg(long, global = true)]
    output_dir: Option<String>,

    /// Resume from checkpoint
    #[arg(long, global = true)]
    resume: bool,

    /// Disable automatic startup discovery (use --hints-file or single-segment)
    #[arg(long, global = true)]
    no_auto_hints: bool,

    // ── S3-compatible observability flags ─────────────────
    /// Delimiter for ListObjectsV2; the default '' is a recursive
    /// full-bucket listing, use --delimiter '/' for hierarchical
    /// top-level listing
    #[arg(long, default_value = "", global = true)]
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

    /// Endpoint compatibility profile name (e.g. "bos", "minio", "r2")
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

    /// Emit machine-readable summaries for AI agents and automation
    #[arg(long, global = true)]
    agent: bool,

    /// Resolve inputs and outputs without contacting S3
    #[arg(long, global = true)]
    dry_run: bool,

    /// Write dry-run plan JSON to this path
    #[arg(long, global = true)]
    plan_json: Option<String>,

    /// Write final run manifest JSON to this path
    #[arg(long, global = true)]
    run_manifest: Option<String>,

    /// Scan and report aggregate metrics without writing Parquet or KeySpace outputs
    #[arg(long, global = true)]
    summary_only: bool,
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

        /// Output format for list results; parquet writes artifacts, tsv/ndjson stream rows to stdout
        #[arg(long, value_enum, default_value_t = ListOutputFormat::Parquet)]
        output_format: ListOutputFormat,
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
        /// Endpoint URL (defaults to the global --endpoint-url)
        #[arg(long = "endpoint")]
        endpoint_url: Option<String>,

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

    /// Summarize a local run manifest JSON file without contacting S3
    ManifestSummary {
        /// Run manifest JSON file written by --run-manifest
        manifest_file: String,

        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Validate manifest success, counters, row checks, and recorded artifacts via exit code
        #[arg(long)]
        check: bool,
    },

    /// Write a starter local TOML config without contacting S3
    InitConfig {
        /// Endpoint compatibility profile template: aws, minio, r2, b2, oss, or bos
        #[arg(long)]
        profile: Option<String>,

        /// Output config path
        #[arg(short, long, default_value = "s3-turbo-list.toml")]
        output: String,

        /// Allow replacing an existing config file
        #[arg(long)]
        overwrite: bool,

        /// Emit JSON report
        #[arg(long)]
        json: bool,
    },

    /// Print guidance without contacting S3: an overview when no topic is
    /// given, a provider quickstart (aws/minio/r2/bos), or a named recipe
    Guide {
        /// Topic: a provider (aws/minio/r2/bos), a recipe name (e.g.
        /// large-bucket, filter, release-check), or `index` to list recipes
        topic: Option<String>,
    },

    /// Run local environment checks without contacting S3
    Doctor {
        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Emit compact OK/WARN/NEXT output
        #[arg(long)]
        simple: bool,

        /// Include command suggestions for common local issues
        #[arg(long)]
        fix_suggestions: bool,
    },

    /// Generate shell completions without contacting S3
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },

    /// Generate a man page to stdout without contacting S3
    Man,

    /// Run a local synthetic streaming-output benchmark without contacting S3
    /// (developer tool; hidden from help)
    #[command(hide = true)]
    BenchmarkLocal {
        /// Local benchmark scenario to run
        #[arg(long, value_enum, default_value_t = LocalBenchmarkKind::ListOutput)]
        benchmark: LocalBenchmarkKind,

        /// Number of synthetic objects to write
        #[arg(long, default_value_t = 10_000)]
        objects: usize,

        /// Objects per synthetic batch
        #[arg(long, default_value_t = 1_000)]
        batch_size: usize,

        /// Number of distinct key prefixes
        #[arg(long, default_value_t = 128)]
        prefixes: usize,

        /// Number of synthetic producer tasks sending into the data-map channel
        #[arg(long, default_value_t = 1)]
        producers: usize,

        /// Synthetic diff data shape for diff-output benchmarks
        #[arg(long, value_enum, default_value_t = LocalDiffShape::Mixed)]
        diff_shape: LocalDiffShape,

        /// Local output path to benchmark
        #[arg(long, value_enum, default_value_t = ListOutputFormat::Parquet)]
        output_format: ListOutputFormat,

        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Write JSON report to this path
        #[arg(short, long)]
        output: Option<String>,

        /// Keep generated local Parquet/KS artifacts
        #[arg(long)]
        keep_artifacts: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ListOutputFormat {
    Parquet,
    Tsv,
    Ndjson,
}

impl ListOutputFormat {
    fn writes_artifacts(self) -> bool {
        matches!(self, Self::Parquet)
    }

    fn writes_stdout_rows(self) -> bool {
        matches!(self, Self::Tsv | Self::Ndjson)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Tsv => "tsv",
            Self::Ndjson => "ndjson",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LocalBenchmarkKind {
    ListOutput,
    DiffMap,
    DiffOutput,
}

impl LocalBenchmarkKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ListOutput => "list-output",
            Self::DiffMap => "diff-map",
            Self::DiffOutput => "diff-output",
        }
    }
}

impl std::fmt::Display for LocalBenchmarkKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LocalDiffShape {
    Mixed,
    AllEqual,
    AllChanged,
}

impl LocalDiffShape {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mixed => "mixed",
            Self::AllEqual => "all-equal",
            Self::AllChanged => "all-changed",
        }
    }
}

impl std::fmt::Display for LocalDiffShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Display for ListOutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<ListOutputFormat> for data_map::ListTextOutputFormat {
    fn from(format: ListOutputFormat) -> Self {
        match format {
            ListOutputFormat::Tsv => Self::Tsv,
            ListOutputFormat::Ndjson => Self::Ndjson,
            ListOutputFormat::Parquet => {
                unreachable!("parquet output does not use the text data-map sink")
            }
        }
    }
}

// ── Main ───────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

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
        } => {
            run_manifest_summary(manifest_file, *json || cli.agent, *check);
            return;
        }
        Commands::InitConfig {
            profile,
            output,
            overwrite,
            json,
        } => {
            run_init_config(profile.as_deref(), output, *overwrite, *json || cli.agent);
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
        .unwrap_or_else(|e| {
            eprintln!("Config error: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        });

    cfg.apply_cli_overrides(
        cli.threads,
        cli.concurrency,
        cli.endpoint.as_deref(),
        cli.addressing_style.as_deref(),
        cli.profile.as_deref(),
        cli.debug_s3,
        cli.trace_compat.as_deref(),
        cli.start_after.as_deref(),
        cli.output_log_file.as_deref(),
        cli.output_ks_file.as_deref(),
        cli.output_parquet_file.as_deref(),
        cli.compression.as_deref(),
        cli.compression_level,
    );
    cfg.apply_profile_preset(command_region(&cli.cmd));
    cfg.normalize_addressing_style();
    apply_output_dir_defaults(&cli, &mut cfg);
    apply_summary_only_output_defaults(&cli, &mut cfg);
    validate_summary_only_command(&cli);
    validate_output_format_command(&cli);
    validate_continuation_token_command(&cli, &cfg);
    validate_diff_hints_command(&cli);
    validate_diff_resume_command(&cli);
    let config_source = agent::ConfigSourceSummary::new(&config_load, cli_config_overrides(&cli));
    let config_source_warnings = config_source.warnings.clone();

    match &cli.cmd {
        Commands::Doctor {
            json,
            simple,
            fix_suggestions,
        } => {
            // doctor absorbed the former hints-validate command: when a hints
            // file is supplied it is linted and embedded in the report.
            let hints = cli.hints_file.as_deref().map(|path| {
                hints::inspect_hints_file(path, 5).unwrap_or_else(|e| {
                    eprintln!("Hints validation failed: {}", e);
                    std::process::exit(agent::ExitCode::CliConfig.code());
                })
            });
            let report = agent::doctor_report(&cfg, config_source.clone(), hints);
            if *json || cli.agent {
                println!("{}", agent::to_pretty_json(&report));
            } else if *simple {
                print_doctor_simple(&report, *fix_suggestions);
            } else {
                println!("Doctor status: {}", report.status);
                for check in &report.checks {
                    println!("  {}: {} — {}", check.name, check.status, check.message);
                }
                print_doctor_config(&report);
                if let Some(hints) = &report.hints {
                    print_doctor_hints(hints);
                }
                if *fix_suggestions {
                    print_doctor_suggestions(&report);
                }
            }
            if report.status == "error" {
                std::process::exit(agent::ExitCode::CliConfig.code());
            }
            return;
        }
        Commands::BenchmarkLocal {
            benchmark,
            objects,
            batch_size,
            prefixes,
            producers,
            diff_shape,
            output_format,
            json,
            output,
            keep_artifacts,
        } => {
            let report = run_benchmark_local(
                *benchmark,
                *objects,
                *batch_size,
                *prefixes,
                *producers,
                *diff_shape,
                *output_format,
                *keep_artifacts,
                &cfg,
            );
            if *json || output.is_some() {
                let rendered = agent::to_pretty_json(&report);
                if let Some(path) = output.as_deref() {
                    if let Err(e) = agent::write_json_file(path, &report) {
                        eprintln!("Benchmark write error: {}", e);
                        std::process::exit(agent::ExitCode::OutputWrite.code());
                    }
                }
                if *json {
                    println!("{}", rendered);
                }
            } else {
                println!(
                    "local benchmark: {} {} objects in {:.3}s ({:.0} objects/sec)",
                    report.benchmark, report.objects, report.elapsed_secs, report.objects_per_sec
                );
                if let Some(path) = &report.parquet_file {
                    println!("  parquet: {}", path);
                }
                if let Some(path) = &report.ks_file {
                    println!("  ks:      {}", path);
                }
                if let Some(path) = &report.text_file {
                    println!("  rows:    {}", path);
                }
                if !report.artifacts_kept {
                    println!("  artifacts removed");
                }
            }
            return;
        }
        _ => {}
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
                eprintln!("Filter error: {}", e);
                std::process::exit(agent::ExitCode::CliConfig.code());
            }
        }
        let report = build_plan_report(&cli, &cfg, config_source.clone());
        if let Some(path) = cli.plan_json.as_deref() {
            if let Err(e) = agent::write_json_file(path, &report) {
                eprintln!("Plan write error: {}", e);
                std::process::exit(agent::ExitCode::OutputWrite.code());
            }
        }
        if cli.agent || cli.plan_json.is_none() {
            println!("{}", agent::to_pretty_json(&report));
        }
        return;
    }

    validate_provider_setup_or_exit(&cli, &cfg);

    let mut run_warnings = config_source_warnings;
    run_warnings.extend(runtime_guardrail_warnings(&cli, &cfg));
    if !cli.agent {
        print_runtime_warnings(&run_warnings);
    }

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
            let endpoint_url = endpoint_url
                .as_deref()
                .or(cli.endpoint.as_deref())
                .unwrap_or_else(|| {
                    eprintln!(
                        "compat-probe requires an endpoint: pass --endpoint-url (global) or --endpoint"
                    );
                    std::process::exit(agent::ExitCode::CliConfig.code());
                });
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
        Commands::Doctor { .. } => {
            unreachable!("local-only commands are handled before runtime setup")
        }
        Commands::Completions { .. } | Commands::Man => {
            unreachable!("local-only commands are handled before config load")
        }
        Commands::ManifestSummary { .. } | Commands::InitConfig { .. } | Commands::Guide { .. } => {
            unreachable!("local tooling commands are handled before config load")
        }
        Commands::BenchmarkLocal { .. } => {
            unreachable!("benchmark-local is handled before runtime setup")
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
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
        info!("Filter installed: \"{}\"", filter_expr);
    }

    // ── Phase 3: orchestration wiring ───────────────────────
    let g_tasks_count = if mode == RunMode::BiDir { 4 } else { 3 }; // list + data_map + mon (+right list)

    let output_stem = output_stem(
        opt_region,
        opt_bucket,
        opt_target_region.flatten(),
        opt_target_bucket,
    );
    let filename_ks = cfg
        .output
        .ks_file
        .clone()
        .unwrap_or_else(|| format!("{}.ks", output_stem));
    let filename_output = cfg.output.parquet_file.clone().unwrap_or_else(|| {
        if mode == RunMode::List {
            format!("{}.parquet", output_stem)
        } else {
            format!("{}.parquet", output_stem)
        }
    });
    ensure_output_dir(&cli);

    // Setup Ctrl-C handler
    let quit = Arc::new(AtomicBool::new(false));
    let interrupted = Arc::new(AtomicBool::new(false));
    let q = quit.clone();
    let i = interrupted.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        q.store(true, Ordering::SeqCst);
        i.store(true, Ordering::SeqCst);
    }) {
        eprintln!("Failed to set ctrl-c signal handler: {}", e);
        std::process::exit(agent::ExitCode::InternalError.code());
    }

    let g_state = core::GlobalState::new(quit, g_tasks_count);
    let run_started_at = chrono::Utc::now();
    let run_timer = Instant::now();

    // Build runtime
    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);

    let list_output_format = list_output_format(&cli).unwrap_or(ListOutputFormat::Parquet);

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

        let g_state = g_state.clone();
        let mut set = tokio::task::JoinSet::new();
        let concurrency = cfg.runtime.max_concurrency;
        let channel_capacity = cfg.channel.capacity;
        let sdk_config = core::S3TaskContext::load_sdk_config(&cfg.s3).await;

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

        // ── Create trace writer ──────────────────────────────
        use crate::trace::S3TraceWriter;
        let trace_writer: Option<Arc<dyn S3TraceWriter>> =
            trace::create_trace_writer_opt(cfg.s3.trace_compat.as_deref(), cfg.s3.debug_s3)
                .map(Arc::from);

        // ── Load or generate KeySpace hints ─────────────────
        let hints_disabled_for_diff = mode == RunMode::BiDir;
        let ks_list: Vec<String> = load_hints(
            cli.hints_file.as_deref(),
            opt_bucket,
            opt_region,
            cfg.s3.profile.as_deref(),
            cli.no_auto_hints || hints_disabled_for_diff,
            hints_disabled_for_diff,
        );
        // ── Startup structural discovery ─────────────────────
        // When no hints exist for a flat (delimiter='') list run, probe the
        // bucket's CommonPrefix structure once at startup so the first run
        // lists in parallel with no prior step. The
        // boundaries are persisted to the conventional hints cache, so
        // subsequent runs (including --resume) reload identical segments
        // through the existing cache path.
        let mut ks_list = ks_list;
        // BOS is excluded: it has a documented start_after + continuation-token
        // incompatibility, so automatically switching BOS runs to hinted
        // multi-segment listing would silently produce non-authoritative output.
        let bos_profile = cfg.s3.profile.as_deref() == Some("bos");
        if ks_list.is_empty()
            && mode == RunMode::List
            && !cli.no_auto_hints
            && !bos_profile
            && cli.hints_file.is_none()
            && cli.delimiter.is_empty()
            && cli.continuation_token.is_none()
            && cfg.s3.start_after.is_none()
        {
            info!("Probing bucket structure for startup key-space boundaries");
            let probe_client = core::build_s3_client(
                &sdk_config,
                opt_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style,
            );
            let target_boundaries = concurrency.saturating_mul(2).clamp(16, 512);
            let boundaries = auto_hints::discover_startup_boundaries(
                &probe_client,
                opt_bucket,
                &opt_prefix,
                target_boundaries,
            )
            .await;
            if boundaries.is_empty() {
                info!(
                    "Startup discovery found no prefix structure — using single-segment listing"
                );
            } else {
                info!(
                    "Startup discovery found {} key-space boundaries",
                    boundaries.len()
                );
                match auto_hints::write_startup_hints_cache(
                    opt_bucket,
                    opt_region,
                    &opt_prefix,
                    &boundaries,
                ) {
                    Ok(path) => info!("Startup hints cached to {} for future runs", path),
                    Err(e) => {
                        log::warn!("{} — resume runs may not see identical segments", e)
                    }
                }
                ks_list = boundaries;
            }
        }
        let ks_list = ks_list;
        let original_hints_count = core::KeySpaceHints::new_from(&ks_list).total_count();

        // Discard a resume journal whose segment count does not match the
        // current hints — completed indices would otherwise skip the wrong
        // segments and silently drop keys.
        let checkpoint_journal = checkpoint_journal.filter(|cj| {
            if cj.total_segments == original_hints_count {
                return true;
            }
            log::warn!(
                "Checkpoint segment count {} does not match current hints ({}) — \
                 discarding checkpoint and starting fresh",
                cj.total_segments,
                original_hints_count
            );
            false
        });

        // Filter out completed segments when resuming.
        let hints = if let Some(ref cj) = checkpoint_journal {
            let filtered =
                core::KeySpaceHints::new_uncompleted_from(&ks_list, &cj.completed_indices);
            info!(
                "Resume: {} segments filtered, {} remaining",
                original_hints_count.saturating_sub(filtered.total_count()),
                filtered.total_count()
            );
            filtered
        } else {
            core::KeySpaceHints::new_from(&ks_list)
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
        let left_checkpoint: Arc<std::sync::Mutex<Vec<usize>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let right_checkpoint: Option<Arc<std::sync::Mutex<Vec<usize>>>> = None;
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
            // own key space.
            let left_bounds =
                diff_side_boundaries(opt_bucket, opt_region, &opt_prefix, &cfg, &cli, &sdk_config)
                    .await;
            let right_bounds = diff_side_boundaries(
                target_bucket,
                target_region,
                &opt_prefix,
                &cfg,
                &cli,
                &sdk_config,
            )
            .await;
            info!(
                "  diff segments: left {}, right {}",
                left_bounds.len() + 1,
                right_bounds.len() + 1
            );

            let (left_senders, left_receivers) = diff_segment_channels(left_bounds.len() + 1);
            let (right_senders, right_receivers) = diff_segment_channels(right_bounds.len() + 1);

            // Base contexts; each segment task swaps in its own sender.
            let (placeholder_tx, _) = tokio::sync::mpsc::channel(1);
            let left_ctx = core::S3TaskContext::new(
                opt_bucket,
                opt_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style,
                &sdk_config,
                &s3_cfg,
                placeholder_tx.clone(),
                core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                g_state.clone(),
                trace_writer.clone(),
                &cfg.s3.addressing_style.to_string(),
                cfg.s3.profile.as_deref(),
                Some(&cli.delimiter),
                cli.max_keys,
                cfg.s3.start_after.as_deref(),
                cli.continuation_token.as_deref(),
                left_checkpoint.clone(),
            );
            let right_ctx = core::S3TaskContext::new(
                target_bucket,
                target_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style,
                &sdk_config,
                &s3_cfg,
                placeholder_tx,
                core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                g_state.clone(),
                trace_writer.clone(),
                &cfg.s3.addressing_style.to_string(),
                cfg.s3.profile.as_deref(),
                Some(&cli.delimiter),
                cli.max_keys,
                cfg.s3.start_after.as_deref(),
                cli.continuation_token.as_deref(),
                left_checkpoint.clone(),
            );

            let prefix = opt_prefix.clone();
            set.spawn(async move {
                tasks_s3::diff_list_side_task(
                    &left_ctx,
                    &prefix,
                    concurrency,
                    &left_bounds,
                    left_senders,
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
                )
                .await
            });

            let sides = data_map::DiffStreamSides {
                left: left_receivers,
                right: right_receivers,
            };
            let diff_g_state = g_state.clone();
            let diff_ks = filename_ks_for_task.clone();
            let diff_output = filename_output_for_task.clone();
            let diff_output_config = output_config.clone();
            set.spawn(async move {
                data_map::data_map_task_diff_streaming(
                    diff_g_state,
                    sides,
                    &diff_ks,
                    &diff_output,
                    diff_output_config,
                )
                .await
            });
        } else {
            let prefix = opt_prefix.clone();
            let task_ctx = core::S3TaskContext::new(
                opt_bucket,
                opt_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style,
                &sdk_config,
                &s3_cfg,
                tx.expect("list mode allocates the streaming channel"),
                core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE,
                g_state.clone(),
                trace_writer.clone(),
                &cfg.s3.addressing_style.to_string(),
                cfg.s3.profile.as_deref(),
                Some(&cli.delimiter),
                cli.max_keys,
                cfg.s3.start_after.as_deref(),
                cli.continuation_token.as_deref(),
                left_checkpoint.clone(),
            );
            set.spawn(async move {
                tasks_s3::flat_list_main_task(&task_ctx, &prefix, concurrency, hints).await
            });
        }

        // ── Spawn data map task (list modes) ─────────────────
        if is_diff {
            // spawned above alongside the side tasks
        } else if cli.summary_only {
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
                g_state.inc_fatal_error();
                g_state.quit();
            }

            // Save checkpoint progress periodically (every 30s or on state change).
            let progress_metrics = g_state.metrics_snapshot();
            if cli.resume
                && last_checkpoint_save.elapsed().as_secs() >= 30
                && g_state.all_list_tasks_is_running()
                && progress_metrics.fatal_errors == 0
                && progress_metrics.output_errors == 0
            {
                if let Some(ref cp_path) = checkpoint_path_opt {
                    let completed = merged_completed_indices(
                        checkpoint_journal.as_ref(),
                        &left_checkpoint,
                        right_checkpoint.as_ref(),
                    );
                    let journal = checkpoint::CheckpointJournal {
                        bucket: opt_bucket.to_string(),
                        prefix: opt_prefix.clone(),
                        total_segments: original_hints_count,
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
                let final_metrics = g_state.metrics_snapshot();
                if final_metrics.fatal_errors > 0 || final_metrics.output_errors > 0 {
                    info!(
                        "Skipping final checkpoint save because run failed before producing reliable output"
                    );
                } else {
                    let completed = merged_completed_indices(
                        checkpoint_journal.as_ref(),
                        &left_checkpoint,
                        right_checkpoint.as_ref(),
                    );
                    if !completed.is_empty() {
                        let journal = checkpoint::CheckpointJournal {
                            bucket: opt_bucket.to_string(),
                            prefix: opt_prefix.clone(),
                            total_segments: original_hints_count,
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
        }

        // ── Diff mode completion notice ────────────────────
        if mode == RunMode::BiDir {
            info!("diff mode comparison complete — data_map will finalize output");
        }

        info!("All tasks completed.");
    });

    rt.shutdown_background();

    let metrics = g_state.metrics_snapshot();
    let interrupted = interrupted.load(Ordering::SeqCst);
    let exit_code = if interrupted {
        agent::ExitCode::Interrupted
    } else if metrics.output_errors > 0 {
        agent::ExitCode::OutputWrite
    } else if metrics.fatal_errors > 0 {
        agent::ExitCode::NetworkRetryExhausted
    } else {
        agent::ExitCode::Success
    };
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
    let manifest = agent::RunManifest {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: status.to_string(),
        exit_code: exit_code.code(),
        started_at: run_started_at.to_rfc3339(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        elapsed_secs: run_timer.elapsed().as_secs_f64(),
        command: agent::redacted_command_args(),
        inputs: command_input_summary(&cli, &cfg),
        artifacts: agent::collect_artifacts(&manifest_outputs),
        outputs: manifest_outputs,
        config_source,
        metrics: metrics.into(),
        checkpoint: agent::checkpoint_plan(
            cli.resume,
            cli.resume
                .then(|| checkpoint::checkpoint_path(opt_bucket, opt_region)),
            Some(&checkpoint::CheckpointIdentity::new(
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
            )),
        ),
        warnings: run_warnings.clone(),
    };

    if let Some(path) = cli.run_manifest.as_deref() {
        if let Err(e) = agent::write_json_file(path, &manifest) {
            eprintln!("Manifest write error: {}", e);
            std::process::exit(agent::ExitCode::OutputWrite.code());
        }
    }
    if cli.agent {
        println!("{}", agent::to_pretty_json(&manifest));
    } else if exit_code == agent::ExitCode::Success && cli.summary_only {
        print_summary(&manifest.metrics);
    } else if exit_code == agent::ExitCode::Success && list_output_format.writes_stdout_rows() {
        // stdout is reserved for TSV/NDJSON rows.
    } else if exit_code == agent::ExitCode::Success {
        print_wrote_summary(&manifest.outputs);
    }
    if exit_code != agent::ExitCode::Success {
        std::process::exit(exit_code.code());
    }
}

fn generate_completions(shell: Shell) {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
}

fn generate_man_page() {
    let cmd = Cli::command();
    let man = clap_mangen::Man::new(cmd);
    let mut buffer: Vec<u8> = Vec::new();
    if let Err(e) = man.render(&mut buffer) {
        eprintln!("Man page generation error: {}", e);
        std::process::exit(agent::ExitCode::InternalError.code());
    }
    if let Err(e) = std::io::stdout().write_all(&buffer) {
        eprintln!("Man page write error: {}", e);
        std::process::exit(agent::ExitCode::OutputWrite.code());
    }
}

fn build_runtime_or_exit(worker_threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Runtime initialization error: {}", e);
            std::process::exit(agent::ExitCode::InternalError.code());
        })
}

fn run_init_config(profile: Option<&str>, output: &str, overwrite: bool, json: bool) {
    match local_tools::init_config(output, profile, overwrite) {
        Ok(report) => {
            if json {
                println!("{}", agent::to_pretty_json(&report));
            } else {
                print!("{}", local_tools::render_init_config_text(&report));
            }
        }
        Err(e) => {
            eprintln!("Init config failed: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn run_guide(topic: Option<&str>) {
    match local_tools::render_guide(topic) {
        Ok(rendered) => print!("{}", rendered),
        Err(e) => {
            eprintln!("Guide error: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn run_manifest_summary(manifest_file: &str, json: bool, check: bool) {
    match local_tools::manifest_summary(manifest_file, check) {
        Ok(report) => {
            let check_passed = report.check_passed;
            if json {
                println!("{}", agent::to_pretty_json(&report));
            } else {
                print!("{}", local_tools::render_manifest_summary_text(&report));
            }
            if check && !check_passed {
                std::process::exit(agent::ExitCode::DataValidation.code());
            }
        }
        Err(e) => {
            eprintln!("Manifest summary failed: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn apply_output_dir_defaults(cli: &Cli, cfg: &mut S3TurboConfig) {
    let Some(output_dir) = cli.output_dir.as_deref() else {
        return;
    };
    if !list_writes_artifacts(cli) {
        return;
    }

    match &cli.cmd {
        Commands::List { region, bucket, .. } => {
            let stem = output_stem(region.as_deref(), bucket, None, None);
            if cfg.output.parquet_file.is_none() {
                cfg.output.parquet_file = Some(format!("{}/{}.parquet", output_dir, stem));
            }
            if cfg.output.ks_file.is_none() {
                cfg.output.ks_file = Some(format!("{}/{}.ks", output_dir, stem));
            }
        }
        Commands::Diff {
            region,
            bucket,
            target_region,
            target_bucket,
        } => {
            let stem = output_stem(
                region.as_deref(),
                bucket,
                target_region.as_deref(),
                Some(target_bucket.as_str()),
            );
            if cfg.output.parquet_file.is_none() {
                cfg.output.parquet_file = Some(format!("{}/{}.parquet", output_dir, stem));
            }
            if cfg.output.ks_file.is_none() {
                cfg.output.ks_file = Some(format!("{}/{}.ks", output_dir, stem));
            }
        }
        _ => {}
    }
}

fn apply_summary_only_output_defaults(cli: &Cli, cfg: &mut S3TurboConfig) {
    if cli.summary_only {
        cfg.output.parquet_file = None;
        cfg.output.ks_file = None;
    }
}

fn validate_summary_only_command(cli: &Cli) {
    if cli.summary_only && !matches!(cli.cmd, Commands::List { .. }) {
        eprintln!("--summary-only is only supported with the list command");
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
}

fn validate_output_format_command(cli: &Cli) {
    let Some(format) = list_output_format(cli) else {
        return;
    };
    if cli.summary_only && format.writes_stdout_rows() {
        eprintln!("--summary-only cannot be combined with --output-format tsv or ndjson");
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
    if cli.agent && !cli.dry_run && format.writes_stdout_rows() {
        eprintln!(
            "--agent writes the run manifest to stdout and cannot be combined with --output-format tsv or ndjson; use --run-manifest instead"
        );
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
}

fn validate_continuation_token_command(cli: &Cli, cfg: &S3TurboConfig) {
    let Some(token) = cli.continuation_token.as_deref() else {
        return;
    };
    if token.trim().is_empty() {
        eprintln!("--continuation-token cannot be empty");
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
    let Commands::List { region, bucket, .. } = &cli.cmd else {
        eprintln!("--continuation-token is only supported with the list command");
        std::process::exit(agent::ExitCode::CliConfig.code());
    };
    if cli.resume {
        eprintln!("--continuation-token cannot be combined with --resume; use checkpoint resume or a continuation token, not both");
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
    if cfg.s3.start_after.is_some() {
        eprintln!("--continuation-token cannot be combined with --start-after");
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
    if cli.hints_file.is_some() {
        eprintln!(
            "--continuation-token is single-chain only and cannot be combined with --hints-file"
        );
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
    if !cli.no_auto_hints {
        let hints_path = agent::conventional_hints_path(bucket, region.as_deref());
        if std::path::Path::new(&hints_path).exists() {
            eprintln!(
                "--continuation-token is single-chain only, but conventional hints file '{}' exists; pass --no-auto-hints to ignore it",
                hints_path
            );
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn validate_diff_hints_command(cli: &Cli) {
    if !matches!(cli.cmd, Commands::Diff { .. }) {
        return;
    }
    if cli.hints_file.is_some() {
        eprintln!(
            "diff with --hints-file is unsupported by design: diff partitions each side automatically and an explicit shared hints file cannot describe both sides; remove --hints-file to run diff"
        );
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
}

fn validate_diff_resume_command(cli: &Cli) {
    if cli.resume && matches!(cli.cmd, Commands::Diff { .. }) {
        eprintln!(
            "diff --resume is unsupported by design: diff does not checkpoint partial paired comparisons; remove --resume to run diff"
        );
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
}

fn validate_provider_setup_or_exit(cli: &Cli, cfg: &S3TurboConfig) {
    let warnings = provider_setup_guardrail_warnings(cli, cfg);
    if let Some(error) = warnings.first() {
        eprintln!("Provider setup error: {}", error);
        std::process::exit(agent::ExitCode::ProviderSetup.code());
    }
}

fn provider_setup_guardrail_warnings(cli: &Cli, cfg: &S3TurboConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    match &cli.cmd {
        Commands::List { .. } | Commands::Diff { .. } => {
            warnings.extend(profiles::endpoint_profile_guardrail_warnings(cfg));
        }
        Commands::CompatProbe { endpoint_url, .. } => {
            let effective = endpoint_url.as_deref().or(cli.endpoint.as_deref());
            if let Some(endpoint) = effective {
                if profiles::endpoint_url_has_template_placeholder(endpoint) {
                    warnings.push(format!(
                        "endpoint URL '{}' still contains template placeholders; replace values such as <account-id> or <region> before a real run",
                        endpoint
                    ));
                }
            }
        }
        _ => {}
    }
    warnings
}

fn merged_completed_indices(
    checkpoint_journal: Option<&checkpoint::CheckpointJournal>,
    left_checkpoint: &Arc<Mutex<Vec<usize>>>,
    right_checkpoint: Option<&Arc<Mutex<Vec<usize>>>>,
) -> Vec<usize> {
    let mut completed = checkpoint_journal
        .map(|journal| journal.completed_indices.clone())
        .unwrap_or_default();
    completed.extend(left_checkpoint.lock().unwrap().iter().copied());
    if let Some(right_checkpoint) = right_checkpoint {
        completed.extend(right_checkpoint.lock().unwrap().iter().copied());
    }
    completed.sort_unstable();
    completed.dedup();
    completed
}

fn list_output_format(cli: &Cli) -> Option<ListOutputFormat> {
    match &cli.cmd {
        Commands::List { output_format, .. } => Some(*output_format),
        _ => None,
    }
}

fn list_writes_artifacts(cli: &Cli) -> bool {
    if cli.summary_only {
        return false;
    }
    list_output_format(cli)
        .map(ListOutputFormat::writes_artifacts)
        .unwrap_or(true)
}

fn output_stem(
    region: Option<&str>,
    bucket: &str,
    target_region: Option<&str>,
    target_bucket: Option<&str>,
) -> String {
    let now = Local::now().format("%Y%m%d%H%M%S");
    output_stem_with_timestamp(
        region,
        bucket,
        target_region,
        target_bucket,
        &now.to_string(),
    )
}

fn output_stem_with_timestamp(
    region: Option<&str>,
    bucket: &str,
    target_region: Option<&str>,
    target_bucket: Option<&str>,
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
    parts.push(timestamp.to_string());
    parts.join("_")
}

fn sanitize_path_component(value: &str) -> String {
    agent::sanitize_path_component(value)
}

fn ensure_output_dir(cli: &Cli) {
    if cli.dry_run {
        return;
    }
    if let Some(dir) = cli.output_dir.as_deref() {
        if let Err(e) = std::fs::create_dir_all(dir) {
            eprintln!("Output directory error: failed to create '{}': {}", dir, e);
            std::process::exit(agent::ExitCode::OutputWrite.code());
        }
    }
}

fn print_wrote_summary(outputs: &agent::OutputPathSummary) {
    println!("Wrote:");
    if let Some(path) = &outputs.parquet_file {
        println!("  Parquet: {}", path);
    }
    if let Some(path) = &outputs.ks_file {
        println!("  KeySpace: {}", path);
    }
    if let Some(path) = &outputs.hints_file {
        println!("  Hints: {}", path);
    }
    if let Some(path) = &outputs.trace_compat {
        println!("  Trace: {}", path);
    }
    if let Some(path) = &outputs.log_file {
        println!("  Log: {}", path);
    }
}

fn print_summary(metrics: &agent::MetricsSummary) {
    println!("Summary:");
    println!("  objects:  {}", metrics.streamed_rows);
    println!(
        "  bytes:    {} ({})",
        metrics.bytes_total,
        human_bytes(metrics.bytes_total)
    );
    println!("  prefixes: {}", metrics.unique_prefixes);
    if !metrics.top_prefixes.is_empty() {
        println!("Top prefixes:");
        for prefix in metrics.top_prefixes.iter().take(10) {
            println!(
                "  {}  objects={} bytes={} ({})",
                prefix.prefix,
                prefix.objects,
                prefix.bytes,
                human_bytes(prefix.bytes)
            );
        }
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = UNITS[0];
    for next_unit in UNITS.iter().skip(1) {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next_unit;
    }
    if unit == "B" {
        format!("{} {}", bytes, unit)
    } else {
        format!("{:.2} {}", value, unit)
    }
}

fn print_doctor_config(report: &agent::DoctorReport) {
    let config = &report.resolved_config;
    println!("Resolved config:");
    println!(
        "  config:       {}",
        report.config_source.loaded_config.as_deref().unwrap_or("-")
    );
    println!("  threads:      {}", config.runtime.worker_threads);
    println!("  concurrency:  {}", config.runtime.max_concurrency);
    println!(
        "  profile:      {}",
        config.s3.profile.as_deref().unwrap_or("-")
    );
    println!(
        "  endpoint:     {}",
        config.s3.endpoint_url.as_deref().unwrap_or("-")
    );
    println!("  addressing:   {}", config.s3.addressing_style);
    for warning in &report.config_source.warnings {
        println!("  warning:      {}", warning);
    }
}

fn print_doctor_simple(report: &agent::DoctorReport, fix_suggestions: bool) {
    for check in &report.checks {
        let label = match check.status.as_str() {
            "ok" => "OK",
            "warn" => "WARN",
            "error" => "ERROR",
            "skipped" => "SKIP",
            _ => "INFO",
        };
        println!("{} {}: {}", label, check.name, check.message);
    }
    if fix_suggestions {
        print_doctor_suggestions(report);
    }
}

fn print_doctor_suggestions(report: &agent::DoctorReport) {
    for check in &report.checks {
        match check.name.as_str() {
            "aws_profile" if check.status == "warn" => {
                println!("NEXT export AWS_PROFILE=default");
            }
            name if name.ends_with("_parent") && check.status == "error" => {
                if let Some(path) = check
                    .message
                    .strip_prefix("parent directory does not exist: ")
                {
                    println!("NEXT mkdir -p {}", path);
                }
            }
            "endpoint_profile" if check.status == "warn" => {
                println!("NEXT s3-turbo-list guide <provider>");
            }
            _ => {}
        }
    }
}

#[derive(Debug, Serialize)]
struct LocalBenchmarkReport {
    schema_version: &'static str,
    tool_version: &'static str,
    status: String,
    benchmark: String,
    network: String,
    compression: String,
    compression_level: u32,
    output_format: String,
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    producers: usize,
    channel_capacity: usize,
    producer_send_wait_secs: f64,
    elapsed_secs: f64,
    objects_per_sec: f64,
    rows_per_sec: f64,
    parquet_bytes_per_object: f64,
    ks_bytes_per_object: f64,
    text_bytes_per_object: f64,
    output_bytes_per_object: f64,
    parquet_mib_per_sec: f64,
    text_mib_per_sec: f64,
    output_mib_per_sec: f64,
    artifact_dir: Option<String>,
    parquet_file: Option<String>,
    parquet_bytes: u64,
    ks_file: Option<String>,
    ks_bytes: u64,
    text_file: Option<String>,
    text_bytes: u64,
    metrics: agent::MetricsSummary,
    artifacts_kept: bool,
}

fn run_benchmark_local(
    benchmark: LocalBenchmarkKind,
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    producers: usize,
    diff_shape: LocalDiffShape,
    output_format: ListOutputFormat,
    keep_artifacts: bool,
    cfg: &S3TurboConfig,
) -> LocalBenchmarkReport {
    if matches!(
        benchmark,
        LocalBenchmarkKind::DiffMap | LocalBenchmarkKind::DiffOutput
    ) {
        return run_benchmark_local_diff(
            benchmark,
            objects,
            batch_size,
            prefixes,
            diff_shape,
            keep_artifacts,
            cfg,
        );
    }

    let objects = objects.max(1);
    let batch_size = batch_size.max(1);
    let prefixes = prefixes.max(1);
    let producers = producers.max(1);
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let artifact_dir = std::env::temp_dir().join(format!("s3-turbo-list-benchmark-{}", suffix));
    std::fs::create_dir_all(&artifact_dir).unwrap_or_else(|e| {
        eprintln!(
            "Benchmark setup error: failed to create {}: {}",
            artifact_dir.display(),
            e
        );
        std::process::exit(agent::ExitCode::OutputWrite.code());
    });
    let parquet_file = artifact_dir.join("benchmark.parquet");
    let ks_file = artifact_dir.join("benchmark.ks");
    let text_file = match output_format {
        ListOutputFormat::Tsv => Some(artifact_dir.join("benchmark.tsv")),
        ListOutputFormat::Ndjson => Some(artifact_dir.join("benchmark.ndjson")),
        ListOutputFormat::Parquet => None,
    };

    let quit = Arc::new(AtomicBool::new(false));
    let g_state = core::GlobalState::new(quit, 2);
    let started = Instant::now();
    let output_config = cfg.output.clone();
    let parquet_path = parquet_file.display().to_string();
    let ks_path = ks_file.display().to_string();
    let channel_capacity = cfg.channel.capacity;
    let send_wait_nanos = Arc::new(AtomicU64::new(0));

    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);

    rt.block_on(async {
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(core::ObjectKey, core::ObjectProps)>>(
            channel_capacity,
        );
        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        let data_map = match output_format {
            ListOutputFormat::Parquet => {
                let data_map_ks = ks_path.clone();
                let data_map_parquet = parquet_path.clone();
                let data_map_output_config = output_config.clone();
                tokio::spawn(async move {
                    data_map::data_map_task_list_streaming(
                        data_map_ctx,
                        &data_map_ks,
                        &data_map_parquet,
                        data_map_output_config,
                    )
                    .await;
                })
            }
            ListOutputFormat::Tsv | ListOutputFormat::Ndjson => {
                let text_path = text_file
                    .as_ref()
                    .expect("text benchmark output path")
                    .clone();
                tokio::spawn(async move {
                    let file = match tokio::fs::File::create(&text_path).await {
                        Ok(file) => file,
                        Err(e) => {
                            eprintln!(
                                "Benchmark setup error: failed to create {}: {}",
                                text_path.display(),
                                e
                            );
                            data_map_ctx.g_state.inc_output_error();
                            data_map_ctx.g_state.quit();
                            return;
                        }
                    };
                    let writer = tokio::io::BufWriter::new(file);
                    data_map::data_map_task_list_text_writer(
                        data_map_ctx,
                        data_map::ListTextOutputFormat::from(output_format),
                        writer,
                    )
                    .await;
                })
            }
        };

        let producer_state = g_state.clone();
        let send_wait_nanos = Arc::clone(&send_wait_nanos);
        let producer = tokio::spawn(async move {
            producer_state.list_task_start(core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
            producer_state.wait_to_start().await;
            let mut handles = Vec::with_capacity(producers);
            for producer_index in 0..producers {
                let tx = tx.clone();
                let send_wait_nanos = Arc::clone(&send_wait_nanos);
                let start = objects.saturating_mul(producer_index) / producers;
                let end = objects.saturating_mul(producer_index + 1) / producers;
                handles.push(tokio::spawn(async move {
                    let mut sent = start;
                    while sent < end {
                        let take = (end - sent).min(batch_size);
                        let mut batch = Vec::with_capacity(take);
                        for offset in 0..take {
                            let index = sent + offset;
                            let prefix_index = index % prefixes;
                            let key_text =
                                format!("prefix-{}/object-{:012}.dat", prefix_index, index);
                            let key = core::ObjectKey::from(key_text.as_str());
                            let mut etag = [0u8; 16];
                            etag[..8].copy_from_slice(&(index as u64).to_le_bytes());
                            etag[8..].copy_from_slice(&(prefix_index as u64).to_le_bytes());
                            let props = core::ObjectProps::new_open(
                                core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE,
                                1024 + (index % 4096) as u64,
                                etag,
                            );
                            batch.push((key, props));
                        }
                        let send_started = Instant::now();
                        if tx.send(batch).await.is_err() {
                            break;
                        }
                        let waited = send_started.elapsed().as_nanos().min(u128::from(u64::MAX));
                        send_wait_nanos.fetch_add(waited as u64, Ordering::Relaxed);
                        sent += take;
                    }
                }));
            }
            drop(tx);
            for handle in handles {
                if let Err(e) = handle.await {
                    eprintln!("Benchmark producer worker failed: {}", e);
                    producer_state.inc_fatal_error();
                }
            }
            producer_state.list_task_complete(core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
        });

        if let Err(e) = producer.await {
            eprintln!("Benchmark producer task failed: {}", e);
            g_state.inc_fatal_error();
        }
        if let Err(e) = data_map.await {
            eprintln!("Benchmark data-map task failed: {}", e);
            g_state.inc_fatal_error();
        }
    });
    rt.shutdown_background();

    let elapsed_secs = started.elapsed().as_secs_f64().max(0.001);
    let producer_send_wait_secs = send_wait_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
    let parquet_bytes = std::fs::metadata(&parquet_file)
        .map(|m| m.len())
        .unwrap_or(0);
    let ks_bytes = std::fs::metadata(&ks_file).map(|m| m.len()).unwrap_or(0);
    let text_bytes = text_file
        .as_ref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|m| m.len())
        .unwrap_or(0);
    let output_bytes = parquet_bytes
        .saturating_add(ks_bytes)
        .saturating_add(text_bytes);
    let metrics: agent::MetricsSummary = g_state.metrics_snapshot().into();
    let artifacts_kept = keep_artifacts;
    let artifact_dir_summary = artifacts_kept.then(|| artifact_dir.display().to_string());
    if !artifacts_kept {
        let _ = std::fs::remove_dir_all(&artifact_dir);
    }

    LocalBenchmarkReport {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: if g_state.read_fatal_error() == 0 {
            "ok".to_string()
        } else {
            "error".to_string()
        },
        benchmark: LocalBenchmarkKind::ListOutput.to_string(),
        network: "none: synthetic local data only".to_string(),
        compression: output_config.compression.clone(),
        compression_level: output_config.compression_level,
        output_format: output_format.to_string(),
        objects,
        batch_size,
        prefixes,
        producers,
        channel_capacity,
        producer_send_wait_secs,
        elapsed_secs,
        objects_per_sec: objects as f64 / elapsed_secs,
        rows_per_sec: metrics.streamed_rows as f64 / elapsed_secs,
        parquet_bytes_per_object: parquet_bytes as f64 / objects as f64,
        ks_bytes_per_object: ks_bytes as f64 / objects as f64,
        text_bytes_per_object: text_bytes as f64 / objects as f64,
        output_bytes_per_object: output_bytes as f64 / objects as f64,
        parquet_mib_per_sec: parquet_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        text_mib_per_sec: text_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        output_mib_per_sec: output_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        artifact_dir: artifact_dir_summary,
        parquet_file: (output_format == ListOutputFormat::Parquet).then_some(parquet_path),
        parquet_bytes,
        ks_file: (output_format == ListOutputFormat::Parquet).then_some(ks_path),
        ks_bytes,
        text_file: text_file.as_ref().map(|path| path.display().to_string()),
        text_bytes,
        metrics,
        artifacts_kept,
    }
}

fn run_benchmark_local_diff(
    benchmark: LocalBenchmarkKind,
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    diff_shape: LocalDiffShape,
    keep_artifacts: bool,
    cfg: &S3TurboConfig,
) -> LocalBenchmarkReport {
    let objects = objects.max(1);
    let batch_size = batch_size.max(1);
    let prefixes = prefixes.max(1);
    let started = Instant::now();

    // DiffOutput writes real Parquet/KS artifacts; DiffMap measures the
    // merge + row encoding against a null writer (no file IO).
    let mut artifact_dir_summary = None;
    let (artifact_dir, parquet_path, ks_path) = if benchmark == LocalBenchmarkKind::DiffOutput {
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let dir = std::env::temp_dir().join(format!("s3-turbo-list-diff-benchmark-{}", suffix));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| {
            eprintln!(
                "Benchmark setup error: failed to create {}: {}",
                dir.display(),
                e
            );
            std::process::exit(agent::ExitCode::OutputWrite.code());
        });
        let parquet = dir.join("diff.parquet");
        let ks = dir.join("diff.ks");
        (Some(dir), Some(parquet), Some(ks))
    } else {
        (None, None, None)
    };

    let output_config = cfg.output.clone();
    let channel_capacity = cfg.channel.capacity;
    let mut parquet_rows = 0usize;
    let mut ks_entries = 0usize;

    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);
    let outcome = rt.block_on(async {
        let (left_tx, left_rx) = tokio::sync::mpsc::channel::<
            Vec<(core::ObjectKey, core::ObjectProps)>,
        >(channel_capacity);
        let (right_tx, right_rx) = tokio::sync::mpsc::channel::<
            Vec<(core::ObjectKey, core::ObjectProps)>,
        >(channel_capacity);

        let producer = tokio::spawn(async move {
            let mut sent = 0usize;
            while sent < objects {
                let take = (objects - sent).min(batch_size);
                let (left, right) =
                    synthetic_diff_batches(sent, take, prefixes, objects, benchmark, diff_shape);
                if !left.is_empty() && left_tx.send(left).await.is_err() {
                    return;
                }
                if !right.is_empty() && right_tx.send(right).await.is_err() {
                    return;
                }
                sent += take;
            }
        });

        let sides = data_map::DiffStreamSides {
            left: vec![left_rx],
            right: vec![right_rx],
        };
        let merged: Result<data_map::DiffMergeOutcome, String> =
            if let (Some(parquet_path), Some(ks_path)) = (&parquet_path, &ks_path) {
                let output = tokio::fs::File::create(parquet_path)
                    .await
                    .map_err(|e| format!("failed to create {}: {}", parquet_path.display(), e))?;
                let writer = tokio::io::BufWriter::with_capacity(100 * 1_048_576, output);
                let ks = ks_path.display().to_string();
                let mut parquet = s3_turbo_list::utils::AsyncParquetOutput::new_with_options(
                    writer,
                    &ks,
                    output_config.row_group_size,
                    &output_config.compression,
                    output_config.compression_level,
                );
                let outcome = data_map::run_diff_merge(sides, &mut parquet).await?;
                parquet_rows = parquet.total_rows();
                ks_entries = outcome.write_ks(&ks).await?;
                parquet.close().await?;
                Ok(outcome)
            } else {
                let mut parquet = s3_turbo_list::utils::AsyncParquetOutput::new_with_options(
                    tokio::io::sink(),
                    "",
                    output_config.row_group_size,
                    &output_config.compression,
                    output_config.compression_level,
                );
                let outcome = data_map::run_diff_merge(sides, &mut parquet).await?;
                parquet_rows = parquet.total_rows();
                Ok(outcome)
            };
        let _ = producer.await;
        merged
    });
    rt.shutdown_background();

    let outcome = outcome.unwrap_or_else(|e| {
        eprintln!("Benchmark output error: {}", e);
        std::process::exit(agent::ExitCode::OutputWrite.code());
    });

    let mut parquet_bytes = 0u64;
    let mut ks_bytes = 0u64;
    let mut parquet_file = None;
    let mut ks_file = None;
    if let (Some(dir), Some(parquet_path), Some(ks_path)) = (&artifact_dir, &parquet_path, &ks_path)
    {
        parquet_bytes = std::fs::metadata(parquet_path)
            .map(|m| m.len())
            .unwrap_or(0);
        ks_bytes = std::fs::metadata(ks_path).map(|m| m.len()).unwrap_or(0);
        parquet_file = Some(parquet_path.display().to_string());
        ks_file = Some(ks_path.display().to_string());
        if keep_artifacts {
            artifact_dir_summary = Some(dir.display().to_string());
        } else {
            let _ = std::fs::remove_dir_all(dir);
            parquet_file = None;
            ks_file = None;
        }
    }

    let elapsed_secs = started.elapsed().as_secs_f64().max(0.001);
    let output_bytes = parquet_bytes.saturating_add(ks_bytes);
    let metrics = agent::MetricsSummary {
        fatal_errors: 0,
        output_errors: 0,
        stream_timeouts: 0,
        s3_client_timeouts: 0,
        s3_client_generic_errors: 0,
        received_batches: outcome.received_batches,
        received_objects: outcome.received_objects,
        streamed_rows: outcome.rows,
        unique_prefixes: outcome.unique_prefixes(),
        parquet_rows,
        ks_entries,
        bytes_total: outcome.bytes_total,
        top_prefixes: Vec::new(),
        summary_only: false,
    };

    LocalBenchmarkReport {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: "ok".to_string(),
        benchmark: benchmark.to_string(),
        network: "none: synthetic local data only".to_string(),
        compression: cfg.output.compression.clone(),
        compression_level: cfg.output.compression_level,
        output_format: benchmark.to_string(),
        objects,
        batch_size,
        prefixes,
        producers: 1,
        channel_capacity: cfg.channel.capacity,
        producer_send_wait_secs: 0.0,
        elapsed_secs,
        objects_per_sec: outcome.received_objects as f64 / elapsed_secs,
        rows_per_sec: outcome.rows as f64 / elapsed_secs,
        parquet_bytes_per_object: parquet_bytes as f64 / objects as f64,
        ks_bytes_per_object: ks_bytes as f64 / objects as f64,
        text_bytes_per_object: 0.0,
        output_bytes_per_object: output_bytes as f64 / objects as f64,
        parquet_mib_per_sec: parquet_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        text_mib_per_sec: 0.0,
        output_mib_per_sec: output_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        artifact_dir: artifact_dir_summary,
        parquet_file,
        parquet_bytes,
        ks_file,
        ks_bytes,
        text_file: None,
        text_bytes: 0,
        metrics,
        artifacts_kept: keep_artifacts,
    }
}

type SyntheticObjectBatch = Vec<(core::ObjectKey, core::ObjectProps)>;
type SyntheticDiffBatches = (SyntheticObjectBatch, SyntheticObjectBatch);

fn synthetic_diff_batches(
    start: usize,
    take: usize,
    prefixes: usize,
    total_objects: usize,
    benchmark: LocalBenchmarkKind,
    diff_shape: LocalDiffShape,
) -> SyntheticDiffBatches {
    let mut left = Vec::with_capacity(take);
    let mut right = Vec::with_capacity(take);
    for offset in 0..take {
        let index = start + offset;
        // Block-partitioned, zero-padded prefixes keep the synthetic key
        // stream in S3 lexicographic order, as the diff merge requires.
        let prefix_index = (index * prefixes) / total_objects.max(1);
        let key_text = format!("prefix-{:06}/object-{:012}.dat", prefix_index, index);
        let key = core::ObjectKey::from(key_text.as_str());
        let mut etag = [0u8; 16];
        etag[..8].copy_from_slice(&(index as u64).to_le_bytes());
        etag[8..].copy_from_slice(&(prefix_index as u64).to_le_bytes());
        etag[15] = etag[15].max(1);
        let size = 1024 + (index % 4096) as u64;
        if benchmark == LocalBenchmarkKind::DiffMap {
            left.push((
                key.clone(),
                core::ObjectProps::new_open(core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, size, etag),
            ));
            right.push((
                key,
                core::ObjectProps::new_open(core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE, size, etag),
            ));
            continue;
        }

        match diff_shape {
            LocalDiffShape::AllEqual => {
                left.push((
                    key.clone(),
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                        size,
                        etag,
                    ),
                ));
                right.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                        size,
                        etag,
                    ),
                ));
            }
            LocalDiffShape::AllChanged => {
                left.push((
                    key.clone(),
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                        size,
                        etag,
                    ),
                ));
                etag[0] = etag[0].wrapping_add(1);
                right.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                        size + 1,
                        etag,
                    ),
                ));
            }
            LocalDiffShape::Mixed => match index % 4 {
                0 => {
                    left.push((
                        key.clone(),
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                            size,
                            etag,
                        ),
                    ));
                    right.push((
                        key,
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                            size,
                            etag,
                        ),
                    ));
                }
                1 => left.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                        size,
                        etag,
                    ),
                )),
                2 => right.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                        size,
                        etag,
                    ),
                )),
                _ => {
                    left.push((
                        key.clone(),
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                            size,
                            etag,
                        ),
                    ));
                    etag[0] = etag[0].wrapping_add(1);
                    right.push((
                        key,
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                            size + 1,
                            etag,
                        ),
                    ));
                }
            },
        }
    }
    (left, right)
}

fn build_plan_report(
    cli: &Cli,
    cfg: &S3TurboConfig,
    config_source: agent::ConfigSourceSummary,
) -> agent::PlanReport {
    let (planned_ks, planned_parquet, planned_hints) = planned_output_paths(cli, cfg);
    let outputs =
        runtime_output_summary(cli, cfg, planned_ks.as_deref(), planned_parquet.as_deref())
            .with_hints(planned_hints);
    let inputs = command_input_summary(cli, cfg);
    let checkpoint_path = inputs
        .bucket
        .as_deref()
        .filter(|_| cli.resume)
        .map(|bucket| checkpoint::checkpoint_path(bucket, inputs.region.as_deref()));
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
        )
    });
    let hints = if inputs.mode == "diff" {
        agent::diff_single_segment_hints_plan(inputs.bucket.as_deref(), inputs.region.as_deref())
    } else {
        agent::detect_hints_plan(
            cli.hints_file.as_deref(),
            inputs.bucket.as_deref(),
            inputs.region.as_deref(),
            cli.no_auto_hints,
        )
    };
    let file_conflicts = agent::output_conflicts(&outputs);
    let mut warnings = config_source.warnings.clone();
    warnings.extend(runtime_guardrail_warnings(cli, cfg));
    if matches!(cli.cmd, Commands::CompatProbe { .. }) {
        warnings.push(
            "compat-probe will contact the configured endpoint when not run with --dry-run"
                .to_string(),
        );
    }
    if inputs.mode == "list"
        && matches!(
            hints.source.as_str(),
            "single_segment_fallback" | "disabled_single_segment_fallback"
        )
    {
        warnings.push(
            "list is planned as a single ListObjectsV2 chain; --concurrency only improves throughput when hints provide multiple key-space segments"
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

    agent::PlanReport {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: "ok".to_string(),
        command: agent::redacted_command_args(),
        network: "none: dry-run only resolves local configuration and planned paths".to_string(),
        inputs,
        outputs,
        config_source,
        resolved_config: cfg.into(),
        hints,
        checkpoint: agent::checkpoint_plan(cli.resume, checkpoint_path, current_identity.as_ref()),
        file_conflicts,
        warnings,
    }
}

fn cli_config_overrides(cli: &Cli) -> Vec<String> {
    let mut overrides = Vec::new();
    if cli.threads.is_some() {
        overrides.push("threads".to_string());
    }
    if cli.concurrency.is_some() {
        overrides.push("concurrency".to_string());
    }
    if cli.endpoint.is_some() {
        overrides.push("endpoint_url".to_string());
    }
    if cli.addressing_style.is_some() {
        overrides.push("addressing_style".to_string());
    }
    if cli.profile.is_some() {
        overrides.push("profile".to_string());
    }
    if cli.debug_s3 {
        overrides.push("debug_s3".to_string());
    }
    if cli.trace_compat.is_some() {
        overrides.push("trace_compat".to_string());
    }
    if cli.start_after.is_some() {
        overrides.push("start_after".to_string());
    }
    if cli.output_log_file.is_some() {
        overrides.push("output_log_file".to_string());
    }
    if cli.output_ks_file.is_some() {
        overrides.push("output_ks_file".to_string());
    }
    if cli.output_parquet_file.is_some() {
        overrides.push("output_parquet_file".to_string());
    }
    if cli.compression.is_some() {
        overrides.push("compression".to_string());
    }
    if cli.compression_level.is_some() {
        overrides.push("compression_level".to_string());
    }
    overrides
}

fn runtime_guardrail_warnings(cli: &Cli, cfg: &S3TurboConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    if matches!(
        cli.cmd,
        Commands::List { .. } | Commands::Diff { .. } | Commands::CompatProbe { .. }
    ) {
        if let Some(profile) = cfg
            .s3
            .profile
            .as_deref()
            .filter(|name| profiles::is_endpoint_preset_name(name))
        {
            if !credential_environment_signal_present() {
                warnings.push(format!(
                    "--profile '{}' is an endpoint compatibility preset only; credentials still come from AWS_PROFILE or the AWS SDK default credential chain. If '{}' is also your credentials profile name, set AWS_PROFILE={}.",
                    profile, profile, profile
                ));
            }
        }
        warnings.extend(provider_setup_guardrail_warnings(cli, cfg));
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

fn credential_environment_signal_present() -> bool {
    [
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    ]
    .iter()
    .any(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .is_some()
    })
}

fn print_runtime_warnings(warnings: &[String]) {
    for warning in warnings {
        eprintln!("WARN {}", warning);
    }
}

trait OutputPathSummaryExt {
    fn with_hints(self, hints_file: Option<String>) -> Self;
}

impl OutputPathSummaryExt for agent::OutputPathSummary {
    fn with_hints(mut self, hints_file: Option<String>) -> Self {
        self.hints_file = hints_file;
        self
    }
}

fn command_input_summary(cli: &Cli, cfg: &S3TurboConfig) -> agent::CommandInputSummary {
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
            Some(region.clone()),
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
        Commands::BenchmarkLocal { .. } => {
            ("benchmark-local".to_string(), None, None, None, None, None)
        }
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
        continuation_token: cli.continuation_token.clone(),
        profile: cfg.s3.profile.clone(),
        addressing_style: cfg.s3.addressing_style.to_string(),
    }
}

fn runtime_output_summary(
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

    let hints_file = None;
    let compat_output = match &cli.cmd {
        Commands::CompatProbe { output, .. } => output.clone(),
        _ => None,
    };
    agent::OutputPathSummary {
        parquet_file: parquet_file.map(str::to_string).or(compat_output),
        ks_file: ks_file.map(str::to_string),
        hints_file,
        trace_compat: cfg.s3.trace_compat.clone(),
        log_file: cfg.output.log_file.clone(),
    }
}

fn planned_output_paths(
    cli: &Cli,
    cfg: &S3TurboConfig,
) -> (Option<String>, Option<String>, Option<String>) {
    if !list_writes_artifacts(cli) {
        return (None, None, None);
    }

    let now = Local::now().format("%Y%m%d%H%M%S").to_string();
    match &cli.cmd {
        Commands::List { region, bucket, .. } => {
            let stem = output_stem_with_timestamp(region.as_deref(), bucket, None, None, &now);
            let ks = cfg
                .output
                .ks_file
                .clone()
                .unwrap_or_else(|| format!("{}.ks", stem));
            let parquet = cfg
                .output
                .parquet_file
                .clone()
                .unwrap_or_else(|| format!("{}.parquet", stem));
            (Some(ks), Some(parquet), None)
        }
        Commands::Diff {
            region,
            bucket,
            target_region,
            target_bucket,
        } => {
            let stem = output_stem_with_timestamp(
                region.as_deref(),
                bucket,
                target_region.as_deref(),
                Some(target_bucket),
                &now,
            );
            let ks = cfg
                .output
                .ks_file
                .clone()
                .unwrap_or_else(|| format!("{}.ks", stem));
            let parquet = cfg
                .output
                .parquet_file
                .clone()
                .unwrap_or_else(|| format!("{}.parquet", stem));
            (Some(ks), Some(parquet), None)
        }
        _ => (None, None, None),
    }
}

// ── Unified hints loader ───────────────────────────────────

/// Top-level hints loader: resolves hints from explicit file, the conventional
/// startup-discovery cache, or falls back to empty (single-segment).
///
/// Priority:
/// 1. `hints_file` (from `--hints-file` CLI flag) — always used first.
/// 2. Auto-hints cache at `{region}_{bucket}_hints.toml` in CWD.
/// 3. Single-segment fallback (empty vec).
type SegmentBatchSender = tokio::sync::mpsc::Sender<Vec<(core::ObjectKey, core::ObjectProps)>>;
type SegmentBatchReceiver = tokio::sync::mpsc::Receiver<Vec<(core::ObjectKey, core::ObjectProps)>>;

/// One small channel per diff segment; the capacity is the per-segment
/// prefetch window, keeping memory bounded while segments list in parallel.
fn diff_segment_channels(segments: usize) -> (Vec<SegmentBatchSender>, Vec<SegmentBatchReceiver>) {
    (0..segments)
        .map(|_| tokio::sync::mpsc::channel(tasks_s3::DIFF_SEGMENT_CHANNEL_CAP))
        .unzip()
}

/// Key-space boundaries for one diff side: cached hints when present,
/// otherwise startup structural discovery (cached for future runs). The
/// same automatic sources as list mode; explicit --hints-file remains
/// rejected for diff. Empty means single-segment, the pre-parallel
/// behavior.
async fn diff_side_boundaries(
    bucket: &str,
    region: Option<&str>,
    prefix: &str,
    cfg: &S3TurboConfig,
    cli: &Cli,
    sdk_config: &aws_config::SdkConfig,
) -> Vec<String> {
    if cli.no_auto_hints
        || !cli.delimiter.is_empty()
        || cfg.s3.profile.as_deref() == Some("bos")
        || cfg.s3.start_after.is_some()
    {
        return Vec::new();
    }

    let cache_path = agent::conventional_hints_path(bucket, region);
    if let Ok(boundaries) = hints::parse_hints_file(&cache_path) {
        return boundaries;
    }

    let client = core::build_s3_client(
        sdk_config,
        region,
        cfg.s3.endpoint_url.as_deref(),
        cfg.s3.force_path_style,
    );
    let target = cfg.runtime.max_concurrency.saturating_mul(2).clamp(16, 512);
    let boundaries = auto_hints::discover_startup_boundaries(&client, bucket, prefix, target).await;
    let boundaries = if boundaries.is_empty() {
        // Flat namespace: structural discovery found no CommonPrefixes, so the
        // side would otherwise list as one serial segment. Bisect the key range
        // with single-key probes so it lists in parallel. The target is smaller
        // than structural discovery's: each cut is a one-time up-front probe,
        // and only `max_concurrency` segments run at once, so spare boundaries
        // beyond that would just cost probes without adding parallelism.
        let flat_target = cfg.runtime.max_concurrency.clamp(8, 64);
        let probe_bucket = bucket.to_string();
        let probe_prefix = prefix.to_string();
        auto_hints::discover_flat_boundaries(prefix, flat_target, |start_after| {
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
                let resp = req.send().await.map_err(|e| format!("{:?}", e))?;
                Ok(resp
                    .contents()
                    .first()
                    .and_then(|o| o.key())
                    .map(str::to_string))
            }
        })
        .await
    } else {
        boundaries
    };
    if !boundaries.is_empty() {
        if let Err(e) = auto_hints::write_startup_hints_cache(bucket, region, prefix, &boundaries) {
            log::warn!("{}", e);
        }
    }
    boundaries
}

// The region supplied by the active subcommand, used for profile endpoint
// templating before the full command dispatch.
fn command_region(cmd: &Commands) -> Option<&str> {
    match cmd {
        Commands::List { region, .. } | Commands::Diff { region, .. } => region.as_deref(),
        Commands::CompatProbe { region, .. } => Some(region.as_str()),
        _ => None,
    }
}

fn load_hints(
    hints_file: Option<&str>,
    bucket: &str,
    region: Option<&str>,
    profile: Option<&str>,
    no_auto_hints: bool,
    disabled_for_diff: bool,
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
                std::process::exit(agent::ExitCode::CliConfig.code());
            }
        }
    }

    if no_auto_hints {
        if disabled_for_diff {
            info!(
                "diff partitions each side independently; per-side hints/discovery are resolved later."
            );
        } else {
            info!(
                "--no-auto-hints set. Skipping conventional hints cache lookup and using single-segment fallback."
            );
        }
        return vec![];
    }

    // 2. Try the startup-discovery cache at the conventional path.
    let cache_filename = if let Some(r) = region {
        agent::conventional_hints_path(bucket, Some(r))
    } else {
        agent::conventional_hints_path(bucket, None)
    };

    if let Ok(boundaries) = hints::parse_hints_file(&cache_filename) {
        hints::warn_bos_hinted_segments(profile, &boundaries);
        return boundaries;
    }

    // 3. No hints — the caller may attempt startup structural discovery
    //    before falling back to a single segment.
    info!(
        "No hints file or cached hints found for bucket '{}'",
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
    let rt = build_runtime_or_exit(2);

    rt.block_on(async {
        if let Err(e) = compat_probe::run_compat_probe(
            endpoint_url,
            region,
            bucket,
            addressing_style,
            output,
            cfg,
        )
        .await
        {
            eprintln!("Compat-probe output error: {}", e);
            std::process::exit(agent::ExitCode::OutputWrite.code());
        }
    });
}

/// Render the hints-file validation section in human doctor output. Doctor
/// absorbed the former hints-validate command; the formatting matches its old
/// `Hints file:` block.
fn print_doctor_hints(report: &hints::HintsValidationReport) {
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
            "  Generated at:    {}",
            metadata.generated_at.as_deref().unwrap_or("-")
        );
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
