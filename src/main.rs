// All modules exported from the library crate (src/lib.rs).
// The binary uses `s3_turbo_list::...` paths to avoid module duplication.
#![allow(
    clippy::borrowed_box,
    clippy::if_same_then_else,
    clippy::too_many_arguments
)]

use s3_turbo_list::{
    agent, auto_hints, checkpoint, config, core, data_map, diff, hints, local_tools, mon, profiles,
    tasks_s3, trace,
};

use chrono::Local;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use config::S3TurboConfig;
use core::RunMode;
use log::{error, info};
use serde::Serialize;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
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

    /// Directory for default Parquet and KeySpace outputs
    #[arg(long, global = true)]
    output_dir: Option<String>,

    /// Resume from checkpoint
    #[arg(long, global = true)]
    resume: bool,

    /// Disable auto-hints (forces manual hints or single-threaded)
    #[arg(long, global = true)]
    no_auto_hints: bool,

    // ── S3-compatible observability flags ─────────────────
    /// Delimiter for ListObjectsV2; default "/" lists top-level keys, use --delimiter '' for recursive full-bucket listing
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

    /// Auto-discover KeySpace hints via a sequential object scan
    AutoHints {
        /// AWS region
        #[arg(long)]
        region: Option<String>,

        /// Bucket to analyze
        #[arg(long)]
        bucket: String,

        /// Output hints file path; content is always TOML regardless of extension
        #[arg(short, long)]
        output: Option<String>,

        /// Stop after scanning this many objects; default scans the full bucket
        #[arg(long)]
        sample_limit: Option<usize>,

        /// Stop after scanning this many ListObjectsV2 pages; default scans all pages
        #[arg(long)]
        max_pages: Option<usize>,
    },

    /// Discover delimiter CommonPrefixes and write prefix hints
    DiscoverPrefixes {
        /// AWS region
        #[arg(long)]
        region: Option<String>,

        /// Bucket to inspect
        #[arg(long)]
        bucket: String,

        /// Output prefixes file; plain text by default
        #[arg(short, long)]
        output: Option<String>,

        /// Stop after scanning this many ListObjectsV2 pages; default scans all pages
        #[arg(long)]
        max_pages: Option<usize>,

        /// Write a TOML report instead of one prefix per line
        #[arg(long)]
        toml: bool,
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

    /// Merge local TOML/plain hints files without contacting S3
    HintsMerge {
        /// Input hints files
        #[arg(required = true)]
        inputs: Vec<String>,

        /// Output hints file path; content is always TOML regardless of extension
        #[arg(short, long)]
        output: Option<String>,

        /// Output format for the command report
        #[arg(long, value_enum, default_value_t = ReportFormat::Text)]
        output_format: ReportFormat,

        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Emit JSON report with stdout reserved for machine-readable output
        #[arg(long)]
        machine_readable: bool,

        /// Write local tooling manifest JSON to this path
        #[arg(long)]
        emit_manifest: Option<String>,

        /// Allow replacing an existing output hints file
        #[arg(long)]
        overwrite: bool,
    },

    /// Summarize a local --trace-compat JSONL file without contacting S3
    TraceSummary {
        /// Trace JSONL file
        trace_file: String,

        /// Output format for the report
        #[arg(long, value_enum, default_value_t = ReportFormat::Text)]
        output_format: ReportFormat,

        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Emit JSON report with stdout reserved for machine-readable output
        #[arg(long)]
        machine_readable: bool,
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

    /// Generate conservative next-run hints from a local trace and hints file
    HintsRebalance {
        /// Trace JSONL file
        #[arg(long)]
        trace: String,

        /// Existing hints file
        #[arg(short = 'H', long)]
        hints_file: String,

        /// Output hints file path; content is always TOML regardless of extension
        #[arg(short, long)]
        output: Option<String>,

        /// Maximum number of new boundaries to add
        #[arg(long, default_value_t = 8)]
        max_new_boundaries: usize,

        /// Segment page ratio over median required before adding a boundary
        #[arg(long, default_value_t = 5.0)]
        long_tail_ratio: f64,

        /// Minimum segment page count before considering a split
        #[arg(long, default_value_t = 5)]
        min_pages: u32,

        /// Explain long-tail decisions in human output
        #[arg(long)]
        explain: bool,

        /// Output format for the command report
        #[arg(long, value_enum, default_value_t = ReportFormat::Text)]
        output_format: ReportFormat,

        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Emit JSON report with stdout reserved for machine-readable output
        #[arg(long)]
        machine_readable: bool,

        /// Write local tooling manifest JSON to this path
        #[arg(long)]
        emit_manifest: Option<String>,

        /// Allow replacing an existing output hints file
        #[arg(long)]
        overwrite: bool,
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

    /// Print concise command recipes without contacting S3
    Recipes {
        /// Recipe name, for example aws-basic, large-bucket, local-minio, or agent-safe
        name: Option<String>,
    },

    /// Print a compact command cheatsheet without contacting S3
    Cheatsheet,

    /// Print provider-specific first-run steps without contacting S3
    Quickstart {
        /// Provider name: aws, minio, r2, or bos
        provider: String,
    },

    /// Inspect resolved local config without contacting S3
    ConfigInspect {
        /// Emit JSON report
        #[arg(long)]
        json: bool,
    },

    /// Run local environment checks without contacting S3
    Doctor {
        /// Do not contact S3 endpoints
        #[arg(long, default_value_t = true)]
        local_only: bool,

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

    /// Show endpoint compatibility profiles without contacting S3
    Profiles {
        #[command(subcommand)]
        cmd: ProfileCommands,
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
    BenchmarkLocal {
        /// Number of synthetic objects to write
        #[arg(long, default_value_t = 10_000)]
        objects: usize,

        /// Objects per synthetic batch
        #[arg(long, default_value_t = 1_000)]
        batch_size: usize,

        /// Number of distinct key prefixes
        #[arg(long, default_value_t = 128)]
        prefixes: usize,

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
enum ReportFormat {
    Text,
    Json,
    Markdown,
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

#[derive(Subcommand)]
enum ProfileCommands {
    /// List known endpoint profiles
    List {
        /// Emit JSON report
        #[arg(long)]
        json: bool,
    },

    /// Show one endpoint profile
    Show {
        /// Profile name, for example aws, minio, bos, r2, b2, or oss
        name: String,

        /// Emit JSON report
        #[arg(long)]
        json: bool,
    },
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
        Commands::Profiles { cmd } => {
            run_profiles(cmd);
            return;
        }
        Commands::HintsMerge {
            inputs,
            output,
            output_format,
            json,
            machine_readable,
            emit_manifest,
            overwrite,
        } => {
            run_hints_merge(
                inputs,
                output.as_deref(),
                cli.dry_run,
                *overwrite,
                *output_format,
                *json || *machine_readable || cli.agent,
                emit_manifest.as_deref(),
            );
            return;
        }
        Commands::TraceSummary {
            trace_file,
            output_format,
            json,
            machine_readable,
        } => {
            run_trace_summary(
                trace_file,
                *output_format,
                *json || *machine_readable || cli.agent,
            );
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
        Commands::HintsRebalance {
            trace,
            hints_file,
            output,
            max_new_boundaries,
            long_tail_ratio,
            min_pages,
            explain,
            output_format,
            json,
            machine_readable,
            emit_manifest,
            overwrite,
        } => {
            run_hints_rebalance(
                trace,
                hints_file,
                output.as_deref(),
                cli.dry_run,
                *overwrite,
                *max_new_boundaries,
                *long_tail_ratio,
                *min_pages,
                *explain,
                *output_format,
                *json || *machine_readable || cli.agent,
                emit_manifest.as_deref(),
            );
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
        Commands::Recipes { name } => {
            run_recipes(name.as_deref());
            return;
        }
        Commands::Cheatsheet => {
            print!("{}", local_tools::render_cheatsheet());
            return;
        }
        Commands::Quickstart { provider } => {
            run_quickstart(provider);
            return;
        }
        _ => {}
    }

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
        std::process::exit(agent::ExitCode::CliConfig.code());
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
    apply_output_dir_defaults(&cli, &mut cfg);
    apply_summary_only_output_defaults(&cli, &mut cfg);
    validate_summary_only_command(&cli);
    validate_output_format_command(&cli);
    validate_continuation_token_command(&cli, &cfg);
    validate_diff_hints_command(&cli);
    validate_diff_resume_command(&cli);

    match &cli.cmd {
        Commands::ConfigInspect { json } => {
            let report = agent::config_inspect_report(&cfg);
            if *json || cli.agent {
                println!("{}", agent::to_pretty_json(&report));
            } else {
                println!("s3-turbo-list {}", env!("CARGO_PKG_VERSION"));
                println!(
                    "  threads:      {}",
                    report.resolved_config.runtime.worker_threads
                );
                println!(
                    "  concurrency:  {}",
                    report.resolved_config.runtime.max_concurrency
                );
                println!(
                    "  profile:      {}",
                    report.resolved_config.s3.profile.as_deref().unwrap_or("-")
                );
                println!(
                    "  endpoint:     {}",
                    report
                        .resolved_config
                        .s3
                        .endpoint_url
                        .as_deref()
                        .unwrap_or("-")
                );
                println!(
                    "  addressing:   {}",
                    report.resolved_config.s3.addressing_style
                );
            }
            return;
        }
        Commands::Doctor {
            local_only,
            json,
            simple,
            fix_suggestions,
        } => {
            let report = agent::doctor_report(*local_only, &cfg);
            if *json || cli.agent {
                println!("{}", agent::to_pretty_json(&report));
            } else if *simple {
                print_doctor_simple(&report, *fix_suggestions);
            } else {
                println!("Doctor status: {}", report.status);
                for check in &report.checks {
                    println!("  {}: {} — {}", check.name, check.status, check.message);
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
            objects,
            batch_size,
            prefixes,
            json,
            output,
            keep_artifacts,
        } => {
            let report =
                run_benchmark_local(*objects, *batch_size, *prefixes, *keep_artifacts, &cfg);
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
                    "local benchmark: {} objects in {:.3}s ({:.0} objects/sec)",
                    report.objects, report.elapsed_secs, report.objects_per_sec
                );
                println!("  parquet: {}", report.parquet_file);
                println!("  ks:      {}", report.ks_file);
                if !report.artifacts_kept {
                    println!("  artifacts removed");
                }
            }
            return;
        }
        _ => {}
    }

    if cli.dry_run {
        let report = build_plan_report(&cli, &cfg);
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

    let run_warnings = runtime_guardrail_warnings(&cli, &cfg);
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
                &cli,
                &cfg,
            );
            return;
        }
        Commands::DiscoverPrefixes {
            region,
            bucket,
            output,
            max_pages,
            toml,
        } => {
            run_discover_prefixes(
                region.as_deref(),
                bucket,
                output.as_deref(),
                *max_pages,
                *toml,
                &cli,
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
        Commands::ConfigInspect { .. } | Commands::Doctor { .. } => {
            unreachable!("local-only commands are handled before runtime setup")
        }
        Commands::Profiles { .. } | Commands::Completions { .. } | Commands::Man => {
            unreachable!("local-only commands are handled before config load")
        }
        Commands::HintsMerge { .. }
        | Commands::TraceSummary { .. }
        | Commands::ManifestSummary { .. }
        | Commands::HintsRebalance { .. }
        | Commands::InitConfig { .. }
        | Commands::Recipes { .. }
        | Commands::Cheatsheet
        | Commands::Quickstart { .. } => {
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
        let hints_disabled_for_diff = mode == RunMode::BiDir;
        let ks_list: Vec<String> = load_hints(
            cli.hints_file.as_deref(),
            opt_bucket,
            opt_region,
            cfg.s3.profile.as_deref(),
            cli.no_auto_hints || hints_disabled_for_diff,
            hints_disabled_for_diff,
        );
        let original_hints_count = core::KeySpaceHints::new_from(&ks_list).total_count();

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
            &sdk_config,
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
            cli.continuation_token.as_deref(),
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
                &sdk_config,
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
                cli.continuation_token.as_deref(),
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
        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        let is_diff = mode == RunMode::BiDir;
        let output_config = cfg.output.clone();
        let filename_ks_for_task = filename_ks.clone();
        let filename_output_for_task = filename_output.clone();
        if is_diff {
            set.spawn(async move {
                data_map::data_map_task(
                    data_map_ctx,
                    &filename_ks_for_task,
                    &filename_output_for_task,
                    true,
                    output_config,
                )
                .await
            });
        } else if cli.summary_only {
            set.spawn(async move { data_map::data_map_task_list_summary_only(data_map_ctx).await });
        } else if list_output_format.writes_stdout_rows() {
            let text_format = data_map::ListTextOutputFormat::from(list_output_format);
            set.spawn(async move {
                data_map::data_map_task_list_stdout(data_map_ctx, text_format).await
            });
        } else {
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
                g_state.inc_fatal_error();
                g_state.quit();
            }

            // Save checkpoint progress periodically (every 30s or on state change).
            if cli.resume
                && last_checkpoint_save.elapsed().as_secs() >= 30
                && g_state.all_list_tasks_is_running()
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

        // ── Diff mode completion notice ────────────────────
        if mode == RunMode::BiDir {
            info!("{}", diff::diff_complete_notice());
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
        command: std::env::args().collect(),
        inputs: command_input_summary(&cli, &cfg),
        artifacts: agent::collect_artifacts(&manifest_outputs),
        outputs: manifest_outputs,
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

fn run_recipes(name: Option<&str>) {
    match local_tools::render_recipe(name) {
        Ok(rendered) => print!("{}", rendered),
        Err(e) => {
            eprintln!("Recipe error: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn run_quickstart(provider: &str) {
    match local_tools::render_quickstart(provider) {
        Ok(rendered) => print!("{}", rendered),
        Err(e) => {
            eprintln!("Quickstart error: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn run_hints_merge(
    inputs: &[String],
    output: Option<&str>,
    dry_run: bool,
    overwrite: bool,
    output_format: ReportFormat,
    json: bool,
    emit_manifest: Option<&str>,
) {
    match local_tools::merge_hints_files(inputs, output, dry_run, overwrite) {
        Ok(report) => {
            if let Some(manifest_path) = emit_manifest {
                let outputs = output.map(|p| vec![p.to_string()]).unwrap_or_default();
                if let Err(e) = local_tools::write_local_manifest(
                    manifest_path,
                    "hints-merge",
                    inputs,
                    &outputs,
                    &report,
                    &report.warnings,
                ) {
                    eprintln!("Manifest write error: {}", e);
                    std::process::exit(agent::ExitCode::OutputWrite.code());
                }
            }
            if json || output_format == ReportFormat::Json {
                println!("{}", agent::to_pretty_json(&report));
            } else {
                print!("{}", local_tools::render_merge_text(&report));
            }
        }
        Err(e) => {
            eprintln!("Hints merge failed: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn run_trace_summary(trace_file: &str, output_format: ReportFormat, json: bool) {
    match local_tools::trace_summary(trace_file) {
        Ok(report) => {
            if json || output_format == ReportFormat::Json {
                println!("{}", agent::to_pretty_json(&report));
            } else if output_format == ReportFormat::Markdown {
                print!("{}", local_tools::render_trace_summary_markdown(&report));
            } else {
                print!("{}", local_tools::render_trace_summary_text(&report));
            }
        }
        Err(e) => {
            eprintln!("Trace summary failed: {}", e);
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

#[allow(clippy::too_many_arguments)]
fn run_hints_rebalance(
    trace: &str,
    hints_file: &str,
    output: Option<&str>,
    dry_run: bool,
    overwrite: bool,
    max_new_boundaries: usize,
    long_tail_ratio: f64,
    min_pages: u32,
    explain: bool,
    output_format: ReportFormat,
    json: bool,
    emit_manifest: Option<&str>,
) {
    if !(long_tail_ratio.is_finite() && long_tail_ratio >= 1.0) {
        eprintln!("Hints rebalance failed: --long-tail-ratio must be >= 1.0");
        std::process::exit(agent::ExitCode::CliConfig.code());
    }

    match local_tools::rebalance_hints(
        trace,
        hints_file,
        output,
        dry_run,
        max_new_boundaries,
        long_tail_ratio,
        min_pages,
        overwrite,
    ) {
        Ok(report) => {
            if let Some(manifest_path) = emit_manifest {
                let inputs = vec![trace.to_string(), hints_file.to_string()];
                let outputs = output.map(|p| vec![p.to_string()]).unwrap_or_default();
                if let Err(e) = local_tools::write_local_manifest(
                    manifest_path,
                    "hints-rebalance",
                    &inputs,
                    &outputs,
                    &report,
                    &report.warnings,
                ) {
                    eprintln!("Manifest write error: {}", e);
                    std::process::exit(agent::ExitCode::OutputWrite.code());
                }
            }
            if json || output_format == ReportFormat::Json {
                println!("{}", agent::to_pretty_json(&report));
            } else {
                print!("{}", local_tools::render_rebalance_text(&report, explain));
            }
        }
        Err(e) => {
            eprintln!("Hints rebalance failed: {}", e);
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

fn run_profiles(cmd: &ProfileCommands) {
    match cmd {
        ProfileCommands::List { json } => {
            if *json {
                println!("{}", agent::to_pretty_json(&profiles::all_profiles()));
            } else {
                println!("Known endpoint compatibility profiles:");
                for profile in profiles::all_profiles() {
                    println!(
                        "  {:<6} {:<36} addressing={:<7} status={}",
                        profile.name,
                        profile.provider,
                        profile.recommended_addressing_style,
                        profile.status
                    );
                }
            }
        }
        ProfileCommands::Show { name, json } => match profiles::get_profile(name) {
            Some(profile) if *json => println!("{}", agent::to_pretty_json(profile)),
            Some(profile) => {
                println!("profile: {}", profile.name);
                println!("provider: {}", profile.provider);
                println!("status: {}", profile.status);
                println!("default_region: {}", profile.default_region.unwrap_or("-"));
                println!(
                    "default_endpoint_url: {}",
                    profile.default_endpoint_url.unwrap_or("-")
                );
                println!(
                    "recommended_addressing_style: {}",
                    profile.recommended_addressing_style
                );
                println!(
                    "requires_explicit_endpoint: {}",
                    profile.requires_explicit_endpoint
                );
                println!("tested_by_project: {}", profile.tested_by_project);
                if !profile.notes.is_empty() {
                    println!("notes:");
                    for note in profile.notes {
                        println!("  - {}", note);
                    }
                }
                if !profile.limitations.is_empty() {
                    println!("limitations:");
                    for limitation in profile.limitations {
                        println!("  - {}", limitation);
                    }
                }
            }
            None => {
                eprintln!("Unknown endpoint profile '{}'", name);
                std::process::exit(agent::ExitCode::CliConfig.code());
            }
        },
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
            "diff with --hints-file is not supported yet: hinted multi-segment diff paired coordination is deferred to v0.2.x; remove --hints-file to run authoritative single-segment diff"
        );
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
}

fn validate_diff_resume_command(cli: &Cli) {
    if cli.resume && matches!(cli.cmd, Commands::Diff { .. }) {
        eprintln!(
            "diff --resume is not supported yet: paired diff/resume coordination is deferred to v0.2.x; remove --resume to run authoritative single-segment diff"
        );
        std::process::exit(agent::ExitCode::CliConfig.code());
    }
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

fn prefixes_output_path(region: Option<&str>, bucket: &str, toml: bool) -> String {
    let extension = if toml { "toml" } else { "txt" };
    let mut parts = Vec::new();
    if let Some(region) = region.filter(|r| !r.is_empty()) {
        parts.push(sanitize_path_component(region));
    }
    parts.push(sanitize_path_component(bucket));
    format!("{}_prefixes.{}", parts.join("_"), extension)
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
                println!("NEXT s3-turbo-list profiles list");
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
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    elapsed_secs: f64,
    objects_per_sec: f64,
    parquet_file: String,
    parquet_bytes: u64,
    ks_file: String,
    ks_bytes: u64,
    metrics: agent::MetricsSummary,
    artifacts_kept: bool,
}

fn run_benchmark_local(
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    keep_artifacts: bool,
    cfg: &S3TurboConfig,
) -> LocalBenchmarkReport {
    let objects = objects.max(1);
    let batch_size = batch_size.max(1);
    let prefixes = prefixes.max(1);
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

    let quit = Arc::new(AtomicBool::new(false));
    let g_state = core::GlobalState::new(quit, 2);
    let started = Instant::now();
    let output_config = cfg.output.clone();
    let parquet_path = parquet_file.display().to_string();
    let ks_path = ks_file.display().to_string();

    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);

    rt.block_on(async {
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(core::ObjectKey, core::ObjectProps)>>(
            cfg.channel.capacity,
        );
        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        let data_map_ks = ks_path.clone();
        let data_map_parquet = parquet_path.clone();
        let data_map_output_config = output_config.clone();
        let data_map = tokio::spawn(async move {
            data_map::data_map_task_list_streaming(
                data_map_ctx,
                &data_map_ks,
                &data_map_parquet,
                data_map_output_config,
            )
            .await;
        });

        let producer_state = g_state.clone();
        let producer = tokio::spawn(async move {
            producer_state.list_task_start(core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
            producer_state.wait_to_start().await;
            let mut sent = 0usize;
            while sent < objects {
                let take = (objects - sent).min(batch_size);
                let mut batch = Vec::with_capacity(take);
                for offset in 0..take {
                    let index = sent + offset;
                    let prefix_index = index % prefixes;
                    let key_text = format!("prefix-{}/object-{:012}.dat", prefix_index, index);
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
                if tx.send(batch).await.is_err() {
                    break;
                }
                sent += take;
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
    let parquet_bytes = std::fs::metadata(&parquet_file)
        .map(|m| m.len())
        .unwrap_or(0);
    let ks_bytes = std::fs::metadata(&ks_file).map(|m| m.len()).unwrap_or(0);
    let metrics = g_state.metrics_snapshot().into();
    let artifacts_kept = keep_artifacts;
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
        benchmark: "local-list-streaming-output".to_string(),
        network: "none: synthetic local data only".to_string(),
        objects,
        batch_size,
        prefixes,
        elapsed_secs,
        objects_per_sec: objects as f64 / elapsed_secs,
        parquet_file: parquet_path,
        parquet_bytes,
        ks_file: ks_path,
        ks_bytes,
        metrics,
        artifacts_kept,
    }
}

fn build_plan_report(cli: &Cli, cfg: &S3TurboConfig) -> agent::PlanReport {
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
    let mut warnings = runtime_guardrail_warnings(cli, cfg);
    if matches!(cli.cmd, Commands::CompatProbe { .. }) {
        warnings.push(
            "compat-probe will contact the configured endpoint when not run with --dry-run"
                .to_string(),
        );
    }
    if matches!(cli.cmd, Commands::AutoHints { .. }) {
        warnings.push("auto-hints will scan S3 pages when not run with --dry-run".to_string());
        if cli.threads.is_some() || cli.concurrency.is_some() {
            warnings.push(
                "auto-hints performs a single sequential object scan; --threads and --concurrency do not change scan parallelism"
                    .to_string(),
            );
        }
    }
    if matches!(cli.cmd, Commands::DiscoverPrefixes { .. }) {
        warnings.push(
            "discover-prefixes will scan S3 ListObjectsV2 pages when not run with --dry-run"
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
        command: std::env::args().collect(),
        network: "none: dry-run only resolves local configuration and planned paths".to_string(),
        inputs,
        outputs,
        resolved_config: cfg.into(),
        hints,
        checkpoint: agent::checkpoint_plan(cli.resume, checkpoint_path, current_identity.as_ref()),
        file_conflicts,
        warnings,
    }
}

fn runtime_guardrail_warnings(cli: &Cli, cfg: &S3TurboConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    if matches!(
        cli.cmd,
        Commands::List { .. }
            | Commands::Diff { .. }
            | Commands::AutoHints { .. }
            | Commands::DiscoverPrefixes { .. }
            | Commands::CompatProbe { .. }
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
        warnings.extend(profiles::endpoint_profile_guardrail_warnings(cfg));
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
            "diff uses single-segment authoritative mode; hinted multi-segment diff paired coordination is deferred to v0.2.x"
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
        Commands::AutoHints { region, bucket, .. } => (
            "auto-hints".to_string(),
            Some(bucket.clone()),
            region.clone(),
            None,
            None,
            None,
        ),
        Commands::DiscoverPrefixes { region, bucket, .. } => (
            "discover-prefixes".to_string(),
            Some(bucket.clone()),
            region.clone(),
            None,
            None,
            None,
        ),
        Commands::HintsValidate { .. } => {
            ("hints-validate".to_string(), None, None, None, None, None)
        }
        Commands::HintsMerge { .. } => ("hints-merge".to_string(), None, None, None, None, None),
        Commands::TraceSummary { .. } => {
            ("trace-summary".to_string(), None, None, None, None, None)
        }
        Commands::ManifestSummary { .. } => {
            ("manifest-summary".to_string(), None, None, None, None, None)
        }
        Commands::HintsRebalance { .. } => {
            ("hints-rebalance".to_string(), None, None, None, None, None)
        }
        Commands::InitConfig { .. } => ("init-config".to_string(), None, None, None, None, None),
        Commands::Recipes { .. } => ("recipes".to_string(), None, None, None, None, None),
        Commands::Cheatsheet => ("cheatsheet".to_string(), None, None, None, None, None),
        Commands::Quickstart { .. } => ("quickstart".to_string(), None, None, None, None, None),
        Commands::ConfigInspect { .. } => {
            ("config-inspect".to_string(), None, None, None, None, None)
        }
        Commands::Doctor { .. } => ("doctor".to_string(), None, None, None, None, None),
        Commands::Profiles { .. } => ("profiles".to_string(), None, None, None, None, None),
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

    let hints_file = match &cli.cmd {
        Commands::AutoHints {
            region,
            bucket,
            output,
            ..
        } => output
            .clone()
            .or_else(|| Some(agent::conventional_hints_path(bucket, region.as_deref()))),
        Commands::DiscoverPrefixes {
            region,
            bucket,
            output,
            toml,
            ..
        } => output
            .clone()
            .or_else(|| Some(prefixes_output_path(region.as_deref(), bucket, *toml))),
        _ => None,
    };
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
        Commands::AutoHints {
            region,
            bucket,
            output,
            ..
        } => (
            None,
            None,
            output
                .clone()
                .or_else(|| Some(agent::conventional_hints_path(bucket, region.as_deref()))),
        ),
        Commands::DiscoverPrefixes {
            region,
            bucket,
            output,
            toml,
            ..
        } => (
            None,
            None,
            output
                .clone()
                .or_else(|| Some(prefixes_output_path(region.as_deref(), bucket, *toml))),
        ),
        _ => (None, None, None),
    }
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
                "diff mode uses single-segment authoritative fallback. Skipping conventional hints cache lookup."
            );
        } else {
            info!(
                "--no-auto-hints set. Skipping conventional hints cache lookup and using single-segment fallback."
            );
        }
        return vec![];
    }

    // 2. Try auto-hints cache at conventional path.
    let cache_filename = if let Some(r) = region {
        agent::conventional_hints_path(bucket, Some(r))
    } else {
        agent::conventional_hints_path(bucket, None)
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
    let rt = build_runtime_or_exit(2);

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
    cli: &Cli,
    cfg: &S3TurboConfig,
) {
    let rt = build_runtime_or_exit(2);

    let endpoint = cfg.s3.endpoint_url.as_deref();
    let fps = cfg.s3.force_path_style;
    let prefix = if cli.prefix == "/" {
        ""
    } else {
        cli.prefix.as_str()
    };

    if cli.threads.is_some() || cli.concurrency.is_some() {
        log::warn!(
            "auto-hints performs a single sequential object scan; --threads and --concurrency do not change scan parallelism"
        );
    }

    if cfg.s3.profile.as_deref() == Some("bos") {
        log::warn!(
            "Generating hints for BOS is allowed, but hinted multi-segment BOS scans \
             are not authoritative until BOS fixes its ListObjectsV2 start_after + \
             continuation-token compatibility behavior."
        );
    }

    rt.block_on(async {
        auto_hints::generate_hints(auto_hints::GenerateHintsOptions {
            region,
            bucket,
            output,
            endpoint_url: endpoint,
            force_path_style: fps,
            prefix,
            max_keys: cli.max_keys,
            max_attempts: cfg.s3.max_attempts,
            initial_backoff_secs: cfg.s3.initial_backoff_secs,
            connect_timeout_secs: cfg.s3.connect_timeout_secs,
            operation_timeout_secs: cfg.s3.operation_timeout_secs,
            sample_threshold: cfg.auto_hints.sample_threshold,
            max_prefix_depth: cfg.auto_hints.max_prefix_depth,
            max_prefix_entries: cfg.auto_hints.max_prefix_entries,
            sample_limit,
            max_pages,
        })
        .await
    });
}

fn run_discover_prefixes(
    region: Option<&str>,
    bucket: &str,
    output: Option<&str>,
    max_pages: Option<usize>,
    toml: bool,
    cli: &Cli,
    cfg: &S3TurboConfig,
) {
    let rt = build_runtime_or_exit(2);

    let prefix = if cli.prefix == "/" {
        ""
    } else {
        cli.prefix.as_str()
    };

    rt.block_on(async {
        auto_hints::discover_prefixes(auto_hints::DiscoverPrefixesOptions {
            region,
            bucket,
            output,
            endpoint_url: cfg.s3.endpoint_url.as_deref(),
            force_path_style: cfg.s3.force_path_style,
            prefix,
            delimiter: &cli.delimiter,
            max_keys: cli.max_keys,
            max_attempts: cfg.s3.max_attempts,
            initial_backoff_secs: cfg.s3.initial_backoff_secs,
            connect_timeout_secs: cfg.s3.connect_timeout_secs,
            operation_timeout_secs: cfg.s3.operation_timeout_secs,
            max_pages,
            toml,
        })
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
