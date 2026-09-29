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
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use config::S3TurboConfig;
use core::RunMode;
use log::{error, info, warn};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

// ── CLI definition ─────────────────────────────────────────
//
// Only the endpoint options are global. Every other option belongs to the
// commands that use it, so clap rejects a misplaced flag itself and each
// command's --help lists only what it takes. Run options written before the
// command name (`s3-turbo-list --output-dir out list …`, the pre-0.37
// spelling) are moved behind it by `hoist_command_flags` before parsing.

#[derive(Parser)]
#[command(name = "s3-turbo-list")]
#[command(
    author,
    version,
    about = "High-performance concurrent S3 bucket listing",
    long_about = None
)]
#[command(propagate_version = true)]
struct CliArgs {
    #[command(subcommand)]
    cmd: Commands,

    #[command(flatten)]
    global: GlobalArgs,
}

#[derive(Args, Debug, Clone, Default)]
struct GlobalArgs {
    /// Path to TOML config file
    #[arg(long, global = true, help_heading = "Endpoint")]
    config: Option<String>,

    /// S3-compatible provider preset: aws, minio, bos, r2, b2 or oss (sets
    /// the endpoint and addressing style; credentials profiles go in
    /// AWS_PROFILE)
    #[arg(long, global = true, alias = "profile", help_heading = "Endpoint")]
    provider: Option<String>,

    /// Custom S3 endpoint URL
    #[arg(
        long = "endpoint-url",
        global = true,
        alias = "endpoint",
        help_heading = "Endpoint"
    )]
    endpoint: Option<String>,

    /// S3 addressing style: path, virtual, or auto
    #[arg(long, global = true, help_heading = "Endpoint")]
    addressing_style: Option<String>,
}

/// What to list: shared by list and diff.
#[derive(Args, Debug, Clone, Default)]
struct SourceArgs {
    /// Key prefix to list, e.g. `logs/2026/` (default: the whole bucket)
    #[arg(
        short,
        long,
        default_value = "",
        hide_default_value = true,
        help_heading = "Source"
    )]
    prefix: String,

    /// ListObjectsV2 delimiter: '/' lists one level (objects plus one row per
    /// folder); the default lists every key recursively
    #[arg(
        long,
        default_value = "",
        hide_default_value = true,
        help_heading = "Source"
    )]
    delimiter: String,

    /// Start listing after this key (lists one segment; not with
    /// --hints-file or --resume)
    #[arg(long, help_heading = "Source")]
    start_after: Option<String>,

    /// Object filter expression (e.g. "SOURCE.size > 1000")
    #[arg(short, long, help_heading = "Source")]
    filter: Option<String>,
}

/// Where results go: shared by list and diff.
#[derive(Args, Debug, Clone, Default)]
struct OutputArgs {
    /// Directory for the auto-named outputs (Parquet, KeySpace, log)
    #[arg(long, help_heading = "Output")]
    output_dir: Option<String>,

    /// Parquet output path; the KeySpace file is written beside it as
    /// `<name>.ks`
    #[arg(long, help_heading = "Output")]
    output_parquet_file: Option<String>,

    /// Parquet compression codec: zstd (default), gzip, snappy, lz4,
    /// lz4_raw, brotli, or uncompressed
    #[arg(long, help_heading = "Output")]
    compression: Option<String>,

    /// Write the run log to a file (`<name>.log` beside the outputs)
    #[arg(short, long, help_heading = "Output")]
    log: bool,

    /// KeySpace output path (deprecated: derived from --output-parquet-file)
    #[arg(long, hide = true)]
    output_ks_file: Option<String>,

    /// Log file path (deprecated: --log names it after the outputs)
    #[arg(long, hide = true)]
    output_log_file: Option<String>,

    /// Parquet compression level (config: output.compression_level)
    #[arg(long, hide = true)]
    compression_level: Option<u32>,
}

/// How hard to list: shared by list and diff. Only --concurrency is shown;
/// the rest are for debugging and tests.
#[derive(Args, Debug, Clone, Default)]
struct TuningArgs {
    /// Max concurrent list operations (default 100)
    #[arg(short, long, help_heading = "Run")]
    concurrency: Option<usize>,

    /// Worker threads (default: CPU count)
    #[arg(short = 'T', long, hide = true)]
    threads: Option<usize>,

    /// Max keys per ListObjectsV2 page
    #[arg(long, hide = true, value_parser = clap::value_parser!(i32).range(1..))]
    max_keys: Option<i32>,

    /// Disable startup discovery (lists one segment unless --hints-file)
    #[arg(long, hide = true)]
    no_auto_hints: bool,
}

/// Machine-facing switches: shared by list and diff.
#[derive(Args, Debug, Clone, Default)]
struct AutomationArgs {
    /// Print the resolved plan as JSON and exit without contacting S3
    #[arg(long, help_heading = "Automation")]
    dry_run: bool,

    /// Print the run manifest JSON on stdout and keep stderr quiet
    #[arg(long, help_heading = "Automation")]
    agent: bool,

    /// Write the run manifest JSON to this path
    #[arg(long, help_heading = "Automation")]
    run_manifest: Option<String>,

    /// Write S3 request trace events as JSONL to this file (`-`: stderr)
    #[arg(long, help_heading = "Automation")]
    trace_compat: Option<String>,

    /// Write the dry-run plan to this path (deprecated: `--dry-run > file`)
    #[arg(long, hide = true, requires = "dry_run")]
    plan_json: Option<String>,

    /// Trace S3 requests to stderr (deprecated: `--trace-compat -`)
    #[arg(long, hide = true)]
    debug_s3: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// List a bucket to Parquet (or tsv / ndjson rows on stdout, or a summary)
    List {
        /// Bucket to list
        #[arg(long)]
        bucket: String,

        /// AWS region (or the provider's region)
        #[arg(long)]
        region: Option<String>,

        #[command(flatten)]
        source: SourceArgs,

        /// parquet writes files; tsv/ndjson stream rows to stdout; summary
        /// only counts
        #[arg(long, value_enum, default_value_t = ListOutputFormat::Parquet, help_heading = "Output")]
        output_format: ListOutputFormat,

        #[command(flatten)]
        output: OutputArgs,

        /// Continue an interrupted run from its checkpoint
        #[arg(long, help_heading = "Run")]
        resume: bool,

        /// Key-space boundaries file (overrides automatic partitioning)
        #[arg(short = 'H', long, help_heading = "Run")]
        hints_file: Option<String>,

        #[command(flatten)]
        tuning: TuningArgs,
        #[command(flatten)]
        automation: AutomationArgs,

        /// Resume from a continuation token (deprecated: use --resume or
        /// --start-after)
        #[arg(long, hide = true)]
        continuation_token: Option<String>,

        /// Count only (deprecated: --output-format summary)
        #[arg(long, hide = true)]
        summary_only: bool,
    },

    /// Diff two buckets into one Parquet file with a flag per key
    Diff {
        /// Source bucket
        #[arg(long)]
        bucket: String,

        /// Source region
        #[arg(long)]
        region: Option<String>,

        /// Target bucket
        #[arg(long)]
        target_bucket: String,

        /// Target region [default: --region]
        #[arg(long)]
        target_region: Option<String>,

        #[command(flatten)]
        source: SourceArgs,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        tuning: TuningArgs,
        #[command(flatten)]
        automation: AutomationArgs,
    },

    /// Check an S3-compatible endpoint with a few requests before a full run
    CompatProbe {
        /// Bucket to probe
        #[arg(long)]
        bucket: String,

        /// Region (default: the provider's, or the SDK's)
        #[arg(long)]
        region: Option<String>,

        /// Key prefix to probe under
        #[arg(short, long, default_value = "", hide_default_value = true)]
        prefix: String,

        /// JSON report path (default: stdout)
        #[arg(short, long)]
        output: Option<String>,

        /// Write the log to a file
        #[arg(short, long)]
        log: bool,

        /// Print the resolved plan as JSON and exit without contacting S3
        #[arg(long)]
        dry_run: bool,

        /// Machine-readable output
        #[arg(long)]
        agent: bool,

        /// Write S3 request trace events as JSONL to this file (`-`: stderr)
        #[arg(long)]
        trace_compat: Option<String>,

        #[arg(long, hide = true)]
        debug_s3: bool,
    },

    /// Local preflight: config, provider, endpoint, proxy, outputs, hints file
    Doctor {
        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Validate this hints file
        #[arg(short = 'H', long)]
        hints_file: Option<String>,

        /// Check that this filter expression compiles
        #[arg(short, long)]
        filter: Option<String>,

        /// Check that outputs can be created in this directory
        #[arg(long)]
        output_dir: Option<String>,

        /// Check that this Parquet output can be created
        #[arg(long)]
        output_parquet_file: Option<String>,

        /// Check that this trace file can be created
        #[arg(long)]
        trace_compat: Option<String>,

        #[arg(long, hide = true)]
        output_ks_file: Option<String>,
        #[arg(long, hide = true)]
        output_log_file: Option<String>,
        #[arg(long, hide = true)]
        agent: bool,
        /// Deprecated: the default output is the compact form
        #[arg(long, hide = true)]
        simple: bool,
        /// Deprecated: suggestions are printed when something is wrong
        #[arg(long, hide = true)]
        fix_suggestions: bool,
    },

    /// Summarize a run manifest, or --check it (exit 6 on a mismatch)
    ManifestSummary {
        /// Run manifest JSON file written by --run-manifest or --agent
        manifest_file: String,

        /// Emit JSON report
        #[arg(long)]
        json: bool,

        /// Validate manifest success, counters, row checks, and recorded
        /// artifacts via exit code
        #[arg(long)]
        check: bool,

        #[arg(long, hide = true)]
        agent: bool,
    },

    /// Provider quickstarts: aws, minio, bos, r2, b2, oss
    Guide {
        /// A provider (aws/minio/bos/r2/b2/oss); omit for an overview
        topic: Option<String>,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },

    /// Generate a man page to stdout
    Man,

    /// Removed in 0.37.0 (see docs/providers.md for a config example)
    #[command(hide = true)]
    InitConfig {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        _args: Vec<String>,
    },
}

/// The parsed command line with every command's options in one place, as
/// the rest of `main` reads them. Options a command does not take keep their
/// defaults.
struct Cli {
    cmd: Commands,
    config: Option<String>,
    prefix: String,
    threads: Option<usize>,
    concurrency: Option<usize>,
    hints_file: Option<String>,
    filter: Option<String>,
    log: bool,
    endpoint: Option<String>,
    output_log_file: Option<String>,
    output_ks_file: Option<String>,
    output_parquet_file: Option<String>,
    compression: Option<String>,
    compression_level: Option<u32>,
    output_dir: Option<String>,
    resume: bool,
    no_auto_hints: bool,
    delimiter: String,
    max_keys: Option<i32>,
    start_after: Option<String>,
    continuation_token: Option<String>,
    profile: Option<String>,
    addressing_style: Option<String>,
    trace_compat: Option<String>,
    agent: bool,
    dry_run: bool,
    plan_json: Option<String>,
    run_manifest: Option<String>,
    summary_only: bool,
    /// Deprecated spellings used on this command line, for a warning.
    deprecated: Vec<&'static str>,
}

impl Cli {
    fn from_args(args: CliArgs) -> Self {
        let GlobalArgs {
            config,
            provider,
            endpoint,
            addressing_style,
        } = args.global;
        let mut cli = Cli {
            cmd: args.cmd,
            config,
            prefix: String::new(),
            threads: None,
            concurrency: None,
            hints_file: None,
            filter: None,
            log: false,
            endpoint,
            output_log_file: None,
            output_ks_file: None,
            output_parquet_file: None,
            compression: None,
            compression_level: None,
            output_dir: None,
            resume: false,
            no_auto_hints: false,
            delimiter: String::new(),
            max_keys: None,
            start_after: None,
            continuation_token: None,
            profile: provider,
            addressing_style,
            trace_compat: None,
            agent: false,
            dry_run: false,
            plan_json: None,
            run_manifest: None,
            summary_only: false,
            deprecated: Vec::new(),
        };
        let mut groups = None;
        match &mut cli.cmd {
            Commands::List {
                output_format,
                resume,
                hints_file,
                continuation_token,
                summary_only,
                source,
                output,
                tuning,
                automation,
                ..
            } => {
                if *summary_only {
                    cli.deprecated
                        .push("--summary-only (use --output-format summary)");
                    if *output_format == ListOutputFormat::Parquet {
                        *output_format = ListOutputFormat::Summary;
                    }
                }
                cli.summary_only = *summary_only || *output_format == ListOutputFormat::Summary;
                cli.resume = *resume;
                cli.hints_file = hints_file.clone();
                if continuation_token.is_some() {
                    cli.deprecated
                        .push("--continuation-token (use --resume or --start-after)");
                }
                cli.continuation_token = continuation_token.clone();
                groups = Some((
                    source.clone(),
                    output.clone(),
                    tuning.clone(),
                    automation.clone(),
                ));
            }
            Commands::Diff {
                source,
                output,
                tuning,
                automation,
                ..
            } => {
                groups = Some((
                    source.clone(),
                    output.clone(),
                    tuning.clone(),
                    automation.clone(),
                ));
            }
            Commands::CompatProbe {
                prefix,
                log,
                dry_run,
                agent,
                trace_compat,
                debug_s3,
                ..
            } => {
                cli.prefix = prefix.clone();
                cli.log = *log;
                cli.dry_run = *dry_run;
                cli.agent = *agent;
                cli.trace_compat = trace_compat.clone();
                if *debug_s3 {
                    cli.deprecated.push("--debug-s3 (use --trace-compat -)");
                    cli.trace_compat.get_or_insert_with(|| "-".to_string());
                }
            }
            Commands::Doctor {
                json,
                hints_file,
                filter,
                output_dir,
                output_parquet_file,
                trace_compat,
                output_ks_file,
                output_log_file,
                agent,
                ..
            } => {
                *json |= *agent;
                cli.agent = *agent;
                cli.hints_file = hints_file.clone();
                cli.filter = filter.clone();
                cli.output_dir = output_dir.clone();
                cli.output_parquet_file = output_parquet_file.clone();
                cli.output_ks_file = output_ks_file.clone();
                cli.output_log_file = output_log_file.clone();
                cli.trace_compat = trace_compat.clone();
            }
            Commands::ManifestSummary { json, agent, .. } => {
                *json |= *agent;
                cli.agent = *agent;
            }
            _ => {}
        }
        if let Some((source, output, tuning, automation)) = groups {
            cli.prefix = source.prefix;
            cli.delimiter = source.delimiter;
            cli.start_after = source.start_after;
            cli.filter = source.filter;
            cli.output_dir = output.output_dir;
            cli.output_parquet_file = output.output_parquet_file;
            cli.compression = output.compression;
            cli.log = output.log;
            if output.output_ks_file.is_some() {
                cli.deprecated
                    .push("--output-ks-file (the KeySpace file follows --output-parquet-file)");
            }
            cli.output_ks_file = output.output_ks_file;
            if output.output_log_file.is_some() {
                cli.deprecated
                    .push("--output-log-file (--log names the file after the outputs)");
            }
            cli.output_log_file = output.output_log_file;
            cli.compression_level = output.compression_level;
            cli.concurrency = tuning.concurrency;
            cli.threads = tuning.threads;
            cli.max_keys = tuning.max_keys;
            cli.no_auto_hints = tuning.no_auto_hints;
            cli.dry_run = automation.dry_run;
            cli.agent = automation.agent;
            cli.run_manifest = automation.run_manifest;
            cli.trace_compat = automation.trace_compat;
            if automation.plan_json.is_some() {
                cli.deprecated.push("--plan-json (use --dry-run > file)");
            }
            cli.plan_json = automation.plan_json;
            if automation.debug_s3 {
                cli.deprecated.push("--debug-s3 (use --trace-compat -)");
                cli.trace_compat.get_or_insert_with(|| "-".to_string());
            }
        }
        cli
    }
}

/// Move command options written before the command name behind it:
/// `s3-turbo-list --output-dir out list --bucket b` was the documented
/// spelling while every option was global, and scripts use it. Global
/// (endpoint) options stay where they are; an option the command does not
/// take is left in place for clap to reject.
fn hoist_command_flags(args: Vec<std::ffi::OsString>) -> Vec<std::ffi::OsString> {
    let command = CliArgs::command();
    let top_level: Vec<&clap::Arg> = command.get_arguments().collect();
    let takes_value = |arg: &clap::Arg| arg.get_action().takes_values();
    let find = |args: &[&clap::Arg], token: &str| -> Option<(bool, bool)> {
        // (known, takes a separate value)
        if let Some(long) = token.strip_prefix("--") {
            let (name, inline) = match long.split_once('=') {
                Some((name, _)) => (name, true),
                None => (long, false),
            };
            return args
                .iter()
                .find(|arg| {
                    arg.get_long() == Some(name)
                        || arg.get_all_aliases().is_some_and(|a| a.contains(&name))
                })
                .map(|arg| (true, takes_value(arg) && !inline));
        }
        let mut chars = token.strip_prefix('-')?.chars();
        let short = chars.next()?;
        args.iter()
            .find(|arg| arg.get_short() == Some(short))
            .map(|arg| (true, takes_value(arg) && chars.as_str().is_empty()))
    };
    let strs: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();

    // Find the command name, skipping option values.
    let mut index = 1;
    let mut command_at = None;
    while index < strs.len() {
        let token = strs[index].as_str();
        if token == "--" {
            break;
        }
        if let Some(sub) = command.find_subcommand(token) {
            command_at = Some((index, sub));
            break;
        }
        if token.starts_with('-') {
            let all: Vec<&clap::Arg> = top_level
                .iter()
                .copied()
                .chain(
                    command
                        .get_subcommands()
                        .flat_map(|sub| sub.get_arguments()),
                )
                .collect();
            if let Some((_, true)) = find(&all, token) {
                index += 1;
            }
        }
        index += 1;
    }
    let Some((at, sub)) = command_at else {
        return args;
    };
    let sub_args: Vec<&clap::Arg> = sub.get_arguments().collect();
    let mut kept = vec![args[0].clone()];
    let mut moved = Vec::new();
    let mut index = 1;
    while index < at {
        let token = strs[index].as_str();
        let global = find(&top_level, token);
        let local = if global.is_none() {
            find(&sub_args, token)
        } else {
            None
        };
        let (target, value) = match (global, local) {
            (Some((_, value)), _) => (&mut kept, value),
            (None, Some((_, value))) => (&mut moved, value),
            (None, None) => (&mut kept, false),
        };
        target.push(args[index].clone());
        if value && index + 1 < at {
            index += 1;
            target.push(args[index].clone());
        }
        index += 1;
    }
    kept.push(args[at].clone());
    kept.extend(moved);
    kept.extend(args[at + 1..].iter().cloned());
    kept
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ListOutputFormat {
    Parquet,
    Tsv,
    Ndjson,
    Summary,
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
            Self::Summary => "summary",
        }
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
            ListOutputFormat::Parquet | ListOutputFormat::Summary => {
                unreachable!("only tsv and ndjson use the text data-map sink")
            }
        }
    }
}

// ── Main ───────────────────────────────────────────────────

/// Set when the command is `doctor --json` (or doctor under `--agent`), so the
/// early config-validation exits still print a JSON report on stdout: an agent
/// that asked for JSON must not get an empty stdout on the very failures
/// doctor exists to diagnose.
static DOCTOR_JSON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Exit 2 on a config/CLI validation error, in doctor's JSON shape when
/// doctor's JSON output was requested.
fn exit_config_error(message: &str) -> ! {
    exit_doctor_check_error("config_parse", message)
}

/// Set once the command is a real `list` / `diff` / `compat-probe` run (not a
/// dry run): its pre-run failures report like failed runs.
static RUN_COMMAND: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
/// `--agent` on such a run: stdout carries a JSON result even when the run
/// stops before listing.
static AGENT_RUN: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Stop before (or instead of) listing with `code`. Every command prints the
/// reason on stderr as before; a run command also prints the documented
/// `s3-turbo-list: run failed (exit N): <reason>` line and, under `--agent`,
/// a minimal JSON result on stdout — agents branch on those, and pre-run
/// failures (a bad filter, no region, an uncreatable output) used to give
/// neither.
fn exit_before_run(code: agent::ExitCode, message: String) -> ! {
    eprintln!("{}", message);
    run_failure_epilogue(code, &message);
    std::process::exit(code.code())
}

fn run_failure_epilogue(code: agent::ExitCode, message: &str) {
    if !RUN_COMMAND.get().copied().unwrap_or(false) {
        return;
    }
    let reason = message
        .lines()
        .next()
        .unwrap_or(message)
        .trim_end_matches('.');
    eprintln!(
        "s3-turbo-list: run failed (exit {}): {}. Nothing was listed.",
        code.code(),
        reason
    );
    if AGENT_RUN.get().copied().unwrap_or(false) {
        println!(
            "{}",
            agent::to_pretty_json(&serde_json::json!({
                "schema_version": agent::AGENT_SCHEMA_VERSION,
                "tool_version": env!("CARGO_PKG_VERSION"),
                "status": "failed",
                "exit_code": code.code(),
                "error": message,
            }))
        );
    }
}

/// Exit 2 on a local input error; `doctor --json` still prints its JSON
/// report (one `error` check named `check`), so its stdout is never empty.
fn exit_doctor_check_error(check: &str, message: &str) -> ! {
    eprintln!("{}", message);
    if DOCTOR_JSON.get().copied().unwrap_or(false) {
        println!(
            "{}",
            agent::to_pretty_json(&serde_json::json!({
                "schema_version": agent::AGENT_SCHEMA_VERSION,
                "tool_version": env!("CARGO_PKG_VERSION"),
                "status": "error",
                "checks": [{
                    "name": check,
                    "status": "error",
                    "message": message,
                }],
            }))
        );
    }
    run_failure_epilogue(agent::ExitCode::CliConfig, message);
    std::process::exit(agent::ExitCode::CliConfig.code());
}

/// Parse the command line. A usage error under `--agent` also prints the
/// failed-run JSON, like every other failure before a run starts.
fn parse_cli() -> Cli {
    let argv = hoist_command_flags(std::env::args_os().collect());
    match CliArgs::try_parse_from(&argv) {
        Ok(args) => Cli::from_args(args),
        Err(e) => {
            use clap::error::ErrorKind;
            let agent = argv.iter().any(|arg| arg == "--agent");
            if agent && !matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                let _ = e.print();
                let _ = RUN_COMMAND.set(true);
                let _ = AGENT_RUN.set(true);
                let rendered = e.to_string();
                let first = rendered.lines().next().unwrap_or_default();
                run_failure_epilogue(
                    agent::ExitCode::CliConfig,
                    first.trim_start_matches("error: "),
                );
                std::process::exit(agent::ExitCode::CliConfig.code());
            }
            e.exit()
        }
    }
}

fn main() {
    let mut cli = parse_cli();
    if let Commands::InitConfig { .. } = cli.cmd {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "init-config was removed in 0.37.0: a config file is only needed for settings \
             the flags do not cover; see the example in docs/providers.md"
                .to_string(),
        );
    }
    for spelling in &cli.deprecated {
        eprintln!("warning: deprecated option {}", spelling);
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
    let run_command = !cli.dry_run
        && matches!(
            cli.cmd,
            Commands::List { .. } | Commands::Diff { .. } | Commands::CompatProbe { .. }
        );
    let _ = RUN_COMMAND.set(run_command);
    let _ = AGENT_RUN.set(run_command && cli.agent);
    let _ = DOCTOR_JSON.set(
        matches!(cli.cmd, Commands::Doctor { json: true, .. })
            || (cli.agent && matches!(cli.cmd, Commands::Doctor { .. })),
    );

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

    validate_addressing_style_command(&cli);
    cfg.apply_cli_overrides(config::CliOverrides {
        threads: cli.threads,
        concurrency: cli.concurrency,
        endpoint: cli.endpoint.as_deref(),
        addressing_style: cli.addressing_style.as_deref(),
        provider: cli.profile.as_deref(),
        trace_compat: cli.trace_compat.as_deref(),
        start_after: cli.start_after.as_deref(),
        log_file: cli.output_log_file.as_deref(),
        ks_file: cli.output_ks_file.as_deref(),
        parquet_file: cli.output_parquet_file.as_deref(),
        compression: cli.compression.as_deref(),
        compression_level: cli.compression_level,
    });
    // Recorded before the preset fills it in: a diff derives the target
    // side's endpoint from its own region only when the user gave none.
    // A misspelled profile (`mino`) applied no preset at all — no endpoint,
    // no addressing style — and the run went to AWS. Like an unknown config
    // key, it is a configuration error.
    if let Some(name) = cfg.s3.profile.as_deref()
        && profiles::get_profile(name).is_none()
    {
        exit_config_error(&format!(
            "--provider '{}' is not a provider preset; use one of: {} \
                 (credentials profiles go in AWS_PROFILE)",
            name,
            profiles::all_profiles()
                .iter()
                .map(|profile| profile.name)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let endpoint_was_explicit = cfg.s3.endpoint_url.is_some();
    // A provider whose endpoint is built from the region (bos, oss, b2)
    // fills in its default region when none is given — and the request must
    // then be signed for that region, not the ambient AWS_REGION (it was:
    // `--provider bos` alone signed for us-east-1 against the bj host).
    if !endpoint_was_explicit
        && let Some(profile) = cfg.s3.profile.as_deref().and_then(profiles::get_profile)
        && profile.endpoint_template.is_some()
        && let Some(default_region) = profile.default_region
    {
        fill_default_region(&mut cli.cmd, default_region);
    }
    let cli = cli;
    cfg.apply_profile_preset(command_region(&cli.cmd));
    let diff_target_endpoint = diff_target_endpoint(&cli, &cfg, endpoint_was_explicit);
    apply_output_dir_defaults(&cli, &mut cfg);
    apply_log_file_default(&cli, &mut cfg);
    apply_summary_only_output_defaults(&cli, &mut cfg);
    validate_runtime_values(&cfg);
    validate_output_format_command(&cli);
    validate_continuation_token_command(&cli, &cfg);
    validate_start_after_command(&cli, &cfg);
    validate_delimiter_hints_command(&cli);
    let config_source = agent::ConfigSourceSummary::new(&config_load, cli_config_overrides(&cli));
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
            // An endpoint/profile error is the same setup failure a real
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
        if let Some(path) = cli.plan_json.as_deref()
            && let Err(e) = agent::write_json_file(path, &report)
        {
            exit_before_run(
                agent::ExitCode::OutputWrite,
                format!("Plan write error: {}", e),
            );
        }
        if cli.agent || cli.plan_json.is_none() {
            println!("{}", agent::to_pretty_json(&report));
        }
        // An explicit hints file the run cannot load stops it with exit 2;
        // so does the plan (still written above).
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
                    agent::ExitCode::CliConfig,
                    "compat-probe requires an endpoint: pass --endpoint-url or --provider, \
                     or set s3.endpoint_url in the config"
                        .to_string(),
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

    let output_stem = output_stem(
        opt_region,
        opt_bucket,
        opt_target_region.flatten(),
        opt_target_bucket,
        &opt_prefix,
        None,
    );
    let filename_ks = cfg
        .output
        .ks_file
        .clone()
        .unwrap_or_else(|| format!("{}.ks", output_stem));
    let filename_output = cfg
        .output
        .parquet_file
        .clone()
        .unwrap_or_else(|| format!("{}.parquet", output_stem));
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
        // ── Checkpoint journal (resume mode) ──────────────────
        let checkpoint_path_opt = if cli.resume {
            Some(checkpoint::checkpoint_path_for_prefix(
                opt_bucket,
                opt_region,
                &opt_prefix,
            ))
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
            cli.filter.as_deref(),
        )
        .with_endpoint(cfg.s3.endpoint_url.as_deref());

        let checkpoint_journal = checkpoint_path_opt
            .as_deref()
            .and_then(|p| checkpoint::CheckpointJournal::load_and_verify(p, &current_identity));

        // A checkpoint that recorded the unwritten key ranges (0.36+) is
        // resumed by listing exactly those ranges; hints, startup discovery
        // and segment indices play no part.
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
        use crate::trace::S3TraceWriter;
        let trace_writer: Option<Arc<dyn S3TraceWriter>> =
            match trace::trace_writer_for_target(cfg.s3.trace_compat.as_deref()) {
                Ok(writer) => writer.map(Arc::from),
                Err(e) => {
                    error!("{}", e);
                    std::process::exit(agent::ExitCode::OutputWrite.code());
                }
            };

        // ── Load or generate KeySpace hints ─────────────────
        let hints_disabled_for_diff = mode == RunMode::BiDir;
        // --start-after is single-chain: hint segments would each override
        // their start with the CLI key and list overlapping ranges, so the
        // cached-hints load is skipped just like startup discovery below.
        // (--hints-file plus --start-after is rejected at CLI validation.)
        let ks_list: Vec<String> = if resume_ranges.is_some() {
            Vec::new()
        } else if cfg.s3.start_after.is_some() {
            info!(
                "--start-after is single-chain: skipping cached hints and listing as one segment"
            );
            Vec::new()
        } else {
            load_hints(
                cli.hints_file.as_deref(),
                opt_bucket,
                opt_region,
                &opt_prefix,
                &cli.delimiter,
                cli.no_auto_hints || hints_disabled_for_diff,
                hints_disabled_for_diff,
            )
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
            && cli.continuation_token.is_none()
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
                info!("Interrupted during startup discovery; no boundaries cached");
            } else if boundaries.is_empty() {
                info!(
                    "Startup discovery found no cuttable key space — using single-segment listing"
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

        // Discard a resume journal whose segment set does not match the
        // current hints — completed indices are positional, so a mismatch
        // would skip the wrong segments and silently drop keys.
        let checkpoint_journal = checkpoint_journal.filter(|cj| {
            cj.remaining.is_some() || cj.verify_segments(&ks_list, original_hints_count)
        });
        // The checkpoint records progress against *this* boundary set; the
        // fingerprint is what a later --resume verifies it against.
        let current_identity = current_identity.with_boundaries(&ks_list);

        // Filter out completed segments when resuming.
        // Segments this run will not list because the checkpoint records them
        // listed. Captured here rather than re-read at manifest time: a run
        // that finishes removes its checkpoint, so the file on disk at the end
        // says nothing about whether this run resumed. Computed after the
        // verification above — a discarded checkpoint skips nothing.
        if let Some(ref cj) = checkpoint_journal {
            let (skipped, warning) = match &cj.remaining {
                Some(ranges) => {
                    let listed = cj.listed_ranges.unwrap_or(0);
                    info!(
                        "Resuming checkpoint: {} key range(s) left to list",
                        ranges.len()
                    );
                    (
                        listed,
                        format!(
                            "Resuming from checkpoint: the key space earlier runs already wrote \
                             ({} range(s), in whole or in part) will not be listed again; this run \
                             lists the {} remaining range(s), so its output covers only the rest \
                             of the key space. Combine it with the output of the interrupted \
                             run(s); writing both to the same path leaves only this run's part.",
                            listed,
                            ranges.len()
                        ),
                    )
                }
                None => {
                    let skipped = cj.completed_indices.len();
                    info!(
                        "Resuming checkpoint: {} of {} segments completed",
                        skipped, cj.total_segments
                    );
                    (
                        skipped,
                        format!(
                            "Resuming from checkpoint: {} of {} segments are already recorded \
                             complete and will not be listed again, so this run's output covers \
                             only the remaining key space. Combine it with the output of the \
                             interrupted run; writing both to the same path leaves only this \
                             run's half.",
                            skipped, cj.total_segments
                        ),
                    )
                }
            };
            resumed_segments_skipped = Some(skipped);
            if skipped > 0 {
                // The runtime warnings were printed before the checkpoint was
                // read, so this one goes to stderr here or it never does.
                if !cli.agent {
                    print_runtime_warnings(std::slice::from_ref(&warning));
                }
                run_warnings.push(warning);
            }
        }

        let hints = if let Some(ranges) = resume_ranges.as_deref() {
            core::KeySpaceHints::from_ranges(ranges)
        } else if let Some(ref cj) = checkpoint_journal {
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
            let left_ctx = core::S3TaskContext::new(
                opt_bucket,
                opt_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style(),
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
                diff_target_endpoint.as_deref(),
                cfg.s3.force_path_style(),
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
            let task_ctx = core::S3TaskContext::new(
                opt_bucket,
                opt_region,
                cfg.s3.endpoint_url.as_deref(),
                cfg.s3.force_path_style(),
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
            resume_slot = Some(task_ctx.resume_progress.clone());
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
        // What the exit line should say about resuming; `None` when this run
        // did not use --resume.
        let mut checkpoint_note: Option<String> = None;
        if cli.resume
            && let Some(ref cp_path) = checkpoint_path_opt
        {
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
                        let completed = merged_completed_indices(
                            checkpoint_journal.as_ref(),
                            &left_checkpoint,
                            right_checkpoint.as_ref(),
                        );
                        let listed_before = checkpoint_journal.as_ref().map_or(0, |cj| {
                            cj.listed_ranges.unwrap_or(cj.completed_indices.len())
                        });
                        let remaining_count = progress.remaining.len();
                        let journal = checkpoint::CheckpointJournal {
                            bucket: opt_bucket.to_string(),
                            prefix: opt_prefix.clone(),
                            total_segments: original_hints_count,
                            completed_indices: completed,
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

    // The hints cache is an output of this run only when this run wrote it
    // (startup discovery on a list run); a cache merely read is an input.
    let run_started_system = std::time::SystemTime::now() - run_timer.elapsed();
    let hints_written = (mode == RunMode::List)
        .then(|| agent::conventional_hints_path_for_prefix(opt_bucket, opt_region, &opt_prefix))
        .filter(|path| {
            std::fs::metadata(path)
                .and_then(|m| m.modified())
                .is_ok_and(|modified| modified >= run_started_system)
        });
    let manifest_outputs = runtime_output_summary(
        &cli,
        &cfg,
        list_writes_artifacts(&cli).then_some(filename_ks.as_str()),
        list_writes_artifacts(&cli).then_some(filename_output.as_str()),
    )
    .with_hints(hints_written);
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
    let manifest = agent::RunManifest {
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
            cli.resume.then(|| {
                checkpoint::checkpoint_path_for_prefix(opt_bucket, opt_region, &opt_prefix)
            }),
            Some(
                &checkpoint::CheckpointIdentity::new(
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
                    cli.filter.as_deref(),
                )
                .with_endpoint(cfg.s3.endpoint_url.as_deref()),
            ),
            resumed_segments_skipped,
        ),
        warnings: manifest_warnings,
    };

    if let Some(path) = cli.run_manifest.as_deref()
        && let Err(e) = agent::write_json_file(path, &manifest)
    {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!("Manifest write error: {}", e),
        );
    }
    if cli.agent {
        println!("{}", agent::to_pretty_json(&manifest));
    } else if exit_code == agent::ExitCode::Success && cli.summary_only {
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
                None => "interrupted (run with --resume to make a run resumable)".to_string(),
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

fn generate_completions(shell: Shell) {
    let mut cmd = CliArgs::command();
    let name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
}

fn generate_man_page() {
    let cmd = CliArgs::command();
    let man = clap_mangen::Man::new(cmd);
    let mut buffer: Vec<u8> = Vec::new();
    if let Err(e) = man.render(&mut buffer) {
        exit_before_run(
            agent::ExitCode::InternalError,
            format!("Man page generation error: {}", e),
        );
    }
    if let Err(e) = std::io::stdout().write_all(&buffer) {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!("Man page write error: {}", e),
        );
    }
}

fn build_runtime_or_exit(worker_threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap_or_else(|e| {
            exit_before_run(
                agent::ExitCode::InternalError,
                format!("Runtime initialization error: {}", e),
            );
        })
}

/// Set the command's region (both sides of a diff) where none was given.
fn fill_default_region(cmd: &mut Commands, default_region: &str) {
    match cmd {
        Commands::List { region, .. } | Commands::CompatProbe { region, .. } => {
            region.get_or_insert_with(|| default_region.to_string());
        }
        Commands::Diff {
            region,
            target_region,
            ..
        } => {
            region.get_or_insert_with(|| default_region.to_string());
            target_region.get_or_insert_with(|| default_region.to_string());
        }
        _ => {}
    }
}

/// The `--json` shape of a command that failed before producing its report:
/// stdout stays machine-readable instead of empty.
fn print_json_error(message: &str) {
    println!(
        "{}",
        agent::to_pretty_json(&serde_json::json!({
            "schema_version": agent::AGENT_SCHEMA_VERSION,
            "tool_version": env!("CARGO_PKG_VERSION"),
            "status": "error",
            "error": message,
        }))
    );
}

fn run_guide(topic: Option<&str>) {
    match local_tools::render_guide(topic) {
        Ok(rendered) => print!("{}", rendered),
        Err(e) => {
            exit_before_run(agent::ExitCode::CliConfig, format!("Guide error: {}", e));
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
            if json {
                print_json_error(&e);
            }
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

/// `--log` without `--output-log-file`: name the log file up front — inside
/// `--output-dir` when one is given — so the plan, the manifest's `outputs`
/// and its artifacts report it like every other output. It used to be named
/// only when logging started, always in the working directory, and neither
/// the plan nor the manifest knew it existed.
fn apply_log_file_default(cli: &Cli, cfg: &mut S3TurboConfig) {
    if !cli.log
        || cfg.output.log_file.is_some()
        || !matches!(cli.cmd, Commands::List { .. } | Commands::Diff { .. })
    {
        return;
    }
    let name = format!("turbo_list_{}.log", Local::now().format("%Y%m%d%H%M%S"));
    cfg.output.log_file = Some(match cli.output_dir.as_deref() {
        Some(dir) => format!("{}/{}", dir, name),
        None => name,
    });
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
            let stem = output_stem(
                region.as_deref(),
                bucket,
                None,
                None,
                &listing_prefix(cli),
                Some(output_dir),
            );
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
            ..
        } => {
            let stem = output_stem(
                region.as_deref(),
                bucket,
                target_region.as_deref(),
                Some(target_bucket.as_str()),
                &listing_prefix(cli),
                Some(output_dir),
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

fn validate_output_format_command(cli: &Cli) {
    let Some(format) = list_output_format(cli) else {
        return;
    };
    if cli.summary_only && format.writes_stdout_rows() {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--summary-only cannot be combined with --output-format tsv or ndjson".to_string(),
        );
    }
    if cli.agent && !cli.dry_run && format.writes_stdout_rows() {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--agent writes the run manifest to stdout and cannot be combined with --output-format tsv or ndjson; use --run-manifest instead".to_string(),
        );
    }
}

/// Cheap shape check for an endpoint: a scheme this SDK can dispatch and a
/// non-empty host. Deliberately not a full URL parse — custom deployments use
/// shapes this tool has no business rejecting, so this only feeds a warning.
fn endpoint_url_looks_usable(endpoint: &str) -> bool {
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "http" | "https") {
        return false;
    }
    let host = rest.split(['/', '?']).next().unwrap_or("");
    !host.is_empty()
}

/// Numeric knobs have no meaningful zero: a zero concurrency listed nothing
/// and still exited 0 (the reactor's fill loop never ran), zero worker threads
/// and a zero-capacity channel both panicked out of the documented exit codes,
/// and a zero operation timeout fails every request. Reject them where every
/// other configuration error is reported, with the offending value named.
fn validate_runtime_values(cfg: &S3TurboConfig) {
    let checks: [(&str, usize); 4] = [
        (
            "runtime.max_concurrency (-c/--concurrency)",
            cfg.runtime.max_concurrency,
        ),
        (
            "runtime.worker_threads (-T/--threads)",
            cfg.runtime.worker_threads,
        ),
        ("channel.capacity", cfg.channel.capacity),
        (
            "s3.operation_timeout_secs",
            cfg.s3.operation_timeout_secs as usize,
        ),
    ];
    for (name, value) in checks {
        if value == 0 {
            exit_config_error(&format!("{} must be at least 1 (got {})", name, value));
        }
    }
    if cfg.s3.connect_timeout_secs == 0 {
        exit_config_error(&format!(
            "s3.connect_timeout_secs must be at least 1 (got {})",
            cfg.s3.connect_timeout_secs
        ));
    }
    if !s3_turbo_list::utils::is_supported_compression(&cfg.output.compression) {
        exit_config_error(&format!(
            "output.compression '{}' is not supported; use one of: {}",
            cfg.output.compression,
            s3_turbo_list::utils::SUPPORTED_COMPRESSION.join(", ")
        ));
    }
    if let Some(reason) = s3_turbo_list::utils::compression_setting_error(
        &cfg.output.compression,
        cfg.output.compression_level,
    ) {
        exit_config_error(&format!(
            "output.compression_level {} is not valid for '{}' (--compression-level): {}",
            cfg.output.compression_level, cfg.output.compression, reason
        ));
    }
}

/// An unparseable `--addressing-style` used to be dropped silently, leaving
/// the run on whatever the config resolved to — a typo then looked like it
/// had been applied.
fn validate_addressing_style_command(cli: &Cli) {
    let Some(style) = cli.addressing_style.as_deref() else {
        return;
    };
    if style.parse::<config::AddressingStyle>().is_err() {
        exit_config_error(&format!(
            "--addressing-style '{}' is not one of: path, virtual, auto",
            style
        ));
    }
}

/// The prefix the run lists under, with the `/` "whole bucket" spelling
/// normalized away — the same normalization the run itself applies.
fn listing_prefix(cli: &Cli) -> String {
    if cli.prefix == "/" {
        String::new()
    } else {
        cli.prefix.clone()
    }
}

fn validate_continuation_token_command(cli: &Cli, cfg: &S3TurboConfig) {
    let Some(token) = cli.continuation_token.as_deref() else {
        return;
    };
    if token.trim().is_empty() {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--continuation-token cannot be empty".to_string(),
        );
    }
    let Commands::List { region, bucket, .. } = &cli.cmd else {
        return;
    };
    if cli.resume {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--continuation-token cannot be combined with --resume; use checkpoint resume or a continuation token, not both".to_string(),
        );
    }
    if cfg.s3.start_after.is_some() {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--continuation-token cannot be combined with --start-after".to_string(),
        );
    }
    if cli.hints_file.is_some() {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--continuation-token is single-chain only and cannot be combined with --hints-file"
                .to_string(),
        );
    }
    if !cli.no_auto_hints {
        let hints_path = agent::conventional_hints_path_for_prefix(
            bucket,
            region.as_deref(),
            &listing_prefix(cli),
        );
        if std::path::Path::new(&hints_path).exists() {
            exit_before_run(
                agent::ExitCode::CliConfig,
                format!(
                    "--continuation-token is single-chain only, but conventional hints file '{}' exists; pass --no-auto-hints to ignore it",
                    hints_path
                ),
            );
        }
    }
}

/// `--start-after` is a single-chain mode: with multiple hint segments, every
/// segment would override its start with the CLI key and list overlapping
/// ranges, duplicating output rows. Reject explicit multi-segment inputs; the
/// conventional hints cache is skipped at load time (with a log line) instead
/// of erroring, because startup discovery writes it automatically on first run.
/// A `--delimiter` listing is one hierarchical segment: CommonPrefixes are not
/// bounded by a segment's key range, so boundaries from `--hints-file` made
/// neighbouring segments drop or repeat folder rows. Runtime splitting and the
/// hints cache were already off for delimiter runs; the explicit file was the
/// remaining way in.
fn validate_delimiter_hints_command(cli: &Cli) {
    if matches!(cli.cmd, Commands::List { .. })
        && !cli.delimiter.is_empty()
        && cli.hints_file.is_some()
    {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--delimiter lists one hierarchical segment and cannot be combined with --hints-file"
                .to_string(),
        );
    }
}

fn validate_start_after_command(cli: &Cli, cfg: &S3TurboConfig) {
    if cfg.s3.start_after.is_none() {
        return;
    }
    // Diff resolves per-side boundaries itself and already skips hints and
    // discovery when --start-after is set; local commands ignore the flag.
    if !matches!(cli.cmd, Commands::List { .. }) {
        return;
    }
    if cli.resume {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--start-after cannot be combined with --resume; checkpoint segments describe the full key space and would mis-resume a partial-range listing".to_string(),
        );
    }
    if cli.hints_file.is_some() {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--start-after is single-chain only and cannot be combined with --hints-file"
                .to_string(),
        );
    }
}

fn validate_provider_setup_or_exit(cli: &Cli, cfg: &S3TurboConfig) {
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
fn offline_region_resolves() -> bool {
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

fn imds_disabled() -> bool {
    std::env::var("AWS_EC2_METADATA_DISABLED").is_ok_and(|v| v.eq_ignore_ascii_case("true"))
}

/// The sides of a list/diff command that name no region of their own.
fn sides_without_region(cli: &Cli) -> Vec<&'static str> {
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

fn provider_setup_guardrail_warnings(cli: &Cli, cfg: &S3TurboConfig) -> Vec<String> {
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

/// Auto-generated output stem for this run, unique in `dir` (the output
/// directory, or the working directory): a stem whose `.parquet` or `.ks`
/// already exists gets a `_N` suffix instead of overwriting another run's
/// files — two runs started in the same second used to share one name, and
/// the second silently replaced the first's artifacts while both reported
/// success.
fn output_stem(
    region: Option<&str>,
    bucket: &str,
    target_region: Option<&str>,
    target_bucket: Option<&str>,
    prefix: &str,
    dir: Option<&str>,
) -> String {
    let now = Local::now().format("%Y%m%d%H%M%S");
    let stem = output_stem_with_timestamp(
        region,
        bucket,
        target_region,
        target_bucket,
        prefix,
        &now.to_string(),
    );
    let taken = |candidate: &str| {
        [".parquet", ".ks"].iter().any(|ext| {
            let name = format!("{}{}", candidate, ext);
            match dir {
                Some(dir) => std::path::Path::new(dir).join(name).exists(),
                None => std::path::Path::new(&name).exists(),
            }
        })
    };
    if !taken(&stem) {
        return stem;
    }
    (1..)
        .map(|n| format!("{}_{}", stem, n))
        .find(|candidate| !taken(candidate))
        .expect("an unused suffix exists")
}

fn output_stem_with_timestamp(
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

fn sanitize_path_component(value: &str) -> String {
    agent::sanitize_path_component(value)
}

/// `<base>.partN.parquet` files that exist for `base`, in index order.
fn stale_parquet_parts(base: &str) -> Vec<String> {
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
fn validate_distinct_output_paths(
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
fn create_output_parents(cli: &Cli, cfg: &S3TurboConfig) {
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

fn ensure_output_dir(cli: &Cli) {
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

fn print_wrote_summary(outputs: &agent::OutputPathSummary, output_files: usize) {
    println!("Wrote:");
    if let Some(path) = &outputs.parquet_file {
        println!("  Parquet: {}", path);
        for part in agent::parquet_part_paths(path, output_files) {
            println!("  Parquet: {}", part);
        }
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

fn print_summary(metrics: &agent::MetricsSummary, delimiter: &str) {
    println!("Summary:");
    if delimiter.is_empty() {
        println!("  objects:  {}", metrics.streamed_rows);
    } else {
        // A --delimiter listing also emits one row per folder (CommonPrefix),
        // which is not an object; bytes and prefixes count objects only.
        println!(
            "  rows:     {} (objects and '{}' folders)",
            metrics.streamed_rows, delimiter
        );
    }
    println!(
        "  bytes:    {} ({})",
        metrics.bytes_total,
        local_tools::human_bytes(metrics.bytes_total)
    );
    println!("  prefixes: {}", metrics.unique_prefixes);
    if !metrics.top_prefixes.is_empty() {
        println!("Top prefixes:");
        for prefix in metrics.top_prefixes.iter().take(10) {
            println!(
                "  {}  objects={} bytes={} ({})",
                if prefix.prefix.is_empty() {
                    "\"\" (root)"
                } else {
                    prefix.prefix.as_str()
                },
                prefix.objects,
                prefix.bytes,
                local_tools::human_bytes(prefix.bytes)
            );
        }
    }
}

/// One compact human format: a line per check, the resolved endpoint
/// settings, and a next step for what is wrong.
fn print_doctor_report(report: &agent::DoctorReport) {
    for check in &report.checks {
        let label = match check.status.as_str() {
            "ok" => "OK   ",
            "warn" => "WARN ",
            "error" => "ERROR",
            "skipped" => "SKIP ",
            _ => "INFO ",
        };
        println!("{} {}: {}", label, check.name, check.message);
    }
    let config = &report.resolved_config;
    println!(
        "Resolved: provider {}, endpoint {}, addressing {}, concurrency {}, threads {}",
        config.s3.provider.as_deref().unwrap_or("-"),
        config.s3.endpoint_url.as_deref().unwrap_or("-"),
        config.s3.addressing_style,
        config.runtime.max_concurrency,
        config.runtime.worker_threads
    );
    for warning in &report.config_source.warnings {
        println!("WARN  config: {}", warning);
    }
    if let Some(hints) = &report.hints {
        print_doctor_hints(hints);
    }
    for check in &report.checks {
        if check.name == "endpoint_url" && check.status == "error" {
            println!("NEXT  pass --endpoint-url <url>, or set s3.endpoint_url in the config");
        }
    }
    println!("Doctor status: {}", report.status);
}

/// Output files the run would fail to create (exit 5), with the reason.
fn planned_output_problems(outputs: &agent::OutputPathSummary, cli: &Cli) -> Vec<String> {
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

fn build_plan_report(
    cli: &Cli,
    cfg: &S3TurboConfig,
    config_source: agent::ConfigSourceSummary,
    diff_target_endpoint: Option<&str>,
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
        .map(|bucket| {
            checkpoint::checkpoint_path_for_prefix(bucket, inputs.region.as_deref(), &inputs.prefix)
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
            None => agent::diff_per_side_hints_plan(
                inputs.bucket.as_deref(),
                inputs.region.as_deref(),
                &inputs.prefix,
            ),
        }
    } else {
        agent::detect_hints_plan(agent::HintsPlanInputs {
            explicit_hints_file: cli.hints_file.as_deref(),
            bucket: inputs.bucket.as_deref(),
            region: inputs.region.as_deref(),
            prefix: &inputs.prefix,
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
            current_identity.as_ref(),
            None,
        ),
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
        overrides.push("provider".to_string());
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
        if let Some(profile) = cfg
            .s3
            .profile
            .as_deref()
            .filter(|name| profiles::is_endpoint_preset_name(name))
            && !credential_environment_signal_present()
        {
            warnings.push(format!(
                    "--profile '{}' is an endpoint compatibility preset only; credentials still come from AWS_PROFILE or the AWS SDK default credential chain. If '{}' is also your credentials profile name, set AWS_PROFILE={}.",
                    profile, profile, profile
                ));
        }
        warnings.extend(provider_setup_guardrail_warnings(cli, cfg));
        // A misspelled profile applies no preset at all — no endpoint
        // template, no addressing recommendation — and the run proceeds as if
        // none had been asked for. The plan already carries
        // `profile_known: false`; say so where an operator will read it.
        if let Some(name) = cfg.s3.profile.as_deref()
            && profiles::get_profile(name).is_none()
        {
            warnings.push(format!(
                    "--profile '{}' matches no endpoint compatibility preset ({}); no endpoint or addressing defaults were applied",
                    name,
                    profiles::all_profiles()
                        .iter()
                        .map(|profile| profile.name)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
        }
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
        continuation_token: cli.continuation_token.clone(),
        profile: cfg.s3.profile.clone(),
        addressing_style: cfg.s3.addressing_style.to_string(),
        filter: cli.filter.clone(),
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
            let stem = output_stem_with_timestamp(
                region.as_deref(),
                bucket,
                None,
                None,
                &listing_prefix(cli),
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
        Commands::Diff {
            region,
            bucket,
            target_region,
            target_bucket,
            ..
        } => {
            let stem = output_stem_with_timestamp(
                region.as_deref(),
                bucket,
                target_region.as_deref(),
                Some(target_bucket),
                &listing_prefix(cli),
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

/// Resolves once the run has been asked to stop (Ctrl-C / SIGTERM set the
/// quit flag). Startup discovery races against it: its probe rounds run
/// before any segment task exists, so nothing else would notice the signal.
async fn quit_requested(g_state: &core::GlobalState) {
    while !g_state.is_quit() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Key-space boundaries for one diff side: cached hints when present,
/// otherwise startup structural discovery (cached for future runs). The
/// same automatic sources as list mode; explicit --hints-file remains
/// rejected for diff. Empty means single-segment, the pre-parallel
/// behavior.
async fn diff_side_boundaries(
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

    let cache_path = agent::conventional_hints_path_for_prefix(bucket, region, prefix);
    match hints::parse_conventional_hints_file(&cache_path, prefix) {
        Ok(boundaries) => return boundaries,
        Err(e) if std::path::Path::new(&cache_path).exists() => {
            log::warn!("Ignoring conventional hints cache: {}", e);
        }
        Err(_) => {}
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
    let Some(boundaries) = discovered else {
        return Vec::new();
    };
    if !boundaries.is_empty()
        && let Err(e) = auto_hints::write_startup_hints_cache(bucket, region, prefix, &boundaries)
    {
        log::warn!("{}", e);
    }
    boundaries
}

/// Bisect a flat key range into boundaries via single-key ListObjectsV2
/// probes on the given client. Shared by list-mode startup discovery and the
/// per-side diff hints resolver.
async fn discover_flat_boundaries_via_client(
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

// The region supplied by the active subcommand, used for profile endpoint
// templating before the full command dispatch.
/// The endpoint the diff target side lists against. A region-templated
/// profile's preset was applied from the source region; when the endpoint
/// came from that template (not from the user) and the target has its own
/// region, the target gets its own region's host instead of the source's.
fn diff_target_endpoint(
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

fn command_region(cmd: &Commands) -> Option<&str> {
    match cmd {
        Commands::List { region, .. } | Commands::Diff { region, .. } => region.as_deref(),
        Commands::CompatProbe { region, .. } => region.as_deref(),
        _ => None,
    }
}

fn load_hints(
    hints_file: Option<&str>,
    bucket: &str,
    region: Option<&str>,
    prefix: &str,
    delimiter: &str,
    no_auto_hints: bool,
    disabled_for_diff: bool,
) -> Vec<String> {
    // 1. Explicit --hints-file takes absolute precedence.
    if let Some(path) = hints_file {
        match hints::parse_hints_file(path) {
            Ok(boundaries) => {
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

    // 2. A hierarchical run rolls every key under a CommonPrefix, and a page's
    //    CommonPrefixes are not range-filtered: each cached segment would
    //    re-list the same prefix set, multiplying requests and the reported
    //    CommonPrefix count with no parallelism to show for it. Startup
    //    discovery and the diff resolver already skip hints for these runs.
    if !delimiter.is_empty() {
        info!(
            "--delimiter '{}' lists hierarchically: skipping the conventional hints cache and using single-segment listing",
            delimiter
        );
        return vec![];
    }

    // 3. Try the startup-discovery cache at the conventional path.
    let cache_filename = agent::conventional_hints_path_for_prefix(bucket, region, prefix);

    match hints::parse_conventional_hints_file(&cache_filename, prefix) {
        Ok(boundaries) => return boundaries,
        Err(e) if std::path::Path::new(&cache_filename).exists() => {
            // Present but unusable (wrong prefix, malformed). Rediscovery
            // below overwrites it with boundaries that fit this run.
            log::warn!("Ignoring conventional hints cache: {}", e);
        }
        Err(_) => {}
    }

    // 4. No hints — the caller may attempt startup structural discovery
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
    region: Option<&str>,
    bucket: &str,
    prefix: &str,
    addressing_style: &str,
    output: Option<&str>,
    cfg: &S3TurboConfig,
) {
    let rt = build_runtime_or_exit(2);

    let report = rt.block_on(async {
        match compat_probe::run_compat_probe(
            endpoint_url,
            region,
            bucket,
            prefix,
            addressing_style,
            output,
            cfg,
        )
        .await
        {
            Ok(report) => report,
            Err(e) => {
                exit_before_run(
                    agent::ExitCode::OutputWrite,
                    format!("Compat-probe output error: {}", e),
                );
            }
        }
    });
    // `partial` keeps exit 0: the report says which operations failed, and an
    // endpoint that only lacks e.g. encoding-type=url can still be listed.
    // `incompatible` — every operation failed — must not read as success.
    if report.overall_status == "incompatible" {
        let exit_code = if report.failures_are_setup_errors() {
            agent::ExitCode::ProviderSetup
        } else {
            agent::ExitCode::NetworkRetryExhausted
        };
        eprintln!(
            "s3-turbo-list: compat-probe found the endpoint incompatible (exit {}): every probe \
             operation failed; see the report's tests[] for each error.",
            exit_code.code()
        );
        std::process::exit(exit_code.code());
    }
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
