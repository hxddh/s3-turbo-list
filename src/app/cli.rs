//! Command-line definition, parsing and validation.

use super::*;

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
pub(crate) struct CliArgs {
    #[command(subcommand)]
    pub(crate) cmd: Commands,

    #[command(flatten)]
    pub(crate) global: GlobalArgs,
}

#[derive(Args, Debug, Clone, Default)]
pub(crate) struct GlobalArgs {
    /// Path to TOML config file
    #[arg(long, global = true, help_heading = "Endpoint")]
    pub(crate) config: Option<String>,

    /// S3-compatible provider preset: aws, minio, bos, r2, b2 or oss (sets
    /// the endpoint and addressing style; credentials profiles go in
    /// AWS_PROFILE)
    #[arg(long, global = true, alias = "profile", help_heading = "Endpoint")]
    pub(crate) provider: Option<String>,

    /// Custom S3 endpoint URL
    #[arg(
        long = "endpoint-url",
        global = true,
        alias = "endpoint",
        help_heading = "Endpoint"
    )]
    pub(crate) endpoint: Option<String>,

    /// S3 addressing style: path, virtual, or auto
    #[arg(long, global = true, help_heading = "Endpoint")]
    pub(crate) addressing_style: Option<String>,
}

/// What to list: shared by list and diff.
#[derive(Args, Debug, Clone, Default)]
pub(crate) struct SourceArgs {
    /// Key prefix to list, e.g. `logs/2026/` (default: the whole bucket)
    #[arg(
        short,
        long,
        default_value = "",
        hide_default_value = true,
        help_heading = "Source"
    )]
    pub(crate) prefix: String,

    /// ListObjectsV2 delimiter: '/' lists one level (objects plus one row per
    /// folder); the default lists every key recursively
    #[arg(
        long,
        default_value = "",
        hide_default_value = true,
        help_heading = "Source"
    )]
    pub(crate) delimiter: String,

    /// Start listing after this key (lists one segment; not with
    /// --hints-file or --resume)
    #[arg(long, help_heading = "Source")]
    pub(crate) start_after: Option<String>,

    /// Object filter expression (e.g. "SOURCE.size > 1000")
    #[arg(short, long, help_heading = "Source")]
    pub(crate) filter: Option<String>,
}

/// Where results go: shared by list and diff.
#[derive(Args, Debug, Clone, Default)]
pub(crate) struct OutputArgs {
    /// Directory for the auto-named outputs (Parquet, KeySpace, log)
    #[arg(long, help_heading = "Output")]
    pub(crate) output_dir: Option<String>,

    /// Parquet output path; the KeySpace file is written beside it as
    /// `<name>.ks`
    #[arg(long, help_heading = "Output")]
    pub(crate) output_parquet_file: Option<String>,

    /// Parquet compression codec: zstd (default), gzip, snappy, lz4,
    /// lz4_raw, brotli, or uncompressed
    #[arg(long, help_heading = "Output")]
    pub(crate) compression: Option<String>,

    /// Write the run log to a file (`<name>.log` beside the outputs)
    #[arg(short, long, help_heading = "Output")]
    pub(crate) log: bool,

    /// KeySpace output path (deprecated: derived from --output-parquet-file)
    #[arg(long, hide = true)]
    pub(crate) output_ks_file: Option<String>,

    /// Log file path (deprecated: --log names it after the outputs)
    #[arg(long, hide = true)]
    pub(crate) output_log_file: Option<String>,

    /// Parquet compression level (config: output.compression_level)
    #[arg(long, hide = true)]
    pub(crate) compression_level: Option<u32>,
}

/// How hard to list: shared by list and diff. Only --concurrency is shown;
/// the rest are for debugging and tests.
#[derive(Args, Debug, Clone, Default)]
pub(crate) struct TuningArgs {
    /// Max concurrent list operations (default 100)
    #[arg(short, long, help_heading = "Run")]
    pub(crate) concurrency: Option<usize>,

    /// Worker threads (default: CPU count)
    #[arg(short = 'T', long, hide = true)]
    pub(crate) threads: Option<usize>,

    /// Max keys per ListObjectsV2 page
    #[arg(long, hide = true, value_parser = clap::value_parser!(i32).range(1..))]
    pub(crate) max_keys: Option<i32>,

    /// Disable startup discovery (lists one segment unless --hints-file)
    #[arg(long, hide = true)]
    pub(crate) no_auto_hints: bool,
}

/// Machine-facing switches: shared by list and diff.
#[derive(Args, Debug, Clone, Default)]
pub(crate) struct AutomationArgs {
    /// Print the resolved plan as JSON and exit without contacting S3
    #[arg(long, help_heading = "Automation")]
    pub(crate) dry_run: bool,

    /// Print the run manifest JSON on stdout and keep stderr quiet
    #[arg(long, help_heading = "Automation")]
    pub(crate) agent: bool,

    /// Write the run manifest JSON to this path
    #[arg(long, help_heading = "Automation")]
    pub(crate) run_manifest: Option<String>,

    /// Write S3 request trace events as JSONL to this file (`-`: stderr)
    #[arg(long, help_heading = "Automation")]
    pub(crate) trace_compat: Option<String>,

    /// Write the dry-run plan to this path (deprecated: `--dry-run > file`)
    #[arg(long, hide = true, requires = "dry_run")]
    pub(crate) plan_json: Option<String>,

    /// Trace S3 requests to stderr (deprecated: `--trace-compat -`)
    #[arg(long, hide = true)]
    pub(crate) debug_s3: bool,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
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
pub(crate) struct Cli {
    pub(crate) cmd: Commands,
    pub(crate) config: Option<String>,
    pub(crate) prefix: String,
    pub(crate) threads: Option<usize>,
    pub(crate) concurrency: Option<usize>,
    pub(crate) hints_file: Option<String>,
    pub(crate) filter: Option<String>,
    pub(crate) log: bool,
    pub(crate) endpoint: Option<String>,
    pub(crate) output_log_file: Option<String>,
    pub(crate) output_ks_file: Option<String>,
    pub(crate) output_parquet_file: Option<String>,
    pub(crate) compression: Option<String>,
    pub(crate) compression_level: Option<u32>,
    pub(crate) output_dir: Option<String>,
    pub(crate) resume: bool,
    pub(crate) no_auto_hints: bool,
    pub(crate) delimiter: String,
    pub(crate) max_keys: Option<i32>,
    pub(crate) start_after: Option<String>,
    pub(crate) continuation_token: Option<String>,
    pub(crate) profile: Option<String>,
    pub(crate) addressing_style: Option<String>,
    pub(crate) trace_compat: Option<String>,
    pub(crate) agent: bool,
    pub(crate) dry_run: bool,
    pub(crate) plan_json: Option<String>,
    pub(crate) run_manifest: Option<String>,
    pub(crate) summary_only: bool,
    /// Deprecated spellings used on this command line, for a warning.
    pub(crate) deprecated: Vec<&'static str>,
}

impl Cli {
    pub(crate) fn from_args(args: CliArgs) -> Self {
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
pub(crate) fn hoist_command_flags(args: Vec<std::ffi::OsString>) -> Vec<std::ffi::OsString> {
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
pub(crate) enum ListOutputFormat {
    Parquet,
    Tsv,
    Ndjson,
    Summary,
}

impl ListOutputFormat {
    pub(crate) fn writes_artifacts(self) -> bool {
        matches!(self, Self::Parquet)
    }

    pub(crate) fn writes_stdout_rows(self) -> bool {
        matches!(self, Self::Tsv | Self::Ndjson)
    }

    pub(crate) fn as_str(self) -> &'static str {
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

/// Parse the command line. A usage error under `--agent` also prints the
/// failed-run JSON, like every other failure before a run starts.
pub(crate) fn parse_cli() -> Cli {
    let argv = hoist_command_flags(std::env::args_os().collect());
    match CliArgs::try_parse_from(&argv) {
        Ok(args) => Cli::from_args(args),
        Err(e) => {
            use clap::error::ErrorKind;
            let has = |flag: &str| argv.iter().any(|arg| arg == flag);
            let usage_error = !matches!(
                e.kind(),
                ErrorKind::DisplayHelp
                    | ErrorKind::DisplayVersion
                    | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            );
            // `doctor --json` promises JSON on stdout for every config error.
            if usage_error && has("doctor") && has("--json") {
                let _ = e.print();
                let _ = DOCTOR_JSON.set(true);
                let rendered = e.to_string();
                let first = rendered.lines().next().unwrap_or_default();
                exit_doctor_check_error("cli", first.trim_start_matches("error: "));
            }
            let agent = has("--agent");
            if agent && usage_error {
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

/// Set the command's region (both sides of a diff) where none was given.
pub(crate) fn fill_default_region(cmd: &mut Commands, default_region: &str) {
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

pub(crate) fn validate_output_format_command(cli: &Cli) {
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
pub(crate) fn endpoint_url_looks_usable(endpoint: &str) -> bool {
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
pub(crate) fn validate_runtime_values(cfg: &S3TurboConfig) {
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
pub(crate) fn validate_addressing_style_command(cli: &Cli) {
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
pub(crate) fn listing_prefix(cli: &Cli) -> String {
    if cli.prefix == "/" {
        String::new()
    } else {
        cli.prefix.clone()
    }
}

pub(crate) fn validate_continuation_token_command(cli: &Cli, cfg: &S3TurboConfig) {
    let Some(token) = cli.continuation_token.as_deref() else {
        return;
    };
    if token.trim().is_empty() {
        exit_before_run(
            agent::ExitCode::CliConfig,
            "--continuation-token cannot be empty".to_string(),
        );
    }
    if !matches!(cli.cmd, Commands::List { .. }) {
        return;
    }
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
}

/// `--start-after` is a single-chain mode: with multiple hint segments, every
/// segment would override its start with the CLI key and list overlapping
/// ranges, duplicating output rows. Reject explicit multi-segment inputs.
/// A `--delimiter` listing is one hierarchical segment: CommonPrefixes are not
/// bounded by a segment's key range, so boundaries from `--hints-file` made
/// neighbouring segments drop or repeat folder rows. Runtime splitting and the
/// hints cache were already off for delimiter runs; the explicit file was the
/// remaining way in.
pub(crate) fn validate_delimiter_hints_command(cli: &Cli) {
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

pub(crate) fn validate_start_after_command(cli: &Cli, cfg: &S3TurboConfig) {
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

pub(crate) fn list_output_format(cli: &Cli) -> Option<ListOutputFormat> {
    match &cli.cmd {
        Commands::List { output_format, .. } => Some(*output_format),
        _ => None,
    }
}

/// Whether the run writes Parquet and KeySpace files: list in parquet
/// format, and diff. (compat-probe writes only its report and log.)
pub(crate) fn list_writes_artifacts(cli: &Cli) -> bool {
    match &cli.cmd {
        Commands::List { output_format, .. } => {
            !cli.summary_only && output_format.writes_artifacts()
        }
        Commands::Diff { .. } => true,
        _ => false,
    }
}

pub(crate) fn cli_config_overrides(cli: &Cli) -> Vec<String> {
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

pub(crate) fn command_region(cmd: &Commands) -> Option<&str> {
    match cmd {
        Commands::List { region, .. } | Commands::Diff { region, .. } => region.as_deref(),
        Commands::CompatProbe { region, .. } => region.as_deref(),
        _ => None,
    }
}
