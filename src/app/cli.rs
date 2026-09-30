//! Command-line definition, parsing and validation.

use super::*;

// Only the endpoint options are global. Every other option belongs to the
// commands that use it, so clap rejects a misplaced flag itself and each
// command's --help lists only what it takes. A run option written before the
// command name (`s3-turbo-list --output-dir out list …`, the pre-0.37
// spelling, removed in 0.39) is a usage error that names the command it must
// follow (`misplaced_option`).

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
    #[arg(long, global = true, help_heading = "Endpoint")]
    pub(crate) provider: Option<String>,

    /// S3-compatible endpoint URL (default: the provider's, or AWS S3)
    #[arg(
        long = "endpoint-url",
        value_name = "URL",
        global = true,
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
    #[arg(long, help_heading = "Source")]
    pub(crate) delimiter: Option<String>,

    /// Start listing after this key (lists one sequential chain)
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

        /// Keep stderr quiet (no log, no default stderr trace; only the
        /// final line on failure) and print a JSON result on stdout even
        /// when the probe stops before running
        #[arg(long)]
        agent: bool,

        /// Write S3 request trace events as JSONL to this file (`-`: stderr;
        /// the default without --agent)
        #[arg(long)]
        trace_compat: Option<String>,
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
    pub(crate) output_parquet_file: Option<String>,
    pub(crate) compression: Option<String>,
    pub(crate) compression_level: Option<u32>,
    pub(crate) output_dir: Option<String>,
    pub(crate) resume: bool,
    pub(crate) no_auto_hints: bool,
    pub(crate) delimiter: String,
    /// `--delimiter` was given on the command line (even as '').
    pub(crate) delimiter_explicit: bool,
    pub(crate) max_keys: Option<i32>,
    pub(crate) start_after: Option<String>,
    pub(crate) provider: Option<String>,
    pub(crate) addressing_style: Option<String>,
    pub(crate) trace_compat: Option<String>,
    pub(crate) agent: bool,
    pub(crate) dry_run: bool,
    pub(crate) run_manifest: Option<String>,
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
            output_parquet_file: None,
            compression: None,
            compression_level: None,
            output_dir: None,
            resume: false,
            no_auto_hints: false,
            delimiter: String::new(),
            delimiter_explicit: false,
            max_keys: None,
            start_after: None,
            provider,
            addressing_style,
            trace_compat: None,
            agent: false,
            dry_run: false,
            run_manifest: None,
        };
        let mut groups = None;
        match &mut cli.cmd {
            Commands::List {
                resume,
                hints_file,
                source,
                output,
                tuning,
                automation,
                ..
            } => {
                cli.resume = *resume;
                cli.hints_file = hints_file.clone();
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
                ..
            } => {
                cli.prefix = prefix.clone();
                cli.log = *log;
                cli.dry_run = *dry_run;
                cli.agent = *agent;
                cli.trace_compat = trace_compat.clone();
            }
            Commands::Doctor {
                hints_file,
                filter,
                output_dir,
                output_parquet_file,
                trace_compat,
                ..
            } => {
                cli.hints_file = hints_file.clone();
                cli.filter = filter.clone();
                cli.output_dir = output_dir.clone();
                cli.output_parquet_file = output_parquet_file.clone();
                cli.trace_compat = trace_compat.clone();
            }
            _ => {}
        }
        if let Some((source, output, tuning, automation)) = groups {
            cli.prefix = source.prefix;
            cli.delimiter_explicit = source.delimiter.is_some();
            cli.delimiter = source.delimiter.unwrap_or_default();
            cli.start_after = source.start_after;
            cli.filter = source.filter;
            cli.output_dir = output.output_dir;
            cli.output_parquet_file = output.output_parquet_file;
            cli.compression = output.compression;
            cli.log = output.log;
            cli.compression_level = output.compression_level;
            cli.concurrency = tuning.concurrency;
            cli.threads = tuning.threads;
            cli.max_keys = tuning.max_keys;
            cli.no_auto_hints = tuning.no_auto_hints;
            cli.dry_run = automation.dry_run;
            cli.agent = automation.agent;
            cli.run_manifest = automation.run_manifest;
            cli.trace_compat = automation.trace_compat;
        }
        cli
    }
}

/// How an option token resolves against a set of arguments: whether every
/// option in it is known, and whether it consumes the next token as its value.
/// A cluster of short options (`-lc 5`) is walked flag by flag; the last one
/// takes the next token when it takes a value and nothing follows it in the
/// token.
fn classify_option(args: &[&clap::Arg], token: &str) -> Option<bool> {
    let takes_value = |arg: &clap::Arg| arg.get_action().takes_values();
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
            .map(|arg| takes_value(arg) && !inline);
    }
    let cluster = token.strip_prefix('-')?;
    if cluster.is_empty() {
        return None;
    }
    for (at, short) in cluster.char_indices() {
        let arg = args.iter().find(|arg| arg.get_short() == Some(short))?;
        if takes_value(arg) {
            // The rest of the token is the value (`-c5`, `-lc5`, `-c=5`).
            return Some(at + short.len_utf8() == cluster.len());
        }
    }
    Some(false)
}

/// Where the command name is in `args` (index 0 is the program), skipping
/// option values, and which command it is. `list --bucket doctor` is a list
/// command: the scan stops at the first command name that is not a value.
pub(crate) fn find_command(args: &[String]) -> Option<(usize, clap::Command)> {
    let command = CliArgs::command();
    let all: Vec<&clap::Arg> = command
        .get_arguments()
        .chain(
            command
                .get_subcommands()
                .flat_map(|sub| sub.get_arguments()),
        )
        .collect();
    let mut index = 1;
    while index < args.len() {
        let token = args[index].as_str();
        if token == "--" {
            return None;
        }
        if let Some(sub) = command.find_subcommand(token) {
            return Some((index, sub.clone()));
        }
        if token.starts_with('-') && classify_option(&all, token) == Some(true) {
            index += 1;
        }
        index += 1;
    }
    None
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

/// The command definition as parsed, `--help`ed, completed and rendered as a
/// man page. The endpoint options are global for the commands that load the
/// config; the local tools (`manifest-summary`, `guide`, `completions`,
/// `man`) accept and ignore them, as before, but do not list them in their
/// `--help`.
pub(crate) fn cli_command() -> clap::Command {
    let command = CliArgs::command();
    let globals: Vec<clap::Arg> = command
        .get_arguments()
        .filter(|arg| arg.is_global_set())
        .map(|arg| arg.clone().global(false).hide(true))
        .collect();
    ["manifest-summary", "guide", "completions", "man"]
        .into_iter()
        .fold(command, |command, name| {
            command.mut_subcommand(name, |sub| sub.args(globals.iter().cloned()))
        })
}

/// Spellings removed in 0.38 and 0.39, and what replaces each: a usage error
/// that names one gets the replacement appended.
const REMOVED_SPELLINGS: &[(&str, &str)] = &[
    ("--endpoint", "removed in 0.39: use --endpoint-url"),
    (
        "--agent",
        "doctor --agent and manifest-summary --agent were removed in 0.39: use --json",
    ),
    (
        "--simple",
        "removed in 0.39: the doctor output is always compact",
    ),
    (
        "--fix-suggestions",
        "removed in 0.39: doctor always prints suggestions",
    ),
    ("--profile", "removed in 0.38: use --provider"),
    (
        "--summary-only",
        "removed in 0.38: use --output-format summary",
    ),
    (
        "--plan-json",
        "removed in 0.38: redirect the plan instead (--dry-run > plan.json)",
    ),
    ("--debug-s3", "removed in 0.38: use --trace-compat -"),
    (
        "--continuation-token",
        "removed in 0.38: use --resume or --start-after",
    ),
    (
        "--output-ks-file",
        "removed in 0.38: the KeySpace file is always <parquet stem>.ks",
    ),
    (
        "--output-log-file",
        "removed in 0.38: --log writes <name>.log beside the outputs",
    ),
    (
        "init-config",
        "removed in 0.38: write the TOML config by hand (see docs/providers.md)",
    ),
];

/// The replacement for a removed spelling a usage error names.
fn removed_spelling_hint(error: &clap::Error) -> Option<&'static str> {
    use clap::error::{ContextKind, ContextValue};
    let named = [ContextKind::InvalidArg, ContextKind::InvalidSubcommand]
        .into_iter()
        .find_map(|kind| match error.get(kind) {
            Some(ContextValue::String(value)) => Some(value.clone()),
            _ => None,
        })?;
    let name = named
        .split_once('=')
        .map_or(named.as_str(), |(name, _)| name);
    REMOVED_SPELLINGS
        .iter()
        .find(|(spelling, _)| *spelling == name)
        .map(|(_, hint)| *hint)
}

/// The first option written before the command name that the command takes
/// (`s3-turbo-list --output-dir out list …`, the pre-0.37 spelling, removed
/// in 0.39), as a message that says where it goes.
fn misplaced_option(strs: &[String], at: usize, sub: &clap::Command) -> Option<String> {
    let command = CliArgs::command();
    let top_level: Vec<&clap::Arg> = command.get_arguments().collect();
    let sub_args: Vec<&clap::Arg> = sub.get_arguments().collect();
    let mut index = 1;
    while index < at {
        let token = strs[index].as_str();
        index += 1;
        if !token.starts_with('-') {
            continue;
        }
        if let Some(takes_value) = classify_option(&top_level, token) {
            index += usize::from(takes_value);
            continue;
        }
        classify_option(&sub_args, token)?;
        let name = token.split_once('=').map_or(token, |(name, _)| name);
        let globals: Vec<String> = top_level
            .iter()
            .filter(|arg| arg.is_global_set())
            .filter_map(|arg| arg.get_long().map(|long| format!("--{}", long)))
            .collect();
        return Some(format!(
            "option '{}' must follow the command name `{}` (options before the command name \
             were removed in 0.39; only {} may come first)",
            name,
            sub.get_name(),
            globals.join(", ")
        ));
    }
    None
}

/// Parse the command line. A usage error under `--agent` also prints the
/// failed-run JSON, like every other failure before a run starts, and one
/// under `doctor --json` prints doctor's JSON.
pub(crate) fn parse_cli() -> Cli {
    use clap::FromArgMatches;
    use clap::error::ErrorKind;
    let argv: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let strs: Vec<String> = argv
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let e = match cli_command()
        .try_get_matches_from(&argv)
        .and_then(|matches| CliArgs::from_arg_matches(&matches))
    {
        Ok(args) => return Cli::from_args(args),
        Err(e) => e,
    };
    if matches!(
        e.kind(),
        ErrorKind::DisplayHelp
            | ErrorKind::DisplayVersion
            | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
    ) {
        e.exit()
    }
    let command = find_command(&strs);
    let misplaced = command
        .as_ref()
        .filter(|_| e.kind() == ErrorKind::UnknownArgument)
        .and_then(|(at, sub)| misplaced_option(&strs, *at, sub));
    let hint = removed_spelling_hint(&e);
    let rendered = e.to_string();
    let first = rendered
        .lines()
        .next()
        .unwrap_or_default()
        .trim_start_matches("error: ");
    let reason = match (&misplaced, hint) {
        (Some(message), _) => message.clone(),
        (None, Some(hint)) => format!("{} ({})", first, hint),
        (None, None) => first.to_string(),
    };
    let print_error = || match &misplaced {
        Some(message) => eprintln!("error: {}\n\nFor more information, try '--help'.", message),
        None => {
            let _ = e.print();
            if let Some(hint) = hint {
                eprintln!("note: {}", hint);
            }
        }
    };
    // Route on the command found by skipping option values, never on raw
    // strings anywhere in argv (`list --bucket doctor --json` is a list
    // usage error). A run command's `--agent` counts on either side of its
    // name: `s3-turbo-list --agent list …` is a misplaced option, and the
    // agent still gets its JSON result.
    if let Some((at, sub)) = command.as_ref() {
        let args = || {
            strs.iter()
                .enumerate()
                .skip(1)
                .take_while(|(_, arg)| arg.as_str() != "--")
        };
        match sub.get_name() {
            // `doctor --json` promises JSON on stdout for every config error.
            "doctor" if args().any(|(index, arg)| index > *at && arg == "--json") => {
                let _ = DOCTOR_JSON.set(true);
                exit_doctor_check_error("cli", &reason);
            }
            "list" | "diff" | "compat-probe" if args().any(|(_, arg)| arg == "--agent") => {
                print_error();
                let _ = RUN_COMMAND.set(true);
                let _ = AGENT_RUN.set(true);
                run_failure_epilogue(agent::ExitCode::CliConfig, &reason);
                std::process::exit(agent::ExitCode::CliConfig.code());
            }
            _ => {}
        }
    }
    print_error();
    std::process::exit(e.exit_code())
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

/// `--agent` prints the run manifest on stdout, which tsv and ndjson reserve
/// for rows: the run stops with exit 2, and its dry-run plan is `blocked`.
pub(crate) fn agent_output_format_conflict(cli: &Cli) -> Option<String> {
    let format = list_output_format(cli)?;
    (cli.agent && format.writes_stdout_rows()).then(|| {
        format!(
            "--agent writes the run manifest to stdout and cannot be combined with \
             --output-format {}; use --run-manifest instead",
            format
        )
    })
}

pub(crate) fn validate_output_format_command(cli: &Cli) {
    if cli.dry_run {
        // The plan reports it (`blocked`) and the dry run exits 2 after it.
        return;
    }
    if let Some(problem) = agent_output_format_conflict(cli) {
        exit_before_run(agent::ExitCode::CliConfig, problem);
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

/// `--start-after` is a single-chain mode: with multiple hint segments, every
/// segment would override its start with the CLI key and list overlapping
/// ranges, duplicating output rows. Reject explicit multi-segment inputs.
/// A `--delimiter` listing is one hierarchical segment: CommonPrefixes are not
/// bounded by a segment's key range, so boundaries from `--hints-file` made
/// neighbouring segments drop or repeat folder rows. Runtime splitting and
/// startup discovery are off for delimiter runs; the explicit file was the
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
        Commands::List { output_format, .. } => output_format.writes_artifacts(),
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
    if cli.provider.is_some() {
        overrides.push("provider".to_string());
    }
    if cli.trace_compat.is_some() {
        overrides.push("trace_compat".to_string());
    }
    if cli.start_after.is_some() {
        overrides.push("start_after".to_string());
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
