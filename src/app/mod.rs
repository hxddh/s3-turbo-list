//! The `s3tl` binary: command line, the result contract, and the runs.
//!
//! Output discipline: stdout carries exactly one JSON result document —
//! except with `list -o -`, where stdout carries NDJSON rows and the result
//! is the last line on stderr. Everything else on stderr is for humans.

mod check;
mod cli;
mod job;
mod result;
mod run;

use clap::Parser;
use result::{ExitCode, Failure, RunResult};
use std::path::PathBuf;

/// What a command produced, ready to be emitted.
pub struct Outcome {
    pub result: RunResult,
    /// stdout carries data rows; the result goes to stderr.
    pub stdout_is_data: bool,
    /// The dataset directory that also receives the result as `_result.json`.
    pub result_dir: Option<PathBuf>,
}

impl Outcome {
    pub fn plain(result: RunResult) -> Self {
        Self {
            result,
            stdout_is_data: false,
            result_dir: None,
        }
    }
}

/// File name of the result copy inside a dataset directory.
const RESULT_FILE: &str = "_result.json";

pub fn main() -> i32 {
    install_panic_hook();
    let cli = match cli::Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => return usage_error(err),
    };
    init_logging(cli.global.verbose);
    let command = cli.cmd.name();
    let outcome = std::panic::catch_unwind(|| dispatch(&cli)).unwrap_or_else(|_| {
        let mut result = RunResult::new(command);
        result.fail(
            Failure::new(ExitCode::Internal, "internal.panic", panic_message())
                .hint("this is a bug; please report it with the command line"),
        );
        Outcome::plain(result)
    });
    emit(outcome)
}

fn dispatch(cli: &cli::Cli) -> Outcome {
    if matches!(cli.cmd, cli::Command::Check { .. }) {
        return check::run(cli);
    }
    let mut result = RunResult::new(cli.cmd.name());
    let job = match job::prepare(cli) {
        Ok(job) => job,
        Err(failure) => {
            result.fail(failure);
            return Outcome::plain(result);
        }
    };
    if cli.global.dry_run {
        run::plan(&job, &mut result);
        return Outcome::plain(result);
    }
    run::execute(job, result)
}

/// A command line clap rejects: help and version print as usual (exit 0);
/// anything else is a usage error with a result document.
fn usage_error(err: clap::Error) -> i32 {
    use clap::error::ErrorKind;
    if matches!(
        err.kind(),
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
    ) {
        let _ = err.print();
        return 0;
    }
    let rendered = err.render().to_string();
    let message = if err.kind() == ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand {
        "no command given: expected list, diff or check".to_string()
    } else {
        // The error and its details, without the usage block clap appends.
        rendered
            .lines()
            .map(str::trim)
            .take_while(|l| !l.starts_with("Usage:") && !l.starts_with("For more information"))
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
            .trim_start_matches("error: ")
            .to_string()
    };
    // The command, when the command line named one.
    let command = std::env::args()
        .skip(1)
        .find_map(|arg| match arg.as_str() {
            "list" => Some("list"),
            "diff" => Some("diff"),
            "check" => Some("check"),
            _ => None,
        })
        .unwrap_or("unknown");
    let mut result = RunResult::new(command);
    result.fail(
        Failure::usage("usage.invalid_argument", message).hint("run `s3tl --help` for usage"),
    );
    eprint!("{}", rendered);
    println!("{}", result.to_json());
    ExitCode::Usage.code()
}

/// Print the result, keep a copy in the dataset directory, and return the
/// exit code.
fn emit(mut outcome: Outcome) -> i32 {
    if let Some(dir) = &outcome.result_dir
        && let Err(e) = write_result_file(dir, &outcome.result)
    {
        if outcome.result.ok {
            outcome.result.fail(Failure::output(format!(
                "cannot write {}: {}",
                RESULT_FILE, e
            )));
        } else {
            outcome
                .result
                .warnings
                .push(format!("cannot write {}: {}", RESULT_FILE, e));
        }
    }
    let result = &outcome.result;
    if let Some(error) = &result.error {
        // One line for humans; agents read the JSON.
        eprintln!(
            "s3tl: {} (exit {}): {}: {}{}",
            serde_json::to_value(result.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default(),
            result.exit_code,
            error.code,
            error.message,
            error
                .hint
                .as_deref()
                .map(|h| format!(" (hint: {})", h))
                .unwrap_or_default()
        );
    }
    if outcome.stdout_is_data {
        eprintln!(
            "{}",
            serde_json::to_string(result).unwrap_or_else(|_| result.to_json())
        );
    } else {
        println!("{}", result.to_json());
    }
    result.exit_code
}

/// Atomically write `_result.json` into the dataset directory.
fn write_result_file(dir: &std::path::Path, result: &RunResult) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = dir.join(format!(".{}.tmp", RESULT_FILE));
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(result.to_json().as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&tmp, dir.join(RESULT_FILE))?;
    s3tl::utils::sync_dir(dir)
}

static PANIC_MESSAGE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Keep the first panic's message for the result; the default hook still
/// prints the backtrace hint on stderr.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Ok(mut slot) = PANIC_MESSAGE.lock()
            && slot.is_none()
        {
            *slot = Some(info.to_string());
        }
        default(info);
    }));
}

fn panic_message() -> String {
    PANIC_MESSAGE
        .lock()
        .ok()
        .and_then(|slot| slot.clone())
        .unwrap_or_else(|| "panic".to_string())
}

/// stderr diagnostics: warnings by default, `-v` info, `-vv` debug;
/// `RUST_LOG` overrides.
fn init_logging(verbose: u8) {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        match verbose {
            0 => "warn",
            1 => "warn,s3tl=info",
            _ => "info,s3tl=debug",
        }
        .to_string()
    });
    env_logger::Builder::new()
        .parse_filters(&filter)
        .format_timestamp_millis()
        .target(env_logger::Target::Stderr)
        .init();
}
