//! How the binary stops: exit codes, the failure epilogue, and the
//! --agent / doctor JSON printed on the way out.

use super::*;

/// Set when the command is `doctor --json` (or doctor under `--agent`), so the
/// early config-validation exits still print a JSON report on stdout: an agent
/// that asked for JSON must not get an empty stdout on the very failures
/// doctor exists to diagnose.
pub(crate) static DOCTOR_JSON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Exit 2 on a config/CLI validation error, in doctor's JSON shape when
/// doctor's JSON output was requested.
pub(crate) fn exit_config_error(message: &str) -> ! {
    exit_doctor_check_error("config_parse", message)
}

/// Set once the command is a real `list` / `diff` / `compat-probe` run (not a
/// dry run): its pre-run failures report like failed runs.
pub(crate) static RUN_COMMAND: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// The output file `pick_output_stem` created to claim its name.
pub(crate) static RESERVED_OUTPUT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// `--agent` on such a run: stdout carries a JSON result even when the run
/// stops before listing.
pub(crate) static AGENT_RUN: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Stop before (or instead of) listing with `code`. Every command prints the
/// reason on stderr as before; a run command also prints the documented
/// `s3-turbo-list: run failed (exit N): <reason>` line and, under `--agent`,
/// a minimal JSON result on stdout — agents branch on those, and pre-run
/// failures (a bad filter, no region, an uncreatable output) used to give
/// neither.
pub(crate) fn exit_before_run(code: agent::ExitCode, message: String) -> ! {
    // `doctor --json` keeps stdout machine-readable on every config error
    // (validators shared with the run used to exit with stdout empty).
    if code == agent::ExitCode::CliConfig && DOCTOR_JSON.get().copied().unwrap_or(false) {
        exit_doctor_check_error("config_parse", &message);
    }
    eprintln!("{}", message);
    run_failure_epilogue(code, &message);
    std::process::exit(code.code())
}

pub(crate) fn run_failure_epilogue(code: agent::ExitCode, message: &str) {
    release_reserved_output();
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
pub(crate) fn exit_doctor_check_error(check: &str, message: &str) -> ! {
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

pub(crate) fn build_runtime_or_exit(worker_threads: usize) -> tokio::runtime::Runtime {
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

/// The `--json` shape of a command that failed before producing its report:
/// stdout stays machine-readable instead of empty.
pub(crate) fn print_json_error(message: &str) {
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

/// Drop the empty file `pick_output_stem` reserved, when the run stops
/// before writing it.
pub(crate) fn release_reserved_output() {
    if let Some(path) = RESERVED_OUTPUT.get()
        && std::fs::metadata(path).is_ok_and(|m| m.len() == 0)
    {
        let _ = std::fs::remove_file(path);
    }
}
