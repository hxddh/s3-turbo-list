// Integration tests for CLI help regression.
// These shell out to `cargo run` to verify help output.
use std::process::Command;

/// Helper: run `cargo run -- <args>` and return (exit_code, stdout, stderr).
fn run_cli(args: &[&str]) -> (i32, String, String) {
    let output = Command::new("cargo")
        .arg("run")
        .arg("--")
        .args(args)
        .output()
        .expect("failed to execute cargo run");

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (exit_code, stdout, stderr)
}

#[test]
fn test_cli_help_top_level() {
    let (code, stdout, _stderr) = run_cli(&["--help"]);
    assert_eq!(code, 0, "cargo run -- --help should exit 0");
    assert!(
        stdout.contains("s3-turbo-list"),
        "help output should contain 's3-turbo-list'"
    );
    assert!(
        stdout.contains("list") || stdout.contains("List"),
        "help output should mention 'list' subcommand"
    );
}

#[test]
fn test_cli_help_list() {
    let (code, stdout, _stderr) = run_cli(&["list", "--help"]);
    assert_eq!(code, 0, "cargo run -- list --help should exit 0");
    assert!(
        stdout.contains("--bucket"),
        "list help should contain '--bucket'"
    );
}

#[test]
fn test_cli_help_diff() {
    let (code, stdout, _stderr) = run_cli(&["diff", "--help"]);
    assert_eq!(code, 0, "cargo run -- diff --help should exit 0");
    assert!(
        stdout.contains("--target-bucket"),
        "diff help should contain '--target-bucket'"
    );
}

#[test]
fn test_cli_help_auto_hints() {
    let (code, stdout, _stderr) = run_cli(&["auto-hints", "--help"]);
    assert_eq!(code, 0, "cargo run -- auto-hints --help should exit 0");
    assert!(
        stdout.contains("bucket"),
        "auto-hints help should mention 'bucket'"
    );
}

#[test]
fn test_cli_help_compat_probe() {
    let (code, stdout, _stderr) = run_cli(&["compat-probe", "--help"]);
    assert_eq!(code, 0, "cargo run -- compat-probe --help should exit 0");
    assert!(
        stdout.contains("compat"),
        "compat-probe help should mention 'compat'"
    );
}
