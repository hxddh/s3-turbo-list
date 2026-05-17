// Integration tests for CLI help regression.
// These shell out to Cargo's already-built binary to keep tests fast.
use std::process::Command;

/// Helper: run `s3-turbo-list <args>` and return (exit_code, stdout, stderr).
fn run_cli(args: &[&str]) -> (i32, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_s3-turbo-list"))
        .args(args)
        .output()
        .expect("failed to execute s3-turbo-list test binary");

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (exit_code, stdout, stderr)
}

#[test]
fn test_cli_help_top_level() {
    let (code, stdout, _stderr) = run_cli(&["--help"]);
    assert_eq!(code, 0, "s3-turbo-list --help should exit 0");
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
    assert_eq!(code, 0, "s3-turbo-list list --help should exit 0");
    assert!(
        stdout.contains("--bucket"),
        "list help should contain '--bucket'"
    );
}

#[test]
fn test_cli_help_diff() {
    let (code, stdout, _stderr) = run_cli(&["diff", "--help"]);
    assert_eq!(code, 0, "s3-turbo-list diff --help should exit 0");
    assert!(
        stdout.contains("--target-bucket"),
        "diff help should contain '--target-bucket'"
    );
}

#[test]
fn test_cli_help_auto_hints() {
    let (code, stdout, _stderr) = run_cli(&["auto-hints", "--help"]);
    assert_eq!(code, 0, "s3-turbo-list auto-hints --help should exit 0");
    assert!(
        stdout.contains("bucket"),
        "auto-hints help should mention 'bucket'"
    );
}

#[test]
fn test_cli_help_compat_probe() {
    let (code, stdout, _stderr) = run_cli(&["compat-probe", "--help"]);
    assert_eq!(code, 0, "s3-turbo-list compat-probe --help should exit 0");
    assert!(
        stdout.contains("compat"),
        "compat-probe help should mention 'compat'"
    );
}

#[test]
fn test_cli_help_hints_validate() {
    let (code, stdout, _stderr) = run_cli(&["hints-validate", "--help"]);
    assert_eq!(code, 0, "s3-turbo-list hints-validate --help should exit 0");
    assert!(
        stdout.contains("--hints-file"),
        "hints-validate help should contain '--hints-file'"
    );
}

#[test]
fn test_cli_hints_validate_plain_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hints.txt");
    std::fs::write(&path, "alpha/\nbeta/\n").unwrap();

    let (code, stdout, stderr) =
        run_cli(&["hints-validate", "--hints-file", path.to_str().unwrap()]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Boundary count"));
    assert!(stdout.contains("2"));
}

#[test]
fn test_cli_hints_validate_json_estimates_summary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hints.toml");
    std::fs::write(
        &path,
        r#"bucket = "b"
region = "us-east-1"
total_objects = 30
boundaries = ["m/"]
generated_at = "2026-05-17T00:00:00Z"
scan_mode = "sampled"
sampled_objects = 30
sampled_pages = 2
sample_limit = 30
max_pages = 2
estimate_mode = "sampled"

[[segment_estimates]]
start_after = ""
end_before = "m/"
estimated_objects = 10

[[segment_estimates]]
start_after = "m/"
estimated_objects = 20
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli(&[
        "hints-validate",
        "--hints-file",
        path.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["metadata"]["scan_mode"], "sampled");
    assert_eq!(json["estimate_summary"]["sampled"], true);
    assert_eq!(json["estimate_summary"]["count"], 2);
    assert_eq!(json["estimate_summary"]["min_estimated_objects"], 10);
    assert_eq!(json["estimate_summary"]["max_estimated_objects"], 20);
    assert_eq!(json["estimate_summary"]["total_estimated_objects"], 30);
    assert_eq!(json["first_estimates"].as_array().unwrap().len(), 2);
}

#[test]
fn test_cli_hints_validate_malformed_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hints.txt");
    std::fs::write(&path, "boundaries = [\nalpha/\n]\n").unwrap();

    let (code, _stdout, stderr) =
        run_cli(&["hints-validate", "--hints-file", path.to_str().unwrap()]);
    assert_ne!(code, 0, "malformed hints should fail");
    assert!(stderr.contains("Hints validation failed"));
}
