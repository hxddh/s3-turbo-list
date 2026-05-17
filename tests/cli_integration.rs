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
fn test_cli_help_agent_local_commands() {
    let (code, stdout, _stderr) = run_cli(&["config-inspect", "--help"]);
    assert_eq!(code, 0, "config-inspect --help should exit 0");
    assert!(stdout.contains("--json"));

    let (code, stdout, _stderr) = run_cli(&["doctor", "--help"]);
    assert_eq!(code, 0, "doctor --help should exit 0");
    assert!(stdout.contains("--local-only"));
}

#[test]
fn test_cli_config_inspect_json_success() {
    let (code, stdout, stderr) = run_cli(&["config-inspect", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["status"], "ok");
    assert!(json["resolved_config"]["runtime"]["worker_threads"].is_number());
    assert!(json["resolved_config"]["s3"]["addressing_style"].is_string());
}

#[test]
fn test_cli_doctor_json_local_only_success() {
    let (code, stdout, stderr) = run_cli(&["doctor", "--local-only", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["local_only"], true);
    let checks = json["checks"].as_array().unwrap();
    assert!(checks
        .iter()
        .any(|check| check["name"] == "network" && check["status"] == "skipped"));
}

#[test]
fn test_cli_dry_run_plan_json_list_no_cloud() {
    let dir = tempfile::tempdir().unwrap();
    let plan_path = dir.path().join("plan.json");
    let ks_path = dir.path().join("out.ks");
    let parquet_path = dir.path().join("out.parquet");

    let (code, stdout, stderr) = run_cli(&[
        "--dry-run",
        "--plan-json",
        plan_path.to_str().unwrap(),
        "--output-ks-file",
        ks_path.to_str().unwrap(),
        "--output-parquet-file",
        parquet_path.to_str().unwrap(),
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        stdout.is_empty(),
        "plan-json without --agent should not print stdout"
    );
    assert!(plan_path.exists());
    assert!(!ks_path.exists(), "dry-run must not create KS output");
    assert!(
        !parquet_path.exists(),
        "dry-run must not create Parquet output"
    );

    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(plan_path).unwrap()).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["status"], "ok");
    assert_eq!(
        json["network"],
        "none: dry-run only resolves local configuration and planned paths"
    );
    assert_eq!(json["inputs"]["mode"], "list");
    assert_eq!(json["inputs"]["bucket"], "agent-test-bucket");
    assert_eq!(json["outputs"]["ks_file"], ks_path.to_str().unwrap());
    assert_eq!(
        json["outputs"]["parquet_file"],
        parquet_path.to_str().unwrap()
    );
}

#[test]
fn test_cli_dry_run_agent_stdout_json() {
    let (code, stdout, stderr) = run_cli(&[
        "--agent",
        "--dry-run",
        "auto-hints",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["inputs"]["mode"], "auto-hints");
    assert_eq!(
        json["outputs"]["hints_file"],
        "us-east-1_agent-test-bucket_hints.toml"
    );
    assert!(json["warnings"][0]
        .as_str()
        .unwrap()
        .contains("auto-hints will scan S3 pages"));
}

#[test]
fn test_cli_bad_config_exits_with_config_code() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "[s3\n").unwrap();

    let (code, _stdout, stderr) = run_cli(&[
        "--config",
        path.to_str().unwrap(),
        "config-inspect",
        "--json",
    ]);
    assert_eq!(code, 2, "bad config should use stable config exit code");
    assert!(stderr.contains("Config error"));
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
