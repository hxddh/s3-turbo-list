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

fn run_cli_without_aws_env(args: &[&str]) -> (i32, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-turbo-list"));
    clear_aws_env(&mut cmd);
    let output = cmd
        .args(args)
        .output()
        .expect("failed to execute s3-turbo-list test binary");

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (exit_code, stdout, stderr)
}

fn run_cli_with_aws_profile(args: &[&str], profile: &str) -> (i32, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-turbo-list"));
    clear_aws_env(&mut cmd);
    let output = cmd
        .env("AWS_PROFILE", profile)
        .args(args)
        .output()
        .expect("failed to execute s3-turbo-list test binary");

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (exit_code, stdout, stderr)
}

fn clear_aws_env(cmd: &mut Command) {
    for name in [
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
    ] {
        cmd.env_remove(name);
    }
}

fn run_cli_in_dir(args: &[&str], cwd: &std::path::Path) -> (i32, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_s3-turbo-list"))
        .current_dir(cwd)
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
    assert!(
        stdout.contains("recursive full-bucket listing"),
        "list help should explain recursive delimiter usage"
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
    assert!(
        stdout.contains("always TOML"),
        "auto-hints help should describe TOML output"
    );
}

#[test]
fn test_cli_help_discover_prefixes() {
    let (code, stdout, _stderr) = run_cli(&["discover-prefixes", "--help"]);
    assert_eq!(
        code, 0,
        "s3-turbo-list discover-prefixes --help should exit 0"
    );
    assert!(stdout.contains("--bucket"));
    assert!(stdout.contains("--toml"));
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

    let (code, stdout, _stderr) = run_cli(&["profiles", "--help"]);
    assert_eq!(code, 0, "profiles --help should exit 0");
    assert!(stdout.contains("list"));

    let (code, stdout, _stderr) = run_cli(&["benchmark-local", "--help"]);
    assert_eq!(code, 0, "benchmark-local --help should exit 0");
    assert!(stdout.contains("--objects"));

    let (code, stdout, _stderr) = run_cli(&["init-config", "--help"]);
    assert_eq!(code, 0, "init-config --help should exit 0");
    assert!(stdout.contains("--overwrite"));

    let (code, stdout, _stderr) = run_cli(&["recipes", "--help"]);
    assert_eq!(code, 0, "recipes --help should exit 0");
    assert!(stdout.contains("Recipe name"));

    let (code, stdout, _stderr) = run_cli(&["quickstart", "--help"]);
    assert_eq!(code, 0, "quickstart --help should exit 0");
    assert!(stdout.contains("Provider name"));

    let (code, stdout, _stderr) = run_cli(&["trace-summary", "--help"]);
    assert_eq!(code, 0, "trace-summary --help should exit 0");
    assert!(stdout.contains("--machine-readable"));

    let (code, stdout, _stderr) = run_cli(&["manifest-summary", "--help"]);
    assert_eq!(code, 0, "manifest-summary --help should exit 0");
    assert!(stdout.contains("--json"));

    let (code, stdout, _stderr) = run_cli(&["hints-merge", "--help"]);
    assert_eq!(code, 0, "hints-merge --help should exit 0");
    assert!(stdout.contains("--emit-manifest"));

    let (code, stdout, _stderr) = run_cli(&["hints-rebalance", "--help"]);
    assert_eq!(code, 0, "hints-rebalance --help should exit 0");
    assert!(stdout.contains("--long-tail-ratio"));
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
fn test_cli_init_config_writes_and_requires_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s3-turbo-list.toml");

    let (code, stdout, stderr) = run_cli(&[
        "init-config",
        "--profile",
        "minio",
        "--output",
        path.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["profile"], "minio");
    let rendered = std::fs::read_to_string(&path).unwrap();
    assert!(rendered.contains("profile = \"minio\""));
    assert!(rendered.contains("AWS_PROFILE"));

    let r2_path = dir.path().join("r2.toml");
    let (code, stdout, stderr) = run_cli(&[
        "init-config",
        "--profile",
        "r2",
        "--output",
        r2_path.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let rendered = std::fs::read_to_string(&r2_path).unwrap();
    assert!(rendered.contains("profile = \"r2\""));
    assert!(rendered.contains("addressing_style = \"path\""));
    assert!(rendered.contains("force_path_style = true"));

    let (code, _stdout, stderr) = run_cli(&["init-config", "--output", path.to_str().unwrap()]);
    assert_ne!(code, 0);
    assert!(stderr.contains("Output file exists"));

    let (code, _stdout, stderr) = run_cli(&[
        "init-config",
        "--output",
        path.to_str().unwrap(),
        "--overwrite",
    ]);
    assert_eq!(code, 0, "stderr: {}", stderr);
}

#[test]
fn test_cli_recipes_quickstart_and_cheatsheet_local_only() {
    let (code, stdout, stderr) = run_cli(&["recipes", "aws-basic"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("--dry-run"));
    assert!(stdout.contains("--output-dir"));
    assert!(stdout.contains("--delimiter ''"));

    let (code, stdout, stderr) = run_cli(&["recipes", "summary"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("--summary-only"));
    assert!(stdout.contains("manifest-summary"));

    let (code, stdout, stderr) = run_cli(&["recipes", "pipe"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("--output-format tsv"));
    assert!(stdout.contains("--output-format ndjson"));
    assert!(stdout.contains("manifest-summary"));

    let (code, stdout, stderr) = run_cli(&["recipes", "filter"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("SOURCE.size > 1073741824"));
    assert!(stdout.contains("SOURCE.last_modified"));
    assert!(stdout.contains("Rejected before network"));

    let (code, stdout, stderr) = run_cli(&["recipes", "release-check"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("./scripts/check-release-env.sh"));
    assert!(stdout.contains("cargo clippy --all-targets -- -D warnings"));
    assert!(stdout.contains("BUILD_MODE=clang"));
    assert!(stdout.contains("do not contact S3-compatible cloud endpoints"));

    let (code, stdout, stderr) = run_cli(&["recipes", "diff-safe"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Safe diff"));
    assert!(stdout.contains("diff --bucket"));
    assert!(stdout.contains("manifest-summary"));

    let (code, stdout, stderr) = run_cli(&["quickstart", "r2"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("AWS_PROFILE"));
    assert!(stdout.contains("--profile r2"));
    assert!(stdout.contains("--delimiter ''"));

    let (code, stdout, stderr) = run_cli(&["cheatsheet"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("First run"));
    assert!(stdout.contains("--delimiter ''"));
    assert!(stdout.contains("recipes filter"));
    assert!(stdout.contains("recipes release-check"));
}

#[test]
fn test_cli_output_dir_dry_run_plans_paths_without_creating_dir() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    assert!(!out.exists());

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--dry-run",
            "--agent",
            "--output-dir",
            out.to_str().unwrap(),
            "list",
            "--bucket",
            "my-bucket",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(!out.exists(), "dry-run must not create output-dir");
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let parquet = json["outputs"]["parquet_file"].as_str().unwrap();
    let ks = json["outputs"]["ks_file"].as_str().unwrap();
    assert!(parquet.starts_with(out.to_str().unwrap()));
    assert!(parquet.ends_with(".parquet"));
    assert!(ks.starts_with(out.to_str().unwrap()));
    assert!(ks.ends_with(".ks"));
}

#[test]
fn test_cli_doctor_simple_fix_suggestions() {
    let dir = tempfile::tempdir().unwrap();
    let missing_parent = dir.path().join("missing").join("out.parquet");
    let (code, stdout, stderr) = run_cli(&[
        "--output-parquet-file",
        missing_parent.to_str().unwrap(),
        "doctor",
        "--local-only",
        "--simple",
        "--fix-suggestions",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("ERROR output_parquet_parent"));
    assert!(stdout.contains("NEXT mkdir -p"));
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
fn test_cli_doctor_warns_when_aws_profile_matches_endpoint_preset_name() {
    let (code, stdout, stderr) =
        run_cli_with_aws_profile(&["doctor", "--local-only", "--json"], "bos");
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let checks = json["checks"].as_array().unwrap();
    assert!(checks.iter().any(|check| {
        check["name"] == "aws_profile_endpoint_preset_name"
            && check["status"] == "warn"
            && check["message"]
                .as_str()
                .unwrap()
                .contains("endpoint compatibility preset name")
    }));
}

#[test]
fn test_cli_profiles_list_json_local_only() {
    let (code, stdout, stderr) = run_cli(&["profiles", "list", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let profiles = json.as_array().unwrap();
    assert!(profiles.iter().any(|profile| profile["name"] == "bos"));
    assert!(profiles.iter().any(|profile| profile["name"] == "r2"));
    assert!(profiles.iter().any(|profile| profile["name"] == "b2"));
    assert!(profiles.iter().any(|profile| profile["name"] == "oss"));
}

#[test]
fn test_cli_profiles_show_r2_json_local_only() {
    let (code, stdout, stderr) = run_cli(&["profiles", "show", "r2", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["name"], "r2");
    assert_eq!(json["default_region"], "auto");
    assert_eq!(json["requires_explicit_endpoint"], true);
    assert_eq!(json["tested_by_project"], false);
}

#[test]
fn test_cli_completions_and_man_local_only() {
    let (code, stdout, stderr) = run_cli(&["completions", "bash"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("s3-turbo-list"));
    assert!(stdout.contains("benchmark-local"));

    let (code, stdout, stderr) = run_cli(&["man"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("s3-turbo-list"));
    assert!(stdout.contains(".SH DESCRIPTION"));
}

#[test]
fn test_cli_benchmark_local_json_no_cloud() {
    let (code, stdout, stderr) = run_cli(&[
        "benchmark-local",
        "--objects",
        "32",
        "--batch-size",
        "8",
        "--prefixes",
        "4",
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["tool_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(json["network"], "none: synthetic local data only");
    assert_eq!(json["compression"], "zstd");
    assert_eq!(json["compression_level"], 3);
    assert_eq!(json["objects"], 32);
    assert!(json["parquet_bytes_per_object"].as_f64().unwrap() > 0.0);
    assert!(json["output_bytes_per_object"].as_f64().unwrap() > 0.0);
    assert!(json["parquet_mib_per_sec"].as_f64().unwrap() > 0.0);
    assert!(json["output_mib_per_sec"].as_f64().unwrap() > 0.0);
    assert_eq!(json["artifact_dir"], serde_json::Value::Null);
    assert_eq!(json["metrics"]["received_objects"], 32);
    assert_eq!(json["metrics"]["streamed_rows"], 32);
    assert_eq!(json["metrics"]["parquet_rows"], 32);
    assert_eq!(json["metrics"]["ks_entries"], 4);
}

#[test]
fn test_cli_benchmark_local_honors_compression_flags_no_cloud() {
    let (code, stdout, stderr) = run_cli(&[
        "--compression",
        "gzip",
        "--compression-level",
        "6",
        "benchmark-local",
        "--objects",
        "32",
        "--batch-size",
        "8",
        "--prefixes",
        "4",
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["network"], "none: synthetic local data only");
    assert_eq!(json["compression"], "gzip");
    assert_eq!(json["compression_level"], 6);
    assert_eq!(json["metrics"]["parquet_rows"], 32);
}

#[test]
fn test_cli_config_inspect_reports_zstd_default_no_cloud() {
    let (code, stdout, stderr) = run_cli(&["config-inspect", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["resolved_config"]["output"]["compression"], "zstd");
    assert_eq!(json["resolved_config"]["output"]["compression_level"], 3);
    assert!(!json["config_source"]["searched"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        json["config_source"]["cli_overrides"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn test_cli_config_inspect_json_reports_explicit_config_source_no_cloud() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3-turbo-list.toml");
    std::fs::write(
        &config_path,
        r#"
[runtime]
worker_threads = 3

[output]
compression = "gzip"
compression_level = 6
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli(&[
        "--config",
        config_path.to_str().unwrap(),
        "config-inspect",
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        json["config_source"]["explicit_config"],
        config_path.to_str().unwrap()
    );
    assert_eq!(
        json["config_source"]["loaded_config"],
        config_path.to_str().unwrap()
    );
    assert_eq!(json["config_source"]["loaded_config_kind"], "explicit");
    assert_eq!(json["resolved_config"]["runtime"]["worker_threads"], 3);
    assert_eq!(json["resolved_config"]["output"]["compression"], "gzip");
    assert_eq!(json["resolved_config"]["output"]["compression_level"], 6);
}

#[test]
fn test_cli_config_inspect_human_reports_loaded_config_no_cloud() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3-turbo-list.toml");
    std::fs::write(
        &config_path,
        r#"
[runtime]
worker_threads = 4
"#,
    )
    .unwrap();

    let (code, stdout, stderr) =
        run_cli(&["--config", config_path.to_str().unwrap(), "config-inspect"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("config:"));
    assert!(stdout.contains(config_path.to_str().unwrap()));
}

#[test]
fn test_cli_config_inspect_warns_for_missing_explicit_config_no_cloud() {
    let config_path = "/tmp/s3-turbo-list-missing-test-config.toml";
    let _ = std::fs::remove_file(config_path);

    let (code, stdout, stderr) = run_cli(&["--config", config_path, "config-inspect", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["config_source"]["explicit_config"], config_path);
    assert_eq!(
        json["config_source"]["loaded_config"],
        serde_json::Value::Null
    );
    assert_eq!(json["config_source"]["loaded_config_kind"], "none");
    assert!(json["config_source"]["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| warning.as_str().unwrap().contains("was not found")));

    let (code, stdout, stderr) = run_cli(&["--config", config_path, "config-inspect"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("config:       -"));
    assert!(stdout.contains("warning:"));
    assert!(stdout.contains("was not found"));
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
    assert_eq!(json["file_conflicts"][0]["exists"], false);
    assert_eq!(json["file_conflicts"][0]["parent_exists"], true);
    assert_eq!(json["file_conflicts"][0]["parent_writable"], true);
}

#[test]
fn test_cli_dry_run_plan_warns_for_missing_explicit_config_no_cloud() {
    let dir = tempfile::tempdir().unwrap();
    let missing_config = dir.path().join("missing.toml");

    let (code, stdout, stderr) = run_cli(&[
        "--agent",
        "--dry-run",
        "--config",
        missing_config.to_str().unwrap(),
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        json["config_source"]["explicit_config"],
        missing_config.to_str().unwrap()
    );
    assert!(json["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| { warning.as_str().unwrap().contains("explicit config file") }));
}

#[test]
fn test_cli_compression_flags_override_config_in_dry_run_no_cloud() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("s3-turbo-list.toml");
    std::fs::write(
        &config_path,
        r#"
[output]
compression = "snappy"
compression_level = 6
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli(&[
        "--agent",
        "--dry-run",
        "--config",
        config_path.to_str().unwrap(),
        "--compression",
        "zstd",
        "--compression-level",
        "3",
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["resolved_config"]["output"]["compression"], "zstd");
    assert_eq!(json["resolved_config"]["output"]["compression_level"], 3);
    assert_eq!(
        json["config_source"]["loaded_config"],
        config_path.to_str().unwrap()
    );
    let overrides = json["config_source"]["cli_overrides"].as_array().unwrap();
    assert!(overrides.iter().any(|value| value == "compression"));
    assert!(overrides.iter().any(|value| value == "compression_level"));
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
fn test_cli_provider_setup_error_uses_exit_code_3_no_cloud() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--profile",
        "r2",
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "auto",
    ]);
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.is_empty());
    assert!(stderr.contains("Provider setup error:"));
    assert!(stderr.contains("requires an explicit endpoint URL"));
}

#[test]
fn test_cli_default_paths_sanitize_bucket_and_region_components() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--agent",
        "--dry-run",
        "--resume",
        "list",
        "--bucket",
        "../evil/bucket",
        "--region",
        "us/east",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    for field in ["parquet_file", "ks_file"] {
        let path = json["outputs"][field].as_str().unwrap();
        assert!(
            !path.contains('/'),
            "{} should not contain slash: {}",
            field,
            path
        );
        assert!(path.contains("us_east_.._evil_bucket"), "{}", path);
    }
    let checkpoint = json["checkpoint"]["path"].as_str().unwrap();
    assert!(!checkpoint.contains("../"), "{}", checkpoint);
    assert!(
        checkpoint.contains("us_east_.._evil_bucket"),
        "{}",
        checkpoint
    );

    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--agent",
        "--dry-run",
        "discover-prefixes",
        "--bucket",
        "../evil/bucket",
        "--region",
        "us/east",
        "--toml",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        json["outputs"]["hints_file"],
        "us_east_.._evil_bucket_prefixes.toml"
    );
}

#[test]
fn test_cli_dry_run_warns_when_endpoint_profile_may_be_credentials_profile() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--profile",
        "bos",
        "--agent",
        "--dry-run",
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "bj",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let warnings = json["warnings"].as_array().unwrap();
    assert!(warnings.iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("--profile 'bos' is an endpoint compatibility preset only")
    }));
}

#[test]
fn test_cli_dry_run_warns_for_profile_missing_or_placeholder_endpoint() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--profile",
        "r2",
        "--agent",
        "--dry-run",
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "auto",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("requires an explicit endpoint URL")
    }));

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        r#"[s3]
profile = "r2"
endpoint_url = "https://<account-id>.r2.cloudflarestorage.com"
"#,
    )
    .unwrap();
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--config",
        config.to_str().unwrap(),
        "--agent",
        "--dry-run",
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "auto",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("still contains template placeholders")
    }));
}

#[test]
fn test_cli_doctor_warns_for_placeholder_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        r#"[s3]
profile = "r2"
endpoint_url = "https://<account-id>.r2.cloudflarestorage.com"
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli(&[
        "--config",
        config.to_str().unwrap(),
        "doctor",
        "--local-only",
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(json["checks"].as_array().unwrap().iter().any(|check| {
        check["name"] == "endpoint_url"
            && check["status"] == "warn"
            && check["message"]
                .as_str()
                .unwrap()
                .contains("template placeholders")
    }));
}

#[test]
fn test_cli_dry_run_summary_only_plans_no_output_artifacts() {
    let (code, stdout, stderr) = run_cli(&[
        "--agent",
        "--dry-run",
        "--summary-only",
        "--output-dir",
        "out",
        "list",
        "--bucket",
        "agent-test-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["outputs"]["parquet_file"], serde_json::Value::Null);
    assert_eq!(json["outputs"]["ks_file"], serde_json::Value::Null);
    assert_eq!(
        json["resolved_config"]["output"]["parquet_file"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["resolved_config"]["output"]["ks_file"],
        serde_json::Value::Null
    );
    assert!(json["file_conflicts"].as_array().unwrap().is_empty());
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("summary-only will scan S3 ListObjectsV2 pages")
    }));
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("output path flags are ignored")
    }));
}

#[test]
fn test_cli_summary_only_rejects_diff() {
    let (code, stdout, stderr) = run_cli(&[
        "--dry-run",
        "--summary-only",
        "diff",
        "--bucket",
        "left",
        "--region",
        "us-east-1",
        "--target-bucket",
        "right",
        "--target-region",
        "us-east-1",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("--summary-only is only supported with the list command"));
}

#[test]
fn test_cli_rejects_agent_with_stdout_output_format() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--agent",
        "list",
        "--bucket",
        "my-bucket",
        "--region",
        "us-east-1",
        "--output-format",
        "ndjson",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("--agent writes the run manifest to stdout"));
}

#[test]
fn test_cli_dry_run_output_format_ndjson_plans_no_output_artifacts() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--dry-run",
        "--agent",
        "--output-dir",
        "out",
        "list",
        "--bucket",
        "my-bucket",
        "--region",
        "us-east-1",
        "--output-format",
        "ndjson",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["inputs"]["output_format"], "ndjson");
    assert_eq!(json["outputs"]["parquet_file"], serde_json::Value::Null);
    assert_eq!(json["outputs"]["ks_file"], serde_json::Value::Null);
    assert!(json["file_conflicts"].as_array().unwrap().is_empty());
    let warnings = json["warnings"].as_array().unwrap();
    assert!(warnings.iter().any(|item| item
        .as_str()
        .unwrap()
        .contains("--output-format tsv/ndjson streams list rows to stdout")));
    assert!(warnings.iter().any(|item| item
        .as_str()
        .unwrap()
        .contains("output path flags are ignored")));
}

#[test]
fn test_cli_dry_run_continuation_token_is_single_chain_list() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--dry-run",
        "--agent",
        "--no-auto-hints",
        "--continuation-token",
        "token-123",
        "list",
        "--bucket",
        "my-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["inputs"]["continuation_token"], "token-123");
    assert!(json["command"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| { item.as_str().unwrap().contains("--continuation-token") }));
    assert!(!json["command"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| { item.as_str().unwrap().contains("token-123") }));
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("continuation-token resumes one sequential")
    }));
}

#[test]
fn test_cli_rejects_continuation_token_with_diff_or_hints() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--dry-run",
        "--continuation-token",
        "token-123",
        "diff",
        "--bucket",
        "left",
        "--target-bucket",
        "right",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("--continuation-token is only supported with the list command"));

    let dir = tempfile::tempdir().unwrap();
    let hints = dir.path().join("hints.txt");
    std::fs::write(&hints, "m/\n").unwrap();
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--dry-run",
        "--continuation-token",
        "token-123",
        "--hints-file",
        hints.to_str().unwrap(),
        "list",
        "--bucket",
        "my-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("single-chain only"));

    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--dry-run",
        "--no-auto-hints",
        "--start-after",
        "already-seen-key",
        "--continuation-token",
        "token-123",
        "list",
        "--bucket",
        "my-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("--continuation-token cannot be combined with --start-after"));
}

#[test]
fn test_cli_rejects_diff_with_explicit_hints_file() {
    let dir = tempfile::tempdir().unwrap();
    let hints = dir.path().join("hints.txt");
    std::fs::write(&hints, "m/\n").unwrap();

    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--dry-run",
        "--hints-file",
        hints.to_str().unwrap(),
        "diff",
        "--bucket",
        "left",
        "--target-bucket",
        "right",
    ]);

    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("diff with --hints-file is not supported yet"));
    assert!(stderr.contains("v0.2.x"));
    assert!(stderr.contains("single-segment diff"));
}

#[test]
fn test_cli_rejects_diff_with_resume() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--dry-run",
        "--resume",
        "diff",
        "--bucket",
        "left",
        "--target-bucket",
        "right",
    ]);

    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("diff --resume is not supported yet"));
    assert!(stderr.contains("v0.2.x"));
}

#[test]
fn test_cli_rejects_unsupported_filter_syntax_before_network() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--filter",
        "max(SOURCE.size, 1) > 0",
        "list",
        "--bucket",
        "my-bucket",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("Filter error"), "{}", stderr);
    assert!(
        stderr.contains("function call \"max\" not allowed"),
        "{}",
        stderr
    );
}

#[test]
fn test_cli_manifest_summary_human_and_json() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("run.json");
    std::fs::write(
        &manifest,
        r#"{
  "tool_version": "0.1.15",
  "status": "success",
  "exit_code": 0,
  "elapsed_secs": 1.25,
  "command": ["s3-turbo-list", "--summary-only"],
  "outputs": {
    "parquet_file": null,
    "ks_file": null,
    "hints_file": null,
    "trace_compat": null,
    "log_file": null
  },
  "artifacts": [],
  "metrics": {
    "received_objects": 3,
    "streamed_rows": 3,
    "unique_prefixes": 2,
    "parquet_rows": 0,
    "ks_entries": 0,
    "bytes_total": 600,
    "summary_only": true,
    "top_prefixes": [
      {"prefix": "logs", "objects": 2, "bytes": 300},
      {"prefix": "images", "objects": 1, "bytes": 300}
    ]
  },
  "warnings": ["example warning"]
}"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &["manifest-summary", manifest.to_str().unwrap()],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Objects:      3"));
    assert!(stdout.contains("Top prefixes:"));
    assert!(stdout.contains("example warning"));

    let (code, stdout, stderr) = run_cli_in_dir(
        &["manifest-summary", manifest.to_str().unwrap(), "--json"],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["streamed_rows"], 3);
    assert_eq!(json["bytes_total"], 600);
    assert_eq!(json["summary_only"], true);
    assert_eq!(
        json["parquet_rows_match_streamed_rows"],
        serde_json::Value::Null
    );
    assert_eq!(json["check_passed"], true);
    assert_eq!(json["check"]["ok"], true);
    assert_eq!(json["check"]["errors"], 0);
    assert_eq!(json["check"]["skipped"], 1);
    assert_eq!(json["check"]["artifacts_checked"], 0);
    assert_eq!(json["check"]["row_check"], "not_applicable");
    assert_eq!(json["check"]["exit_code_check"], "ok");
    assert_eq!(json["top_prefixes"][0]["prefix"], "logs");

    let (code, stdout, stderr) = run_cli_in_dir(
        &["manifest-summary", manifest.to_str().unwrap(), "--check"],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Check:        PASS"));
}

#[test]
fn test_cli_manifest_summary_check_fails_bad_parquet_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("run.json");
    std::fs::write(
        &manifest,
        r#"{
  "tool_version": "0.1.16",
  "status": "success",
  "exit_code": 0,
  "elapsed_secs": 1.25,
  "command": ["s3-turbo-list"],
  "inputs": {"output_format": "parquet"},
  "outputs": {
    "parquet_file": "out.parquet",
    "ks_file": "out.ks",
    "hints_file": null,
    "trace_compat": null,
    "log_file": null
  },
  "artifacts": [
    {"kind": "parquet", "path": "out.parquet", "exists": true, "size_bytes": 10},
    {"kind": "ks", "path": "out.ks", "exists": false, "size_bytes": null}
  ],
  "metrics": {
    "fatal_errors": 0,
    "output_errors": 0,
    "received_objects": 3,
    "streamed_rows": 3,
    "unique_prefixes": 2,
    "parquet_rows": 2,
    "ks_entries": 2,
    "bytes_total": 600,
    "summary_only": false,
    "top_prefixes": []
  },
  "warnings": []
}"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &["manifest-summary", manifest.to_str().unwrap(), "--check"],
        dir.path(),
    );
    assert_eq!(code, 6, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Check:        FAIL"));
    assert!(stdout.contains("parquet_rows_match_streamed_rows"));
    assert!(stdout.contains("artifact_exists:ks"));
}

#[test]
fn test_cli_manifest_summary_check_verifies_artifact_size_and_hash() {
    let dir = tempfile::tempdir().unwrap();
    let artifact = dir.path().join("out.ks");
    std::fs::write(&artifact, "\"logs\",\"1\"\n").unwrap();
    let manifest = dir.path().join("run.json");
    std::fs::write(
        &manifest,
        format!(
            r#"{{
  "tool_version": "0.1.23",
  "status": "success",
  "exit_code": 0,
  "elapsed_secs": 1.25,
  "command": ["s3-turbo-list"],
  "inputs": {{"output_format": "parquet"}},
  "outputs": {{
    "parquet_file": null,
    "ks_file": "{artifact}",
    "hints_file": null,
    "trace_compat": null,
    "log_file": null
  }},
  "artifacts": [
    {{"kind": "ks", "path": "{artifact}", "exists": true, "size_bytes": 999, "sha256": "0000000000000000000000000000000000000000000000000000000000000000"}}
  ],
  "metrics": {{
    "fatal_errors": 0,
    "output_errors": 0,
    "received_objects": 1,
    "streamed_rows": 1,
    "unique_prefixes": 1,
    "parquet_rows": 1,
    "ks_entries": 1,
    "bytes_total": 100,
    "summary_only": false,
    "top_prefixes": []
  }},
  "warnings": []
}}"#,
            artifact = artifact.display()
        ),
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &["manifest-summary", manifest.to_str().unwrap(), "--check"],
        dir.path(),
    );
    assert_eq!(code, 6, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("artifact_size:ks"));
    assert!(stdout.contains("artifact_sha256:ks"));

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "manifest-summary",
            manifest.to_str().unwrap(),
            "--json",
            "--check",
        ],
        dir.path(),
    );
    assert_eq!(code, 6, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["check"]["ok"], false);
    assert_eq!(json["check"]["artifacts_checked"], 1);
    assert_eq!(json["check"]["artifacts_missing"], 0);
    assert_eq!(json["check"]["row_check"], "ok");
    assert_eq!(json["check"]["exit_code_check"], "ok");
    assert!(json["check"]["errors"].as_u64().unwrap() >= 2);
}

#[test]
fn test_cli_manifest_summary_ndjson_row_check_is_not_applicable() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("run.json");
    std::fs::write(
        &manifest,
        r#"{
  "tool_version": "0.1.16",
  "status": "success",
  "exit_code": 0,
  "elapsed_secs": 1.25,
  "command": ["s3-turbo-list"],
  "inputs": {"output_format": "ndjson"},
  "outputs": {
    "parquet_file": null,
    "ks_file": null,
    "hints_file": null,
    "trace_compat": null,
    "log_file": null
  },
  "artifacts": [],
  "metrics": {
    "fatal_errors": 0,
    "output_errors": 0,
    "received_objects": 3,
    "streamed_rows": 3,
    "unique_prefixes": 2,
    "parquet_rows": 0,
    "ks_entries": 0,
    "bytes_total": 600,
    "summary_only": false,
    "top_prefixes": []
  },
  "warnings": []
}"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "manifest-summary",
            manifest.to_str().unwrap(),
            "--json",
            "--check",
        ],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        json["parquet_rows_match_streamed_rows"],
        serde_json::Value::Null
    );
    assert_eq!(json["check_passed"], true);
    assert_eq!(json["check"]["ok"], true);
    assert_eq!(json["check"]["row_check"], "not_applicable");
    assert_eq!(json["check"]["parquet_schema_check"], "not_applicable");
    assert!(json["checks"].as_array().unwrap().iter().any(|check| {
        check["name"] == "parquet_rows_match_streamed_rows" && check["status"] == "skip"
    }));
}

#[test]
fn test_cli_dry_run_reports_hints_and_checkpoint_summary() {
    let dir = tempfile::tempdir().unwrap();
    let hints_path = dir.path().join("hints.toml");
    std::fs::write(
        &hints_path,
        r#"bucket = "test-bucket"
region = "us-east-1"
total_objects = 30
boundaries = ["m/"]
generated_at = "2026-05-17T00:00:00Z"
scan_mode = "sampled"
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

    std::fs::write(
        dir.path().join("us-east-1_test-bucket_checkpoint.toml"),
        r#"bucket = "test-bucket"
prefix = ""
total_segments = 2
completed_indices = [0]
last_updated = "2026-05-17T00:00:00Z"

[identity]
bucket = "test-bucket"
region = "us-east-1"
prefix = ""
delimiter = "/"
addressing_style = "auto"
mode = "list"
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--agent",
            "--dry-run",
            "--resume",
            "--hints-file",
            hints_path.to_str().unwrap(),
            "list",
            "--bucket",
            "test-bucket",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();

    assert_eq!(json["hints"]["source"], "explicit");
    assert_eq!(json["hints"]["exists"], true);
    assert_eq!(json["hints"]["valid"], true);
    assert_eq!(json["hints"]["format"], "toml");
    assert_eq!(json["hints"]["boundary_count"], 1);
    assert_eq!(
        json["hints"]["estimate_summary"]["total_estimated_objects"],
        30
    );

    assert_eq!(json["checkpoint"]["enabled"], true);
    assert_eq!(json["checkpoint"]["exists"], true);
    assert_eq!(json["checkpoint"]["valid"], true);
    assert_eq!(json["checkpoint"]["identity_matches"], true);
    assert_eq!(json["checkpoint"]["completed_segments"], 1);
    assert_eq!(json["checkpoint"]["total_segments"], 2);
}

#[test]
fn test_cli_dry_run_no_auto_hints_reports_disabled_cache() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("us-east-1_test-bucket_hints.toml"),
        r#"bucket = "test-bucket"
region = "us-east-1"
total_objects = 30
boundaries = ["m/"]
generated_at = "2026-05-18T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--agent",
            "--dry-run",
            "--no-auto-hints",
            "list",
            "--bucket",
            "test-bucket",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["hints"]["source"], "disabled_single_segment_fallback");
    assert_eq!(json["hints"]["exists"], false);
}

#[test]
fn test_cli_dry_run_list_still_reports_conventional_hints_cache() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("us-east-1_test-bucket_hints.toml"),
        r#"bucket = "test-bucket"
region = "us-east-1"
total_objects = 30
boundaries = ["m/"]
generated_at = "2026-05-18T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--agent",
            "--dry-run",
            "list",
            "--bucket",
            "test-bucket",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["hints"]["source"], "auto_cache");
    assert_eq!(json["hints"]["exists"], true);
    assert_eq!(json["hints"]["boundary_count"], 1);
}

#[test]
fn test_cli_dry_run_diff_ignores_conventional_hints_cache() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("us-east-1_left_hints.toml"),
        r#"bucket = "left"
region = "us-east-1"
total_objects = 30
boundaries = ["m/"]
generated_at = "2026-05-18T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--agent",
            "--dry-run",
            "diff",
            "--bucket",
            "left",
            "--region",
            "us-east-1",
            "--target-bucket",
            "right",
        ],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["hints"]["source"], "disabled_for_diff_single_segment");
    assert_eq!(json["hints"]["exists"], true);
    assert_eq!(json["hints"]["boundary_count"], 1);
    assert!(json["hints"]["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| {
            warning
                .as_str()
                .unwrap()
                .contains("conventional hints cache is ignored")
        }));
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("hinted multi-segment diff paired coordination is deferred")
    }));
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
fn test_cli_hints_validate_plain_allows_partition_and_bracket_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hints.txt");
    std::fs::write(&path, "dt=2026-05-23/part=0/\n[backups]\n").unwrap();

    let (code, stdout, stderr) = run_cli(&[
        "hints-validate",
        "--hints-file",
        path.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["format"], "plain");
    assert_eq!(json["boundary_count"], 2);
    let first = json["first_boundaries"].as_array().unwrap();
    assert!(first
        .iter()
        .any(|value| value.as_str() == Some("dt=2026-05-23/part=0/")));
    assert!(first
        .iter()
        .any(|value| value.as_str() == Some("[backups]")));
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

#[test]
fn test_cli_hints_merge_json_writes_toml() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.txt");
    let b = dir.path().join("b.toml");
    let out = dir.path().join("merged.toml");
    let manifest = dir.path().join("merge.manifest.json");
    std::fs::write(&a, "logs/a/\nlogs/c/\n").unwrap();
    std::fs::write(
        &b,
        r#"bucket = "b"
total_objects = 0
boundaries = ["logs/b/", "logs/c/"]
generated_at = "2026-05-19T00:00:00Z"
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli(&[
        "hints-merge",
        a.to_str().unwrap(),
        b.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
        "--emit-manifest",
        manifest.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["boundary_count"], 3);
    assert_eq!(json["duplicate_count"], 1);
    assert_eq!(json["output_written"], true);

    let merged = std::fs::read_to_string(&out).unwrap();
    assert!(merged.contains("logs/a/"));
    assert!(merged.contains("logs/b/"));
    assert!(merged.contains("logs/c/"));

    let manifest_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(manifest).unwrap()).unwrap();
    assert_eq!(manifest_json["command"], "hints-merge");
    assert_eq!(manifest_json["outputs"].as_array().unwrap().len(), 1);

    let (code, _stdout, stderr) = run_cli(&[
        "hints-merge",
        a.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
    ]);
    assert_ne!(code, 0);
    assert!(stderr.contains("Output file exists"));
}

#[test]
fn test_cli_trace_summary_json() {
    let dir = tempfile::tempdir().unwrap();
    let trace = dir.path().join("trace.jsonl");
    std::fs::write(
        &trace,
        r#"{"timestamp":"2026-05-19T00:00:00Z","operation":"ListObjectsV2","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":10,"retryable":false,"fatal":false,"is_truncated":true,"start_after":"a/","contents_count":2,"first_key":"a/1","last_key":"a/2"}
{"timestamp":"2026-05-19T00:00:01Z","operation":"ListObjectsV2SegmentSummary","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":50,"retryable":false,"fatal":false,"is_truncated":false,"segment_index":0,"segment_pages":3,"segment_objects":30,"segment_common_prefixes":0,"ended_by":"boundary","end_before":"m/"}
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli(&["trace-summary", trace.to_str().unwrap(), "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["segment_count"], 1);
    assert_eq!(json["total_pages"], 3);
    assert_eq!(json["total_objects"], 30);
    assert_eq!(json["list_events"], 1);
}

#[test]
fn test_cli_hints_rebalance_adds_trace_sample_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let hints = dir.path().join("hints.txt");
    let trace = dir.path().join("trace.jsonl");
    let out = dir.path().join("new.toml");
    std::fs::write(&hints, "m/\n").unwrap();
    std::fs::write(
        &trace,
        r#"{"timestamp":"2026-05-19T00:00:00Z","operation":"ListObjectsV2","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":10,"retryable":false,"fatal":false,"is_truncated":true,"contents_count":2,"first_key":"a/1","last_key":"a/2"}
{"timestamp":"2026-05-19T00:00:01Z","operation":"ListObjectsV2","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":10,"retryable":false,"fatal":false,"is_truncated":true,"contents_count":2,"first_key":"c/1","last_key":"c/2"}
{"timestamp":"2026-05-19T00:00:02Z","operation":"ListObjectsV2","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":10,"retryable":false,"fatal":false,"is_truncated":true,"contents_count":2,"first_key":"e/1","last_key":"e/2"}
{"timestamp":"2026-05-19T00:00:03Z","operation":"ListObjectsV2","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":10,"retryable":false,"fatal":false,"is_truncated":true,"contents_count":2,"first_key":"g/1","last_key":"g/2"}
{"timestamp":"2026-05-19T00:00:04Z","operation":"ListObjectsV2","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":10,"retryable":false,"fatal":false,"is_truncated":true,"contents_count":2,"first_key":"i/1","last_key":"i/2"}
{"timestamp":"2026-05-19T00:00:05Z","operation":"ListObjectsV2SegmentSummary","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":500,"retryable":false,"fatal":false,"is_truncated":false,"segment_index":0,"segment_pages":5,"segment_objects":50,"segment_common_prefixes":0,"ended_by":"boundary","end_before":"m/"}
{"timestamp":"2026-05-19T00:00:06Z","operation":"ListObjectsV2SegmentSummary","endpoint_url":"http://localhost","addressing_style":"path","bucket":"b","prefix":"","http_status":200,"retry_attempt":0,"latency_ms":10,"retryable":false,"fatal":false,"is_truncated":false,"segment_index":1,"segment_pages":1,"segment_objects":10,"segment_common_prefixes":0,"ended_by":"pagination","start_after":"m/"}
"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli(&[
        "hints-rebalance",
        "--trace",
        trace.to_str().unwrap(),
        "--hints-file",
        hints.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
        "--long-tail-ratio",
        "2",
        "--min-pages",
        "2",
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["added_boundaries"].as_array().unwrap().len(), 1);
    assert_eq!(json["output_written"], true);

    let rendered = std::fs::read_to_string(out).unwrap();
    assert!(rendered.contains("e/2"));
    assert!(rendered.contains("m/"));
}
