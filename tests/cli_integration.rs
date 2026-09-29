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
    assert!(stdout.contains("--bucket"), "{}", stdout);
    assert!(stdout.contains("lists every key recursively"), "{}", stdout);
    // Tuning knobs and deprecated spellings stay out of --help.
    for hidden in [
        "--threads",
        "--max-keys",
        "--no-auto-hints",
        "--compression-level",
        "--output-ks-file",
        "--output-log-file",
        "--plan-json",
        "--debug-s3",
        "--summary-only",
        "--continuation-token",
    ] {
        assert!(!stdout.contains(hidden), "{} in {}", hidden, stdout);
    }
    // The local tools show only their own options, not the run flags.
    let (code, stdout, _stderr) = run_cli(&["guide", "--help"]);
    assert_eq!(code, 0);
    assert!(!stdout.contains("--output-dir"), "{}", stdout);
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
fn test_cli_help_compat_probe() {
    let (code, stdout, _stderr) = run_cli(&["compat-probe", "--help"]);
    assert_eq!(code, 0, "s3-turbo-list compat-probe --help should exit 0");
    assert!(
        stdout.contains("compat"),
        "compat-probe help should mention 'compat'"
    );
}

#[test]
fn test_cli_hints_validate_removed() {
    // hints-validate was folded into `doctor --hints-file`.
    let (code, _stdout, stderr) = run_cli(&["hints-validate"]);
    assert_ne!(code, 0, "hints-validate should no longer be a subcommand");
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("invalid"),
        "stderr should report an unknown subcommand: {}",
        stderr
    );
}

#[test]
fn test_cli_help_agent_local_commands() {
    let (code, stdout, _stderr) = run_cli(&["doctor", "--help"]);
    assert_eq!(code, 0, "doctor --help should exit 0");
    assert!(stdout.contains("--json"));

    let (code, stdout, _stderr) = run_cli(&["guide", "--help"]);
    assert_eq!(code, 0, "guide --help should exit 0");
    assert!(stdout.contains("provider"));

    let (code, stdout, _stderr) = run_cli(&["manifest-summary", "--help"]);
    assert_eq!(code, 0, "manifest-summary --help should exit 0");
    assert!(stdout.contains("--json"));

    // benchmark-local moved to `cargo run --example bench_local`.
    let (code, _stdout, _stderr) = run_cli(&["benchmark-local", "--help"]);
    assert_eq!(code, 2);
}

#[test]
fn test_cli_doctor_json_includes_resolved_config() {
    // doctor absorbed the former config-inspect: its JSON carries the
    // resolved configuration and its provenance.
    let (code, stdout, stderr) = run_cli(&["doctor", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["status"], "ok");
    assert!(json["resolved_config"]["runtime"]["worker_threads"].is_number());
    assert!(json["resolved_config"]["s3"]["addressing_style"].is_string());
    assert!(json["config_source"]["searched"].is_array());
}

#[test]
fn test_cli_init_config_was_removed_with_a_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s3-turbo-list.toml");
    let (code, _stdout, stderr) = run_cli(&[
        "init-config",
        "--profile",
        "minio",
        "--output",
        path.to_str().unwrap(),
    ]);
    assert_eq!(code, 2, "stderr: {}", stderr);
    assert!(stderr.contains("init-config was removed"), "{}", stderr);
    assert!(stderr.contains("docs/providers.md"), "{}", stderr);
    assert!(!path.exists());
}

#[test]
fn test_cli_guide_local_only() {
    // No topic prints the overview in the current command syntax.
    let (code, stdout, stderr) = run_cli(&["guide"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("First run"));
    assert!(stdout.contains("list --bucket my-bucket"));
    assert!(stdout.contains("--output-format summary"));
    assert!(!stdout.contains("--delimiter ''"), "{}", stdout);

    // Provider topics print a quickstart plus the preset's facts.
    let (code, stdout, stderr) = run_cli(&["guide", "r2"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("AWS_PROFILE"));
    assert!(stdout.contains("--provider r2"));

    // The recipes are gone; an unknown topic names the valid ones.
    let (code, _stdout, stderr) = run_cli(&["guide", "release-check"]);
    assert_eq!(code, 2);
    assert!(stderr.contains("aws, minio"), "{}", stderr);
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
fn test_cli_doctor_output_checks_agree_with_the_run() {
    let dir = tempfile::tempdir().unwrap();
    // The run creates missing parent directories itself: not an error.
    let missing_parent = dir.path().join("missing").join("out.parquet");
    let (code, stdout, stderr) = run_cli(&[
        "--output-parquet-file",
        missing_parent.to_str().unwrap(),
        "doctor",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(!dir.path().join("missing").exists());
    // An output path that is a directory fails the run: an error.
    let (code, stdout, stderr) = run_cli(&[
        "--output-parquet-file",
        dir.path().to_str().unwrap(),
        "doctor",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("output_parquet_parent"), "{}", stdout);
    assert!(stdout.contains("is a directory"), "{}", stdout);
}

#[test]
fn test_cli_doctor_json_local_only_success() {
    let (code, stdout, stderr) = run_cli(&["doctor", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["status"], "ok");
    let checks = json["checks"].as_array().unwrap();
    assert!(
        checks
            .iter()
            .any(|check| check["name"] == "network" && check["status"] == "skipped")
    );
}

#[test]
fn test_cli_doctor_provider_and_legacy_profile_spelling() {
    // --provider selects the preset; --profile is its hidden pre-0.37 alias,
    // and an unset AWS_PROFILE is the normal case, not a warning.
    for flag in ["--provider", "--profile"] {
        let (code, stdout, stderr) = run_cli(&[
            flag,
            "minio",
            "--endpoint-url",
            "http://127.0.0.1:9000",
            "doctor",
            "--json",
        ]);
        assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
        let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(json["resolved_config"]["s3"]["provider"], "minio");
        assert_eq!(json["resolved_config"]["s3"]["addressing_style"], "path");
        assert!(
            json["checks"]
                .as_array()
                .unwrap()
                .iter()
                .all(|check| check["status"] != "warn"),
            "{}",
            stdout
        );
    }
}

#[test]
fn test_cli_profiles_removed() {
    // The profiles subcommand was folded into `guide <provider>`.
    let (code, _stdout, stderr) = run_cli(&["profiles"]);
    assert_ne!(code, 0, "profiles should no longer be a subcommand");
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("invalid"),
        "stderr should report an unknown subcommand: {}",
        stderr
    );
}

#[test]
fn test_cli_guide_provider_shows_profile_facts() {
    let (code, stdout, stderr) = run_cli(&["guide", "aws"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("aws quickstart"));
    assert!(stdout.contains("Provider preset 'aws':"));
    assert!(stdout.contains("provider: AWS S3"));

    let (code, stdout, stderr) = run_cli(&["guide", "oss"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Alibaba Cloud OSS"));
    assert!(stdout.contains("(from --region)"));
}

#[test]
fn test_cli_completions_and_man_local_only() {
    let (code, stdout, stderr) = run_cli(&["completions", "bash"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("s3-turbo-list"));
    assert!(stdout.contains("compat-probe"));

    let (code, stdout, stderr) = run_cli(&["man"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("s3-turbo-list"));
    assert!(stdout.contains(".SH DESCRIPTION"));
}

#[test]
fn test_cli_doctor_reports_zstd_default_no_cloud() {
    let (code, stdout, stderr) = run_cli(&["doctor", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["resolved_config"]["output"]["compression"], "zstd");
    assert_eq!(json["resolved_config"]["output"]["compression_level"], 1);
    assert!(
        !json["config_source"]["searched"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        json["config_source"]["cli_overrides"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn test_cli_doctor_json_reports_explicit_config_source_no_cloud() {
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
        "doctor",
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
fn test_cli_doctor_human_reports_loaded_config_no_cloud() {
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

    let (code, stdout, stderr) = run_cli(&["--config", config_path.to_str().unwrap(), "doctor"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("config_file"));
    assert!(stdout.contains(config_path.to_str().unwrap()));
}

#[test]
fn test_cli_doctor_rejects_missing_explicit_config_no_cloud() {
    // A missing explicit --config is an error (it used to fall back to
    // defaults, i.e. real AWS). doctor --json still prints JSON.
    let config_path = "/tmp/s3-turbo-list-missing-test-config.toml";
    let _ = std::fs::remove_file(config_path);

    let (code, stdout, stderr) = run_cli(&["--config", config_path, "doctor", "--json"]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "error");
    assert_eq!(json["checks"][0]["name"], "config_parse");
    assert!(
        json["checks"][0]["message"]
            .as_str()
            .unwrap()
            .contains("was not found")
    );
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
fn test_cli_dry_run_rejects_missing_explicit_config_no_cloud() {
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
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("was not found"), "{}", stderr);
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
fn test_cli_removed_subcommands_are_gone() {
    for cmd in ["auto-hints", "discover-prefixes"] {
        let (code, _stdout, stderr) = run_cli(&[cmd, "--help"]);
        assert_ne!(code, 0, "{} should no longer be a valid subcommand", cmd);
        assert!(
            stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
            "{} removal should produce a clap error, got: {}",
            cmd,
            stderr
        );
    }
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
fn test_cli_compat_probe_placeholder_endpoint_uses_exit_code_3_no_cloud() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "compat-probe",
        "--endpoint",
        "https://<account-id>.r2.cloudflarestorage.com",
        "--region",
        "auto",
        "--bucket",
        "agent-test-bucket",
    ]);
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.is_empty());
    assert!(stderr.contains("Provider setup error:"));
    assert!(stderr.contains("still contains template placeholders"));
}

#[test]
fn test_cli_compat_probe_dry_run_warns_for_placeholder_endpoint() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--agent",
        "--dry-run",
        "compat-probe",
        "--endpoint",
        "https://<account-id>.r2.cloudflarestorage.com",
        "--region",
        "auto",
        "--bucket",
        "agent-test-bucket",
    ]);
    // The plan is still printed, but the dry run fails the way the real run
    // would: a placeholder endpoint is a provider setup error (exit 3).
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("Provider setup error:"), "{}", stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "blocked");
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("still contains template placeholders")
    }));
}

#[test]
fn test_cli_compat_probe_dry_run_uses_command_endpoint_for_profile_guardrail() {
    let (code, stdout, stderr) = run_cli_without_aws_env(&[
        "--profile",
        "r2",
        "--agent",
        "--dry-run",
        "compat-probe",
        "--endpoint",
        "https://account.example.com",
        "--region",
        "auto",
        "--bucket",
        "agent-test-bucket",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(!json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("requires an explicit endpoint URL")
    }));
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
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "blocked");
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
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "blocked");
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

    let (code, stdout, stderr) =
        run_cli(&["--config", config.to_str().unwrap(), "doctor", "--json"]);
    // A placeholder endpoint stops every real run with exit 3; doctor says so.
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "error");
    assert!(json["checks"].as_array().unwrap().iter().any(|check| {
        check["name"] == "endpoint_url"
            && check["status"] == "error"
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
    assert!(
        stderr.contains("unexpected argument '--summary-only'"),
        "{}",
        stderr
    );
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
    assert!(warnings.iter().any(|item| {
        item.as_str()
            .unwrap()
            .contains("--output-format tsv/ndjson streams list rows to stdout")
    }));
    assert!(warnings.iter().any(|item| {
        item.as_str()
            .unwrap()
            .contains("output path flags are ignored")
    }));
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
    assert_eq!(json["inputs"]["continuation_token"], "<redacted>");
    assert!(
        json["command"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| { item.as_str().unwrap().contains("--continuation-token") })
    );
    assert!(
        !json["command"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| { item.as_str().unwrap().contains("token-123") })
    );
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
    assert!(
        stderr.contains("unexpected argument '--continuation-token'"),
        "{}",
        stderr
    );

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
    // diff has no --hints-file: clap rejects it as for any unknown option.
    assert!(
        stderr.contains("unexpected argument '--hints-file'"),
        "{}",
        stderr
    );
    assert!(!stderr.contains("v0.2.x"));
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
    assert!(
        stderr.contains("unexpected argument '--resume'"),
        "{}",
        stderr
    );
    assert!(!stderr.contains("v0.2.x"));
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
    assert_eq!(json["check"]["skipped"], 2);
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
    assert!(json["checks"].as_array().unwrap().iter().any(|check| {
        check["name"] == "artifact_parquet_metadata:parquet"
            && check["status"] == "skip"
            && check["message"]
                .as_str()
                .unwrap()
                .contains("size, and sha256 checks still apply")
    }));

    let (code, stdout, stderr) = run_cli_in_dir(
        &["manifest-summary", manifest.to_str().unwrap(), "--check"],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Parquet row/schema checks are not applicable"));
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
last_updated = "2026-05-17T00:00:00Z"
remaining = [{ start_after = "m/" }]

[identity]
bucket = "test-bucket"
region = "us-east-1"
prefix = ""
delimiter = ""
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

    assert_eq!(json["checkpoint"]["enabled"], true);
    assert_eq!(json["checkpoint"]["resume"], true);
    assert_eq!(json["checkpoint"]["exists"], true);
    assert_eq!(json["checkpoint"]["valid"], true);
    assert_eq!(json["checkpoint"]["identity_matches"], true);
    assert_eq!(json["checkpoint"]["remaining_ranges"], 1);
    assert_eq!(
        json["checkpoint"]["completed_segments"],
        serde_json::Value::Null
    );

    // Without --resume the run still saves a checkpoint if interrupted
    // (enabled), in --output-dir when one is given; it just does not read it.
    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--agent",
            "--dry-run",
            "--output-dir",
            "out",
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
    assert_eq!(json["checkpoint"]["enabled"], true);
    assert_eq!(json["checkpoint"]["resume"], false);
    assert_eq!(
        json["checkpoint"]["path"],
        "out/us-east-1_test-bucket_checkpoint.toml"
    );
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
fn test_cli_dry_run_ignores_a_leftover_hints_cache() {
    // Runs no longer read or write a hints cache in the working directory:
    // a file an older version left there is not an input.
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "us-east-1_test-bucket_hints.toml",
        "us-east-1_left_hints.toml",
    ] {
        std::fs::write(
            dir.path().join(name),
            "bucket = \"x\"\nboundaries = [\"m/\"]\ngenerated_at = \"2026-05-18T00:00:00Z\"\n",
        )
        .unwrap();
    }
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
    assert_eq!(json["hints"]["source"], "startup_discovery");
    assert_eq!(json["hints"]["path"], serde_json::Value::Null);

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
    assert_eq!(json["hints"]["source"], "diff_per_side_automatic");
    assert_eq!(json["hints"]["exists"], false);
}

#[test]
fn test_cli_bad_config_exits_with_config_code() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "[s3\n").unwrap();

    let (code, _stdout, stderr) =
        run_cli(&["--config", path.to_str().unwrap(), "doctor", "--json"]);
    assert_eq!(code, 2, "bad config should use stable config exit code");
    assert!(stderr.contains("Config error"));
}

#[test]
fn test_cli_doctor_hints_file_plain_success() {
    // hints-validate folded into `doctor --hints-file`.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hints.txt");
    std::fs::write(&path, "alpha/\nbeta/\n").unwrap();

    let (code, stdout, stderr) = run_cli(&["--hints-file", path.to_str().unwrap(), "doctor"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Hints file:"));
    assert!(stdout.contains("Boundary count"));
    assert!(stdout.contains("2"));
}

#[test]
fn test_cli_doctor_hints_file_json_metadata() {
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

    let (code, stdout, stderr) =
        run_cli(&["--hints-file", path.to_str().unwrap(), "doctor", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let hints = &json["hints"];
    assert_eq!(hints["metadata"]["region"], "us-east-1");
    assert_eq!(hints["metadata"]["generated_at"], "2026-05-17T00:00:00Z");
    assert_eq!(hints["boundary_count"], 1);
    // Decorative legacy fields are accepted on input but no longer surfaced.
    assert!(hints["metadata"].get("scan_mode").is_none());
    assert!(hints["metadata"].get("estimate_mode").is_none());
    assert!(hints["metadata"].get("total_objects").is_none());
    assert!(hints["metadata"].get("sampled_objects").is_none());
    assert!(hints.get("estimate_summary").is_none());
    assert!(hints.get("first_estimates").is_none());
}

#[test]
fn test_cli_doctor_hints_file_malformed_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hints.txt");
    std::fs::write(&path, "boundaries = [\nalpha/\n]\n").unwrap();

    let (code, _stdout, stderr) = run_cli(&["--hints-file", path.to_str().unwrap(), "doctor"]);
    assert_ne!(code, 0, "malformed hints should fail");
    assert!(stderr.contains("Hints validation failed"));
}

#[test]
fn test_cli_trace_summary_removed() {
    // The offline trace-summary workflow was removed; --trace-compat still
    // writes the raw JSONL for manual inspection.
    let (code, _stdout, stderr) = run_cli(&["trace-summary", "trace.jsonl"]);
    assert_ne!(code, 0, "trace-summary should no longer be a subcommand");
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("invalid"),
        "stderr should report an unknown subcommand: {}",
        stderr
    );
}

#[test]
fn test_cli_rejects_unknown_addressing_style() {
    // A typo used to be dropped silently, leaving the run on whatever the
    // config resolved to while looking like the flag had been applied.
    let (code, stdout, stderr) = run_cli(&["--addressing-style", "bogus", "doctor"]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        stderr.contains("path, virtual, auto"),
        "expected the accepted values in the error: {}",
        stderr
    );
}

#[test]
fn test_cli_accepts_known_addressing_styles() {
    for style in ["path", "virtual", "auto"] {
        let (code, stdout, stderr) = run_cli(&["--addressing-style", style, "doctor"]);
        assert_eq!(code, 0, "{}: stdout: {}\nstderr: {}", style, stdout, stderr);
    }
}

#[test]
fn test_cli_manifest_summary_attributes_checks_per_part_file() {
    // A pooled list run records one `parquet` artifact per writer. Their
    // checks used to share a name, so a failing part was indistinguishable
    // from a passing one in a report keyed by check name.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("out.parquet"), b"x").unwrap();
    std::fs::write(dir.path().join("out.part1.parquet"), b"x").unwrap();
    let manifest = dir.path().join("run.json");
    std::fs::write(
        &manifest,
        r#"{
  "tool_version": "0.24.0",
  "status": "success",
  "exit_code": 0,
  "inputs": {"output_format": "parquet"},
  "outputs": {"parquet_file": "out.parquet", "ks_file": null, "hints_file": null, "trace_compat": null, "log_file": null},
  "artifacts": [
    {"kind": "parquet", "path": "out.parquet", "exists": true, "size_bytes": 1, "sha256": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"},
    {"kind": "parquet", "path": "out.part1.parquet", "exists": true, "size_bytes": 999, "sha256": "deadbeef"}
  ],
  "metrics": {"received_objects": 2, "streamed_rows": 2, "unique_prefixes": 1, "parquet_rows": 2, "ks_entries": 0, "bytes_total": 2, "summary_only": false, "fatal_errors": 0, "output_errors": 0, "top_prefixes": []},
  "warnings": []
}"#,
    )
    .unwrap();

    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "manifest-summary",
            manifest.to_str().unwrap(),
            "--check",
            "--json",
        ],
        dir.path(),
    );
    assert_eq!(code, 6, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["check"]["ok"], false);

    let names: Vec<&str> = json["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| check["name"].as_str().unwrap())
        .collect();
    // The first artifact keeps the bare kind so existing consumers still
    // find it; the second is attributable to its own file.
    assert!(names.contains(&"artifact_sha256:parquet"), "{:?}", names);
    assert!(names.contains(&"artifact_sha256:parquet#1"), "{:?}", names);
    let mut sorted = names.clone();
    sorted.sort_unstable();
    let before = sorted.len();
    sorted.dedup();
    assert_eq!(
        before,
        sorted.len(),
        "check names must be unique: {:?}",
        names
    );

    let status_of = |name: &str| -> &str {
        json["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == name)
            .map(|check| check["status"].as_str().unwrap())
            .unwrap()
    };
    assert_eq!(status_of("artifact_sha256:parquet"), "ok");
    assert_eq!(status_of("artifact_sha256:parquet#1"), "fail");
}

// ── Runtime value validation ────────────────────────────────
//
// A zero here used to be accepted: zero concurrency listed nothing and still
// exited 0 (the reactor's fill loop never ran, so an agent reading the
// manifest saw an empty bucket), while zero worker threads and a
// zero-capacity channel panicked with exit 101, outside the documented
// exit-code contract.
#[test]
fn test_cli_rejects_zero_valued_runtime_knobs() {
    for (flag, value) in [("--concurrency", "0"), ("--threads", "0")] {
        let (code, stdout, stderr) = run_cli(&[
            "--dry-run",
            flag,
            value,
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ]);
        assert_eq!(code, 2, "{}: stdout: {}\nstderr: {}", flag, stdout, stderr);
        assert!(
            stderr.contains("must be at least 1"),
            "{} should name the bound: {}",
            flag,
            stderr
        );
    }
}

#[test]
fn test_cli_rejects_zero_valued_config_knobs() {
    for (key, body) in [
        ("channel.capacity", "[channel]\ncapacity = 0\n"),
        (
            "s3.operation_timeout_secs",
            "[s3]\noperation_timeout_secs = 0\n",
        ),
        (
            "s3.connect_timeout_secs",
            "[s3]\nconnect_timeout_secs = 0\n",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("c.toml");
        std::fs::write(&config, body).unwrap();
        let (code, stdout, stderr) = run_cli(&[
            "--config",
            config.to_str().unwrap(),
            "--dry-run",
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ]);
        assert_eq!(code, 2, "{}: stdout: {}\nstderr: {}", key, stdout, stderr);
        assert!(
            stderr.contains(key),
            "error should name {}: {}",
            key,
            stderr
        );
    }
}

#[test]
fn test_cli_rejects_unknown_compression_instead_of_falling_back() {
    // The old behaviour silently wrote gzip — a different codec from the
    // documented default — while the dry-run plan still echoed the value the
    // user typed.
    let (code, stdout, stderr) = run_cli(&[
        "--dry-run",
        "--compression",
        "zstdd",
        "list",
        "--bucket",
        "b",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        stderr.contains("zstd"),
        "should list valid codecs: {}",
        stderr
    );

    for codec in [
        "uncompressed",
        "snappy",
        "gzip",
        "lz4",
        "lz4_raw",
        "zstd",
        "brotli",
    ] {
        let (code, _stdout, stderr) = run_cli(&[
            "--dry-run",
            "--compression",
            codec,
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ]);
        assert_eq!(code, 0, "{} must stay accepted: {}", codec, stderr);
    }
}

#[test]
fn test_cli_rejects_unknown_profile_and_warns_malformed_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    // An unknown profile applies no preset (the run would go to AWS): exit 2.
    let (code, _, stderr) = run_cli_in_dir(
        &[
            "--dry-run",
            "--profile",
            "nosuchprofile",
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 2, "{}", stderr);
    assert!(stderr.contains("nosuchprofile"), "{}", stderr);

    let plan = dir.path().join("plan.json");
    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--dry-run",
            "--plan-json",
            plan.to_str().unwrap(),
            "--endpoint-url",
            "not-a-url",
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&plan).unwrap()).unwrap();
    let warnings = json["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .filter_map(|w| w.as_str())
            .any(|w| w.contains("not-a-url")),
        "a malformed endpoint must be reported before the run: {:?}",
        warnings
    );
}

#[test]
fn test_cli_rejects_invalid_values_before_any_work() {
    // --plan-json only makes sense with --dry-run; without it a real scan ran
    // and the plan was never written.
    let (code, _, stderr) = run_cli(&["--plan-json", "plan.json", "list", "--bucket", "b"]);
    assert_eq!(code, 2, "{}", stderr);
    assert!(stderr.contains("--dry-run"), "{}", stderr);

    let (code, _, stderr) = run_cli(&["--max-keys", "0", "--dry-run", "list", "--bucket", "b"]);
    assert_eq!(code, 2, "{}", stderr);

    // An out-of-range level used to fall back to gzip while reports said zstd.
    let (code, _, stderr) = run_cli(&[
        "--compression",
        "zstd",
        "--compression-level",
        "99",
        "--dry-run",
        "list",
        "--bucket",
        "b",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 2, "{}", stderr);
    assert!(stderr.contains("compression_level"), "{}", stderr);
}

#[test]
fn test_cli_doctor_suggestions_respect_env_credentials() {
    // Static keys in the environment already give the SDK credentials;
    // suggesting `export AWS_PROFILE=default` would point it elsewhere.
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-turbo-list"));
    clear_aws_env(&mut cmd);
    let output = cmd
        .env("AWS_ACCESS_KEY_ID", "test-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "test-secret-key")
        .args(["--provider", "minio", "doctor"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(3), "{}", stdout);
    assert!(!stdout.contains("NEXT export AWS_PROFILE"), "{}", stdout);
    assert!(stdout.contains("pass --endpoint-url"), "{}", stdout);
}

#[test]
fn test_cli_dry_run_hints_plan_matches_run_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    let cases: [(&[&str], &str, bool); 4] = [
        // No cached hints: startup discovery partitions the run.
        (&[], "startup_discovery", false),
        // Runtime splitting still fans the run out.
        (
            &["--no-auto-hints"],
            "disabled_single_segment_fallback",
            false,
        ),
        // These can never fan out, and only these say so.
        (&["--delimiter", "/"], "delimiter_single_segment", true),
        (&["--start-after", "k"], "single_chain", true),
    ];
    for (extra, source, single_chain_warning) in cases {
        let mut args: Vec<&str> = extra.to_vec();
        args.extend([
            "--dry-run",
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ]);
        let (code, stdout, stderr) = run_cli_in_dir(&args, dir.path());
        assert_eq!(code, 0, "{:?}: {}", extra, stderr);
        let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(json["hints"]["source"], source, "{:?}", extra);
        let warned = json["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("single ListObjectsV2 chain"));
        assert_eq!(
            warned, single_chain_warning,
            "{:?}: {}",
            extra, json["warnings"]
        );
    }
}

#[test]
fn test_cli_diff_target_uses_its_own_region_endpoint() {
    let target_warning = |args: &[&str]| -> Option<String> {
        let (code, stdout, stderr) = run_cli_without_aws_env(args);
        assert_eq!(code, 0, "{:?}: {}", args, stderr);
        let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        json["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|w| w.as_str())
            .find(|w| w.contains("diff target side lists against"))
            .map(str::to_string)
    };
    // Region-templated profile, different target region: the target gets its
    // own region's host, not the source's.
    let warning = target_warning(&[
        "--profile",
        "bos",
        "--dry-run",
        "diff",
        "--bucket",
        "a",
        "--region",
        "bj",
        "--target-bucket",
        "c",
        "--target-region",
        "gz",
    ])
    .expect("target endpoint should be reported");
    assert!(warning.contains("https://s3.gz.bcebos.com"), "{}", warning);
    // An explicit endpoint is the user's choice for both sides.
    assert!(
        target_warning(&[
            "--profile",
            "bos",
            "--endpoint-url",
            "https://s3.bj.bcebos.com",
            "--dry-run",
            "diff",
            "--bucket",
            "a",
            "--region",
            "bj",
            "--target-bucket",
            "c",
            "--target-region",
            "gz",
        ])
        .is_none()
    );
    // Same region on both sides: nothing to derive.
    assert!(
        target_warning(&[
            "--profile",
            "bos",
            "--dry-run",
            "diff",
            "--bucket",
            "a",
            "--region",
            "bj",
            "--target-bucket",
            "c",
            "--target-region",
            "bj",
        ])
        .is_none()
    );
}

#[test]
fn test_cli_rejects_outputs_sharing_a_path() {
    let dir = tempfile::tempdir().unwrap();
    let base = [
        "--dry-run",
        "list",
        "--bucket",
        "b",
        "--region",
        "us-east-1",
    ];
    let run = |extra: &[&str]| {
        let mut args: Vec<&str> = extra.to_vec();
        args.extend(base);
        run_cli_in_dir(&args, dir.path())
    };
    // Parquet and KS at one path: the KS file used to overwrite the Parquet.
    let (code, _, stderr) = run(&[
        "--output-parquet-file",
        "o/same",
        "--output-ks-file",
        "o/same",
    ]);
    assert_eq!(code, 2, "{}", stderr);
    assert!(stderr.contains("same file"), "{}", stderr);
    // A pooled part-file name counts too.
    let (code, _, stderr) = run(&[
        "--output-parquet-file",
        "o/x.parquet",
        "--output-ks-file",
        "o/./x.part2.parquet",
    ]);
    assert_eq!(code, 2, "{}", stderr);
    // Trace and log collide just the same.
    let (code, _, _) = run(&["--trace-compat", "o/t", "--output-log-file", "o/t"]);
    assert_eq!(code, 2);
    // Devices may be shared.
    let (code, _, stderr) = run(&[
        "--output-ks-file",
        "/dev/null",
        "--trace-compat",
        "/dev/null",
    ]);
    assert_eq!(code, 0, "{}", stderr);

    // Stale part files from an earlier run are announced in the plan.
    std::fs::create_dir_all(dir.path().join("o")).unwrap();
    std::fs::write(dir.path().join("o/x.part1.parquet"), b"stale").unwrap();
    let (code, stdout, _) = run(&["--output-parquet-file", "o/x.parquet"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Parquet part file(s) from an earlier run"),
        "{}",
        stdout
    );
}

#[test]
fn test_cli_preflight_matches_run_for_probe_hints_and_local_tools() {
    let dir = tempfile::tempdir().unwrap();
    // compat-probe without any endpoint is a setup error (3), like list.
    let (code, _, stderr) = run_cli_in_dir(
        &[
            "--dry-run",
            "compat-probe",
            "--region",
            "us-east-1",
            "--bucket",
            "b",
        ],
        dir.path(),
    );
    assert_eq!(code, 3, "{}", stderr);
    // ...and listing-only flags are rejected instead of ignored.
    let (code, _, stderr) = run_cli_in_dir(
        &[
            "--endpoint-url",
            "http://127.0.0.1:1",
            "--filter",
            "SOURCE.size > 1",
            "compat-probe",
            "--region",
            "us-east-1",
            "--bucket",
            "b",
        ],
        dir.path(),
    );
    assert_eq!(code, 2, "{}", stderr);
    assert!(stderr.contains("--filter"), "{}", stderr);

    // A hints file the run cannot load fails the dry run with exit 2.
    let (code, _, stderr) = run_cli_in_dir(
        &[
            "--hints-file",
            "nope.txt",
            "--dry-run",
            "list",
            "--bucket",
            "b",
            "--region",
            "r",
        ],
        dir.path(),
    );
    assert_eq!(code, 2, "{}", stderr);

    // init-config refuses --dry-run rather than writing the file anyway.
    let (code, _, _) = run_cli_in_dir(
        &["--dry-run", "init-config", "--output", "x.toml"],
        dir.path(),
    );
    assert_eq!(code, 2);
    assert!(!dir.path().join("x.toml").exists());

    // manifest-summary names a plan for what it is, in JSON when asked.
    let (code, _, _) = run_cli_in_dir(
        &[
            "--dry-run",
            "--plan-json",
            "plan.json",
            "list",
            "--bucket",
            "b",
            "--region",
            "r",
        ],
        dir.path(),
    );
    assert_eq!(code, 0);
    let (code, stdout, _) =
        run_cli_in_dir(&["manifest-summary", "plan.json", "--json"], dir.path());
    assert_eq!(code, 2);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "error");
    assert!(json["error"].as_str().unwrap().contains("dry-run plan"));
}

#[test]
fn test_cli_rejects_delimiter_with_hints_file() {
    // CommonPrefixes are not bounded by a segment's range, so hint boundaries
    // made delimiter runs drop or repeat folder rows.
    let dir = tempfile::tempdir().unwrap();
    let hints = dir.path().join("hints.txt");
    std::fs::write(&hints, "b\n").unwrap();
    let (code, _stdout, stderr) = run_cli_in_dir(
        &[
            "--dry-run",
            "--delimiter",
            "/",
            "--hints-file",
            hints.to_str().unwrap(),
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 2, "stderr: {}", stderr);
    assert!(stderr.contains("--delimiter"), "stderr: {}", stderr);
}

#[test]
fn test_dry_run_predicts_outputs_the_run_cannot_create() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("exist.parquet"), b"").unwrap();
    let (code, stdout, stderr) = run_cli_in_dir(
        &[
            "--dry-run",
            "--output-dir",
            "exist.parquet",
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 5, "stderr: {}", stderr);
    let plan: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(plan["status"], "blocked");
    // An existing explicit output is still fine, but the plan says it goes.
    let (code, stdout, _stderr) = run_cli_in_dir(
        &[
            "--dry-run",
            "--output-parquet-file",
            "exist.parquet",
            "list",
            "--bucket",
            "b",
            "--region",
            "us-east-1",
        ],
        dir.path(),
    );
    assert_eq!(code, 0);
    assert!(stdout.contains("will be overwritten"), "{}", stdout);
}

#[test]
fn test_dry_run_plans_prefix_distinct_names_log_file_and_slash_warning() {
    let dir = tempfile::tempdir().unwrap();
    let plan_for = |extra: &[&str]| -> serde_json::Value {
        let mut args = vec!["--dry-run"];
        args.extend_from_slice(extra);
        args.extend_from_slice(&["list", "--bucket", "b", "--region", "us-east-1"]);
        let (code, stdout, stderr) = run_cli_in_dir(&args, dir.path());
        assert_eq!(code, 0, "stderr: {}", stderr);
        serde_json::from_str(&stdout).unwrap()
    };
    // Different prefixes get different auto-generated names.
    let a = plan_for(&["--prefix", "dir0/"]);
    let b = plan_for(&["--prefix", "dir1/"]);
    let name = |plan: &serde_json::Value| {
        let path = plan["outputs"]["parquet_file"]
            .as_str()
            .unwrap()
            .to_string();
        path.rsplit_once('_').unwrap().0.to_string()
    };
    assert_ne!(name(&a), name(&b));
    // --log is named after the outputs, beside them, and reported; the
    // KeySpace file follows an explicit Parquet path.
    let logged = plan_for(&["--log", "--output-dir", "out"]);
    let parquet = logged["outputs"]["parquet_file"].as_str().unwrap();
    let base = parquet.strip_suffix(".parquet").unwrap();
    assert!(base.starts_with("out/"), "{}", logged["outputs"]);
    assert_eq!(logged["outputs"]["log_file"], format!("{}.log", base));
    assert_eq!(logged["outputs"]["ks_file"], format!("{}.ks", base));
    let explicit = plan_for(&["--log", "--output-parquet-file", "x/run.parquet"]);
    assert_eq!(explicit["outputs"]["ks_file"], "x/run.ks");
    assert_eq!(explicit["outputs"]["log_file"], "x/run.log");
    // A leading '/' matches no ordinary key; the plan says so.
    let slashed = plan_for(&["--prefix", "/dir1/"]);
    assert!(
        slashed["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("Did you mean 'dir1/'")),
        "{}",
        slashed["warnings"]
    );
}

#[test]
fn test_diff_target_region_defaults_to_region() {
    // It used to fall through to the ambient AWS_REGION, invisibly.
    let (code, stdout, stderr) = run_cli(&[
        "--dry-run",
        "diff",
        "--bucket",
        "src",
        "--region",
        "us-west-2",
        "--target-bucket",
        "dst",
    ]);
    assert_eq!(code, 0, "stderr: {}", stderr);
    let plan: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(plan["inputs"]["target_region"], "us-west-2");
}

#[test]
fn test_pre_run_failures_report_like_failed_runs() {
    let (code, stdout, stderr) = run_cli(&[
        "--agent",
        "--filter",
        "SOURCE.size >",
        "list",
        "--bucket",
        "b",
        "--region",
        "us-east-1",
    ]);
    assert_eq!(code, 2);
    assert!(
        stderr.contains("s3-turbo-list: run failed (exit 2):"),
        "stderr: {}",
        stderr
    );
    let result: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(result["status"], "failed");
    assert_eq!(result["exit_code"], 2);
}

#[test]
fn test_doctor_json_reports_an_unreadable_hints_file_as_json() {
    let (code, stdout, _stderr) =
        run_cli(&["--hints-file", "does-not-exist.txt", "doctor", "--json"]);
    assert_eq!(code, 2);
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["status"], "error");
    assert_eq!(report["checks"][0]["name"], "hints");
}

#[test]
fn test_doctor_json_reports_usage_errors_as_json() {
    // An option doctor does not take is a usage error; stdout still carries
    // doctor's JSON shape.
    let (code, stdout, stderr) = run_cli(&["doctor", "--json", "--summary-only"]);
    assert_eq!(code, 2, "stderr: {}", stderr);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["status"], "error");
    assert_eq!(json["checks"][0]["name"], "cli");
}
