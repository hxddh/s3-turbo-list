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

    let (code, stdout, _stderr) = run_cli(&["benchmark-local", "--help"]);
    assert_eq!(code, 0, "benchmark-local --help should exit 0");
    assert!(stdout.contains("--objects"));
    assert!(stdout.contains("--output-format"));

    let (code, stdout, _stderr) = run_cli(&["init-config", "--help"]);
    assert_eq!(code, 0, "init-config --help should exit 0");
    assert!(stdout.contains("--overwrite"));

    let (code, stdout, _stderr) = run_cli(&["guide", "--help"]);
    assert_eq!(code, 0, "guide --help should exit 0");
    assert!(stdout.contains("provider quickstart") || stdout.contains("recipe"));

    let (code, stdout, _stderr) = run_cli(&["manifest-summary", "--help"]);
    assert_eq!(code, 0, "manifest-summary --help should exit 0");
    assert!(stdout.contains("--json"));
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
fn test_cli_guide_local_only() {
    // Named recipes.
    let (code, stdout, stderr) = run_cli(&["guide", "aws-basic"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("--dry-run"));
    assert!(stdout.contains("--output-dir"));
    assert!(stdout.contains("--delimiter ''"));

    let (code, stdout, stderr) = run_cli(&["guide", "summary"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("--summary-only"));
    assert!(stdout.contains("manifest-summary"));

    let (code, stdout, stderr) = run_cli(&["guide", "pipe"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("--output-format tsv"));
    assert!(stdout.contains("--output-format ndjson"));
    assert!(stdout.contains("manifest-summary"));

    let (code, stdout, stderr) = run_cli(&["guide", "filter"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("SOURCE.size > 1073741824"));
    assert!(stdout.contains("SOURCE.last_modified"));
    assert!(stdout.contains("Rejected before network"));

    let (code, stdout, stderr) = run_cli(&["guide", "release-check"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("./scripts/check-release-env.sh"));
    assert!(stdout.contains("cargo clippy --all-targets -- -D warnings"));
    assert!(stdout.contains("BUILD_MODE=clang"));
    assert!(stdout.contains("Benchmark smoke checks"));
    assert!(stdout.contains("OBJECTS=1000 BATCH_SIZE=100 PREFIXES=16 ./scripts/benchmark-local.sh"));
    assert!(stdout.contains("BIN=./target/release/s3-turbo-list"));
    assert!(stdout.contains("gh workflow run release-assets.yml"));
    assert!(stdout.contains("./scripts/verify-release-assets.sh"));
    assert!(stdout.contains("do not contact S3-compatible cloud endpoints"));

    let (code, stdout, stderr) = run_cli(&["guide", "diff-safe"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Safe diff"));
    assert!(stdout.contains("diff --bucket"));
    assert!(stdout.contains("manifest-summary"));

    // Provider topics dispatch to quickstarts.
    let (code, stdout, stderr) = run_cli(&["guide", "r2"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("AWS_PROFILE"));
    assert!(stdout.contains("--profile r2"));
    assert!(stdout.contains("--delimiter ''"));

    // No topic prints the overview, which points back at guide topics.
    let (code, stdout, stderr) = run_cli(&["guide"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("First run"));
    assert!(stdout.contains("--delimiter ''"));
    assert!(stdout.contains("guide filter"));
    assert!(stdout.contains("release-check"));

    // index lists the recipe names.
    let (code, stdout, stderr) = run_cli(&["guide", "index"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Available recipes"));
    assert!(stdout.contains("guide <name>"));
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
        "--simple",
        "--fix-suggestions",
    ]);
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("ERROR output_parquet_parent"));
    assert!(stdout.contains("NEXT mkdir -p"));
}

#[test]
fn test_cli_doctor_json_local_only_success() {
    let (code, stdout, stderr) = run_cli(&["doctor", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["status"], "ok");
    let checks = json["checks"].as_array().unwrap();
    assert!(checks
        .iter()
        .any(|check| check["name"] == "network" && check["status"] == "skipped"));
}

#[test]
fn test_cli_doctor_warns_when_aws_profile_matches_endpoint_preset_name() {
    let (code, stdout, stderr) = run_cli_with_aws_profile(&["doctor", "--json"], "bos");
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
    // `guide aws` prints the quickstart plus the endpoint-compatibility facts.
    let (code, stdout, stderr) = run_cli(&["guide", "aws"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("AWS quickstart"));
    assert!(stdout.contains("Endpoint compatibility profile:"));
    assert!(stdout.contains("provider: AWS S3"));
    assert!(stdout.contains("requires_explicit_endpoint: false"));

    // Providers without a hand-written quickstart still print profile facts.
    let (code, stdout, stderr) = run_cli(&["guide", "oss"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("Endpoint compatibility profile:"));
    assert!(stdout.contains("Alibaba Cloud OSS"));
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
        "--producers",
        "2",
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(json["tool_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(json["network"], "none: synthetic local data only");
    assert_eq!(json["compression"], "zstd");
    assert_eq!(json["compression_level"], 1);
    assert_eq!(json["output_format"], "parquet");
    assert_eq!(json["objects"], 32);
    assert_eq!(json["producers"], 2);
    assert!(json["channel_capacity"].as_u64().unwrap() > 0);
    assert!(json["producer_send_wait_secs"].as_f64().unwrap() >= 0.0);
    assert!(json["rows_per_sec"].as_f64().unwrap() > 0.0);
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
fn test_cli_benchmark_local_ndjson_no_cloud() {
    let (code, stdout, stderr) = run_cli(&[
        "benchmark-local",
        "--objects",
        "32",
        "--batch-size",
        "8",
        "--prefixes",
        "4",
        "--output-format",
        "ndjson",
        "--json",
    ]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["network"], "none: synthetic local data only");
    assert_eq!(json["output_format"], "ndjson");
    assert_eq!(json["parquet_file"], serde_json::Value::Null);
    assert_eq!(json["ks_file"], serde_json::Value::Null);
    assert!(json["text_bytes"].as_u64().unwrap() > 0);
    assert!(json["text_bytes_per_object"].as_f64().unwrap() > 0.0);
    assert!(json["text_mib_per_sec"].as_f64().unwrap() > 0.0);
    assert_eq!(json["metrics"]["received_objects"], 32);
    assert_eq!(json["metrics"]["streamed_rows"], 32);
    assert_eq!(json["metrics"]["parquet_rows"], 0);
    assert_eq!(json["metrics"]["ks_entries"], 0);
}

#[test]
fn test_cli_benchmark_local_diff_map_no_cloud() {
    let (code, stdout, stderr) = run_cli(&[
        "benchmark-local",
        "--benchmark",
        "diff-map",
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
    assert_eq!(json["benchmark"], "diff-map");
    assert_eq!(json["output_format"], "diff-map");
    assert_eq!(json["objects"], 32);
    assert_eq!(json["metrics"]["received_batches"], 8);
    assert_eq!(json["metrics"]["received_objects"], 64);
    assert_eq!(json["metrics"]["streamed_rows"], 32);
    assert_eq!(json["metrics"]["unique_prefixes"], 4);
    // diff-map measures the merge plus row encoding against a null writer.
    assert_eq!(json["metrics"]["parquet_rows"], 32);
    assert_eq!(json["parquet_file"], serde_json::Value::Null);
    assert_eq!(json["text_file"], serde_json::Value::Null);
    assert!(json["objects_per_sec"].as_f64().unwrap() > 0.0);
}

#[test]
fn test_cli_benchmark_local_diff_output_no_cloud() {
    let (code, stdout, stderr) = run_cli(&[
        "benchmark-local",
        "--benchmark",
        "diff-output",
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
    assert_eq!(json["benchmark"], "diff-output");
    assert_eq!(json["output_format"], "diff-output");
    assert_eq!(json["objects"], 32);
    assert_eq!(json["metrics"]["received_batches"], 8);
    assert_eq!(json["metrics"]["received_objects"], 48);
    assert_eq!(json["metrics"]["streamed_rows"], 32);
    assert_eq!(json["metrics"]["unique_prefixes"], 4);
    assert_eq!(json["metrics"]["parquet_rows"], 32);
    assert_eq!(json["metrics"]["ks_entries"], 4);
    assert_eq!(json["parquet_file"], serde_json::Value::Null);
    assert_eq!(json["ks_file"], serde_json::Value::Null);
    assert!(json["parquet_bytes"].as_u64().unwrap() > 0);
    assert!(json["output_bytes_per_object"].as_f64().unwrap() > 0.0);
    assert!(json["output_mib_per_sec"].as_f64().unwrap() > 0.0);
}

#[test]
fn test_cli_benchmark_local_diff_output_shapes_no_cloud() {
    for (shape, expected_received_objects) in
        [("mixed", 48), ("all-equal", 64), ("all-changed", 64)]
    {
        let (code, stdout, stderr) = run_cli(&[
            "benchmark-local",
            "--benchmark",
            "diff-output",
            "--diff-shape",
            shape,
            "--objects",
            "32",
            "--batch-size",
            "8",
            "--prefixes",
            "4",
            "--json",
        ]);
        assert_eq!(
            code, 0,
            "shape: {}\nstdout: {}\nstderr: {}",
            shape, stdout, stderr
        );

        let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(json["network"], "none: synthetic local data only");
        assert_eq!(json["benchmark"], "diff-output");
        assert_eq!(json["objects"], 32);
        assert_eq!(
            json["metrics"]["received_objects"],
            expected_received_objects
        );
        assert_eq!(json["metrics"]["streamed_rows"], 32);
        assert_eq!(json["metrics"]["unique_prefixes"], 4);
        assert_eq!(json["metrics"]["parquet_rows"], 32);
        assert_eq!(json["metrics"]["ks_entries"], 4);
        assert!(json["output_bytes_per_object"].as_f64().unwrap() > 0.0);
    }
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
fn test_cli_doctor_reports_zstd_default_no_cloud() {
    let (code, stdout, stderr) = run_cli(&["doctor", "--json"]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["resolved_config"]["output"]["compression"], "zstd");
    assert_eq!(json["resolved_config"]["output"]["compression_level"], 1);
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
    assert!(stdout.contains("config:"));
    assert!(stdout.contains(config_path.to_str().unwrap()));
}

#[test]
fn test_cli_doctor_warns_for_missing_explicit_config_no_cloud() {
    let config_path = "/tmp/s3-turbo-list-missing-test-config.toml";
    let _ = std::fs::remove_file(config_path);

    let (code, stdout, stderr) = run_cli(&["--config", config_path, "doctor", "--json"]);
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

    let (code, stdout, stderr) = run_cli(&["--config", config_path, "doctor"]);
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

    let (code, stdout, stderr) =
        run_cli(&["--config", config.to_str().unwrap(), "doctor", "--json"]);
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
    assert!(stderr.contains("diff with --hints-file is unsupported by design"));
    assert!(stderr.contains("unsupported by design"));
    assert!(stderr.contains("cannot describe both sides"));
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
    assert!(stderr.contains("diff --resume is unsupported by design"));
    assert!(stderr.contains("unsupported by design"));
    assert!(stderr.contains("partial paired comparisons"));
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
total_segments = 2
completed_indices = [0]
last_updated = "2026-05-17T00:00:00Z"

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
    assert_eq!(json["hints"]["source"], "diff_per_side_automatic");
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
                .contains("partitions each side automatically")
        }));
    assert!(json["warnings"].as_array().unwrap().iter().any(|warning| {
        warning
            .as_str()
            .unwrap()
            .contains("partitions each side automatically")
    }));
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
    assert_eq!(hints["metadata"]["scan_mode"], "sampled");
    assert_eq!(hints["metadata"]["estimate_mode"], "sampled");
    assert_eq!(hints["boundary_count"], 1);
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
