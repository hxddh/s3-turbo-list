//! The command-line contract of the `s3tl` binary, without any network:
//! help and version, usage errors, dry-run plans and the result document.
//!
//! Every run here either fails validation or is a `--dry-run`, so nothing
//! is sent anywhere; the endpoint points at the discard port of 127.0.0.1
//! regardless.

mod common;

use serde_json::{Value, json};
use std::path::Path;

/// Where nothing listens (and nothing is ever sent).
const ENDPOINT: &str = "http://127.0.0.1:9";

/// Every top-level key of `s3tl.result/1`, always present.
const RESULT_KEYS: [&str; 16] = [
    "schema",
    "version",
    "command",
    "ok",
    "status",
    "exit_code",
    "error",
    "source",
    "target",
    "output",
    "stats",
    "diff",
    "resume",
    "plan",
    "check",
    "warnings",
];

fn run_in(cwd: &Path, args: &[&str], extra_env: &[(&str, &str)]) -> (i32, String, String) {
    let output = common::hermetic_command(cwd)
        .current_dir(cwd)
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .envs(extra_env.iter().copied())
        .args(args)
        .output()
        .expect("run s3tl");
    common::exit_and_output(output)
}

fn run(args: &[&str]) -> (i32, String, String) {
    let dir = tempfile::tempdir().unwrap();
    run_in(dir.path(), args, &[])
}

/// stdout must be exactly one JSON document carrying every result key.
fn one_result(stdout: &str) -> Value {
    let mut docs = serde_json::Deserializer::from_str(stdout).into_iter::<Value>();
    let result = docs
        .next()
        .unwrap_or_else(|| panic!("no JSON document on stdout: {:?}", stdout))
        .unwrap_or_else(|e| panic!("stdout is not JSON ({}): {:?}", e, stdout));
    assert!(
        docs.next().is_none(),
        "more than one document on stdout: {}",
        stdout
    );
    let object = result.as_object().expect("the result is an object");
    for key in RESULT_KEYS {
        assert!(object.contains_key(key), "missing key {}: {}", key, result);
    }
    assert_eq!(object.len(), RESULT_KEYS.len(), "{}", result);
    assert_eq!(result["schema"], "s3tl.result/1");
    assert_eq!(result["version"], env!("CARGO_PKG_VERSION"));
    assert!(result["warnings"].is_array());
    result
}

/// A usage error: exit 2, one result with `ok=false` and `error_code`, and
/// a human line on stderr naming the code.
fn usage_error(args: &[&str], cwd: &Path, error_code: &str) -> Value {
    let (code, stdout, stderr) = run_in(cwd, args, &[]);
    assert_eq!(
        code, 2,
        "{:?}\nstdout: {}\nstderr: {}",
        args, stdout, stderr
    );
    let result = one_result(&stdout);
    assert_eq!(result["ok"], false, "{}", result);
    assert_eq!(result["status"], "failed", "{}", result);
    assert_eq!(result["exit_code"], 2, "{}", result);
    assert_eq!(
        result["error"]["code"], error_code,
        "{:?}: {}",
        args, result
    );
    assert_eq!(result["error"]["retryable"], false, "{}", result);
    assert!(
        result["error"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty())
    );
    // A message for humans on stderr: clap's own for a malformed command
    // line, else one line naming the code.
    if error_code == "usage.invalid_argument" {
        assert!(stderr.contains("error:"), "{:?}: {}", args, stderr);
    } else {
        assert!(
            stderr.starts_with(&format!("s3tl: failed (exit 2): {}", error_code)),
            "{:?}: {}",
            args,
            stderr
        );
    }
    result
}

/// A successful dry run: exit 0, status `planned`, no stderr noise.
fn plan(args: &[&str], cwd: &Path, extra_env: &[(&str, &str)]) -> Value {
    let (code, stdout, stderr) = run_in(cwd, args, extra_env);
    assert_eq!(
        code, 0,
        "{:?}\nstdout: {}\nstderr: {}",
        args, stdout, stderr
    );
    let result = one_result(&stdout);
    assert_eq!(result["ok"], true, "{}", result);
    assert_eq!(result["status"], "planned", "{}", result);
    assert_eq!(result["exit_code"], 0);
    assert_eq!(result["error"], Value::Null);
    assert_eq!(result["stats"], Value::Null, "a plan lists nothing");
    assert!(stderr.is_empty(), "{}", stderr);
    result
}

/// Write `DIR/_checkpoint.json` for `s3://b/<prefix>` against `ENDPOINT`.
fn write_checkpoint(dir: &Path, prefix: &str, ranges: &[(&str, Option<&str>)]) {
    std::fs::create_dir_all(dir).unwrap();
    let remaining: Vec<Value> = ranges
        .iter()
        .map(|(start_after, end)| json!({"start_after": start_after, "end": end}))
        .collect();
    let checkpoint = json!({
        "schema": "s3tl.checkpoint/1",
        "identity": {
            "bucket": "b",
            "prefix": prefix,
            "endpoint_url": ENDPOINT,
            "delimiter": "",
            "max_keys": null,
        },
        "remaining": remaining,
        "updated_at": "2026-05-17T00:00:00Z",
    });
    std::fs::write(dir.join("_checkpoint.json"), checkpoint.to_string()).unwrap();
}

// ── Help and version ────────────────────────────────────────

#[test]
fn help_and_version_exit_zero() {
    for args in [
        &["--help"][..],
        &["-h"],
        &["list", "--help"],
        &["diff", "--help"],
        &["check", "--help"],
    ] {
        let (code, stdout, stderr) = run(args);
        assert_eq!(code, 0, "{:?}: {}", args, stderr);
        assert!(stdout.contains("Usage: s3tl"), "{:?}: {}", args, stdout);
    }
    let (code, stdout, _) = run(&["--help"]);
    assert_eq!(code, 0);
    for command in ["list", "diff", "check"] {
        assert!(stdout.contains(command), "{}", stdout);
    }
    // Hidden testing options stay out of the help.
    assert!(!stdout.contains("--max-keys"), "{}", stdout);
    assert!(!stdout.contains("--no-auto-hints"), "{}", stdout);

    for flag in ["--version", "-V"] {
        let (code, stdout, _) = run(&[flag]);
        assert_eq!(code, 0);
        assert_eq!(stdout.trim(), format!("s3tl {}", env!("CARGO_PKG_VERSION")));
    }
}

// ── Usage errors ────────────────────────────────────────────

#[test]
fn command_line_parse_errors_are_usage_errors_with_a_result() {
    let dir = tempfile::tempdir().unwrap();
    for args in [
        &["list", "s3://b", "--bogus"][..],
        &["list"],
        &["frobnicate"],
        &["list", "s3://b", "-c", "0"],
        &["list", "s3://b", "--max-keys", "1001"],
        &["diff", "s3://a", "s3://b", "--resume"],
        &["diff", "s3://a", "s3://b", "--delimiter", "/"],
    ] {
        let result = usage_error(args, dir.path(), "usage.invalid_argument");
        assert!(result["error"]["hint"].is_string(), "{}", result);
    }
    let result = usage_error(
        &["list", "s3://b", "--bogus"],
        dir.path(),
        "usage.invalid_argument",
    );
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--bogus"),
        "{}",
        result
    );
    // The whole error, naming what is missing, and the command it is for.
    let result = usage_error(&["list"], dir.path(), "usage.invalid_argument");
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("<S3_URL>"),
        "{}",
        result
    );
    assert_eq!(result["command"], "list");
    // No command at all: help on stderr, and a usage-error result.
    let (code, stdout, stderr) = run_in(dir.path(), &[], &[]);
    assert_eq!(code, 2, "{}", stderr);
    assert!(stderr.contains("Usage:"), "{}", stderr);
    let result = one_result(&stdout);
    assert_eq!(result["error"]["code"], "usage.invalid_argument");
    assert_eq!(result["command"], "unknown");
}

#[test]
fn bad_urls_and_endpoints_are_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    for args in [
        &["list", "b/prefix"][..],
        &["list", "s3:///prefix"],
        &["list", "https://b.s3.amazonaws.com/x"],
        &["diff", "s3://a", "not-a-url"],
        &["check", "b"],
    ] {
        usage_error(args, dir.path(), "usage.invalid_url");
    }
    let result = usage_error(
        &["--endpoint-url", "localhost:9000", "list", "s3://b"],
        dir.path(),
        "usage.invalid_endpoint",
    );
    assert_eq!(result["command"], "list");
    // The endpoint is redacted even in the error message.
    let result = usage_error(
        &[
            "--endpoint-url",
            "ftp://user:hunter2@host",
            "list",
            "s3://b",
        ],
        dir.path(),
        "usage.invalid_endpoint",
    );
    assert!(!result.to_string().contains("hunter2"), "{}", result);
    let (_, stdout, stderr) = run_in(
        dir.path(),
        &[
            "--endpoint-url",
            "ftp://user:hunter2@host",
            "list",
            "s3://b",
        ],
        &[],
    );
    assert!(!stdout.contains("hunter2") && !stderr.contains("hunter2"));
}

#[test]
fn output_rules_are_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let e = ["--endpoint-url", ENDPOINT];

    // diff writes a dataset, never NDJSON on stdout.
    usage_error(
        &[&e[..], &["diff", "s3://a", "s3://b", "-o", "-"]].concat(),
        dir.path(),
        "usage.invalid_output",
    );
    usage_error(
        &[&e[..], &["list", "s3://b", "-o", ""]].concat(),
        dir.path(),
        "usage.invalid_output",
    );

    // --resume continues a dataset directory.
    usage_error(
        &[&e[..], &["list", "s3://b", "--resume"]].concat(),
        dir.path(),
        "usage.resume_needs_output",
    );
    // With -o - the result is on stderr, stdout being the data channel.
    let (code, result) = stderr_result(
        &[&e[..], &["list", "s3://b", "--resume", "-o", "-"]].concat(),
        dir.path(),
    );
    assert_eq!(code, 2);
    assert_eq!(result["error"]["code"], "usage.resume_needs_output");

    // A fresh run needs an absent or empty directory.
    let full = dir.path().join("full");
    std::fs::create_dir(&full).unwrap();
    std::fs::write(full.join("something"), b"x").unwrap();
    let result = usage_error(
        &[&e[..], &["list", "s3://b", "-o", full.to_str().unwrap()]].concat(),
        dir.path(),
        "usage.output_not_empty",
    );
    assert!(
        result["error"]["hint"]
            .as_str()
            .unwrap()
            .contains("new directory"),
        "{}",
        result
    );
    // ...and says so differently when the directory holds an interrupted run.
    let interrupted = dir.path().join("interrupted");
    write_checkpoint(&interrupted, "", &[("", None)]);
    let result = usage_error(
        &[
            &e[..],
            &["list", "s3://b", "-o", interrupted.to_str().unwrap()],
        ]
        .concat(),
        dir.path(),
        "usage.output_not_empty",
    );
    assert!(
        result["error"]["hint"]
            .as_str()
            .unwrap()
            .contains("--resume"),
        "{}",
        result
    );
    // diff follows the same rule.
    usage_error(
        &[
            &e[..],
            &["diff", "s3://a", "s3://b", "-o", full.to_str().unwrap()],
        ]
        .concat(),
        dir.path(),
        "usage.output_not_empty",
    );

    // A regular file is not a dataset directory.
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    usage_error(
        &[&e[..], &["list", "s3://b", "-o", file.to_str().unwrap()]].concat(),
        dir.path(),
        "usage.output_not_a_directory",
    );

    // Nothing was touched.
    assert_eq!(std::fs::read(full.join("something")).unwrap(), b"x");
    assert_eq!(std::fs::read_dir(&full).unwrap().count(), 1);
    assert_eq!(std::fs::read(&file).unwrap(), b"x");
}

#[test]
fn resume_needs_a_matching_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let e = ["--endpoint-url", ENDPOINT];

    // No checkpoint: an absent directory, an empty one, a finished dataset.
    let absent = dir.path().join("absent");
    let empty = dir.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    let finished = dir.path().join("finished");
    std::fs::create_dir(&finished).unwrap();
    std::fs::write(finished.join("_result.json"), b"{}").unwrap();
    for out in [&absent, &empty, &finished] {
        let result = usage_error(
            &[
                &e[..],
                &["list", "s3://b", "-o", out.to_str().unwrap(), "--resume"],
            ]
            .concat(),
            dir.path(),
            "usage.no_checkpoint",
        );
        assert!(result["error"]["hint"].is_string());
    }
    assert!(!absent.exists(), "validation must not create the directory");

    // A checkpoint of another run: each identity field is compared.
    let out = dir.path().join("out");
    write_checkpoint(&out, "logs/", &[("logs/m", None)]);
    let cases: [(&[&str], &str); 5] = [
        (
            &["--endpoint-url", ENDPOINT, "list", "s3://other/logs/"],
            "bucket",
        ),
        (
            &["--endpoint-url", ENDPOINT, "list", "s3://b/data/"],
            "prefix",
        ),
        (
            &[
                "--endpoint-url",
                "http://127.0.0.1:10",
                "list",
                "s3://b/logs/",
            ],
            "endpoint_url",
        ),
        (
            &[
                "--endpoint-url",
                ENDPOINT,
                "list",
                "s3://b/logs/",
                "--delimiter",
                "/",
            ],
            "delimiter",
        ),
        (
            &[
                "--endpoint-url",
                ENDPOINT,
                "list",
                "s3://b/logs/",
                "--max-keys",
                "10",
            ],
            "max_keys",
        ),
    ];
    for (args, field) in cases {
        let args = [args, &["-o", out.to_str().unwrap(), "--resume"]].concat();
        let result = usage_error(&args, dir.path(), "usage.checkpoint_mismatch");
        assert!(
            result["error"]["message"].as_str().unwrap().contains(field),
            "{}: {}",
            field,
            result
        );
    }

    // An unreadable checkpoint.
    let broken = dir.path().join("broken");
    std::fs::create_dir(&broken).unwrap();
    std::fs::write(broken.join("_checkpoint.json"), b"not json").unwrap();
    usage_error(
        &[
            &e[..],
            &["list", "s3://b", "-o", broken.to_str().unwrap(), "--resume"],
        ]
        .concat(),
        dir.path(),
        "usage.checkpoint_unreadable",
    );
    // Validation touched none of them.
    assert_eq!(
        std::fs::read(broken.join("_checkpoint.json")).unwrap(),
        b"not json"
    );
}

// ── Dry-run plans ───────────────────────────────────────────

#[test]
fn list_dry_run_plans_without_touching_anything() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let result = plan(
        &[
            "--endpoint-url",
            ENDPOINT,
            "list",
            "s3://b/logs/",
            "-o",
            out.to_str().unwrap(),
            "-c",
            "8",
            "--dry-run",
        ],
        dir.path(),
        &[],
    );
    assert!(!out.exists(), "a dry run must not create the output");
    assert_eq!(result["command"], "list");
    assert_eq!(
        result["source"],
        json!({
            "url": "s3://b/logs/",
            "bucket": "b",
            "prefix": "logs/",
            "region": null,
            "endpoint_url": ENDPOINT,
            "path_style": true,
        })
    );
    assert_eq!(result["target"], Value::Null);
    assert_eq!(
        result["output"],
        json!({
            "format": "parquet",
            "path": out.display().to_string(),
            "files": [],
            "rows": 0,
        })
    );
    assert_eq!(result["resume"], Value::Null);
    assert_eq!(result["diff"], Value::Null);
    assert_eq!(result["check"], Value::Null);
    assert_eq!(
        result["plan"],
        json!({
            "partitioning": "startup_discovery",
            "partitioning_reason": null,
            "concurrency": 8,
            "compare": null,
            "versions": false,
            "max_keys": null,
            "resume_ranges": null,
            "first_part": 0,
            "region": "AWS_REGION, else us-east-1",
        })
    );
}

#[test]
fn list_dry_run_reports_how_it_would_partition_and_sign() {
    let dir = tempfile::tempdir().unwrap();
    // NDJSON on stdout (so the plan is on stderr); a delimiter listing is
    // one chain.
    let (code, result) = stderr_result(
        &[
            "list",
            "s3://b",
            "--endpoint-url",
            ENDPOINT,
            "--delimiter",
            "/",
            "-o",
            "-",
            "--dry-run",
        ],
        dir.path(),
    );
    assert_eq!(code, 0);
    assert_eq!(result["status"], "planned");
    assert_eq!(result["output"]["format"], "ndjson");
    assert_eq!(result["output"]["path"], "-");
    assert_eq!(result["plan"]["partitioning"], "single_segment");
    assert!(
        result["plan"]["partitioning_reason"]
            .as_str()
            .unwrap()
            .contains("--delimiter")
    );

    // Statistics only; the hidden testing options show in the plan.
    let result = plan(
        &[
            "list",
            "s3://b",
            "--endpoint-url",
            ENDPOINT,
            "--no-auto-hints",
            "--max-keys",
            "7",
            "--region",
            "eu-west-1",
            "--dry-run",
        ],
        dir.path(),
        &[],
    );
    assert_eq!(result["output"]["format"], "none");
    assert_eq!(result["output"]["path"], Value::Null);
    assert_eq!(result["plan"]["partitioning"], "single_segment");
    assert_eq!(result["plan"]["partitioning_reason"], "--no-auto-hints");
    assert_eq!(result["plan"]["max_keys"], 7);
    assert_eq!(result["plan"]["concurrency"], 100);
    assert_eq!(result["plan"]["region"], "explicit");
    assert_eq!(result["source"]["region"], "eu-west-1");

    // AWS S3 (no endpoint): virtual-hosted, region from the bucket.
    let result = plan(&["list", "s3://b", "--dry-run"], dir.path(), &[]);
    assert_eq!(result["source"]["endpoint_url"], Value::Null);
    assert_eq!(result["source"]["path_style"], false);
    assert_eq!(result["plan"]["region"], "detected from the bucket");
    // --path-style is honoured anywhere on the command line.
    let result = plan(
        &["--path-style", "list", "s3://b", "--dry-run"],
        dir.path(),
        &[],
    );
    assert_eq!(result["source"]["path_style"], true);

    // The endpoint can come from the SDK's standard variable.
    let result = plan(
        &["list", "s3://b", "--dry-run"],
        dir.path(),
        &[("AWS_ENDPOINT_URL_S3", "http://localhost:9000")],
    );
    assert_eq!(result["source"]["endpoint_url"], "http://localhost:9000");
    assert_eq!(result["source"]["path_style"], true);
}

#[test]
fn diff_dry_run_describes_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let result = plan(
        &[
            "--endpoint-url",
            ENDPOINT,
            "diff",
            "s3://left/a/",
            "s3://right/b/",
            "--target-endpoint-url",
            "https://other.example.test",
            "-o",
            out.to_str().unwrap(),
            "--dry-run",
        ],
        dir.path(),
        &[],
    );
    assert!(!out.exists());
    assert_eq!(result["command"], "diff");
    assert_eq!(result["source"]["bucket"], "left");
    assert_eq!(result["source"]["prefix"], "a/");
    assert_eq!(result["source"]["endpoint_url"], ENDPOINT);
    assert_eq!(result["target"]["bucket"], "right");
    assert_eq!(result["target"]["prefix"], "b/");
    assert_eq!(
        result["target"]["endpoint_url"],
        "https://other.example.test"
    );
    assert_eq!(result["target"]["path_style"], false);
    assert_eq!(result["output"]["format"], "parquet");
    assert_eq!(result["plan"]["partitioning"], "startup_discovery");
    // A diff is not resumable and its counts come from the run.
    assert_eq!(result["resume"], Value::Null);
    assert_eq!(result["diff"], Value::Null);

    // Without --target-endpoint-url the target uses --endpoint-url; without
    // -o it only counts.
    let result = plan(
        &[
            "--endpoint-url",
            ENDPOINT,
            "diff",
            "s3://l",
            "s3://r",
            "--dry-run",
        ],
        dir.path(),
        &[],
    );
    assert_eq!(result["target"]["endpoint_url"], ENDPOINT);
    assert_eq!(result["output"]["format"], "none");
}

#[test]
fn resume_dry_run_plans_the_checkpoint_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    write_checkpoint(&out, "", &[("", Some("m")), ("t", None)]);
    std::fs::write(out.join("part-00000.parquet"), b"").unwrap();
    std::fs::write(out.join("part-00002.parquet"), b"").unwrap();
    std::fs::write(out.join(".part-00003.parquet.tmp"), b"").unwrap();
    let result = plan(
        &[
            "--endpoint-url",
            ENDPOINT,
            "list",
            "s3://b",
            "-o",
            out.to_str().unwrap(),
            "--resume",
            "--dry-run",
        ],
        dir.path(),
        &[],
    );
    assert_eq!(result["plan"]["partitioning"], "checkpoint");
    assert_eq!(result["plan"]["resume_ranges"], 2);
    // New parts are appended after the committed ones.
    assert_eq!(result["plan"]["first_part"], 3);
    assert_eq!(
        result["resume"],
        json!({
            "resumed": true,
            "checkpoint": out.join("_checkpoint.json").display().to_string(),
            "remaining_ranges": 2,
        })
    );
    let files = result["output"]["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "{}", result);
    // A dry run removes nothing, not even a stale temp part.
    assert!(out.join(".part-00003.parquet.tmp").exists());
    assert!(out.join("_checkpoint.json").exists());
}

#[test]
fn check_dry_run_reports_the_setup_without_probing() {
    let dir = tempfile::tempdir().unwrap();
    let (code, stdout, stderr) = run_in(
        dir.path(),
        &[
            "--endpoint-url",
            ENDPOINT,
            "check",
            "s3://b/p/",
            "--dry-run",
        ],
        &[],
    );
    assert_eq!(code, 0, "{}", stderr);
    let result = one_result(&stdout);
    // check reports its findings (a dry run only skips the probe); it has
    // no plan.
    assert_eq!(result["command"], "check");
    assert_eq!(result["ok"], true);
    assert_eq!(result["plan"], Value::Null);
    let check = &result["check"];
    assert_eq!(check["credentials"]["found"], true);
    assert_eq!(check["region"], "us-east-1");
    assert_eq!(check["endpoint_url"], ENDPOINT);
    assert_eq!(check["path_style"], true);
    assert_eq!(check["request_url"], format!("{}/b", ENDPOINT));
    assert_eq!(check["proxy"], Value::Null);
    assert_eq!(check["probe"], Value::Null);
}

// ── Redaction ───────────────────────────────────────────────

#[test]
fn endpoint_userinfo_is_redacted_in_results() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = "http://user:s3cr3tpw@127.0.0.1:9";
    let redacted = "http://<redacted>@127.0.0.1:9";
    for args in [
        &["--endpoint-url", endpoint, "list", "s3://b", "--dry-run"][..],
        &[
            "--endpoint-url",
            endpoint,
            "diff",
            "s3://a",
            "s3://b",
            "--dry-run",
        ],
        &[
            "diff",
            "s3://a",
            "s3://b",
            "--endpoint-url",
            ENDPOINT,
            "--target-endpoint-url",
            endpoint,
            "--dry-run",
        ],
        &["--endpoint-url", endpoint, "check", "s3://b", "--dry-run"],
    ] {
        let (code, stdout, stderr) = run_in(dir.path(), args, &[]);
        assert_eq!(code, 0, "{:?}: {}", args, stderr);
        assert!(!stdout.contains("s3cr3tpw"), "{:?}: {}", args, stdout);
        assert!(!stderr.contains("s3cr3tpw"), "{:?}: {}", args, stderr);
        let result = one_result(&stdout);
        let shown: Vec<&Value> = [
            &result["source"]["endpoint_url"],
            &result["target"]["endpoint_url"],
            &result["check"]["endpoint_url"],
        ]
        .into_iter()
        .filter(|v| v.as_str().is_some_and(|s| s.contains("127.0.0.1:9")))
        .collect();
        assert!(!shown.is_empty(), "{:?}: {}", args, result);
        assert!(
            shown.iter().any(|v| *v == redacted),
            "{:?}: {}",
            args,
            result
        );
    }
}

/// A run with `-o -`: stdout must be empty and the result is the last line
/// of stderr.
fn stderr_result(args: &[&str], cwd: &Path) -> (i32, Value) {
    let (code, stdout, stderr) = run_in(cwd, args, &[]);
    assert!(stdout.is_empty(), "{:?}: stdout {:?}", args, stdout);
    let last = stderr.lines().last().unwrap_or_default();
    let result: Value = serde_json::from_str(last).unwrap_or_else(|e| {
        panic!(
            "{:?}: last stderr line is not JSON ({}): {}",
            args, e, stderr
        )
    });
    assert_eq!(result["schema"], "s3tl.result/1");
    for key in RESULT_KEYS {
        assert!(result.get(key).is_some(), "missing key {}: {}", key, result);
    }
    (code, result)
}

#[test]
fn stdout_stays_a_data_channel_with_dash_output_even_on_errors() {
    let dir = tempfile::tempdir().unwrap();
    for args in [
        &["list", "not-a-url", "-o", "-"][..],
        &[
            "list",
            "s3://b/",
            "-o",
            "-",
            "--endpoint-url",
            ENDPOINT,
            "--dry-run",
        ],
    ] {
        let (code, stdout, stderr) = run_in(dir.path(), args, &[]);
        assert!(stdout.is_empty(), "{:?}: stdout {:?}", args, stdout);
        let last = stderr.lines().last().unwrap_or_default();
        let result: Value = serde_json::from_str(last).unwrap_or_else(|e| {
            panic!(
                "{:?}: last stderr line is not JSON ({}): {}",
                args, e, stderr
            )
        });
        assert_eq!(result["schema"], "s3tl.result/1");
        assert_eq!(result["exit_code"], code, "{}", result);
    }
}

#[test]
fn diff_compare_mode_is_planned_and_validated() {
    let dir = tempfile::tempdir().unwrap();
    let base = [
        "diff",
        "s3://a/",
        "s3://b/",
        "--endpoint-url",
        ENDPOINT,
        "--dry-run",
    ];
    let result = plan(&base, dir.path(), &[]);
    assert_eq!(result["plan"]["compare"], "etag");
    for mode in ["size", "checksum"] {
        let result = plan(&[&base[..], &["--compare", mode]].concat(), dir.path(), &[]);
        assert_eq!(result["plan"]["compare"], mode);
    }
    usage_error(
        &[&base[..], &["--compare", "md5"]].concat(),
        dir.path(),
        "usage.invalid_argument",
    );
    usage_error(
        &["list", "s3://a/", "--compare", "size"],
        dir.path(),
        "usage.invalid_argument",
    );
}
