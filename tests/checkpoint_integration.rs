// Integration tests for checkpoint identity verification and legacy format rejection.
use s3_turbo_list::checkpoint::{self, CheckpointIdentity, CheckpointJournal};

fn make_identity(
    delimiter: Option<&str>,
    max_keys: Option<i32>,
    profile: Option<&str>,
    addressing_style: Option<&str>,
    mode: Option<&str>,
) -> CheckpointIdentity {
    CheckpointIdentity::new(
        "test-bucket",
        Some("us-east-1"),
        "",
        delimiter,
        max_keys,
        profile,
        addressing_style,
        mode,
        None,
    )
}

fn make_journal(identity: CheckpointIdentity, remaining: &[&str]) -> CheckpointJournal {
    CheckpointJournal {
        bucket: "test-bucket".into(),
        prefix: "".into(),
        last_updated: String::new(),
        identity: Some(identity),
        remaining: Some(
            remaining
                .iter()
                .map(|start| checkpoint::ResumeRange {
                    start_after: start.to_string(),
                    end: None,
                })
                .collect(),
        ),
        listed_ranges: Some(2),
    }
}

// ── Identity exact match ──────────────────────────────────

#[test]
fn test_identity_exact_match_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let id = make_identity(
        Some("/"),
        Some(1000),
        Some("bos"),
        Some("path"),
        Some("list"),
    );
    let journal = make_journal(id.clone(), &["m"]);
    journal.save(path_str).unwrap();

    let loaded = CheckpointJournal::load_and_verify(path_str, &id);
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().remaining.unwrap()[0].start_after, "m");
}

// ── Identity mismatch — each field separately ─────────────

#[test]
fn test_identity_delimiter_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(Some("/"), None, None, None, None);
    let journal = make_journal(stored, &["m"]);
    journal.save(path_str).unwrap();

    let current = make_identity(Some("#"), None, None, None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_max_keys_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, Some(100), None, None, None);
    let journal = make_journal(stored, &["m"]);
    journal.save(path_str).unwrap();

    let current = make_identity(None, Some(500), None, None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_profile_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, None, Some("bos"), None, None);
    let journal = make_journal(stored, &["m"]);
    journal.save(path_str).unwrap();

    let current = make_identity(None, None, Some("minio"), None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_mode_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, None, None, None, Some("list"));
    let journal = make_journal(stored, &["m"]);
    journal.save(path_str).unwrap();

    let current = make_identity(None, None, None, None, Some("bidir"));
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_addressing_style_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, None, None, Some("path"), None);
    let journal = make_journal(stored, &["m"]);
    journal.save(path_str).unwrap();

    let current = make_identity(None, None, None, Some("virtual"), None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

// ── Legacy format rejection ────────────────────────────────

#[test]
fn test_legacy_checkpoint_no_identity_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    // Write a checkpoint with no `[identity]` section.
    let legacy_toml = r#"
bucket = "test-bucket"
prefix = ""
total_segments = 4
completed_indices = [0, 2]
last_updated = "2026-01-01T00:00:00Z"
"#;
    std::fs::write(path_str, legacy_toml).unwrap();

    let current = make_identity(Some("/"), None, None, None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_legacy_checkpoint_blank_identity_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    // Write a checkpoint with identity = None (serialized with serde default).
    let journal = CheckpointJournal {
        bucket: "test-bucket".into(),
        prefix: "".into(),
        last_updated: String::new(),
        identity: None,
        remaining: Some(Vec::new()),
        listed_ranges: None,
    };
    journal.save(path_str).unwrap();

    let current = make_identity(Some("/"), None, None, None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_checkpoint_path_format() {
    let path_with_region = checkpoint::checkpoint_path("my-bucket", Some("us-east-1"));
    assert_eq!(path_with_region, "us-east-1_my-bucket_checkpoint.toml");

    let path_without_region = checkpoint::checkpoint_path("my-bucket", None);
    assert_eq!(path_without_region, "my-bucket_checkpoint.toml");
}

#[test]
fn test_prefixed_runs_get_their_own_checkpoint_path() {
    let whole = checkpoint::checkpoint_path_for_prefix("b", Some("r"), "");
    assert_eq!(whole, checkpoint::checkpoint_path("b", Some("r")));
    let logs = checkpoint::checkpoint_path_for_prefix("b", Some("r"), "logs/");
    let img = checkpoint::checkpoint_path_for_prefix("b", Some("r"), "img/");
    assert_ne!(logs, whole);
    assert_ne!(logs, img);
    assert!(
        logs.starts_with("r_b_") && logs.ends_with("_checkpoint.toml"),
        "{}",
        logs
    );
}

#[test]
fn test_checkpoint_identity_includes_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cp.toml");
    let path_str = path.to_str().unwrap();
    let id = |endpoint: Option<&str>| {
        CheckpointIdentity::new(
            "b",
            Some("r"),
            "",
            Some(""),
            None,
            None,
            Some("auto"),
            Some("list"),
            None,
        )
        .with_endpoint(endpoint)
    };
    let journal = CheckpointJournal {
        bucket: "b".into(),
        prefix: String::new(),
        last_updated: "now".into(),
        identity: Some(id(Some("http://x:9000"))),
        remaining: Some(vec![checkpoint::ResumeRange {
            start_after: "k".into(),
            end: None,
        }]),
        listed_ranges: None,
    };
    journal.save(path_str).unwrap();
    // Saved atomically: no temp file left behind.
    assert!(!dir.path().join("cp.toml.tmp").exists());
    assert!(CheckpointJournal::load_and_verify(path_str, &id(Some("http://x:9000"))).is_some());
    // Same bucket name on another endpoint is another bucket.
    assert!(CheckpointJournal::load_and_verify(path_str, &id(Some("http://y:9000"))).is_none());
    assert!(CheckpointJournal::load_and_verify(path_str, &id(None)).is_none());
}
