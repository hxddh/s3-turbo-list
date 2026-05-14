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
    )
}

fn make_journal(identity: CheckpointIdentity, completed: Vec<usize>) -> CheckpointJournal {
    CheckpointJournal {
        bucket: "test-bucket".into(),
        prefix: "".into(),
        total_segments: 4,
        completed_indices: completed,
        last_updated: String::new(),
        identity: Some(identity),
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
    let journal = make_journal(id.clone(), vec![0, 2]);
    journal.save(path_str);

    let loaded = CheckpointJournal::load_and_verify(path_str, &id);
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().completed_indices, vec![0, 2]);
}

// ── Identity mismatch — each field separately ─────────────

#[test]
fn test_identity_delimiter_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(Some("/"), None, None, None, None);
    let journal = make_journal(stored, vec![0]);
    journal.save(path_str);

    let current = make_identity(Some("#"), None, None, None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_max_keys_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, Some(100), None, None, None);
    let journal = make_journal(stored, vec![0]);
    journal.save(path_str);

    let current = make_identity(None, Some(500), None, None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_profile_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, None, Some("bos"), None, None);
    let journal = make_journal(stored, vec![0]);
    journal.save(path_str);

    let current = make_identity(None, None, Some("minio"), None, None);
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_mode_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, None, None, None, Some("list"));
    let journal = make_journal(stored, vec![0]);
    journal.save(path_str);

    let current = make_identity(None, None, None, None, Some("bidir"));
    assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
}

#[test]
fn test_identity_addressing_style_mismatch_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ckpt.toml");
    let path_str = path.to_str().unwrap();

    let stored = make_identity(None, None, None, Some("path"), None);
    let journal = make_journal(stored, vec![0]);
    journal.save(path_str);

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
        total_segments: 4,
        completed_indices: vec![0, 2],
        last_updated: String::new(),
        identity: None,
    };
    journal.save(path_str);

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
