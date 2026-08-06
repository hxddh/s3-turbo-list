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

// ── Boundary-set verification ─────────────────────────────
//
// Completed segment indices are positional: carrying a checkpoint over to a
// different boundary set marks ranges complete that were never listed.  The
// segment count alone does not catch it — flat-namespace bisection always
// produces exactly `target` boundaries, so two runs of a bucket that took
// writes in between agree on the count and disagree on every boundary.

fn journal_for(boundaries: &[String], completed: Vec<usize>) -> CheckpointJournal {
    let identity =
        make_identity(Some(""), None, None, Some("path"), Some("list")).with_boundaries(boundaries);
    CheckpointJournal {
        bucket: "test-bucket".into(),
        prefix: "".into(),
        total_segments: boundaries.len() + 1,
        completed_indices: completed,
        last_updated: String::new(),
        identity: Some(identity),
    }
}

fn boundaries(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

#[test]
fn test_same_boundaries_resume_accepted() {
    let current = boundaries(&["obj-0050", "obj-0100", "obj-0150"]);
    let journal = journal_for(&current, vec![0, 2]);
    assert!(journal.verify_segments(&current, current.len() + 1));
}

#[test]
fn test_same_count_different_boundaries_rejected() {
    let recorded = boundaries(&["obj-0050", "obj-0100", "obj-0150"]);
    let journal = journal_for(&recorded, vec![0, 2]);
    // Same segment count, every boundary shifted — the old count-only guard
    // accepted this and silently skipped whatever indices 0 and 2 now cover.
    let current = boundaries(&["obj-0060", "obj-0120", "obj-0180"]);
    assert_eq!(recorded.len(), current.len());
    assert!(!journal.verify_segments(&current, current.len() + 1));
}

#[test]
fn test_segment_count_mismatch_still_rejected() {
    let recorded = boundaries(&["a/", "b/"]);
    let journal = journal_for(&recorded, vec![0]);
    let current = boundaries(&["a/", "b/", "c/"]);
    assert!(!journal.verify_segments(&current, current.len() + 1));
}

#[test]
fn test_checkpoint_without_boundary_digest_rejected() {
    // Written by a version that recorded no fingerprint: the boundary set it
    // resumed against cannot be verified, so it is discarded.
    let current = boundaries(&["a/", "b/"]);
    let identity = make_identity(Some(""), None, None, Some("path"), Some("list"));
    let journal = CheckpointJournal {
        bucket: "test-bucket".into(),
        prefix: "".into(),
        total_segments: current.len() + 1,
        completed_indices: vec![0],
        last_updated: String::new(),
        identity: Some(identity),
    };
    assert!(!journal.verify_segments(&current, current.len() + 1));
}

#[test]
fn test_boundaries_digest_is_order_and_separator_sensitive() {
    // "ab" + "c" must not collide with "a" + "bc".
    assert_ne!(
        checkpoint::boundaries_digest(&boundaries(&["ab", "c"])),
        checkpoint::boundaries_digest(&boundaries(&["a", "bc"]))
    );
    assert_ne!(
        checkpoint::boundaries_digest(&boundaries(&["a", "b"])),
        checkpoint::boundaries_digest(&boundaries(&["b", "a"]))
    );
    assert_eq!(
        checkpoint::boundaries_digest(&boundaries(&["a", "b"])),
        checkpoint::boundaries_digest(&boundaries(&["a", "b"]))
    );
}
