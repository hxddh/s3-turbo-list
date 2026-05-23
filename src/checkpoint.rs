use log::{info, warn};
use serde::{Deserialize, Serialize};

// ── Checkpoint identity ───────────────────────────────────

/// Immutable identity fields that must match between a checkpoint
/// and the current run for resume to be valid.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointIdentity {
    pub bucket: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default)]
    pub max_keys: Option<i32>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub addressing_style: Option<String>,
    #[serde(default)]
    pub mode: Option<String>, // "list" or "bidir"
}

impl CheckpointIdentity {
    /// Build the identity for the current run.
    pub fn new(
        bucket: &str,
        region: Option<&str>,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: Option<i32>,
        profile: Option<&str>,
        addressing_style: Option<&str>,
        mode: Option<&str>,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            region: region.map(|r| r.to_string()),
            prefix: prefix.to_string(),
            delimiter: delimiter.map(|d| d.to_string()),
            max_keys,
            profile: profile.map(|p| p.to_string()),
            addressing_style: addressing_style.map(|a| a.to_string()),
            mode: mode.map(|m| m.to_string()),
        }
    }

    /// Compare the checkpoint identity against the current run's identity.
    /// Returns a list of field names that differ (empty means match).
    pub fn diff(&self, current: &CheckpointIdentity) -> Vec<String> {
        let mut mismatches: Vec<String> = Vec::new();

        if self.bucket != current.bucket {
            mismatches.push("bucket".into());
        }
        if self.region != current.region {
            mismatches.push("region".into());
        }
        if self.prefix != current.prefix {
            mismatches.push("prefix".into());
        }
        if self.delimiter != current.delimiter {
            mismatches.push("delimiter".into());
        }
        if self.max_keys != current.max_keys {
            mismatches.push("max_keys".into());
        }
        if self.profile != current.profile {
            mismatches.push("profile".into());
        }
        if self.addressing_style != current.addressing_style {
            mismatches.push("addressing_style".into());
        }
        if self.mode != current.mode {
            mismatches.push("mode".into());
        }

        mismatches
    }

    /// Returns `true` if the identity was written by an older version
    /// that didn't include identity fields (all optional fields are `None`).
    pub fn is_legacy(&self) -> bool {
        // A legacy checkpoint would have only bucket populated (the old
        // struct had bucket, prefix, total_segments, completed_indices,
        // last_updated).  If delimiter/max_keys/profile/addressing_style/mode
        // are all None, treat it as legacy — we can't verify identity.
        self.delimiter.is_none()
            && self.max_keys.is_none()
            && self.profile.is_none()
            && self.addressing_style.is_none()
            && self.mode.is_none()
    }
}

// ── CheckpointJournal ─────────────────────────────────────

/// Lightweight journal tracking which KeySpace segments are complete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointJournal {
    pub bucket: String,
    pub prefix: String,
    pub total_segments: usize,
    pub completed_indices: Vec<usize>,
    pub last_updated: String,
    /// Run identity — must match current run for resume to be valid.
    /// Absent in legacy checkpoints (pre-identity-hardening).
    #[serde(default)]
    pub identity: Option<CheckpointIdentity>,
}

impl CheckpointJournal {
    /// Load a checkpoint file if it exists (raw load — no identity check).
    pub fn load(path: &str) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        toml::from_str(&content).ok()
    }

    /// Load a checkpoint file AND verify that the run identity matches.
    ///
    /// Returns `None` when:
    /// - the file does not exist or is unparseable
    /// - the checkpoint is from an older version without identity fields
    /// - any identity field differs from `current_identity`
    ///
    /// In all mismatch cases a clear warning is logged so the operator
    /// knows the checkpoint was discarded and why.
    pub fn load_and_verify(path: &str, current_identity: &CheckpointIdentity) -> Option<Self> {
        let journal = Self::load(path)?;

        let stored = match &journal.identity {
            Some(id) => id,
            None => {
                warn!(
                    "Checkpoint {} has no identity block (pre-hardening format) — \
                     discarding checkpoint and starting fresh",
                    path
                );
                return None;
            }
        };

        if stored.is_legacy() {
            warn!(
                "Checkpoint {} was written by an older version without identity \
                 fields — discarding checkpoint and starting fresh",
                path
            );
            return None;
        }

        let mismatches = stored.diff(current_identity);
        if !mismatches.is_empty() {
            warn!(
                "Checkpoint {} identity mismatch on field(s): {} — \
                 discarding checkpoint and starting fresh",
                path,
                mismatches.join(", ")
            );
            return None;
        }

        info!(
            "Checkpoint {} identity verified — resuming with {} of {} segments completed",
            path,
            journal.completed_indices.len(),
            journal.total_segments
        );
        Some(journal)
    }

    /// Write the current checkpoint state.
    pub fn save(&self, path: &str) {
        let toml_str = toml::to_string_pretty(self).expect("Failed to serialize checkpoint");
        if let Err(e) = std::fs::write(path, &toml_str) {
            log::warn!("Failed to write checkpoint {}: {}", path, e);
        }
    }

    /// Create a new checkpoint journal for a run.
    #[allow(dead_code)] // Phase 5: used when creating fresh checkpoints (non-resume runs)
    pub fn new(
        bucket: &str,
        prefix: &str,
        total_segments: usize,
        identity: CheckpointIdentity,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            total_segments,
            completed_indices: Vec::new(),
            last_updated: chrono::Local::now().to_rfc3339(),
            identity: Some(identity),
        }
    }
}

/// Generate the checkpoint file path for a given bucket.
pub fn checkpoint_path(bucket: &str, region: Option<&str>) -> String {
    let bucket = crate::agent::sanitize_path_component(bucket);
    if let Some(r) = region {
        format!(
            "{}_{}_checkpoint.toml",
            crate::agent::sanitize_path_component(r),
            bucket
        )
    } else {
        format!("{}_checkpoint.toml", bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Identity tests ────────────────────────────────────

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

    fn make_journal(identity: CheckpointIdentity) -> CheckpointJournal {
        CheckpointJournal {
            bucket: "test-bucket".into(),
            prefix: "".into(),
            total_segments: 4,
            completed_indices: vec![0, 2],
            last_updated: String::new(),
            identity: Some(identity),
        }
    }

    #[test]
    fn test_identity_exact_match_allows_resume() {
        let id = make_identity(
            Some("/"),
            Some(1000),
            Some("bos"),
            Some("path"),
            Some("list"),
        );
        let current = id.clone();
        assert!(id.diff(&current).is_empty());
    }

    #[test]
    fn test_identity_changed_delimiter_detected() {
        let stored = make_identity(Some("/"), None, None, None, None);
        let current = make_identity(Some("#"), None, None, None, None);
        let mismatches = stored.diff(&current);
        assert!(mismatches.contains(&"delimiter".to_string()));
    }

    #[test]
    fn test_identity_changed_max_keys_detected() {
        let stored = make_identity(None, Some(100), None, None, None);
        let current = make_identity(None, Some(500), None, None, None);
        let mismatches = stored.diff(&current);
        assert!(mismatches.contains(&"max_keys".to_string()));
    }

    #[test]
    fn test_identity_changed_profile_detected() {
        let stored = make_identity(None, None, Some("bos"), None, None);
        let current = make_identity(None, None, Some("minio"), None, None);
        let mismatches = stored.diff(&current);
        assert!(mismatches.contains(&"profile".to_string()));
    }

    #[test]
    fn test_identity_changed_addressing_style_detected() {
        let stored = make_identity(None, None, None, Some("path"), None);
        let current = make_identity(None, None, None, Some("virtual"), None);
        let mismatches = stored.diff(&current);
        assert!(mismatches.contains(&"addressing_style".to_string()));
    }

    #[test]
    fn test_identity_changed_mode_detected() {
        let stored = make_identity(None, None, None, None, Some("list"));
        let current = make_identity(None, None, None, None, Some("bidir"));
        let mismatches = stored.diff(&current);
        assert!(mismatches.contains(&"mode".to_string()));
    }

    #[test]
    fn test_identity_legacy_detection() {
        let legacy = CheckpointIdentity::new("bucket", None, "", None, None, None, None, None);
        assert!(legacy.is_legacy());

        let modern = make_identity(Some("/"), None, None, None, None);
        assert!(!modern.is_legacy());
    }

    #[test]
    fn test_identity_mismatch_many_fields() {
        let stored = make_identity(
            Some("/"),
            Some(100),
            Some("bos"),
            Some("path"),
            Some("list"),
        );
        let current = make_identity(
            Some("#"),
            Some(500),
            Some("minio"),
            Some("virtual"),
            Some("bidir"),
        );
        let mismatches = stored.diff(&current);
        assert_eq!(mismatches.len(), 5);
        assert!(mismatches.contains(&"delimiter".to_string()));
        assert!(mismatches.contains(&"max_keys".to_string()));
        assert!(mismatches.contains(&"profile".to_string()));
        assert!(mismatches.contains(&"addressing_style".to_string()));
        assert!(mismatches.contains(&"mode".to_string()));
    }

    #[test]
    fn test_load_and_verify_identity_match_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.toml");
        let path_str = path.to_str().unwrap();

        let id = make_identity(Some("/"), Some(1000), None, None, Some("list"));
        let journal = make_journal(id.clone());
        journal.save(path_str);

        let loaded = CheckpointJournal::load_and_verify(path_str, &id);
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().completed_indices, vec![0, 2]);
    }

    #[test]
    fn test_load_and_verify_identity_mismatch_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.toml");
        let path_str = path.to_str().unwrap();

        let stored_id = make_identity(Some("/"), None, None, None, None);
        let journal = make_journal(stored_id);
        journal.save(path_str);

        let current_id = make_identity(Some("#"), None, None, None, None);
        let loaded = CheckpointJournal::load_and_verify(path_str, &current_id);
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_and_verify_legacy_checkpoint_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.toml");
        let path_str = path.to_str().unwrap();

        // Write a checkpoint with no identity block (legacy format).
        let legacy_toml = r#"
bucket = "test-bucket"
prefix = ""
total_segments = 4
completed_indices = [0, 2]
last_updated = "2026-01-01T00:00:00Z"
"#;
        std::fs::write(path_str, legacy_toml).unwrap();

        let current_id = make_identity(Some("/"), None, None, None, None);
        let loaded = CheckpointJournal::load_and_verify(path_str, &current_id);
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_and_verify_identity_none_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.toml");
        let path_str = path.to_str().unwrap();

        // Write a checkpoint with explicit `identity = "__none__"` —
        // we simulate a format that has no identity field via TOML.
        // Actually: serde default for Option<CheckpointIdentity> is None.
        // So we just save a journal with identity=None (constructed manually).
        let journal = CheckpointJournal {
            bucket: "test-bucket".into(),
            prefix: "".into(),
            total_segments: 4,
            completed_indices: vec![0, 2],
            last_updated: String::new(),
            identity: None,
        };
        journal.save(path_str);

        let current_id = make_identity(Some("/"), None, None, None, None);
        let loaded = CheckpointJournal::load_and_verify(path_str, &current_id);
        assert!(loaded.is_none());
    }
}
