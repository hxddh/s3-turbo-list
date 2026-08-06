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
    /// The `--filter` expression, verbatim, or `None` when the run had no
    /// filter.  It belongs here for the same reason `prefix` and `max_keys`
    /// do: it decides which objects reach the output, so resuming under a
    /// different one splices two populations into a single file that reads
    /// as one coherent listing.
    ///
    /// A checkpoint written before this field existed also deserializes to
    /// `None`, which is indistinguishable from "no filter". Resuming such a
    /// checkpoint under a filter is caught (`None` vs `Some`); resuming a
    /// filtered pre-upgrade checkpoint without one is not. That residual gap
    /// closes as soon as a checkpoint is written by this version or later.
    #[serde(default)]
    pub filter: Option<String>,
    /// Fingerprint of the key-space boundary set the checkpoint's segment
    /// indices refer to.  Absent in checkpoints written before boundary
    /// verification existed.
    #[serde(default)]
    pub boundaries_digest: Option<String>,
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
        filter: Option<&str>,
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
            filter: filter.map(|f| f.to_string()),
            boundaries_digest: None,
        }
    }

    /// Attach the fingerprint of the boundary set this run is partitioned by.
    /// Resolved after the identity is built, because hints resolution needs
    /// the identity-verified checkpoint first.
    pub fn with_boundaries(mut self, boundaries: &[String]) -> Self {
        self.boundaries_digest = Some(boundaries_digest(boundaries));
        self
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
        if self.filter != current.filter {
            mismatches.push("filter".into());
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

    /// Verify that this checkpoint's completed segment indices still describe
    /// the boundary set the current run resolved.  Completed indices are
    /// positional, so a checkpoint carried over to a *different* boundary set
    /// would mark ranges complete that were never listed — silently dropping
    /// keys from the output.  The segment count alone does not catch this:
    /// flat-namespace bisection always produces exactly `target` boundaries,
    /// so two runs of a bucket that took writes in between agree on the count
    /// while disagreeing on every boundary.
    pub fn verify_segments(&self, boundaries: &[String], total_segments: usize) -> bool {
        if self.total_segments != total_segments {
            warn!(
                "Checkpoint segment count {} does not match current hints ({}) — \
                 discarding checkpoint and starting fresh",
                self.total_segments, total_segments
            );
            return false;
        }

        let current = boundaries_digest(boundaries);
        match self.identity.as_ref().and_then(|id| {
            id.boundaries_digest
                .as_deref()
                .filter(|digest| !digest.is_empty())
        }) {
            Some(stored) if stored == current => true,
            Some(_) => {
                warn!(
                    "Checkpoint key-space boundaries differ from this run's ({} segments either \
                     way) — resuming would mark ranges complete that were never listed; \
                     discarding checkpoint and starting fresh",
                    total_segments
                );
                false
            }
            None => {
                warn!(
                    "Checkpoint has no key-space boundary fingerprint (written by an older \
                     version) — discarding checkpoint and starting fresh"
                );
                false
            }
        }
    }

    /// Write the current checkpoint state.
    pub fn save(&self, path: &str) {
        let toml_str = toml::to_string_pretty(self).expect("Failed to serialize checkpoint");
        if let Err(e) = std::fs::write(path, &toml_str) {
            log::warn!("Failed to write checkpoint {}: {}", path, e);
        }
    }
}

/// Fingerprint of a key-space boundary set.  Boundaries are separated by a
/// NUL byte so no concatenation of different boundaries can collide.
pub fn boundaries_digest(boundaries: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for boundary in boundaries {
        hasher.update(boundary.as_bytes());
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
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
            None,
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

    fn make_identity_with_filter(filter: Option<&str>) -> CheckpointIdentity {
        CheckpointIdentity::new(
            "test-bucket",
            Some("us-east-1"),
            "",
            Some("/"),
            None,
            None,
            Some("path"),
            Some("list"),
            filter,
        )
    }

    #[test]
    fn test_identity_changed_filter_detected() {
        // Resuming under a different filter would splice rows written under
        // the old expression together with rows written under the new one.
        let stored = make_identity_with_filter(Some("SOURCE.size > 1000"));
        let current = make_identity_with_filter(Some("SOURCE.size > 2000"));
        assert!(stored.diff(&current).contains(&"filter".to_string()));
    }

    #[test]
    fn test_identity_filter_added_or_dropped_detected() {
        let none = make_identity_with_filter(None);
        let some = make_identity_with_filter(Some("SOURCE.size > 1000"));
        assert!(none.diff(&some).contains(&"filter".to_string()));
        assert!(some.diff(&none).contains(&"filter".to_string()));
    }

    #[test]
    fn test_identity_same_filter_allows_resume() {
        let stored = make_identity_with_filter(Some("SOURCE.size > 1000"));
        let current = make_identity_with_filter(Some("SOURCE.size > 1000"));
        assert!(stored.diff(&current).is_empty());
    }

    #[test]
    fn test_load_and_verify_changed_filter_discards_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.toml");
        let path_str = path.to_str().unwrap();

        let journal = make_journal(make_identity_with_filter(Some("SOURCE.size > 1000")));
        journal.save(path_str);

        let current = make_identity_with_filter(Some("SOURCE.size > 2000"));
        assert!(CheckpointJournal::load_and_verify(path_str, &current).is_none());
    }

    #[test]
    fn test_identity_without_filter_field_deserializes_to_none() {
        // A checkpoint written before the field existed must still load —
        // it just carries no filter, and is caught the moment the current
        // run has one.
        let toml_src = r#"
bucket = "test-bucket"
prefix = ""
delimiter = "/"
addressing_style = "path"
mode = "list"
"#;
        let id: CheckpointIdentity = toml::from_str(toml_src).unwrap();
        assert_eq!(id.filter, None);
        assert!(id
            .diff(&make_identity_with_filter(Some("SOURCE.size > 1000")))
            .contains(&"filter".to_string()));
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
        let legacy =
            CheckpointIdentity::new("bucket", None, "", None, None, None, None, None, None);
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
