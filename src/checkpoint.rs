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
    /// The provider preset. Written as `profile` up to 0.37; those
    /// checkpoints still load and resume.
    #[serde(default, alias = "profile")]
    pub provider: Option<String>,
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
    /// The endpoint the listing ran against (`None`: the SDK's default AWS
    /// endpoint).  A bucket name is only unique per endpoint: resuming a
    /// checkpoint against another endpoint's same-named bucket would skip
    /// segments that were listed somewhere else.  Checkpoints written before
    /// this field existed read as `None` and are discarded once when resumed
    /// against a custom endpoint — a relist, never a silent skip.
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

impl CheckpointIdentity {
    /// Build the identity for the current run.
    pub fn new(
        bucket: &str,
        region: Option<&str>,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: Option<i32>,
        provider: Option<&str>,
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
            provider: provider.map(|p| p.to_string()),
            addressing_style: addressing_style.map(|a| a.to_string()),
            mode: mode.map(|m| m.to_string()),
            filter: filter.map(|f| f.to_string()),
            endpoint_url: None,
        }
    }

    /// Attach the endpoint this run lists against (see `endpoint_url`).
    pub fn with_endpoint(mut self, endpoint_url: Option<&str>) -> Self {
        self.endpoint_url = endpoint_url.map(str::to_string);
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
        // Case-insensitive: 0.37 stored the preset name as typed (`BOS`);
        // it is normalized to the canonical lowercase name now.
        let same_provider = match (&self.provider, &current.provider) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
            (a, b) => a == b,
        };
        if !same_provider {
            mismatches.push("provider".into());
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
        if self.endpoint_url != current.endpoint_url {
            mismatches.push("endpoint_url".into());
        }

        mismatches
    }
}

// ── CheckpointJournal ─────────────────────────────────────

/// A key range still to be listed: keys after `start_after` (exclusive, ""
/// for the start of the listing prefix) up to and including `end` (`None`:
/// the end of the listing prefix). The same bounds a segment task uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeRange {
    pub start_after: String,
    #[serde(default)]
    pub end: Option<String>,
}

/// What a list run's reactor knows at exit about the key space it did not
/// finish: published for the final checkpoint save.
#[derive(Debug, Clone, Default)]
pub struct ResumeProgress {
    /// Ranges not yet written (unstarted segments, pending split children,
    /// and in-flight segments cut at their last durably sent key).
    pub remaining: Vec<ResumeRange>,
    /// Ranges this run listed in whole or in part.
    pub ranges_with_progress: usize,
}

/// What a graceful interrupt of a list run leaves behind: the key ranges it
/// did not write, and the identity of the run they belong to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointJournal {
    pub bucket: String,
    pub prefix: String,
    pub last_updated: String,
    /// Run identity — must match the resuming run.
    #[serde(default)]
    pub identity: Option<CheckpointIdentity>,
    /// Exactly the key ranges not yet written, cut at each segment's last
    /// durably written key. A resume lists these ranges and nothing else.
    /// Absent in checkpoints written before 0.36 (which recorded completed
    /// segment indices instead): those are discarded and the run starts over.
    #[serde(default)]
    pub remaining: Option<Vec<ResumeRange>>,
    /// How many key ranges earlier runs listed in whole or in part, summed
    /// across a chain of resumes (reported as `resumed_segments_skipped`).
    #[serde(default)]
    pub listed_ranges: Option<usize>,
}

impl CheckpointJournal {
    /// Load a checkpoint file if it exists (raw load — no identity check).
    pub fn load(path: &str) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        match toml::from_str(&content) {
            Ok(journal) => Some(journal),
            Err(e) => {
                // Saves are atomic, so this is not a torn write of ours; say
                // so rather than silently relisting everything.
                warn!(
                    "Checkpoint {} could not be parsed ({}) — ignoring it and starting fresh",
                    path, e
                );
                None
            }
        }
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

        let Some(stored) = &journal.identity else {
            warn!(
                "Checkpoint {} has no identity block (written by an old version) — \
                 discarding checkpoint and starting fresh",
                path
            );
            return None;
        };

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

        let Some(remaining) = &journal.remaining else {
            warn!(
                "Checkpoint {} was written before 0.36 (segment indices, not key ranges) — \
                 discarding checkpoint and starting fresh",
                path
            );
            return None;
        };
        // 0.36 saved one of these when Ctrl-C arrived after the listing had
        // finished; resuming from it lists nothing and reports success.
        if remaining.is_empty() {
            warn!(
                "Checkpoint {} has no key ranges left (the listing it records had \
                 finished) — discarding checkpoint and starting fresh",
                path
            );
            return None;
        }
        info!(
            "Checkpoint {} identity verified — {} key range(s) left to list",
            path,
            remaining.len()
        );
        Some(journal)
    }

    /// Write the current checkpoint state.
    /// Written to a sibling temp file and renamed into place, so a crash or
    /// a full disk mid-write leaves the previous checkpoint intact instead of
    /// a truncated one.
    ///
    /// Returns the error so the caller can say so: a run that exits
    /// "interrupted — a checkpoint may allow resuming" while none was written
    /// sends the next `--resume` back to the start without a word.
    pub fn save(&self, path: &str) -> Result<(), String> {
        let toml_str = toml::to_string_pretty(self).expect("Failed to serialize checkpoint");
        let tmp = format!("{}.tmp", path);
        let result = std::fs::write(&tmp, &toml_str).and_then(|()| std::fs::rename(&tmp, path));
        if let Err(e) = result {
            let _ = std::fs::remove_file(&tmp);
            log::warn!("Failed to write checkpoint {}: {}", path, e);
            return Err(format!("could not write checkpoint {}: {}", path, e));
        }
        Ok(())
    }

    /// Whether the file at `path` may be replaced or removed by a run with
    /// `current` identity: it is absent, unreadable, or belongs to this run.
    /// A checkpoint of another job that shares the file name (same bucket,
    /// region and prefix but another endpoint, filter or page size) is left
    /// alone — finishing or interrupting this run used to delete or overwrite
    /// that job's resume point.
    pub fn may_replace(path: &str, current: &CheckpointIdentity) -> bool {
        if !std::path::Path::new(path).exists() {
            return true;
        }
        match Self::load(path) {
            None => true,
            Some(journal) => journal
                .identity
                .as_ref()
                .is_none_or(|stored| stored.diff(current).is_empty()),
        }
    }
}

/// Checkpoint file name for a run over `prefix`.  A whole-bucket run keeps
/// the historical `checkpoint_path` name; a prefixed run gets its own file,
/// so two jobs on different prefixes of one bucket never overwrite — and on
/// completion delete — each other's.
pub fn checkpoint_path_for_prefix(bucket: &str, region: Option<&str>, prefix: &str) -> String {
    let base = checkpoint_path(bucket, region);
    if prefix.is_empty() {
        return base;
    }
    let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(prefix.as_bytes()));
    format!(
        "{}_{}_checkpoint.toml",
        base.trim_end_matches("_checkpoint.toml"),
        &digest[..8]
    )
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

    fn identity() -> CheckpointIdentity {
        CheckpointIdentity::new(
            "test-bucket",
            Some("us-east-1"),
            "",
            Some(""),
            None,
            None,
            Some("path"),
            Some("list"),
            None,
        )
    }

    fn journal(identity: CheckpointIdentity) -> CheckpointJournal {
        CheckpointJournal {
            bucket: "test-bucket".into(),
            prefix: "".into(),
            last_updated: String::new(),
            identity: Some(identity),
            remaining: Some(vec![ResumeRange {
                start_after: "k".into(),
                end: None,
            }]),
            listed_ranges: Some(1),
        }
    }

    #[test]
    fn test_may_replace_only_absent_or_matching_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.toml");
        let path_str = path.to_str().unwrap();
        let identity = identity();
        assert!(CheckpointJournal::may_replace(path_str, &identity));
        journal(identity.clone()).save(path_str).unwrap();
        assert!(CheckpointJournal::may_replace(path_str, &identity));
        // Same file name, another job: another endpoint or another filter.
        let other_endpoint = identity
            .clone()
            .with_endpoint(Some("http://127.0.0.1:9000"));
        assert!(!CheckpointJournal::may_replace(path_str, &other_endpoint));
        let mut other_filter = identity.clone();
        other_filter.filter = Some("SOURCE.size > 0".to_string());
        assert!(!CheckpointJournal::may_replace(path_str, &other_filter));
    }

    #[test]
    fn test_ranges_round_trip_and_identity_is_verified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.toml");
        let path_str = path.to_str().unwrap();
        journal(identity()).save(path_str).unwrap();
        let loaded = CheckpointJournal::load_and_verify(path_str, &identity()).unwrap();
        assert_eq!(loaded.remaining.unwrap()[0].start_after, "k");
        let mut other = identity();
        other.prefix = "logs/".into();
        assert!(CheckpointJournal::load_and_verify(path_str, &other).is_none());
    }

    #[test]
    fn test_checkpoint_written_by_037_with_profile_field_resumes() {
        // 0.37 wrote the provider preset as `profile`, as typed on the
        // command line; 0.38 names it `provider` and normalizes the case.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.toml");
        std::fs::write(
            &path,
            "bucket = \"test-bucket\"\nprefix = \"\"\nlast_updated = \"\"\n\
             listed_ranges = 1\n\n[identity]\nbucket = \"test-bucket\"\n\
             region = \"us-east-1\"\nprefix = \"\"\ndelimiter = \"\"\nprofile = \"BOS\"\n\
             addressing_style = \"path\"\nmode = \"list\"\n\n\
             [[remaining]]\nstart_after = \"k\"\n",
        )
        .unwrap();
        let mut current = identity();
        current.provider = Some("bos".to_string());
        let loaded = CheckpointJournal::load_and_verify(path.to_str().unwrap(), &current).unwrap();
        assert_eq!(
            loaded.identity.unwrap().provider.as_deref(),
            Some("BOS"),
            "the 0.37 `profile` field is read as the provider"
        );
        assert!(CheckpointJournal::may_replace(
            path.to_str().unwrap(),
            &current
        ));
        current.provider = Some("minio".to_string());
        assert!(CheckpointJournal::load_and_verify(path.to_str().unwrap(), &current).is_none());
        // Written back under the new name.
        let mut saved = identity();
        saved.provider = Some("bos".to_string());
        let text = toml::to_string(&saved).unwrap();
        assert!(text.contains("provider = \"bos\""), "{}", text);
        assert!(!text.contains("profile"), "{}", text);
    }

    #[test]
    fn test_pre_036_index_checkpoint_is_discarded() {
        // The old format recorded completed segment indices; it parses (the
        // extra fields are ignored) but has no ranges to resume from.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.toml");
        let mut old = toml::to_string(&identity()).unwrap();
        old = format!(
            "bucket = \"test-bucket\"\nprefix = \"\"\ntotal_segments = 4\n\
             completed_indices = [0, 2]\nlast_updated = \"\"\n\n[identity]\n{}",
            old
        );
        std::fs::write(&path, old).unwrap();
        assert!(CheckpointJournal::load_and_verify(path.to_str().unwrap(), &identity()).is_none());
    }

    #[test]
    fn test_checkpoint_with_no_ranges_left_is_discarded() {
        // Resuming from it would list nothing and report success.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.toml");
        let path_str = path.to_str().unwrap();
        let mut finished = journal(identity());
        finished.remaining = Some(Vec::new());
        finished.save(path_str).unwrap();
        assert!(CheckpointJournal::load_and_verify(path_str, &identity()).is_none());
    }
}
