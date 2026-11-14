//! Resume state of an interrupted `list -o DIR` run: `DIR/_checkpoint.json`.
//!
//! The checkpoint lives in the dataset directory it describes, so it can
//! only ever be resumed into the same output. It records exactly the key
//! ranges whose rows were not yet committed, and the identity of the run
//! (source, endpoint, delimiter, page size): a resume under a different
//! identity is refused rather than splicing two listings together.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// File name of the checkpoint inside the output directory.
pub const CHECKPOINT_FILE: &str = "_checkpoint.json";

/// Fields that must match between a checkpoint and the resuming run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointIdentity {
    pub bucket: String,
    pub prefix: String,
    /// The endpoint the listing ran against (`None`: AWS S3).
    pub endpoint_url: Option<String>,
    pub delimiter: String,
    pub max_keys: Option<i32>,
}

impl CheckpointIdentity {
    /// Names of the fields that differ (empty: same run).
    pub fn diff(&self, current: &CheckpointIdentity) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.bucket != current.bucket {
            out.push("bucket");
        }
        if self.prefix != current.prefix {
            out.push("prefix");
        }
        if self.endpoint_url != current.endpoint_url {
            out.push("endpoint_url");
        }
        if self.delimiter != current.delimiter {
            out.push("delimiter");
        }
        if self.max_keys != current.max_keys {
            out.push("max_keys");
        }
        out
    }
}

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

/// The checkpoint document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub schema: String,
    pub identity: CheckpointIdentity,
    /// Exactly the key ranges not yet written.
    pub remaining: Vec<ResumeRange>,
    pub updated_at: String,
}

pub const CHECKPOINT_SCHEMA: &str = "s3tl.checkpoint/1";

/// Why a checkpoint cannot be resumed.
#[derive(Debug, PartialEq, Eq)]
pub enum CheckpointError {
    Missing,
    Unreadable(String),
    Mismatch(Vec<&'static str>),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "no checkpoint in the output directory"),
            Self::Unreadable(e) => write!(f, "checkpoint is unreadable: {}", e),
            Self::Mismatch(fields) => write!(
                f,
                "checkpoint belongs to a different run (differs in: {})",
                fields.join(", ")
            ),
        }
    }
}

impl Checkpoint {
    pub fn new(identity: CheckpointIdentity, remaining: Vec<ResumeRange>) -> Self {
        Self {
            schema: CHECKPOINT_SCHEMA.to_string(),
            identity,
            remaining,
            updated_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn path(dir: &Path) -> std::path::PathBuf {
        dir.join(CHECKPOINT_FILE)
    }

    /// Load the checkpoint of `dir` and check it belongs to `identity`.
    pub fn load(dir: &Path, identity: &CheckpointIdentity) -> Result<Self, CheckpointError> {
        let path = Self::path(dir);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(CheckpointError::Missing);
            }
            Err(e) => return Err(CheckpointError::Unreadable(e.to_string())),
        };
        let cp: Checkpoint = serde_json::from_str(&content)
            .map_err(|e| CheckpointError::Unreadable(e.to_string()))?;
        if cp.schema != CHECKPOINT_SCHEMA {
            return Err(CheckpointError::Unreadable(format!(
                "unsupported schema '{}'",
                cp.schema
            )));
        }
        let mismatch = cp.identity.diff(identity);
        if !mismatch.is_empty() {
            return Err(CheckpointError::Mismatch(mismatch));
        }
        Ok(cp)
    }

    /// Write atomically and durably: temp file, fsync, rename, fsync of the
    /// directory. A crash leaves the previous checkpoint (or none) intact.
    pub fn save(&self, dir: &Path) -> Result<(), String> {
        let path = Self::path(dir);
        let tmp = dir.join(format!("{}.tmp", CHECKPOINT_FILE));
        let json = serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?;
        let result = (|| -> std::io::Result<()> {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&json)?;
            f.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            crate::utils::sync_dir(dir)
        })();
        result.map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            format!("could not write checkpoint {}: {}", path.display(), e)
        })
    }

    /// Remove the checkpoint of `dir` (a finished run). Missing is fine.
    pub fn remove(dir: &Path) -> Result<(), String> {
        match std::fs::remove_file(Self::path(dir)) {
            Ok(()) => crate::utils::sync_dir(dir).map_err(|e| e.to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> CheckpointIdentity {
        CheckpointIdentity {
            bucket: "b".into(),
            prefix: "p/".into(),
            endpoint_url: Some("http://localhost:9000".into()),
            delimiter: String::new(),
            max_keys: None,
        }
    }

    fn range(start: &str, end: Option<&str>) -> ResumeRange {
        ResumeRange {
            start_after: start.into(),
            end: end.map(str::to_string),
        }
    }

    #[test]
    fn save_then_load_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let cp = Checkpoint::new(identity(), vec![range("", Some("m")), range("q", None)]);
        cp.save(dir.path()).unwrap();
        assert!(!dir.path().join("_checkpoint.json.tmp").exists());
        let loaded = Checkpoint::load(dir.path(), &identity()).unwrap();
        assert_eq!(loaded, cp);
    }

    #[test]
    fn load_reports_missing_and_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            Checkpoint::load(dir.path(), &identity()),
            Err(CheckpointError::Missing)
        );
        Checkpoint::new(identity(), vec![range("", None)])
            .save(dir.path())
            .unwrap();
        let mut other = identity();
        other.endpoint_url = None;
        other.max_keys = Some(2);
        assert_eq!(
            Checkpoint::load(dir.path(), &other),
            Err(CheckpointError::Mismatch(vec!["endpoint_url", "max_keys"]))
        );
    }

    #[test]
    fn load_rejects_garbage_and_foreign_schema() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(Checkpoint::path(dir.path()), "not json").unwrap();
        assert!(matches!(
            Checkpoint::load(dir.path(), &identity()),
            Err(CheckpointError::Unreadable(_))
        ));
        let mut cp = Checkpoint::new(identity(), vec![]);
        cp.schema = "other/9".into();
        std::fs::write(
            Checkpoint::path(dir.path()),
            serde_json::to_string(&cp).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            Checkpoint::load(dir.path(), &identity()),
            Err(CheckpointError::Unreadable(_))
        ));
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        Checkpoint::remove(dir.path()).unwrap();
        Checkpoint::new(identity(), vec![])
            .save(dir.path())
            .unwrap();
        Checkpoint::remove(dir.path()).unwrap();
        assert!(!Checkpoint::path(dir.path()).exists());
    }
}
