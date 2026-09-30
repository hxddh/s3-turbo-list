//! The agent contract: the JSON documents (plan, run manifest, doctor
//! report) and exit codes an automated caller relies on.
//!
//! - `schema`: the serialized report and manifest types;
//! - `redact`: command-line and URL redaction;
//! - `plan`: the dry-run plan's hints, checkpoint and output checks;
//! - `artifacts`: the manifest's artifact summaries;
//! - `doctor`: the doctor report and request routing.

use crate::checkpoint::{CheckpointIdentity, CheckpointJournal};
use crate::config::{ConfigLoadSummary, S3TurboConfig};
use crate::core::RunMetricsSnapshot;
use crate::hints;
use crate::profiles;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};

mod artifacts;
mod doctor;
mod plan;
mod redact;
mod schema;

pub use artifacts::*;
pub use doctor::*;
pub use plan::*;
pub use redact::*;
pub use schema::*;

pub const AGENT_SCHEMA_VERSION: &str = "s3-turbo-list.agent.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    Success = 0,
    InternalError = 1,
    CliConfig = 2,
    ProviderSetup = 3,
    NetworkRetryExhausted = 4,
    OutputWrite = 5,
    DataValidation = 6,
    Interrupted = 7,
}

impl ExitCode {
    pub fn code(self) -> i32 {
        self as i32
    }
}

pub fn write_json_file<T: Serialize>(path: &str, value: &T) -> Result<(), String> {
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "failed to create parent directory {}: {}",
                parent.display(),
                e
            )
        })?;
    }
    let json = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    std::fs::write(path, json).map_err(|e| format!("failed to write {}: {}", path, e))
}

pub fn to_pretty_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string())
}
