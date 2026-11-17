//! Engine settings. There is no config file: endpoint, region and
//! credentials come from the command line or the standard AWS SDK chain,
//! and everything else is a fixed default. A few test-only tunables are
//! read from `S3TL_*` environment variables (`EngineConfig::from_env`).

/// Request policy of every ListObjectsV2 call.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Attempts per segment page (the segment loop owns retries).
    pub max_attempts: u32,
    /// Seed for the backoff between retries of one segment.
    pub initial_backoff_secs: u64,
    pub connect_timeout_secs: u64,
    /// Deadline of one request (and the per-page watchdog).
    pub operation_timeout_secs: u64,
    /// Explicit endpoint; `None` means the SDK's resolution.
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
}

impl S3Config {
    pub fn force_path_style(&self) -> bool {
        self.force_path_style
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff_secs: 1,
            connect_timeout_secs: 60,
            operation_timeout_secs: 10,
            endpoint_url: None,
            force_path_style: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub worker_threads: usize,
    pub max_concurrency: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            // Match the machine: oversubscribing small hosts measurably
            // degrades throughput.
            worker_threads: std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(4),
            max_concurrency: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub row_group_size: usize,
    pub compression: String,
    pub compression_level: u32,
    /// Write the `s3tl.list-versions/1` schema (`list --versions`).
    pub versions: bool,
    /// Multipart upload part size of an S3 output.
    pub upload_part_bytes: usize,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            row_group_size: 100_000,
            compression: "zstd".to_string(),
            compression_level: 1,
            versions: false,
            upload_part_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelConfig {
    pub capacity: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self { capacity: 64 }
    }
}

/// Everything the engine is configured with.
#[derive(Debug, Clone, Default)]
pub struct EngineConfig {
    pub s3: S3Config,
    pub runtime: RuntimeConfig,
    pub output: OutputConfig,
    pub channel: ChannelConfig,
}

impl EngineConfig {
    /// Defaults plus the undocumented `S3TL_MAX_ATTEMPTS`,
    /// `S3TL_BACKOFF_SECS`, `S3TL_TIMEOUT_SECS` and `S3TL_UPLOAD_PART_BYTES`
    /// overrides the test suite
    /// uses to fail fast against a local mock. Unparsable values are
    /// ignored.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        let var = |name: &str| std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok());
        if let Some(v) = var("S3TL_MAX_ATTEMPTS") {
            cfg.s3.max_attempts = (v as u32).max(1);
        }
        if let Some(v) = var("S3TL_BACKOFF_SECS") {
            cfg.s3.initial_backoff_secs = v;
        }
        if let Some(v) = var("S3TL_UPLOAD_PART_BYTES") {
            cfg.output.upload_part_bytes = (v as usize).max(1);
        }
        if let Some(v) = var("S3TL_TIMEOUT_SECS") {
            cfg.s3.operation_timeout_secs = v.max(1);
            cfg.s3.connect_timeout_secs = v.max(1);
        }
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.s3.max_attempts, 10);
        assert_eq!(cfg.runtime.max_concurrency, 100);
        assert!(cfg.runtime.worker_threads >= 1);
        assert_eq!(cfg.output.compression, "zstd");
        assert!(!cfg.s3.force_path_style());
    }
}
