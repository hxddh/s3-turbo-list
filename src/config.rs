use crate::core::{ObjectFilter, ObjectProps, RunMode, OBJECT_FILTER};
use crate::filter_expr::FilterExpr;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ── AddressingStyle ───────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AddressingStyle {
    Path,
    Virtual,
    Auto,
}

impl Default for AddressingStyle {
    fn default() -> Self {
        Self::Auto
    }
}

impl std::str::FromStr for AddressingStyle {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "path" => Ok(Self::Path),
            "virtual" => Ok(Self::Virtual),
            "auto" => Ok(Self::Auto),
            other => Err(format!(
                "invalid addressing style '{}'. Valid values: path, virtual, auto",
                other
            )),
        }
    }
}

impl std::fmt::Display for AddressingStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Path => write!(f, "path"),
            Self::Virtual => write!(f, "virtual"),
            Self::Auto => write!(f, "auto"),
        }
    }
}

// ── S3Config ──────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3Config {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_initial_backoff_secs")]
    pub initial_backoff_secs: u64,
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_operation_timeout_secs")]
    pub operation_timeout_secs: u64,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default)]
    pub force_path_style: bool,
    #[serde(default)]
    pub addressing_style: AddressingStyle,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub debug_s3: bool,
    #[serde(default)]
    pub trace_compat: Option<String>,
    /// CLI `--start-after` override — if set, listing begins after this key
    /// regardless of hint-segment boundaries.
    #[serde(default)]
    pub start_after: Option<String>,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_backoff_secs: default_initial_backoff_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
            operation_timeout_secs: default_operation_timeout_secs(),
            endpoint_url: None,
            force_path_style: false,
            addressing_style: AddressingStyle::default(),
            profile: None,
            debug_s3: false,
            trace_compat: None,
            start_after: None,
        }
    }
}

// ── RuntimeConfig ─────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: default_worker_threads(),
            max_concurrency: default_max_concurrency(),
        }
    }
}

// ── OutputConfig ──────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutputConfig {
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_compression_level")]
    pub compression_level: u32,
    #[serde(default)]
    pub log_file: Option<String>,
    #[serde(default)]
    pub ks_file: Option<String>,
    #[serde(default)]
    pub parquet_file: Option<String>,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            row_group_size: default_row_group_size(),
            compression: default_compression(),
            compression_level: default_compression_level(),
            log_file: None,
            ks_file: None,
            parquet_file: None,
        }
    }
}

// ── AutoHintsConfig ───────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AutoHintsConfig {
    #[serde(default = "default_sample_threshold")]
    pub sample_threshold: usize,
    #[serde(default = "default_max_prefix_depth")]
    pub max_prefix_depth: usize,
    #[serde(default = "default_max_prefix_entries")]
    pub max_prefix_entries: usize,
}

impl Default for AutoHintsConfig {
    fn default() -> Self {
        Self {
            sample_threshold: default_sample_threshold(),
            max_prefix_depth: default_max_prefix_depth(),
            max_prefix_entries: default_max_prefix_entries(),
        }
    }
}

// ── ChannelConfig ─────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChannelConfig {
    #[serde(default = "default_channel_capacity")]
    pub capacity: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            capacity: default_channel_capacity(),
        }
    }
}

// ── S3TurboConfig ─────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3TurboConfig {
    #[serde(default)]
    pub s3: S3Config,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub auto_hints: AutoHintsConfig,
    #[serde(default)]
    pub channel: ChannelConfig,
}

#[derive(Debug, Clone)]
pub struct ConfigLoadSummary {
    pub explicit_config: Option<String>,
    pub loaded_config: Option<String>,
    pub loaded_config_kind: String,
    pub searched: Vec<String>,
    pub warnings: Vec<String>,
}

impl Default for S3TurboConfig {
    fn default() -> Self {
        Self {
            s3: S3Config::default(),
            runtime: RuntimeConfig::default(),
            output: OutputConfig::default(),
            auto_hints: AutoHintsConfig::default(),
            channel: ChannelConfig::default(),
        }
    }
}

// ── Default value functions ────────────────────────────────

fn default_max_attempts() -> u32 {
    10
}
fn default_initial_backoff_secs() -> u64 {
    30
}
fn default_connect_timeout_secs() -> u64 {
    60
}
fn default_operation_timeout_secs() -> u64 {
    5
}
fn default_worker_threads() -> usize {
    10
}
fn default_max_concurrency() -> usize {
    100
}
fn default_row_group_size() -> usize {
    100000
}
fn default_compression() -> String {
    "zstd".to_string()
}
fn default_compression_level() -> u32 {
    1
}
fn default_sample_threshold() -> usize {
    10000
}
fn default_max_prefix_depth() -> usize {
    5
}
fn default_max_prefix_entries() -> usize {
    1_000_000
}
fn default_channel_capacity() -> usize {
    64
}

// ── Config loading ────────────────────────────────────────

impl S3TurboConfig {
    /// Load config from default locations, then merge CLI overrides.
    pub fn load(cli_config_path: Option<&str>) -> Result<Self, String> {
        Self::load_with_summary(cli_config_path).map(|(config, _summary)| config)
    }

    pub fn load_with_summary(
        cli_config_path: Option<&str>,
    ) -> Result<(Self, ConfigLoadSummary), String> {
        let mut config = Self::default();

        let search_paths: Vec<(PathBuf, &'static str)> = if let Some(p) = cli_config_path {
            vec![(PathBuf::from(p), "explicit")]
        } else {
            vec![
                (PathBuf::from("./s3-turbo-list.toml"), "workspace"),
                (
                    dirs_next::home_dir()
                        .unwrap_or_default()
                        .join(".s3-turbo-list.toml"),
                    "home",
                ),
            ]
        };
        let mut loaded_config = None;
        let mut loaded_config_kind = "none".to_string();
        let mut warnings = Vec::new();

        for (path, kind) in &search_paths {
            if path.exists() {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| format!("Failed to read config {}: {}", path.display(), e))?;
                let file_config: S3TurboConfig = toml::from_str(&content)
                    .map_err(|e| format!("Failed to parse config {}: {}", path.display(), e))?;
                config.merge(file_config);
                log::info!("Loaded config from {}", path.display());
                loaded_config = Some(path.display().to_string());
                loaded_config_kind = (*kind).to_string();
                break;
            }
        }

        if let (Some(explicit), None) = (cli_config_path, loaded_config.as_ref()) {
            warnings.push(format!(
                "explicit config file '{}' was not found; using built-in defaults",
                explicit
            ));
        }

        let summary = ConfigLoadSummary {
            explicit_config: cli_config_path.map(str::to_string),
            loaded_config,
            loaded_config_kind,
            searched: search_paths
                .iter()
                .map(|(path, _kind)| path.display().to_string())
                .collect(),
            warnings,
        };

        Ok((config, summary))
    }

    fn merge(&mut self, other: S3TurboConfig) {
        self.s3.max_attempts = other.s3.max_attempts;
        self.s3.initial_backoff_secs = other.s3.initial_backoff_secs;
        self.s3.connect_timeout_secs = other.s3.connect_timeout_secs;
        self.s3.operation_timeout_secs = other.s3.operation_timeout_secs;
        if other.s3.endpoint_url.is_some() {
            self.s3.endpoint_url = other.s3.endpoint_url;
        }
        self.s3.force_path_style = other.s3.force_path_style;
        self.s3.addressing_style = other.s3.addressing_style;
        if other.s3.profile.is_some() {
            self.s3.profile = other.s3.profile;
        }
        self.s3.debug_s3 = other.s3.debug_s3;
        if other.s3.trace_compat.is_some() {
            self.s3.trace_compat = other.s3.trace_compat;
        }
        if other.s3.start_after.is_some() {
            self.s3.start_after = other.s3.start_after;
        }
        self.runtime.worker_threads = other.runtime.worker_threads;
        self.runtime.max_concurrency = other.runtime.max_concurrency;
        self.output.row_group_size = other.output.row_group_size;
        if other.output.compression != default_compression()
            || self.output.compression == default_compression()
        {
            self.output.compression = other.output.compression;
        }
        self.output.compression_level = other.output.compression_level;
        if other.output.log_file.is_some() {
            self.output.log_file = other.output.log_file;
        }
        if other.output.ks_file.is_some() {
            self.output.ks_file = other.output.ks_file;
        }
        if other.output.parquet_file.is_some() {
            self.output.parquet_file = other.output.parquet_file;
        }
        self.auto_hints.sample_threshold = other.auto_hints.sample_threshold;
        self.auto_hints.max_prefix_depth = other.auto_hints.max_prefix_depth;
        self.auto_hints.max_prefix_entries = other.auto_hints.max_prefix_entries;
        self.channel.capacity = other.channel.capacity;
    }

    pub fn apply_cli_overrides(
        &mut self,
        threads: Option<usize>,
        concurrency: Option<usize>,
        endpoint: Option<&str>,
        force_path_style: bool,
        addressing_style: Option<&str>,
        profile: Option<&str>,
        debug_s3: bool,
        trace_compat: Option<&str>,
        start_after: Option<&str>,
        log_file: Option<&str>,
        ks_file: Option<&str>,
        parquet_file: Option<&str>,
        compression: Option<&str>,
        compression_level: Option<u32>,
    ) {
        if let Some(t) = threads {
            self.runtime.worker_threads = t;
        }
        if let Some(c) = concurrency {
            self.runtime.max_concurrency = c;
        }
        if let Some(e) = endpoint {
            self.s3.endpoint_url = Some(e.to_string());
        }
        if let Some(s) = addressing_style {
            if let Ok(style) = s.parse::<AddressingStyle>() {
                match style {
                    AddressingStyle::Path => {
                        self.s3.addressing_style = AddressingStyle::Path;
                        self.s3.force_path_style = true;
                    }
                    AddressingStyle::Virtual => {
                        self.s3.addressing_style = AddressingStyle::Virtual;
                        self.s3.force_path_style = false;
                    }
                    AddressingStyle::Auto => {
                        self.s3.addressing_style = AddressingStyle::Auto;
                        self.s3.force_path_style = false;
                    }
                }
            }
        }
        if force_path_style {
            self.s3.force_path_style = true;
            self.s3.addressing_style = AddressingStyle::Path;
        }
        if let Some(p) = profile {
            self.s3.profile = Some(p.to_string());
        }
        if debug_s3 {
            self.s3.debug_s3 = true;
        }
        if let Some(tc) = trace_compat {
            self.s3.trace_compat = Some(tc.to_string());
        }
        if let Some(sa) = start_after {
            self.s3.start_after = Some(sa.to_string());
        }
        if let Some(lf) = log_file {
            self.output.log_file = Some(lf.to_string());
        }
        if let Some(kf) = ks_file {
            self.output.ks_file = Some(kf.to_string());
        }
        if let Some(pf) = parquet_file {
            self.output.parquet_file = Some(pf.to_string());
        }
        if let Some(codec) = compression {
            self.output.compression = codec.to_string();
        }
        if let Some(level) = compression_level {
            self.output.compression_level = level;
        }
    }

    pub fn apply_profile_preset(&mut self) {
        if let Some(application) = crate::profiles::apply_profile_preset(self) {
            if application.known {
                log::info!(
                    "Applied endpoint profile '{}': endpoint applied {}, addressing applied {}",
                    application.name,
                    application.endpoint_url_applied,
                    application.addressing_style_applied
                );
            } else {
                log::warn!(
                    "Unknown vendor/profile '{}' — no preset applied",
                    application.name
                );
            }
        }
    }

    pub fn normalize_addressing_style(&mut self) {
        if self.s3.force_path_style {
            self.s3.addressing_style = AddressingStyle::Path;
            return;
        }

        match self.s3.addressing_style {
            AddressingStyle::Path => self.s3.force_path_style = true,
            AddressingStyle::Virtual | AddressingStyle::Auto => self.s3.force_path_style = false,
        }
    }
}

// ── Filter compilation ─────────────────────────────────────

const OBJECT_FILTER_MAX_LEN: usize = 512;

fn build_filter_engine(expr: &str, mode: Option<&RunMode>) -> Result<ObjectFilter, String> {
    if expr.len() > OBJECT_FILTER_MAX_LEN {
        return Err(format!(
            "Filter expression is too long: {} bytes, max {}",
            expr.len(),
            OBJECT_FILTER_MAX_LEN
        ));
    }
    if expr.contains('"') || expr.contains('\'') {
        return Err("Filter expression cannot contain string or character literals".to_string());
    }

    let allow_target = mode == Some(&RunMode::BiDir);
    let compiled = FilterExpr::compile(expr, allow_target).map_err(|e| {
        format!(
            "Filter expression contains unsupported syntax or identifiers: {}",
            e
        )
    })?;

    Ok(ObjectFilter::new(Box::new(
        move |source: &ObjectProps, target: Option<&ObjectProps>| compiled.evaluate(source, target),
    )))
}

#[allow(dead_code)] // Phase 5: standalone convenience wrapper
pub fn compile_filter(expr: &str) -> Result<ObjectFilter, String> {
    build_filter_engine(expr, None)
}

pub fn compile_filter_with_mode(expr: &str, mode: &RunMode) -> Result<ObjectFilter, String> {
    build_filter_engine(expr, Some(mode))
}

/// Install the global object filter (called once at startup).
pub fn install_filter(expr: &str, mode: &RunMode) -> Result<(), String> {
    let filter = compile_filter_with_mode(expr, mode)?;
    OBJECT_FILTER
        .set(filter)
        .map_err(|_| "Object filter already installed".to_string())
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = S3TurboConfig::default();
        assert_eq!(config.s3.max_attempts, 10);
        assert_eq!(config.s3.initial_backoff_secs, 30);
        assert_eq!(config.runtime.worker_threads, 10);
        assert_eq!(config.runtime.max_concurrency, 100);
        assert_eq!(config.output.row_group_size, 100000);
        assert_eq!(config.output.compression, "zstd");
        assert_eq!(config.output.compression_level, 1);
        assert_eq!(config.channel.capacity, 64);
        assert_eq!(config.s3.addressing_style, AddressingStyle::Auto);
        assert!(config.s3.profile.is_none());
        assert!(!config.s3.debug_s3);
        assert!(config.s3.trace_compat.is_none());
    }

    #[test]
    fn test_parse_toml_config() {
        let toml_str = r#"
[s3]
max_attempts = 5

[runtime]
max_concurrency = 50
"#;
        let config: S3TurboConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.s3.max_attempts, 5);
        assert_eq!(config.runtime.max_concurrency, 50);
        assert_eq!(config.s3.initial_backoff_secs, 30);
    }

    #[test]
    fn test_parse_bos_profile_toml() {
        let toml_str = r#"
[s3]
endpoint_url = "https://s3.bj.bcebos.com"
addressing_style = "path"
profile = "bos"
"#;
        let config: S3TurboConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("https://s3.bj.bcebos.com")
        );
        assert_eq!(config.s3.addressing_style, AddressingStyle::Path);
        assert_eq!(config.s3.profile.as_deref(), Some("bos"));
    }

    #[test]
    fn test_addressing_style_from_str() {
        assert_eq!(
            "path".parse::<AddressingStyle>().unwrap(),
            AddressingStyle::Path
        );
        assert_eq!(
            "virtual".parse::<AddressingStyle>().unwrap(),
            AddressingStyle::Virtual
        );
        assert_eq!(
            "auto".parse::<AddressingStyle>().unwrap(),
            AddressingStyle::Auto
        );
        assert_eq!(
            "PATH".parse::<AddressingStyle>().unwrap(),
            AddressingStyle::Path
        );
        assert!("invalid".parse::<AddressingStyle>().is_err());
    }

    #[test]
    fn test_addressing_style_display() {
        assert_eq!(AddressingStyle::Path.to_string(), "path");
        assert_eq!(AddressingStyle::Virtual.to_string(), "virtual");
        assert_eq!(AddressingStyle::Auto.to_string(), "auto");
    }

    #[test]
    fn test_apply_cli_overrides() {
        let mut config = S3TurboConfig::default();
        config.apply_cli_overrides(
            Some(4),
            Some(200),
            Some("https://custom.example.com"),
            true,
            Some("path"),
            Some("test-profile"),
            true,
            Some("/tmp/trace.jsonl"),
            Some("after-key"),
            Some("log.txt"),
            Some("ks.csv"),
            Some("out.parquet"),
            Some("zstd"),
            Some(3),
        );
        assert_eq!(config.runtime.worker_threads, 4);
        assert_eq!(config.runtime.max_concurrency, 200);
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("https://custom.example.com")
        );
        assert!(config.s3.force_path_style);
        assert_eq!(config.s3.addressing_style, AddressingStyle::Path);
        assert_eq!(config.s3.profile.as_deref(), Some("test-profile"));
        assert!(config.s3.debug_s3);
        assert_eq!(config.s3.trace_compat.as_deref(), Some("/tmp/trace.jsonl"));
        assert_eq!(config.s3.start_after.as_deref(), Some("after-key"));
        assert_eq!(config.output.log_file.as_deref(), Some("log.txt"));
        assert_eq!(config.output.ks_file.as_deref(), Some("ks.csv"));
        assert_eq!(config.output.parquet_file.as_deref(), Some("out.parquet"));
        assert_eq!(config.output.compression, "zstd");
        assert_eq!(config.output.compression_level, 3);
    }

    #[test]
    fn test_apply_bos_profile_preset() {
        let mut config = S3TurboConfig::default();
        config.s3.profile = Some("bos".to_string());
        config.apply_profile_preset();
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("https://s3.bj.bcebos.com")
        );
        assert_eq!(config.s3.addressing_style, AddressingStyle::Virtual);
        assert!(!config.s3.force_path_style);
    }

    #[test]
    fn test_apply_bos_profile_preserves_explicit_path_style() {
        let mut config = S3TurboConfig::default();
        config.s3.profile = Some("bos".to_string());
        config.s3.addressing_style = AddressingStyle::Path;
        config.apply_profile_preset();
        config.normalize_addressing_style();
        assert_eq!(config.s3.addressing_style, AddressingStyle::Path);
        assert!(config.s3.force_path_style);
    }

    #[test]
    fn test_normalize_addressing_style_path_forces_path_style() {
        let mut config = S3TurboConfig::default();
        config.s3.addressing_style = AddressingStyle::Path;
        config.normalize_addressing_style();
        assert!(config.s3.force_path_style);
    }

    #[test]
    fn test_normalize_addressing_style_force_path_style_wins() {
        let mut config = S3TurboConfig::default();
        config.s3.addressing_style = AddressingStyle::Virtual;
        config.s3.force_path_style = true;
        config.normalize_addressing_style();
        assert_eq!(config.s3.addressing_style, AddressingStyle::Path);
        assert!(config.s3.force_path_style);
    }

    #[test]
    fn test_endpoint_url_does_not_force_path_style() {
        let mut config = S3TurboConfig::default();
        config.apply_cli_overrides(
            None,
            None,
            Some("https://s3.bj.bcebos.com"),
            false,
            None,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        config.normalize_addressing_style();
        assert_eq!(config.s3.addressing_style, AddressingStyle::Auto);
        assert!(!config.s3.force_path_style);
    }

    #[test]
    fn test_cli_virtual_addressing_overrides_config_force_path_style() {
        let mut config = S3TurboConfig::default();
        config.s3.force_path_style = true;
        config.s3.addressing_style = AddressingStyle::Path;
        config.apply_cli_overrides(
            None,
            None,
            None,
            false,
            Some("virtual"),
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        config.normalize_addressing_style();
        assert_eq!(config.s3.addressing_style, AddressingStyle::Virtual);
        assert!(!config.s3.force_path_style);
    }

    #[test]
    fn test_basic_filter_compile() {
        let filter = compile_filter("SOURCE.size > 1000").unwrap();
        let mut props = ObjectProps::default();
        props.size = 2000;
        assert_eq!(filter.evaluate(&props, None), Some(true));
    }

    #[test]
    fn test_filter_compile_rejects_disallowed_property() {
        assert!(compile_filter("SOURCE.etag == \"abc\"").is_err());
    }

    #[test]
    fn test_filter_compile_rejects_disallowed_variable() {
        assert!(compile_filter("OTHER > 5").is_err());
    }

    #[test]
    fn test_filter_compile_accepts_boolean_comparison_chain() {
        let filter =
            compile_filter("SOURCE.size > 1000 && SOURCE.last_modified >= 1715700000").unwrap();
        let mut props = ObjectProps::default();
        props.size = 2048;
        props.last_modified = 1715700001;
        assert_eq!(filter.evaluate(&props, None), Some(true));
    }

    #[test]
    fn test_filter_compile_rejects_long_expression() {
        let expr = format!("SOURCE.size > {}", "1".repeat(OBJECT_FILTER_MAX_LEN));
        let err = compile_filter(&expr).unwrap_err();
        assert!(err.contains("too long"), "{}", err);
    }

    #[test]
    fn test_filter_compile_rejects_function_call() {
        let err = compile_filter("max(SOURCE.size, 1) > 0").unwrap_err();
        assert!(
            err.contains("unsupported syntax") || err.contains("not allowed"),
            "{}",
            err
        );
    }

    #[test]
    fn test_filter_compile_rejects_string_literals() {
        let err = compile_filter("SOURCE.size > 0 && \"x\" == \"x\"").unwrap_err();
        assert!(
            err.contains("unsupported syntax")
                || err.contains("not supported")
                || err.contains("cannot contain"),
            "{}",
            err
        );
    }

    #[test]
    fn test_filter_compile_with_mode_bidir() {
        let filter =
            compile_filter_with_mode("SOURCE.size > TARGET.size", &RunMode::BiDir).unwrap();
        let mut left = ObjectProps::default();
        left.size = 2000;
        let mut right = ObjectProps::default();
        right.size = 1000;
        assert_eq!(filter.evaluate(&left, Some(&right)), Some(true));
    }
}
