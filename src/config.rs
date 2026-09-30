use crate::core::{OBJECT_FILTER, ObjectFilter, ObjectProps, RunMode};
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
#[serde(deny_unknown_fields)]
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
    /// The addressing style set in the config file, if any. The resolved
    /// style (CLI, then this, then the provider preset, then auto) is
    /// `addressing_style`.
    #[serde(default, rename = "addressing_style", skip_serializing)]
    pub addressing_style_setting: Option<AddressingStyle>,
    #[serde(skip)]
    pub addressing_style: AddressingStyle,
    /// Whether `addressing_style` came from the CLI or the config file (a
    /// provider preset only fills in a style nobody chose).
    #[serde(skip)]
    pub addressing_style_explicit: bool,
    /// The provider preset.
    #[serde(default)]
    pub provider: Option<String>,
    /// Per-run settings from the command line; not config-file keys (a
    /// `start_after` in a config file silently truncated every run).
    #[serde(skip)]
    pub trace_compat: Option<String>,
    #[serde(skip)]
    pub start_after: Option<String>,
}

impl S3Config {
    pub fn force_path_style(&self) -> bool {
        self.addressing_style == AddressingStyle::Path
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_backoff_secs: default_initial_backoff_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
            operation_timeout_secs: default_operation_timeout_secs(),
            endpoint_url: None,
            addressing_style_setting: None,
            addressing_style: AddressingStyle::default(),
            addressing_style_explicit: false,
            provider: None,
            trace_compat: None,
            start_after: None,
        }
    }
}

// ── RuntimeConfig ─────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_compression_level")]
    pub compression_level: u32,
    /// Output paths are per-run settings from the command line, not
    /// config-file keys: a path in the config file made every run write
    /// (and overwrite) the same files.
    #[serde(skip)]
    pub log_file: Option<String>,
    #[serde(skip)]
    pub ks_file: Option<String>,
    #[serde(skip)]
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

// ── ChannelConfig ─────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct S3TurboConfig {
    #[serde(default)]
    pub s3: S3Config,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub channel: ChannelConfig,
}

#[derive(Debug, Clone)]
pub struct ConfigLoadSummary {
    pub explicit_config: Option<String>,
    pub loaded_config: Option<String>,
    pub loaded_config_kind: String,
    pub searched: Vec<String>,
}

impl Default for S3TurboConfig {
    fn default() -> Self {
        Self {
            s3: S3Config::default(),
            runtime: RuntimeConfig::default(),
            output: OutputConfig::default(),
            channel: ChannelConfig::default(),
        }
    }
}

// ── Default value functions ────────────────────────────────

fn default_max_attempts() -> u32 {
    10
}
fn default_initial_backoff_secs() -> u64 {
    1
}
fn default_connect_timeout_secs() -> u64 {
    60
}
fn default_operation_timeout_secs() -> u64 {
    5
}
fn default_worker_threads() -> usize {
    // Match the machine instead of a fixed count: oversubscribing small
    // hosts (e.g. 10 workers on 2 vCPUs) measurably degrades throughput.
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4)
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
fn default_channel_capacity() -> usize {
    64
}

// ── Config loading ────────────────────────────────────────

/// `[s3]` keys removed in 0.39, and what to write instead.
const REMOVED_S3_KEYS: &[(&str, &str)] = &[
    ("profile", "use s3.provider"),
    ("force_path_style", "use s3.addressing_style = \"path\""),
];

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
                    std::env::home_dir()
                        .unwrap_or_default()
                        .join(".s3-turbo-list.toml"),
                    "home",
                ),
            ]
        };
        let mut loaded_config = None;
        let mut loaded_config_kind = "none".to_string();

        for (path, kind) in &search_paths {
            if path.exists() {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| format!("Failed to read config {}: {}", path.display(), e))?;
                config = Self::parse(&content)
                    .map_err(|e| format!("Failed to parse config {}: {}", path.display(), e))?;
                log::info!("Loaded config from {}", path.display());
                loaded_config = Some(path.display().to_string());
                loaded_config_kind = (*kind).to_string();
                break;
            }
        }

        // An explicit --config is a strict opt-in. Falling back to built-in
        // defaults when it is missing used to list the same-named bucket on
        // real AWS with ambient credentials instead of the intended endpoint.
        if let (Some(explicit), None) = (cli_config_path, loaded_config.as_ref()) {
            return Err(format!(
                "config file '{}' was not found (pass an existing file to --config, or omit it \
                 to use ./s3-turbo-list.toml or ~/.s3-turbo-list.toml when present)",
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
        };

        Ok((config, summary))
    }

    /// A config file's content over the defaults.
    pub fn parse(content: &str) -> Result<Self, String> {
        // The keys removed in 0.39 would be serde's bare "unknown field";
        // say what replaces them.
        if let Ok(table) = toml::from_str::<toml::Table>(content)
            && let Some(s3) = table.get("s3").and_then(toml::Value::as_table)
        {
            for (key, replacement) in REMOVED_S3_KEYS {
                if s3.contains_key(*key) {
                    return Err(format!(
                        "unknown field `{}` in [s3]: s3.{} was removed in 0.39; {}",
                        key, key, replacement
                    ));
                }
            }
        }
        let file_config: S3TurboConfig = toml::from_str(content).map_err(|e| e.to_string())?;
        let mut config = Self::default();
        config.merge(file_config);
        Ok(config)
    }

    /// Replace a provider preset's name with its canonical spelling
    /// (`BOS` → `bos`); an unknown name is left as given.
    pub fn normalize_provider(&mut self) {
        if let Some(preset) = self
            .s3
            .provider
            .as_deref()
            .and_then(crate::profiles::get_profile)
        {
            self.s3.provider = Some(preset.name.to_string());
        }
    }

    fn merge(&mut self, other: S3TurboConfig) {
        self.s3.max_attempts = other.s3.max_attempts;
        self.s3.initial_backoff_secs = other.s3.initial_backoff_secs;
        self.s3.connect_timeout_secs = other.s3.connect_timeout_secs;
        self.s3.operation_timeout_secs = other.s3.operation_timeout_secs;
        if other.s3.endpoint_url.is_some() {
            self.s3.endpoint_url = other.s3.endpoint_url;
        }
        if let Some(style) = other.s3.addressing_style_setting {
            self.s3.addressing_style = style;
            self.s3.addressing_style_explicit = true;
        }
        if other.s3.provider.is_some() {
            self.s3.provider = other.s3.provider;
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
        self.channel.capacity = other.channel.capacity;
    }

    /// Command-line settings, applied over the config file.
    pub fn apply_cli_overrides(&mut self, cli: CliOverrides<'_>) {
        if let Some(t) = cli.threads {
            self.runtime.worker_threads = t;
        }
        if let Some(c) = cli.concurrency {
            self.runtime.max_concurrency = c;
        }
        if let Some(e) = cli.endpoint {
            self.s3.endpoint_url = Some(e.to_string());
        }
        if let Some(style) = cli.addressing_style.and_then(|s| s.parse().ok()) {
            self.s3.addressing_style = style;
            self.s3.addressing_style_explicit = true;
        }
        if let Some(p) = cli.provider {
            self.s3.provider = Some(p.to_string());
        }
        self.s3.trace_compat = cli.trace_compat.map(str::to_string);
        self.s3.start_after = cli.start_after.map(str::to_string);
        self.output.parquet_file = cli.parquet_file.map(str::to_string);
        if let Some(codec) = cli.compression {
            self.output.compression = codec.to_string();
        }
        if let Some(level) = cli.compression_level {
            self.output.compression_level = level;
        }
    }

    pub fn apply_profile_preset(&mut self, region: Option<&str>) {
        if let Some(application) = crate::profiles::apply_profile_preset(self, region) {
            if application.known {
                log::info!(
                    "Applied provider preset '{}': endpoint applied {}, addressing applied {}",
                    application.name,
                    application.endpoint_url_applied,
                    application.addressing_style_applied
                );
            } else {
                log::warn!(
                    "Unknown provider '{}' — no preset applied",
                    application.name
                );
            }
        }
    }
}

/// The command-line values `apply_cli_overrides` takes.
#[derive(Debug, Default, Clone, Copy)]
pub struct CliOverrides<'a> {
    pub threads: Option<usize>,
    pub concurrency: Option<usize>,
    pub endpoint: Option<&'a str>,
    pub addressing_style: Option<&'a str>,
    pub provider: Option<&'a str>,
    pub trace_compat: Option<&'a str>,
    pub start_after: Option<&'a str>,
    pub parquet_file: Option<&'a str>,
    pub compression: Option<&'a str>,
    pub compression_level: Option<u32>,
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

#[cfg(test)]
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
        assert_eq!(config.s3.initial_backoff_secs, 1);
        assert_eq!(
            config.runtime.worker_threads,
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(4)
        );
        assert_eq!(config.runtime.max_concurrency, 100);
        assert_eq!(config.output.row_group_size, 100000);
        assert_eq!(config.output.compression, "zstd");
        assert_eq!(config.output.compression_level, 1);
        assert_eq!(config.channel.capacity, 64);
        assert_eq!(config.s3.addressing_style, AddressingStyle::Auto);
        assert!(config.s3.provider.is_none());
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
        assert_eq!(config.s3.initial_backoff_secs, 1);
    }

    #[test]
    fn test_unknown_config_keys_are_rejected() {
        // A misspelled key used to be ignored, silently running on the default.
        for bad in [
            "[runtime]\nmax_concurency = 5\n",
            "[s3]\nendpoint = \"http://127.0.0.1:9000\"\n",
            "[outputs]\ncompression = \"zstd\"\n",
        ] {
            let err = toml::from_str::<S3TurboConfig>(bad)
                .unwrap_err()
                .to_string();
            assert!(err.contains("unknown field"), "{bad:?}: {err}");
        }
    }

    #[test]
    fn test_parse_provider_toml() {
        let config = S3TurboConfig::parse(
            "[s3]\nendpoint_url = \"https://s3.bj.bcebos.com\"\naddressing_style = \"path\"\nprovider = \"bos\"\n",
        )
        .unwrap();
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("https://s3.bj.bcebos.com")
        );
        assert_eq!(config.s3.addressing_style, AddressingStyle::Path);
        assert!(config.s3.addressing_style_explicit);
        assert_eq!(config.s3.provider.as_deref(), Some("bos"));
    }

    #[test]
    fn test_removed_config_keys_are_unknown_keys_that_name_the_replacement() {
        for (content, replacement) in [
            ("[s3]\nprofile = \"bos\"\n", "use s3.provider"),
            (
                "[s3]\nprovider = \"bos\"\nprofile = \"minio\"\n",
                "use s3.provider",
            ),
            (
                "[s3]\nforce_path_style = true\n",
                "use s3.addressing_style = \"path\"",
            ),
        ] {
            let err = S3TurboConfig::parse(content).unwrap_err();
            assert!(err.contains("unknown field"), "{content:?}: {err}");
            assert!(err.contains("removed in 0.39"), "{content:?}: {err}");
            assert!(err.contains(replacement), "{content:?}: {err}");
        }
        // Neither is offered among the expected keys any more.
        let err = S3TurboConfig::parse("[s3]\nprofil = \"bos\"\n").unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
        assert!(!err.contains("`profile`"), "{err}");
        assert!(!err.contains("force_path_style"), "{err}");
    }

    #[test]
    fn test_normalize_provider_uses_the_canonical_name() {
        let mut config = S3TurboConfig::default();
        config.s3.provider = Some("BOS".to_string());
        config.normalize_provider();
        assert_eq!(config.s3.provider.as_deref(), Some("bos"));
        config.s3.provider = Some("NoSuch".to_string());
        config.normalize_provider();
        assert_eq!(config.s3.provider.as_deref(), Some("NoSuch"));
    }

    #[test]
    fn test_per_run_keys_are_not_config_keys() {
        // Output paths and start_after pinned every run to the same files or
        // silently truncated it; debug_s3/trace_compat are command-line only.
        for key in [
            "[s3]\nstart_after = \"k\"\n",
            "[s3]\ndebug_s3 = true\n",
            "[s3]\ntrace_compat = \"t.jsonl\"\n",
            "[output]\nparquet_file = \"o.parquet\"\n",
            "[output]\nks_file = \"o.ks\"\n",
            "[output]\nlog_file = \"o.log\"\n",
        ] {
            let err = S3TurboConfig::parse(key).unwrap_err();
            assert!(err.contains("unknown field"), "{key:?}: {err}");
        }
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
        config.apply_cli_overrides(CliOverrides {
            threads: Some(4),
            concurrency: Some(200),
            endpoint: Some("https://custom.example.com"),
            addressing_style: Some("path"),
            provider: Some("test-profile"),
            trace_compat: Some("/tmp/trace.jsonl"),
            start_after: Some("after-key"),
            parquet_file: Some("out.parquet"),
            compression: Some("zstd"),
            compression_level: Some(3),
        });
        assert_eq!(config.runtime.worker_threads, 4);
        assert_eq!(config.runtime.max_concurrency, 200);
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("https://custom.example.com")
        );
        assert!(config.s3.force_path_style());
        assert_eq!(config.s3.provider.as_deref(), Some("test-profile"));
        assert_eq!(config.s3.trace_compat.as_deref(), Some("/tmp/trace.jsonl"));
        assert_eq!(config.s3.start_after.as_deref(), Some("after-key"));
        assert_eq!(config.output.parquet_file.as_deref(), Some("out.parquet"));
        assert_eq!(config.output.compression, "zstd");
        assert_eq!(config.output.compression_level, 3);
    }

    #[test]
    fn test_profile_endpoint_templates_derive_from_region() {
        for (profile, region, expected) in [
            (
                "oss",
                "oss-cn-beijing",
                "https://oss-cn-beijing.aliyuncs.com",
            ),
            ("bos", "gz", "https://s3.gz.bcebos.com"),
            (
                "b2",
                "us-west-004",
                "https://s3.us-west-004.backblazeb2.com",
            ),
        ] {
            let mut config = S3TurboConfig::default();
            config.s3.provider = Some(profile.to_string());
            config.apply_profile_preset(Some(region));
            assert_eq!(
                config.s3.endpoint_url.as_deref(),
                Some(expected),
                "profile {}",
                profile
            );
        }
    }

    #[test]
    fn test_profile_template_never_overrides_explicit_endpoint() {
        let mut config = S3TurboConfig::default();
        config.s3.provider = Some("oss".to_string());
        config.s3.endpoint_url = Some("https://oss-cn-beijing-internal.aliyuncs.com".to_string());
        config.apply_profile_preset(Some("oss-cn-beijing"));
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("https://oss-cn-beijing-internal.aliyuncs.com")
        );
    }

    #[test]
    fn test_profile_template_without_region_leaves_endpoint_unset() {
        let mut config = S3TurboConfig::default();
        config.s3.provider = Some("oss".to_string());
        config.apply_profile_preset(None);
        assert_eq!(config.s3.endpoint_url, None);
    }

    #[test]
    fn test_apply_bos_profile_preset() {
        let mut config = S3TurboConfig::default();
        config.s3.provider = Some("bos".to_string());
        config.apply_profile_preset(None);
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("https://s3.bj.bcebos.com")
        );
        assert_eq!(config.s3.addressing_style, AddressingStyle::Virtual);
        assert!(!config.s3.force_path_style());
    }

    #[test]
    fn test_explicit_addressing_style_wins_over_the_preset() {
        // Explicit `auto` included: it used to be indistinguishable from the
        // default and was replaced by the preset's style.
        for style in ["path", "auto"] {
            let mut config = S3TurboConfig::default();
            config.apply_cli_overrides(CliOverrides {
                provider: Some("minio"),
                addressing_style: Some(style),
                ..CliOverrides::default()
            });
            config.apply_profile_preset(None);
            assert_eq!(config.s3.addressing_style.to_string(), style);
        }
        let mut config = S3TurboConfig::default();
        config.s3.provider = Some("minio".to_string());
        config.apply_profile_preset(None);
        assert_eq!(config.s3.addressing_style, AddressingStyle::Path);
    }

    #[test]
    fn test_endpoint_url_does_not_force_path_style() {
        let mut config = S3TurboConfig::default();
        config.apply_cli_overrides(CliOverrides {
            endpoint: Some("https://s3.bj.bcebos.com"),
            ..CliOverrides::default()
        });
        assert_eq!(config.s3.addressing_style, AddressingStyle::Auto);
        assert!(!config.s3.force_path_style());
    }

    #[test]
    fn test_cli_virtual_addressing_overrides_config_file_style() {
        let mut config = S3TurboConfig::parse("[s3]\naddressing_style = \"path\"\n").unwrap();
        config.apply_cli_overrides(CliOverrides {
            addressing_style: Some("virtual"),
            ..CliOverrides::default()
        });
        assert_eq!(config.s3.addressing_style, AddressingStyle::Virtual);
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
