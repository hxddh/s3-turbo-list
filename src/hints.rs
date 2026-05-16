use crate::auto_hints::HintsCache;
use log::info;
use serde::Serialize;

const TOML_FIELDS: &[&str] = &[
    "bucket",
    "region",
    "total_objects",
    "boundaries",
    "generated_at",
    "scan_mode",
    "sampled_objects",
    "sampled_pages",
    "sample_limit",
    "max_pages",
];

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HintsFormat {
    Toml,
    Plain,
}

#[derive(Debug, Clone, Serialize)]
pub struct HintsMetadata {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub total_objects: Option<usize>,
    pub generated_at: Option<String>,
    pub scan_mode: Option<String>,
    pub sampled_objects: Option<usize>,
    pub sampled_pages: Option<usize>,
    pub sample_limit: Option<usize>,
    pub max_pages: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HintsValidationReport {
    pub path: String,
    pub format: HintsFormat,
    pub boundary_count: usize,
    pub metadata: Option<HintsMetadata>,
    pub first_boundaries: Vec<String>,
    pub warnings: Vec<String>,
    pub valid: bool,
}

pub fn parse_hints_file(path: &str) -> Result<Vec<String>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read hints file '{}': {}", path, e))?;

    if looks_like_toml_hints(&content) {
        parse_as_toml(path, &content)
    } else {
        parse_as_plain(path, &content)
    }
}

pub fn inspect_hints_file(
    path: &str,
    preview_limit: usize,
) -> Result<HintsValidationReport, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read hints file '{}': {}", path, e))?;

    if looks_like_toml_hints(&content) {
        let cache = parse_toml_cache(path, &content)?;
        let mut warnings = boundary_warnings(&cache.boundaries);
        if cache.boundaries.is_empty() {
            warnings.push("hints file contains no boundaries".to_string());
        }
        let first_boundaries = cache
            .boundaries
            .iter()
            .take(preview_limit)
            .cloned()
            .collect();
        Ok(HintsValidationReport {
            path: path.to_string(),
            format: HintsFormat::Toml,
            boundary_count: cache.boundaries.len(),
            metadata: Some(HintsMetadata::from(&cache)),
            first_boundaries,
            warnings,
            valid: true,
        })
    } else {
        let parsed = collect_plain_boundaries(path, &content)?;
        let mut boundaries = parsed.boundaries.clone();
        boundaries.sort();
        boundaries.dedup();
        let mut warnings = parsed.warnings;
        if boundaries.is_empty() {
            warnings.push("hints file contains no boundaries".to_string());
        }
        let first_boundaries = boundaries.iter().take(preview_limit).cloned().collect();
        Ok(HintsValidationReport {
            path: path.to_string(),
            format: HintsFormat::Plain,
            boundary_count: boundaries.len(),
            metadata: None,
            first_boundaries,
            warnings,
            valid: true,
        })
    }
}

pub fn warn_bos_hinted_segments(profile: Option<&str>, boundaries: &[String]) {
    if profile == Some("bos") && !boundaries.is_empty() {
        log::warn!(
            "BOS profile with hinted multi-segment listing detected ({} boundaries). \
             BOS has a documented ListObjectsV2 start_after + continuation-token \
             compatibility issue; hinted BOS scans are not authoritative until BOS \
             fixes the service-side behavior. Use single-segment listing for \
             authoritative BOS output.",
            boundaries.len()
        );
    }
}

pub(crate) fn looks_like_toml_hints(content: &str) -> bool {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.contains("boundaries = [") || trimmed.contains("boundaries=[") {
        return true;
    }

    for line in trimmed.lines() {
        let stripped = line.trim();
        if stripped.starts_with('[') && stripped.ends_with(']') && stripped.len() > 2 {
            return true;
        }
    }

    let mut toml_kv_count = 0usize;
    let mut known_field = false;
    for line in trimmed.lines() {
        let stripped = line.trim();
        if stripped.is_empty() || stripped.starts_with('#') {
            continue;
        }
        if let Some((key, _)) = stripped.split_once('=') {
            toml_kv_count += 1;
            let k = key.trim();
            if TOML_FIELDS.contains(&k) {
                known_field = true;
            }
        }
    }

    let non_blank = trimmed.lines().filter(|l| !l.trim().is_empty()).count();
    toml_kv_count >= non_blank / 2 && known_field
}

fn parse_as_toml(path: &str, content: &str) -> Result<Vec<String>, String> {
    let cached = parse_toml_cache(path, content)?;
    info!(
        "Loaded {} auto-hints boundaries from TOML cache '{}'",
        cached.boundaries.len(),
        path
    );
    Ok(cached.boundaries)
}

fn parse_toml_cache(path: &str, content: &str) -> Result<HintsCache, String> {
    let cached: HintsCache = toml::from_str(content).map_err(|e| {
        format!(
            "Hints file '{}' looks like a TOML hints cache but failed to parse: {}. \
             If this is a plain hints file, remove any lines containing '=', '[', or ']'.",
            path, e
        )
    })?;

    for (i, b) in cached.boundaries.iter().enumerate() {
        if let Err(reason) = validate_boundary_line(b, i) {
            return Err(format!(
                "Hints file '{}' boundary {} is malformed: {}",
                path, i, reason
            ));
        }
        if b.chars().any(char::is_control) {
            return Err(format!(
                "Hints file '{}' boundary {} contains control characters",
                path, i
            ));
        }
    }

    Ok(cached)
}

fn parse_as_plain(path: &str, content: &str) -> Result<Vec<String>, String> {
    let parsed = collect_plain_boundaries(path, content)?;
    let mut boundaries = parsed.boundaries;
    boundaries.sort();
    boundaries.dedup();

    info!(
        "Loaded {} plain-text hints boundaries from '{}'",
        boundaries.len(),
        path
    );
    Ok(boundaries)
}

struct PlainParse {
    boundaries: Vec<String>,
    warnings: Vec<String>,
}

fn collect_plain_boundaries(path: &str, content: &str) -> Result<PlainParse, String> {
    let mut boundaries: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    for (line_no, raw) in content.lines().enumerate() {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if let Err(reason) = validate_boundary_line(trimmed, line_no) {
            return Err(format!(
                "Hints file '{}' line {} looks like leaked TOML syntax: '{}' - {}",
                path,
                line_no + 1,
                trimmed,
                reason
            ));
        }

        if trimmed.chars().any(char::is_control) {
            warnings.push(format!(
                "line {} contains control characters and should be regenerated",
                line_no + 1
            ));
        }
        boundaries.push(trimmed.to_string());
    }

    warnings.extend(boundary_warnings(&boundaries));
    Ok(PlainParse {
        boundaries,
        warnings,
    })
}

fn boundary_warnings(boundaries: &[String]) -> Vec<String> {
    let mut warnings = Vec::new();
    for pair in boundaries.windows(2) {
        if pair[0] > pair[1] {
            warnings.push(
                "boundaries are not sorted; regenerate hints for deterministic segment ordering"
                    .to_string(),
            );
            break;
        }
    }

    let mut sorted = boundaries.to_vec();
    sorted.sort();
    let before = sorted.len();
    sorted.dedup();
    if sorted.len() != before {
        warnings.push(format!(
            "duplicate boundaries detected; {} duplicate entries will be ignored",
            before - sorted.len()
        ));
    }

    warnings
}

fn validate_boundary_line(s: &str, line_no: usize) -> Result<(), String> {
    if (s.starts_with('"') && s.ends_with(',')) || (s.starts_with('\'') && s.ends_with(',')) {
        return Err(format!(
            "value '{}' looks like a quoted TOML array entry with trailing comma. \
             Plain hints files should contain bare keys (e.g. 'alpha/'), not quoted TOML values.",
            s
        ));
    }

    if s.starts_with("boundaries =") || s.starts_with("boundaries=") {
        return Err(format!(
            "line {} is TOML array header '{}'. Plain hints files should contain \
             only object keys, not TOML structure.",
            line_no + 1,
            s
        ));
    }

    if s == "]" {
        return Err(format!(
            "line {} is a TOML closing bracket ']'. Plain hints files should contain only object keys.",
            line_no + 1
        ));
    }

    if s.starts_with('[') && s.ends_with(']') && s.len() > 2 {
        return Err(format!(
            "line {} looks like a TOML table header '{}'. Plain hints files should contain only object keys.",
            line_no + 1,
            s
        ));
    }

    if s.contains('=') && !s.starts_with('#') {
        return Err(format!(
            "line {} looks like a TOML assignment '{}'. Plain hints files should contain only object keys. \
             If this IS an object key containing '=', use TOML format instead.",
            line_no + 1,
            s
        ));
    }

    Ok(())
}

impl From<&HintsCache> for HintsMetadata {
    fn from(cache: &HintsCache) -> Self {
        Self {
            bucket: Some(cache.bucket.clone()),
            region: cache.region.clone(),
            total_objects: Some(cache.total_objects),
            generated_at: Some(cache.generated_at.clone()),
            scan_mode: cache.scan_mode.clone(),
            sampled_objects: cache.sampled_objects,
            sampled_pages: cache.sampled_pages,
            sample_limit: cache.sample_limit,
            max_pages: cache.max_pages,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tmp(contents: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hints.toml");
        std::fs::write(&path, contents).unwrap();
        let path_str = path.to_str().unwrap().to_string();
        (dir, path_str)
    }

    #[test]
    fn test_looks_like_toml_boundaries_array() {
        assert!(looks_like_toml_hints(
            "bucket = \"b\"\nboundaries = [\n    \"alpha\",\n]\n"
        ));
    }

    #[test]
    fn test_does_not_look_like_toml_plain_keys() {
        assert!(!looks_like_toml_hints("alpha/\nbeta/\ngamma/\n"));
    }

    #[test]
    fn test_parse_toml_hints_clean() {
        let content = r#"bucket = "b"
region = "r"
total_objects = 10
boundaries = [
    "alpha/",
    "beta/file-05.txt",
    "logs/file with spaces.log",
    "logs/file+plus.log",
]
generated_at = "2026-01-01T00:00:00Z"
"#;
        let (_dir, path) = write_tmp(content);
        let result = parse_hints_file(&path).unwrap();
        assert_eq!(
            result,
            vec![
                "alpha/",
                "beta/file-05.txt",
                "logs/file with spaces.log",
                "logs/file+plus.log",
            ]
        );
    }

    #[test]
    fn test_parse_toml_hints_fails_on_malformed() {
        let content = "boundaries = [\n    alpha,\n    beta\n";
        let (_dir, path) = write_tmp(content);
        assert!(parse_hints_file(&path).is_err());
    }

    #[test]
    fn test_parse_plain_hints_clean() {
        let content = "alpha/\nbeta/file-05.txt\n";
        let (_dir, path) = write_tmp(content);
        let result = parse_hints_file(&path).unwrap();
        assert_eq!(result, vec!["alpha/", "beta/file-05.txt"]);
    }

    #[test]
    fn test_parse_plain_hints_skips_empty_and_comments() {
        let content = "\nalpha/\n# this is a comment\nbeta/\n  \n";
        let (_dir, path) = write_tmp(content);
        let result = parse_hints_file(&path).unwrap();
        assert_eq!(result, vec!["alpha/", "beta/"]);
    }

    #[test]
    fn test_parse_plain_rejects_quoted_comma() {
        let content = "alpha/\n\"beta\",\n";
        let (_dir, path) = write_tmp(content);
        let result = parse_hints_file(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("leaked TOML"));
    }

    #[test]
    fn test_parse_plain_rejects_boundaries_eq_bracket() {
        let content = "boundaries = [\nalpha/\n]\n";
        let (_dir, path) = write_tmp(content);
        assert!(parse_hints_file(&path).is_err());
    }

    #[test]
    fn test_inspect_plain_warns_duplicate_and_unsorted() {
        let content = "beta/\nalpha/\nalpha/\n";
        let (_dir, path) = write_tmp(content);
        let report = inspect_hints_file(&path, 5).unwrap();
        assert_eq!(report.format, HintsFormat::Plain);
        assert_eq!(report.boundary_count, 2);
        assert!(report.warnings.iter().any(|w| w.contains("not sorted")));
        assert!(report.warnings.iter().any(|w| w.contains("duplicate")));
    }

    #[test]
    fn test_inspect_toml_metadata() {
        let content = r#"bucket = "b"
region = "r"
total_objects = 2
boundaries = ["a/", "b/"]
generated_at = "2026-01-01T00:00:00Z"
scan_mode = "sampled"
sampled_objects = 2
sampled_pages = 1
sample_limit = 2
max_pages = 1
"#;
        let (_dir, path) = write_tmp(content);
        let report = inspect_hints_file(&path, 1).unwrap();
        let metadata = report.metadata.unwrap();
        assert_eq!(metadata.scan_mode.as_deref(), Some("sampled"));
        assert_eq!(metadata.sample_limit, Some(2));
        assert_eq!(report.first_boundaries, vec!["a/"]);
    }
}
