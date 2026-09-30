//! Structured S3-compatible observability trace module.
//!
//! Every listing page request (and each compat-probe request) produces one
//! [`S3CompatEvent`] written via a [`S3TraceWriter`]; startup discovery,
//! flat-namespace bisection and runtime split probes are not traced.  The
//! trace target (`--trace-compat`) is one of:
//!   - [`JsonlTraceWriter`]  — a JSONL file (`--trace-compat <file>`)
//!   - [`StderrTraceWriter`] — stderr       (`--trace-compat -`)

use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Mutex;

// ── S3CompatEvent ──────────────────────────────────────────

/// One structured trace event per S3 API call.  Every field is serialised
/// for JSONL output; optional fields use `skip_serializing_if`.
///
/// Field count: additive schema; optional fields use `skip_serializing_if`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3CompatEvent {
    // ── request identity ──────────────────────────────────
    pub timestamp: String, // ISO 8601, wall-clock
    pub operation: String, // "ListObjectsV2", "HeadBucket"
    /// The provider preset (e.g. "bos").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub endpoint_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub addressing_style: String, // "path", "virtual", "auto"
    pub bucket: String,
    pub prefix: String,

    // ── request parameters ────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,

    // ── outcome ───────────────────────────────────────────
    pub http_status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>, // x-amz-request-id or equivalent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id_2: Option<String>, // x-amz-id-2 (AWS extended)
    pub retry_attempt: u32, // 0-indexed
    pub latency_ms: u64,
    pub retryable: bool,
    pub fatal: bool,

    // ── pagination metadata (ListObjectsV2) ───────────────
    pub is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub common_prefixes_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_continuation_token_present: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_key: Option<String>,

    // ── segment summary metadata ─────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_pages: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_objects: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_common_prefixes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_by: Option<String>,

    // ── error body ────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated_raw_body: Option<String>, // first 512 bytes of error body
}

impl S3CompatEvent {
    /// Builder entry-point — caller fills remaining fields and calls
    /// [`S3TraceWriter::write_event`].
    pub fn new(operation: &str, endpoint_url: &str, bucket: &str, prefix: &str) -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            operation: operation.to_string(),
            provider: None,
            // Trace files get shared for diagnosis: never carry credentials.
            endpoint_url: crate::agent::redact_url_userinfo(endpoint_url),
            region: None,
            addressing_style: "auto".to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            delimiter: None,
            start_after: None,
            max_keys: None,
            continuation_token: None,
            http_status: 0,
            s3_error_code: None,
            s3_error_message: None,
            request_id: None,
            request_id_2: None,
            retry_attempt: 0,
            latency_ms: 0,
            retryable: false,
            fatal: false,
            is_truncated: false,
            next_continuation_token: None,
            key_count: None,
            contents_count: None,
            common_prefixes_count: None,
            next_continuation_token_present: None,
            first_key: None,
            last_key: None,
            segment_index: None,
            end_before: None,
            segment_pages: None,
            segment_objects: None,
            segment_common_prefixes: None,
            ended_by: None,
            truncated_raw_body: None,
        }
    }

    /// Record the provider preset.
    pub fn set_provider(&mut self, provider: Option<&str>) {
        self.provider = provider.map(str::to_string);
    }
}

// ── S3TraceWriter trait ────────────────────────────────────

/// Trait for writing trace events.  Implementations are `Send + Sync` so
/// they can be shared across concurrent list tasks.
pub trait S3TraceWriter: Send + Sync {
    fn write_event(&self, event: S3CompatEvent);
}

// ── JsonlTraceWriter ───────────────────────────────────────

/// Writes one JSON line per event to a file.  Thread-safe via internal
/// `Mutex<BufWriter<File>>`.  Flushes after every line so no events are
/// lost on crash.
pub struct JsonlTraceWriter {
    inner: Mutex<std::io::BufWriter<std::fs::File>>,
}

impl JsonlTraceWriter {
    pub fn new(path: &str) -> Result<Self, std::io::Error> {
        let file = std::fs::File::create(path)?;
        Ok(Self {
            inner: Mutex::new(std::io::BufWriter::new(file)),
        })
    }
}

impl S3TraceWriter for JsonlTraceWriter {
    fn write_event(&self, event: S3CompatEvent) {
        let json = serde_json::to_string(&event).unwrap_or_default();
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return, // poisoned — best effort
        };
        let _ = guard.write_all(json.as_bytes());
        let _ = guard.write_all(b"\n");
        let _ = guard.flush();
    }
}

// ── StderrTraceWriter ──────────────────────────────────────

/// Writes events to stderr (one JSON object per line): `--trace-compat -`.
pub struct StderrTraceWriter;

impl S3TraceWriter for StderrTraceWriter {
    fn write_event(&self, event: S3CompatEvent) {
        if let Ok(json) = serde_json::to_string(&event) {
            eprintln!("{}", json);
        }
    }
}

// ── Convenience constructor ────────────────────────────────

/// The trace writer for a `--trace-compat` target: a JSONL file, `-` for
/// stderr, or `None` for no tracing. An unwritable path is an output failure
/// the caller reports through the documented exit codes, not a panic.
pub fn trace_writer_for_target(
    target: Option<&str>,
) -> Result<Option<Box<dyn S3TraceWriter>>, String> {
    match target {
        None => Ok(None),
        Some("-") => Ok(Some(Box::new(StderrTraceWriter))),
        Some(path) => {
            let writer = JsonlTraceWriter::new(path)
                .map_err(|e| format!("Failed to create trace-compat file '{}': {}", path, e))?;
            Ok(Some(Box::new(writer)))
        }
    }
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_compat_event_json_roundtrip() {
        let mut event = S3CompatEvent::new(
            "ListObjectsV2",
            "https://s3.amazonaws.com",
            "my-bucket",
            "logs/",
        );
        event.region = Some("us-east-1".into());
        event.addressing_style = "virtual".into();
        event.http_status = 200;
        event.retry_attempt = 0;
        event.latency_ms = 42;
        event.is_truncated = false;
        event.key_count = Some(100);
        event.contents_count = Some(100);
        event.common_prefixes_count = Some(0);

        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["operation"], "ListObjectsV2");
        assert_eq!(parsed["http_status"], 200);
        assert_eq!(parsed["latency_ms"], 42);
        assert_eq!(parsed["key_count"], 100);
        assert!(parsed.get("s3_error_code").is_none());
    }

    #[test]
    fn test_s3_compat_event_error_fields() {
        let mut event = S3CompatEvent::new(
            "HeadBucket",
            "https://s3.bj.bcebos.com",
            "missing-bucket",
            "/",
        );
        event.region = Some("bj".into());
        event.addressing_style = "path".into();
        event.set_provider(Some("bos"));
        event.http_status = 404;
        event.s3_error_code = Some("NoSuchBucket".into());
        event.s3_error_message = Some("The specified bucket does not exist".into());
        event.request_id = Some("abc-123".into());
        event.fatal = true;

        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["s3_error_code"], "NoSuchBucket");
        assert_eq!(parsed["request_id"], "abc-123");
        assert_eq!(parsed["fatal"], true);
    }

    #[test]
    fn test_s3_compat_event_pagination_fields() {
        let mut event =
            S3CompatEvent::new("ListObjectsV2", "https://s3.example.com", "bucket", "pref/");
        event.delimiter = Some("/".into());
        event.start_after = Some("pref/abc".into());
        event.max_keys = Some(1000);
        event.is_truncated = true;
        event.next_continuation_token = Some("token-xyz".into());
        event.next_continuation_token_present = Some(true);

        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["delimiter"], "/");
        assert_eq!(parsed["max_keys"], 1000);
        assert_eq!(parsed["is_truncated"], true);
        assert_eq!(parsed["next_continuation_token"], "token-xyz");
        assert_eq!(parsed["next_continuation_token_present"], true);
    }

    #[test]
    fn test_jsonl_trace_writer_writes_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");
        let path_str = path.to_str().unwrap();

        let writer = JsonlTraceWriter::new(path_str).unwrap();
        let event = S3CompatEvent::new("HeadBucket", "http://x", "b", "/");
        writer.write_event(event);

        let content = std::fs::read_to_string(path_str).unwrap();
        assert!(content.contains("\"operation\":\"HeadBucket\""));
        assert!(content.ends_with('\n'));
    }

    #[test]
    fn test_trace_writer_for_target() {
        assert!(trace_writer_for_target(None).unwrap().is_none());
        assert!(trace_writer_for_target(Some("-")).unwrap().is_some());
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.jsonl");
        let p_str = p.to_str().unwrap();
        let w = trace_writer_for_target(Some(p_str)).unwrap().unwrap();
        w.write_event(S3CompatEvent::new("X", "e", "b", "/"));
        assert!(
            std::fs::read_to_string(p_str)
                .unwrap()
                .contains("\"operation\":\"X\"")
        );
    }

    #[test]
    fn test_trace_writer_unwritable_path_is_an_error_not_a_panic() {
        // A path whose parent is a regular file can never be created, by any
        // user — unlike a merely absent directory, which some other test or
        // tool may have since created.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("not-a-directory");
        std::fs::write(&blocker, b"x").unwrap();
        let path = blocker.join("trace.jsonl");
        let err = match trace_writer_for_target(Some(path.to_str().unwrap())) {
            Ok(_) => panic!("an unwritable trace path must surface as an error"),
            Err(e) => e,
        };
        assert!(err.contains("trace-compat"), "{}", err);
    }

    #[test]
    fn test_set_provider_fills_only_the_provider_field() {
        let mut event = S3CompatEvent::new("ListObjectsV2", "http://x", "b", "/");
        event.set_provider(Some("bos"));
        let v: serde_json::Value = serde_json::to_value(&event).unwrap();
        assert_eq!(v["provider"], "bos");
        // The deprecated `profile` copy was removed in 0.39.
        assert!(v.get("profile").is_none());
    }

    #[test]
    fn test_s3_compat_event_optional_fields_omitted() {
        let event = S3CompatEvent::new("ListObjectsV2", "http://x", "b", "/");
        let json = serde_json::to_string(&event).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = v.as_object().unwrap();
        // These optional fields should NOT appear as keys in the output
        assert!(!obj.contains_key("s3_error_code"));
        assert!(!obj.contains_key("request_id"));
        assert!(!obj.contains_key("delimiter"));
        assert!(!obj.contains_key("continuation_token"));
        assert!(!obj.contains_key("truncated_raw_body"));
    }
}
