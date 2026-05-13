//! Structured S3-compatible observability trace module.
//!
//! Every S3 API call produces one [`S3CompatEvent`] written via a
//! [`S3TraceWriter`] implementation.  Three writers are provided:
//!   - [`JsonlTraceWriter`]  — JSONL file  (--trace-compat <file>)
//!   - [`StderrTraceWriter`] — stderr      (--debug-s3)
//!   - [`NoopTraceWriter`]   — silent       (default)

use serde::Serialize;
use std::io::Write;
use std::sync::Mutex;

// ── S3CompatEvent ──────────────────────────────────────────

/// One structured trace event per S3 API call.  Every field is serialised
/// for JSONL output; optional fields use `skip_serializing_if`.
///
/// Field count: 28 fields (12 required + 16 optional).
#[derive(Debug, Clone, Serialize)]
pub struct S3CompatEvent {
    // ── request identity ──────────────────────────────────
    pub timestamp: String, // ISO 8601, wall-clock
    pub operation: String, // "ListObjectsV2", "HeadBucket"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>, // vendor/profile name (e.g. "bos")
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
            profile: None,
            endpoint_url: endpoint_url.to_string(),
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
            truncated_raw_body: None,
        }
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

/// Writes events to stderr (one JSON object per line).  Used for
/// `--debug-s3`.
pub struct StderrTraceWriter;

impl S3TraceWriter for StderrTraceWriter {
    fn write_event(&self, event: S3CompatEvent) {
        if let Ok(json) = serde_json::to_string(&event) {
            eprintln!("{}", json);
        }
    }
}

// ── NoopTraceWriter ────────────────────────────────────────

/// Silent writer — used when tracing is disabled.
pub struct NoopTraceWriter;

impl S3TraceWriter for NoopTraceWriter {
    fn write_event(&self, _event: S3CompatEvent) {}
}

// ── Convenience constructors ───────────────────────────────

/// Create a [`JsonlTraceWriter`] if `path` is `Some`, otherwise create a
/// [`NoopTraceWriter`].  Panics if file creation fails.
pub fn create_trace_writer(
    trace_compat: Option<&str>,
    debug_s3: bool,
) -> (Box<dyn S3TraceWriter>, Box<dyn S3TraceWriter>) {
    let file_writer: Box<dyn S3TraceWriter> = match trace_compat {
        Some(path) => {
            Box::new(JsonlTraceWriter::new(path).expect("failed to create trace-compat file"))
        }
        None => Box::new(NoopTraceWriter),
    };

    let debug_writer: Box<dyn S3TraceWriter> = if debug_s3 {
        Box::new(StderrTraceWriter)
    } else {
        Box::new(NoopTraceWriter)
    };

    (file_writer, debug_writer)
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
        event.profile = Some("bos".into());
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
    fn test_noop_trace_writer_is_silent() {
        let writer = NoopTraceWriter;
        let event = S3CompatEvent::new("HeadBucket", "http://x", "b", "/");
        writer.write_event(event); // should not panic
    }

    #[test]
    fn test_create_trace_writer_combinations() {
        // No tracing
        let (fw, dw) = create_trace_writer(None, false);
        fw.write_event(S3CompatEvent::new("X", "e", "b", "/"));
        dw.write_event(S3CompatEvent::new("X", "e", "b", "/"));
        // just should not panic

        // File only
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.jsonl");
        let p_str = p.to_str().unwrap();
        let (fw, _dw) = create_trace_writer(Some(p_str), false);
        fw.write_event(S3CompatEvent::new("X", "e", "b", "/"));
        assert!(std::fs::read_to_string(p_str)
            .unwrap()
            .contains("\"operation\":\"X\""));
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
