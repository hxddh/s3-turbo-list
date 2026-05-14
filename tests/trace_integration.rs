// Integration tests for S3CompatEvent JSONL roundtrip and truncated_raw_body fix.
use s3_turbo_list::trace::{
    create_trace_writer, JsonlTraceWriter, NoopTraceWriter, S3CompatEvent, S3TraceWriter,
};

// ── JSON roundtrip — all required fields present ──────────

#[test]
fn test_trace_event_jsonl_roundtrip() {
    let mut event = S3CompatEvent::new(
        "ListObjectsV2",
        "https://s3.amazonaws.com",
        "my-bucket",
        "logs/",
    );
    event.region = Some("us-east-1".into());
    event.addressing_style = "virtual".into();
    event.profile = Some("aws".into());
    event.http_status = 200;
    event.retry_attempt = 1;
    event.latency_ms = 42;
    event.is_truncated = true;
    event.key_count = Some(100);
    event.contents_count = Some(100);
    event.common_prefixes_count = Some(5);
    event.next_continuation_token = Some("token-xyz".into());
    event.next_continuation_token_present = Some(true);
    event.delimiter = Some("/".into());
    event.start_after = Some("logs/app".into());
    event.max_keys = Some(1000);

    let json = serde_json::to_string(&event).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["operation"], "ListObjectsV2");
    assert_eq!(parsed["http_status"], 200);
    assert_eq!(parsed["latency_ms"], 42);
    assert_eq!(parsed["key_count"], 100);
    assert_eq!(parsed["region"], "us-east-1");
    assert_eq!(parsed["profile"], "aws");
    assert_eq!(parsed["is_truncated"], true);
    assert_eq!(parsed["next_continuation_token"], "token-xyz");
    assert_eq!(parsed["contents_count"], 100);
    assert_eq!(parsed["common_prefixes_count"], 5);
    assert_eq!(parsed["max_keys"], 1000);
    assert_eq!(parsed["delimiter"], "/");
    assert_eq!(parsed["start_after"], "logs/app");

    // Error fields should be absent on success.
    assert!(parsed.get("s3_error_code").is_none());
    assert!(parsed.get("s3_error_message").is_none());
}

// ── Optional fields omitted when None ─────────────────────

#[test]
fn test_trace_event_optional_fields_omitted() {
    let event = S3CompatEvent::new("ListObjectsV2", "http://x", "b", "/");
    let json = serde_json::to_string(&event).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    let obj = v.as_object().unwrap();

    // These optional fields should NOT appear as keys.
    assert!(!obj.contains_key("s3_error_code"));
    assert!(!obj.contains_key("s3_error_message"));
    assert!(!obj.contains_key("request_id"));
    assert!(!obj.contains_key("request_id_2"));
    assert!(!obj.contains_key("delimiter"));
    assert!(!obj.contains_key("continuation_token"));
    assert!(!obj.contains_key("truncated_raw_body"));
    assert!(!obj.contains_key("next_continuation_token"));
    assert!(!obj.contains_key("key_count"));
    assert!(!obj.contains_key("contents_count"));
    assert!(!obj.contains_key("common_prefixes_count"));
}

// ── truncated_raw_body present in JSON output ─────────────

#[test]
fn test_trace_event_truncated_raw_body_present() {
    let mut event = S3CompatEvent::new(
        "ListObjectsV2",
        "https://s3.bj.bcebos.com",
        "missing-bucket",
        "/",
    );
    event.http_status = 404;
    event.s3_error_code = Some("NoSuchBucket".into());
    event.s3_error_message = Some("The specified bucket does not exist".into());
    event.truncated_raw_body = Some("<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>".into());
    event.fatal = true;

    let json = serde_json::to_string(&event).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["truncated_raw_body"].as_str().unwrap(),
        "<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>");
}

// ── JSONL writer writes one line per event ────────────────

#[test]
fn test_jsonl_writer_writes_lines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.jsonl");
    let path_str = path.to_str().unwrap();

    let writer = JsonlTraceWriter::new(path_str).unwrap();

    for i in 0..3 {
        let mut event = S3CompatEvent::new(&format!("Test{}", i), "http://x", "b", "/");
        event.http_status = 200;
        event.latency_ms = i as u64;
        writer.write_event(event);
    }

    let content = std::fs::read_to_string(path_str).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    assert_eq!(lines.len(), 3, "should have exactly 3 JSON lines");

    for line in &lines {
        let _: serde_json::Value =
            serde_json::from_str(line).expect("each line should be valid JSON");
    }
}

// ── Noop writer is silent ─────────────────────────────────

#[test]
fn test_noop_writer_is_silent() {
    let writer = NoopTraceWriter;
    let event = S3CompatEvent::new("HeadBucket", "http://x", "b", "/");
    writer.write_event(event); // should not panic
}

// ── create_trace_writer with file ─────────────────────────

#[test]
fn test_create_trace_writer_with_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.jsonl");
    let path_str = path.to_str().unwrap();

    let w = create_trace_writer(Some(path_str), false);

    w.write_event(S3CompatEvent::new("X", "e", "b", "/"));

    // File writer should have written a line.
    let content = std::fs::read_to_string(path_str).unwrap();
    assert!(content.contains("\"operation\":\"X\""));
}

// ── create_trace_writer with debug_s3 ─────────────────────

#[test]
fn test_create_trace_writer_debug_s3_combo() {
    // Just verify no panic with both enabled.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trace.jsonl");
    let path_str = path.to_str().unwrap();

    let w = create_trace_writer(Some(path_str), true);
    w.write_event(S3CompatEvent::new("X", "e", "b", "/"));
    // No panic = success.
}
