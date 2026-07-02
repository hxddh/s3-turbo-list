use arrow::array::{Array, StringArray, UInt8Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
}

struct MockResponse {
    status: u16,
    reason: &'static str,
    body: String,
}

impl MockResponse {
    fn ok_xml(body: String) -> Self {
        Self {
            status: 200,
            reason: "OK",
            body,
        }
    }

    fn empty_ok() -> Self {
        Self {
            status: 200,
            reason: "OK",
            body: String::new(),
        }
    }

    fn error(status: u16, code: &str, message: &str) -> Self {
        Self {
            status,
            reason: if status == 503 {
                "Service Unavailable"
            } else {
                "Error"
            },
            body: format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>{}</Message><RequestId>mock-request</RequestId></Error>"#,
                code, message
            ),
        }
    }
}

struct MockS3Server {
    addr: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockS3Server {
    fn start(
        handler: impl Fn(RecordedRequest, usize) -> MockResponse + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        listener
            .set_nonblocking(true)
            .expect("set mock server nonblocking");
        let addr = listener.local_addr().expect("mock server local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handler = Arc::new(handler);

        let thread_requests = requests.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            let mut sequence = 0usize;
            while !thread_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        sequence += 1;
                        handle_connection(stream, sequence, &thread_requests, handler.as_ref());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            addr,
            requests,
            shutdown,
            handle: Some(handle),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for MockS3Server {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    sequence: usize,
    requests: &Arc<Mutex<Vec<RecordedRequest>>>,
    handler: &(dyn Fn(RecordedRequest, usize) -> MockResponse + Send + Sync),
) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    requests.lock().unwrap().push(request.clone());
    let response = handler(request, sequence);
    write_response(&mut stream, response);
}

fn read_request(stream: &mut TcpStream) -> Option<RecordedRequest> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buf);
    let first_line = request.lines().next()?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?;
    let (path, raw_query) = target.split_once('?').unwrap_or((target, ""));
    Some(RecordedRequest {
        method,
        path: path.to_string(),
        query: parse_query(raw_query),
    })
}

fn write_response(stream: &mut TcpStream, response: MockResponse) {
    let body = response.body.as_bytes();
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nx-amz-request-id: mock-request\r\nx-amz-id-2: mock-request-2\r\nConnection: close\r\n\r\n",
        response.status,
        response.reason,
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn parse_query(raw: &str) -> BTreeMap<String, String> {
    raw.split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (percent_decode(key), percent_decode(value))
        })
        .collect()
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(decoded) = u8::from_str_radix(hex, 16) {
                    out.push(decoded);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn list_bucket_xml(
    prefix: &str,
    max_keys: i32,
    contents: &[&str],
    common_prefixes: &[&str],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    let contents_xml: String = contents
        .iter()
        .enumerate()
        .map(|(index, key)| {
            format!(
                "<Contents><Key>{}</Key><LastModified>2026-05-17T00:00:{:02}.000Z</LastModified><ETag>&quot;{:032x}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                xml_escape(key),
                index,
                index + 1,
                100 + index
            )
        })
        .collect();
    let common_prefixes_xml: String = common_prefixes
        .iter()
        .map(|prefix| {
            format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(prefix)
            )
        })
        .collect();
    let next_token_xml = next_token
        .map(|token| {
            format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                xml_escape(token)
            )
        })
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>mock-bucket</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>{}{}{}</ListBucketResult>"#,
        xml_escape(prefix),
        contents.len() + common_prefixes.len(),
        max_keys,
        if truncated { "true" } else { "false" },
        contents_xml,
        common_prefixes_xml,
        next_token_xml
    )
}

fn list_bucket_xml_without_key_count(
    prefix: &str,
    max_keys: i32,
    contents: &[&str],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    let mut xml = list_bucket_xml(prefix, max_keys, contents, &[], truncated, next_token);
    if let Some(start) = xml.find("<KeyCount>") {
        if let Some(end) = xml[start..].find("</KeyCount>") {
            let end = start + end + "</KeyCount>".len();
            xml.replace_range(start..end, "");
        }
    }
    xml
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn write_fast_config(path: &std::path::Path) {
    std::fs::write(
        path,
        r#"[s3]
max_attempts = 3
initial_backoff_secs = 0
connect_timeout_secs = 2
operation_timeout_secs = 2
"#,
    )
    .unwrap();
}

fn run_cli(args: &[String], cwd: &std::path::Path) -> (i32, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_s3-turbo-list"))
        .current_dir(cwd)
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args(args)
        .output()
        .expect("run s3-turbo-list");

    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn parquet_keys(path: &std::path::Path) -> Vec<String> {
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut keys = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..column.len() {
            keys.push(column.value(row).to_string());
        }
    }
    keys
}

/// Read the `DiffFlag` column (index 4) from a diff Parquet output.
fn parquet_diff_flags(path: &std::path::Path) -> Vec<u8> {
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut flags = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let column = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        for row in 0..column.len() {
            flags.push(column.value(row));
        }
    }
    flags
}

fn checkpoint_completed_indices(path: &std::path::Path) -> Option<Vec<u64>> {
    let content = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&content).ok()?;
    value.get("completed_indices")?.as_array().map(|items| {
        items
            .iter()
            .filter_map(toml::Value::as_integer)
            .map(|v| v as u64)
            .collect()
    })
}

#[test]
fn local_mock_list_paginates_and_records_protocol_fields() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        assert_eq!(request.path.trim_end_matches('/'), "/mock-bucket");
        assert_eq!(
            request.query.get("list-type").map(String::as_str),
            Some("2")
        );

        match request.query.get("continuation-token").map(String::as_str) {
            None => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                2,
                &["logs/a.txt", "logs/b.txt"],
                &[],
                true,
                Some("token-1"),
            )),
            Some("token-1") => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                2,
                &["logs/c.txt"],
                &["logs/archive/"],
                false,
                None,
            )),
            Some(_) => MockResponse::error(400, "InvalidToken", "unexpected continuation token"),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    let ks = dir.path().join("out.ks");
    let trace = dir.path().join("trace.jsonl");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--max-keys".into(),
        "2".into(),
        "--prefix".into(),
        "logs/".into(),
        "--delimiter".into(),
        "/".into(),
        "--trace-compat".into(),
        trace.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    assert_eq!(
        parquet_keys(&parquet),
        vec!["logs/a.txt", "logs/b.txt", "logs/c.txt"]
    );
    assert_eq!(std::fs::read_to_string(&ks).unwrap(), "\"logs\",\"3\"\n");

    let requests = server.requests();
    let list_requests: Vec<_> = requests.iter().filter(|r| r.method == "GET").collect();
    assert_eq!(list_requests.len(), 2, "{:#?}", list_requests);
    assert_eq!(
        list_requests[0].query.get("prefix").map(String::as_str),
        Some("logs/")
    );
    assert_eq!(
        list_requests[0].query.get("delimiter").map(String::as_str),
        Some("/")
    );
    assert_eq!(
        list_requests[0].query.get("max-keys").map(String::as_str),
        Some("2")
    );
    assert!(!list_requests[0].query.contains_key("continuation-token"));
    assert_eq!(
        list_requests[1]
            .query
            .get("continuation-token")
            .map(String::as_str),
        Some("token-1")
    );

    let trace_lines = std::fs::read_to_string(trace).unwrap();
    let trace_events: Vec<Value> = trace_lines
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(trace_events
        .iter()
        .any(|event| event["next_continuation_token_present"] == true));
    assert!(trace_events
        .iter()
        .any(|event| event["common_prefixes_count"] == 1));
    assert!(trace_events.iter().any(|event| {
        event["operation"] == "ListObjectsV2SegmentSummary"
            && event["segment_index"] == 0
            && event["segment_pages"] == 2
            && event["segment_objects"] == 3
            && event["segment_common_prefixes"] == 1
            && event["ended_by"] == "pagination"
    }));
}

#[test]
fn local_mock_list_empty_delimiter_omits_request_parameter() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Startup structural discovery probe — flat namespace, no
            // CommonPrefixes, so the run falls back to a single segment.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        assert!(
            !request.query.contains_key("delimiter"),
            "{:?}",
            request.query
        );
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["logs/a.txt", "logs/nested/b.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    let ks = dir.path().join("out.ks");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--delimiter".into(),
        "".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    assert_eq!(
        parquet_keys(&parquet),
        vec!["logs/a.txt", "logs/nested/b.txt"]
    );
    // The only delimiter-bearing request is the single discovery probe;
    // listing requests omit the empty delimiter entirely.
    let requests = server.requests();
    let probes: Vec<_> = requests
        .iter()
        .filter(|r| r.query.contains_key("delimiter"))
        .collect();
    assert_eq!(probes.len(), 1, "{:#?}", probes);
    assert_eq!(
        probes[0].query.get("delimiter").map(String::as_str),
        Some("/")
    );
}

#[test]
fn local_mock_list_startup_discovery_splits_segments() {
    let all_keys = ["a/1.txt", "a/x/2.txt", "b/3.txt", "c.txt"];
    let server = MockS3Server::start(move |request, _sequence| {
        assert_eq!(request.method, "GET");
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Startup structural discovery probe.
            let children: &[&str] = match request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or("")
            {
                "" => &["a/", "b/"],
                "a/" => &["a/x/"],
                _ => &[],
            };
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], children, false, None));
        }
        // Flat listing request from one of the segments.
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let keys: Vec<&str> = all_keys
            .iter()
            .copied()
            .filter(|key| *key > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &keys, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    let ks = dir.path().join("out.ks");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--delimiter".into(),
        "".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // Segments stream concurrently, so output order is not deterministic.
    let mut keys = parquet_keys(&parquet);
    keys.sort();
    assert_eq!(keys, all_keys);

    let requests = server.requests();
    let probes: Vec<_> = requests
        .iter()
        .filter(|r| r.query.contains_key("delimiter"))
        .collect();
    let lists: Vec<_> = requests
        .iter()
        .filter(|r| !r.query.contains_key("delimiter"))
        .collect();
    // BFS probes: root, a/, b/, a/x/. Boundaries a/, a/x/, b/ → 4 segments.
    assert_eq!(probes.len(), 4, "{:#?}", probes);
    assert_eq!(lists.len(), 4, "{:#?}", lists);

    // Discovered boundaries are cached for future runs (incl. --resume).
    let cache = dir.path().join("us-east-1_mock-bucket_hints.toml");
    let cache_content = std::fs::read_to_string(&cache).unwrap();
    assert!(cache_content.contains("a/x/"), "{}", cache_content);
}

#[test]
fn local_mock_list_runtime_split_covers_long_tail() {
    // A deliberately coarse hints file leaves one huge segment holding all
    // of big/*. With idle concurrency, the reactor must probe the long
    // tail, split it at a real CommonPrefix (big/b/), and a child segment
    // must list the right half — with every key emitted exactly once.
    let mut keys: Vec<String> = Vec::new();
    for i in 0..40 {
        keys.push(format!("big/a/{:02}", i));
    }
    for i in 0..20 {
        keys.push(format!("big/b/{:02}", i));
    }
    for i in 0..20 {
        keys.push(format!("big/c/{:02}", i));
    }
    keys.push("small/x".to_string());

    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        assert_eq!(request.method, "GET");
        let prefix = request.query.get("prefix").cloned().unwrap_or_default();
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();

        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Split probe: next-level dirs under `prefix` for keys after
            // `start_after`.
            let mut cps: Vec<String> = all_keys
                .iter()
                .filter(|k| k.starts_with(&prefix) && k.as_str() > start_after.as_str())
                .filter_map(|k| {
                    k[prefix.len()..]
                        .split_once('/')
                        .map(|(d, _)| format!("{}{}/", prefix, d))
                })
                .collect();
            cps.sort();
            cps.dedup();
            let cps_ref: Vec<&str> = cps.iter().map(String::as_str).collect();
            return MockResponse::ok_xml(list_bucket_xml(
                &prefix,
                1000,
                &[],
                &cps_ref,
                false,
                None,
            ));
        }

        // Flat listing with offset-encoded continuation tokens, 2 keys per
        // page, slowed down so the long-tail segment is still running when
        // the reactor's split check fires.
        std::thread::sleep(Duration::from_millis(80));
        let start_idx = match request.query.get("continuation-token") {
            Some(token) => token
                .strip_prefix("off-")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(0),
            None => all_keys.partition_point(|k| k.as_str() <= start_after.as_str()),
        };
        let page: Vec<&str> = all_keys[start_idx..]
            .iter()
            .take(2)
            .map(String::as_str)
            .collect();
        let truncated = start_idx + page.len() < all_keys.len();
        let token = truncated.then(|| format!("off-{}", start_idx + page.len()));
        MockResponse::ok_xml(list_bucket_xml(
            &prefix,
            2,
            &page,
            &[],
            truncated,
            token.as_deref(),
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let hints = dir.path().join("hints.txt");
    let parquet = dir.path().join("out.parquet");
    let ks = dir.path().join("out.ks");
    write_fast_config(&config);
    std::fs::write(&hints, "small/\n").unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--delimiter".into(),
        "".into(),
        "--max-keys".into(),
        "2".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // Every key exactly once, regardless of which segment emitted it.
    let mut listed = parquet_keys(&parquet);
    listed.sort();
    assert_eq!(listed, keys);

    let requests = server.requests();
    let probes: Vec<_> = requests
        .iter()
        .filter(|r| r.query.contains_key("delimiter"))
        .collect();
    assert!(
        probes
            .iter()
            .any(|r| r.query.get("prefix").map(String::as_str) == Some("big/")),
        "expected a split probe under big/: {:#?}",
        probes
    );
    // A child segment starts listing from the accepted cut (which of the
    // candidate prefixes wins depends on probe timing).
    assert!(
        requests.iter().any(|r| {
            !r.query.contains_key("delimiter")
                && matches!(
                    r.query.get("start-after").map(String::as_str),
                    Some("big/b/") | Some("big/c/")
                )
        }),
        "expected a child segment starting at big/b/ or big/c/"
    );
}

#[test]
fn local_mock_list_flat_namespace_runtime_split() {
    // A flat namespace (no '/' anywhere): startup discovery finds no
    // structure, so the run starts single-segment. The flat-cut fallback
    // must derive a cut from the cursor (max_keys=1 probe returning a real
    // key) and fan out a child segment — with every key emitted once.
    // 240 keys at ~80ms/page gives the reactor a wide window (~10s) to
    // probe and split even on slow, contended CI runners.
    let keys: Vec<String> = (0..240).map(|i| format!("obj-{:04}", i)).collect();

    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        assert_eq!(request.method, "GET");
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();

        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Discovery / ladder probes: flat namespace, no CommonPrefixes.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        if request.query.get("max-keys").map(String::as_str) == Some("1") {
            // Flat-cut probe: first real key after the candidate, no delay.
            let first: Vec<&str> = all_keys
                .iter()
                .find(|k| k.as_str() > start_after.as_str())
                .map(|k| vec![k.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }

        // Flat listing, 2 keys per page, slowed so the segment is still
        // running when the reactor's split check fires.
        std::thread::sleep(Duration::from_millis(80));
        let start_idx = match request.query.get("continuation-token") {
            Some(token) => token
                .strip_prefix("off-")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(0),
            None => all_keys.partition_point(|k| k.as_str() <= start_after.as_str()),
        };
        let page: Vec<&str> = all_keys[start_idx..]
            .iter()
            .take(2)
            .map(String::as_str)
            .collect();
        let truncated = start_idx + page.len() < all_keys.len();
        let token = truncated.then(|| format!("off-{}", start_idx + page.len()));
        MockResponse::ok_xml(list_bucket_xml(
            "",
            2,
            &page,
            &[],
            truncated,
            token.as_deref(),
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    let ks = dir.path().join("out.ks");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--max-keys".into(),
        "2".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let mut listed = parquet_keys(&parquet);
    listed.sort();
    assert_eq!(listed, keys);

    let requests = server.requests();
    assert!(
        requests
            .iter()
            .any(|r| r.query.get("max-keys").map(String::as_str) == Some("1")),
        "expected at least one flat-cut probe (max-keys=1)"
    );
    // A child segment starts listing from a real-key cut.
    assert!(
        requests.iter().any(|r| {
            !r.query.contains_key("delimiter")
                && r.query.get("max-keys").map(String::as_str) != Some("1")
                && r.query
                    .get("start-after")
                    .is_some_and(|sa| sa.starts_with("obj-"))
        }),
        "expected a child segment starting at a flat cut"
    );
}

#[test]
fn local_mock_summary_only_reports_metrics_without_outputs() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        match request.query.get("continuation-token").map(String::as_str) {
            None => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                2,
                &["logs/a.txt", "logs/b.txt"],
                &[],
                true,
                Some("token-1"),
            )),
            Some("token-1") => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                2,
                &["images/c.jpg"],
                &[],
                false,
                None,
            )),
            Some(_) => MockResponse::error(400, "InvalidToken", "unexpected continuation token"),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--max-keys".into(),
        "2".into(),
        "--summary-only".into(),
        "--agent".into(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let manifest: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(manifest["metrics"]["summary_only"], true);
    assert_eq!(manifest["metrics"]["received_objects"], 3);
    assert_eq!(manifest["metrics"]["streamed_rows"], 3);
    assert_eq!(manifest["metrics"]["parquet_rows"], 0);
    assert_eq!(manifest["metrics"]["ks_entries"], 0);
    assert_eq!(manifest["metrics"]["bytes_total"], 301);
    assert_eq!(manifest["outputs"]["parquet_file"], Value::Null);
    assert_eq!(manifest["outputs"]["ks_file"], Value::Null);
    assert!(manifest["artifacts"].as_array().unwrap().is_empty());
    assert!(!dir.path().join("out.parquet").exists());
    assert!(!dir.path().join("out.ks").exists());
}

#[test]
fn local_mock_list_tsv_streams_rows_to_stdout_without_artifacts() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        match request.query.get("continuation-token").map(String::as_str) {
            None => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                2,
                &["logs/a.txt", "logs/b.txt"],
                &[],
                true,
                Some("token-1"),
            )),
            Some("token-1") => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                2,
                &["images/c.jpg"],
                &[],
                false,
                None,
            )),
            Some(_) => MockResponse::error(400, "InvalidToken", "unexpected continuation token"),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let manifest = dir.path().join("run.json");

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--max-keys".into(),
        "2".into(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        "--output-format".into(),
        "tsv".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        stderr.contains("--output-format tsv/ndjson streams list rows to stdout"),
        "stderr should include pipe-output warning: {}",
        stderr
    );

    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "stdout should contain only TSV rows");
    let first: Vec<_> = lines[0].split('\t').collect();
    assert_eq!(first.len(), 3);
    assert_eq!(first[0], "logs/a.txt");
    assert_eq!(first[1], "100");
    assert!(first[2].parse::<u64>().unwrap() > 0);
    assert!(lines
        .iter()
        .any(|line| line.starts_with("images/c.jpg\t100\t")));

    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(manifest_json["metrics"]["streamed_rows"], 3);
    assert_eq!(manifest_json["metrics"]["parquet_rows"], 0);
    assert_eq!(manifest_json["metrics"]["ks_entries"], 0);
    assert_eq!(manifest_json["metrics"]["summary_only"], false);
    assert_eq!(manifest_json["metrics"]["bytes_total"], 301);
    assert_eq!(manifest_json["inputs"]["output_format"], "tsv");
    assert_eq!(manifest_json["outputs"]["parquet_file"], Value::Null);
    assert_eq!(manifest_json["outputs"]["ks_file"], Value::Null);
    assert!(manifest_json["artifacts"].as_array().unwrap().is_empty());
}

#[test]
fn local_mock_list_tsv_escapes_control_chars_and_preserves_rows() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &[
                "plain.txt",
                "tab\tkey.txt",
                "line\nkey.txt",
                "slash\\key.txt",
            ],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        "--output-format".into(),
        "tsv".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines.len(), 4);
    assert!(lines
        .iter()
        .any(|line| line.starts_with("plain.txt\t100\t")));
    assert!(lines
        .iter()
        .any(|line| line.starts_with("tab\\tkey.txt\t101\t")));
    assert!(lines
        .iter()
        .any(|line| line.starts_with("line\\nkey.txt\t102\t")));
    assert!(lines
        .iter()
        .any(|line| line.starts_with("slash\\\\key.txt\t103\t")));
}

#[test]
fn local_mock_list_ndjson_streams_parseable_rows_and_manifest_summary_reads_it() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["logs/a.txt", "logs/b.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let manifest = dir.path().join("run.json");

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        "--output-format".into(),
        "ndjson".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let rows: Vec<Value> = stdout
        .lines()
        .inspect(|line| assert!(!line.is_empty()))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["k"], "logs/a.txt");
    assert_eq!(rows[0]["s"], 100);
    assert!(rows[0]["m"].as_u64().unwrap() > 0);

    let summary_args = vec![
        "manifest-summary".into(),
        manifest.display().to_string(),
        "--json".into(),
    ];
    let (code, summary_stdout, summary_stderr) = run_cli(&summary_args, dir.path());
    assert_eq!(
        code, 0,
        "stdout: {}\nstderr: {}",
        summary_stdout, summary_stderr
    );
    let summary: Value = serde_json::from_str(&summary_stdout).unwrap();
    assert_eq!(summary["status"], "success");
    assert_eq!(summary["run_status"], "success");
    assert_eq!(summary["streamed_rows"], 2);
    assert_eq!(summary["parquet_rows"], 0);
    assert_eq!(summary["bytes_total"], 201);
    assert_eq!(summary["outputs"]["parquet_file"], Value::Null);
}

#[test]
fn local_mock_list_stdout_formats_emit_no_blank_rows_for_empty_results() {
    for format in ["tsv", "ndjson"] {
        let server = MockS3Server::start(|request, _sequence| {
            assert_eq!(request.method, "GET");
            MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                1000,
                &[],
                &[],
                false,
                None,
            ))
        });

        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        write_fast_config(&config);

        let args = vec![
            "--config".into(),
            config.display().to_string(),
            "--endpoint-url".into(),
            server.endpoint(),
            "--addressing-style".into(),
            "path".into(),
            "list".into(),
            "--bucket".into(),
            "mock-bucket".into(),
            "--region".into(),
            "us-east-1".into(),
            "--output-format".into(),
            format.into(),
        ];
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
        assert!(stdout.is_empty(), "{format} should not emit blank rows");
    }
}

#[test]
fn local_mock_list_uses_initial_continuation_token_for_single_chain() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        assert_eq!(
            request.query.get("continuation-token").map(String::as_str),
            Some("seed-token")
        );
        assert!(!request.query.contains_key("start-after"));
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["logs/resumed.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let manifest = dir.path().join("run.json");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--no-auto-hints".into(),
        "--continuation-token".into(),
        "seed-token".into(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        "--output-format".into(),
        "ndjson".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let rows: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["k"], "logs/resumed.txt");

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]
            .query
            .get("continuation-token")
            .map(String::as_str),
        Some("seed-token")
    );

    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(
        manifest_json["config_source"]["loaded_config"],
        config.to_str().unwrap()
    );
    assert_eq!(
        manifest_json["config_source"]["loaded_config_kind"],
        "explicit"
    );
    assert!(manifest_json["command"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "--endpoint-url"));
    assert!(!manifest_json["command"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value.as_str() == Some(server.endpoint().as_str())));
    assert!(!manifest_json["command"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value.as_str() == Some("seed-token")));
    assert!(manifest_json["config_source"]["cli_overrides"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "endpoint_url"));
    assert_eq!(manifest_json["inputs"]["continuation_token"], "seed-token");
    assert_eq!(manifest_json["metrics"]["streamed_rows"], 1);
}

#[test]
fn local_mock_compat_probe_covers_head_list_and_pagination() {
    let server = MockS3Server::start(|request, _sequence| match request.method.as_str() {
        "HEAD" => MockResponse::empty_ok(),
        "GET" => match request.query.get("continuation-token").map(String::as_str) {
            Some("probe-page-2") => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                3,
                &["probe/d.txt"],
                &[],
                false,
                None,
            )),
            _ if request.query.get("max-keys").map(String::as_str) == Some("3") => {
                MockResponse::ok_xml(list_bucket_xml(
                    request
                        .query
                        .get("prefix")
                        .map(String::as_str)
                        .unwrap_or(""),
                    3,
                    &["probe/a.txt", "probe/b.txt", "probe/c.txt"],
                    &[],
                    true,
                    Some("probe-page-2"),
                ))
            }
            _ => MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                1,
                &["probe/a.txt"],
                &[],
                false,
                None,
            )),
        },
        _ => MockResponse::error(405, "MethodNotAllowed", "unexpected method"),
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let report = dir.path().join("compat.json");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "compat-probe".into(),
        "--endpoint".into(),
        server.endpoint(),
        "--region".into(),
        "us-east-1".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--addressing-style".into(),
        "path".into(),
        "--output".into(),
        report.display().to_string(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let report: Value = serde_json::from_str(&std::fs::read_to_string(report).unwrap()).unwrap();
    assert_eq!(report["overall_status"], "compatible");
    assert!(report["tests"].as_array().unwrap().iter().any(|test| {
        test["test"] == "ListObjectsV2 pagination check" && test["status"] == "ok"
    }));

    let requests = server.requests();
    assert!(requests.iter().any(|request| request.method == "HEAD"));
    assert!(requests.iter().any(|request| {
        request.method == "GET"
            && request.query.get("encoding-type").map(String::as_str) == Some("url")
    }));
    assert!(requests.iter().any(|request| {
        request.query.get("continuation-token").map(String::as_str) == Some("probe-page-2")
    }));
}

#[test]
fn local_mock_compat_probe_reports_s3_error_metadata() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(501, "NotImplemented", "delimiter is not supported")
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let report = dir.path().join("compat-errors.json");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "compat-probe".into(),
        "--endpoint".into(),
        server.endpoint(),
        "--region".into(),
        "us-east-1".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--addressing-style".into(),
        "path".into(),
        "--output".into(),
        report.display().to_string(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let report: Value = serde_json::from_str(&std::fs::read_to_string(report).unwrap()).unwrap();
    assert_eq!(report["overall_status"], "incompatible");
    let tests = report["tests"].as_array().unwrap();
    assert!(tests.iter().all(|test| test["status"] == "error"));
    let service_error = tests
        .iter()
        .find(|test| test["s3_error_code"] == "NotImplemented")
        .expect("compat-probe should expose a modeled S3 service error");
    assert_eq!(service_error["status"], "error");
    assert_eq!(service_error["http_status"], 501);
    assert_eq!(service_error["s3_error_code"], "NotImplemented");
    assert_eq!(service_error["error_kind"], "service");
    assert_eq!(service_error["diagnostic_code"], "operation_not_supported");
    assert!(service_error["recommendation"]
        .as_str()
        .unwrap()
        .contains("does not implement"));
    assert_eq!(service_error["request_id"], "mock-request");
    assert_eq!(service_error["request_id_2"], "mock-request-2");
    assert!(service_error
        .as_object()
        .unwrap()
        .contains_key("error_message"));
}

#[test]
fn local_mock_compat_probe_output_write_failure_exits_without_panic() {
    let server = MockS3Server::start(|request, _sequence| match request.method.as_str() {
        "HEAD" => MockResponse::empty_ok(),
        "GET" => MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1,
            &["probe/a.txt"],
            &[],
            false,
            None,
        )),
        _ => MockResponse::error(405, "MethodNotAllowed", "unexpected method"),
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let not_a_dir = dir.path().join("not-a-dir");
    let report = not_a_dir.join("compat.json");
    std::fs::write(&not_a_dir, "file blocks directory creation").unwrap();
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "compat-probe".into(),
        "--endpoint".into(),
        server.endpoint(),
        "--region".into(),
        "us-east-1".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--addressing-style".into(),
        "path".into(),
        "--output".into(),
        report.display().to_string(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 5, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.is_empty());
    assert!(stderr.contains("Compat-probe output error:"));
    assert!(!stderr.contains("panicked"));
}

#[test]
fn local_mock_resume_keeps_original_segment_start_after() {
    let server = MockS3Server::start(|request, _sequence| {
        if request.query.get("start-after").map(String::as_str) != Some("m/") {
            return MockResponse::error(
                500,
                "UnexpectedSegment",
                "resume should only request the uncompleted m/ segment",
            );
        }
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["z-last.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let hints = dir.path().join("hints.toml");
    let checkpoint = dir.path().join("us-east-1_mock-bucket_checkpoint.toml");
    let parquet = dir.path().join("resume.parquet");
    let ks = dir.path().join("resume.ks");
    write_fast_config(&config);
    std::fs::write(
        &hints,
        r#"bucket = "mock-bucket"
region = "us-east-1"
total_objects = 2
boundaries = ["m/"]
generated_at = "2026-05-17T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();
    std::fs::write(
        &checkpoint,
        r#"bucket = "mock-bucket"
prefix = ""
total_segments = 2
completed_indices = [0]
last_updated = "2026-05-17T00:00:00Z"

[identity]
bucket = "mock-bucket"
region = "us-east-1"
prefix = ""
delimiter = ""
addressing_style = "path"
mode = "list"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--resume".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(parquet_keys(&parquet), vec!["z-last.txt"]);

    let requests = server.requests();
    assert_eq!(requests.len(), 1, "{:#?}", requests);
    assert_eq!(
        requests[0].query.get("start-after").map(String::as_str),
        Some("m/")
    );

    let checkpoint_after = std::fs::read_to_string(&checkpoint).unwrap();
    assert!(
        checkpoint_after.contains("completed_indices = [\n    0,\n    1,\n]")
            || checkpoint_after.contains("completed_indices = [0, 1]"),
        "{}",
        checkpoint_after
    );
}

#[test]
fn local_mock_resume_does_not_mark_failed_segment_completed() {
    let server = MockS3Server::start(|request, _sequence| {
        if request.query.get("start-after").map(String::as_str) == Some("m/") {
            return MockResponse::error(500, "InjectedFailure", "segment should fail");
        }
        MockResponse::error(
            500,
            "UnexpectedSegment",
            "resume should only request the uncompleted m/ segment",
        )
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let hints = dir.path().join("hints.toml");
    let checkpoint = dir.path().join("us-east-1_mock-bucket_checkpoint.toml");
    let parquet = dir.path().join("failed-segment.parquet");
    let ks = dir.path().join("failed-segment.ks");
    write_fast_config(&config);
    std::fs::write(
        &hints,
        r#"bucket = "mock-bucket"
region = "us-east-1"
total_objects = 2
boundaries = ["m/"]
generated_at = "2026-05-24T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();
    std::fs::write(
        &checkpoint,
        r#"bucket = "mock-bucket"
prefix = ""
total_segments = 2
completed_indices = [0]
last_updated = "2026-05-24T00:00:00Z"

[identity]
bucket = "mock-bucket"
region = "us-east-1"
prefix = ""
delimiter = ""
addressing_style = "path"
mode = "list"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--resume".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_ne!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(checkpoint_completed_indices(&checkpoint), Some(vec![0]));

    let requests = server.requests();
    assert!(
        requests
            .iter()
            .any(|request| { request.query.get("start-after").map(String::as_str) == Some("m/") }),
        "{:#?}",
        requests
    );
}

#[test]
fn local_mock_resume_skips_final_checkpoint_when_output_write_fails() {
    let server = MockS3Server::start(|request, _sequence| {
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["logs/a.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let checkpoint = dir.path().join("us-east-1_mock-bucket_checkpoint.toml");
    let bad_parquet_path = dir.path().join("parquet-is-directory");
    let ks = dir.path().join("output-failure.ks");
    write_fast_config(&config);
    std::fs::create_dir(&bad_parquet_path).unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--resume".into(),
        "--no-auto-hints".into(),
        "--output-parquet-file".into(),
        bad_parquet_path.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 5, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        !checkpoint.exists(),
        "failed output should not create a completed checkpoint"
    );
}

#[test]
fn local_mock_resume_on_error_advances_without_key_count() {
    let token_error_count = Arc::new(AtomicUsize::new(0));
    let handler_token_error_count = token_error_count.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        if request.query.get("continuation-token").map(String::as_str) == Some("token-1") {
            handler_token_error_count.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_secs(3));
            return MockResponse::ok_xml(list_bucket_xml(
                "",
                1000,
                &["timeout-late.txt"],
                &[],
                false,
                None,
            ));
        }
        if request.query.get("start-after").map(String::as_str) == Some("logs/b.txt") {
            return MockResponse::ok_xml(list_bucket_xml(
                request
                    .query
                    .get("prefix")
                    .map(String::as_str)
                    .unwrap_or(""),
                1000,
                &["logs/c.txt"],
                &[],
                false,
                None,
            ));
        }
        if request.query.contains_key("start-after") {
            return MockResponse::error(
                500,
                "UnexpectedStartAfter",
                "retry should resume from the last processed key",
            );
        }
        MockResponse::ok_xml(list_bucket_xml_without_key_count(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            2,
            &["logs/a.txt", "logs/b.txt"],
            true,
            Some("token-1"),
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("resume-no-key-count.parquet");
    let ks = dir.path().join("resume-no-key-count.ks");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        token_error_count.load(Ordering::SeqCst) > 0,
        "mock should force an error on the token page"
    );
    assert_eq!(
        parquet_keys(&parquet),
        vec!["logs/a.txt", "logs/b.txt", "logs/c.txt"]
    );

    let requests = server.requests();
    assert!(
        requests.iter().any(|request| {
            request.query.get("start-after").map(String::as_str) == Some("logs/b.txt")
                && !request.query.contains_key("continuation-token")
        }),
        "{:#?}",
        requests
    );
}

#[test]
fn local_mock_segment_boundary_key_is_not_dropped() {
    let server = MockS3Server::start(|request, _sequence| {
        let start_after = request.query.get("start-after").map(String::as_str);
        let contents = match start_after {
            None => vec!["a.txt", "m/"],
            Some("m/") => vec!["z.txt"],
            Some(other) => {
                return MockResponse::error(
                    500,
                    "UnexpectedStartAfter",
                    &format!("unexpected start-after {}", other),
                );
            }
        };
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &contents,
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let hints = dir.path().join("hints.toml");
    let parquet = dir.path().join("boundary.parquet");
    let ks = dir.path().join("boundary.ks");
    write_fast_config(&config);
    std::fs::write(
        &hints,
        r#"bucket = "mock-bucket"
region = "us-east-1"
total_objects = 3
boundaries = ["m/"]
generated_at = "2026-05-18T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let mut keys = parquet_keys(&parquet);
    keys.sort();
    assert_eq!(keys, vec!["a.txt", "m/", "z.txt"]);
}

#[test]
fn local_mock_multi_segment_boundaries_include_boundary_keys() {
    let server = MockS3Server::start(|request, _sequence| {
        assert!(
            !request.query.contains_key("delimiter"),
            "{:?}",
            request.query
        );
        let start_after = request.query.get("start-after").map(String::as_str);
        let contents = match start_after {
            None => vec!["a.txt", "m/", "n.txt"],
            Some("m/") => vec!["n.txt", "t/", "u.txt"],
            Some("t/") => vec!["z.txt"],
            Some(other) => {
                return MockResponse::error(
                    500,
                    "UnexpectedStartAfter",
                    &format!("unexpected start-after {}", other),
                );
            }
        };
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &contents,
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let hints = dir.path().join("hints.toml");
    let parquet = dir.path().join("multi-boundary.parquet");
    let ks = dir.path().join("multi-boundary.ks");
    write_fast_config(&config);
    std::fs::write(
        &hints,
        r#"bucket = "mock-bucket"
region = "us-east-1"
total_objects = 5
boundaries = ["m/", "t/"]
generated_at = "2026-05-18T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--delimiter".into(),
        "".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let mut keys = parquet_keys(&parquet);
    keys.sort();
    assert_eq!(keys, vec!["a.txt", "m/", "n.txt", "t/", "z.txt"]);
}

#[test]
fn local_mock_no_auto_hints_skips_conventional_cache() {
    let server = MockS3Server::start(|request, _sequence| {
        if request.query.contains_key("start-after") {
            return MockResponse::error(
                500,
                "UnexpectedHints",
                "--no-auto-hints should force single-segment listing",
            );
        }
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["single-segment.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("no-auto.parquet");
    let ks = dir.path().join("no-auto.ks");
    write_fast_config(&config);
    std::fs::write(
        dir.path().join("us-east-1_mock-bucket_hints.toml"),
        r#"bucket = "mock-bucket"
region = "us-east-1"
total_objects = 2
boundaries = ["m/"]
generated_at = "2026-05-18T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--no-auto-hints".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(parquet_keys(&parquet), vec!["single-segment.txt"]);

    let requests = server.requests();
    assert_eq!(requests.len(), 1, "{:#?}", requests);
    assert!(!requests[0].query.contains_key("start-after"));
}

#[test]
fn local_mock_diff_lists_sides_in_parallel_segments() {
    // diff partitions each side automatically. The left side has a cached
    // hints boundary ("m/") and lists two segments; the right side has no
    // cache and a flat namespace, so structural discovery finds nothing and
    // the flat-cut bisection (max-keys=1 probes) partitions it instead — so it
    // also lists in parallel rather than as one serial segment. The merge must
    // classify across both sides' segment boundaries with every key exactly
    // once.
    let left_keys = ["a.txt", "left-only.txt", "z-extra.txt"];
    let right_keys = ["a.txt", "right-only.txt", "z-extra.txt"];

    let server = MockS3Server::start(move |request, _sequence| {
        let bucket_keys: &[&str] = if request.path.contains("/left") {
            &left_keys
        } else if request.path.contains("/right") {
            &right_keys
        } else {
            return MockResponse::error(500, "UnexpectedBucket", &request.path);
        };
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Right-side startup discovery: flat namespace, no structure.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let contents: Vec<&str> = bucket_keys
            .iter()
            .copied()
            .filter(|k| *k > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &contents, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("diff.parquet");
    let ks = dir.path().join("diff.ks");
    write_fast_config(&config);
    // Cached hints partition the left side at "m/".
    std::fs::write(
        dir.path().join("us-east-1_left_hints.toml"),
        r#"bucket = "left"
region = "us-east-1"
total_objects = 3
boundaries = ["m/"]
generated_at = "2026-05-18T00:00:00Z"
scan_mode = "full"
estimate_mode = "full"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "diff".into(),
        "--bucket".into(),
        "left".into(),
        "--region".into(),
        "us-east-1".into(),
        "--target-bucket".into(),
        "right".into(),
        "--target-region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let mut keys = parquet_keys(&parquet);
    keys.sort();
    assert_eq!(
        keys,
        vec!["a.txt", "left-only.txt", "right-only.txt", "z-extra.txt"]
    );

    let requests = server.requests();
    // Left side lists two segments: one chain from the start, one from m/.
    let left_lists: Vec<_> = requests
        .iter()
        .filter(|r| r.path.contains("/left") && !r.query.contains_key("delimiter"))
        .collect();
    assert_eq!(left_lists.len(), 2, "{:#?}", left_lists);
    assert!(left_lists
        .iter()
        .any(|r| !r.query.contains_key("start-after")));
    assert!(left_lists
        .iter()
        .any(|r| r.query.get("start-after").map(String::as_str) == Some("m/")));
    // Right side: structural discovery probe, then flat-cut bisection probes
    // (max-keys=1) that partition the flat namespace for parallel listing.
    assert!(requests
        .iter()
        .any(|r| r.path.contains("/right") && r.query.contains_key("delimiter")));
    assert!(
        requests.iter().any(|r| r.path.contains("/right")
            && r.query.get("max-keys").map(String::as_str) == Some("1")),
        "expected right-side flat-cut probes to partition the flat namespace",
    );
}

#[test]
fn local_mock_diff_identical_sides_classify_all_equal() {
    // Diffing two buckets with identical contents must classify every row as
    // equal (DiffFlag = 0): the ordered merge pairs each key with its twin and
    // none are dropped or mis-flagged.
    let keys = ["a.txt", "m/b.txt", "z.txt"];

    let server = MockS3Server::start(move |request, _sequence| {
        if !request.path.contains("/left") && !request.path.contains("/right") {
            return MockResponse::error(500, "UnexpectedBucket", &request.path);
        }
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Flat namespace: structural discovery finds no CommonPrefixes.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let contents: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|k| *k > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &contents, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("diff.parquet");
    let ks = dir.path().join("diff.ks");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "diff".into(),
        "--bucket".into(),
        "left".into(),
        "--region".into(),
        "us-east-1".into(),
        "--target-bucket".into(),
        "right".into(),
        "--target-region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let mut got = parquet_keys(&parquet);
    got.sort();
    assert_eq!(got, vec!["a.txt", "m/b.txt", "z.txt"]);

    let flags = parquet_diff_flags(&parquet);
    assert_eq!(flags.len(), 3, "every key should appear exactly once");
    assert!(
        flags.iter().all(|&f| f == 0),
        "identical sides must all classify as equal, got {:?}",
        flags
    );
}

#[test]
fn local_mock_sdk_retries_transient_list_error() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        assert_eq!(request.method, "GET");
        let attempt = handler_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            return MockResponse::error(503, "SlowDown", "retry this request");
        }
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["retry/succeeded.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("retry.parquet");
    let ks = dir.path().join("retry.ks");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "SDK should retry the initial 503 SlowDown"
    );
    assert_eq!(parquet_keys(&parquet), vec!["retry/succeeded.txt"]);
}

// ── --start-after single-chain guarantees ──────────────────
//
// A cached conventional hints file must not fan a --start-after run out into
// multiple segments: every segment would override its start with the CLI key
// and list overlapping ranges, duplicating output rows. The cache is skipped
// (single chain); explicit --hints-file and --resume are rejected up front.

#[test]
fn local_mock_start_after_ignores_cached_hints_and_lists_single_chain() {
    // Real-S3 semantics: sorted keys, honor start-after, single page.
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let keys: Vec<&str> = ["a.txt", "b.txt", "m/x.txt", "z.txt"]
            .iter()
            .copied()
            .filter(|k| *k > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &keys, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    let ks = dir.path().join("out.ks");
    write_fast_config(&config);
    // Conventional startup-discovery cache (cwd-relative), as written by a
    // previous run of the same bucket: two segments split at "m/".
    std::fs::write(
        dir.path().join("us-east-1_mock-bucket_hints.toml"),
        r#"bucket = "mock-bucket"
region = "us-east-1"
boundaries = ["m/"]
generated_at = "2026-05-17T00:00:00Z"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--start-after".into(),
        "a.txt".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--output-ks-file".into(),
        ks.display().to_string(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // Every key after a.txt exactly once — no duplicates from overlapping
    // segments, no keys dropped.
    assert_eq!(parquet_keys(&parquet), vec!["b.txt", "m/x.txt", "z.txt"]);

    // Single chain: one ListObjectsV2 request, starting after the CLI key.
    let requests = server.requests();
    assert_eq!(requests.len(), 1, "{:#?}", requests);
    assert_eq!(
        requests[0].query.get("start-after").map(String::as_str),
        Some("a.txt")
    );
}

#[test]
fn local_mock_start_after_with_hints_file_is_rejected() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(500, "Unexpected", "validation must fail before any request")
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let hints = dir.path().join("hints.toml");
    write_fast_config(&config);
    std::fs::write(
        &hints,
        r#"bucket = "mock-bucket"
region = "us-east-1"
boundaries = ["m/"]
generated_at = "2026-05-17T00:00:00Z"
"#,
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--start-after".into(),
        "a.txt".into(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        stderr.contains("--start-after is single-chain only"),
        "{}",
        stderr
    );
    assert!(server.requests().is_empty());
}

#[test]
fn local_mock_start_after_with_resume_is_rejected() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(500, "Unexpected", "validation must fail before any request")
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "--resume".into(),
        "--start-after".into(),
        "a.txt".into(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 2, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        stderr.contains("--start-after cannot be combined with --resume"),
        "{}",
        stderr
    );
    assert!(server.requests().is_empty());
}
