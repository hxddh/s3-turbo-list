mod common;

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
    drop_connection: bool,
}

impl MockResponse {
    fn ok_xml(body: String) -> Self {
        Self {
            status: 200,
            reason: "OK",
            body,
            drop_connection: false,
        }
    }

    fn empty_ok() -> Self {
        Self {
            status: 200,
            reason: "OK",
            body: String::new(),
            drop_connection: false,
        }
    }

    /// Close the TCP connection without writing a response, simulating a
    /// connection-level failure (the SDK surfaces a DispatchFailure).
    fn drop_connection() -> Self {
        Self {
            status: 0,
            reason: "",
            body: String::new(),
            drop_connection: true,
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
            drop_connection: false,
        }
    }
}

/// Concurrency the mock actually served, so a test can assert that segments
/// overlapped rather than merely that the right requests were sent.
#[derive(Default)]
struct ServedConcurrency {
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl ServedConcurrency {
    fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

struct MockS3Server {
    addr: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    concurrency: Arc<ServedConcurrency>,
    /// First panic raised inside a handler. Handlers run on connection
    /// threads, where a panic would otherwise surface only as a connection
    /// reset in the client — an assertion failure must not read as a network
    /// error, so it is re-raised when the server is dropped.
    handler_panic: Arc<Mutex<Option<String>>>,
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

        let concurrency = Arc::new(ServedConcurrency::default());
        let handler_panic: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        let thread_requests = requests.clone();
        let thread_shutdown = shutdown.clone();
        let thread_concurrency = Arc::clone(&concurrency);
        let thread_panic = Arc::clone(&handler_panic);
        // One thread per connection: the client opens a connection per request
        // (responses close it), so serving them serially would make every
        // parallel listing look sequential — no test could tell a run that
        // fans out from one that does not, and handler-side latency could not
        // model a slow range.
        let handle = thread::spawn(move || {
            let mut sequence = 0usize;
            let mut workers: Vec<thread::JoinHandle<()>> = Vec::new();
            while !thread_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        sequence += 1;
                        let seq = sequence;
                        let requests = Arc::clone(&thread_requests);
                        let handler = Arc::clone(&handler);
                        let concurrency = Arc::clone(&thread_concurrency);
                        let panic_slot = Arc::clone(&thread_panic);
                        workers.push(thread::spawn(move || {
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    handle_connection(
                                        stream,
                                        seq,
                                        &requests,
                                        handler.as_ref(),
                                        &concurrency,
                                    )
                                }));
                            if let Err(payload) = result {
                                let message = payload
                                    .downcast_ref::<&str>()
                                    .map(|s| (*s).to_string())
                                    .or_else(|| payload.downcast_ref::<String>().cloned())
                                    .unwrap_or_else(|| "<non-string panic>".to_string());
                                let mut slot = panic_slot.lock().unwrap();
                                if slot.is_none() {
                                    *slot = Some(message);
                                }
                            }
                        }));
                        workers.retain(|worker| !worker.is_finished());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
            for worker in workers {
                let _ = worker.join();
            }
        });

        Self {
            addr,
            requests,
            shutdown,
            handle: Some(handle),
            concurrency,
            handler_panic,
        }
    }

    /// Highest number of requests the mock served at the same instant.
    fn max_in_flight(&self) -> usize {
        self.concurrency.max_in_flight.load(Ordering::SeqCst)
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
        // Re-raise a handler assertion on the test thread; on a connection
        // thread it would have reached the client as a reset socket.
        if !thread::panicking()
            && let Some(message) = self.handler_panic.lock().unwrap().take()
        {
            panic!("mock handler panicked: {}", message);
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    sequence: usize,
    requests: &Arc<Mutex<Vec<RecordedRequest>>>,
    handler: &(dyn Fn(RecordedRequest, usize) -> MockResponse + Send + Sync),
    concurrency: &ServedConcurrency,
) {
    // The listener is non-blocking, and on macOS (BSD) an accepted socket
    // inherits O_NONBLOCK. A read that raced ahead of the client's bytes then
    // failed with WouldBlock and the connection was dropped unrecorded — the
    // client saw a transport error the endpoint never sent (e.g. a startup
    // discovery probe "failing", so the run fell back to bisection).
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    requests.lock().unwrap().push(request.clone());
    // Count a request as in flight only while its response is being
    // produced. Counting the whole connection thread let a strictly serial
    // client overlap itself: it sends the next request as soon as it has the
    // previous response, possibly before that connection's thread returned.
    concurrency.enter();
    let response = handler(request, sequence);
    concurrency.leave();
    if response.drop_connection {
        let _ = stream.shutdown(std::net::Shutdown::Both);
        return;
    }
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
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3])
            && let Ok(decoded) = u8::from_str_radix(hex, 16)
        {
            out.push(decoded);
            i += 3;
            continue;
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
                "<Contents><Key>{}</Key><LastModified>2026-05-17T00:{:02}:{:02}.000Z</LastModified><ETag>&quot;{:032x}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                xml_escape(key),
                // Keep the timestamp valid for pages of any size (index used
                // directly as seconds broke pages past 60 entries).
                (index / 60) % 60,
                index % 60,
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
    if let Some(start) = xml.find("<KeyCount>")
        && let Some(end) = xml[start..].find("</KeyCount>")
    {
        let end = start + end + "</KeyCount>".len();
        xml.replace_range(start..end, "");
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
    run_cli_with_env(args, cwd, &[])
}

/// Run the CLI in `cwd` (also its `HOME`), isolated from the developer's
/// config, AWS profile and proxy (`common::hermetic_command`: an inherited
/// proxy would reroute requests meant for the local mock), with mock
/// credentials plus `extra_env`.
fn run_cli_with_env(
    args: &[String],
    cwd: &std::path::Path,
    extra_env: &[(&str, &str)],
) -> (i32, String, String) {
    let mut command = common::hermetic_command(cwd);
    command.envs(extra_env.iter().copied());
    let output = command
        .current_dir(cwd)
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args(args)
        .output()
        .expect("run s3-turbo-list");
    common::exit_and_output(output)
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

/// The `start_after` of each range a checkpoint left to list.
fn checkpoint_remaining_starts(path: &std::path::Path) -> Option<Vec<String>> {
    let content = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&content).ok()?;
    value.get("remaining")?.as_array().map(|items| {
        items
            .iter()
            .filter_map(|item| item.get("start_after")?.as_str().map(str::to_string))
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
        "list".into(),
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
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // The page's CommonPrefix is a row, merged in key order.
    assert_eq!(
        parquet_keys(&parquet),
        vec!["logs/a.txt", "logs/b.txt", "logs/archive/", "logs/c.txt"]
    );
    assert_eq!(std::fs::read_to_string(&ks).unwrap(), "\"logs/\",\"3\"\n");

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
    assert!(
        trace_events
            .iter()
            .any(|event| event["next_continuation_token_present"] == true)
    );
    assert!(
        trace_events
            .iter()
            .any(|event| event["common_prefixes_count"] == 1)
    );
    // Each page's event records the continuation token that request sent.
    let page_tokens: Vec<&Value> = trace_events
        .iter()
        .filter(|event| event["operation"] == "ListObjectsV2" && event["http_status"] == 200)
        .filter(|event| event["contents_count"] != 0)
        .map(|event| &event["continuation_token"])
        .collect();
    assert_eq!(page_tokens, [&Value::Null, &Value::from("token-1")]);
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
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--delimiter".into(),
        "".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--delimiter".into(),
        "".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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

    // Nothing is cached in the working directory: the next run discovers
    // again.
    assert!(!dir.path().join("us-east-1_mock-bucket_hints.toml").exists());
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
    write_fast_config(&config);
    std::fs::write(&hints, "small/\n").unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--delimiter".into(),
        "".into(),
        "--max-keys".into(),
        "2".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        // Skip startup discovery (which now pre-partitions flat namespaces
        // too) so the run starts single-segment and the RUNTIME split path
        // is what fans out — the mechanism under test here.
        "--no-auto-hints".into(),
        "--max-keys".into(),
        "2".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
        "list".into(),
        "--max-keys".into(),
        "2".into(),
        "--output-format".into(),
        "summary".into(),
        "--agent".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    // --agent keeps stderr quiet: no log lines (unless RUST_LOG asks).
    if std::env::var_os("RUST_LOG").is_none() {
        assert!(stderr.is_empty(), "{}", stderr);
    }

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
        "list".into(),
        "--max-keys".into(),
        "2".into(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        "--output-format".into(),
        "tsv".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    // Streaming rows is what tsv means: no warning about it.
    assert!(!stderr.contains("WARN "), "{}", stderr);

    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "stdout should contain only TSV rows");
    let first: Vec<_> = lines[0].split('\t').collect();
    assert_eq!(first.len(), 3);
    assert_eq!(first[0], "logs/a.txt");
    assert_eq!(first[1], "100");
    assert!(first[2].parse::<u64>().unwrap() > 0);
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("images/c.jpg\t100\t"))
    );

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
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("plain.txt\t100\t"))
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("tab\\tkey.txt\t101\t"))
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("line\\nkey.txt\t102\t"))
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("slash\\\\key.txt\t103\t"))
    );
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
        "list".into(),
        "--run-manifest".into(),
        manifest.display().to_string(),
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
    // A missing parent directory is created, as for any other output.
    let report = dir.path().join("reports/probe/compat.json");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "compat-probe".into(),
        "--endpoint-url".into(),
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
    assert_eq!(report["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(report["tool_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(report["status"], "success");
    assert_eq!(report["exit_code"], 0);
    // Always present, empty in the normal case.
    assert_eq!(report["warnings"], serde_json::json!([]));
    assert!(report["tests"].as_array().unwrap().iter().any(|test| {
        test["test"] == "ListObjectsV2 pagination check" && test["status"] == "ok"
    }));

    let requests = server.requests();
    assert!(requests.iter().any(|request| request.method == "HEAD"));
    // The probe sends only requests a listing run sends (it used to try
    // encoding-type=url, which the list engine never uses).
    assert!(
        requests
            .iter()
            .all(|request| !request.query.contains_key("encoding-type"))
    );
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
        "--endpoint-url".into(),
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
    // Every operation failed, and not on a setup error: an incompatible
    // endpoint must not exit 0 (it used to).
    assert_eq!(code, 4, "stdout: {}\nstderr: {}", stdout, stderr);
    // The documented run line (nothing about "listed": a probe lists nothing).
    assert!(
        stderr.contains(
            "s3-turbo-list: run failed (exit 4): compat-probe found the endpoint incompatible"
        ),
        "stderr: {}",
        stderr
    );
    assert!(!stderr.contains("Nothing was listed"), "stderr: {}", stderr);

    let report: Value = serde_json::from_str(&std::fs::read_to_string(report).unwrap()).unwrap();
    assert_eq!(report["overall_status"], "incompatible");
    assert_eq!(report["status"], "failed");
    assert_eq!(report["exit_code"], 4);
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
    assert!(
        service_error["recommendation"]
            .as_str()
            .unwrap()
            .contains("does not implement")
    );
    assert_eq!(service_error["request_id"], "mock-request");
    assert_eq!(service_error["request_id_2"], "mock-request-2");
    assert!(
        service_error
            .as_object()
            .unwrap()
            .contains_key("error_message")
    );
}

#[test]
fn local_mock_compat_probe_uses_config_endpoint_style_and_trace() {
    // Endpoint and addressing style come only from the config file, as for a
    // listing run; every request is denied, which is a setup error (exit 3).
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(403, "AccessDenied", "Access Denied")
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let trace = dir.path().join("trace.jsonl");
    std::fs::write(
        &config,
        format!(
            "[s3]\nmax_attempts = 1\ninitial_backoff_secs = 0\nconnect_timeout_secs = 2\n\
             operation_timeout_secs = 2\nendpoint_url = \"{}\"\naddressing_style = \"path\"\n",
            server.endpoint()
        ),
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "compat-probe".into(),
        "--trace-compat".into(),
        trace.display().to_string(),
        "--region".into(),
        "us-east-1".into(),
        "--bucket".into(),
        "mock-bucket".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);

    let report: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["endpoint_url"], server.endpoint());
    assert_eq!(report["addressing_style"], "path");
    assert_eq!(report["overall_status"], "incompatible");
    // Path style: the bucket is in the request path, not the host.
    let requests = server.requests();
    assert!(!requests.is_empty());
    assert!(requests.iter().all(|r| r.path.starts_with("/mock-bucket")));
    // --trace-compat is honoured.
    let trace_lines = std::fs::read_to_string(&trace).unwrap();
    assert!(
        trace_lines.lines().count() >= requests.len(),
        "{}",
        trace_lines
    );
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
        "--endpoint-url".into(),
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
    // The report path is checked with the other outputs, before any request.
    assert!(stderr.contains("compat.json"), "{}", stderr);
    assert!(stderr.contains("cannot be created"), "{}", stderr);
    assert!(
        stderr.contains("s3-turbo-list: run failed (exit 5)"),
        "{}",
        stderr
    );
    assert!(!stderr.contains("panicked"));
    assert!(server.requests().is_empty());
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
    // The identity carries the fingerprint of the boundary set the completed
    // indices refer to (["m/"], the same set --hints-file supplies below);
    // resume verifies it before trusting the indices.
    std::fs::write(
        &checkpoint,
        format!(
            r#"bucket = "mock-bucket"
prefix = ""
last_updated = "2026-05-17T00:00:00Z"
remaining = [{{ start_after = "m/" }}]

[identity]
bucket = "mock-bucket"
region = "us-east-1"
prefix = ""
delimiter = ""
addressing_style = "path"
mode = "list"
endpoint_url = "{}"
"#,
            server.endpoint()
        ),
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--resume".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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

    // This run finished the remaining segment, so both segments are now done
    // and there is no resume point left. The checkpoint used to be rewritten
    // here as `completed_indices = [0, 1]` and left on disk — which is the
    // trap: the next `--resume` invocation would find every segment recorded
    // complete and list nothing at all, while still reporting success.
    assert!(
        !checkpoint.exists(),
        "a run that completed the remaining segments must leave no resume \
         point, but the checkpoint survived: {}",
        std::fs::read_to_string(&checkpoint).unwrap_or_default()
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
last_updated = "2026-05-24T00:00:00Z"
remaining = [{ start_after = "m/" }]

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
        "list".into(),
        "--resume".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_ne!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(
        checkpoint_remaining_starts(&checkpoint),
        Some(vec!["m/".to_string()])
    );

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
    write_fast_config(&config);
    std::fs::create_dir(&bad_parquet_path).unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--resume".into(),
        "--no-auto-hints".into(),
        "--output-parquet-file".into(),
        bad_parquet_path.display().to_string(),
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
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
        "list".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
        "list".into(),
        "--delimiter".into(),
        "".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
fn local_mock_no_auto_hints_ignores_a_leftover_hints_cache() {
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
        "list".into(),
        "--no-auto-hints".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
    // diff partitions each side automatically. The left side's startup
    // discovery finds one CommonPrefix ("m/") and lists two segments; the
    // right side is a flat namespace, so structural discovery finds nothing
    // and the flat-cut bisection (max-keys=1 probes) partitions it instead —
    // so it also lists in parallel rather than as one serial segment. The
    // merge must classify across both sides' segment boundaries with every
    // key exactly once.
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
            let prefix = request.query.get("prefix").cloned().unwrap_or_default();
            if request.path.contains("/left") {
                // Left-side startup discovery: one top-level prefix.
                let cps: &[&str] = if prefix.is_empty() { &["m/"] } else { &[] };
                return MockResponse::ok_xml(list_bucket_xml(&prefix, 1000, &[], cps, false, None));
            }
            // Right-side startup discovery: flat namespace, no structure, and
            // more pages to come — a side worth partitioning.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], true, Some("token")));
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
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "diff".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
    assert!(
        left_lists
            .iter()
            .any(|r| !r.query.contains_key("start-after"))
    );
    assert!(
        left_lists
            .iter()
            .any(|r| r.query.get("start-after").map(String::as_str) == Some("m/"))
    );
    // Right side: structural discovery probe, then flat-cut bisection probes
    // (max-keys=1) that partition the flat namespace for parallel listing.
    assert!(
        requests
            .iter()
            .any(|r| r.path.contains("/right") && r.query.contains_key("delimiter"))
    );
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
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "diff".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
// A leftover hints cache file must not fan a --start-after run out into
// multiple segments: every segment would override its start with the CLI key
// and list overlapping ranges, duplicating output rows. The file is ignored
// (single chain); explicit --hints-file and --resume are rejected up front.

#[test]
fn local_mock_start_after_ignores_a_leftover_hints_cache_and_lists_single_chain() {
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
    write_fast_config(&config);
    // A hints cache (cwd-relative) as older versions wrote for the same
    // bucket: two segments split at "m/". Runs no longer read it.
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
        "list".into(),
        "--start-after".into(),
        "a.txt".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
        "list".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--start-after".into(),
        "a.txt".into(),
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
        "list".into(),
        "--resume".into(),
        "--start-after".into(),
        "a.txt".into(),
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

// ── Filtered-run KS/metrics consistency ─────────────────────
//
// Prefix/byte accounting must describe the objects included in the output:
// the Parquet path previously counted every received object (pre-filter)
// while stdout/summary counted post-filter, so the same filtered scan
// reported different KS counts and bytes_total per output format.
#[test]
fn local_mock_filtered_list_ks_counts_only_included_objects() {
    let server = MockS3Server::start(|request, _sequence| {
        // Sizes are 100+index: p/a.txt=100, p/b.txt=101, p/c.txt=102.
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["p/a.txt", "p/b.txt", "p/c.txt"],
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
        "list".into(),
        "--no-auto-hints".into(),
        "--filter".into(),
        "SOURCE.size > 101".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // Only p/c.txt (size 102) passes the filter.
    assert_eq!(parquet_keys(&parquet), vec!["p/c.txt"]);
    // The KS counts must describe the Parquet artifact, not the raw listing.
    assert_eq!(std::fs::read_to_string(&ks).unwrap(), "\"p/\",\"1\"\n");
}

// ── Fatal segment fails the run without inflating errors ────
//
// A non-retryable failure in one segment fails the whole run fast via the
// global quit signal. It must not clear the side's lifecycle bit while
// sibling segments are still listing (which made the data map finalize
// early and killed the siblings with channel errors, inflating
// fatal_errors past the one real failure).
#[test]
fn local_mock_fatal_segment_counts_one_fatal_error() {
    let server = MockS3Server::start(|request, _sequence| {
        if request.query.get("start-after").map(String::as_str) == Some("m/") {
            return MockResponse::error(404, "NoSuchBucket", "injected fatal");
        }
        // Root segment: several truncated pages so it is still listing when
        // the sibling segment fails.
        match request.query.get("continuation-token").map(String::as_str) {
            None => MockResponse::ok_xml(list_bucket_xml(
                "",
                1000,
                &["a1.txt"],
                &[],
                true,
                Some("r1"),
            )),
            Some("r1") => MockResponse::ok_xml(list_bucket_xml(
                "",
                1000,
                &["a2.txt"],
                &[],
                true,
                Some("r2"),
            )),
            Some("r2") => {
                MockResponse::ok_xml(list_bucket_xml("", 1000, &["a3.txt"], &[], false, None))
            }
            Some(other) => MockResponse::error(400, "InvalidToken", other),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let hints = dir.path().join("hints.toml");
    let parquet = dir.path().join("out.parquet");
    let manifest = dir.path().join("manifest.json");
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
        "list".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_ne!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(
        manifest_json["metrics"]["fatal_errors"], 1,
        "exactly the one injected failure must be counted: {}",
        manifest_json["metrics"]
    );
    // The sibling segment must not die on "channel closed" errors.
    assert!(!stderr.contains("Data map channel closed"), "{}", stderr);
}

// ── Retry cursor advances over CommonPrefixes-only pages ────
//
// Delimiter pages can contain only CommonPrefixes (no Contents). The resume
// cursor previously only advanced on real keys, so a retry after such pages
// restarted from many pages back and re-listed them; with the CP-only page
// followed by a persistent transient error, the retry made no progress at
// all and the run failed. The cursor now advances over the last
// CommonPrefix (safe: a prefix sorts before every key it covers, and those
// keys are rolled up into the prefix, never emitted).
#[test]
fn local_mock_retry_resumes_after_common_prefixes_only_page() {
    let server = MockS3Server::start(|request, _sequence| {
        if request.query.get("continuation-token").map(String::as_str) == Some("t1") {
            // Persistent connection-level failure on the token chain (named
            // S3 service errors classify fatal once the SDK's own retries
            // are spent; only connection/timeout errors reach the outer
            // cursor-resume retry). The retry must restart from
            // start-after=cp2/, not from scratch.
            return MockResponse::drop_connection();
        }
        match request.query.get("start-after").map(String::as_str) {
            None => MockResponse::ok_xml(list_bucket_xml(
                "",
                1000,
                &[],
                &["cp1/", "cp2/"],
                true,
                Some("t1"),
            )),
            Some("cp2/") => {
                MockResponse::ok_xml(list_bucket_xml("", 1000, &["zz.txt"], &[], false, None))
            }
            Some(other) => MockResponse::error(400, "UnexpectedStartAfter", other),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--delimiter".into(),
        "/".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    // CommonPrefixes are emitted as rows in a --delimiter run.
    assert_eq!(parquet_keys(&parquet), vec!["cp1/", "cp2/", "zz.txt"]);

    let requests = server.requests();
    assert!(
        requests
            .iter()
            .any(|r| r.query.get("start-after").map(String::as_str) == Some("cp2/")),
        "retry must resume after the last CommonPrefix: {:#?}",
        requests
    );
}

// ── Flat-namespace startup pre-partitioning ─────────────────
//
// When structural discovery finds no CommonPrefixes, list mode now bisects
// the flat key range up front (the same partitioner diff sides use) instead
// of starting single-segment and ramping via runtime splits. The first run
// must list in parallel segments with every key emitted exactly once, and
// nothing may be cached in the working directory.
#[test]
fn local_mock_list_flat_namespace_prepartitions_at_startup() {
    let keys: Vec<String> = (0..200).map(|i| format!("obj-{:04}", i)).collect();

    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Structural discovery: flat namespace, no CommonPrefixes, and
            // more pages to come — a bucket worth partitioning.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], true, Some("token")));
        }
        if request.query.get("max-keys").map(String::as_str) == Some("1") {
            // Bisection probe: first real key after the candidate.
            let first: Vec<&str> = all_keys
                .iter()
                .find(|k| k.as_str() > start_after.as_str())
                .map(|k| vec![k.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }
        // Segment listing: real-S3 semantics, single page (well under 1000).
        let page: Vec<&str> = all_keys
            .iter()
            .filter(|k| k.as_str() > start_after.as_str())
            .map(String::as_str)
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--concurrency".into(),
        "8".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // Every key exactly once — parallel segments must not overlap or drop.
    let mut listed = parquet_keys(&parquet);
    listed.sort();
    assert_eq!(listed, keys);

    let requests = server.requests();
    // Bisection probes ran at startup.
    assert!(
        requests
            .iter()
            .any(|r| r.query.get("max-keys").map(String::as_str) == Some("1")),
        "expected startup bisection probes (max-keys=1)"
    );
    // The listing itself fanned out: multiple segment requests with distinct
    // real-key start-after boundaries, from the very first run.
    let segment_starts: std::collections::BTreeSet<&str> = requests
        .iter()
        .filter(|r| {
            !r.query.contains_key("delimiter")
                && r.query.get("max-keys").map(String::as_str) != Some("1")
        })
        .filter_map(|r| r.query.get("start-after").map(String::as_str))
        .collect();
    assert!(
        segment_starts.len() >= 2,
        "expected multiple parallel segments from startup pre-partitioning, got starts {:?}",
        segment_starts
    );
    // Nothing is cached in the working directory.
    assert!(!dir.path().join("us-east-1_mock-bucket_hints.toml").exists());
}

// Flat keys sharing a long constant suffix after a numeric run
// (`obj-000000123.snappy.parquet`) used to defeat bisection: every candidate
// bumped a character inside the suffix, so each cut landed on the key right
// after the range start and the run listed one giant tail segment. Cuts must
// now land near the middle of each range, giving balanced segments.
#[test]
fn local_mock_list_flat_suffix_heavy_namespace_partitions_evenly() {
    const KEYS: usize = 4000;
    const PAGE: usize = 250;
    let keys: Vec<String> = (0..KEYS)
        .map(|i| format!("obj-{:09}.snappy.parquet", i))
        .collect();

    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Structural discovery: flat, and more pages to come.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], true, Some("t")));
        }
        let max_keys: usize = request
            .query
            .get("max-keys")
            .and_then(|value| value.parse().ok())
            .unwrap_or(PAGE)
            .min(PAGE);
        let start_idx = match request.query.get("continuation-token") {
            Some(token) => token
                .strip_prefix("off-")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(0),
            None => all_keys.partition_point(|k| k.as_str() <= start_after.as_str()),
        };
        let page: Vec<&str> = all_keys[start_idx..]
            .iter()
            .take(max_keys)
            .map(String::as_str)
            .collect();
        let next = start_idx + page.len();
        let truncated = next < all_keys.len() && max_keys > 1;
        let token = format!("off-{}", next);
        MockResponse::ok_xml(list_bucket_xml(
            "",
            max_keys as i32,
            &page,
            &[],
            truncated,
            truncated.then_some(token.as_str()),
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    write_fast_config(&config);
    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--concurrency".into(),
        "8".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) =
        run_cli_with_env(&args, dir.path(), &[("RUST_LOG", "s3_turbo_list=debug")]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let mut listed = parquet_keys(&parquet);
    listed.sort();
    assert_eq!(listed, keys);

    // One boundary per worker, every one a real key, strictly ascending.
    let boundaries: Vec<String> = stderr
        .lines()
        .find_map(|line| line.split_once("Startup boundaries: ").map(|(_, b)| b))
        .expect("startup boundaries logged")
        .split('\t')
        .map(str::to_string)
        .collect();
    assert_eq!(boundaries.len(), 8, "{:?}", boundaries);
    let mut positions: Vec<usize> = boundaries
        .iter()
        .map(|b| keys.binary_search(b).expect("boundary is a real key"))
        .collect();
    assert!(
        positions.windows(2).all(|w| w[0] < w[1]),
        "{:?}",
        boundaries
    );
    // Balanced: no segment holds more than ~2.5x its even share.
    positions.push(KEYS - 1);
    let mut prev = 0usize;
    let largest = positions
        .iter()
        .map(|&p| {
            let size = p - prev;
            prev = p;
            size
        })
        .max()
        .unwrap();
    let ideal = KEYS / (boundaries.len() + 1);
    assert!(
        largest <= ideal * 5 / 2,
        "largest segment {} keys (ideal {}): {:?}",
        largest,
        ideal,
        boundaries
    );
    // The cut search stays cheap: a bounded number of single-key probes.
    let probes = server
        .requests()
        .iter()
        .filter(|r| r.query.get("max-keys").map(String::as_str) == Some("1"))
        .count();
    assert!(probes <= 120, "{} bisection probes", probes);
}

// ── Leftover hints cache files ──────────────────────────────
//
// A hierarchical run rolls every key under a CommonPrefix and a page's
// CommonPrefixes are not range-filtered, so segments from a leftover cache
// file would each
// re-list the same prefix set — N× the requests, N× the reported
// CommonPrefix count, and no parallelism to show for it.
#[test]
fn local_mock_delimiter_run_ignores_a_leftover_hints_cache() {
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        assert!(
            start_after.is_empty(),
            "a delimiter run must list as one segment, got start-after '{}'",
            start_after
        );
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &["top-level.txt"],
            &["logs/", "data/"],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    write_fast_config(&config);
    // A cache left behind by an earlier recursive run of the same bucket.
    std::fs::write(
        dir.path().join("us-east-1_mock-bucket_hints.toml"),
        r#"bucket = "mock-bucket"
region = "us-east-1"
boundaries = ["data/", "logs/"]
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
        "list".into(),
        "--delimiter".into(),
        "/".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    // CommonPrefixes are emitted as rows in a --delimiter run (in the order
    // this mock returns them).
    assert_eq!(
        parquet_keys(&parquet),
        vec!["logs/", "data/", "top-level.txt"]
    );
    // One request total: no cached segment fan-out.
    assert_eq!(
        server.requests().len(),
        1,
        "expected a single hierarchical listing request, got {:?}",
        server.requests()
    );
}

// ── Unwritable output paths exit, never panic ───────────────
//
// Both paths are resolved before any listing request, so the run must fail
// with the documented OutputWrite code (5) instead of a panic (101, which is
// not part of the exit-code contract agents branch on).
fn run_expecting_output_write_failure(extra: &[&str]) {
    let server = MockS3Server::start(|_request, _sequence| {
        panic!("no S3 request should be issued when output setup fails")
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    std::fs::write(dir.path().join("blocker"), b"a regular file").unwrap();

    let mut args: Vec<String> = vec![
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
        "summary".into(),
    ];
    args.extend(extra.iter().map(|arg| arg.to_string()));

    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 5, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        !stderr.contains("panicked"),
        "expected a clean failure, got a panic: {}",
        stderr
    );
    assert!(server.requests().is_empty());
}

#[test]
fn local_mock_unwritable_trace_path_exits_output_write() {
    // A path under a regular file cannot be created, even as root (missing
    // parent directories alone are now created, as --output-dir does).
    run_expecting_output_write_failure(&["--trace-compat", "blocker/trace.jsonl"]);
}

#[test]
fn local_mock_unwritable_log_path_exits_output_write() {
    // The --log file is named after the outputs, in --output-dir.
    run_expecting_output_write_failure(&["--output-dir", "blocker", "--log"]);
}

// A listing that fits in one page has nothing to partition: bisecting it
// costs several single-key probes per cut to split work one request already
// finishes, and caches boundaries that pin that shape for later runs.
#[test]
fn local_mock_single_page_flat_listing_skips_bisection() {
    let keys: Vec<String> = (0..200).map(|i| format!("obj-{:04}", i)).collect();
    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let page: Vec<&str> = all_keys
            .iter()
            .filter(|k| k.as_str() > start_after.as_str())
            .map(String::as_str)
            .collect();
        // Flat (no CommonPrefixes) and untruncated, whether probed with a
        // delimiter or not: the whole bucket is one page.
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--concurrency".into(),
        "16".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
        !requests
            .iter()
            .any(|r| r.query.get("max-keys").map(String::as_str) == Some("1")),
        "a single-page listing must not pay bisection probes, got {:?}",
        requests
    );
    // One structural probe plus one listing request.
    assert_eq!(requests.len(), 2, "{:?}", requests);
    // Nothing worth caching either: no boundaries were discovered.
    assert!(!dir.path().join("us-east-1_mock-bucket_hints.toml").exists());
}

// The discovery probe uses the provider's default page size, so an
// untruncated probe page does not mean the *run* is single-page: with
// --max-keys below that default the same keys paginate, and the run wants
// segments after all.
#[test]
fn local_mock_small_max_keys_still_partitions_untruncated_listing() {
    let keys: Vec<String> = (0..200).map(|i| format!("obj-{:04}", i)).collect();
    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let max_keys: usize = request
            .query
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Structural discovery: flat, and the whole listing fits in the
            // probe's default-sized page.
            let all: Vec<&str> = all_keys.iter().map(String::as_str).collect();
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &all, &[], false, None));
        }
        // Offset-encoded continuation tokens; pages are --max-keys long.
        let start_idx = match request.query.get("continuation-token") {
            Some(token) => token
                .strip_prefix("off-")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(0),
            None => all_keys.partition_point(|k| k.as_str() <= start_after.as_str()),
        };
        let page: Vec<&str> = all_keys[start_idx..]
            .iter()
            .take(max_keys)
            .map(String::as_str)
            .collect();
        let next_idx = start_idx + page.len();
        let truncated = next_idx < all_keys.len();
        let token = format!("off-{}", next_idx);
        MockResponse::ok_xml(list_bucket_xml(
            "",
            max_keys as i32,
            &page,
            &[],
            truncated,
            truncated.then_some(token.as_str()),
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--concurrency".into(),
        "8".into(),
        "--max-keys".into(),
        "5".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
        "a run paginating at --max-keys 5 must still pre-partition, got {:?}",
        requests
    );
}

// ── Failed diff side leaves no artifact ─────────────────────
//
// A side that fails fatally closes its channels mid-listing, which the merge
// cannot distinguish from that side having ended: every remaining key of the
// other side would be classified one-sided, and the run would leave behind a
// structurally valid Parquet asserting that the unread bucket does not have
// them. Only the exit code said otherwise.
#[test]
fn local_mock_diff_with_failing_side_writes_no_diff_output() {
    let server = MockS3Server::start(move |request, _sequence| {
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        if !request.path.contains("/left") {
            return MockResponse::error(403, "AccessDenied", "injected fatal on the right side");
        }
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let keys = ["a.txt", "b.txt", "c.txt"];
        let page: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|k| *k > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
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
        "diff".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
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
    assert_ne!(code, 0, "a failed side must fail the run");
    assert!(
        !parquet.exists(),
        "a partial diff must not be left behind: stdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(!ks.exists(), "no KS file describes a diff that never ran");
}

// ── Observable parallelism ──────────────────────────────────
//
// These are the tests the serial mock could not express: with per-request
// latency and a mock that serves connections concurrently, a run that fans
// out is distinguishable from one that does not — by the concurrency the
// mock actually served, and by finishing well inside the serial bound.

/// Mock for a flat namespace of `count` keys where every listing request
/// costs `latency`. Bisection probes stay fast so startup is not the thing
/// under measurement.
fn slow_flat_namespace_server(count: usize, page: usize, latency: Duration) -> MockS3Server {
    let keys: Vec<String> = (0..count).map(|i| format!("obj-{:06}", i)).collect();
    MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let max_keys: usize = request
            .query
            .get("max-keys")
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000);
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            // Flat, and more pages to come, so the run pre-partitions.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], true, Some("t")));
        }
        if max_keys == 1 {
            let first: Vec<&str> = keys
                .iter()
                .find(|key| key.as_str() > start_after.as_str())
                .map(|key| vec![key.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }
        thread::sleep(latency);
        let start_idx = match request.query.get("continuation-token") {
            Some(token) => token
                .strip_prefix("off-")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(0),
            None => keys.partition_point(|key| key.as_str() <= start_after.as_str()),
        };
        let page_keys: Vec<&str> = keys[start_idx..]
            .iter()
            .take(max_keys.min(page))
            .map(String::as_str)
            .collect();
        let next = start_idx + page_keys.len();
        let truncated = next < keys.len();
        let token = format!("off-{}", next);
        MockResponse::ok_xml(list_bucket_xml(
            "",
            page as i32,
            &page_keys,
            &[],
            truncated,
            truncated.then_some(token.as_str()),
        ))
    })
}

#[test]
fn local_mock_list_serves_segments_concurrently() {
    const KEYS: usize = 2000;
    const PAGE: usize = 50;
    const LATENCY_MS: u64 = 20;

    // Same bucket and the same per-request latency twice: once partitioned,
    // once as a single chain. The baseline uses --start-after with a key that
    // sorts before every object, so nothing is skipped and both partitioning
    // and runtime splitting are off. Both runs pay the same fixed process
    // overhead, so the difference between them is the listing itself — which
    // is what makes parallelism measurable at all.
    fn run(extra: &[&str]) -> (std::time::Duration, usize, usize) {
        let server = slow_flat_namespace_server(KEYS, PAGE, Duration::from_millis(LATENCY_MS));
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let parquet = dir.path().join("out.parquet");
        write_fast_config(&config);

        let mut args: Vec<String> = vec![
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
            "--concurrency".into(),
            "8".into(),
            "--max-keys".into(),
            PAGE.to_string(),
            "--output-parquet-file".into(),
            parquet.display().to_string(),
        ];
        args.extend(extra.iter().map(|arg| arg.to_string()));

        let started = std::time::Instant::now();
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        let elapsed = started.elapsed();
        assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
        assert_eq!(parquet_keys(&parquet).len(), KEYS);
        (elapsed, server.max_in_flight(), server.requests().len())
    }

    let (serial_elapsed, serial_peak, serial_requests) = run(&["--start-after", "obj-"]);
    let (parallel_elapsed, parallel_peak, parallel_requests) = run(&[]);
    assert!(serial_requests > 0 && parallel_requests > 0);

    assert_eq!(
        serial_peak, 1,
        "a single-segment run has nothing to overlap"
    );
    assert!(
        parallel_peak >= 2,
        "partitioned listing must overlap requests, peak in-flight was {}",
        parallel_peak
    );
    // Wall clock is deliberately not asserted here: a run shorter than the
    // monitor's heartbeat window finishes inside it, so both runs measure the
    // same fixed process time (~5s) whatever the listing cost. The served
    // concurrency above is the signal that does not depend on that.
    let _ = (serial_elapsed, parallel_elapsed);
}

#[test]
fn local_mock_diff_serves_both_sides_concurrently() {
    const KEYS: usize = 600;
    const PAGE: usize = 50;
    let keys: Vec<String> = (0..KEYS).map(|i| format!("obj-{:06}", i)).collect();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let max_keys: usize = request
            .query
            .get("max-keys")
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000);
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], true, Some("t")));
        }
        if max_keys == 1 {
            let first: Vec<&str> = keys
                .iter()
                .find(|key| key.as_str() > start_after.as_str())
                .map(|key| vec![key.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }
        thread::sleep(Duration::from_millis(20));
        let start_idx = match request.query.get("continuation-token") {
            Some(token) => token
                .strip_prefix("off-")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(0),
            None => keys.partition_point(|key| key.as_str() <= start_after.as_str()),
        };
        let page_keys: Vec<&str> = keys[start_idx..]
            .iter()
            .take(max_keys.min(PAGE))
            .map(String::as_str)
            .collect();
        let next = start_idx + page_keys.len();
        let truncated = next < keys.len();
        let token = format!("off-{}", next);
        MockResponse::ok_xml(list_bucket_xml(
            "",
            PAGE as i32,
            &page_keys,
            &[],
            truncated,
            truncated.then_some(token.as_str()),
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
        "diff".into(),
        "--concurrency".into(),
        "8".into(),
        "--max-keys".into(),
        PAGE.to_string(),
        "--output-parquet-file".into(),
        dir.path().join("d.parquet").display().to_string(),
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
    assert!(
        server.max_in_flight() >= 2,
        "diff sides and their segments must overlap, peak in-flight was {}",
        server.max_in_flight()
    );
}

// The monitor used to sleep a whole heartbeat between checks, so a run held
// the process open for up to that long after the listing had finished — a
// fixed tail that dwarfed the work on anything small (a two-request listing
// took ~5s). The heartbeat still prints on its own cadence; only the exit
// check got faster.
#[test]
fn local_mock_small_run_exits_without_waiting_for_a_heartbeat() {
    let keys = ["a.txt", "b.txt", "c.txt"];
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let page: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|key| *key > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("out.parquet");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let started = std::time::Instant::now();
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let elapsed = started.elapsed();
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(parquet_keys(&parquet).len(), keys.len());
    // Generous: the run itself is milliseconds, and the heartbeat interval it
    // must not wait for is 5s.
    assert!(
        elapsed < Duration::from_secs(4),
        "a three-key listing took {:?} — the process is waiting on a timer, not on work",
        elapsed
    );
}

// ── KS output is real CSV ───────────────────────────────────
//
// Object keys may contain quotes, commas and newlines, and all three reach
// the KS prefix column. The writer used to interpolate them bare, so three
// objects could produce four lines and a quote inside a key broke the row —
// while the run exited 0 and the README called the file a two-column CSV.
#[test]
fn local_mock_ks_output_escapes_keys_as_csv() {
    let keys = [
        "comma,prefix/a.txt",
        "multi\nline/b.txt",
        "plain/c.txt",
        "we\"ird/d.txt",
    ];
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let page: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|key| *key > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let ks = dir.path().join("out.ks");
    write_fast_config(&config);
    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--output-parquet-file".into(),
        dir.path().join("out.parquet").display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // Parse it the way a consumer would, with a real CSV reader.
    let script = dir.path().join("read_ks.py");
    std::fs::write(
        &script,
        r#"import csv, sys
rows = list(csv.reader(open(sys.argv[1], newline="")))
print(len(rows))
for prefix, count in rows:
    print(repr(prefix), count)
"#,
    )
    .unwrap();
    let out = std::process::Command::new("python3")
        .arg(&script)
        .arg(&ks)
        .output()
        .expect("python3 available for CSV round-trip");
    let parsed = String::from_utf8_lossy(&out.stdout).to_string();
    let mut lines = parsed.lines();
    assert_eq!(
        lines.next(),
        Some("4"),
        "one CSV row per distinct prefix: {}",
        parsed
    );
    let body: Vec<&str> = lines.collect();
    assert!(
        body.iter().any(|l| l.contains("we\"ird")),
        "an embedded quote must survive a CSV round-trip: {:?}",
        body
    );
    assert!(
        body.iter().any(|l| l.contains("multi\\nline")),
        "an embedded newline must stay inside its field: {:?}",
        body
    );
    assert!(
        body.iter().any(|l| l.contains("comma,prefix")),
        "an embedded comma must stay inside its field: {:?}",
        body
    );
}

/// A ListObjectsV2 page whose single object carries a caller-supplied raw ETag,
/// so a test can model what an S3-compatible endpoint actually put on the wire
/// rather than what a well-behaved one would.
fn list_bucket_xml_with_raw_etag(key: &str, raw_etag: &str, next_token: Option<&str>) -> String {
    let next_token_xml = next_token
        .map(|token| format!("<NextContinuationToken>{}</NextContinuationToken>", token))
        .unwrap_or_default();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>mock-bucket</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>{}</IsTruncated>{}<Contents><Key>{}</Key><LastModified>2026-05-17T00:00:00.000Z</LastModified><ETag>{}</ETag><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"#,
        next_token.is_some(),
        next_token_xml,
        xml_escape(key),
        xml_escape(raw_etag),
    )
}

/// A malformed ETag must cost its object an ETag, never its segment.
///
/// Regression: `ObjectProps::from` sliced the raw ETag as `&x[1..33]`, which
/// panics when byte 1 lands inside a multi-byte character. The panic killed the
/// segment task, and the reactor logged the `JoinError` and carried on — so the
/// segment's whole key range vanished from the output while the run reported
/// `status: success` with `fatal_errors: 0`, and `manifest-summary --check`
/// passed on the short file.
#[test]
fn local_mock_non_ascii_etag_keeps_every_key_and_succeeds() {
    // 34 bytes, so it takes the single-part branch, but byte 1 is mid-character.
    let bad_etag = format!("\u{65e5}{}", "a".repeat(31));
    assert_eq!(bad_etag.len(), 34);

    let server = MockS3Server::start(move |request, _sequence| {
        match request.query.get("continuation-token").map(String::as_str) {
            None => MockResponse::ok_xml(list_bucket_xml_with_raw_etag(
                "a-good.txt",
                "\"d41d8cd98f00b204e9800998ecf8427e\"",
                Some("token-1"),
            )),
            Some("token-1") => MockResponse::ok_xml(list_bucket_xml_with_raw_etag(
                "b-bad-etag.txt",
                &bad_etag,
                Some("token-2"),
            )),
            _ => MockResponse::ok_xml(list_bucket_xml_with_raw_etag(
                "c-good.txt",
                "\"d41d8cd98f00b204e9800998ecf8427e-3\"",
                None,
            )),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("etag.parquet");
    let manifest = dir.path().join("run.json");
    write_fast_config(&config);

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());

    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        !stderr.contains("panicked"),
        "a malformed ETag must not panic a segment task: {}",
        stderr
    );
    assert_eq!(
        parquet_keys(&parquet),
        vec!["a-good.txt", "b-bad-etag.txt", "c-good.txt"],
        "the object with the malformed ETag — and the rest of its segment — must still be listed"
    );

    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(manifest_json["status"], "success");
    assert_eq!(manifest_json["metrics"]["fatal_errors"], 0);
}

/// One key per page, resumable by either continuation token or `start-after`,
/// with a caller-chosen set of pages that fail hard before they will serve.
///
/// `drops_per_page` is how many times a chosen page drops the connection before
/// answering: the SDK retries internally, so a page has to fail at least
/// `max_attempts` times in a row before the failure surfaces to the segment's
/// own retry loop, which is the layer under test.
fn paged_mock_with_failing_pages(
    total_keys: usize,
    failing: &'static [usize],
    drops_per_page: usize,
) -> (MockS3Server, Arc<Mutex<BTreeMap<usize, usize>>>) {
    let attempts: Arc<Mutex<BTreeMap<usize, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let handler_attempts = Arc::clone(&attempts);
    let server = MockS3Server::start(move |request, _sequence| {
        // A delimiter request is a split probe. Answering it with keys would
        // hand the reactor a boundary and fan the run out into several
        // segments, which would scramble both the page order and the retry
        // budget under test; an empty result keeps this a single chain.
        if request.query.contains_key("delimiter") {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }

        // Which key this request is asking for, however it was resumed.
        let index = match (
            request.query.get("continuation-token"),
            request.query.get("start-after"),
        ) {
            (Some(token), _) => match token
                .strip_prefix("tok-")
                .and_then(|t| t.parse::<usize>().ok())
            {
                Some(i) => i + 1,
                None => {
                    return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
                }
            },
            (None, Some(after)) if !after.is_empty() => {
                match after
                    .strip_prefix("key-")
                    .and_then(|k| k.parse::<usize>().ok())
                {
                    Some(i) => i + 1,
                    None => {
                        return MockResponse::ok_xml(list_bucket_xml(
                            "",
                            1000,
                            &[],
                            &[],
                            false,
                            None,
                        ));
                    }
                }
            }
            _ => 0,
        };
        if index >= total_keys {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }

        if failing.contains(&index) {
            let mut seen = handler_attempts.lock().unwrap();
            let count = seen.entry(index).or_insert(0);
            *count += 1;
            if *count <= drops_per_page {
                return MockResponse::drop_connection();
            }
        }

        let key = format!("key-{:02}", index);
        let last = index + 1 >= total_keys;
        let token = format!("tok-{:02}", index);
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &[key.as_str()],
            &[],
            !last,
            if last { None } else { Some(token.as_str()) },
        ))
    });
    (server, attempts)
}

fn paged_run_args(
    server: &MockS3Server,
    config: &std::path::Path,
    parquet: &std::path::Path,
    manifest: &std::path::Path,
) -> Vec<String> {
    vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--output-parquet-file".into(),
        parquet.display().to_string(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        // One segment, listed in order: the budget under test is per-segment,
        // and a fanned-out run would interleave pages from several of them.
        "--no-auto-hints".into(),
        "--concurrency".into(),
        "1".into(),
    ]
}

/// A segment that keeps advancing between hiccups must not run out of retries.
///
/// Regression: `max_attempts` was charged over the segment's whole lifetime and
/// never refunded, so a healthy listing died once it had accumulated that many
/// isolated transient failures — no matter how much ground it had covered in
/// between. The failure rate scaled with how long a run was, which is exactly
/// backwards for a tool whose job is big buckets.
#[test]
fn local_mock_retry_budget_is_refunded_by_progress() {
    // `write_fast_config` sets max_attempts = 3, so four separate hiccups is
    // more than a lifetime budget could absorb but each one is survivable on
    // its own.
    const FAILING: &[usize] = &[2, 4, 6, 8];
    let (server, attempts) = paged_mock_with_failing_pages(10, FAILING, 3);

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("refund.parquet");
    let manifest = dir.path().join("run.json");
    write_fast_config(&config);

    let args = paged_run_args(&server, &config, &parquet, &manifest);
    let (code, stdout, stderr) = run_cli(&args, dir.path());

    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(
        parquet_keys(&parquet),
        (0..10)
            .map(|i| format!("key-{:02}", i))
            .collect::<Vec<String>>(),
        "every page must be listed despite four separate hiccups"
    );

    // Each chosen page really did exhaust the SDK's own retries.
    let seen = attempts.lock().unwrap().clone();
    for index in FAILING {
        assert!(
            seen.get(index).copied().unwrap_or(0) > 3,
            "page {} should have been retried past the SDK budget: {:?}",
            index,
            seen
        );
    }

    // The segment's own retry loop is what resumed: it drops the continuation
    // token and re-requests with `start-after`. A purely SDK-internal retry
    // replays the tokened request instead, so this is the layer under test.
    let requests = server.requests();
    assert!(
        requests.iter().any(|request| {
            request
                .query
                .get("start-after")
                .is_some_and(|k| k == "key-01")
                && !request.query.contains_key("continuation-token")
        }),
        "expected a segment-level resume after the first hiccup: {:#?}",
        requests
    );
}

/// The refund must require real progress, or a stuck segment retries forever.
///
/// This is the guard on the fix above: a page that never succeeds leaves the
/// resume point where it was, so the budget is charged normally and the run
/// fails instead of spinning.
#[test]
fn local_mock_retry_budget_still_exhausts_without_progress() {
    // Page 2 never answers, so every retry after the first resumes from the
    // same key and buys no ground.
    const FAILING: &[usize] = &[2];
    let (server, _attempts) = paged_mock_with_failing_pages(10, FAILING, usize::MAX);

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let parquet = dir.path().join("stuck.parquet");
    let manifest = dir.path().join("run.json");
    write_fast_config(&config);

    let args = paged_run_args(&server, &config, &parquet, &manifest);
    let (code, stdout, stderr) = run_cli(&args, dir.path());

    assert_ne!(
        code, 0,
        "a segment that cannot advance must fail the run: stdout: {}\nstderr: {}",
        stdout, stderr
    );
    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(manifest_json["status"], "failed");
    assert!(
        manifest_json["metrics"]["fatal_errors"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "the run must record the failure: {}",
        manifest_json
    );
}

// ── Checkpoint lifecycle: a completed run must not leave a resume point ──
//
// Runtime-split segments deliberately record no checkpoint progress, so a run
// that finishes cleanly still ends with a checkpoint claiming only some of its
// segments are done.  Nothing removed that file, so the next ordinary
// `--resume` invocation read it, skipped those segments, and wrote an output
// covering only the remainder — success, no warning, and `manifest-summary
// --check` passing, because the manifest honestly described its own short
// artifact.  A nightly inventory was correct on its first run and quietly
// short on every run after it.

/// Keys skewed so bisection produces both tiny segments (which complete
/// without splitting) and a dominant tail (which splits and loses credit).
fn skewed_keys() -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for i in 0..40 {
        keys.push(format!("a-{:04}", i));
    }
    for i in 0..40 {
        keys.push(format!("m-{:04}", i));
    }
    for i in 0..1920 {
        keys.push(format!("z-{:06}", i));
    }
    keys.sort();
    keys
}

/// A flat namespace whose root page is truncated, so the run does not take the
/// single-page shortcut and instead bisects into several segments.
fn multi_segment_flat_server(keys: Vec<String>) -> MockS3Server {
    MockS3Server::start(move |request, _sequence| {
        let start_after = request
            .query
            .get("start-after")
            .cloned()
            .unwrap_or_default();

        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            let head: Vec<&str> = keys.iter().take(3).map(String::as_str).collect();
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &head, &[], true, Some("disc")));
        }
        if request.query.get("max-keys").map(String::as_str) == Some("1") {
            let first: Vec<&str> = keys
                .iter()
                .find(|k| k.as_str() > start_after.as_str())
                .map(|k| vec![k.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }

        std::thread::sleep(Duration::from_millis(20));
        let start_idx = match request.query.get("continuation-token") {
            Some(token) => token
                .strip_prefix("off-")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(0),
            None => keys.partition_point(|k| k.as_str() <= start_after.as_str()),
        };
        let page: Vec<&str> = keys[start_idx.min(keys.len())..]
            .iter()
            .take(4)
            .map(String::as_str)
            .collect();
        let truncated = start_idx + page.len() < keys.len() && !page.is_empty();
        let token = truncated.then(|| format!("off-{}", start_idx + page.len()));
        MockResponse::ok_xml(list_bucket_xml(
            "",
            4,
            &page,
            &[],
            truncated,
            token.as_deref(),
        ))
    })
}

#[test]
fn local_mock_successful_run_leaves_no_checkpoint() {
    let keys = skewed_keys();
    let server = multi_segment_flat_server(keys.clone());

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let args: Vec<String> = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--concurrency".into(),
        "8".into(),
        "--resume".into(),
        "--output-parquet-file".into(),
        dir.path().join("out.parquet").display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];

    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(
        parquet_keys(&dir.path().join("out.parquet")).len(),
        keys.len()
    );

    let checkpoint = dir.path().join("us-east-1_mock-bucket_checkpoint.toml");
    assert!(
        !checkpoint.exists(),
        "a run that listed the whole key space has nothing to resume, but it \
         left a checkpoint behind: {:?}",
        checkpoint_remaining_starts(&checkpoint)
    );
}

#[test]
fn local_mock_repeated_resume_runs_stay_complete() {
    // The end-to-end shape of the bug: an unattended job that always passes
    // --resume must produce the same complete listing every time.
    let keys = skewed_keys();
    let server = multi_segment_flat_server(keys.clone());

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let args: Vec<String> = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--concurrency".into(),
        "8".into(),
        "--resume".into(),
        "--output-parquet-file".into(),
        dir.path().join("out.parquet").display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];

    for run in 1..=3 {
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        assert_eq!(
            code, 0,
            "run {} stdout: {}\nstderr: {}",
            run, stdout, stderr
        );
        let listed = parquet_keys(&dir.path().join("out.parquet"));
        assert_eq!(
            listed.len(),
            keys.len(),
            "run {} produced a short listing ({} of {} keys) while reporting success",
            run,
            listed.len(),
            keys.len()
        );
    }
}

#[test]
fn local_mock_resumed_run_declares_its_partial_coverage() {
    // A resumed run's artifacts cover only the segments it listed. Nothing
    // said so, so pointing the interrupted run and the resumed run at one
    // output path silently left only the resumed half — the earlier run's
    // rows overwritten, the manifest reporting success over the remainder.
    let server = MockS3Server::start(|request, _sequence| {
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
    let manifest = dir.path().join("run.json");
    write_fast_config(&config);
    std::fs::write(
        &hints,
        "bucket = \"mock-bucket\"\nregion = \"us-east-1\"\nboundaries = [\"m/\"]\ngenerated_at = \"2026-05-17T00:00:00Z\"\n",
    )
    .unwrap();
    std::fs::write(
        &checkpoint,
        format!(
            r#"bucket = "mock-bucket"
prefix = ""
last_updated = "2026-05-17T00:00:00Z"
remaining = [{{ start_after = "m/" }}]
listed_ranges = 1

[identity]
bucket = "mock-bucket"
region = "us-east-1"
prefix = ""
delimiter = ""
addressing_style = "path"
mode = "list"
endpoint_url = "{}"
"#,
            server.endpoint()
        ),
    )
    .unwrap();

    let args = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--resume".into(),
        "--hints-file".into(),
        hints.display().to_string(),
        "--output-parquet-file".into(),
        dir.path().join("resume.parquet").display().to_string(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(
        manifest_json["checkpoint"]["resumed_segments_skipped"].as_u64(),
        Some(1),
        "the manifest must record that this run skipped a segment: {}",
        manifest_json
    );

    let warnings = manifest_json["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .filter_map(Value::as_str)
            .any(|w| w.contains("only the rest of the key space")),
        "a resumed run must warn that its output is partial: {:?}",
        warnings
    );

    // The identity list the manifest advertises must name every field that is
    // actually compared, or an operator cannot tell what resume verified.
    let fields: Vec<&str> = manifest_json["checkpoint"]["identity_fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        fields.contains(&"filter"),
        "identity_fields omits a field resume compares: {:?}",
        fields
    );
}

// ── Throttling must be visible and must be backed off ──────
//
// `operation_timeout_secs` served as three budgets at once: the SDK's
// per-attempt timeout, the SDK's whole-operation budget (attempts plus
// backoff), and the app's own per-page watchdog. With all three equal, the
// SDK's retry sequence was always cut off before it could return the
// `ServiceError` it was holding, so a bucket that was rate-limiting the run
// surfaced as a stream timeout. The throttle counters increment where that
// error is handled, so they stayed at zero — pointing an operator at "the
// endpoint is hanging" while the endpoint was saying "slow down".

/// Rate-limit the first `throttle_count` listing requests, then serve
/// normally. A transient throttle is the case worth getting right: the run
/// should ride it out, and should say that it happened.
fn recovering_throttle_server(throttle_count: usize) -> (MockS3Server, Arc<Mutex<Vec<Duration>>>) {
    let gaps: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(AtomicUsize::new(0));
    let last = Arc::new(Mutex::new(None::<std::time::Instant>));
    let (g, sn, lt) = (Arc::clone(&gaps), Arc::clone(&seen), Arc::clone(&last));

    let server = MockS3Server::start(move |request, _sequence| {
        if request.query.get("delimiter").map(String::as_str) == Some("/") {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        // Single-key probes are key-space partitioning, not listing attempts.
        // Serving them normally keeps the measurement to the retry cadence of
        // the segment loop, which is what is under test.
        if request.query.get("max-keys").map(String::as_str) == Some("1") {
            return MockResponse::ok_xml(list_bucket_xml(
                "",
                1,
                &["only-key.txt"],
                &[],
                false,
                None,
            ));
        }
        let now = std::time::Instant::now();
        {
            let mut last_at = lt.lock().unwrap();
            if let Some(prev) = *last_at {
                g.lock().unwrap().push(now.duration_since(prev));
            }
            *last_at = Some(now);
        }
        if sn.fetch_add(1, Ordering::SeqCst) < throttle_count {
            return MockResponse::error(503, "SlowDown", "Please reduce your request rate.");
        }
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &["only-key.txt"],
            &[],
            false,
            None,
        ))
    });
    (server, gaps)
}

fn throttle_test_args(
    dir: &std::path::Path,
    config: &std::path::Path,
    endpoint: String,
    manifest: Option<&std::path::Path>,
) -> Vec<String> {
    let mut args = vec![
        "--config".to_string(),
        config.display().to_string(),
        "--endpoint-url".into(),
        endpoint,
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        "--output-parquet-file".into(),
        dir.join("out.parquet").display().to_string(),
    ];
    if let Some(path) = manifest {
        args.extend(vec![
            "--run-manifest".to_string(),
            path.display().to_string(),
        ]);
    }
    args
}

/// The shipped defaults. All three matter here: the retry budget is what makes
/// the SDK's backoff sequence outlast the operation budget, and the two
/// timeouts are the budgets it outlasts.
fn write_default_config(path: &std::path::Path) {
    std::fs::write(
        path,
        r#"[s3]
max_attempts = 10
initial_backoff_secs = 1
connect_timeout_secs = 60
operation_timeout_secs = 5
"#,
    )
    .unwrap();
}

#[test]
fn local_mock_throttling_backs_off_and_is_reported_as_throttling() {
    // One run serves both checks (each costs the SDK's real backoff).
    let (server, gaps) = recovering_throttle_server(2);
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let manifest = dir.path().join("run.json");
    write_default_config(&config);

    let args = throttle_test_args(dir.path(), &config, server.endpoint(), Some(&manifest));
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(
        code, 0,
        "the endpoint recovered, so the run should too.\nstdout: {}\nstderr: {}",
        stdout, stderr
    );

    // Reported: the 503s are throttling, not timeouts.
    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    let metrics = &manifest_json["metrics"];
    assert!(
        metrics["throttled_responses"].as_u64().unwrap_or(0) >= 2,
        "the endpoint sent two 503 SlowDown responses; the run must report \
         them rather than counting only timeouts: {}",
        metrics
    );
    let statuses: Vec<u64> = metrics["http_error_statuses"]
        .as_array()
        .map(|a| a.iter().filter_map(|s| s["status"].as_u64()).collect())
        .unwrap_or_default();
    assert!(
        statuses.contains(&503),
        "the 503s must reach the status histogram: {}",
        metrics
    );

    // Backed off: re-issuing immediately is the wrong answer to `SlowDown`;
    // the endpoint has just asked for less load.
    let observed = gaps.lock().unwrap().clone();
    assert!(
        !observed.is_empty(),
        "expected retries after the throttled responses"
    );
    assert!(
        observed
            .iter()
            .all(|gap| *gap >= Duration::from_millis(500)),
        "a retry followed a SlowDown with no pause; gaps between listing \
         requests were {:?}",
        observed
    );
}

#[test]
fn local_mock_access_denied_is_a_setup_error_that_explains_itself() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(403, "AccessDenied", "Access Denied")
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let manifest = dir.path().join("run.json");
    write_fast_config(&config);

    let args = vec![
        "--config".to_string(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--no-auto-hints".into(),
        "--run-manifest".into(),
        manifest.display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
        "--output-format".into(),
        "summary".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    // Credentials/bucket problems are setup errors (3), not retryable network
    // failures (4), and the reason reaches stderr and the manifest.
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("AccessDenied"), "stderr: {}", stderr);
    let manifest_json: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(manifest_json["exit_code"], 3);
    assert!(
        manifest_json["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap_or("").contains("AccessDenied")),
        "{}",
        manifest_json["warnings"]
    );
}

#[test]
fn local_mock_missing_region_fails_fast_without_requests() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(500, "InternalError", "should not be reached")
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);

    let started = std::time::Instant::now();
    let output = common::hermetic_command(dir.path())
        .current_dir(dir.path())
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env_remove("AWS_REGION")
        .env_remove("AWS_DEFAULT_REGION")
        .env_remove("AWS_PROFILE")
        .env("AWS_CONFIG_FILE", dir.path().join("no-aws-config"))
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            dir.path().join("no-aws-credentials"),
        )
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args([
            "--config",
            config.to_str().unwrap(),
            "--endpoint-url",
            &server.endpoint(),
            "--addressing-style",
            "path",
            "list",
            "--bucket",
            "mock-bucket",
            "--output-format",
            "summary",
        ])
        .output()
        .expect("run s3-turbo-list");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(3), "stderr: {}", stderr);
    assert!(stderr.contains("--region"), "stderr: {}", stderr);
    assert!(server.requests().is_empty());
    assert!(started.elapsed() < Duration::from_secs(20));
}

#[test]
fn local_mock_manifest_check_verifies_artifacts_against_metrics() {
    let server = MockS3Server::start(|request, _sequence| {
        MockResponse::ok_xml(list_bucket_xml(
            request
                .query
                .get("prefix")
                .map(String::as_str)
                .unwrap_or(""),
            1000,
            &["logs/a.txt", "logs/b.txt", "logs/c.txt"],
            &[],
            false,
            None,
        ))
    });
    let dir = tempfile::tempdir().unwrap();
    let run_dir = dir.path().join("run1");
    std::fs::create_dir_all(&run_dir).unwrap();
    let config = run_dir.join("config.toml");
    write_fast_config(&config);
    // Relative output paths, as an agent would pass them.
    let args: Vec<String> = [
        "--config",
        "config.toml",
        "--endpoint-url",
        &server.endpoint(),
        "--addressing-style",
        "path",
        "list",
        "--no-auto-hints",
        "--run-manifest",
        "run.json",
        "--output-dir",
        "out",
        "--bucket",
        "mock-bucket",
        "--region",
        "us-east-1",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let (code, stdout, stderr) = run_cli(&args, &run_dir);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // --check from the parent directory resolves the run-relative artifact
    // paths against the run's cwd (it used to report them missing, exit 6).
    let check = |manifest: &std::path::Path| {
        run_cli(
            &[
                "manifest-summary".to_string(),
                manifest.display().to_string(),
                "--check".to_string(),
                "--json".to_string(),
            ],
            dir.path(),
        )
    };
    let manifest = run_dir.join("run.json");
    let (code, stdout, stderr) = check(&manifest);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    // A manifest whose metrics claim more rows than its Parquet artifacts
    // hold must fail: the two in-memory counters agree with each other.
    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    value["metrics"]["parquet_rows"] = 4.into();
    value["metrics"]["streamed_rows"] = 4.into();
    let tampered = run_dir.join("tampered.json");
    std::fs::write(&tampered, serde_json::to_string(&value).unwrap()).unwrap();
    let (code, stdout, _) = check(&tampered);
    assert_eq!(code, 6, "{}", stdout);
    assert!(stdout.contains("artifact_parquet_rows_total"), "{}", stdout);

    // A Parquet artifact recorded without metadata (unreadable footer) fails.
    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    for artifact in value["artifacts"].as_array_mut().unwrap() {
        if artifact["kind"] == "parquet" {
            artifact["parquet"] = Value::Null;
        }
    }
    std::fs::write(&tampered, serde_json::to_string(&value).unwrap()).unwrap();
    let (code, stdout, _) = check(&tampered);
    assert_eq!(code, 6, "{}", stdout);
    assert!(
        stdout.contains("no recorded Parquet metadata"),
        "{}",
        stdout
    );
}

#[test]
fn local_mock_delimiter_listing_of_only_folders_emits_them() {
    // A bucket whose top level holds only "folders" used to list as empty.
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(
            request.query.get("delimiter").map(String::as_str),
            Some("/")
        );
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &[],
            &["dir0/", "dir1/", "dir2/"],
            false,
            None,
        ))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let args: Vec<String> = [
        "--config",
        config.to_str().unwrap(),
        "--endpoint-url",
        &server.endpoint(),
        "--addressing-style",
        "path",
        "list",
        "--delimiter",
        "/",
        "--bucket",
        "mock-bucket",
        "--region",
        "us-east-1",
        "--output-format",
        "ndjson",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let rows: Vec<Value> = stdout
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let keys: Vec<&str> = rows.iter().map(|r| r["k"].as_str().unwrap()).collect();
    assert_eq!(keys, vec!["dir0/", "dir1/", "dir2/"]);
    assert!(rows.iter().all(|r| r["s"] == 0 && r["m"] == 0));

    // `--filter` selects objects, not folders: a size predicate must not
    // drop the folder rows (Size 0) and bring the empty listing back.
    let mut filtered = args.clone();
    filtered.extend(["--filter".to_string(), "SOURCE.size > 0".to_string()]);
    let (code, stdout, stderr) = run_cli(&filtered, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(stdout.lines().count(), 3, "stdout: {}", stdout);
}

#[test]
fn local_mock_manifest_check_follows_a_moved_run_directory() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &["a/1.txt", "b/2.txt"],
            &[],
            false,
            None,
        ))
    });
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run1");
    std::fs::create_dir(&run_dir).unwrap();
    let config = run_dir.join("config.toml");
    write_fast_config(&config);
    let args: Vec<String> = [
        "--config",
        config.to_str().unwrap(),
        "--endpoint-url",
        &server.endpoint(),
        "--addressing-style",
        "path",
        "list",
        "--no-auto-hints",
        "--output-parquet-file",
        "out.parquet",
        "--run-manifest",
        "run.json",
        "--bucket",
        "mock-bucket",
        "--region",
        "us-east-1",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let (code, _stdout, stderr) = run_cli(&args, &run_dir);
    assert_eq!(code, 0, "stderr: {}", stderr);

    // The run directory moves; the manifest's recorded cwd no longer exists.
    let moved = root.path().join("moved");
    std::fs::rename(&run_dir, &moved).unwrap();
    let check: Vec<String> = ["manifest-summary", "run.json", "--check", "--json"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let (code, stdout, stderr) = run_cli(&check, &moved);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let report: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["check"]["parquet_schema_check"], "ok");

    // A missing Parquet artifact fails the schema check rather than reading
    // "not applicable".
    std::fs::remove_file(moved.join("out.parquet")).unwrap();
    let (code, stdout, _stderr) = run_cli(&check, &moved);
    assert_eq!(code, 6);
    let report: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["check"]["parquet_schema_check"], "fail");
}

#[test]
fn local_mock_diff_side_access_denied_exits_3_with_the_s3_reason() {
    // A side's AccessDenied aborts the merge; that used to be counted as an
    // output failure, so the run exited 5 "an output write failed".
    let server = MockS3Server::start(|request, _sequence| {
        if request.path.starts_with("/dst") {
            MockResponse::error(403, "AccessDenied", "Access Denied")
        } else {
            MockResponse::ok_xml(list_bucket_xml(
                "",
                1000,
                &["a/1.txt", "b/2.txt"],
                &[],
                false,
                None,
            ))
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let args: Vec<String> = [
        "--config",
        config.to_str().unwrap(),
        "--endpoint-url",
        &server.endpoint(),
        "--addressing-style",
        "path",
        "diff",
        "--no-auto-hints",
        "--output-parquet-file",
        "diff.parquet",
        "--bucket",
        "src",
        "--region",
        "us-east-1",
        "--target-bucket",
        "dst",
        "--target-region",
        "us-east-1",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("AccessDenied"), "stderr: {}", stderr);
    assert!(!dir.path().join("diff.parquet").exists());
}

#[cfg(unix)]
#[test]
fn local_mock_interrupted_then_resumed_run_lists_every_key_exactly_once() {
    // The checkpoint used to record only whole, unsplit segments, while an
    // interrupt still wrote the rows of partly listed ones — so `--resume`
    // listed those again from their start and the documented "combine both
    // outputs" produced duplicates. It now records the unwritten ranges.
    let keys: Vec<String> = (0..6000).map(|i| format!("k{:05}", i)).collect();
    let served = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        thread::sleep(Duration::from_millis(40));
        let after = request
            .query
            .get("continuation-token")
            .or_else(|| request.query.get("start-after"))
            .cloned()
            .unwrap_or_default();
        let max_keys: usize = request
            .query
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let page: Vec<&str> = served
            .iter()
            .filter(|k| k.as_str() > after.as_str())
            .take(max_keys + 1)
            .map(String::as_str)
            .collect();
        let truncated = page.len() > max_keys;
        let page = &page[..page.len().min(max_keys)];
        let token = truncated.then(|| page.last().unwrap().to_string());
        MockResponse::ok_xml(list_bucket_xml(
            "",
            max_keys as i32,
            page,
            &[],
            truncated,
            token.as_deref(),
        ))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let hints = dir.path().join("hints.txt");
    std::fs::write(&hints, "k01500\nk03000\nk04500\n").unwrap();
    let args: Vec<String> = [
        "--config",
        config.to_str().unwrap(),
        "--endpoint-url",
        &server.endpoint(),
        "--addressing-style",
        "path",
        "list",
        "--max-keys",
        "50",
        "-c",
        "4",
        "--hints-file",
        hints.to_str().unwrap(),
        "--bucket",
        "mock-bucket",
        "--region",
        "us-east-1",
        "--output-format",
        "tsv",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let child = common::hermetic_command(dir.path())
        .current_dir(dir.path())
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // Interrupt once the listing is well under way (not on a timer, which a
    // slow CI runner could hit before any page or a fast one after the last).
    let waited = std::time::Instant::now();
    while server.requests().len() < 20 && waited.elapsed() < Duration::from_secs(20) {
        thread::sleep(Duration::from_millis(10));
    }
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let first = child.wait_with_output().unwrap();
    assert_eq!(
        first.status.code(),
        Some(7),
        "stderr: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_rows = String::from_utf8(first.stdout).unwrap();
    assert!(
        first_rows.lines().count() < keys.len(),
        "the first run must be interrupted mid-listing"
    );

    // The first run was not started with --resume: an interrupted run saves
    // its checkpoint regardless, and the next run opts into reading it.
    let mut args = args;
    args.push("--resume".to_string());
    let (code, second_rows, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stderr: {}", stderr);
    // The partial-output warning reaches stderr, not only the manifest.
    assert!(
        stderr.contains("Resuming from checkpoint"),
        "stderr: {}",
        stderr
    );

    let mut listed: Vec<&str> = first_rows
        .lines()
        .chain(second_rows.lines())
        .map(|line| line.split('\t').next().unwrap())
        .collect();
    let total = listed.len();
    listed.sort_unstable();
    listed.dedup();
    assert_eq!(total, listed.len(), "resume produced duplicate rows");
    assert_eq!(listed, keys.iter().map(String::as_str).collect::<Vec<_>>());
}

#[cfg(unix)]
#[test]
fn local_mock_interrupt_during_startup_discovery_stops_promptly() {
    // Every probe is slow and the key space looks flat and endless, so
    // startup discovery would run dozens of probe rounds. An interrupt used
    // to be ignored until discovery finished (a minute here) and was then
    // followed by a full first fill of segment tasks.
    let server = MockS3Server::start(|_request, _sequence| {
        thread::sleep(Duration::from_millis(1500));
        MockResponse::ok_xml(list_bucket_xml("", 1, &["k0000"], &[], true, Some("t1")))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let mut child = common::hermetic_command(dir.path())
        .current_dir(dir.path())
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args([
            "--config",
            config.to_str().unwrap(),
            "--endpoint-url",
            &server.endpoint(),
            "--addressing-style",
            "path",
            "list",
            "--output-format",
            "summary",
            "--bucket",
            "mock-bucket",
            "--region",
            "us-east-1",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(700));
    let started = std::time::Instant::now();
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let code = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status.code();
        }
        if started.elapsed() > Duration::from_secs(20) {
            let _ = child.kill();
            panic!("run did not stop within 20s of SIGINT");
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(code, Some(7));
    assert!(started.elapsed() < Duration::from_secs(8));
    let cached: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("_hints.toml"))
        .collect();
    assert!(
        cached.is_empty(),
        "interrupted discovery must not cache boundaries"
    );
}

#[test]
fn local_mock_list_routes_through_http_proxy_from_environment() {
    // The mock plays the forward proxy: the endpoint host does not resolve,
    // so the listing can only succeed through HTTP_PROXY, and a proxied plain
    // HTTP request names its target in absolute form.
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &["a/1.txt", "b/2.txt"],
            &[],
            false,
            None,
        ))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let endpoint = "http://s3-proxy-only.invalid:9000";
    let args: Vec<String> = [
        "--config",
        &config.display().to_string(),
        "--endpoint-url",
        endpoint,
        "--addressing-style",
        "path",
        "list",
        "--no-auto-hints",
        "--bucket",
        "mock-bucket",
        "--region",
        "us-east-1",
        "--output-format",
        "tsv",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let proxy = server.endpoint();
    let (code, stdout, stderr) =
        run_cli_with_env(&args, dir.path(), &[("HTTP_PROXY", proxy.as_str())]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(stdout.lines().count(), 2, "stdout: {}", stdout);
    // The run names the proxy for the resolved request URL before any request.
    assert!(
        stderr.contains(&format!(
            "source requests to http://s3-proxy-only.invalid:9000/mock-bucket go through proxy {}",
            proxy
        )),
        "stderr: {}",
        stderr
    );
    let requests = server.requests();
    assert!(!requests.is_empty());
    for request in &requests {
        assert!(
            request
                .path
                .starts_with("http://s3-proxy-only.invalid:9000/mock-bucket"),
            "request should reach the proxy in absolute form: {}",
            request.path
        );
    }

    // NO_PROXY exempts the host: the run connects directly, cannot resolve
    // the endpoint, and the proxy sees nothing new.
    let before = server.requests().len();
    let (code, _stdout, stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[("HTTP_PROXY", proxy.as_str()), ("NO_PROXY", ".invalid")],
    );
    assert_ne!(code, 0, "stderr: {}", stderr);
    assert_eq!(server.requests().len(), before);
}

#[test]
fn local_mock_doctor_reports_the_proxy_without_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let args: Vec<String> = [
        "--endpoint-url",
        "https://storage.example.test",
        "--addressing-style",
        "path",
        "doctor",
        "--json",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let (code, stdout, stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[("HTTPS_PROXY", "http://user:secret@proxy.example.test:3128")],
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "proxy")
        .expect("proxy check");
    let message = check["message"].as_str().unwrap();
    assert!(
        message.contains("http://proxy.example.test:3128"),
        "{}",
        message
    );
    assert!(!message.contains("secret"), "{}", message);
    assert!(!stdout.contains("secret"), "{}", stdout);

    let (_code, stdout, _stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[
            ("HTTPS_PROXY", "http://proxy.example.test:3128"),
            ("NO_PROXY", "example.test"),
        ],
    );
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "proxy")
        .expect("proxy check");
    assert!(
        check["message"]
            .as_str()
            .unwrap()
            .contains("connect directly"),
        "{}",
        check["message"]
    );
}

#[test]
fn local_mock_doctor_skips_the_proxy_check_when_the_host_depends_on_the_bucket() {
    // Without an explicit path-style endpoint the request host is
    // bucket-qualified and regional (e.g. <bucket>.s3.us-west-2.amazonaws.com);
    // doctor has no bucket, so any stand-in host could disagree with a
    // NO_PROXY rule. It reports the check as skipped instead of guessing.
    let dir = tempfile::tempdir().unwrap();
    for extra in [
        vec![],
        vec!["--endpoint-url", "https://storage.example.test"],
    ] {
        let mut args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        args.extend(["doctor".to_string(), "--json".to_string()]);
        let (code, stdout, stderr) = run_cli_with_env(
            &args,
            dir.path(),
            &[
                ("HTTPS_PROXY", "http://proxy.example.test:3128"),
                ("NO_PROXY", ".s3.us-west-2.amazonaws.com"),
            ],
        );
        assert!(
            code == 0 || code == 2,
            "stdout: {}\nstderr: {}",
            stdout,
            stderr
        );
        let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "proxy")
            .expect("proxy check");
        assert_eq!(check["status"], "skipped", "{}", check);
    }
}

// ── Truncated pages without a followable token ─────────────
//
// A page that says IsTruncated=true but gives no usable NextContinuationToken,
// or hands back the token it was just sent, is not the end of the listing.
// The SDK paginator used to end the stream there, and the run exited 0 with
// the rest of the range silently missing. Such a page now fails the attempt
// at the last key seen, so the retry loop resumes with start-after.

const GUARD_KEYS: [&str; 6] = ["k1", "k2", "k3", "k4", "k5", "k6"];

/// Keys after `start_after`, at most two per page, and whether more remain.
fn guard_page(start_after: Option<&str>) -> (Vec<&'static str>, bool) {
    let rest: Vec<&str> = GUARD_KEYS
        .iter()
        .copied()
        .filter(|key| start_after.is_none_or(|after| *key > after))
        .collect();
    let page: Vec<&str> = rest.iter().copied().take(2).collect();
    (page, rest.len() > 2)
}

fn guard_list_args(server: &MockS3Server, dir: &std::path::Path) -> Vec<String> {
    let config = dir.join("config.toml");
    write_fast_config(&config);
    vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--no-auto-hints".into(),
        "--output-parquet-file".into(),
        dir.join("out.parquet").display().to_string(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--region".into(),
        "us-east-1".into(),
    ]
}

#[test]
fn local_mock_truncated_page_without_token_resumes_with_start_after() {
    let server = MockS3Server::start(|request, _sequence| {
        assert!(!request.query.contains_key("continuation-token"));
        let (page, more) = guard_page(request.query.get("start-after").map(String::as_str));
        // IsTruncated=true while keys remain, but never a token.
        MockResponse::ok_xml(list_bucket_xml("", 2, &page, &[], more, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = guard_list_args(&server, dir.path());
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(parquet_keys(&dir.path().join("out.parquet")), GUARD_KEYS);

    let start_afters: Vec<Option<String>> = server
        .requests()
        .iter()
        .map(|r| r.query.get("start-after").cloned())
        .collect();
    assert_eq!(
        start_afters,
        vec![None, Some("k2".to_string()), Some("k4".to_string())]
    );
}

#[test]
fn local_mock_repeated_continuation_token_resumes_with_start_after() {
    let server = MockS3Server::start(|request, _sequence| {
        match request.query.get("continuation-token").map(String::as_str) {
            // The first page hands out a token; following it returns the
            // next page but repeats the same token, still truncated.
            Some("t1") => {
                let (page, more) = guard_page(Some("k2"));
                MockResponse::ok_xml(list_bucket_xml("", 2, &page, &[], more, Some("t1")))
            }
            Some(other) => MockResponse::error(400, "InvalidToken", other),
            None => {
                let start_after = request.query.get("start-after").map(String::as_str);
                let (page, more) = guard_page(start_after);
                let token = (start_after.is_none() && more).then_some("t1");
                MockResponse::ok_xml(list_bucket_xml("", 2, &page, &[], more, token))
            }
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let args = guard_list_args(&server, dir.path());
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    // Previously 4 of 6: the paginator stopped at the repeated token.
    assert_eq!(parquet_keys(&dir.path().join("out.parquet")), GUARD_KEYS);
    assert!(
        server
            .requests()
            .iter()
            .any(|r| r.query.get("start-after").map(String::as_str) == Some("k4")),
        "the retry must resume after the last key listed: {:#?}",
        server.requests()
    );
}

#[test]
fn local_mock_truncated_page_that_never_advances_fails_the_run() {
    let requests_served = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&requests_served);
    let server = MockS3Server::start(move |_request, _sequence| {
        served.fetch_add(1, Ordering::SeqCst);
        // Ignores start-after: always the first page, truncated, no token.
        MockResponse::ok_xml(list_bucket_xml("", 2, &["k1", "k2"], &[], true, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = guard_list_args(&server, dir.path());
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_ne!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    // One advancing attempt, then max_attempts (3) that make no progress.
    assert_eq!(requests_served.load(Ordering::SeqCst), 4);
}

// Runs started at the same moment over the same bucket used to pick the same
// auto-generated name (the `_N` check ran long before the files were
// created) and all reported success over one set of files; `--log` files were
// never suffixed at all, so earlier manifests then failed `--check`.
#[test]
fn local_mock_concurrent_runs_get_distinct_outputs_and_logs() {
    let server = MockS3Server::start(|request, _sequence| {
        let prefix = request.query.get("prefix").cloned().unwrap_or_default();
        MockResponse::ok_xml(list_bucket_xml(
            &prefix,
            1000,
            &["a", "b"],
            &[],
            false,
            None,
        ))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let args: Vec<String> = [
        "--config",
        config.to_str().unwrap(),
        "--endpoint-url",
        &server.endpoint(),
        "--addressing-style",
        "path",
        "list",
        "--bucket",
        "mock-bucket",
        "--region",
        "us-east-1",
        "--output-dir",
        "out",
        "--log",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let runs: Vec<_> = (0..4)
        .map(|_| {
            let args = args.clone();
            let cwd = dir.path().to_path_buf();
            thread::spawn(move || run_cli(&args, &cwd))
        })
        .collect();
    for run in runs {
        let (code, stdout, stderr) = run.join().unwrap();
        assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    }
    let names = |ext: &str| {
        std::fs::read_dir(dir.path().join("out"))
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|name| name.ends_with(ext))
            .count()
    };
    assert_eq!(names(".parquet"), 4);
    assert_eq!(names(".ks"), 4);
    assert_eq!(names(".log"), 4);
}

// An output the run cannot create used to surface only after the whole
// listing (and its requests) had been paid for.
#[test]
fn local_mock_uncreatable_output_fails_before_any_request() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::ok_xml(list_bucket_xml("", 1000, &["a"], &[], false, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let manifest_dir = dir.path().join("manifest-is-a-dir");
    std::fs::create_dir(&manifest_dir).unwrap();
    let args: Vec<String> = [
        "--config",
        config.to_str().unwrap(),
        "--endpoint-url",
        &server.endpoint(),
        "--addressing-style",
        "path",
        "list",
        "--bucket",
        "mock-bucket",
        "--region",
        "us-east-1",
        "--output-dir",
        "out",
        "--run-manifest",
        manifest_dir.to_str().unwrap(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let (code, _stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 5, "stderr: {}", stderr);
    assert!(stderr.contains("is a directory"), "{}", stderr);
    assert!(server.requests().is_empty(), "{:#?}", server.requests());
    // The name it reserved is released again: no empty output left behind.
    let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("out"))
        .map(|entries| entries.filter_map(Result::ok).collect())
        .unwrap_or_default();
    assert!(leftovers.is_empty(), "{:?}", leftovers);
}

// ── Delimiter runs restarted at a CommonPrefix ───────────────
//
// A ListObjectsV2 emulator with prefix, delimiter, start-after,
// continuation-token and max-keys semantics: a start-after that names a
// CommonPrefix returns that prefix again, because the keys under it sort
// after it and roll up into it.
fn emulated_list(keys: &[String], q: &BTreeMap<String, String>) -> String {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let delim = q.get("delimiter").cloned().unwrap_or_default();
    let max: usize = q
        .get("max-keys")
        .and_then(|m| m.parse().ok())
        .unwrap_or(1000);
    let after = q
        .get("continuation-token")
        .cloned()
        .or_else(|| q.get("start-after").cloned())
        .unwrap_or_default();
    let mut entries: Vec<(String, bool)> = Vec::new(); // (name, is_prefix)
    for k in keys
        .iter()
        .filter(|k| k.starts_with(&prefix) && k.as_str() > after.as_str())
    {
        let rest = &k[prefix.len()..];
        let entry = match (!delim.is_empty()).then(|| rest.find(&delim)).flatten() {
            Some(i) => (format!("{}{}", prefix, &rest[..i + delim.len()]), true),
            None => (k.clone(), false),
        };
        if entries.last().is_some_and(|last| last.0 == entry.0) {
            continue;
        }
        // A continuation token naming a prefix resumes past the whole prefix.
        if q.contains_key("continuation-token") && entry.1 && entry.0 == after {
            continue;
        }
        entries.push(entry);
        if entries.len() > max {
            break;
        }
    }
    let truncated = entries.len() > max;
    entries.truncate(max);
    let contents: Vec<&str> = entries
        .iter()
        .filter(|e| !e.1)
        .map(|e| e.0.as_str())
        .collect();
    let prefixes: Vec<&str> = entries
        .iter()
        .filter(|e| e.1)
        .map(|e| e.0.as_str())
        .collect();
    let token = truncated.then(|| entries.last().unwrap().0.clone());
    list_bucket_xml(
        &prefix,
        max as i32,
        &contents,
        &prefixes,
        truncated,
        token.as_deref(),
    )
}

#[test]
fn local_mock_delimiter_retry_after_common_prefix_emits_each_folder_once() {
    // Two entries per page, so the first page ends on the CommonPrefix b/.
    let keys: Vec<String> = ["a/1", "a/2", "b/1", "b/2", "c/1", "d.txt", "e.txt"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let failed = Arc::new(AtomicBool::new(false));
    let failed_once = failed.clone();
    let server = MockS3Server::start(move |request, _| {
        // Fail the first continuation request once: the retry restarts the
        // chain with start-after=b/.
        if request.query.contains_key("continuation-token")
            && !failed_once.swap(true, Ordering::SeqCst)
        {
            return MockResponse::error(500, "InternalError", "boom");
        }
        MockResponse::ok_xml(emulated_list(&keys, &request.query))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let args: Vec<String> = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "list".into(),
        "--bucket".into(),
        "b".into(),
        "--region".into(),
        "us-east-1".into(),
        "--delimiter".into(),
        "/".into(),
        "--max-keys".into(),
        "2".into(),
        "--output-format".into(),
        "tsv".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        failed.load(Ordering::SeqCst),
        "the retry path was not exercised"
    );
    let rows: Vec<&str> = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.split('\t').next().unwrap())
        .collect();
    assert_eq!(rows, ["a/", "b/", "c/", "d.txt", "e.txt"], "{}", stdout);
}

#[test]
fn local_mock_compat_probe_agent_report_and_trace_carry_the_contract_fields() {
    // Under --agent stderr stays quiet: the report on stdout is the result,
    // with the fields every other JSON result has.
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(501, "NotImplemented", "not supported")
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    let trace = dir.path().join("trace.jsonl");
    write_fast_config(&config);
    let args: Vec<String> = vec![
        "--config".into(),
        config.display().to_string(),
        "--provider".into(),
        "minio".into(),
        "compat-probe".into(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--region".into(),
        "us-east-1".into(),
        "--bucket".into(),
        "mock-bucket".into(),
        "--prefix".into(),
        "probe/".into(),
        "--trace-compat".into(),
        trace.display().to_string(),
        "--agent".into(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 4, "stdout: {}\nstderr: {}", stdout, stderr);
    let report: Value = serde_json::from_str(&stdout).expect("report JSON on stdout");
    assert_eq!(report["schema_version"], "s3-turbo-list.agent.v1");
    assert_eq!(report["status"], "failed");
    assert_eq!(report["exit_code"], 4);
    assert_eq!(report["warnings"], serde_json::json!([]));
    // Only the run line on stderr.
    assert_eq!(stderr.lines().count(), 1, "{}", stderr);
    assert!(
        stderr.starts_with("s3-turbo-list: run failed (exit 4)"),
        "{}",
        stderr
    );
    // Probe trace events record the prefix and the provider, as listing
    // events do.
    let events: Vec<Value> = std::fs::read_to_string(&trace)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(!events.is_empty());
    for event in &events {
        assert_eq!(event["prefix"], "probe/", "{}", event);
        assert_eq!(event["provider"], "minio", "{}", event);
        assert!(event.get("profile").is_none(), "{}", event);
    }
}

#[test]
fn local_mock_delimiter_start_after_inside_a_folder_keeps_the_folder_row() {
    // --start-after a/b/c: a/b/d and a/z sort after it and roll up into a/,
    // which the endpoint returns and the run must emit (0.38 dropped every
    // CommonPrefix sorting before the start key).
    let cases: [(&[&str], &[&str], &[&str]); 2] = [
        (
            &["a/b/1", "a/b/c", "a/b/d", "a/z", "b.txt"],
            &[],
            &["a/", "b.txt"],
        ),
        (
            &["a/b/1", "a/b/c", "a/b/d", "a/z"],
            &["--prefix", "a/"],
            &["a/b/", "a/z"],
        ),
    ];
    for (keys, extra, expected) in cases {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        let server = MockS3Server::start(move |request, _| {
            MockResponse::ok_xml(emulated_list(&keys, &request.query))
        });
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        write_fast_config(&config);
        let mut args: Vec<String> = vec![
            "--config".into(),
            config.display().to_string(),
            "--endpoint-url".into(),
            server.endpoint(),
            "--addressing-style".into(),
            "path".into(),
            "list".into(),
            "--bucket".into(),
            "b".into(),
            "--region".into(),
            "us-east-1".into(),
            "--output-format".into(),
            "tsv".into(),
            "--delimiter".into(),
            "/".into(),
            "--start-after".into(),
            "a/b/c".into(),
        ];
        args.extend(extra.iter().map(|s| s.to_string()));
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
        let rows: Vec<&str> = stdout
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| l.split('\t').next().unwrap())
            .collect();
        assert_eq!(rows, expected, "{}", stdout);
    }
}

// ── Setup errors that are not the endpoint's ─────────────────

/// Run with no credentials anywhere the SDK looks: no key variables, no
/// profile, an empty HOME (no shared config files), no instance metadata.
fn run_cli_without_credentials(args: &[String], cwd: &std::path::Path) -> (i32, String, String) {
    let mut command = common::hermetic_command(cwd);
    for var in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    ] {
        command.env_remove(var);
    }
    let output = command
        .current_dir(cwd)
        .env("HOME", cwd)
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

#[test]
fn local_mock_missing_credentials_is_a_setup_error_without_retries() {
    // The SDK reports no credentials as a dispatch failure, which used to be
    // retried as a network error for minutes and then exit 4.
    let server = MockS3Server::start(|request, _| {
        MockResponse::ok_xml(emulated_list(&["k".to_string()], &request.query))
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        "[s3]\nmax_attempts = 3\ninitial_backoff_secs = 1\nconnect_timeout_secs = 2\noperation_timeout_secs = 2\n",
    )
    .unwrap();
    let base = |cmd: &str| -> Vec<String> {
        vec![
            "--config".into(),
            config.display().to_string(),
            "--endpoint-url".into(),
            server.endpoint(),
            "--addressing-style".into(),
            "path".into(),
            cmd.into(),
            "--bucket".into(),
            "b".into(),
            "--region".into(),
            "us-east-1".into(),
        ]
    };
    let started = std::time::Instant::now();
    let mut list = base("list");
    list.extend(["--output-format".into(), "summary".into()]);
    let (code, stdout, stderr) = run_cli_without_credentials(&list, dir.path());
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stderr.contains("no AWS credentials found"), "{}", stderr);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "retried for {:?}",
        started.elapsed()
    );

    let (code, stdout, stderr) = run_cli_without_credentials(&base("compat-probe"), dir.path());
    assert_eq!(code, 3, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.contains("credentials_missing"), "{}", stdout);
    // Nothing left the process.
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[test]
fn local_mock_compat_probe_without_a_region_is_a_setup_error() {
    let server = MockS3Server::start(|_, _| MockResponse::error(500, "InternalError", "unused"));
    let dir = tempfile::tempdir().unwrap();
    let args: Vec<String> = vec![
        "--endpoint-url".into(),
        server.endpoint(),
        "compat-probe".into(),
        "--bucket".into(),
        "b".into(),
    ];
    let output = common::hermetic_command(dir.path())
        .current_dir(dir.path())
        .env("HOME", dir.path())
        .env_remove("AWS_REGION")
        .env_remove("AWS_DEFAULT_REGION")
        .env_remove("AWS_PROFILE")
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args(&args)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    // It was exit 5, "output error".
    assert_eq!(output.status.code(), Some(3), "{}", stderr);
    assert!(stderr.contains("no region"), "{}", stderr);
}

#[test]
fn local_mock_trace_and_probe_report_redact_endpoint_userinfo() {
    let server = MockS3Server::start(|request, _| match request.method.as_str() {
        "HEAD" => MockResponse::empty_ok(),
        _ => MockResponse::ok_xml(emulated_list(&["k".to_string()], &request.query)),
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_fast_config(&config);
    let endpoint = server
        .endpoint()
        .replacen("http://", "http://user:secretpw@", 1);
    let trace = dir.path().join("trace.jsonl");
    let report = dir.path().join("report.json");
    let args: Vec<String> = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        endpoint,
        "--addressing-style".into(),
        "path".into(),
        "compat-probe".into(),
        "--bucket".into(),
        "b".into(),
        "--region".into(),
        "us-east-1".into(),
        "--trace-compat".into(),
        trace.display().to_string(),
        "--output".into(),
        report.display().to_string(),
    ];
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    for path in [&trace, &report] {
        let text = std::fs::read_to_string(path).unwrap();
        assert!(!text.is_empty());
        assert!(!text.contains("secretpw"), "{}: {}", path.display(), text);
    }
}

// ── Interrupts at the edges ──────────────────────────────────

/// SIGTERM after the listing finished while stdout is still held: the rows
/// are all written, so the run reports success in every mode (it exited 7
/// "partial" with --start-after or another job's checkpoint in place).
#[cfg(unix)]
#[test]
fn local_mock_interrupt_after_the_listing_finished_reports_success() {
    let keys: Vec<String> = (0..5000)
        .map(|i| format!("key-{:06}-padding-padding-padding", i))
        .collect();
    let foreign_checkpoint = r#"bucket = "b"
prefix = ""
last_updated = "x"
listed_ranges = 1
[identity]
bucket = "b"
region = "us-east-1"
prefix = ""
delimiter = ""
max_keys = 5
provider = "aws"
addressing_style = "path"
mode = "list"
[[remaining]]
start_after = "key-000100"
"#;
    for (label, extra, foreign) in [
        ("plain", vec![], false),
        ("start-after", vec!["--start-after", "k"], false),
        ("another job's checkpoint", vec![], true),
    ] {
        let served = keys.clone();
        let server = MockS3Server::start(move |request, _| {
            MockResponse::ok_xml(emulated_list(&served, &request.query))
        });
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        write_fast_config(&config);
        if foreign {
            std::fs::write(
                dir.path().join("us-east-1_b_checkpoint.toml"),
                foreign_checkpoint,
            )
            .unwrap();
        }
        let mut args: Vec<String> = vec![
            "--config".into(),
            config.display().to_string(),
            "--endpoint-url".into(),
            server.endpoint(),
            "--addressing-style".into(),
            "path".into(),
            "list".into(),
            "--bucket".into(),
            "b".into(),
            "--region".into(),
            "us-east-1".into(),
            "--output-format".into(),
            "tsv".into(),
            "--no-auto-hints".into(),
        ];
        args.extend(extra.iter().map(|s| s.to_string()));
        let child = common::hermetic_command(dir.path())
            .current_dir(dir.path())
            .env("AWS_ACCESS_KEY_ID", "mock-access-key")
            .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        // Five 1000-key pages list everything; stdout is not read yet, so
        // the writer is still busy when the signal arrives.
        let waited = std::time::Instant::now();
        while server.requests().len() < 5 && waited.elapsed() < Duration::from_secs(20) {
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(500));
        let _ = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        let output = child.wait_with_output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        let rows = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|l| !l.is_empty())
            .count();
        assert_eq!(rows, keys.len(), "{}: {}", label, stderr);
        assert_eq!(output.status.code(), Some(0), "{}: {}", label, stderr);
    }
}

/// Ctrl-C while diff segments sit in a retry backoff: the exit waited out
/// the backoff (up to 30 s); list mode already stopped at once.
#[cfg(unix)]
#[test]
fn local_mock_diff_ctrl_c_during_retry_backoff_exits_promptly() {
    let keys: Vec<String> = (0..50).map(|i| format!("k{:03}", i)).collect();
    let server = MockS3Server::start(move |request, _| {
        if request.query.contains_key("delimiter")
            || request.query.get("max-keys").map(String::as_str) == Some("1")
        {
            return MockResponse::ok_xml(emulated_list(&keys, &request.query));
        }
        MockResponse::error(500, "InternalError", "flaky")
    });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        "[s3]\nmax_attempts = 10\ninitial_backoff_secs = 1\noperation_timeout_secs = 2\nconnect_timeout_secs = 2\n",
    )
    .unwrap();
    let args: Vec<String> = vec![
        "--config".into(),
        config.display().to_string(),
        "--endpoint-url".into(),
        server.endpoint(),
        "--addressing-style".into(),
        "path".into(),
        "diff".into(),
        "--bucket".into(),
        "left".into(),
        "--region".into(),
        "us-east-1".into(),
        "--target-bucket".into(),
        "right".into(),
        "--output-dir".into(),
        "out".into(),
    ];
    let mut child = common::hermetic_command(dir.path())
        .current_dir(dir.path())
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args(&args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    // A few failed attempts in: the backoff has grown past several seconds.
    thread::sleep(Duration::from_secs(8));
    let signalled = std::time::Instant::now();
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
    let code = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status.code();
        }
        if signalled.elapsed() > Duration::from_secs(60) {
            let _ = child.kill();
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(code, Some(7));
    assert!(
        signalled.elapsed() < Duration::from_secs(2),
        "exit took {:?} after Ctrl-C",
        signalled.elapsed()
    );
}
