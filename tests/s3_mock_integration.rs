//! End-to-end runs of the `s3tl` binary against an in-process S3 protocol
//! mock (127.0.0.1 only; nothing here contacts a real endpoint).
//!
//! Each test drives the engine through the 1.0 command line and asserts on
//! the result document (`s3tl.result/1`), the Parquet dataset directory, the
//! NDJSON rows, the checkpoint and the requests the mock saw.

mod common;

use arrow::array::{Array, RecordBatch, StringArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ── The mock server ─────────────────────────────────────────

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    /// Request target without the query (absolute form through a proxy).
    path: String,
    query: BTreeMap<String, String>,
    /// Header names lowercased.
    headers: BTreeMap<String, String>,
    /// The request body (`Content-Length` bytes).
    body: Vec<u8>,
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
        Self::ok_xml(String::new())
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

type Handler = dyn Fn(RecordedRequest, usize) -> MockResponse + Send + Sync;

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
        // A blocking accept: `Drop` sets `shutdown` and then connects once to
        // wake it.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock server local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handler: Arc<Handler> = Arc::new(handler);

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
                    // The wake-up connection from `Drop`.
                    Ok(_) if thread_shutdown.load(Ordering::SeqCst) => break,
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
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
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
    handler: &Handler,
    concurrency: &ServedConcurrency,
) {
    // On macOS (BSD) an accepted socket can inherit O_NONBLOCK; a read that
    // raced ahead of the client's bytes then failed with WouldBlock and the
    // connection was dropped unrecorded.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    requests.lock().unwrap().push(request.clone());
    // Count a request as in flight only while its response is being
    // produced: a strictly serial client sends the next request as soon as
    // it has the previous response, possibly before that connection's
    // thread returned.
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

    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let request = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = request.lines();
    let first_line = lines.next()?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?;
    let (path, raw_query) = target.split_once('?').unwrap_or((target, ""));
    let headers: BTreeMap<String, String> = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while buf.len() < header_end + length {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf[header_end..buf.len().min(header_end + length)].to_vec();
    Some(RecordedRequest {
        method,
        path: path.to_string(),
        query: parse_query(raw_query),
        headers,
        body,
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

/// `value` as S3 encodes keys and prefixes for `encoding-type=url`: form
/// encoding (space as `+`, `+` as `%2B`), unreserved characters and `/` kept.
fn form_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// ── ListObjectsV2 response bodies ───────────────────────────

/// Attributes of one `<Contents>` entry.
struct ObjectXml<'a> {
    key: &'a str,
    size: u64,
    etag: String,
    last_modified: String,
}

impl<'a> ObjectXml<'a> {
    /// Attributes that depend on the entry's position in its page (the
    /// historical mock shape: sizes 100, 101, ...).
    fn by_index(key: &'a str, index: usize) -> Self {
        Self {
            key,
            size: 100 + index as u64,
            etag: format!("{:032x}", index + 1),
            // Valid for pages of any size.
            last_modified: format!(
                "2026-05-17T00:{:02}:{:02}.000Z",
                (index / 60) % 60,
                index % 60
            ),
        }
    }

    /// Attributes that depend only on the key, so the same object looks the
    /// same whichever segment or page lists it (diff tests need this).
    fn stable(key: &'a str) -> Self {
        let hash = key.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
        });
        Self {
            key,
            size: 10 + key.len() as u64,
            etag: format!("{:032x}", hash),
            last_modified: "2026-05-17T00:00:00.000Z".to_string(),
        }
    }
}

struct PageXml<'a> {
    prefix: &'a str,
    max_keys: i32,
    objects: Vec<ObjectXml<'a>>,
    common_prefixes: &'a [&'a str],
    truncated: bool,
    next_token: Option<&'a str>,
    key_count: bool,
    /// Answer as an endpoint honouring `encoding-type=url`.
    url_encoded: bool,
}

impl PageXml<'_> {
    fn render(&self) -> String {
        let field = |value: &str| {
            if self.url_encoded {
                form_encode(value)
            } else {
                xml_escape(value)
            }
        };
        let contents_xml: String = self
            .objects
            .iter()
            .map(|o| {
                format!(
                    "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    field(o.key),
                    o.last_modified,
                    o.etag,
                    o.size
                )
            })
            .collect();
        let common_prefixes_xml: String = self
            .common_prefixes
            .iter()
            .map(|prefix| {
                format!(
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                    field(prefix)
                )
            })
            .collect();
        let next_token_xml = self
            .next_token
            .map(|token| {
                format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    xml_escape(token)
                )
            })
            .unwrap_or_default();
        let key_count_xml = if self.key_count {
            format!(
                "<KeyCount>{}</KeyCount>",
                self.objects.len() + self.common_prefixes.len()
            )
        } else {
            String::new()
        };
        let encoding_xml = if self.url_encoded {
            "<EncodingType>url</EncodingType>"
        } else {
            ""
        };
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>mock-bucket</Name><Prefix>{}</Prefix>{}<MaxKeys>{}</MaxKeys>{}<IsTruncated>{}</IsTruncated>{}{}{}</ListBucketResult>"#,
            field(self.prefix),
            key_count_xml,
            self.max_keys,
            encoding_xml,
            if self.truncated { "true" } else { "false" },
            contents_xml,
            common_prefixes_xml,
            next_token_xml
        )
    }
}

/// A page whose object attributes depend on their position in it.
fn list_bucket_xml(
    prefix: &str,
    max_keys: i32,
    contents: &[&str],
    common_prefixes: &[&str],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    PageXml {
        prefix,
        max_keys,
        objects: contents
            .iter()
            .enumerate()
            .map(|(i, k)| ObjectXml::by_index(k, i))
            .collect(),
        common_prefixes,
        truncated,
        next_token,
        key_count: true,
        url_encoded: false,
    }
    .render()
}

/// A page whose object attributes depend only on their keys.
fn list_bucket_xml_stable(
    contents: &[&str],
    common_prefixes: &[&str],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    PageXml {
        prefix: "",
        max_keys: 1000,
        objects: contents.iter().map(|k| ObjectXml::stable(k)).collect(),
        common_prefixes,
        truncated,
        next_token,
        key_count: true,
        url_encoded: false,
    }
    .render()
}

/// A listing page over `keys` as S3 serves it: `page` keys after the
/// request's continuation token or start-after, truncated (with the last key
/// as the token) while more follow. Pages are delayed by `PACE`, probes are
/// not: startup discovery, which now runs while the first segment lists,
/// returns before the first page, so its boundaries reach a segment that is
/// still listing and the test sees the partition it is about.
const PACE: Duration = Duration::from_millis(120);

fn paced_page(keys: &[&str], request: &RecordedRequest, page: usize, stable: bool) -> String {
    thread::sleep(PACE);
    let after = query(request, "continuation-token")
        .or_else(|| query(request, "start-after"))
        .unwrap_or("");
    let rest: Vec<&str> = keys.iter().copied().filter(|k| *k > after).collect();
    let truncated = rest.len() > page;
    let contents = &rest[..rest.len().min(page)];
    let token = contents.last().copied().filter(|_| truncated);
    if stable {
        list_bucket_xml_stable(contents, &[], truncated, token)
    } else {
        list_bucket_xml("", page as i32, contents, &[], truncated, token)
    }
}

fn list_bucket_xml_without_key_count(
    prefix: &str,
    max_keys: i32,
    contents: &[&str],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    PageXml {
        prefix,
        max_keys,
        objects: contents
            .iter()
            .enumerate()
            .map(|(i, k)| ObjectXml::by_index(k, i))
            .collect(),
        common_prefixes: &[],
        truncated,
        next_token,
        key_count: false,
        url_encoded: false,
    }
    .render()
}

/// A ListObjectsV2 emulator with prefix, delimiter, start-after,
/// continuation-token and max-keys semantics: a start-after that names a
/// CommonPrefix returns that prefix again, because the keys under it sort
/// after it and roll up into it. Honours `encoding-type=url` when
/// `honour_encoding` is set (and ignores it otherwise, as an endpoint may).
fn emulated_list_with(
    keys: &[String],
    q: &BTreeMap<String, String>,
    honour_encoding: bool,
) -> String {
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
    PageXml {
        prefix: &prefix,
        max_keys: max as i32,
        objects: contents
            .iter()
            .enumerate()
            .map(|(i, k)| ObjectXml::by_index(k, i))
            .collect(),
        common_prefixes: &prefixes,
        truncated,
        next_token: token.as_deref(),
        key_count: true,
        url_encoded: honour_encoding && q.get("encoding-type").map(String::as_str) == Some("url"),
    }
    .render()
}

fn emulated_list(keys: &[String], q: &BTreeMap<String, String>) -> String {
    emulated_list_with(keys, q, false)
}

fn query<'a>(request: &'a RecordedRequest, name: &str) -> Option<&'a str> {
    request.query.get(name).map(String::as_str)
}

fn is_delimiter_probe(request: &RecordedRequest) -> bool {
    query(request, "delimiter") == Some("/")
}

fn is_single_key_probe(request: &RecordedRequest) -> bool {
    query(request, "max-keys") == Some("1")
}

// ── Running the binary ──────────────────────────────────────

/// The fail-fast request policy every run gets unless a test overrides it
/// (the shipped defaults are 10 attempts, 1 s backoff, 5 s timeouts).
const FAST_ENV: &[(&str, &str)] = &[
    ("S3TL_MAX_ATTEMPTS", "3"),
    ("S3TL_BACKOFF_SECS", "0"),
    ("S3TL_TIMEOUT_SECS", "2"),
];

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

/// `--endpoint-url <mock> <command> <rest...>`.
fn cli(server: &MockS3Server, command: &str, rest: &[&str]) -> Vec<String> {
    let mut args = strings(&["--endpoint-url", &server.endpoint(), command]);
    args.extend(strings(rest));
    args
}

/// A `Command` for the binary in `cwd` (also its `HOME`), isolated from the
/// developer's AWS profile, proxy and log settings
/// (`common::hermetic_command`), with mock credentials, `AWS_REGION`, the
/// fail-fast request policy, and then `extra_env` (which wins).
fn mock_command(cwd: &Path, extra_env: &[(&str, &str)]) -> Command {
    let mut command = common::hermetic_command(cwd);
    command
        .current_dir(cwd)
        .env("AWS_ACCESS_KEY_ID", "mock-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .envs(FAST_ENV.iter().copied())
        .envs(extra_env.iter().copied());
    command
}

fn run_cli(args: &[String], cwd: &Path) -> (i32, String, String) {
    run_cli_with_env(args, cwd, &[])
}

fn run_cli_with_env(
    args: &[String],
    cwd: &Path,
    extra_env: &[(&str, &str)],
) -> (i32, String, String) {
    let output = mock_command(cwd, extra_env)
        .args(args)
        .output()
        .expect("run s3tl");
    common::exit_and_output(output)
}

/// The result document on stdout (exactly one JSON document).
fn result_of(stdout: &str) -> Value {
    serde_json::from_str(stdout)
        .unwrap_or_else(|e| panic!("stdout is not one JSON document ({}): {}", e, stdout))
}

/// The result of a `-o -` run: the last line of stderr.
fn stderr_result(stderr: &str) -> Value {
    let line = stderr.lines().last().expect("stderr has a result line");
    serde_json::from_str(line)
        .unwrap_or_else(|e| panic!("last stderr line is not a result ({}): {}", e, stderr))
}

/// Assert a run succeeded and return its result.
fn ok_result(code: i32, stdout: &str, stderr: &str) -> Value {
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let result = result_of(stdout);
    assert_eq!(result["ok"], true, "{}", result);
    assert_eq!(result["status"], "success", "{}", result);
    result
}

/// Assert a run failed with `exit` / `error_code` and return its result.
fn failed_result(code: i32, stdout: &str, stderr: &str, exit: i32, error_code: &str) -> Value {
    assert_eq!(code, exit, "stdout: {}\nstderr: {}", stdout, stderr);
    let result = result_of(stdout);
    assert_eq!(result["ok"], false, "{}", result);
    assert_eq!(result["exit_code"], exit, "{}", result);
    assert_eq!(result["error"]["code"], error_code, "{}", result);
    result
}

fn ndjson_rows(stdout: &str) -> Vec<Value> {
    stdout
        .lines()
        .map(|line| {
            assert!(!line.is_empty(), "blank NDJSON row in {:?}", stdout);
            serde_json::from_str(line).unwrap()
        })
        .collect()
}

fn ndjson_keys(stdout: &str) -> Vec<String> {
    ndjson_rows(stdout)
        .iter()
        .map(|row| row["key"].as_str().unwrap().to_string())
        .collect()
}

// ── Reading a dataset directory ─────────────────────────────

/// Committed part files of a dataset directory, in index order.
fn part_files(dir: &Path) -> Vec<PathBuf> {
    let mut parts: Vec<(usize, PathBuf)> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().into_string().ok()?;
            let index = name
                .strip_prefix("part-")?
                .strip_suffix(".parquet")?
                .parse()
                .ok()?;
            Some((index, entry.path()))
        })
        .collect();
    parts.sort();
    parts.into_iter().map(|(_, path)| path).collect()
}

/// Every record batch of the dataset, parts in index order.
fn dataset_batches(dir: &Path) -> Vec<RecordBatch> {
    let parts = part_files(dir);
    assert!(!parts.is_empty(), "no part files in {}", dir.display());
    parts
        .iter()
        .flat_map(|part| {
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(part).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .map(|batch| batch.unwrap())
        })
        .collect()
}

fn dataset_strings(dir: &Path, column: &str) -> Vec<Option<String>> {
    dataset_batches(dir)
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("no column {}", column))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..array.len())
                .map(|i| (!array.is_null(i)).then(|| array.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn dataset_u64(dir: &Path, column: &str) -> Vec<Option<u64>> {
    dataset_batches(dir)
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("no column {}", column))
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone();
            (0..array.len())
                .map(|i| (!array.is_null(i)).then(|| array.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The `key` column of every part, in part order.
fn dataset_keys(dir: &Path) -> Vec<String> {
    dataset_strings(dir, "key")
        .into_iter()
        .map(|k| k.expect("key is never null"))
        .collect()
}

fn sorted(mut keys: Vec<String>) -> Vec<String> {
    keys.sort();
    keys
}

/// The `s3tl.schema` key-value metadata of a part file.
fn schema_tag(part: &Path) -> Option<String> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(part).unwrap()).unwrap();
    builder
        .metadata()
        .file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == "s3tl.schema")
        .and_then(|kv| kv.value.clone())
}

/// Hidden temp parts (`.part-NNNNN.parquet.tmp`) left in a directory.
fn temp_parts(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok()?.file_name().into_string().ok())
                .filter(|name| name.starts_with(".part-") && name.ends_with(".tmp"))
                .collect()
        })
        .unwrap_or_default()
}

// ── Checkpoints ─────────────────────────────────────────────

fn checkpoint_path(dir: &Path) -> PathBuf {
    dir.join("_checkpoint.json")
}

/// Write `DIR/_checkpoint.json` as an interrupted `list` run of
/// `s3://<bucket>/<prefix>` against `endpoint` would have left it.
fn write_checkpoint(
    dir: &Path,
    bucket: &str,
    prefix: &str,
    endpoint: &str,
    delimiter: &str,
    ranges: &[(&str, Option<&str>)],
) {
    std::fs::create_dir_all(dir).unwrap();
    let remaining: Vec<Value> = ranges
        .iter()
        .map(|(start_after, end)| json!({"start_after": start_after, "end": end}))
        .collect();
    let checkpoint = json!({
        "schema": "s3tl.checkpoint/1",
        "identity": {
            "bucket": bucket,
            "prefix": prefix,
            "endpoint_url": endpoint,
            "delimiter": delimiter,
            "max_keys": null,
        },
        "remaining": remaining,
        "updated_at": "2026-05-17T00:00:00Z",
    });
    std::fs::write(
        checkpoint_path(dir),
        serde_json::to_string_pretty(&checkpoint).unwrap(),
    )
    .unwrap();
}

/// The `start_after` of each range a checkpoint left to list.
fn checkpoint_remaining_starts(dir: &Path) -> Option<Vec<String>> {
    let content = std::fs::read_to_string(checkpoint_path(dir)).ok()?;
    let value: Value = serde_json::from_str(&content).ok()?;
    assert_eq!(value["schema"], "s3tl.checkpoint/1");
    value["remaining"].as_array().map(|items| {
        items
            .iter()
            .filter_map(|item| item["start_after"].as_str().map(str::to_string))
            .collect()
    })
}

// ── Pagination and protocol fields ──────────────────────────

#[test]
fn local_mock_list_paginates_and_records_protocol_fields() {
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        assert_eq!(request.path.trim_end_matches('/'), "/mock-bucket");
        assert_eq!(query(&request, "list-type"), Some("2"));
        let prefix = query(&request, "prefix").unwrap_or("");
        match query(&request, "continuation-token") {
            None => MockResponse::ok_xml(list_bucket_xml(
                prefix,
                2,
                &["logs/a.txt", "logs/b.txt"],
                &[],
                true,
                Some("token-1"),
            )),
            Some("token-1") => MockResponse::ok_xml(list_bucket_xml(
                prefix,
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
    let out = dir.path().join("out");
    let trace = dir.path().join("trace.jsonl");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/logs/",
            "--max-keys",
            "2",
            "--delimiter",
            "/",
            "--trace",
            trace.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);

    // The page's CommonPrefix is a row, merged in key order.
    assert_eq!(
        dataset_keys(&out),
        vec!["logs/a.txt", "logs/b.txt", "logs/archive/", "logs/c.txt"]
    );
    assert_eq!(result["command"], "list");
    assert_eq!(result["source"]["bucket"], "mock-bucket");
    assert_eq!(result["source"]["prefix"], "logs/");
    assert_eq!(result["source"]["url"], "s3://mock-bucket/logs/");
    assert_eq!(result["source"]["path_style"], true);
    assert_eq!(result["source"]["region"], "us-east-1");
    assert_eq!(result["stats"]["objects"], 3);
    assert_eq!(result["stats"]["folders"], 1);
    assert_eq!(result["output"]["format"], "parquet");
    assert_eq!(result["output"]["rows"], 4);

    let requests = server.requests();
    let list_requests: Vec<_> = requests.iter().filter(|r| r.method == "GET").collect();
    assert_eq!(list_requests.len(), 2, "{:#?}", list_requests);
    assert_eq!(query(list_requests[0], "prefix"), Some("logs/"));
    assert_eq!(query(list_requests[0], "delimiter"), Some("/"));
    assert_eq!(query(list_requests[0], "max-keys"), Some("2"));
    // Every listing request asks for URL-encoded keys.
    assert!(
        list_requests
            .iter()
            .all(|r| query(r, "encoding-type") == Some("url"))
    );
    assert!(!list_requests[0].query.contains_key("continuation-token"));
    assert_eq!(
        query(list_requests[1], "continuation-token"),
        Some("token-1")
    );

    let trace_events: Vec<Value> = std::fs::read_to_string(trace)
        .unwrap()
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
        if is_delimiter_probe(&request) {
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
            query(&request, "prefix").unwrap_or(""),
            1000,
            &["logs/a.txt", "logs/nested/b.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--delimiter",
            "",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);

    assert_eq!(dataset_keys(&out), vec!["logs/a.txt", "logs/nested/b.txt"]);
    // The only delimiter-bearing request is the single discovery probe;
    // listing requests omit the empty delimiter entirely.
    let requests = server.requests();
    let probes: Vec<_> = requests
        .iter()
        .filter(|r| r.query.contains_key("delimiter"))
        .collect();
    assert_eq!(probes.len(), 1, "{:#?}", probes);
    assert_eq!(query(probes[0], "delimiter"), Some("/"));
}

// ── Startup partitioning ────────────────────────────────────

#[test]
fn local_mock_list_startup_discovery_splits_segments() {
    let all_keys = ["a/1.txt", "a/x/2.txt", "b/3.txt", "c.txt"];
    let server = MockS3Server::start(move |request, _sequence| {
        assert_eq!(request.method, "GET");
        if is_delimiter_probe(&request) {
            // Startup structural discovery probe.
            let children: &[&str] = match query(&request, "prefix").unwrap_or("") {
                "" => &["a/", "b/"],
                "a/" => &["a/x/"],
                _ => &[],
            };
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], children, false, None));
        }
        // Flat listing request from one of the segments.
        MockResponse::ok_xml(paced_page(&all_keys, &request, 1, false))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);

    // Segments stream concurrently, so output order is not deterministic.
    assert_eq!(sorted(dataset_keys(&out)), all_keys);

    let requests = server.requests();
    // Startup probes carry no start-after; runtime split probes (also
    // delimiter requests) start after a segment's cursor, and may run too
    // once the cut segments are idle.
    let probes: Vec<_> = requests
        .iter()
        .filter(|r| r.query.contains_key("delimiter") && !r.query.contains_key("start-after"))
        .collect();
    let lists: Vec<_> = requests
        .iter()
        .filter(|r| !r.query.contains_key("delimiter"))
        .collect();
    // BFS probes: root, a/, b/, a/x/. Boundaries a/, a/x/, b/: the first
    // segment, listing from the start meanwhile, is cut at those still ahead
    // of it (past its first page, "a/1.txt": a/x/ and b/).
    assert_eq!(probes.len(), 4, "{:#?}", probes);
    let starts: Vec<&str> = lists
        .iter()
        .filter_map(|r| query(r, "start-after"))
        .collect();
    assert!(
        starts.contains(&"a/x/") && starts.contains(&"b/"),
        "{:#?}",
        lists
    );
    // Nothing but the dataset is written: no cache next to the run.
    let mut names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["out"]);
}

#[test]
fn local_mock_segment_boundary_key_is_not_dropped() {
    // Discovery cuts the key space at "m/", and "m/" is also a real object:
    // segment ranges are (start_after, end], so the first segment must keep
    // it and the second must start after it.
    let server = MockS3Server::start(|request, _sequence| {
        if is_delimiter_probe(&request) {
            let cps: &[&str] = if query(&request, "prefix").unwrap_or("").is_empty() {
                &["m/"]
            } else {
                &[]
            };
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], cps, false, None));
        }
        MockResponse::ok_xml(paced_page(&["a.txt", "m/", "z.txt"], &request, 1, false))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(sorted(dataset_keys(&out)), vec!["a.txt", "m/", "z.txt"]);
    assert!(
        server
            .requests()
            .iter()
            .any(|r| query(r, "start-after") == Some("m/")),
        "the run must have listed a second segment from m/"
    );
}

#[test]
fn local_mock_multi_segment_boundaries_include_boundary_keys() {
    // The listing ignores segment ends (as S3 does), so every segment sees
    // the keys of the next ones; each boundary key must be emitted once.
    let server = MockS3Server::start(|request, _sequence| {
        if is_delimiter_probe(&request) {
            let cps: &[&str] = if query(&request, "prefix").unwrap_or("").is_empty() {
                &["m/", "t/"]
            } else {
                &[]
            };
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], cps, false, None));
        }
        // Pages run past segment ends (as S3's do): every boundary key
        // must still be emitted once.
        let keys = ["a.txt", "m/", "n.txt", "t/", "u.txt", "z.txt"];
        MockResponse::ok_xml(paced_page(&keys, &request, 2, false))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(
        sorted(dataset_keys(&out)),
        vec!["a.txt", "m/", "n.txt", "t/", "u.txt", "z.txt"]
    );
    assert_eq!(result["stats"]["objects"], 6);
}

#[test]
fn local_mock_no_auto_hints_lists_a_single_chain() {
    let server = MockS3Server::start(|request, _sequence| {
        if request.query.contains_key("start-after") || request.query.contains_key("delimiter") {
            return MockResponse::error(
                500,
                "UnexpectedPartitioning",
                "--no-auto-hints should force single-segment listing",
            );
        }
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &["single-segment.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(dataset_keys(&out), vec!["single-segment.txt"]);
    assert_eq!(server.requests().len(), 1, "{:#?}", server.requests());
}

#[test]
fn local_mock_list_flat_namespace_prepartitions_at_startup() {
    // When structural discovery finds no CommonPrefixes, the run bisects the
    // flat key range up front: parallel segments from the first run, every
    // key exactly once.
    let keys: Vec<String> = (0..200).map(|i| format!("obj-{:04}", i)).collect();
    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        if is_delimiter_probe(&request) {
            // Flat namespace, no CommonPrefixes, and more pages to come — a
            // bucket worth partitioning.
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], true, Some("token")));
        }
        if is_single_key_probe(&request) {
            let first: Vec<&str> = all_keys
                .iter()
                .find(|k| k.as_str() > start_after.as_str())
                .map(|k| vec![k.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }
        let keys: Vec<&str> = all_keys.iter().map(String::as_str).collect();
        MockResponse::ok_xml(paced_page(&keys, &request, 10, false))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-c", "8", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(sorted(dataset_keys(&out)), keys);

    let requests = server.requests();
    assert!(
        requests.iter().any(is_single_key_probe),
        "expected startup bisection probes (max-keys=1)"
    );
    let segment_starts: std::collections::BTreeSet<&str> = requests
        .iter()
        .filter(|r| !r.query.contains_key("delimiter") && !is_single_key_probe(r))
        .filter_map(|r| query(r, "start-after"))
        .collect();
    assert!(
        segment_starts.len() >= 2,
        "expected multiple parallel segments from startup pre-partitioning, got starts {:?}",
        segment_starts
    );
}

// Flat keys sharing a long constant suffix after a numeric run
// (`obj-000000123.snappy.parquet`) used to defeat bisection: every candidate
// bumped a character inside the suffix, so each cut landed on the key right
// after the range start. Cuts must land near the middle of each range.
#[test]
fn local_mock_list_flat_suffix_heavy_namespace_partitions_evenly() {
    const KEYS: usize = 4000;
    const PAGE: usize = 250;
    let keys: Vec<String> = (0..KEYS)
        .map(|i| format!("obj-{:09}.snappy.parquet", i))
        .collect();

    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        if is_delimiter_probe(&request) {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], true, Some("t")));
        }
        let max_keys: usize = query(&request, "max-keys")
            .and_then(|value| value.parse().ok())
            .unwrap_or(PAGE)
            .min(PAGE);
        let start_idx = match query(&request, "continuation-token") {
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
        if max_keys > 1 {
            // Listing pages are paced so startup discovery lands first.
            thread::sleep(PACE);
        }
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
    let out = dir.path().join("out");
    let trace = dir.path().join("trace.jsonl");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "-c",
            "8",
            "-v",
            "--trace",
            trace.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(sorted(dataset_keys(&out)), keys);

    // One segment per worker (boundaries + 1 = -c): a ninth segment would
    // wait for a slot and list its whole range alone at the end...
    let count: usize = stderr
        .lines()
        .find_map(|line| {
            line.split_once("Startup discovery: ")
                .and_then(|(_, rest)| rest.split_whitespace().next()?.parse().ok())
        })
        .unwrap_or_else(|| panic!("startup boundary count logged: {}", stderr));
    assert_eq!(count, 7, "{}", stderr);
    // ...each the start of one segment cut from the first (listing from the
    // start meanwhile, it is the only segment discovery's cuts can go to;
    // its children are numbered 1..=count, last cut first), every one a real
    // key.
    let mut boundaries: Vec<(String, u64)> = std::fs::read_to_string(&trace)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|e| e["operation"] == "ListObjectsV2SegmentSummary")
        .filter_map(|e| {
            let index = e["segment_index"].as_u64()?;
            (1..=count as u64)
                .contains(&index)
                .then(|| (e["start_after"].as_str().unwrap().to_string(), index))
        })
        .collect();
    boundaries.sort();
    boundaries.dedup();
    assert_eq!(boundaries.len(), count, "{:?}", boundaries);
    let mut positions: Vec<usize> = boundaries
        .iter()
        .map(|(b, _)| keys.binary_search(b).expect("boundary is a real key"))
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
    let ideal = KEYS / (count + 1);
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
        .filter(|r| is_single_key_probe(r))
        .count();
    assert!(probes <= 120, "{} bisection probes", probes);
}

// A listing that fits in one page has nothing to partition: bisecting it
// costs several single-key probes per cut to split work one request
// already finishes.
#[test]
fn local_mock_single_page_flat_listing_skips_bisection() {
    let keys: Vec<String> = (0..200).map(|i| format!("obj-{:04}", i)).collect();
    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        let page: Vec<&str> = all_keys
            .iter()
            .filter(|k| k.as_str() > start_after.as_str())
            .map(String::as_str)
            .collect();
        // Flat and untruncated, whether probed with a delimiter or not.
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-c", "16", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(sorted(dataset_keys(&out)), keys);

    let requests = server.requests();
    assert!(
        !requests.iter().any(is_single_key_probe),
        "a single-page listing must not pay bisection probes, got {:?}",
        requests
    );
    // One structural probe plus one listing request.
    assert_eq!(requests.len(), 2, "{:?}", requests);
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
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        let max_keys: usize = query(&request, "max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        if is_delimiter_probe(&request) {
            let all: Vec<&str> = all_keys.iter().map(String::as_str).collect();
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &all, &[], false, None));
        }
        let start_idx = match query(&request, "continuation-token") {
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
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "-c",
            "8",
            "--max-keys",
            "5",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(sorted(dataset_keys(&out)), keys);
    assert!(
        server.requests().iter().any(is_single_key_probe),
        "a run paginating at --max-keys 5 must still pre-partition"
    );
}

// ── Runtime splitting ───────────────────────────────────────

#[test]
fn local_mock_list_runtime_split_covers_long_tail() {
    // The run starts as one segment holding everything. With idle
    // concurrency, the reactor must probe the long tail, split it at a real
    // CommonPrefix, and a child segment must list the right half — with
    // every key emitted exactly once.
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
        let prefix = query(&request, "prefix").unwrap_or("").to_string();
        let start_after = query(&request, "start-after").unwrap_or("").to_string();

        if is_delimiter_probe(&request) {
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
        if is_single_key_probe(&request) {
            let first: Vec<&str> = all_keys
                .iter()
                .find(|k| k.starts_with(&prefix) && k.as_str() > start_after.as_str())
                .map(|k| vec![k.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml(&prefix, 1, &first, &[], false, None));
        }

        // Flat listing with offset-encoded continuation tokens, 2 keys per
        // page, slowed down so the long-tail segment is still running when
        // the reactor's split check fires.
        thread::sleep(Duration::from_millis(80));
        let start_idx = match query(&request, "continuation-token") {
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
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "--max-keys",
            "2",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli_with_env(&args, dir.path(), &[("RUST_LOG", "s3tl=info")]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        stderr.contains("accepted runtime split"),
        "the run never split at runtime: {}",
        stderr
    );

    // Every key exactly once, regardless of which segment emitted it.
    assert_eq!(sorted(dataset_keys(&out)), keys);

    // A child segment starts listing from an accepted cut inside big/.
    let requests = server.requests();
    assert!(
        requests.iter().any(|r| {
            !r.query.contains_key("delimiter")
                && !is_single_key_probe(r)
                && query(r, "start-after").is_some_and(|sa| sa.starts_with("big/"))
        }),
        "expected a child segment starting inside big/: {:#?}",
        requests
    );
}

#[test]
fn local_mock_list_flat_namespace_runtime_split() {
    // A flat namespace started as one segment (--no-auto-hints): the
    // flat-cut fallback must derive a cut from the cursor (max_keys=1 probe
    // returning a real key) and fan out a child segment — with every key
    // emitted once. 240 keys at ~80ms/page gives the reactor a wide window.
    let keys: Vec<String> = (0..240).map(|i| format!("obj-{:04}", i)).collect();

    let all_keys = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        assert_eq!(request.method, "GET");
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        if is_delimiter_probe(&request) {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        if is_single_key_probe(&request) {
            let first: Vec<&str> = all_keys
                .iter()
                .find(|k| k.as_str() > start_after.as_str())
                .map(|k| vec![k.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }
        thread::sleep(Duration::from_millis(80));
        let start_idx = match query(&request, "continuation-token") {
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
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "--max-keys",
            "2",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(sorted(dataset_keys(&out)), keys);

    let requests = server.requests();
    assert!(
        requests.iter().any(is_single_key_probe),
        "expected at least one flat-cut probe (max-keys=1)"
    );
    assert!(
        requests.iter().any(|r| {
            !r.query.contains_key("delimiter")
                && !is_single_key_probe(r)
                && query(r, "start-after").is_some_and(|sa| sa.starts_with("obj-"))
        }),
        "expected a child segment starting at a flat cut"
    );
}

// ── Outputs: statistics only, NDJSON, Parquet dataset ───────

/// Two pages: logs/a.txt (100), logs/b.txt (101), then images/c.jpg (100).
fn two_page_server() -> MockS3Server {
    MockS3Server::start(|request, _sequence| {
        assert_eq!(request.method, "GET");
        if is_delimiter_probe(&request) {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        match query(&request, "continuation-token") {
            None => MockResponse::ok_xml(list_bucket_xml(
                "",
                2,
                &["images/c.jpg", "logs/a.txt"],
                &[],
                true,
                Some("token-1"),
            )),
            Some("token-1") => {
                MockResponse::ok_xml(list_bucket_xml("", 2, &["logs/b.txt"], &[], false, None))
            }
            Some(_) => MockResponse::error(400, "InvalidToken", "unexpected continuation token"),
        }
    })
}

#[test]
fn local_mock_without_output_reports_statistics_only() {
    let server = two_page_server();
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "--max-keys", "2", "--no-auto-hints"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    // No diagnostics on stderr for a clean run without -v.
    assert!(stderr.is_empty(), "{}", stderr);

    assert_eq!(result["stats"]["objects"], 3);
    assert_eq!(result["stats"]["folders"], 0);
    assert_eq!(result["stats"]["bytes"], 301);
    assert_eq!(result["output"]["format"], "none");
    assert_eq!(result["output"]["path"], Value::Null);
    assert_eq!(result["output"]["rows"], 0);
    assert_eq!(result["output"]["files"], json!([]));
    // A statistics-only run has nothing to resume.
    assert_eq!(result["resume"], Value::Null);
    assert_eq!(result["diff"], Value::Null);
    // Nothing is written anywhere.
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn local_mock_ndjson_streams_rows_and_puts_the_result_on_stderr() {
    let server = two_page_server();
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--max-keys",
            "2",
            "--no-auto-hints",
            "-o",
            "-",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);

    let rows = ndjson_rows(&stdout);
    assert_eq!(rows.len(), 3, "stdout should contain only NDJSON rows");
    assert_eq!(
        rows[0],
        json!({
            "key": "images/c.jpg",
            "size": 100,
            "last_modified": "2026-05-17T00:00:00.000Z",
            "etag": format!("{:032x}", 1),
            "storage_class": "STANDARD",
        })
    );
    assert_eq!(rows[1]["key"], "logs/a.txt");
    assert_eq!(rows[1]["size"], 101);
    assert_eq!(rows[1]["last_modified"], "2026-05-17T00:00:01.000Z");
    assert_eq!(rows[2]["key"], "logs/b.txt");

    // The result is the last line of stderr, compact.
    let result = stderr_result(&stderr);
    assert_eq!(stderr.lines().last().unwrap().lines().count(), 1);
    assert_eq!(result["ok"], true);
    assert_eq!(result["output"]["format"], "ndjson");
    assert_eq!(result["output"]["path"], "-");
    assert_eq!(result["output"]["rows"], 3);
    assert_eq!(result["stats"]["bytes"], 301);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn local_mock_ndjson_keeps_control_characters_in_keys() {
    // Tab, newline and backslash are legal in XML and in keys; each row must
    // still be one parseable line carrying the exact key.
    let keys = [
        "line\nkey.txt",
        "plain.txt",
        "slash\\key.txt",
        "tab\tkey.txt",
    ];
    let server = MockS3Server::start(move |request, _sequence| {
        let page: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|k| *k > query(&request, "start-after").unwrap_or(""))
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "--no-auto-hints", "-o", "-"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(stdout.lines().count(), 4, "{:?}", stdout);
    assert_eq!(ndjson_keys(&stdout), keys);
}

#[test]
fn local_mock_ndjson_emits_no_blank_rows_for_an_empty_listing() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "list", &["s3://mock-bucket", "-o", "-"]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(stdout.is_empty(), "{:?}", stdout);
    let result = stderr_result(&stderr);
    assert_eq!(result["ok"], true);
    assert_eq!(result["output"]["rows"], 0);
    assert_eq!(result["stats"]["objects"], 0);
}

#[test]
fn local_mock_empty_listing_still_commits_one_readable_part() {
    // Readers need at least one file to see the schema.
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    let parts = part_files(&out);
    assert_eq!(parts.len(), 1, "{:?}", parts);
    assert!(dataset_keys(&out).is_empty());
    assert_eq!(schema_tag(&parts[0]).as_deref(), Some("s3tl.list/1"));
    assert_eq!(result["output"]["files"][0]["rows"], 0);
}

#[test]
fn local_mock_dataset_matches_its_result_document() {
    // Relative -o, as an agent would pass it; the result on stdout, the copy
    // in the dataset and the part files must all agree.
    let keys: Vec<String> = (0..2500).map(|i| format!("logs/{:05}.txt", i)).collect();
    let served = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        MockResponse::ok_xml(emulated_list(&served, &request.query))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket/logs/", "--no-auto-hints", "-o", "out"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    let out = dir.path().join("out");

    // The same document is kept next to the data.
    let copy: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("_result.json")).unwrap()).unwrap();
    assert_eq!(copy, result);

    assert_eq!(result["output"]["format"], "parquet");
    assert_eq!(result["output"]["path"], "out");
    assert_eq!(result["output"]["rows"], 2500);
    assert_eq!(result["stats"]["objects"], 2500);
    let files = result["output"]["files"].as_array().unwrap();
    let parts = part_files(&out);
    assert_eq!(files.len(), parts.len());
    for (file, part) in files.iter().zip(&parts) {
        assert_eq!(
            Path::new(file["path"].as_str().unwrap()).file_name(),
            part.file_name()
        );
        assert_eq!(
            file["bytes"].as_u64().unwrap(),
            std::fs::metadata(part).unwrap().len()
        );
        assert_eq!(schema_tag(part).as_deref(), Some("s3tl.list/1"));
    }
    let rows: u64 = files.iter().map(|f| f["rows"].as_u64().unwrap()).sum();
    assert_eq!(rows, 2500);
    assert_eq!(dataset_keys(&out), keys);
    // Only committed parts and the result remain: no temp files, no
    // checkpoint after a successful run.
    assert!(temp_parts(&out).is_empty());
    assert!(!checkpoint_path(&out).exists());
    assert_eq!(result["resume"]["resumed"], false);
    assert_eq!(result["resume"]["checkpoint"], Value::Null);
    assert_eq!(result["resume"]["remaining_ranges"], 0);
}

#[test]
fn local_mock_delimiter_listing_of_only_folders_emits_them() {
    // A bucket whose top level holds only "folders" used to list as empty.
    let server = MockS3Server::start(|request, _sequence| {
        assert_eq!(query(&request, "delimiter"), Some("/"));
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
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "--delimiter", "/", "-o", "-"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let rows = ndjson_rows(&stdout);
    let keys: Vec<&str> = rows.iter().map(|r| r["key"].as_str().unwrap()).collect();
    assert_eq!(keys, vec!["dir0/", "dir1/", "dir2/"]);
    for row in &rows {
        assert_eq!(row["size"], 0, "{}", row);
        assert_eq!(row["last_modified"], Value::Null, "{}", row);
        assert_eq!(row["etag"], Value::Null, "{}", row);
    }
    let result = stderr_result(&stderr);
    assert_eq!(result["stats"]["folders"], 3);
    assert_eq!(result["stats"]["objects"], 0);
}

// ── encoding-type=url ───────────────────────────────────────

/// Keys a listing must carry through unchanged, in UTF-8 byte order: a
/// control character (which XML 1.0 cannot carry raw), `%`, `+`, a space
/// and a multi-byte character.
const AWKWARD_KEYS: [&str; 6] = [
    "dir one/ctl\u{1}key.txt",
    "dir one/pct%41.txt",
    "dir one/plain.txt",
    "dir one/plus+sign.txt",
    "dir one/sp ace.txt",
    "dir one/\u{4e2d}.txt",
];

#[test]
fn local_mock_url_encoded_keys_round_trip() {
    // The endpoint honours encoding-type=url: keys and prefixes come back
    // form-encoded with <EncodingType>url</EncodingType>, and must be
    // decoded to exactly what was stored, in every output.
    let keys: Vec<String> = AWKWARD_KEYS.iter().map(|k| k.to_string()).collect();
    let served = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        MockResponse::ok_xml(emulated_list_with(&served, &request.query, true))
    });
    let dir = tempfile::tempdir().unwrap();

    // Two keys per page: the continuation tokens and the resume cursor
    // carry decoded keys too.
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/dir one/",
            "--no-auto-hints",
            "--max-keys",
            "2",
            "-o",
            "-",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(ndjson_keys(&stdout), keys);
    // Every listing request asked for encoded keys, and the prefix went out
    // as stored (the mock decodes the query).
    for request in server.requests() {
        assert_eq!(query(&request, "encoding-type"), Some("url"));
        assert_eq!(query(&request, "prefix"), Some("dir one/"));
    }

    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket/dir one/", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(dataset_keys(&out), keys);

    // Folder rows are decoded too.
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "--delimiter", "/", "-o", "-"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(ndjson_keys(&stdout), vec!["dir one/"]);
}

#[test]
fn local_mock_endpoint_ignoring_encoding_type_keeps_keys_verbatim() {
    // An endpoint may ignore encoding-type (no <EncodingType> in the
    // answer): its keys are raw and must not be decoded a second time — a
    // literal '+' stays a '+', a literal "%41" stays "%41".
    let keys: Vec<String> = AWKWARD_KEYS
        .iter()
        .filter(|k| !k.contains('\u{1}'))
        .map(|k| k.to_string())
        .collect();
    let served = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        MockResponse::ok_xml(emulated_list_with(&served, &request.query, false))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/dir one/",
            "--no-auto-hints",
            "--max-keys",
            "2",
            "-o",
            "-",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(ndjson_keys(&stdout), keys);
}

// ── The listing contract guard ──────────────────────────────

#[test]
fn local_mock_out_of_order_keys_fail_as_an_inconsistent_listing() {
    for (label, page) in [
        ("reordered", vec!["b.txt", "a.txt"]),
        ("repeated", vec!["a.txt", "a.txt"]),
    ] {
        let server = MockS3Server::start(move |_request, _sequence| {
            MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
        });
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let args = cli(
            &server,
            "list",
            &[
                "s3://mock-bucket",
                "--no-auto-hints",
                "-o",
                out.to_str().unwrap(),
            ],
        );
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        let result = failed_result(code, &stdout, &stderr, 3, "endpoint.inconsistent_listing");
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .contains("out of order"),
            "{}: {}",
            label,
            result
        );
        assert_eq!(result["error"]["retryable"], false);
        // Not retried: the endpoint is broken, not busy.
        assert_eq!(server.requests().len(), 1, "{}", label);
        // A failed run leaves no dataset behind.
        assert!(!out.exists(), "{}", label);
    }
}

#[test]
fn local_mock_page_repeating_keys_before_its_start_after_fails() {
    // An endpoint that ignores start-after hands back keys the chain already
    // has: that would duplicate rows, so it fails the run instead.
    let server = MockS3Server::start(|request, _sequence| {
        let (page, more): (&[&str], bool) = match query(&request, "continuation-token") {
            None => (&["k1", "k2"], true),
            // Following the token returns the first page again.
            Some(_) => (&["k1", "k2"], false),
        };
        MockResponse::ok_xml(list_bucket_xml(
            "",
            2,
            page,
            &[],
            more,
            more.then_some("t1"),
        ))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "list", &["s3://mock-bucket", "--no-auto-hints"]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 3, "endpoint.inconsistent_listing");
}

#[test]
fn local_mock_untruncated_page_with_a_token_ends_the_listing() {
    // A token on a page marked IsTruncated=false must not be followed (an
    // endpoint that hands one out on every final page would page forever).
    let server =
        MockS3Server::start(
            |request, _sequence| match query(&request, "continuation-token") {
                None => MockResponse::ok_xml(list_bucket_xml(
                    "",
                    1000,
                    &["a.txt", "b.txt"],
                    &[],
                    false,
                    Some("bogus-token"),
                )),
                Some(_) => MockResponse::error(400, "InvalidToken", "must not be followed"),
            },
        );
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "--no-auto-hints", "-o", "-"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(ndjson_keys(&stdout), vec!["a.txt", "b.txt"]);
    assert_eq!(server.requests().len(), 1, "{:#?}", server.requests());
}

// ── Retries ─────────────────────────────────────────────────

#[test]
fn local_mock_sdk_retries_transient_list_error() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        assert_eq!(request.method, "GET");
        if handler_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return MockResponse::error(503, "SlowDown", "retry this request");
        }
        MockResponse::ok_xml(list_bucket_xml(
            "",
            1000,
            &["retry/succeeded.txt"],
            &[],
            false,
            None,
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "the initial 503 SlowDown must be retried"
    );
    assert_eq!(dataset_keys(&out), vec!["retry/succeeded.txt"]);
    assert!(result["stats"]["throttled"].as_u64().unwrap() >= 1);
}

#[test]
fn local_mock_retry_after_timeout_advances_without_key_count() {
    // The token page hangs past the request timeout (S3TL_TIMEOUT_SECS=2).
    // The retry must resume with start-after at the last key it has, even
    // though the endpoint sends no <KeyCount>.
    let token_error_count = Arc::new(AtomicUsize::new(0));
    let handler_token_error_count = token_error_count.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        if query(&request, "continuation-token") == Some("token-1") {
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
        if query(&request, "start-after") == Some("logs/b.txt") {
            return MockResponse::ok_xml(list_bucket_xml(
                "",
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
            "",
            2,
            &["logs/a.txt", "logs/b.txt"],
            true,
            Some("token-1"),
        ))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert!(
        token_error_count.load(Ordering::SeqCst) > 0,
        "mock should force a timeout on the token page"
    );
    assert_eq!(
        dataset_keys(&out),
        vec!["logs/a.txt", "logs/b.txt", "logs/c.txt"]
    );
    assert!(
        result["stats"]["timeouts"].as_u64().unwrap() >= 1,
        "{}",
        result
    );
    assert!(
        server.requests().iter().any(|request| {
            query(request, "start-after") == Some("logs/b.txt")
                && !request.query.contains_key("continuation-token")
        }),
        "{:#?}",
        server.requests()
    );
}

// Delimiter pages can contain only CommonPrefixes (no Contents). The resume
// cursor advances over the last CommonPrefix (safe: a prefix sorts before
// every key it covers, and those keys are rolled up into it), so a retry
// after such a page does not restart from scratch.
#[test]
fn local_mock_retry_resumes_after_common_prefixes_only_page() {
    let server = MockS3Server::start(|request, _sequence| {
        if query(&request, "continuation-token") == Some("t1") {
            // Persistent connection-level failure on the token chain: only
            // connection/timeout errors reach the cursor-resume retry.
            return MockResponse::drop_connection();
        }
        match query(&request, "start-after") {
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
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--delimiter",
            "/",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(dataset_keys(&out), vec!["cp1/", "cp2/", "zz.txt"]);
    assert_eq!(result["stats"]["folders"], 2);
    assert_eq!(result["stats"]["objects"], 1);
    assert!(
        server
            .requests()
            .iter()
            .any(|r| query(r, "start-after") == Some("cp2/")),
        "retry must resume after the last CommonPrefix: {:#?}",
        server.requests()
    );
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
    let args = cli(
        &server,
        "list",
        &["s3://b", "--delimiter", "/", "--max-keys", "2", "-o", "-"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert!(
        failed.load(Ordering::SeqCst),
        "the retry path was not exercised"
    );
    assert_eq!(
        ndjson_keys(&stdout),
        ["a/", "b/", "c/", "d.txt", "e.txt"],
        "{}",
        stdout
    );
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
        // A delimiter request is a split probe; an empty result keeps this a
        // single chain, so page order and the retry budget stay observable.
        if request.query.contains_key("delimiter") {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        let empty = || MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));

        // Which key this request is asking for, however it was resumed.
        let index = match (
            query(&request, "continuation-token"),
            query(&request, "start-after"),
        ) {
            (Some(token), _) => match token
                .strip_prefix("tok-")
                .and_then(|t| t.parse::<usize>().ok())
            {
                Some(i) => i + 1,
                None => return empty(),
            },
            (None, Some(after)) if !after.is_empty() => match after
                .strip_prefix("key-")
                .and_then(|k| k.parse::<usize>().ok())
            {
                Some(i) => i + 1,
                None => return empty(),
            },
            _ => 0,
        };
        if index >= total_keys {
            return empty();
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

fn paged_run_args(server: &MockS3Server, out: &Path) -> Vec<String> {
    // One segment, listed in order: the budget under test is per-segment,
    // and a fanned-out run would interleave pages from several of them.
    cli(
        server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "-c",
            "1",
            "-o",
            out.to_str().unwrap(),
        ],
    )
}

/// A segment that keeps advancing between hiccups must not run out of retries.
///
/// Regression: `max_attempts` was charged over the segment's whole lifetime and
/// never refunded, so a healthy listing died once it had accumulated that many
/// isolated transient failures — no matter how much ground it had covered.
#[test]
fn local_mock_retry_budget_is_refunded_by_progress() {
    // S3TL_MAX_ATTEMPTS=3, so four separate hiccups is more than a lifetime
    // budget could absorb but each one is survivable on its own.
    const FAILING: &[usize] = &[2, 4, 6, 8];
    let (server, attempts) = paged_mock_with_failing_pages(10, FAILING, 3);

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let (code, stdout, stderr) = run_cli(&paged_run_args(&server, &out), dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(
        dataset_keys(&out),
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
    // token and re-requests with `start-after`.
    assert!(
        server.requests().iter().any(|request| {
            query(request, "start-after") == Some("key-01")
                && !request.query.contains_key("continuation-token")
        }),
        "expected a segment-level resume after the first hiccup: {:#?}",
        server.requests()
    );
}

/// The refund must require real progress, or a stuck segment retries forever.
#[test]
fn local_mock_retry_budget_still_exhausts_without_progress() {
    // Page 2 never answers, so every retry after the first resumes from the
    // same key and buys no ground.
    const FAILING: &[usize] = &[2];
    let (server, _attempts) = paged_mock_with_failing_pages(10, FAILING, usize::MAX);

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let (code, stdout, stderr) = run_cli(&paged_run_args(&server, &out), dir.path());
    let result = failed_result(code, &stdout, &stderr, 4, "net.error");
    assert_eq!(result["status"], "failed");
    assert_eq!(result["error"]["retryable"], true);
    // The failed run leaves no partial dataset (and no directory it made).
    assert!(!out.exists());
}

// A page that says IsTruncated=true but gives no usable NextContinuationToken,
// or hands back the token it was just sent, is not the end of the listing:
// the attempt fails at the last key seen and the retry resumes with
// start-after.

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

fn guard_list_args(server: &MockS3Server, out: &Path) -> Vec<String> {
    cli(
        server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "-o",
            out.to_str().unwrap(),
        ],
    )
}

#[test]
fn local_mock_truncated_page_without_token_resumes_with_start_after() {
    let server = MockS3Server::start(|request, _sequence| {
        assert!(!request.query.contains_key("continuation-token"));
        let (page, more) = guard_page(query(&request, "start-after"));
        // IsTruncated=true while keys remain, but never a token.
        MockResponse::ok_xml(list_bucket_xml("", 2, &page, &[], more, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let (code, stdout, stderr) = run_cli(&guard_list_args(&server, &out), dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(dataset_keys(&out), GUARD_KEYS);

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
        match query(&request, "continuation-token") {
            // The first page hands out a token; following it returns the
            // next page but repeats the same token, still truncated.
            Some("t1") => {
                let (page, more) = guard_page(Some("k2"));
                MockResponse::ok_xml(list_bucket_xml("", 2, &page, &[], more, Some("t1")))
            }
            Some(other) => MockResponse::error(400, "InvalidToken", other),
            None => {
                let start_after = query(&request, "start-after");
                let (page, more) = guard_page(start_after);
                let token = (start_after.is_none() && more).then_some("t1");
                MockResponse::ok_xml(list_bucket_xml("", 2, &page, &[], more, token))
            }
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let (code, stdout, stderr) = run_cli(&guard_list_args(&server, &out), dir.path());
    ok_result(code, &stdout, &stderr);
    assert_eq!(dataset_keys(&out), GUARD_KEYS);
    assert!(
        server
            .requests()
            .iter()
            .any(|r| query(r, "start-after") == Some("k4")),
        "the retry must resume after the last key listed: {:#?}",
        server.requests()
    );
}

#[test]
fn local_mock_truncated_page_that_never_advances_fails_the_run() {
    let requests_served = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&requests_served);
    let server = MockS3Server::start(move |request, _sequence| {
        served.fetch_add(1, Ordering::SeqCst);
        // The first page has keys; every resumed page is empty, truncated
        // and without a token: the chain never gets past k2.
        let page: &[&str] = if request.query.contains_key("start-after") {
            &[]
        } else {
            &["k1", "k2"]
        };
        MockResponse::ok_xml(list_bucket_xml("", 2, page, &[], true, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let (code, stdout, stderr) = run_cli(&guard_list_args(&server, &out), dir.path());
    assert_ne!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(result_of(&stdout)["ok"], false);
    // One advancing attempt, then max_attempts (3) that make no progress.
    assert_eq!(requests_served.load(Ordering::SeqCst), 4);
    assert!(!out.exists());
}

// ── Throttling must be visible and must be backed off ──────

/// Rate-limit the first `throttle_count` listing requests, then serve
/// normally. Returns the gaps between consecutive listing requests.
fn recovering_throttle_server(throttle_count: usize) -> (MockS3Server, Arc<Mutex<Vec<Duration>>>) {
    let gaps: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(AtomicUsize::new(0));
    let last = Arc::new(Mutex::new(None::<std::time::Instant>));
    let (g, sn, lt) = (Arc::clone(&gaps), Arc::clone(&seen), Arc::clone(&last));

    let server = MockS3Server::start(move |request, _sequence| {
        if is_delimiter_probe(&request) {
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &[], false, None));
        }
        if is_single_key_probe(&request) {
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

#[test]
fn local_mock_throttling_backs_off_and_is_reported_as_throttling() {
    // The shipped request policy: the SDK's backoff sequence must fit in
    // the operation budget, so the 503s surface as throttling, not as a
    // stream timeout.
    let (server, gaps) = recovering_throttle_server(2);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[
            ("S3TL_MAX_ATTEMPTS", "10"),
            ("S3TL_BACKOFF_SECS", "1"),
            ("S3TL_TIMEOUT_SECS", "5"),
        ],
    );
    let result = ok_result(code, &stdout, &stderr);
    assert!(
        result["stats"]["throttled"].as_u64().unwrap_or(0) >= 2,
        "the endpoint sent two 503 SlowDown responses: {}",
        result["stats"]
    );
    assert_eq!(result["stats"]["timeouts"], 0, "{}", result["stats"]);
    assert_eq!(dataset_keys(&out), vec!["only-key.txt"]);

    // Backed off: re-issuing immediately is the wrong answer to `SlowDown`.
    let observed = gaps.lock().unwrap().clone();
    assert!(
        !observed.is_empty(),
        "expected retries after the throttled responses"
    );
    assert!(
        observed
            .iter()
            .all(|gap| *gap >= Duration::from_millis(500)),
        "a retry followed a SlowDown with no pause; gaps were {:?}",
        observed
    );
}

// ── Setup errors ────────────────────────────────────────────

#[test]
fn local_mock_access_denied_is_a_setup_error_that_explains_itself() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(403, "AccessDenied", "Access Denied")
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "list", &["s3://mock-bucket", "--no-auto-hints"]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    // Credentials/bucket problems are setup errors (3), not retryable network
    // failures (4), and the reason reaches the result and stderr.
    let result = failed_result(code, &stdout, &stderr, 3, "auth.access_denied");
    assert_eq!(result["error"]["retryable"], false);
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("AccessDenied"),
        "{}",
        result
    );
    assert!(result["error"]["hint"].is_string(), "{}", result);
    assert!(
        stderr.contains("auth.access_denied") && stderr.contains("AccessDenied"),
        "stderr: {}",
        stderr
    );
    // Not retried for minutes: one request per attempt at most.
    assert!(server.requests().len() <= 3, "{:#?}", server.requests());
}

#[test]
fn local_mock_fatal_segment_fails_the_run_with_its_own_error() {
    // A non-retryable failure in one segment fails the whole run fast. The
    // sibling segment must not die on "channel closed" errors that would
    // replace the real reason.
    let server = MockS3Server::start(|request, _sequence| {
        if is_delimiter_probe(&request) {
            let cps: &[&str] = if query(&request, "prefix").unwrap_or("").is_empty() {
                &["m/"]
            } else {
                &[]
            };
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], cps, false, None));
        }
        if query(&request, "start-after") == Some("m/") {
            return MockResponse::error(404, "NoSuchBucket", "injected fatal");
        }
        // Root segment: several truncated pages so it is still listing when
        // the sibling segment fails.
        thread::sleep(Duration::from_millis(50));
        match query(&request, "continuation-token") {
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
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = failed_result(code, &stdout, &stderr, 3, "config.no_such_bucket");
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("NoSuchBucket"),
        "{}",
        result
    );
    assert!(!stderr.contains("channel closed"), "{}", stderr);
    // No partial dataset: temp parts are deleted and the directory the run
    // created is removed again.
    assert!(!out.exists(), "{:?}", std::fs::read_dir(&out).ok());
}

/// Run with no credentials anywhere the SDK looks: no key variables, no
/// profile, an empty HOME (no shared config files), no instance metadata.
fn run_cli_without_credentials(args: &[String], cwd: &Path) -> (i32, String, String) {
    let mut command = mock_command(cwd, &[]);
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
    common::exit_and_output(command.args(args).output().expect("run s3tl"))
}

#[test]
fn local_mock_missing_credentials_is_a_setup_error_without_retries() {
    // The SDK reports no credentials as a dispatch failure, which used to be
    // retried as a network error for minutes and then exit 4.
    let server = MockS3Server::start(|request, _| {
        MockResponse::ok_xml(emulated_list(&["k".to_string()], &request.query))
    });
    let dir = tempfile::tempdir().unwrap();
    let started = std::time::Instant::now();
    let list = cli(&server, "list", &["s3://b"]);
    let (code, stdout, stderr) = run_cli_without_credentials(&list, dir.path());
    let result = failed_result(code, &stdout, &stderr, 3, "auth.missing_credentials");
    assert!(result["error"]["hint"].is_string());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "retried for {:?}",
        started.elapsed()
    );

    let check = cli(&server, "check", &["s3://b"]);
    let (code, stdout, stderr) = run_cli_without_credentials(&check, dir.path());
    let result = failed_result(code, &stdout, &stderr, 3, "auth.missing_credentials");
    assert_eq!(result["check"]["credentials"]["found"], false, "{}", result);
    // Nothing left the process.
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[test]
fn local_mock_custom_endpoint_signs_with_aws_region_else_us_east_1() {
    // A custom endpoint has no bucket-region lookup: the run signs with
    // --region, else AWS_REGION, else us-east-1 — it never fails for want
    // of a region, and never sends a HeadBucket to find one.
    let server = MockS3Server::start(|request, _| {
        MockResponse::ok_xml(emulated_list(&["k".to_string()], &request.query))
    });
    let dir = tempfile::tempdir().unwrap();
    let signing_region = |request: &RecordedRequest| -> String {
        let auth = request
            .headers
            .get("authorization")
            .expect("signed request");
        // Credential=<key>/<date>/<region>/s3/aws4_request
        auth.split("Credential=")
            .nth(1)
            .and_then(|c| c.split('/').nth(2))
            .unwrap()
            .to_string()
    };
    let cases: [(&str, &[&str], Option<&str>); 3] = [
        ("no region anywhere", &[], None),
        ("AWS_REGION", &[], Some("eu-west-1")),
        ("--region", &["--region", "ap-south-1"], Some("eu-west-1")),
    ];
    for (label, extra, env_region) in cases {
        let before = server.requests().len();
        let mut args = cli(&server, "list", &["s3://b", "--no-auto-hints"]);
        args.extend(strings(extra));
        let mut command = mock_command(dir.path(), &[]);
        command
            .env_remove("AWS_REGION")
            .env_remove("AWS_DEFAULT_REGION");
        if let Some(region) = env_region {
            command.env("AWS_REGION", region);
        }
        let (code, stdout, stderr) = common::exit_and_output(command.args(&args).output().unwrap());
        let result = ok_result(code, &stdout, &stderr);
        let expected = match (extra.is_empty(), env_region) {
            (false, _) => "ap-south-1",
            (true, Some(region)) => region,
            (true, None) => "us-east-1",
        };
        assert_eq!(result["source"]["region"], expected, "{}", label);
        let requests = server.requests();
        let new = &requests[before..];
        assert!(!new.is_empty(), "{}", label);
        for request in new {
            assert_eq!(request.method, "GET", "{}", label);
            assert_eq!(signing_region(request), expected, "{}", label);
        }
    }
}

// ── check: the endpoint probe ───────────────────────────────

#[test]
fn local_mock_check_probe_covers_head_list_and_pagination() {
    let server = MockS3Server::start(|request, _sequence| match request.method.as_str() {
        "HEAD" => MockResponse::empty_ok(),
        "GET" => match query(&request, "continuation-token") {
            Some("probe-page-2") => {
                MockResponse::ok_xml(list_bucket_xml("", 3, &["probe/d.txt"], &[], false, None))
            }
            _ if query(&request, "max-keys") == Some("3") => MockResponse::ok_xml(list_bucket_xml(
                "",
                3,
                &["probe/a.txt", "probe/b.txt", "probe/c.txt"],
                &[],
                true,
                Some("probe-page-2"),
            )),
            _ => MockResponse::ok_xml(list_bucket_xml("", 1, &["probe/a.txt"], &[], false, None)),
        },
        _ => MockResponse::error(405, "MethodNotAllowed", "unexpected method"),
    });

    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "check", &["s3://mock-bucket"]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["command"], "check");
    assert_eq!(result["warnings"], json!([]));
    let check = &result["check"];
    assert_eq!(check["credentials"]["found"], true);
    assert_eq!(check["region"], "us-east-1");
    assert_eq!(check["path_style"], true);
    assert_eq!(check["endpoint_url"], server.endpoint());
    assert_eq!(check["proxy"], Value::Null);
    let probe = &check["probe"];
    assert_eq!(probe["overall_status"], "compatible", "{}", probe);
    assert_eq!(probe["bucket"], "mock-bucket");
    assert_eq!(probe["addressing_style"], "path");
    assert!(probe["tests"].as_array().unwrap().iter().any(|test| {
        test["test"] == "ListObjectsV2 pagination check" && test["status"] == "ok"
    }));

    let requests = server.requests();
    assert!(requests.iter().any(|request| request.method == "HEAD"));
    // Path style: the bucket is in the request path, not the host.
    assert!(requests.iter().all(|r| r.path.starts_with("/mock-bucket")));
    assert!(
        requests
            .iter()
            .any(|request| query(request, "continuation-token") == Some("probe-page-2"))
    );
}

#[test]
fn local_mock_check_probe_reports_s3_error_metadata() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(501, "NotImplemented", "delimiter is not supported")
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "check", &["s3://mock-bucket"]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    // Every operation failed, and not on a setup error: an incompatible
    // endpoint must not exit 0.
    let result = failed_result(code, &stdout, &stderr, 4, "net.error");
    let probe = &result["check"]["probe"];
    assert_eq!(probe["overall_status"], "incompatible");
    let tests = probe["tests"].as_array().unwrap();
    assert!(tests.iter().all(|test| test["status"] == "error"));
    let service_error = tests
        .iter()
        .find(|test| test["s3_error_code"] == "NotImplemented")
        .expect("check should expose a modeled S3 service error");
    assert_eq!(service_error["http_status"], 501);
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
    assert!(service_error["error_message"].is_string());
}

#[test]
fn local_mock_check_access_denied_is_a_setup_error() {
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(403, "AccessDenied", "Access Denied")
    });
    let dir = tempfile::tempdir().unwrap();
    let trace = dir.path().join("trace.jsonl");
    let args = cli(
        &server,
        "check",
        &["s3://mock-bucket", "--trace", trace.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = failed_result(code, &stdout, &stderr, 3, "auth.access_denied");
    assert_eq!(result["check"]["probe"]["overall_status"], "incompatible");
    // --trace records every probe request.
    let requests = server.requests();
    assert!(!requests.is_empty());
    assert!(
        std::fs::read_to_string(&trace).unwrap().lines().count() >= requests.len(),
        "every probe request is traced"
    );
}

#[test]
fn local_mock_check_result_and_trace_carry_the_contract_fields() {
    // stderr carries only the one human line; the result is on stdout, and
    // probe trace events record the prefix as listing events do.
    let server = MockS3Server::start(|_request, _sequence| {
        MockResponse::error(501, "NotImplemented", "not supported")
    });
    let dir = tempfile::tempdir().unwrap();
    let trace = dir.path().join("trace.jsonl");
    let args = cli(
        &server,
        "check",
        &[
            "s3://mock-bucket/probe/",
            "--trace",
            trace.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = failed_result(code, &stdout, &stderr, 4, "net.error");
    assert_eq!(result["schema"], "s3tl.result/1");
    assert_eq!(result["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(result["status"], "failed");
    assert_eq!(result["warnings"], json!([]));
    assert_eq!(stderr.lines().count(), 1, "{}", stderr);
    assert!(
        stderr.starts_with("s3tl: failed (exit 4): net.error"),
        "{}",
        stderr
    );
    let events: Vec<Value> = std::fs::read_to_string(&trace)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(!events.is_empty());
    for event in &events {
        assert_eq!(event["prefix"], "probe/", "{}", event);
        assert_eq!(event["bucket"], "mock-bucket", "{}", event);
    }
}

#[test]
fn local_mock_check_reports_the_proxy_without_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let args = strings(&[
        "check",
        "--endpoint-url",
        "https://storage.example.test",
        "--path-style",
    ]);
    let (code, stdout, stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[("HTTPS_PROXY", "http://user:secret@proxy.example.test:3128")],
    );
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["check"]["proxy"], "http://proxy.example.test:3128");
    assert_eq!(result["check"]["probe"], Value::Null);
    assert!(!stdout.contains("secret"), "{}", stdout);
    assert!(!stderr.contains("secret"), "{}", stderr);

    let (code, stdout, stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[
            ("HTTPS_PROXY", "http://proxy.example.test:3128"),
            ("NO_PROXY", "example.test"),
        ],
    );
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["check"]["proxy"], Value::Null, "{}", result);
}

#[test]
fn local_mock_check_resolves_the_proxy_for_the_bucket_host() {
    // Without a path-style endpoint the request host is bucket-qualified
    // and regional, so a NO_PROXY rule for the regional domain decides it.
    // A dry run resolves this without sending anything.
    let dir = tempfile::tempdir().unwrap();
    let args = strings(&[
        "check",
        "s3://mock-bucket",
        "--region",
        "us-west-2",
        "--dry-run",
    ]);
    let (code, stdout, stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[("HTTPS_PROXY", "http://proxy.example.test:3128")],
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let result = result_of(&stdout);
    assert_eq!(result["status"], "planned", "{}", result);
    let check = &result["check"];
    assert_eq!(check["path_style"], false);
    assert!(
        check["request_url"]
            .as_str()
            .unwrap()
            .starts_with("https://mock-bucket.s3.us-west-2.amazonaws.com"),
        "{}",
        check
    );
    assert_eq!(check["proxy"], "http://proxy.example.test:3128");
    assert_eq!(check["probe"], Value::Null);

    let (code, stdout, stderr) = run_cli_with_env(
        &args,
        dir.path(),
        &[
            ("HTTPS_PROXY", "http://proxy.example.test:3128"),
            ("NO_PROXY", ".s3.us-west-2.amazonaws.com"),
        ],
    );
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let result = result_of(&stdout);
    assert_eq!(result["check"]["proxy"], Value::Null, "{}", result);
}

#[test]
fn local_mock_trace_check_and_results_redact_endpoint_userinfo() {
    let server = MockS3Server::start(|request, _| match request.method.as_str() {
        "HEAD" => MockResponse::empty_ok(),
        _ => MockResponse::ok_xml(emulated_list(&["k".to_string()], &request.query)),
    });
    let dir = tempfile::tempdir().unwrap();
    let endpoint = server
        .endpoint()
        .replacen("http://", "http://user:secretpw@", 1);
    let trace = dir.path().join("trace.jsonl");
    let args = strings(&[
        "--endpoint-url",
        &endpoint,
        "check",
        "s3://b",
        "--trace",
        trace.to_str().unwrap(),
    ]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert!(
        result["check"]["endpoint_url"]
            .as_str()
            .unwrap()
            .contains("<redacted>@"),
        "{}",
        result
    );
    let trace_text = std::fs::read_to_string(&trace).unwrap();
    assert!(!trace_text.is_empty());
    for text in [&stdout, &stderr, &trace_text] {
        assert!(!text.contains("secretpw"), "{}", text);
    }

    // A listing run redacts the endpoint in its result, its copy in the
    // dataset and its trace too.
    let out = dir.path().join("out");
    let list_trace = dir.path().join("list-trace.jsonl");
    let args = strings(&[
        "--endpoint-url",
        &endpoint,
        "list",
        "s3://b",
        "--trace",
        list_trace.to_str().unwrap(),
        "-o",
        out.to_str().unwrap(),
    ]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert!(
        result["source"]["endpoint_url"]
            .as_str()
            .unwrap()
            .contains("<redacted>@"),
        "{}",
        result
    );
    let saved = std::fs::read_to_string(out.join("_result.json")).unwrap();
    let list_trace_text = std::fs::read_to_string(&list_trace).unwrap();
    assert!(!list_trace_text.is_empty());
    for text in [&stdout, &stderr, &saved, &list_trace_text] {
        assert!(!text.contains("secretpw"), "{}", text);
    }
}

// ── Output failures ─────────────────────────────────────────

#[test]
fn local_mock_unwritable_trace_path_fails_before_any_request() {
    // A path under a regular file cannot be created, even as root.
    let server = MockS3Server::start(|_request, _sequence| {
        panic!("no S3 request should be issued when output setup fails")
    });
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("blocker"), b"a regular file").unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--trace",
            "blocker/trace.jsonl",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = failed_result(code, &stdout, &stderr, 5, "output.write_failed");
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("trace.jsonl"),
        "{}",
        result
    );
    assert!(!stderr.contains("panicked"), "{}", stderr);
    assert!(server.requests().is_empty());
    // The output directory the run created is removed again.
    assert!(!out.exists());
}

#[test]
fn local_mock_output_that_is_not_a_directory_fails_before_any_request() {
    let server = MockS3Server::start(|_request, _sequence| {
        panic!("no S3 request should be issued for an unusable output")
    });
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("taken");
    std::fs::write(&file, b"a regular file").unwrap();
    for output in [file.clone(), file.join("below")] {
        let args = cli(
            &server,
            "list",
            &["s3://mock-bucket", "-o", output.to_str().unwrap()],
        );
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        failed_result(code, &stdout, &stderr, 2, "usage.output_not_a_directory");
    }
    assert_eq!(std::fs::read(&file).unwrap(), b"a regular file");
    assert!(server.requests().is_empty(), "{:#?}", server.requests());
}

// ── A malformed ETag costs its object an ETag, never its segment ──

/// A ListObjectsV2 page whose single object carries a caller-supplied raw
/// ETag, as an S3-compatible endpoint might put it on the wire.
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

/// Regression: slicing a raw ETag at byte 1 panicked when that byte landed
/// inside a multi-byte character, killing the segment task — and its whole
/// key range vanished from a run that still reported success.
#[test]
fn local_mock_non_ascii_etag_keeps_every_key_and_succeeds() {
    // 34 bytes, so it takes the single-part branch, but byte 1 is mid-character.
    let bad_etag = format!("\u{65e5}{}", "a".repeat(31));
    assert_eq!(bad_etag.len(), 34);

    let bad = bad_etag.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        match query(&request, "continuation-token") {
            None => MockResponse::ok_xml(list_bucket_xml_with_raw_etag(
                "a-good.txt",
                "\"d41d8cd98f00b204e9800998ecf8427e\"",
                Some("token-1"),
            )),
            Some("token-1") => MockResponse::ok_xml(list_bucket_xml_with_raw_etag(
                "b-bad-etag.txt",
                &bad,
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
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--no-auto-hints",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    assert!(!stderr.contains("panicked"), "{}", stderr);
    assert_eq!(
        dataset_keys(&out),
        vec!["a-good.txt", "b-bad-etag.txt", "c-good.txt"],
        "the object with the malformed ETag — and the rest of its segment — must still be listed"
    );
    let etags = dataset_strings(&out, "etag");
    assert_eq!(
        etags[0].as_deref(),
        Some("d41d8cd98f00b204e9800998ecf8427e")
    );
    // Not an MD5: kept verbatim (another vendor's format), never mangled.
    assert_eq!(etags[1].as_deref(), Some(bad_etag.as_str()), "{:?}", etags);
    assert_eq!(
        etags[2].as_deref(),
        Some("d41d8cd98f00b204e9800998ecf8427e-3")
    );
    assert_eq!(dataset_u64(&out, "size"), vec![Some(10); 3]);
}

// ── Checkpoint and resume ───────────────────────────────────

#[test]
fn local_mock_resume_lists_only_the_checkpoint_ranges_into_the_same_dataset() {
    // An earlier run committed part-00000 (a.txt) and was interrupted with
    // the range after "m/" left. The resume must list exactly that range,
    // append part-00001, and leave a complete dataset and no checkpoint.
    let seed = MockS3Server::start(|_request, _| {
        MockResponse::ok_xml(list_bucket_xml("", 1000, &["a.txt"], &[], false, None))
    });
    let server = MockS3Server::start(|request, _sequence| {
        if query(&request, "start-after") != Some("m/") {
            return MockResponse::error(
                500,
                "UnexpectedSegment",
                "resume should only request the uncompleted m/ range",
            );
        }
        MockResponse::ok_xml(list_bucket_xml("", 1000, &["z-last.txt"], &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let seed_out = dir.path().join("seed");
    let args = cli(
        &seed,
        "list",
        &["s3://mock-bucket", "-o", seed_out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);

    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    std::fs::copy(
        seed_out.join("part-00000.parquet"),
        out.join("part-00000.parquet"),
    )
    .unwrap();
    // A temp part a crashed run left behind is never committed.
    std::fs::write(out.join(".part-00007.parquet.tmp"), b"garbage").unwrap();
    write_checkpoint(
        &out,
        "mock-bucket",
        "",
        &server.endpoint(),
        "",
        &[("m/", None)],
    );

    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap(), "--resume"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);

    let requests = server.requests();
    assert_eq!(requests.len(), 1, "{:#?}", requests);
    assert_eq!(query(&requests[0], "start-after"), Some("m/"));

    // The dataset holds both runs' rows, in part order.
    assert_eq!(dataset_keys(&out), vec!["a.txt", "z-last.txt"]);
    let parts: Vec<String> = part_files(&out)
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(parts, vec!["part-00000.parquet", "part-00001.parquet"]);
    assert!(temp_parts(&out).is_empty(), "{:?}", temp_parts(&out));
    assert_eq!(result["output"]["rows"], 1, "rows this run wrote");
    assert_eq!(result["output"]["files"].as_array().unwrap().len(), 2);
    assert_eq!(result["resume"]["resumed"], true);
    assert_eq!(result["resume"]["checkpoint"], Value::Null);
    assert_eq!(result["resume"]["remaining_ranges"], 0);

    // Nothing is left to resume: a completed resume must not leave a
    // checkpoint that a later --resume would trust and skip ranges by.
    assert!(!checkpoint_path(&out).exists());
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 2, "usage.no_checkpoint");
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn local_mock_failed_resume_keeps_the_checkpoint_and_earlier_parts() {
    let server = MockS3Server::start(|request, _sequence| {
        if query(&request, "start-after") == Some("m/") {
            return MockResponse::error(500, "InternalError", "range should fail");
        }
        MockResponse::error(
            500,
            "UnexpectedSegment",
            "resume should only request the uncompleted m/ range",
        )
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    write_checkpoint(
        &out,
        "mock-bucket",
        "",
        &server.endpoint(),
        "",
        &[("m/", None)],
    );
    let before = std::fs::read_to_string(checkpoint_path(&out)).unwrap();
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap(), "--resume"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = failed_result(code, &stdout, &stderr, 4, "net.unavailable");
    assert_eq!(result["error"]["retryable"], true);

    // The range is not recorded as done: the checkpoint is untouched, and
    // no part of the failed attempt was committed or left behind.
    assert_eq!(
        std::fs::read_to_string(checkpoint_path(&out)).unwrap(),
        before
    );
    assert_eq!(
        checkpoint_remaining_starts(&out),
        Some(vec!["m/".to_string()])
    );
    assert!(part_files(&out).is_empty());
    assert!(temp_parts(&out).is_empty());
    assert!(
        server
            .requests()
            .iter()
            .any(|request| query(request, "start-after") == Some("m/")),
        "{:#?}",
        server.requests()
    );
}

#[test]
fn local_mock_resume_that_cannot_write_keeps_the_checkpoint() {
    let server = MockS3Server::start(|_request, _sequence| {
        panic!("no S3 request should be issued when output setup fails")
    });
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("blocker"), b"a regular file").unwrap();
    let out = dir.path().join("out");
    write_checkpoint(
        &out,
        "mock-bucket",
        "",
        &server.endpoint(),
        "",
        &[("m/", None)],
    );
    let before = std::fs::read_to_string(checkpoint_path(&out)).unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--trace",
            "blocker/trace.jsonl",
            "-o",
            out.to_str().unwrap(),
            "--resume",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 5, "output.write_failed");
    assert_eq!(
        std::fs::read_to_string(checkpoint_path(&out)).unwrap(),
        before,
        "a failed run must neither remove nor rewrite the checkpoint"
    );
    assert!(server.requests().is_empty());
}

#[test]
fn local_mock_resumed_delimiter_listing_inside_a_folder_keeps_the_folder_row() {
    // A delimiter range resumed after a/b/c: a/b/d and a/z sort after it and
    // roll up into a/, which the endpoint returns and the run must emit.
    let cases: [(&[&str], &str, &[&str]); 2] = [
        (
            &["a/b/1", "a/b/c", "a/b/d", "a/z", "b.txt"],
            "",
            &["a/", "b.txt"],
        ),
        (&["a/b/1", "a/b/c", "a/b/d", "a/z"], "a/", &["a/b/", "a/z"]),
    ];
    for (keys, prefix, expected) in cases {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        let server = MockS3Server::start(move |request, _| {
            MockResponse::ok_xml(emulated_list(&keys, &request.query))
        });
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        write_checkpoint(
            &out,
            "b",
            prefix,
            &server.endpoint(),
            "/",
            &[("a/b/c", None)],
        );
        let url = format!("s3://b/{}", prefix);
        let args = cli(
            &server,
            "list",
            &[
                &url,
                "--delimiter",
                "/",
                "-o",
                out.to_str().unwrap(),
                "--resume",
            ],
        );
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        let result = ok_result(code, &stdout, &stderr);
        assert_eq!(dataset_keys(&out), expected, "prefix '{}'", prefix);
        assert_eq!(result["plan"], Value::Null);
    }
}

/// Keys skewed so bisection produces both tiny segments (which complete
/// without splitting) and a dominant tail of `tail` keys (which splits).
fn skewed_keys(tail: usize) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for i in 0..40 {
        keys.push(format!("a-{:04}", i));
    }
    for i in 0..40 {
        keys.push(format!("m-{:04}", i));
    }
    for i in 0..tail {
        keys.push(format!("z-{:06}", i));
    }
    keys.sort();
    keys
}

/// A flat namespace whose root page is truncated, so the run bisects into
/// several segments. Listing pages hold 4 keys and each takes
/// `page_latency`, so a long segment pages long enough for runtime
/// splitting to act on it.
fn multi_segment_flat_server(keys: Vec<String>, page_latency: Duration) -> MockS3Server {
    MockS3Server::start(move |request, _sequence| {
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        if is_delimiter_probe(&request) {
            let head: Vec<&str> = keys.iter().take(3).map(String::as_str).collect();
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &head, &[], true, Some("disc")));
        }
        if is_single_key_probe(&request) {
            let first: Vec<&str> = keys
                .iter()
                .find(|k| k.as_str() > start_after.as_str())
                .map(|k| vec![k.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml("", 1, &first, &[], false, None));
        }

        thread::sleep(page_latency);
        let start_idx = match query(&request, "continuation-token") {
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
fn local_mock_completed_run_with_runtime_splits_leaves_no_checkpoint() {
    // Runtime-split segments are tracked like any other range: a run that
    // listed the whole key space must leave nothing to resume — a stale
    // checkpoint would let a later --resume skip ranges and report success
    // over a short dataset. A tail of 960 keys at 5 ms per 4-key page lists
    // for over a second serially: several split-probe ticks in.
    let keys = skewed_keys(960);
    let server = multi_segment_flat_server(keys.clone(), Duration::from_millis(5));

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-c", "8", "-o", out.to_str().unwrap()],
    );
    // Info level names each accepted runtime split.
    let (code, stdout, stderr) = run_cli_with_env(&args, dir.path(), &[("RUST_LOG", "s3tl=info")]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    let result = result_of(&stdout);
    assert!(
        stderr.contains("accepted runtime split"),
        "the run did not split at runtime, so it no longer exercises the \
         split-then-complete path: {}",
        stderr
    );
    assert_eq!(sorted(dataset_keys(&out)), keys);
    assert_eq!(dataset_keys(&out).len(), keys.len(), "no duplicates");
    assert!(!checkpoint_path(&out).exists());
    assert_eq!(result["resume"]["checkpoint"], Value::Null);
    assert_eq!(result["resume"]["remaining_ranges"], 0);

    // An unattended job that always passes --resume is told there is
    // nothing to resume, instead of silently listing a remainder.
    let mut resume = args.clone();
    resume.push("--resume".into());
    let (code, stdout, stderr) = run_cli(&resume, dir.path());
    failed_result(code, &stdout, &stderr, 2, "usage.no_checkpoint");
    // And a fresh run refuses to mix into the finished dataset.
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 2, "usage.output_not_empty");
}

/// Send `signal` to a running child.
#[cfg(unix)]
fn kill(child: &std::process::Child, signal: &str) {
    let status = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

/// Wait up to `limit` for `child` to exit; its exit code.
#[cfg(unix)]
fn wait_with_limit(child: &mut std::process::Child, limit: Duration) -> Option<i32> {
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code();
        }
        if started.elapsed() > limit {
            let _ = child.kill();
            panic!("run did not stop within {:?} of the signal", limit);
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
#[test]
fn local_mock_interrupted_then_resumed_run_lists_every_key_exactly_once() {
    // The checkpoint records exactly the unwritten ranges (unstarted
    // segments, split children, and in-flight segments cut at their last
    // committed key), so interrupt + --resume yields every key once.
    let keys: Vec<String> = (0..6000).map(|i| format!("k{:05}", i)).collect();
    let served = keys.clone();
    let server = MockS3Server::start(move |request, _sequence| {
        thread::sleep(Duration::from_millis(40));
        let after = query(&request, "continuation-token")
            .or_else(|| query(&request, "start-after"))
            .unwrap_or("")
            .to_string();
        let max_keys: usize = query(&request, "max-keys")
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
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--max-keys",
            "50",
            "-c",
            "4",
            "-o",
            out.to_str().unwrap(),
        ],
    );

    let child = mock_command(dir.path(), &[])
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // Interrupt once the listing is well under way (not on a timer, which a
    // slow CI runner could hit before any page or a fast one after the last).
    let waited = std::time::Instant::now();
    while server.requests().len() < 40 && waited.elapsed() < Duration::from_secs(20) {
        thread::sleep(Duration::from_millis(10));
    }
    kill(&child, "-TERM");
    let first = child.wait_with_output().unwrap();
    let (code, stdout, stderr) = common::exit_and_output(first);
    let result = failed_result(code, &stdout, &stderr, 130, "interrupted");
    assert_eq!(result["status"], "interrupted");
    assert_eq!(result["error"]["retryable"], true);
    let remaining = result["resume"]["remaining_ranges"].as_u64().unwrap();
    assert!(remaining > 0, "{}", result);
    assert_eq!(
        result["resume"]["checkpoint"],
        checkpoint_path(&out).display().to_string()
    );
    assert_eq!(
        checkpoint_remaining_starts(&out).unwrap().len() as u64,
        remaining
    );
    let first_rows = result["output"]["rows"].as_u64().unwrap() as usize;
    assert!(
        first_rows < keys.len(),
        "the first run must be interrupted mid-listing"
    );
    // What it listed is committed; nothing half-written is left.
    assert!(temp_parts(&out).is_empty());
    let committed = if part_files(&out).is_empty() {
        0
    } else {
        dataset_keys(&out).len()
    };
    assert_eq!(committed, first_rows);

    let mut resume = args.clone();
    resume.push("--resume".to_string());
    let (code, stdout, stderr) = run_cli(&resume, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["resume"]["resumed"], true);
    assert_eq!(
        result["output"]["rows"].as_u64().unwrap() as usize,
        keys.len() - first_rows
    );
    assert!(!checkpoint_path(&out).exists());

    // The dataset in the directory is complete: every key exactly once.
    let mut listed = dataset_keys(&out);
    let total = listed.len();
    listed.sort_unstable();
    listed.dedup();
    assert_eq!(total, listed.len(), "resume produced duplicate rows");
    assert_eq!(listed, keys);
}

#[cfg(unix)]
#[test]
fn local_mock_interrupt_during_startup_discovery_stops_promptly() {
    // Every probe is slow and the key space looks flat and endless, so
    // startup discovery would run dozens of probe rounds; an interrupt must
    // stop it at once, without a full first fill of segment tasks.
    let server = MockS3Server::start(|_request, _sequence| {
        thread::sleep(Duration::from_millis(1500));
        MockResponse::ok_xml(list_bucket_xml("", 1, &["k0000"], &[], true, Some("t1")))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "list", &["s3://mock-bucket"]);
    let mut child = mock_command(dir.path(), &[])
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(700));
    let started = std::time::Instant::now();
    kill(&child, "-INT");
    let code = wait_with_limit(&mut child, Duration::from_secs(20));
    assert_eq!(code, Some(130));
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    let result = result_of(&stdout);
    assert_eq!(result["status"], "interrupted");
    assert_eq!(result["exit_code"], 130);
}

/// SIGTERM after the listing finished while stdout is still held: the rows
/// are all written, so the run reports success.
#[cfg(unix)]
#[test]
fn local_mock_interrupt_after_the_listing_finished_reports_success() {
    let keys: Vec<String> = (0..5000)
        .map(|i| format!("key-{:06}-padding-padding-padding", i))
        .collect();
    let served = keys.clone();
    let server = MockS3Server::start(move |request, _| {
        MockResponse::ok_xml(emulated_list(&served, &request.query))
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "list", &["s3://b", "--no-auto-hints", "-o", "-"]);
    let child = mock_command(dir.path(), &[])
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // Five 1000-key pages list everything; stdout is not read yet, so the
    // writer is still busy when the signal arrives.
    let waited = std::time::Instant::now();
    while server.requests().len() < 5 && waited.elapsed() < Duration::from_secs(20) {
        thread::sleep(Duration::from_millis(5));
    }
    thread::sleep(Duration::from_millis(500));
    kill(&child, "-TERM");
    let output = child.wait_with_output().unwrap();
    let (code, stdout, stderr) = common::exit_and_output(output);
    assert_eq!(stdout.lines().count(), keys.len(), "{}", stderr);
    assert_eq!(code, 0, "{}", stderr);
    assert_eq!(stderr_result(&stderr)["status"], "success");
}

// ── Diff ────────────────────────────────────────────────────

/// Keys of the bucket a diff request is for (by its path), or `None`.
fn diff_side<'a>(
    request: &RecordedRequest,
    left: &'a [&'a str],
    right: &'a [&'a str],
) -> Option<&'a [&'a str]> {
    if request.path.starts_with("/left") {
        Some(left)
    } else if request.path.starts_with("/right") {
        Some(right)
    } else {
        None
    }
}

#[test]
fn local_mock_diff_lists_sides_in_parallel_segments() {
    // The left side's startup discovery finds one CommonPrefix ("m/") and
    // lists two segments; the right side is a flat namespace, so the
    // flat-cut bisection (max-keys=1 probes) partitions it instead. The
    // merge must classify across both sides' segment boundaries with every
    // key exactly once, and write only the keys that differ.
    const LEFT: [&str; 3] = ["a.txt", "left-only.txt", "z-extra.txt"];
    const RIGHT: [&str; 3] = ["a.txt", "right-only.txt", "z-extra.txt"];

    let server = MockS3Server::start(move |request, _sequence| {
        let Some(bucket_keys) = diff_side(&request, &LEFT, &RIGHT) else {
            return MockResponse::error(500, "UnexpectedBucket", &request.path);
        };
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        if is_delimiter_probe(&request) {
            if request.path.starts_with("/left") {
                let prefix = query(&request, "prefix").unwrap_or("");
                let cps: &[&str] = if prefix.is_empty() { &["m/"] } else { &[] };
                return MockResponse::ok_xml(list_bucket_xml_stable(&[], cps, false, None));
            }
            // Flat, and more pages to come: a side worth partitioning.
            return MockResponse::ok_xml(list_bucket_xml_stable(&[], &[], true, Some("token")));
        }
        if is_single_key_probe(&request) {
            let first: Vec<&str> = bucket_keys
                .iter()
                .copied()
                .filter(|k| *k > start_after.as_str())
                .take(1)
                .collect();
            return MockResponse::ok_xml(list_bucket_xml_stable(&first, &[], false, None));
        }
        MockResponse::ok_xml(paced_page(bucket_keys, &request, 1, true))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "diff",
        &["s3://left", "s3://right", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["command"], "diff");
    assert_eq!(
        result["diff"],
        json!({"left_only": 1, "right_only": 1, "changed": 0, "equal": 2, "verified_equal": 0, "unverified": 0})
    );
    assert_eq!(result["target"]["bucket"], "right");
    assert_eq!(result["output"]["rows"], 2);
    assert_eq!(result["resume"], Value::Null, "diff is not resumable");
    assert_eq!(dataset_keys(&out), vec!["left-only.txt", "right-only.txt"]);
    assert_eq!(
        dataset_strings(&out, "status"),
        vec![
            Some("left_only".to_string()),
            Some("right_only".to_string())
        ]
    );
    assert_eq!(
        schema_tag(&part_files(&out)[0]).as_deref(),
        Some("s3tl.diff/1")
    );

    let requests = server.requests();
    // Left side lists two segments: one chain from the start (its later
    // pages follow the continuation token), one cut off at m/.
    let left_lists: Vec<_> = requests
        .iter()
        .filter(|r| {
            r.path.starts_with("/left")
                && !r.query.contains_key("delimiter")
                && !is_single_key_probe(r)
        })
        .collect();
    let starts: Vec<Option<&str>> = left_lists
        .iter()
        .filter(|r| !r.query.contains_key("continuation-token"))
        .map(|r| query(r, "start-after"))
        .collect();
    // (Runtime splits may add more once the cut segments are idle.)
    assert!(starts.len() >= 2, "{:#?}", left_lists);
    assert!(left_lists.iter().any(|r| query(r, "start-after").is_none()));
    assert!(
        left_lists
            .iter()
            .any(|r| query(r, "start-after") == Some("m/"))
    );
    // Right side: flat-cut bisection probes partition the flat namespace.
    assert!(
        requests
            .iter()
            .any(|r| r.path.starts_with("/right") && is_single_key_probe(r)),
        "expected right-side flat-cut probes to partition the flat namespace",
    );
}

#[test]
fn local_mock_diff_identical_sides_write_no_rows() {
    // Identical contents: the ordered merge pairs every key with its twin,
    // counts it equal, and writes nothing — but still one readable part.
    const KEYS: [&str; 3] = ["a.txt", "m/b.txt", "z.txt"];
    let server = MockS3Server::start(move |request, _sequence| {
        if diff_side(&request, &KEYS, &KEYS).is_none() {
            return MockResponse::error(500, "UnexpectedBucket", &request.path);
        }
        if is_delimiter_probe(&request) {
            return MockResponse::ok_xml(list_bucket_xml_stable(&[], &[], false, None));
        }
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        let contents: Vec<&str> = KEYS
            .iter()
            .copied()
            .filter(|k| *k > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml_stable(&contents, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "diff",
        &["s3://left", "s3://right", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(
        result["diff"],
        json!({"left_only": 0, "right_only": 0, "changed": 0, "equal": 3, "verified_equal": 0, "unverified": 0})
    );
    assert_eq!(result["stats"]["objects"], 3, "distinct keys compared");
    assert_eq!(result["output"]["rows"], 0);
    assert!(dataset_keys(&out).is_empty());
}

#[test]
fn local_mock_diff_changed_rows_carry_both_sides_across_endpoints() {
    // The right side lives on another endpoint (--target-endpoint-url).
    // "same" is identical, "size" and "etag" differ, so both are changed
    // rows with each side's columns filled. The sides use different
    // prefixes: keys are compared, and written, relative to each side's.
    let left = MockS3Server::start(|request, _| {
        assert!(request.path.starts_with("/src"), "{}", request.path);
        let body = r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>src</Name><Prefix>p/</Prefix><KeyCount>3</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>p/etag</Key><LastModified>2026-05-17T00:00:00.123Z</LastModified><ETag>&quot;aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&quot;</ETag><Size>5</Size></Contents><Contents><Key>p/same</Key><LastModified>2026-05-17T00:00:00.000Z</LastModified><ETag>&quot;cccccccccccccccccccccccccccccccc&quot;</ETag><Size>7</Size></Contents><Contents><Key>p/size</Key><LastModified>2026-05-17T00:00:00.000Z</LastModified><ETag>&quot;dddddddddddddddddddddddddddddddd&quot;</ETag><Size>1</Size></Contents></ListBucketResult>"#;
        MockResponse::ok_xml(body.to_string())
    });
    let right = MockS3Server::start(|request, _| {
        assert!(request.path.starts_with("/dst"), "{}", request.path);
        let body = r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>dst</Name><Prefix>q/</Prefix><KeyCount>3</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>q/etag</Key><LastModified>2026-05-17T00:00:00.123Z</LastModified><ETag>&quot;bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb&quot;</ETag><Size>5</Size></Contents><Contents><Key>q/same</Key><LastModified>2026-05-17T00:00:00.000Z</LastModified><ETag>&quot;cccccccccccccccccccccccccccccccc&quot;</ETag><Size>7</Size></Contents><Contents><Key>q/size</Key><LastModified>2026-05-17T00:00:00.000Z</LastModified><ETag>&quot;dddddddddddddddddddddddddddddddd&quot;</ETag><Size>2</Size></Contents></ListBucketResult>"#;
        MockResponse::ok_xml(body.to_string())
    });
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &left,
        "diff",
        &[
            "s3://src/p/",
            "s3://dst/q/",
            "--target-endpoint-url",
            &right.endpoint(),
            "--no-auto-hints",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(
        result["diff"],
        json!({"left_only": 0, "right_only": 0, "changed": 2, "equal": 1, "verified_equal": 0, "unverified": 0})
    );
    assert_eq!(result["source"]["endpoint_url"], left.endpoint());
    assert_eq!(result["target"]["endpoint_url"], right.endpoint());
    assert_eq!(result["target"]["bucket"], "dst");
    assert_eq!(dataset_keys(&out), vec!["etag", "size"]);
    assert_eq!(
        dataset_strings(&out, "status"),
        vec![Some("changed".to_string()), Some("changed".to_string())]
    );
    assert_eq!(dataset_u64(&out, "left_size"), vec![Some(5), Some(1)]);
    assert_eq!(dataset_u64(&out, "right_size"), vec![Some(5), Some(2)]);
    assert_eq!(
        dataset_strings(&out, "left_etag")[0].as_deref(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    assert_eq!(
        dataset_strings(&out, "right_etag")[0].as_deref(),
        Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    );
}

#[test]
fn local_mock_diff_with_failing_side_writes_no_output() {
    // A side that fails closes its channels mid-listing, which the merge
    // could mistake for that side having ended — every remaining key of
    // the other side would be classified one-sided. No dataset may be left.
    let server = MockS3Server::start(move |request, _sequence| {
        if is_delimiter_probe(&request) {
            return MockResponse::ok_xml(list_bucket_xml_stable(&[], &[], false, None));
        }
        if !request.path.starts_with("/left") {
            return MockResponse::error(403, "AccessDenied", "injected fatal on the right side");
        }
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        let page: Vec<&str> = ["a.txt", "b.txt", "c.txt"]
            .into_iter()
            .filter(|k| *k > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml_stable(&page, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "diff",
        &["s3://left", "s3://right", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    // A side's AccessDenied is the run's error (it used to be reported as an
    // output failure, exit 5).
    let result = failed_result(code, &stdout, &stderr, 3, "auth.access_denied");
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("AccessDenied"),
        "{}",
        result
    );
    assert!(
        !out.exists(),
        "a partial diff must not be left behind: {:?}",
        std::fs::read_dir(&out).ok().map(|d| d.count())
    );
}

#[test]
fn local_mock_diff_side_out_of_order_fails_as_an_inconsistent_listing() {
    let server = MockS3Server::start(move |request, _sequence| {
        let page: &[&str] = if request.path.starts_with("/left") {
            &["a.txt", "b.txt"]
        } else {
            &["b.txt", "a.txt"]
        };
        MockResponse::ok_xml(list_bucket_xml_stable(page, &[], false, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "diff",
        &[
            "s3://left",
            "s3://right",
            "--no-auto-hints",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 3, "endpoint.inconsistent_listing");
    assert!(!out.exists());
}

/// Ctrl-C while diff segments sit in a retry backoff: the exit must not
/// wait out the backoff, and a diff leaves no partial dataset.
#[cfg(unix)]
#[test]
fn local_mock_diff_ctrl_c_during_retry_backoff_exits_promptly() {
    let keys: Vec<String> = (0..50).map(|i| format!("k{:03}", i)).collect();
    let server = MockS3Server::start(move |request, _| {
        if request.query.contains_key("delimiter") || is_single_key_probe(&request) {
            return MockResponse::ok_xml(emulated_list(&keys, &request.query));
        }
        MockResponse::error(500, "InternalError", "flaky")
    });
    let dir = tempfile::tempdir().unwrap();
    let args = cli(&server, "diff", &["s3://left", "s3://right", "-o", "out"]);
    let mut child = mock_command(
        dir.path(),
        &[
            ("S3TL_MAX_ATTEMPTS", "10"),
            ("S3TL_BACKOFF_SECS", "1"),
            ("S3TL_TIMEOUT_SECS", "2"),
        ],
    )
    .args(&args)
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::null())
    .spawn()
    .unwrap();
    // A few failed attempts in: the backoff has grown past several seconds.
    thread::sleep(Duration::from_secs(8));
    let signalled = std::time::Instant::now();
    kill(&child, "-INT");
    let code = wait_with_limit(&mut child, Duration::from_secs(60));
    assert_eq!(code, Some(130));
    assert!(
        signalled.elapsed() < Duration::from_secs(2),
        "exit took {:?} after Ctrl-C",
        signalled.elapsed()
    );
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    let result = result_of(&stdout);
    assert_eq!(result["status"], "interrupted");
    assert_eq!(result["resume"], Value::Null);
    assert!(!dir.path().join("out").exists());
}

// ── Observable parallelism ──────────────────────────────────
//
// With per-request latency and a mock that serves connections concurrently,
// a run that fans out is distinguishable from one that does not — by the
// concurrency the mock actually served.

/// Mock for a flat namespace of `count` keys where every listing request
/// costs `latency`. Bisection probes stay fast so startup is not the thing
/// under measurement.
fn slow_flat_namespace_server(count: usize, page: usize, latency: Duration) -> MockS3Server {
    let keys: Vec<String> = (0..count).map(|i| format!("obj-{:06}", i)).collect();
    MockS3Server::start(move |request, _sequence| {
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        let max_keys: usize = query(&request, "max-keys")
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000);
        if is_delimiter_probe(&request) {
            // Flat, and more pages to come, so the run pre-partitions.
            return MockResponse::ok_xml(list_bucket_xml_stable(&[], &[], true, Some("t")));
        }
        if max_keys == 1 {
            let first: Vec<&str> = keys
                .iter()
                .find(|key| key.as_str() > start_after.as_str())
                .map(|key| vec![key.as_str()])
                .unwrap_or_default();
            return MockResponse::ok_xml(list_bucket_xml_stable(&first, &[], false, None));
        }
        thread::sleep(latency);
        let start_idx = match query(&request, "continuation-token") {
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
        MockResponse::ok_xml(list_bucket_xml_stable(
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

    // Same bucket and latency twice: once partitioned with 8 workers, once
    // with one worker as a single chain.
    fn run(extra: &[&str]) -> (usize, usize) {
        let server = slow_flat_namespace_server(KEYS, PAGE, Duration::from_millis(20));
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let mut args = cli(
            &server,
            "list",
            &[
                "s3://mock-bucket",
                "--max-keys",
                &PAGE.to_string(),
                "-o",
                out.to_str().unwrap(),
            ],
        );
        args.extend(strings(extra));
        let (code, stdout, stderr) = run_cli(&args, dir.path());
        ok_result(code, &stdout, &stderr);
        assert_eq!(dataset_keys(&out).len(), KEYS);
        (server.max_in_flight(), server.requests().len())
    }

    let (serial_peak, serial_requests) = run(&["-c", "1", "--no-auto-hints"]);
    let (parallel_peak, parallel_requests) = run(&["-c", "8"]);
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
}

#[test]
fn local_mock_diff_serves_both_sides_concurrently() {
    let server = slow_flat_namespace_server(600, 50, Duration::from_millis(20));
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "diff",
        &["s3://left", "s3://right", "-c", "8", "--max-keys", "50"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["diff"]["equal"], 600, "{}", result);
    assert_eq!(result["output"]["format"], "none");
    assert!(
        server.max_in_flight() >= 2,
        "diff sides and their segments must overlap, peak in-flight was {}",
        server.max_in_flight()
    );
}

// The monitor used to sleep a whole heartbeat between checks, so a run held
// the process open for up to that long after the listing had finished.
#[test]
fn local_mock_small_run_exits_without_waiting_for_a_heartbeat() {
    let keys = ["a.txt", "b.txt", "c.txt"];
    let server = MockS3Server::start(move |request, _sequence| {
        let start_after = query(&request, "start-after").unwrap_or("").to_string();
        let page: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|key| *key > start_after.as_str())
            .collect();
        MockResponse::ok_xml(list_bucket_xml("", 1000, &page, &[], false, None))
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-o", out.to_str().unwrap()],
    );
    let started = std::time::Instant::now();
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let elapsed = started.elapsed();
    ok_result(code, &stdout, &stderr);
    assert_eq!(dataset_keys(&out).len(), keys.len());
    // Generous: the run itself is milliseconds, and the heartbeat interval it
    // must not wait for is 5s.
    assert!(
        elapsed < Duration::from_secs(4),
        "a three-key listing took {:?} — the process is waiting on a timer, not on work",
        elapsed
    );
}

// ── Proxy routing ───────────────────────────────────────────

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
    let endpoint = "http://s3-proxy-only.invalid:9000";
    let args = strings(&[
        "--endpoint-url",
        endpoint,
        "--path-style",
        "list",
        "s3://mock-bucket",
        "--no-auto-hints",
        "-o",
        "-",
    ]);

    let proxy = server.endpoint();
    let (code, stdout, stderr) =
        run_cli_with_env(&args, dir.path(), &[("HTTP_PROXY", proxy.as_str())]);
    assert_eq!(code, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    assert_eq!(ndjson_keys(&stdout), vec!["a/1.txt", "b/2.txt"]);
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
    assert_eq!(code, 4, "stderr: {}", stderr);
    assert_eq!(stderr_result(&stderr)["error"]["retryable"], true);
    assert_eq!(server.requests().len(), before);
}

// ── list --versions (ListObjectVersions) ────────────────────

/// One version or delete marker: `(key, version_id, epoch_secs, latest,
/// delete_marker)`.
type MockVersion = (&'static str, &'static str, u64, bool, bool);

/// Versions of a small bucket in ListObjectVersions order (key ascending,
/// newest first). `b` is deleted (its latest version is a delete marker);
/// `c d` needs URL encoding.
const MOCK_VERSIONS: &[MockVersion] = &[
    ("a", "a-3", 3_000, true, false),
    ("a", "a-2", 2_000, false, false),
    ("a", "a-1", 1_000, false, false),
    ("b", "b-dm", 5_000, true, true),
    ("b", "b-1", 4_000, false, false),
    ("c d", "cd-1", 6_000, true, false),
    ("e", "null", 7_000, true, false),
];

/// A ListObjectVersions response over `MOCK_VERSIONS` honouring
/// `prefix`, `key-marker`, `version-id-marker` and `max-keys`; any other
/// GET is answered as ListObjectsV2 over the current (non-deleted) keys, so
/// startup discovery probes work.
fn emulated_versions(q: &BTreeMap<String, String>) -> String {
    let prefix = q.get("prefix").map(String::as_str).unwrap_or("");
    let max_keys: usize = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let key_marker = q.get("key-marker").map(String::as_str).unwrap_or("");
    let version_marker = q.get("version-id-marker").map(String::as_str);
    let mut entries: Vec<&MockVersion> = MOCK_VERSIONS
        .iter()
        .filter(|v| v.0.starts_with(prefix))
        .collect();
    let start = match version_marker {
        Some(marker) => entries
            .iter()
            .position(|v| v.0 == key_marker && v.1 == marker)
            .map_or(entries.len(), |i| i + 1),
        None => entries
            .iter()
            .position(|v| v.0 > key_marker)
            .unwrap_or(entries.len()),
    };
    entries.drain(..start);
    let truncated = entries.len() > max_keys;
    entries.truncate(max_keys);
    let mut body = String::new();
    for (key, id, secs, latest, dm) in &entries {
        let modified = format!(
            "2026-01-01T{:02}:{:02}:00.000Z",
            secs / 3600,
            secs / 60 % 60
        );
        if *dm {
            body.push_str(&format!(
                "<DeleteMarker><Key>{}</Key><VersionId>{}</VersionId><IsLatest>{}</IsLatest>\
                 <LastModified>{}</LastModified></DeleteMarker>",
                form_encode(key),
                id,
                latest,
                modified
            ));
        } else {
            body.push_str(&format!(
                "<Version><Key>{}</Key><VersionId>{}</VersionId><IsLatest>{}</IsLatest>\
                 <LastModified>{}</LastModified><ETag>\"{:032x}\"</ETag><Size>{}</Size>\
                 <StorageClass>STANDARD</StorageClass></Version>",
                form_encode(key),
                id,
                latest,
                modified,
                secs,
                secs / 1000
            ));
        }
    }
    let next = match (truncated, entries.last()) {
        (true, Some(last)) => format!(
            "<NextKeyMarker>{}</NextKeyMarker><NextVersionIdMarker>{}</NextVersionIdMarker>",
            form_encode(last.0),
            last.1
        ),
        _ => String::new(),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult>\
         <Name>mock-bucket</Name><Prefix>{}</Prefix><MaxKeys>{}</MaxKeys>\
         <EncodingType>url</EncodingType><IsTruncated>{}</IsTruncated>{}{}</ListVersionsResult>",
        form_encode(prefix),
        max_keys,
        truncated,
        next,
        body
    )
}

fn versions_server() -> MockS3Server {
    MockS3Server::start(|request, _sequence| {
        if request.query.contains_key("versions") {
            assert_eq!(query(&request, "encoding-type"), Some("url"));
            return MockResponse::ok_xml(emulated_versions(&request.query));
        }
        let mut current: Vec<String> = Vec::new();
        for (key, _, _, latest, dm) in MOCK_VERSIONS {
            if *latest && !*dm {
                current.push(key.to_string());
            }
        }
        MockResponse::ok_xml(emulated_list(&current, &request.query))
    })
}

/// `(key, version_id, is_latest, is_delete_marker)` of every expected row.
fn expected_versions() -> Vec<(String, String, bool, bool)> {
    MOCK_VERSIONS
        .iter()
        .map(|(k, id, _, latest, dm)| (k.to_string(), id.to_string(), *latest, *dm))
        .collect()
}

#[test]
fn local_mock_versions_lists_every_version_across_pages() {
    let server = versions_server();
    let dir = tempfile::tempdir().unwrap();
    // Two entries a page: `a`'s three versions span pages, and the delete
    // marker of `b` shares a page with a version of `a`.
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--versions",
            "--no-auto-hints",
            "--max-keys",
            "2",
            "-o",
            "-",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    assert_eq!(code, 0, "{}", stderr);
    let rows = ndjson_rows(&stdout);
    let got: Vec<_> = rows
        .iter()
        .map(|r| {
            (
                r["key"].as_str().unwrap().to_string(),
                r["version_id"].as_str().unwrap().to_string(),
                r["is_latest"].as_bool().unwrap(),
                r["is_delete_marker"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(got, expected_versions());
    // A delete marker has no size, ETag or storage class.
    assert_eq!(rows[3]["size"], 0);
    assert_eq!(rows[3]["etag"], Value::Null);
    assert_eq!(rows[4]["etag"], format!("{:032x}", 4_000));
    let result = stderr_result(&stderr);
    assert_eq!(result["stats"]["objects"], 7);

    let requests = server.requests();
    assert!(requests.iter().all(|r| r.query.contains_key("versions")));
    assert!(requests.iter().all(|r| !r.query.contains_key("list-type")));
    // Each page continues from the markers of the one before.
    assert_eq!(query(&requests[1], "key-marker"), Some("a"));
    assert_eq!(query(&requests[1], "version-id-marker"), Some("a-2"));
}

#[test]
fn local_mock_versions_partitioned_dataset_has_the_versions_schema() {
    let server = versions_server();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket",
            "--versions",
            "--max-keys",
            "1",
            "-c",
            "4",
            "-o",
            out.to_str().unwrap(),
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["output"]["rows"], 7);
    assert_eq!(
        schema_tag(&part_files(&out)[0]).as_deref(),
        Some("s3tl.list-versions/1")
    );
    let keys = dataset_keys(&out);
    let ids: Vec<String> = dataset_strings(&out, "version_id")
        .into_iter()
        .map(Option::unwrap)
        .collect();
    let mut got: Vec<(String, String)> = keys.into_iter().zip(ids).collect();
    got.sort();
    let mut want: Vec<(String, String)> = expected_versions()
        .into_iter()
        .map(|(k, id, _, _)| (k, id))
        .collect();
    want.sort();
    assert_eq!(got, want);
    let batches = dataset_batches(&out);
    let deleted: usize = batches
        .iter()
        .map(|b| {
            let col = b
                .column_by_name("is_delete_marker")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap()
                .clone();
            (0..col.len()).filter(|i| col.value(*i)).count()
        })
        .sum();
    assert_eq!(deleted, 1);
}

#[test]
fn versions_and_delimiter_are_exclusive() {
    let server = versions_server();
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "--versions", "--delimiter", "/"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 2, "usage.invalid_argument");
    assert!(server.requests().is_empty());
}

// ── -o s3:// (multipart uploads) ────────────────────────────

/// An in-memory bucket for S3 output: objects, open multipart uploads, and
/// the source listing (`src/` keys) served from `keys`.
#[derive(Default)]
struct MockStore {
    objects: BTreeMap<String, Vec<u8>>,
    uploads: BTreeMap<String, (String, BTreeMap<u32, Vec<u8>>)>,
    next_upload: usize,
    aborted: usize,
    /// How `PUT _result.json` behaves: 0 stores it, 1 fails without
    /// storing it, 2 stores it and still fails (a lost response).
    result_put: u8,
}

fn store_server(
    store: Arc<Mutex<MockStore>>,
    keys: Vec<String>,
    fail_complete: bool,
) -> MockS3Server {
    MockS3Server::start(move |request, _sequence| {
        let key = request
            .path
            .trim_start_matches('/')
            .split_once('/')
            .map(|(_, k)| percent_decode(k))
            .unwrap_or_default();
        let mut store = store.lock().unwrap();
        match request.method.as_str() {
            "GET" => {
                let prefix = query(&request, "prefix").unwrap_or("");
                if prefix.starts_with("src/") {
                    return MockResponse::ok_xml(emulated_list(&keys, &request.query));
                }
                let existing: Vec<String> = store
                    .objects
                    .keys()
                    .filter(|k| k.starts_with(prefix))
                    .cloned()
                    .collect();
                MockResponse::ok_xml(emulated_list(&existing, &request.query))
            }
            "POST" if request.query.contains_key("uploads") => {
                store.next_upload += 1;
                let id = format!("upload-{}", store.next_upload);
                store
                    .uploads
                    .insert(id.clone(), (key.clone(), BTreeMap::new()));
                MockResponse::ok_xml(format!(
                    "<InitiateMultipartUploadResult><Bucket>out-bucket</Bucket><Key>{}</Key>\
                     <UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                    xml_escape(&key),
                    id
                ))
            }
            "PUT" if request.query.contains_key("partNumber") => {
                let id = query(&request, "uploadId").unwrap().to_string();
                let number: u32 = query(&request, "partNumber").unwrap().parse().unwrap();
                let upload = store.uploads.get_mut(&id).expect("known upload");
                upload.1.insert(number, request.body.clone());
                MockResponse::empty_ok()
            }
            "POST" if request.query.contains_key("uploadId") => {
                if fail_complete {
                    return MockResponse::error(500, "InternalError", "complete failed");
                }
                let id = query(&request, "uploadId").unwrap().to_string();
                let (key, parts) = store.uploads.remove(&id).expect("known upload");
                let body: Vec<u8> = parts.into_values().flatten().collect();
                store.objects.insert(key.clone(), body);
                MockResponse::ok_xml(format!(
                    "<CompleteMultipartUploadResult><Key>{}</Key><ETag>\"x\"</ETag>\
                     </CompleteMultipartUploadResult>",
                    xml_escape(&key)
                ))
            }
            "DELETE" if request.query.contains_key("uploadId") => {
                let id = query(&request, "uploadId").unwrap().to_string();
                store.uploads.remove(&id);
                store.aborted += 1;
                MockResponse::empty_ok()
            }
            "DELETE" => {
                store.objects.remove(&key);
                MockResponse::empty_ok()
            }
            "HEAD" => match store.objects.contains_key(&key) {
                true => MockResponse::empty_ok(),
                false => MockResponse::error(404, "NotFound", "no such key"),
            },
            "PUT" => {
                let marker = key.ends_with("_result.json");
                if marker && store.result_put == 1 {
                    return MockResponse::error(500, "InternalError", "put failed");
                }
                store.objects.insert(key, request.body.clone());
                if marker && store.result_put == 2 {
                    return MockResponse::error(500, "InternalError", "response lost");
                }
                MockResponse::empty_ok()
            }
            other => panic!("unexpected {} {}", other, request.path),
        }
    })
}

fn parquet_keys(bytes: &[u8]) -> Vec<String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        out.extend((0..col.len()).map(|i| col.value(i).to_string()));
    }
    out
}

fn source_keys() -> Vec<String> {
    (0..50).map(|i| format!("src/k{:03}", i)).collect()
}

#[test]
fn local_mock_s3_output_commits_parts_and_result_last() {
    let store = Arc::new(Mutex::new(MockStore::default()));
    let server = store_server(Arc::clone(&store), source_keys(), false);
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/src/",
            "-o",
            "s3://mock-bucket/out/run1",
            "--no-auto-hints",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["output"]["format"], "parquet");
    assert_eq!(result["output"]["path"], "s3://mock-bucket/out/run1/");
    assert_eq!(result["output"]["rows"], 50);
    let files = result["output"]["files"].as_array().unwrap();
    assert!(!files.is_empty());
    assert!(files.iter().all(|f| {
        f["path"]
            .as_str()
            .unwrap()
            .starts_with("s3://mock-bucket/out/run1/part-")
    }));

    let store = store.lock().unwrap();
    assert!(
        store.uploads.is_empty(),
        "every upload completed or aborted"
    );
    let mut keys = Vec::new();
    for (name, body) in &store.objects {
        if name.ends_with(".parquet") {
            assert!(name.starts_with("out/run1/part-"), "{}", name);
            keys.extend(parquet_keys(body));
        }
    }
    keys.sort();
    assert_eq!(keys, source_keys());
    let stored: Value = serde_json::from_slice(&store.objects["out/run1/_result.json"]).unwrap();
    assert_eq!(stored["ok"], true);
    assert_eq!(stored["output"]["rows"], 50);
    // `_result.json` is written after the last part completed.
    let requests = server.requests();
    let last_put = requests
        .iter()
        .rposition(|r| r.method == "PUT" && r.path.ends_with("_result.json"))
        .unwrap();
    let last_complete = requests
        .iter()
        .rposition(|r| r.method == "POST" && r.query.contains_key("uploadId"))
        .unwrap();
    assert!(last_complete < last_put);
}

#[test]
fn local_mock_s3_output_refuses_a_non_empty_prefix() {
    let store = Arc::new(Mutex::new(MockStore::default()));
    store
        .lock()
        .unwrap()
        .objects
        .insert("out/old.txt".into(), b"x".to_vec());
    let server = store_server(Arc::clone(&store), source_keys(), false);
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket/src/", "-o", "s3://mock-bucket/out/"],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 2, "usage.output_not_empty");
    assert!(server.requests().iter().all(|r| r.method == "GET"));
}

#[test]
fn local_mock_s3_output_failed_commit_leaves_nothing() {
    let store = Arc::new(Mutex::new(MockStore::default()));
    let server = store_server(Arc::clone(&store), source_keys(), true);
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/src/",
            "-o",
            "s3://mock-bucket/out/",
            "--no-auto-hints",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 5, "output.write_failed");
    let store = store.lock().unwrap();
    assert!(store.objects.is_empty(), "{:?}", store.objects.keys());
    assert!(store.uploads.is_empty());
    assert!(store.aborted >= 1);
}

#[test]
fn s3_output_is_not_resumable() {
    let dir = tempfile::tempdir().unwrap();
    let args = strings(&["list", "s3://b/p", "-o", "s3://b/out/", "--resume"]);
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    failed_result(code, &stdout, &stderr, 2, "usage.resume_needs_output");
}

#[test]
fn local_mock_s3_output_assembles_multi_part_uploads_in_order() {
    let store = Arc::new(Mutex::new(MockStore::default()));
    let keys: Vec<String> = (0..20_000).map(|i| format!("src/key-{:06}", i)).collect();
    let server = store_server(Arc::clone(&store), keys.clone(), false);
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/src/",
            "-o",
            "s3://mock-bucket/out/",
            "--no-auto-hints",
        ],
    );
    // 1 KiB upload parts: every part file is many upload parts.
    let (code, stdout, stderr) =
        run_cli_with_env(&args, dir.path(), &[("S3TL_UPLOAD_PART_BYTES", "1024")]);
    ok_result(code, &stdout, &stderr);
    let uploads = server
        .requests()
        .iter()
        .filter(|r| r.method == "PUT" && r.query.contains_key("partNumber"))
        .count();
    assert!(uploads > 4, "{} upload parts", uploads);
    let store = store.lock().unwrap();
    let mut got = Vec::new();
    for (name, body) in &store.objects {
        if name.ends_with(".parquet") {
            got.extend(parquet_keys(body));
        }
    }
    got.sort();
    assert_eq!(got, keys);
}

#[test]
fn local_mock_s3_output_failed_result_marker_takes_the_parts_back() {
    let store = Arc::new(Mutex::new(MockStore {
        result_put: 1,
        ..Default::default()
    }));
    let server = store_server(Arc::clone(&store), source_keys(), false);
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/src/",
            "-o",
            "s3://mock-bucket/out/",
            "--no-auto-hints",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = failed_result(code, &stdout, &stderr, 5, "output.write_failed");
    assert_eq!(result["output"]["files"], json!([]));
    let store = store.lock().unwrap();
    assert!(store.objects.is_empty(), "{:?}", store.objects.keys());
}

#[test]
fn local_mock_s3_output_stored_result_marker_with_a_lost_response_commits() {
    let store = Arc::new(Mutex::new(MockStore {
        result_put: 2,
        ..Default::default()
    }));
    let server = store_server(Arc::clone(&store), source_keys(), false);
    let dir = tempfile::tempdir().unwrap();
    let args = cli(
        &server,
        "list",
        &[
            "s3://mock-bucket/src/",
            "-o",
            "s3://mock-bucket/out/",
            "--no-auto-hints",
        ],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    ok_result(code, &stdout, &stderr);
    let store = store.lock().unwrap();
    assert!(store.objects.contains_key("out/_result.json"));
    assert!(store.objects.keys().any(|k| k.ends_with(".parquet")));
}

// ── Wide namespaces: more top-level prefixes than a page ────

/// An endpoint with 100-entry pages over `keys`: startup discovery has to
/// follow pages, and past its page budget bisect the rest.
fn small_page_server(keys: Vec<String>) -> MockS3Server {
    MockS3Server::start(move |request, _sequence| {
        let mut q = request.query.clone();
        let asked: usize = q
            .get("max-keys")
            .and_then(|m| m.parse().ok())
            .unwrap_or(1000);
        q.insert("max-keys".into(), asked.min(100).to_string());
        MockResponse::ok_xml(emulated_list(&keys, &q))
    })
}

fn wide_keys() -> Vec<String> {
    let mut keys: Vec<String> = (0..2500)
        .flat_map(|d| (0..3).map(move |i| format!("d{d:05}/f{i}")))
        .collect();
    keys.sort();
    keys
}

#[test]
fn local_mock_wide_namespace_is_partitioned_past_the_first_page() {
    let keys = wide_keys();
    let server = small_page_server(keys.clone());
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let args = cli(
        &server,
        "list",
        &["s3://mock-bucket", "-c", "16", "-o", out.to_str().unwrap()],
    );
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["stats"]["objects"], keys.len());
    assert_eq!(sorted(dataset_keys(&out)), keys, "every key exactly once");

    let requests = server.requests();
    // The root delimiter probe followed its pages...
    assert!(requests.iter().any(|r| is_delimiter_probe(r)
        && query(r, "prefix").unwrap_or("").is_empty()
        && r.query.contains_key("continuation-token")));
    // ...and the tail past them (8 pages x 100 prefixes) was bisected by
    // single-key probes instead of being left to one segment.
    assert!(
        requests
            .iter()
            .any(|r| is_single_key_probe(r)
                && query(r, "start-after").is_some_and(|s| s > "d00800/"))
    );
    // Listing requests start well past the first 800 prefixes too.
    let starts: Vec<&str> = requests
        .iter()
        .filter(|r| !is_delimiter_probe(r) && !is_single_key_probe(r))
        .filter_map(|r| query(r, "start-after"))
        .filter(|s| *s > "d00900/")
        .collect();
    assert!(starts.len() >= 2, "segments in the tail: {:?}", starts);
}

#[test]
fn local_mock_wide_namespace_diff_matches_key_for_key() {
    let keys = wide_keys();
    let mut right = keys.clone();
    right.retain(|k| !k.ends_with("/f1") || !k.starts_with("d02"));
    let server = small_page_server(keys.clone());
    let other = small_page_server(right.clone());
    let dir = tempfile::tempdir().unwrap();
    let mut args = cli(
        &server,
        "diff",
        &["s3://mock-bucket", "s3://mock-bucket", "-c", "16"],
    );
    args.extend(strings(&["--target-endpoint-url", &other.endpoint()]));
    let (code, stdout, stderr) = run_cli(&args, dir.path());
    let result = ok_result(code, &stdout, &stderr);
    assert_eq!(result["diff"]["left_only"], keys.len() - right.len());
    assert_eq!(result["diff"]["right_only"], 0);
    // Every key on both sides was paired (the emulator's size and ETag
    // depend on a key's position in its page, so pairs may read changed).
    let paired =
        result["diff"]["equal"].as_u64().unwrap() + result["diff"]["changed"].as_u64().unwrap();
    assert_eq!(paired as usize, right.len());
}

#[test]
fn local_mock_listing_does_not_wait_for_startup_discovery() {
    // Startup discovery runs alongside the listing: a slow probe no longer
    // delays the first object, and a listing that finishes first ends the
    // run without waiting for it.
    let server = MockS3Server::start(|request, _sequence| {
        if is_delimiter_probe(&request) || is_single_key_probe(&request) {
            thread::sleep(Duration::from_secs(3));
            return MockResponse::ok_xml(list_bucket_xml("", 1000, &[], &["a/"], false, None));
        }
        MockResponse::ok_xml(list_bucket_xml("", 1000, &["a/1", "a/2"], &[], false, None))
    });
    let dir = tempfile::tempdir().unwrap();
    let started = std::time::Instant::now();
    let args = cli(&server, "list", &["s3://mock-bucket", "-o", "-"]);
    let (code, stdout, stderr) =
        run_cli_with_env(&args, dir.path(), &[("S3TL_TIMEOUT_SECS", "10")]);
    assert_eq!(code, 0, "{}", stderr);
    assert_eq!(ndjson_keys(&stdout), vec!["a/1", "a/2"]);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the run waited for discovery: {:?}",
        started.elapsed()
    );
}
