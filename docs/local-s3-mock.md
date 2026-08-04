# Local S3 Protocol Mock

An integration-test-only S3-compatible mock server.  It is a local
correctness harness for the CLI, AWS SDK request path, XML response parsing,
trace fields, checkpoint/resume segment selection, and retry behavior.

The mock listens on `127.0.0.1` with an ephemeral port and is started by
`cargo test`.  Tests use dummy AWS credentials and force path-style addressing:

```bash
cargo test --test s3_mock_integration
```

## What It Covers

- ListObjectsV2 pagination with `NextContinuationToken`.
- Requests containing `prefix`, `delimiter`, `max-keys`, `start-after`, and
  `continuation-token`.
- XML responses containing `Contents`, `CommonPrefixes`, and error bodies.
- `compat-probe` local behavior for `HeadBucket`, single-page list variants,
  encoding-type, and pagination.
- Checkpoint/resume identity behavior for hinted segments.
- SDK retry of a transient local `503 SlowDown` response.
- Concurrent listing: connections are served on their own threads, so a
  handler can model per-request latency and a test can assert that a run
  actually overlapped requests.

## Observing Concurrency

The client opens a connection per request, so served connections track
in-flight requests.  `MockS3Server::max_in_flight()` reports the peak, which
is what distinguishes a run that fans out from one that does not — request
counts and query shapes cannot.  A handler that sleeps models a slow endpoint
or a hot key range; because connections are served concurrently, that latency
overlaps the way a real endpoint's would.

Prefer `max_in_flight()` over wall-clock assertions.  A run shorter than the
monitor heartbeat finishes inside that window, so process wall clock is
dominated by fixed overhead rather than by listing cost, and timing
assertions are flaky under CI load.

Handlers run on connection threads.  A panic there — an `assert!` in a
handler, for instance — would otherwise reach the client as a reset socket
and be reported as a network error; the mock captures it and re-raises it on
the test thread when the server is dropped.

## Safety Boundary

This harness never contacts AWS S3, BOS, MinIO, R2, B2, OSS, Spaces, or any
other real provider.  It is not a provider validation substitute and does not
claim compatibility for undocumented endpoints.

The mock intentionally ignores request signatures.  It validates only the
request method, path-style bucket path, and S3 query fields that
`s3-turbo-list` relies on.  This keeps the harness focused on local regression
coverage instead of becoming a general S3 emulator.

## Maintenance Notes

Keep the mock narrow.  Add scenarios only for behavior the CLI depends on:
listing, trace metadata, checkpoint/resume, retry, and compat-probe.  Do not add
provider-specific workarounds or BOS pagination behavior to the mock unless the
production feature is explicitly approved.
