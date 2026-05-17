# Local S3 Protocol Mock

v0.1.9 adds an integration-test-only S3-compatible mock server.  It is a local
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
