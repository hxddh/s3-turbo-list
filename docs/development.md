# Development

Build, test and benchmark without contacting a real endpoint. Release
builds are in [`releasing.md`](releasing.md).

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Layout

| Path | |
|---|---|
| `src/app/` | The binary: command line (`cli.rs`), validation without I/O (`job.rs`), the run (`run.rs`), `check`, and the result contract (`result.rs`). |
| `src/tasks_s3/` | The listing engine: the reactor (segments, runtime splits, resume ranges), one ListObjectsV2 chain per segment, split probes. |
| `src/startup.rs`, `auto_hints.rs`, `flat_cut.rs` | Startup partitioning: discovery, bisection, flat cut search. |
| `src/list_page.rs` | The fast ListObjectsV2 `<Contents>` parser. |
| `src/data_map.rs` | Output: Parquet parts, NDJSON, statistics, the diff merge. |
| `src/segment_order.rs` | A diff side's segment order under runtime splits. |
| `src/utils.rs` | The Parquet writer and the output schemas. |
| `src/checkpoint.rs` | `_checkpoint.json`. |

## Tests

- Unit tests next to the code (`cargo test --lib`), including a
  differential test of the fast page parser against the SDK's deserializer.
- `tests/cli_integration.rs`: the command-line contract without any network
  (usage errors, dry runs, the result document).
- `tests/s3_mock_integration.rs`: the binary against an in-process
  ListObjectsV2 mock on `127.0.0.1` (pagination, partitioning, runtime
  splits, retries and throttling, interrupt and resume, diff, `check`,
  traces). Each connection is served on its own thread, so a handler can
  model latency; `MockS3Server::max_in_flight()` asserts that a run really
  overlapped requests. A panic inside a handler is re-raised on the test
  thread.
- `tests/data_map_integration.rs`: the diff merge and output writers.

Spawned binaries run with an isolated `HOME` and without the developer's
AWS, proxy and `S3TL_*` variables (`tests/common`). The mock tests make the
binary fail fast with the undocumented `S3TL_MAX_ATTEMPTS`,
`S3TL_BACKOFF_SECS` and `S3TL_TIMEOUT_SECS` variables.

The mock is a regression harness, not a provider emulator: it ignores
signatures and checks only what `s3tl` relies on. Add scenarios for
behaviour the CLI depends on; never add provider workarounds to it.

## Benchmarks

Performance changes need numbers.

**Output pipeline** (no S3): synthetic objects through the production
Parquet, NDJSON or diff path.

```bash
cargo run --release --example bench_local -- parquet 1000000
cargo run --release --example bench_local -- ndjson 1000000
cargo run --release --example bench_local -- diff 1000000
```

**End to end** against a local mock with per-request latency:

```bash
python3 scripts/mock-s3.py 19000 20 skew:skew:200000 skew-mod:skew:200000 tree:tree:50000 &
export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y AWS_EC2_METADATA_DISABLED=true
s3tl list s3://skew -o /tmp/out --endpoint-url http://127.0.0.1:19000 | jq .stats
s3tl diff s3://skew s3://skew-mod --endpoint-url http://127.0.0.1:19000 | jq .diff
```

The Python mock spends CPU per key; for A/B comparisons run both binaries
against the same mock and compare medians of several runs (`NOENC=1` makes
it ignore `encoding-type=url`, removing its URL-encoding cost). Results for
1.0 are in `docs/validation-results/s3tl-1.0-redesign-20261114.md`.

Do not point benchmarks at AWS or any other real endpoint unless that run is
explicitly authorized.
