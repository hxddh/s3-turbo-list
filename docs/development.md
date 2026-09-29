# Development: Local Benchmarks and the S3 Mock

Two local harnesses for working on s3-turbo-list without contacting a real
endpoint: a synthetic output benchmark, and an integration-test S3 mock.
Build and release instructions are in [`releasing.md`](releasing.md).

## Local synthetic benchmark

`bench_local` is a Cargo example (it was the hidden `benchmark-local`
subcommand before 0.37).  It generates in-memory object batches and drives
them through the normal data-map writers, measuring the output pipeline
without contacting S3:

```bash
cargo run --release --example bench_local -- \
  --benchmark list-output \
  --objects 100000 \
  --batch-size 5000 \
  --prefixes 512 \
  --producers 1 \
  --output-format parquet \
  --compression zstd \
  --compression-level 1 \
  --json
```

`cargo run --release --example bench_local -- --help` lists every option.

| `--benchmark` | Measures |
|---|---|
| `list-output` (default) | With `--output-format parquet`, the Parquet plus KeySpace streaming path; with `tsv` / `ndjson`, the list stdout row formatters, writing to a temporary local file rather than the terminal. |
| `diff-map` | The streaming diff merge plus row encoding against a null writer (no file IO, no artifacts). |
| `diff-output` | The same merge with real Parquet and KeySpace artifacts. |

Both diff scenarios drive the production merge engine.  `--diff-shape`
selects the synthetic distribution: `mixed` (default), `all-equal`, or
`all-changed`.  `--keep-artifacts` keeps the generated files.

The JSON report includes the tool version, scenario, Parquet codec and level,
object/batch/prefix/producer counts, channel capacity, elapsed seconds,
objects/sec and rows/sec, Parquet/KS (or TSV/NDJSON) byte sizes and
per-object byte counts, local output MiB/sec, cumulative producer send-wait
seconds (useful when exploring channel backpressure), and the data-map
metrics: received batches and objects, streamed rows, unique prefixes,
Parquet rows, and KS entries.

### Wrapper scripts

```bash
./scripts/benchmark-local.sh

OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 ./scripts/benchmark-local.sh
PRODUCERS=8 OBJECTS=1000000 ./scripts/benchmark-local.sh
COMPRESSION=zstd COMPRESSION_LEVEL=3 ./scripts/benchmark-local.sh
OUTPUT_FORMAT=ndjson ./scripts/benchmark-local.sh
BENCHMARK=diff-map OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 ./scripts/benchmark-local.sh
BENCHMARK=diff-output DIFF_SHAPE=all-changed OBJECTS=1000000 ./scripts/benchmark-local.sh
```

The wrappers build `target/release/examples/bench_local` when it is missing.
`BUILD_MODE=clang|gcc10|no-asm` selects the same compiler workarounds as
release builds (needed on Ubuntu 20.04 arm64; see
[`releasing.md`](releasing.md)).  `BIN=/path/to/bench_local` runs a
previously built benchmark binary as-is and fails if it is not executable.

Compare all local output formats with repeated runs and median summaries:

```bash
./scripts/benchmark-output-formats.sh
RUNS=5 OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=benchmark-results/output-formats.json \
  MARKDOWN=benchmark-results/output-formats.md \
  ./scripts/benchmark-output-formats.sh
```

It writes a combined JSON summary (schema
`s3-turbo-list.output-format-benchmark.v1`) and a Markdown table of median
elapsed seconds, objects/sec, output MiB/sec, and bytes/object per format,
with reproducibility metadata: git commit and dirty state, UTC start time,
platform, Rust host triple, build profile, binary path, compression settings,
producer count, and the benchmark command template.

For stdout formatter changes, run each format at least three times and
compare medians against the previous release, so single-run CPU noise does
not drive release decisions.

### Compression matrix

`scripts/benchmark-compression.sh` compares `gzip(6)`, `zstd(3)`, `zstd(6)`,
`lz4`, and `snappy` on the same synthetic dataset and writes a JSON summary
and a Markdown table:

```bash
./scripts/benchmark-compression.sh
OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=benchmark-results/compression.json \
  MARKDOWN=benchmark-results/compression.md \
  ./scripts/benchmark-compression.sh
```

The default Parquet compression is `zstd(1)`, chosen for a better speed/size
balance than `gzip(6)` on the streaming output path.  Compression affects
local CPU and output size only, not S3 request behavior, so these numbers do
not predict end-to-end runtime when the endpoint or network is the
bottleneck.  For traditional gzip output, pass codec and level explicitly
(`--compression-level` is a supported option hidden from `--help`):

```bash
s3-turbo-list list --bucket my-bucket --region us-east-1 \
  --compression gzip --compression-level 6
```

```toml
[output]
compression = "gzip"
compression_level = 6
```

Real endpoint benchmarks are opt-in: do not point benchmark scripts at AWS,
BOS, R2, B2, OSS, or other cloud endpoints unless the run is explicitly
authorized.

## Local S3 protocol mock

An integration-test-only S3-compatible mock server
(`tests/s3_mock_integration.rs`), used as a local correctness harness for
the CLI, the AWS SDK request path, XML response parsing, trace fields,
checkpoint/resume, and retry behavior.  It listens on `127.0.0.1` on an
ephemeral port, is started by `cargo test`, and tests use dummy AWS
credentials with path-style addressing:

```bash
cargo test --test s3_mock_integration
```

It covers:

- ListObjectsV2 pagination with `NextContinuationToken`.
- Requests carrying `prefix`, `delimiter`, `max-keys`, `start-after`, and
  `continuation-token`.
- XML responses with `Contents`, `CommonPrefixes`, and error bodies.
- `compat-probe` behavior for `HeadBucket`, single-page list variants, and
  pagination.
- Checkpoint/resume identity and remaining-range behavior.
- SDK retry of a transient `503 SlowDown`.
- Concurrent listing: each connection is served on its own thread, so a
  handler can model per-request latency and a test can assert that a run
  really overlapped requests.

### Observing concurrency

The client opens a connection per request, so served connections track
in-flight requests.  `MockS3Server::max_in_flight()` reports the peak, which
is what distinguishes a run that fans out from one that does not — request
counts and query shapes cannot.  A handler that sleeps models a slow endpoint
or a hot key range, and because connections are served concurrently that
latency overlaps the way a real endpoint's would.

Prefer `max_in_flight()` over wall-clock assertions: a run shorter than the
monitor heartbeat is dominated by fixed overhead, and timing assertions are
flaky under CI load.

Handlers run on connection threads.  A panic there (an `assert!` in a
handler, for instance) would otherwise reach the client as a reset socket and
look like a network error; the mock captures it and re-raises it on the test
thread when the server is dropped.

### Safety boundary and maintenance

The mock never contacts AWS S3, BOS, MinIO, R2, B2, OSS, Spaces, or any other
real provider.  It is not a provider validation substitute and does not
claim compatibility for any endpoint.  It ignores request signatures and
validates only the request method, the path-style bucket path, and the S3
query fields s3-turbo-list relies on — a local regression harness, not a
general S3 emulator.

Keep it narrow: add scenarios only for behavior the CLI depends on (listing,
trace metadata, checkpoint/resume, retry, compat-probe), and do not add
provider-specific workarounds to it unless the production feature is
explicitly approved.
