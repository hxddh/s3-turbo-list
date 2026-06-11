# Benchmarking

The local synthetic benchmark harness measures list-mode output paths without
contacting S3.  It generates in-memory object batches and writes them through
the normal data-map code.

```bash
cargo build --release
./target/release/s3-turbo-list benchmark-local \
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

Or use the wrapper:

```bash
./scripts/benchmark-local.sh

# Ubuntu 20.04 arm64 may need the same aws-lc-sys workaround as release builds:
BUILD_MODE=clang ./scripts/benchmark-local.sh
```

To compare all local output formats with repeated runs and median summaries:

```bash
./scripts/benchmark-output-formats.sh
RUNS=5 OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=benchmark-results/output-formats.json \
  MARKDOWN=benchmark-results/output-formats.md \
  ./scripts/benchmark-output-formats.sh
```

Environment overrides:

```bash
OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 ./scripts/benchmark-local.sh
PRODUCERS=8 OBJECTS=1000000 ./scripts/benchmark-local.sh
COMPRESSION=zstd COMPRESSION_LEVEL=3 ./scripts/benchmark-local.sh
OUTPUT_FORMAT=ndjson ./scripts/benchmark-local.sh
BENCHMARK=diff-map OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 ./scripts/benchmark-local.sh
BENCHMARK=diff-output OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 ./scripts/benchmark-local.sh
BENCHMARK=diff-output DIFF_SHAPE=all-changed OBJECTS=1000000 ./scripts/benchmark-local.sh
```

All benchmark wrapper scripts use `scripts/build-release.sh` when the default
release binary is missing, so `BUILD_MODE=clang|gcc10|no-asm` has the same
meaning as in release builds.  To compare a published or previously built
binary, set `BIN=/path/to/s3-turbo-list`; when `BIN` is set, the wrappers use
that binary as-is and fail if it is not executable.

For stdout formatter changes, run each format at least three times and compare
medians against the previous release binary.  This keeps single-run CPU noise
from driving release decisions.

`--benchmark list-output` is the default. In that mode,
`--output-format parquet` measures the Parquet plus KeySpace streaming path.
`--output-format tsv` and `--output-format ndjson` measure the list stdout row
formatters by writing rows to a temporary local file, not to the terminal.
`--benchmark diff-map` measures only the local diff data-map construction path:
it inserts the requested object count for both left and right sides into the
in-memory prefix/object map and does not write output artifacts.
`--benchmark diff-output` measures local diff construction plus `PrefixMap`
dump, Parquet output, and KeySpace output. Its `--diff-shape` option supports
`mixed` (the default), `all-equal`, and `all-changed` synthetic distributions.

The JSON report includes:

- tool version
- benchmark scenario name
- Parquet compression codec and level
- object count, batch size, and prefix count
- synthetic producer count and configured channel capacity
- elapsed seconds and objects/sec
- Parquet and KS byte sizes
- text output byte size for TSV/NDJSON runs
- per-object byte counts and local output MiB/sec rates for easier comparisons
- streamed rows/sec
- cumulative producer send-wait seconds, useful when exploring channel backpressure
- data-map metrics: received batches, received objects, streamed rows,
  unique prefixes, Parquet rows, and KS entries

`scripts/benchmark-output-formats.sh` writes a combined JSON summary with schema
`s3-turbo-list.output-format-benchmark.v1` and a Markdown table containing
median elapsed seconds, objects/sec, output MiB/sec, and bytes/object for each
format.  The report also records reproducibility metadata: git commit and dirty
state, UTC start time, platform, Rust host triple, build profile, binary path,
compression settings, producer count, and the local `benchmark-local` command
template.

Real endpoint benchmarks remain intentionally opt-in.  Do not use benchmark
scripts against AWS, BOS, R2, B2, OSS, or other cloud endpoints unless the run
has been explicitly authorized.

## Compression Benchmark Matrix

Use `scripts/benchmark-compression.sh` to compare common Parquet codecs on the
same local synthetic dataset.  It runs `gzip(6)`, `zstd(3)`, `zstd(6)`, `lz4`,
and `snappy`, then writes both a machine-readable JSON summary and a Markdown
table:

```bash
./scripts/benchmark-compression.sh
OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=benchmark-results/compression.json \
  MARKDOWN=benchmark-results/compression.md \
  ./scripts/benchmark-compression.sh
```

The benchmark does not contact S3.  It measures the local list-mode streaming
output path, so the results are useful for comparing CPU and file-size tradeoffs
in Parquet writing.  They do not predict end-to-end runtime when a real endpoint
or network link is the bottleneck.

The v0.1.26 release includes a local arm64 reference run in
[`docs/validation-results/compression-benchmark-20260524.md`](validation-results/compression-benchmark-20260524.md).
The v0.2.3 release includes a local stdout formatter reference run in
[`docs/validation-results/output-benchmark-20260525.md`](validation-results/output-benchmark-20260525.md).
In that run, the v0.2.3 batch-buffered stdout path improved median local TSV
formatter time by about 8.2% and NDJSON formatter time by about 9.4% versus the
published v0.2.2 binary.  These numbers isolate local rendering and file
output; real endpoint latency or network throughput can dominate end-to-end
runtime.
The v0.2.8 development cycle adds a diff data-map construction baseline in
[`docs/validation-results/diff-map-benchmark-20260531.md`](validation-results/diff-map-benchmark-20260531.md).
The v0.2.15 development cycle includes a list-mode Parquet hot-path comparison
in
[`docs/validation-results/list-output-hot-path-benchmark-20260606.md`](validation-results/list-output-hot-path-benchmark-20260606.md).
The v0.2.16 development cycle includes a follow-up single-pass list-mode
Parquet comparison in
[`docs/validation-results/list-parquet-single-pass-benchmark-20260606.md`](validation-results/list-parquet-single-pass-benchmark-20260606.md).
The v0.2.17 development cycle includes a list-output inclusion fast-path
comparison in
[`docs/validation-results/list-output-include-fast-path-benchmark-20260607.md`](validation-results/list-output-include-fast-path-benchmark-20260607.md).
The v0.2.18 development cycle includes a list-mode Parquet ETag buffer
comparison in
[`docs/validation-results/list-parquet-etag-buffer-benchmark-20260611.md`](validation-results/list-parquet-etag-buffer-benchmark-20260611.md).

## Compression Notes

The default Parquet compression is `zstd(1)`.  The v0.1.26 local benchmark
showed a better speed/size balance than the previous `gzip(6)` default on the
list-mode streaming output path.  For traditional gzip output, pass both codec
and level explicitly:

```bash
s3-turbo-list --compression gzip --compression-level 6 \
  --delimiter '' list --bucket my-bucket --region us-east-1
```

```toml
[output]
compression = "gzip"
compression_level = 6
```

Compression choice affects local CPU time and output size only; it does not
change S3 request behavior.
