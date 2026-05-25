# Benchmarking

The local synthetic benchmark harness measures list-mode output paths without
contacting S3.  It generates in-memory object batches and writes them through
the normal data-map code.

```bash
cargo build --release
./target/release/s3-turbo-list benchmark-local \
  --objects 100000 \
  --batch-size 5000 \
  --prefixes 512 \
  --output-format parquet \
  --compression gzip \
  --compression-level 6 \
  --json
```

Or use the wrapper:

```bash
./scripts/benchmark-local.sh
```

Environment overrides:

```bash
OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 ./scripts/benchmark-local.sh
COMPRESSION=zstd COMPRESSION_LEVEL=3 ./scripts/benchmark-local.sh
OUTPUT_FORMAT=ndjson ./scripts/benchmark-local.sh
```

`--output-format parquet` measures the Parquet plus KeySpace streaming path.
`--output-format tsv` and `--output-format ndjson` measure the list stdout row
formatters by writing rows to a temporary local file, not to the terminal.

The JSON report includes:

- tool version
- Parquet compression codec and level
- object count, batch size, and prefix count
- elapsed seconds and objects/sec
- Parquet and KS byte sizes
- text output byte size for TSV/NDJSON runs
- per-object byte counts and local output MiB/sec rates for easier comparisons
- streamed rows/sec
- data-map metrics: received batches, received objects, streamed rows,
  unique prefixes, Parquet rows, and KS entries

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

## Compression Notes

The default Parquet compression is `zstd(3)`.  The v0.1.26 local benchmark
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
