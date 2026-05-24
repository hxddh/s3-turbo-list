# Compression Benchmark — 2026-05-24

This local benchmark validates the v0.1.26 compression benchmark harness.  It
uses synthetic in-memory object batches and the list-mode streaming Parquet
output path.  It does not contact S3 or any S3-compatible cloud endpoint.

Environment:

- Host: Ubuntu 20.04 arm64
- Build: `CC=clang CXX=clang++ cargo build --release`
- Binary: `target/release/s3-turbo-list`
- Tool version: `0.1.26`
- Objects: `100000`
- Batch size: `5000`
- Prefixes: `512`

Command:

```bash
OUT=/tmp/s3-turbo-list-v0126-release-compression.json \
MARKDOWN=/tmp/s3-turbo-list-v0126-release-compression.md \
BIN=/home/ubuntu/s3-turbo-list/target/release/s3-turbo-list \
OBJECTS=100000 \
BATCH_SIZE=5000 \
PREFIXES=512 \
./scripts/benchmark-compression.sh
```

Results:

| Codec | Level | Seconds | Objects/sec | Parquet bytes | Bytes/object | Parquet MiB/sec |
|---|---:|---:|---:|---:|---:|---:|
| gzip | 6 | 0.3984 | 250993 | 1307674 | 13.08 | 3.13 |
| zstd | 3 | 0.1352 | 739439 | 758302 | 7.58 | 5.35 |
| zstd | 6 | 0.1620 | 617301 | 784274 | 7.84 | 4.62 |
| lz4 | 6 | 0.1229 | 813568 | 2309301 | 23.09 | 17.92 |
| snappy | 6 | 0.1183 | 844972 | 2410916 | 24.11 | 19.43 |

Interpretation:

- `zstd(3)` was substantially faster than `gzip(6)` on this local output path
  while also producing a smaller Parquet file for this synthetic dataset.
- `lz4` and `snappy` were fastest in wall-clock time but produced much larger
  Parquet files.
- These results measure local output and compression behavior only.  They do
  not predict end-to-end runtime when real endpoint latency or network
  throughput is the bottleneck.
- The default remains `gzip(6)` in v0.1.26.  A default codec change should be
  handled as a separate behavior-change release.
