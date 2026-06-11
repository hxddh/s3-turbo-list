# List Parquet ETag Buffer Benchmark - 2026-06-11

**Network:** none. All runs used `benchmark-local` with synthetic local objects.

**Purpose:** validate the v0.2.18 development change that formats list-mode
Parquet ETags into a fixed stack buffer before appending them to Arrow string
builders.

**Host:** Ubuntu 20.04 arm64.

## Commands

```bash
./dist/s3-turbo-list-0.2.17-linux-aarch64 benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --json

./dist/s3-turbo-list-0.2.17-linux-aarch64 benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --producers 8 --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --producers 8 --json

./dist/s3-turbo-list-0.2.17-linux-aarch64 benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format tsv --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format tsv --json

./dist/s3-turbo-list-0.2.17-linux-aarch64 benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format ndjson --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format ndjson --json
```

Each scenario was run three times and compared by median `objects_per_sec`.

## Results

| Scenario | Binary | Median objects/sec | Median output MiB/sec |
| --- | --- | ---: | ---: |
| Parquet, producers=1 | v0.2.17 dist | 1,189,775 | 9.56 |
| Parquet, producers=1 | v0.2.18 dev | 1,238,511 | 9.95 |
| Parquet, producers=8 | v0.2.17 dist | 1,178,018 | 9.27 |
| Parquet, producers=8 | v0.2.18 dev | 1,201,560 | 9.55 |
| TSV | v0.2.17 dist | 2,524,264 | 100.91 |
| TSV | v0.2.18 dev | 2,495,218 | 99.74 |
| NDJSON | v0.2.17 dist | 2,726,665 | 150.60 |
| NDJSON | v0.2.18 dev | 2,780,742 | 153.59 |

## Summary

| Scenario | Median objects/sec change |
| --- | ---: |
| Parquet, producers=1 | +4.1% |
| Parquet, producers=8 | +2.0% |
| TSV | -1.2% |
| NDJSON | +2.0% |

The targeted Parquet list-output path improved in both producer scenarios while
preserving output format and command behavior. TSV and NDJSON do not use the
changed Parquet ETag writer path; those runs were smoke checks for unrelated
formatter regressions, and the small TSV delta is within local run-to-run noise.
