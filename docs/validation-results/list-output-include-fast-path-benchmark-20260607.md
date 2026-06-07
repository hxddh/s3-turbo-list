# List Output Include Fast-Path Benchmark - 2026-06-07

**Network:** none. All runs used `benchmark-local` with synthetic local objects.

**Purpose:** validate the v0.2.17 development change that uses a lightweight
list-only inclusion check for list output instead of the diff-oriented final
status state machine.

**Host:** Ubuntu 20.04 arm64.

## Commands

```bash
./dist/s3-turbo-list-0.2.16-linux-aarch64 benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --json

./dist/s3-turbo-list-0.2.16-linux-aarch64 benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --producers 8 --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format parquet --producers 8 --json

./dist/s3-turbo-list-0.2.16-linux-aarch64 benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format tsv --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --objects 1000000 --batch-size 10000 \
  --prefixes 1024 --output-format tsv --json

./dist/s3-turbo-list-0.2.16-linux-aarch64 benchmark-local \
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
| Parquet, producers=1 | v0.2.16 dist | 1,209,020 | 9.72 |
| Parquet, producers=1 | v0.2.17 dev | 1,229,328 | 9.88 |
| Parquet, producers=8 | v0.2.16 dist | 1,198,030 | 9.53 |
| Parquet, producers=8 | v0.2.17 dev | 1,225,829 | 9.76 |
| TSV | v0.2.16 dist | 2,629,335 | 105.11 |
| TSV | v0.2.17 dev | 2,715,813 | 108.56 |
| NDJSON | v0.2.16 dist | 2,918,070 | 161.17 |
| NDJSON | v0.2.17 dev | 2,981,559 | 164.68 |

## Summary

| Scenario | Median objects/sec change |
| --- | ---: |
| Parquet, producers=1 | +1.7% |
| Parquet, producers=8 | +2.3% |
| TSV | +3.3% |
| NDJSON | +2.2% |

The change keeps external behavior unchanged and reduces per-object work in
the default list output pipeline. The local benchmark does not exercise real
endpoint latency or provider-side throughput.
