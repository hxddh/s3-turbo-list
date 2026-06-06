# List Output Hot Path Benchmark - 2026-06-06

**Network:** none. All runs used `benchmark-local` with synthetic local objects.

**Purpose:** validate the v0.2.15 development change that writes list-mode
Parquet batches through Arrow builders and avoids the intermediate filtered row
vector.

**Host:** Ubuntu 20.04 arm64.

## Commands

```bash
./dist/s3-turbo-list-0.2.14-linux-aarch64 benchmark-local \
  --benchmark list-output --output-format parquet \
  --objects 1000000 --batch-size 10000 --prefixes 1024 --json

CC=clang cargo build --release

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --output-format parquet \
  --objects 1000000 --batch-size 10000 --prefixes 1024 --json
```

TSV and NDJSON smoke runs used the same object, batch, and prefix settings.

## Results

| Binary | Output | Objects/sec | Output MiB/sec | Producer send wait secs |
| --- | --- | ---: | ---: | ---: |
| v0.2.14 dist | parquet | 999,027 | 8.03 | 0.212 |
| v0.2.15 dev | parquet | 1,220,101 | 9.80 | 0.160 |
| v0.2.14 dist | tsv | 2,369,770 | 94.73 | 0.013 |
| v0.2.15 dev | tsv | 2,393,680 | 95.69 | 0.001 |
| v0.2.14 dist | ndjson | 2,391,105 | 132.07 | 0.041 |
| v0.2.15 dev | ndjson | 2,593,820 | 143.26 | 0.001 |

The targeted Parquet list-output path improved by about 22.1% in this local
synthetic run. Parquet bytes per object and KeySpace bytes per object were
unchanged at 8.406889 and 0.01937, respectively.

The TSV and NDJSON runs are smoke checks for obvious regressions. The code
change does not alter their formatter path, and their measured differences
should be treated as local run variance rather than the v0.2.15 target metric.
