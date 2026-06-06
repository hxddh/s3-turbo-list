# List Parquet Single-Pass Benchmark - 2026-06-06

**Network:** none. All runs used `benchmark-local` with synthetic local objects.

**Purpose:** validate the v0.2.16 development change that folds list-mode
prefix/byte accounting into the Parquet writer selection pass and removes the
writer's key-length pre-scan.

**Host:** Ubuntu 20.04 arm64.

## Commands

```bash
./dist/s3-turbo-list-0.2.15-linux-aarch64 benchmark-local \
  --benchmark list-output --output-format parquet \
  --objects 1000000 --batch-size 10000 --prefixes 1024 --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --output-format parquet \
  --objects 1000000 --batch-size 10000 --prefixes 1024 --json

./dist/s3-turbo-list-0.2.15-linux-aarch64 benchmark-local \
  --benchmark list-output --output-format parquet \
  --objects 1000000 --batch-size 10000 --prefixes 1024 --producers 8 --json

./target/release/s3-turbo-list benchmark-local \
  --benchmark list-output --output-format parquet \
  --objects 1000000 --batch-size 10000 --prefixes 1024 --producers 8 --json
```

Each Parquet scenario was run three times and compared by median
`objects_per_sec`. TSV and NDJSON were smoke-checked with the same object,
batch, and prefix settings.

## Results

| Binary | Producers | Median objects/sec | Median output MiB/sec |
| --- | ---: | ---: | ---: |
| v0.2.15 dist | 1 | 1,130,062 | 9.08 |
| v0.2.16 dev | 1 | 1,224,276 | 9.84 |
| v0.2.15 dist | 8 | 1,194,210 | 9.49 |
| v0.2.16 dev | 8 | 1,194,510 | 9.50 |

The targeted single-producer Parquet list-output path improved by about 8.3%
in this local synthetic run. The `--producers 8` run was effectively flat,
which is acceptable for this change because it primarily removes local
single-consumer batch traversal overhead.

Parquet bytes per object and KeySpace bytes per object were unchanged at
8.406889 and 0.01937, respectively.

## Stdout Smoke Checks

| Output | Objects/sec | Output MiB/sec |
| --- | ---: | ---: |
| TSV | 2,476,830 | 99.01 |
| NDJSON | 2,638,055 | 145.71 |

The TSV and NDJSON paths do not use the changed Parquet writer path; these runs
only check for obvious local benchmark regressions.
