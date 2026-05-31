# Diff Output Benchmark - 2026-05-31

**Network:** none. All runs used `benchmark-local` with synthetic local
left/right object streams.

**Purpose:** establish a v0.2.9 development baseline for the diff output path
after adding `benchmark-local --benchmark diff-output` and reducing dump-stage
allocation by classifying objects directly from the per-prefix map.

## Commands

```bash
BUILD_MODE=clang ./scripts/build-release.sh

BENCHMARK=diff-output OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=/tmp/s3-turbo-list-v029-diff-output.json \
  ./scripts/benchmark-local.sh

BENCHMARK=diff-map OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=/tmp/s3-turbo-list-v029-diff-map.json \
  ./scripts/benchmark-local.sh
```

The `diff-output` run uses a mixed synthetic distribution: equal, left-only,
right-only, and changed objects are generated evenly. It writes Parquet and
KeySpace output to temporary files, measures their size, then removes the
artifacts unless `--keep-artifacts` is used.

## Results

| Benchmark | Elapsed secs | Input side-rows/sec | Unique rows/sec | Received side-rows | Unique rows | Parquet rows | KS entries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| diff-output | 2.765 | 542,426 | 361,617 | 1,500,000 | 1,000,000 | 1,000,000 | 1,024 |
| diff-map | 1.869 | 1,069,911 | 534,956 | 2,000,000 | 1,000,000 | 0 | 0 |

`diff-output` wrote 11,073,827 Parquet bytes and 19,370 KeySpace bytes,
for 11.09 output bytes per unique object with zstd level 3.

## Notes

- The reported `tool_version` is `0.2.8` because this benchmark was run before
  the v0.2.9 version bump and release preparation.
- `diff-map` measures construction only. It does not write Parquet or KeySpace
  output, so it is an orientation point rather than a direct throughput target
  for the full output path.
- These runs do not contact S3-compatible endpoints.
