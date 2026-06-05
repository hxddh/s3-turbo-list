# Diff Output Shapes Benchmark - 2026-06-05

**Network:** none. All runs used `benchmark-local --benchmark diff-output`
with synthetic local left/right object streams.

**Purpose:** validate the v0.2.10 development `--diff-shape` option for local
diff-output benchmarks without changing real S3 diff execution.

## Commands

```bash
BUILD_MODE=clang ./scripts/build-release.sh

BENCHMARK=diff-output DIFF_SHAPE=mixed OBJECTS=1000000 \
  BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=/tmp/s3-turbo-list-v0210-diff-output-mixed.json \
  ./scripts/benchmark-local.sh

BENCHMARK=diff-output DIFF_SHAPE=all-equal OBJECTS=1000000 \
  BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=/tmp/s3-turbo-list-v0210-diff-output-all-equal.json \
  ./scripts/benchmark-local.sh

BENCHMARK=diff-output DIFF_SHAPE=all-changed OBJECTS=1000000 \
  BATCH_SIZE=10000 PREFIXES=1024 \
  OUT=/tmp/s3-turbo-list-v0210-diff-output-all-changed.json \
  ./scripts/benchmark-local.sh
```

These runs were executed in parallel, so elapsed time should be treated as a
functional smoke benchmark rather than a release-quality median comparison.
Use serial repeated runs when comparing performance percentages.

## Results

| Shape | Elapsed secs | Input side-rows/sec | Unique rows/sec | Received side-rows | Unique rows | Parquet rows | KS entries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mixed | 2.881 | 520,707 | 347,138 | 1,500,000 | 1,000,000 | 1,000,000 | 1,024 |
| all-equal | 3.363 | 594,783 | 297,392 | 2,000,000 | 1,000,000 | 1,000,000 | 1,024 |
| all-changed | 3.402 | 587,805 | 293,902 | 2,000,000 | 1,000,000 | 1,000,000 | 1,024 |

## Notes

- `mixed` generates equal, plus, minus, and changed objects evenly, so it has
  fewer side rows than the all-paired shapes.
- `all-equal` and `all-changed` generate both left and right side rows for each
  unique object.
- The reported `tool_version` is `0.2.9` because this benchmark was run during
  v0.2.10 development before a version bump.
