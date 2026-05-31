# Diff Data-Map Benchmark - 2026-05-31

**Network:** none. All runs used `benchmark-local --benchmark diff-map`
with synthetic local left/right object streams.

**Purpose:** establish a v0.2.8 baseline for the diff data-map construction
path after replacing the single-consumer `DashMap` storage with `HashMap`-
backed maps. The published v0.2.7 binary does not include this benchmark mode,
so this note records a reproducible baseline rather than a cross-version
percentage comparison.

## Command

```bash
CC=clang CXX=clang++ cargo build --release

./target/release/s3-turbo-list benchmark-local \
  --benchmark diff-map \
  --objects 1000000 \
  --batch-size 10000 \
  --prefixes 1024 \
  --json
```

Each run inserts 1,000,000 synthetic objects for the left side and the same
1,000,000 object keys for the right side. The resulting map contains 1,000,000
deduplicated object entries across 1,024 prefixes.

## Results

| Run | Elapsed secs | Inserted objects/sec | Received objects | Unique objects | Prefixes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.923 | 1,039,859 | 2,000,000 | 1,000,000 | 1,024 |
| 2 | 1.944 | 1,028,949 | 2,000,000 | 1,000,000 | 1,024 |
| 3 | 1.905 | 1,049,805 | 2,000,000 | 1,000,000 | 1,024 |

Median elapsed time: **1.923s**.

Median inserted objects/sec: **1.04M**.

## Notes

- This benchmark isolates local data-map grouping, insertion, deduplication,
  and left/right matching. It does not write Parquet, write KeySpace output, or
  contact S3-compatible endpoints.
- The benchmark is intended to make future data-structure and sharding changes
  comparable. Do not compare these numbers directly with list-output Parquet,
  TSV, or NDJSON benchmarks because those measure different output work.
