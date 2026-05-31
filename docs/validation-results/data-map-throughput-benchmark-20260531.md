# Data Map Throughput Benchmark

**Date:** 2026-05-31
**Network:** none. All runs used `benchmark-local` with synthetic local objects.
**Host:** Ubuntu 20.04 arm64.

## Scope

This benchmark checks the low-risk list output hot-path changes after v0.2.6:

- borrow only the object prefix instead of calling `ObjectKey::decode()` and
  allocating a discarded object name;
- aggregate list prefix statistics with `HashMap` and sort only when writing
  the KeySpace file;
- add `benchmark-local --producers` plus `producer_send_wait_secs` so local
  runs can expose channel send waiting.

It does not contact AWS, BOS, OSS, R2, B2, Spaces, or any other endpoint.

## Commands

Baseline binary:

```bash
./dist/s3-turbo-list-0.2.6-linux-aarch64 benchmark-local \
  --objects 1000000 --batch-size 10000 --prefixes 1024 \
  --output-format <format> --output <json>
```

Candidate binary:

```bash
CC=clang CXX=clang++ cargo build --release
./target/release/s3-turbo-list benchmark-local \
  --objects 1000000 --batch-size 10000 --prefixes 1024 \
  --output-format <format> --output <json>
```

Note: plain `cargo build --release` hit the known aarch64 `aws-lc-sys` GCC
memcmp guard. The clang build completed successfully.

## Results

Medians over three runs:

| Format | v0.2.6 median seconds | Candidate median seconds | Elapsed change | v0.2.6 median objects/sec | Candidate median objects/sec | Throughput change |
|---|---:|---:|---:|---:|---:|---:|
| Parquet | 1.2552 | 1.0550 | -15.9% | 796,717 | 947,853 | +19.0% |
| TSV | 0.6097 | 0.3739 | -38.7% | 1,640,226 | 2,674,417 | +63.1% |
| NDJSON | 0.5861 | 0.3341 | -43.0% | 1,706,204 | 2,992,718 | +75.4% |

The candidate binary still reports tool version `0.2.6` because the release
version has not been bumped on this branch.

## Interpretation

The gains are local synthetic output-path measurements, not end-to-end cloud
listing guarantees. Real endpoint latency, provider pagination behavior, object
metadata shape, disk speed, and compression settings can dominate production
runs.

The results support keeping the prefix borrowing and hash aggregation changes:
they reduce per-object CPU work without changing the default output contract.
The KeySpace file remains sorted by prefix at finalization.
