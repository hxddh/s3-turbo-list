# v0.2.3 Local Stdout Formatter Benchmark

Date: 2026-05-25

Host: Ubuntu 20.04 arm64, `/home/ubuntu/s3-turbo-list`

Network: none.  All runs used `benchmark-local` with synthetic in-memory object
batches and temporary local output files.

## Method

Baseline:

```bash
./dist/s3-turbo-list-0.2.2-linux-aarch64 benchmark-local \
  --objects 100000 --batch-size 5000 --output-format <format> --json
```

Candidate:

```bash
CC=clang cargo build --release
./target/release/s3-turbo-list benchmark-local \
  --objects 100000 --batch-size 5000 --output-format <format> --json
```

Each format was run three times.  The table uses the median elapsed time for
each version and format.

## Median Results

| Format | Version | Elapsed sec | Objects/sec | Text MiB/sec |
|---|---:|---:|---:|---:|
| TSV | v0.2.2 published binary | 0.067981 | 1,470,998 | 57.714 |
| TSV | v0.2.3 candidate | 0.062394 | 1,602,713 | 62.882 |
| NDJSON | v0.2.2 published binary | 0.065974 | 1,515,760 | 82.599 |
| NDJSON | v0.2.3 candidate | 0.059797 | 1,672,320 | 91.130 |

## Interpretation

The v0.2.3 candidate reduces local stdout formatter overhead in both measured
formats:

- TSV median elapsed time improved by about 8.2%.
- NDJSON median elapsed time improved by about 9.4%.

The benchmark isolates local rendering and file output.  It does not predict
end-to-end runtime when S3 endpoint latency, throttling, or network throughput
is the bottleneck.
