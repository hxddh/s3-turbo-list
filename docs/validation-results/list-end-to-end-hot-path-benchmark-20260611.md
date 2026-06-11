# List End-to-End Hot Path Benchmark - 2026-06-11

Purpose: compare the v0.2.19 development binary against the published v0.2.18
linux-aarch64 release binary on local synthetic list-output paths. This does
not contact S3 or any cloud endpoint.

Baseline binary:
`/tmp/s3tl-v0.2.18-baseline/s3-turbo-list-0.2.18-linux-aarch64`

Development binary:
`./target/release/s3-turbo-list`

Common arguments:

```bash
benchmark-local --benchmark list-output \
  --objects 1000000 \
  --batch-size 10000 \
  --prefixes 1024 \
  --json
```

Median of three runs except where noted:

| Scenario | v0.2.18 objects/sec | dev objects/sec | Change |
| --- | ---: | ---: | ---: |
| Parquet, 1 producer | 1,163,600 | 1,576,025 | +35.44% |
| Parquet, 8 producers (5 runs) | 1,193,443 | 1,521,979 | +27.53% |
| TSV, 1 producer | 2,553,221 | 2,661,299 | +4.23% |
| NDJSON, 1 producer | 2,759,660 | 2,502,035 | -9.34% |

Parquet output changed from `zstd(3)` with 10,000-row groups to `zstd(1)` with
100,000-row groups. In this synthetic fixture, compressed bytes per object also
fell from about 8.4 bytes to 4.6-5.2 bytes because the larger row groups gave
the codec more repeated structure per group.

The disabled-trace optimization is not exercised by `benchmark-local`, because
the local harness bypasses real ListObjectsV2 task execution. It is covered by
unit tests and the existing mock-S3 integration path.
