# OSS List Hints Benchmark - 2026-06-11

Purpose: summarize a third-party real-provider test of `s3-turbo-list v0.2.19`
against Alibaba Cloud OSS. Credentials, instance identifiers, and account
details are intentionally omitted.

Environment:

- Provider: Alibaba Cloud OSS
- Region: `oss-cn-beijing`
- Client host: Alibaba Cloud ECS, Ubuntu 24.04, linux-x86_64, 2 vCPU, 3.4 GiB RAM
- Tool: `s3-turbo-list 0.2.19`
- Bucket shape: 1,000,044 objects, about 265 logical prefixes
- Object shape: mostly small load-test objects under 256 hex prefix shards

Compatibility:

- `compat-probe` reported all six core checks as compatible:
  HeadBucket, ListObjectsV2, prefix, delimiter, URL encoding, and pagination.
- OSS rejected `delimiter=` with HTTP 400 in a hints-style recursive listing
  scenario. The intended recursive listing behavior is to omit the delimiter
  request parameter when the CLI receives `--delimiter ''`.

Performance summary:

| Scenario | Segments | Concurrency | Threads | Output | Elapsed | Objects/sec | Timeouts |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| Single-chain hot summary | 1 | 100 | 10 | summary | 46.650s | 21,437 | 0 |
| Single-chain Parquet | 1 | 100 | 10 | Parquet | 47.825s | 20,910 | 0 |
| Hints Parquet | 266 | 8 | 4 | Parquet | 18.049s | 55,407 | 0 |
| Hints NDJSON | 266 | 16 | 6 | NDJSON | 19.144s | 52,239 | 1 |
| Hints summary | 266 | 20 | 10 | summary | 19.483s | 51,330 | 3 |
| Hints TSV | 266 | 32 | 10 | TSV | 24.383s | 41,014 | 19 |

Findings:

- Hints improved the hot full-bucket scan from 46.650s to 18.049s, about 2.6x.
- `--concurrency` only improved throughput when hints provided multiple
  key-space segments. A single ListObjectsV2 chain remained single-chain even
  with high concurrency settings.
- Moderate concurrency was best for this OSS test. `-c 8 -T 4` produced the
  fastest result with no stream timeouts; higher concurrency increased timeout
  counts and eventually reduced throughput.
- Parquet output was not the dominant bottleneck in this provider test:
  single-chain Parquet and single-chain summary had similar elapsed time.

Recommended conservative OSS starting point:

```bash
s3-turbo-list --delimiter '' auto-hints \
  --profile oss \
  --endpoint-url https://oss-cn-beijing.aliyuncs.com \
  --region oss-cn-beijing \
  --bucket my-bucket \
  -o hints.toml

s3-turbo-list --delimiter '' -c 8 -T 4 \
  --profile oss \
  --endpoint-url https://oss-cn-beijing.aliyuncs.com \
  --region oss-cn-beijing \
  --bucket my-bucket \
  --hints-file hints.toml
```

No project-owned cloud validation was run for this document; it records
third-party measurements and informs local mock coverage for provider-compatible
delimiter handling.
