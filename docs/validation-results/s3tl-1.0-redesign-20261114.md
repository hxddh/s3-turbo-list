# s3tl 1.0 redesign — local validation (2026-11-14)

Scope: the 1.0 redesign (new command line and result contract, dataset
output, v2 schemas, `encoding-type=url`, key-order guard, runtime splitting
for diff, bounded probes). Everything here ran locally; no real cloud
endpoint was contacted. Earlier notes in this directory describe 0.x and
remain as historical evidence.

## Correctness

- `cargo test`: 269 tests pass (lib 159, bin 11, CLI contract 11, data map
  15, S3 mock 66, trace 6, bench example 1). The mock suite covers
  pagination, partitioning, runtime splits, retries/throttling, interrupt
  and resume exactly-once, diff, `check`, `encoding-type=url` (a key with
  U+0001, `+`, `%`, a space and CJK), the order guard and
  `IsTruncated=false` with a token.
- Local mock (`scripts/mock-s3.py`, 20 ms per request):
  - `list` of 200,000 keys interrupted with SIGINT after 0.8 s: exit 130,
    53,000 rows committed, checkpoint with 1 range; `--resume` added 147,000
    rows; the dataset held exactly 200,000 rows.
  - `diff` of 200,000 keys against a copy with every 777th key removed and
    every 1000th ETag changed: `left_only 258, changed 200, equal 199,542`,
    identical with startup partitioning and with one static segment per side
    (14 runtime splits).
  - `diff s3://tree/d00/ s3://tree/d01/` (different prefixes, same relative
    names): all 2,500 keys matched relative to their prefixes.

## Performance (A/B against v0.39.0)

Same machine, same mock (20 ms per request, `NOENC=1` so the Python mock
does not spend CPU URL-encoding keys), median of 5 runs, wall time:

| Workload | v0.39.0 | s3tl 1.0 |
|---|---:|---:|
| list, 200k keys, 90% under one prefix | 1.45 s | 1.45 s |
| list, 50k keys in 20 directories | 0.91 s | 0.91 s |
| diff, 200k vs 200k, skewed | 2.31 s | 2.26 s |
| diff, same, one static segment per side (`--no-auto-hints`) | 4.80 s | 3.30 s |

The last row is the case runtime splitting for diff addresses: a side whose
startup partition is too coarse no longer serializes the diff behind its
largest segment. A first measurement capped partitioning probes at `-c`,
which made the 20-directory listing 25% slower (startup bisection wants
more than 100 concurrent single-key probes); probes are now capped at three
per `-c` slot (at most 512).

Output pipeline, `bench_local`, 1M objects (no S3): Parquet list 2.7M
objects/s, NDJSON 5.1M objects/s, diff merge with 1% changed 6.2M keys/s
(only differing keys are written).

## Not validated here

Real endpoints (AWS S3, MinIO, BOS, R2, B2, OSS) were not contacted. Before
release, rerun the endpoint validation flow in `endpoint-validation-plan-20260514.md`
with the new command line (`s3tl check`, then `list`/`diff` with `-o`).
