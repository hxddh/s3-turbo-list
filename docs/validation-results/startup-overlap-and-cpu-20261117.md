# s3tl 1.3: overlapping startup, scale cuts, allocator (2026-11-17)

All measurements on 127.0.0.1 against a Go ListObjectsV2 mock written for
profiling (pre-generated keys, 1,000-key pages, configurable per-request
latency), 4 vCPU shared by the mock and s3tl. No cloud endpoint was
contacted.

## Where 1.2.0 spent its time

At 30 ms per request and `-c 100`:

| Bucket | Run | Startup discovery | Listing |
|---|---:|---:|---:|
| 100k keys, flat | 0.40 s | 0.28 s (70%) | 0.12 s |
| 1M keys, 20 dirs | 1.10 s | 0.50 s (45%) | 0.60 s |
| 5M keys, flat | 2.73 s | 0.30 s (11%) | 2.43 s |

Client CPU: 0.85-0.94 ms per 1,000-object page (`gdb` stack sampling of
a symbolized build over a 20M-key listing): fast page parse 34% of active
samples, SDK orchestration ~27%, socket I/O 16%, the summary data map 17%,
SigV4 5%; malloc/free ~20% self time. SDK per-request options
(stalled-stream protection, checksum validation, S3 Express auth) made no
measurable difference.

## Changes

1. **Listing overlaps discovery.** The reactor takes discovery as a future;
   its boundaries become cuts of the running segments through the k-way
   split path. On its own this gained little: discovery's 6-17 sequential
   rounds left the first segment listing alone for most of them (10% of a
   100k-key bucket).
2. **Scale cuts.** A flat run's page (first key, last key, count) gives its
   numeric scale; one round of sizing probes bounds its end within 4x and one
   round of `4 x target` probes at evenly spread positions returns real-key
   cuts, thinned to the target. Bisection is the fallback (no numeric
   reading, or fewer than half the cuts). Discovery for a flat 100k bucket:
   0.28 s → ~0.13 s.
3. **Flat target up to 255** (`-c` − 1), now affordable.
4. **mimalloc.** Client CPU per page 0.94 → 0.67 ms (stats only), 1.09 →
   0.87 (Parquet), 1.13 → 0.89 (NDJSON).

## Benchmark (1.2.0 vs 1.3.0, seconds, median of five)

Rows without `-c` use 30 ms per request and the default `-c 100`; the last
row is CPU-bound (no latency, `-c 64`). `small` 100k flat, `midflat` 1M
flat, `mid` 1M under 20 dirs, `flat`/`tree`/`wide5000` 5M keys.

| Scenario | 1.2.0 | 1.3.0 | Speed-up |
|---|---:|---:|---:|
| list s3://small | 0.43 | 0.33 | 1.28x |
| list s3://midflat | 0.94 | 0.70 | 1.35x |
| list s3://mid | 1.11 | 0.90 | 1.23x |
| list s3://flat | 3.01 | 2.52 | 1.19x |
| list s3://tree | 3.12 | 2.65 | 1.18x |
| list s3://wide5000 | 2.74 | 2.58 | 1.06x |
| diff s3://mid s3://mid | 2.44 | 1.86 | 1.31x |
| diff s3://midflat s3://midflat | 1.53 | 1.51 | 1.02x |
| -c 64 list s3://flat | 1.94 | 1.42 | 1.37x |

Small runs (~0.3 s) vary by ±20% between repetitions.

## Not done

- Bypassing the SDK on the hot path: ~27% of client CPU is SDK
  orchestration, but replacing it means re-implementing endpoint
  resolution, credentials refresh and error classification for one
  request type. Not worth the risk to compatibility.
- The tail of a run (segments finishing unevenly) still costs ~0.3-0.5 s at
  30 ms on 5M keys.
- Real endpoints: gains there depend on their latency and request-rate
  ceiling; unmeasured.
