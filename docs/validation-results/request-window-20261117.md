# s3tl 1.4: one adaptive request window (2026-11-17)

An external review of 1.3.1 raised five points; each was checked against the
code and measured on a local Go ListObjectsV2 mock (127.0.0.1, 1,000-key
pages, 4 vCPU shared with s3tl). No cloud endpoint was contacted.

| Point | Verdict | Evidence (1.3.1) |
|---|---|---|
| Concurrency adapts to page throughput only; no back-off on throttles | true | 1,000 req/s limit, 1M keys: `-c 32` 1.32 s, default `-c 100` 7.33 s (448 503s), `-c 256` 10.14 s |
| Fixed, short page timeout; retries do not lower concurrency | true, and worse in discovery | 1% of pages at 6 s: 107 s at `-c 100` (a slow probe held a whole discovery round, which also gated runtime splits) |
| Too many tiny segments | partly | tail pages added 0-16% list requests; probes were the amplifier: 2,361 probes for 1,000 pages at `-c 256` on 20 dirs, 485 of 505 segments of 1-2 pages |
| Weak observability | partly | probes untraced, no request/segment/concurrency counts in the result (per-segment pages were in the trace) |
| Parquet writer not parallel | no | writers scale with CPU (2 at 0 ms); at 30 ms Parquet = stats-only time; +18% only when CPU-bound |

## Design

Four controls that each saw one signal (governor: page throughput;
per-segment backoff: throttles; probe semaphore: nothing; fixed timeout)
became one window, capped at `-c`, shared by pages and probes:
multiplicative decrease (x0.7, once per round trip) on a throttle or a
timeout, additive increase (+1 per window of answers). A page takes a slot,
a probe a third. Runtime splits fill the window. The fan-out governor and the
probe semaphore are deleted; no option was added.

Around it: adaptive page timeout (10x typical latency, 10-60 s); short
throttle retries (100 ms doubling; the window already slowed the run);
probes give up after 4x typical latency and a missing answer no longer voids
a round; runtime splitting waits for discovery at most 2 s; cuts take effect
immediately (the page in flight is trimmed at the new end); at least 4 pages
per startup segment of a sized flat run; runtime probes within ~1 per 8
pages.

Tried and dropped: slow start (window from 32, doubling). It did not help
the rate-limited case (2.07 s vs 2.00 s opening at `-c`) and cost small
listings a few round trips. Probes taking whole slots queued startup
partitioning behind the listing (1M under 20 dirs: 0.94 s vs 0.87 s).

## Benchmark (1.3.1 vs 1.4.0, median of three)

| Scenario | 1.3.1 s | 1.4.0 s | Speed-up | 1.3.1 requests | 1.4.0 requests |
|---|---:|---:|---:|---:|---:|
| limited 1000 req/s, 1M flat, -c 100 | 7.57 | 2.12 | 3.58x | 2273 | 1827 |
| limited, 1M 20 dirs, -c 100 | 6.60 | 2.17 | 3.05x | 3275 | 2109 |
| limited, 1M flat, -c 256 | 10.63 | 2.11 | 5.04x | 3336 | 2051 |
| limited, 1M flat, -c 32 | 1.31 | 1.32 | 1.00x | 1156 | 1158 |
| slow tail 1%x6s, 1M flat, -c 100 | 22.64 | 8.31 | 2.72x | 1873 | 1973 |
| slow tail, 1M 20 dirs, -c 100 | 23.16 | 8.94 | 2.59x | 2722 | 2358 |
| slow tail, 1M flat, -c 32 | 20.20 | 8.61 | 2.35x | 1305 | 1441 |
| 30 ms, 100k flat | 0.34 | 0.32 | 1.08x | 556 | 340 |
| 30 ms, 1M flat | 0.74 | 0.69 | 1.07x | 1535 | 1528 |
| 30 ms, 1M 20 dirs | 0.87 | 0.80 | 1.09x | 2114 | 1940 |
| 30 ms, 5M flat | 2.49 | 2.46 | 1.01x | 5769 | 5777 |
| 30 ms, 5M 20 dirs | 2.81 | 2.73 | 1.03x | 6151 | 6224 |
| 30 ms, 5M 5000 dirs | 2.61 | 2.59 | 1.01x | 5370 | 5351 |
| 30 ms, diff 1M 20 dirs | 1.89 | 1.72 | 1.09x | 4115 | 3761 |
| 0 ms, 5M flat, -c 64 | 1.77 | 1.61 | 1.10x | 5441 | 5416 |

`limited`: 1,000 requests/s, then 503 SlowDown; `slow tail`: 1% of requests
take 6 s more; otherwise 30 ms per request; requests include probes and
rejected ones. No scenario is slower.

## Not covered

- Real endpoints. The window reacts to 503/429/SlowDown and timeouts; an
  endpoint that slows down without either is not detected (no
  latency-gradient signal, by choice: it misreads normal latency variance).
- One window per process: a cross-endpoint `diff` slows both sides when one
  throttles.
