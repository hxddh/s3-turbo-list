# s3tl 1.6: diff pipeline, latency-raised concurrency, agent-facing failures (2026-11-19)

A review of 1.5.0 covered performance, correctness and agent use. It was
measured on a local Go ListObjectsV2 mock (127.0.0.1, 1,000-key pages, 4
vCPU shared with s3tl) and by exercising the CLI against local endpoints.
No cloud endpoint was contacted.

## Findings

| Finding | Evidence (1.5.0) |
|---|---|
| A large diff listed one page per round trip | 5M keys per side: 56 s at 30 ms (list: 2.2 s), 427 s at 200 ms, average concurrency 5-6. Segments with full 32-page buffers kept their slots and their unlisted ranges; once the merge reached one, it listed the rest serially. Splitting it needed idle slots (held) and probe budget (spent in the first seconds on segments about to wait). |
| `-c 100` halved throughput on a high-latency endpoint | 5M keys at 200 ms: `-c 100` 11.6 s, `-c 256` 5.75 s, `-c 1024` 5.0 s; at 30 ms the same change bought nothing |
| A wrong endpoint took 3 minutes to fail, as `net.error` (retryable) with a Debug dump | connection refused: 180 s; `check` diagnosed it in 11 ms |
| A failed run discarded hours of listing | only an interrupt saved a checkpoint |
| Resume and check were not idempotent or decisive | `--resume` of a finished dataset: `usage.no_checkpoint`; `check` passed without credentials and when HeadBucket answered but every listing failed |
| Two narrow races (review, not reproduced) | a probe answering while the run quit could leave a child out of the checkpoint; one answering after its segment finished linked a child after a sealed diff segment |

Client CPU was not a bottleneck: about 0.64 µs per object (1.6M objects/s a
core); 5M keys to Parquet peaked at 278 MB.

## Changes

- Diff: a segment about to wait (buffer full) hands its remaining range to a
  child and ends; waiting segments do not count against concurrency; a side
  starts segments within 128 of the merge (8 pages each: 1,024 pages, as
  before), cuts the lowest in key order, checks every 50 ms, runs up to 64.
- Runtime cuts of a flat run are read off the segment's latest page when its
  range ends inside the run (no probe). "Too short to cut now" no longer
  backs off: a waiting segment lists no pages, and the back-off left it
  uncut until the merge reached it.
- Without `-c`: the window starts at 100 and its cap rises to cores x 1,000
  pages/s x the median latency of the first three quiet pages, up to 1,024.
- `endpoint.unreachable`; root-cause messages and hints; progress kept on
  exit 3/4; idempotent resume; normalized checkpoint endpoints; `check`
  decisive; `--trace` validated early.
- Probes stop before the quit path reads the last children; a finished
  segment accepts no cut; the one-page rule uses the run's page size.

Tried and dropped while fixing the diff, each measured on the 5M diff at 30 ms:
counting only blocked segments out (56 s, no change); lowest-first cuts (16 s);
cuts read off pages (13.8 s); 64-segment reach (deadlocked until the reach
was counted in key order from the merge, then 7.6-8.3 s with 128 x 8);
splitting waiting segments instead of handing off (8.5 s, but 1M diffs went
from 1.5 s to 4.2 s and 200 ms diffs from 7.5 s to 39 s: every segment kept
a serial remainder); a per-side cap of 32 (limited diff 1.7 s vs 2.0 s, but
5M at 30 ms 10.8 s vs 6.8 s).

## Benchmark (1.5.0 vs 1.6.0)

Median of five runs (200 ms 5M list, 200 ms 1M diff and 30 ms 5M diff:
three; slow tail: nine; 200 ms 5M diff: one), seconds; requests are those
the mock received, probes and rejected ones included.

| Scenario | 1.5.0 s | 1.6.0 s | Speed-up | 1.5.0 requests | 1.6.0 requests |
|---|---:|---:|---:|---:|---:|
| limited 1000 req/s, 1M flat | 1.51 | 1.50 | 1.00x | 1438 | 1498 |
| limited, 1M 20 dirs | 2.19 | 2.09 | 1.05x | 2455 | 2425 |
| limited, 1M flat, -c 256 | 1.50 | 1.47 | 1.02x | 1658 | 1626 |
| limited, 1M flat, -c 32 | 1.25 | 1.23 | 1.01x | 1103 | 1091 |
| saturated (32 in service), 1M flat | 1.39 | 1.42 | 0.98x | 1291 | 1312 |
| saturated, 1M flat, -c 256 | 1.86 | 1.49 | 1.25x | 1853 | 1415 |
| saturated, 1M 20 dirs | 1.70 | 1.65 | 1.03x | 1613 | 1576 |
| jitter 30-200 ms, 1M flat | 2.20 | 1.59 | 1.38x | 1348 | 1398 |
| jitter, 1M 20 dirs | 2.69 | 1.94 | 1.38x | 1670 | 1778 |
| 30 ms, 100k flat | 0.32 | 0.31 | 1.01x | 280 | 262 |
| 30 ms, 1M flat | 0.61 | 0.67 | 0.91x | 1286 | 1304 |
| 30 ms, 1M flat, -c 256 | 0.57 | 0.58 | 0.98x | 1392 | 1269 |
| 30 ms, 1M 20 dirs | 0.75 | 0.72 | 1.04x | 1617 | 1543 |
| 30 ms, 1M 20 dirs, -c 16 | 2.62 | 2.54 | 1.04x | 1315 | 1306 |
| 30 ms, 5M flat | 2.18 | 2.10 | 1.04x | 5477 | 5516 |
| 30 ms, 5M 20 dirs | 2.43 | 2.10 | 1.16x | 5811 | 5615 |
| 30 ms, 5M 5000 dirs | 2.55 | 2.36 | 1.08x | 5340 | 5873 |
| 200 ms, 1M flat | 3.02 | 2.43 | 1.24x | 1312 | 1483 |
| 200 ms, 5M flat | 11.34 | 3.33 | 3.41x | 5493 | 6105 |
| 30 ms, diff 1M 20 dirs | 1.49 | 1.22 | 1.22x | 2843 | 2876 |
| diff, right side limited | 1.52 | 2.00 | 0.76x | 1496 | 1450 |
| 200 ms, diff 1M flat | 7.48 | 4.34 | 1.72x | 2251 | 2386 |
| 30 ms, diff 5M flat | 57.36 | 6.41 | 8.95x | 11149 | 11014 |
| 200 ms, diff 5M flat | 391.03 | 35.31 | 11.08x | 11075 | 11062 |
| 0 ms, 5M flat, -c 64 | 1.27 | 1.30 | 0.98x | 5340 | 5319 |
| slow tail 1%x6s, 1M flat | 7.55 | 7.54 | 1.00x | 1333 | 1322 |
| slow tail, 1M 20 dirs | 8.31 | 9.25 | 0.90x | 1818 | 1772 |
| slow tail, 1M flat, -c 32 | 8.29 | 8.50 | 0.97x | 1204 | 1122 |

`limited`: 1,000 requests/s, then 503 SlowDown; `saturated`: the mock serves
32 requests at a time and queues the rest; `jitter`: 30 ms plus 0-170 ms;
`slow tail`: 1% of requests take 6 s more; otherwise the stated latency.

Rows below 1.00x, checked:

- **30 ms, 1M flat:** re-measured at five runs each, 0.64 s (1.5.0), 0.66 s
  (1.6.0) and 0.63 s (1.6.0 with `-c 100`). At 30 ms the automatic cap is
  about 130, so the difference is mostly noise.
- **Slow tail, 1M 20 dirs:** over thirteen runs each the means are 9.32 s
  and 9.20 s. The distribution has three modes (about 7.5, 9.3 and 12.9 s,
  depending on how many 6 s pages land late), so medians move between modes.
- **Diff with a rate-limited right side:** that side now lists at the pace
  of a listing of it (`limited, 1M 20 dirs`: 2.09 s). In 1.5.0, the per-side
  cap of 32 happened to sit just under the 1,000 req/s limit. A cap of 32
  costs unlimited diffs 40-60%.

## Not covered

- Real endpoints. The automatic cap assumes the endpoint serves the extra
  concurrency or throttles (which the window answers); an endpoint that
  queues without throttling is served at the same throughput, with more
  requests waiting.
- Startup probes on a rate-limited tree still rival its listing pages
  (about 1,000-1,400 probes for 1,000 pages in `limited, 1M 20 dirs`); left
  for a later release.
