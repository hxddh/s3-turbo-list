# s3tl 1.5: per-side windows, streamed discovery, scoped runtime cuts (2026-11-18)

A review of 1.4.0 raised four points about how the run sizes its work. Each
was checked against the code and measured on a local Go ListObjectsV2 mock
(127.0.0.1, 1,000-key pages, 4 vCPU shared with s3tl). No cloud endpoint was
contacted.

| Point | Verdict | Evidence (1.4.0) |
|---|---|---|
| The window reacts only to throttles and timeouts, not latency | true in code; its cost was misattributed | an endpoint serving 32 requests at a time (excess queued, never throttled): `-c 32` 1.32 s, `-c 256` 2.26 s. The extra time matched the extra *probes* (1,181 vs 146), not the queue: queueing does not lower throughput |
| The default `-c` ignores what the endpoint takes | true | 1,000 requests/s: `-c 32` 1.28 s, default 2.14 s (291 throttled, 674 probes, average concurrency 22) |
| Segments get finer as `-c` grows | partly | listing pages stayed within 1-10% of the minimum; probes were the excess (1,059 probes for 1,049 pages at `-c 256`). No 1-page segments, but 3-page ones at `-c 256` |
| Startup probes grow with `-c` and compete with listing | true | 4 x (`-c` − 1) scale candidates (396 at `-c 100`) overflowed the window's 300 probe units into a second wave; for 30-40% of a 1M run the listing was nearly idle |

Further findings from traces:

- Throttled probes waited 500 ms before their retry, so a throttled
  discovery round stalled the whole startup (0.4 s idle at 1,000 req/s).
- In a tree, runtime split probes cut a directory's run across the listing
  prefix: candidates fell past the directory, and few cuts landed in range
  (`-c 16`, 20 directories: 2.9 s, 505 probes).
- Startup boundaries arrived only after the flat runs under the root's
  CommonPrefixes were sized and cut (about 6 round trips), although the
  CommonPrefixes were known after one.
- A diff's two sides shared one window: one side's throttles slowed both.

## Changes

- One window per side (a diff has two), each capped at `-c`.
- Discovery hands its boundaries to the listing after each round.
- Two scale candidates per cut (one round); throttled probe retry 100 ms.
- Runtime cuts of a flat run along its number, scoped to its directory, at
  least two pages per child; no cut when one page holds the rest. Every
  flat run gets a startup cut; a diff side starts waiting children
  lowest-key first.

## Tried and dropped

- **Latency as a queue signal.** The window held growth when pages took
  twice their unqueued latency and shrank (by at most 30% a round trip)
  towards twice the concurrency served (throughput x unqueued latency) at
  three times; runtime splitting sized to the same. It helped only the
  queueing mock (`-c 256`: 1.87 s → 1.51 s). Elsewhere: the first answers of
  a burst are the fastest, so the unqueued latency came out low (44 ms
  against a 110 ms median under 30-200 ms jitter: 2.39 s → 3.75 s); client
  CPU raised latency at 0 ms; at the end of a diff, throughput fell with
  the segments left and the "served" concurrency with it, halting splits
  (1.6 s → 3.3 s). Sampling only quiet pages fixed the first, not the
  others. Removed.
- **Capping startup partitioning at 64 segments** until capacity is
  measured: helped the queueing mock, cost 25% on a 1M flat listing at
  `-c 100` (0.60 → 0.75 s).
- **One startup segment per slot for flat runs under structure** (was two):
  a diff, at most 32 segments a side, took twice as long on its larger,
  later segments (1.6 → 3.3 s).
- **50 ms runtime split checks** (200 ms): probed segments about to finish,
  many more probes (5000 dirs: 7 → 671), no gain.
- **A runtime probe base of one per window slot** (two per `-c`): ran out
  late in low-`-c` runs; with 1% of pages at 6 s, `-c 32` took 12.4 s
  (median of nine) instead of 9.1 s.
- **Three or four candidates per cut**: 5M flat 2.24-2.31 s (vs 2.40 s
  with two), but rate-limited 1.63-1.69 s (vs 1.47 s) and queueing `-c 256`
  2.04-2.07 s (vs 1.86 s). Spreading two candidates over the estimated
  rather than the largest run size halved 1M flat speed.

## Benchmark (1.4.0 vs 1.5.0)

Median of five runs (slow tail: nine), seconds; requests are those the mock
received, probes and rejected ones included.

| Scenario | 1.4.0 s | 1.5.0 s | Speed-up | 1.4.0 requests | 1.5.0 requests |
|---|---:|---:|---:|---:|---:|
| limited 1000 req/s, 1M flat, -c 100 | 2.08 | 1.51 | 1.37x | 1820 | 1458 |
| limited, 1M 20 dirs, -c 100 | 2.41 | 1.79 | 1.34x | 2165 | 2221 |
| limited, 1M flat, -c 256 | 2.10 | 1.48 | 1.42x | 2085 | 1655 |
| limited, 1M flat, -c 32 | 1.27 | 1.25 | 1.02x | 1170 | 1116 |
| saturated (32 in service), 1M flat, -c 100 | 1.61 | 1.39 | 1.16x | 1539 | 1291 |
| saturated, 1M flat, -c 256 | 2.24 | 1.91 | 1.17x | 2243 | 1861 |
| saturated, 1M 20 dirs, -c 100 | 2.03 | 1.71 | 1.18x | 2007 | 1641 |
| jitter 30-200 ms, 1M flat | 2.32 | 2.20 | 1.05x | 1659 | 1349 |
| jitter, 1M 20 dirs | 2.61 | 2.58 | 1.01x | 2124 | 1675 |
| 30 ms, 100k flat | 0.29 | 0.27 | 1.10x | 340 | 198 |
| 30 ms, 1M flat | 0.66 | 0.61 | 1.08x | 1496 | 1296 |
| 30 ms, 1M flat, -c 256 | 0.65 | 0.53 | 1.23x | 1925 | 1391 |
| 30 ms, 1M 20 dirs | 0.77 | 0.77 | 1.00x | 1925 | 1620 |
| 30 ms, 1M 20 dirs, -c 16 | 2.89 | 2.62 | 1.10x | 1663 | 1318 |
| 30 ms, 5M flat | 2.28 | 2.42 | 0.94x | 5775 | 5637 |
| 30 ms, 5M 20 dirs | 2.51 | 2.33 | 1.08x | 6234 | 5760 |
| 30 ms, 5M 5000 dirs | 2.50 | 2.54 | 0.99x | 5555 | 5297 |
| 30 ms, diff 1M 20 dirs | 1.59 | 1.48 | 1.08x | 3740 | 2861 |
| diff, right side limited 1000 req/s | 2.56 | 1.46 | 1.75x | 1841 | 1440 |
| 0 ms, 5M flat, -c 64 | 1.26 | 1.26 | 1.00x | 5404 | 5296 |
| slow tail 1%x6s, 1M flat, -c 100 | 8.11 | 7.52 | 1.08x | 2024 | 1348 |
| slow tail, 1M 20 dirs, -c 100 | 9.14 | 8.32 | 1.10x | 2349 | 1784 |
| slow tail, 1M flat, -c 32 | 9.50 | 8.40 | 1.13x | 1431 | 1197 |

`limited`: 1,000 requests/s, then 503 SlowDown; `saturated`: the mock serves
32 requests at a time and queues the rest; `jitter`: 30 ms plus 0-170 ms;
`slow tail`: 1% of requests take 6 s more; otherwise 30 ms per request.
The diff with a limited right side lists the same keys from two mocks.

5M flat is the one slower scenario, 0.93-0.97x over repeated runs: it is
client-CPU-bound here, and two candidates per cut found 60 of 99 startup
cuts, so runtime splitting filled the rest later. Slow-tail medians vary by
±1.5 s between runs (a chain that meets two 6 s pages in a row at the end).

## Not covered

- Real endpoints (not contacted). An endpoint that queues without throttling
  is listed at `-c`; it serves the same throughput, and partitioning no
  longer multiplies probes for it.
