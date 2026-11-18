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
- Two scale candidates per cut, and a second round over the extent the
  first one found when it falls short of the share; throttled probe retry
  100 ms.
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
  with two in one round), but rate-limited 1.63-1.69 s (vs 1.47 s) and
  queueing `-c 256` 2.04-2.07 s (vs 1.86 s). Spreading two candidates over
  the estimated rather than the largest run size halved 1M flat speed.
  Kept: two candidates, and a second round when the first falls short.
- **Handing a short first scale round to the listing early**, before the
  second: 5M flat 2.50 s vs 2.12 s (nine runs each); no gain at 100k.

## Benchmark (1.4.0 vs 1.5.0)

Median of five runs (100k and 5M flat, 0 ms and slow tail: nine; slow tail
1M flat at `-c 100`: fifteen), seconds; requests are those the mock received,
probes and rejected ones included.

| Scenario | 1.4.0 s | 1.5.0 s | Speed-up | 1.4.0 requests | 1.5.0 requests |
|---|---:|---:|---:|---:|---:|
| limited 1000 req/s, 1M flat, -c 100 | 2.10 | 1.51 | 1.40x | 1838 | 1447 |
| limited, 1M 20 dirs, -c 100 | 2.57 | 2.03 | 1.27x | 2128 | 2336 |
| limited, 1M flat, -c 256 | 2.00 | 1.44 | 1.39x | 2039 | 1668 |
| limited, 1M flat, -c 32 | 1.28 | 1.23 | 1.04x | 1176 | 1117 |
| saturated (32 in service), 1M flat, -c 100 | 1.58 | 1.38 | 1.15x | 1503 | 1291 |
| saturated, 1M flat, -c 256 | 2.24 | 1.86 | 1.21x | 2245 | 1856 |
| saturated, 1M 20 dirs, -c 100 | 2.03 | 1.71 | 1.19x | 2007 | 1644 |
| jitter 30-200 ms, 1M flat | 2.32 | 2.27 | 1.02x | 1647 | 1348 |
| jitter, 1M 20 dirs | 2.67 | 2.60 | 1.02x | 2123 | 1658 |
| 30 ms, 100k flat | 0.31 | 0.31 | 1.00x | 340 | 293 |
| 30 ms, 1M flat | 0.64 | 0.62 | 1.03x | 1540 | 1297 |
| 30 ms, 1M flat, -c 256 | 0.65 | 0.55 | 1.17x | 2027 | 1391 |
| 30 ms, 1M 20 dirs | 0.77 | 0.75 | 1.03x | 1942 | 1608 |
| 30 ms, 1M 20 dirs, -c 16 | 2.89 | 2.60 | 1.12x | 1619 | 1311 |
| 30 ms, 5M flat | 2.30 | 2.15 | 1.07x | 5737 | 5477 |
| 30 ms, 5M 20 dirs | 2.55 | 2.36 | 1.08x | 6164 | 5789 |
| 30 ms, 5M 5000 dirs | 2.55 | 2.56 | 0.99x | 5416 | 5439 |
| 30 ms, diff 1M 20 dirs | 1.64 | 1.48 | 1.10x | 3699 | 2817 |
| diff, right side limited 1000 req/s | 2.60 | 1.55 | 1.68x | 1841 | 1499 |
| 0 ms, 5M flat, -c 64 | 1.31 | 1.32 | 0.99x | 5404 | 5334 |
| slow tail 1%x6s, 1M flat, -c 100 | 8.11 | 7.56 | 1.07x | 2048 | 1338 |
| slow tail, 1M 20 dirs, -c 100 | 10.79 | 8.30 | 1.30x | 2336 | 1738 |
| slow tail, 1M flat, -c 32 | 8.90 | 8.16 | 1.09x | 1461 | 1189 |

`limited`: 1,000 requests/s, then 503 SlowDown; `saturated`: the mock serves
32 requests at a time and queues the rest; `jitter`: 30 ms plus 0-170 ms;
`slow tail`: 1% of requests take 6 s more; otherwise 30 ms per request.
The diff with a limited right side lists the same keys from two mocks.

The three scenarios within ±1% are parity (5000 dirs: 2.548 vs 2.562 s over
nine runs). Slow-tail runs are bimodal: one or two in fifteen meet two 6 s
pages in a row at the end and take ~13 s, in every version, so a median of
five or nine can land on either mode; the 1M flat `-c 100` row is the
median of fifteen (a nine-run median showed 12.5 s).

In the first full run, scale cuts took two candidates per cut in one round
and 5M flat was 5% slower (two candidates found 60 of 99 startup cuts).
The second round over the extent the first one found makes up the share
(and restores the flat-run test's exact budget).

## Not covered

- Real endpoints (not contacted). An endpoint that queues without throttling
  is listed at `-c`; it serves the same throughput, and partitioning no
  longer multiplies probes for it.
