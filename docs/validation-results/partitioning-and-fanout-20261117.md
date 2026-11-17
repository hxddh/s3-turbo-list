# s3tl 1.2: partitioning and fan-out (2026-11-17)

Follow-up to an external review of 1.1.0 that reported two issues, both
confirmed in the code:

1. Startup discovery read one page per delimiter probe, so with more
   CommonPrefixes than a page holds (or smaller endpoint pages) everything
   after the first page was one segment (covered, never lost: the last
   segment is open-ended, but it was listed serially until runtime splitting
   caught up).
2. The fan-out governor lowered its cap after a window where added segments
   gained less than 5%, and only a 5% window-over-window rise reopened it.
   With few, steady segments left, a cap lowered by one noisy window stayed
   for the rest of the run (reproduced on the pure `observe_at` core:
   cap 64 → 2, then 2 for every following window).

Measuring them on the local mock found three more costs, fixed in the same
release:

3. Flat bisection produced `-c` boundaries, i.e. `-c + 1` segments for `-c`
   workers: the last segment started after the first wave (~3.4 s) and
   listed its whole range alone (`0 runtime splits` in the log).
4. Runtime splitting halved one segment per probe and a child waited five
   pages before it could be split, so fan-out doubled only every ~0.7 s.
5. Flat runs under a few prefixes got the flat budget minus the structural
   boundaries already found (11 cuts for 20 directories), and a diff side
   ran at most 16 segments (lookahead) and did not split while held.

## Changes

- Root probe follows up to 8 pages; a still-truncated probe leaves a tail
  run, bisected with the flat partitioner anchored on its last entry; the
  next level probes a spread of the prefixes.
- Flat bisection: `slots - 1` boundaries (list `-c`, diff per side
  `min(-c, 32)`).
- Flat-run refinement shares the structural target.
- k-way runtime splits (idle workers shared by the probed segments, at most
  15 cuts per probe); children are candidates after one page.
- Governor: after 3 windows saturated at a lowered cap with a flat rate, the
  cap grows by half; a try that does not help is taken back.
- Diff: lookahead 32 segments (the side's concurrency); a held side splits.

## Benchmark

`scripts/mock-s3.py` (with an extra `wideN` key layout: N top-level
directories), 1,000,000 keys per bucket, on 127.0.0.1; median of three runs;
seconds. No cloud endpoint was contacted.

| Scenario | 1.1.0 | 1.2.0 | Speed-up |
|---|---:|---:|---:|
| 100 ms, -c 32, list, 5000 top-level dirs | 11.60 | 5.35 | 2.17x |
| 100 ms, -c 32, list, 800 top-level dirs | 5.14 | 4.87 | 1.06x |
| 100 ms, -c 32, list, flat | 7.23 | 4.92 | 1.47x |
| 100 ms, -c 32, list, tree (20 dirs) | 8.20 | 7.02 | 1.17x |
| 100 ms, -c 32, list, skew (90% in one dir) | 10.64 | 9.07 | 1.17x |
| 100 ms, -c 32, diff skew vs skew-mod | 70.37 | 12.87 | 5.47x |
| 100 ms, -c 100, list, 5000 top-level dirs | 12.20 | 3.89 | 3.13x |
| 100 ms, -c 100, list, flat | 4.11 | 4.12 | 1.00x |
| 20 ms, -c 32, list, 5000 top-level dirs | 7.61 | 3.03 | 2.51x |
| 20 ms, -c 32, list, flat | 3.27 | 2.79 | 1.17x |
| 20 ms, -c 32, list, tree (20 dirs) | 4.22 | 4.21 | 1.00x |
| 20 ms, -c 32, list, skew (90% in one dir) | 5.34 | 5.00 | 1.07x |

The mock serves about 290 pages/s at 100 ms and `-c 32`, so ~3.5 s is the
floor for 1,000 pages. The remaining gap in the tree and skew rows is mostly
startup: the single-threaded Python mock serializes the 20 one-thousand-key
probe pages and the bisection probes (1.8 s at 20 ms, 3 s at 100 ms), which
a real store serves in parallel. Throughput during listing is at the mock's
ceiling in every row.

Tradeoffs:
- A truncated root listing costs up to 7 extra sequential requests at
  startup (0.7 s at 100 ms, ~0.2 s at typical AWS latency).
- Halving the diff segment channel to keep the buffer at its old size made
  diff 2.5x slower, so the buffer bound grew instead: 32 segments x 32 pages
  per side (from 16 x 32).
- The governor's tries add up to 50% more segments for a few seconds on a
  saturated bucket.

## Tests

New: root paging and tail runs, spread sampling, k-way split acceptance
(children contiguous and linked in key order), quantile cuts, child
eligibility, governor recovery and take-back, end-to-end list and diff over
2,500 top-level prefixes on an endpoint with 100-entry pages (every key
exactly once). `local_mock_list_flat_suffix_heavy_namespace_partitions_evenly`
now expects 7 boundaries for `-c 8` (one segment per worker).
