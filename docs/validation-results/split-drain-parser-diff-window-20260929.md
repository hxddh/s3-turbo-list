# Split-child drain fix, parser speedups, and the diff lookahead window

**Date:** 2026-09-29
**Scope:** local measurements only (no real cloud endpoint was contacted).
**Baseline:** v0.36.0 (`cde565f`).

## 1. Runtime-split children lost at reactor exit (data loss)

**Defect.** A segment that accepts a runtime split shrinks its own end to
the cut `P` and sends the child range `(P, old_end]` to the reactor over
`split_rx`. The reactor's `select!` could pick the segment's `join_next`
first, and both reactor exits returned without reading what was still
queued: after an interrupt the checkpoint recorded neither the child range
nor its keys; on a normal completion the last segment could finish before
the child was received. In both cases the run, and a later `--resume`,
reported success with a gap in the listing.

**Harness.** A Python mock S3 server with a flat namespace (3,000 keys,
20 ms per page), the debug binary run with
`--no-auto-hints --max-keys 2 --resume --output-format tsv` (runtime
splitting on), `SIGINT` at a random 0.3–3.0 s. For each run the harness
checks that every key is in the output or in exactly one checkpoint
`remaining` range (no key missing, duplicated, or both listed and pending).

| Build | Interrupted runs | Runs with lost keys |
|---|---:|---:|
| v0.36.0 | 100 | 8 |
| this change | 100 | 0 |

Every v0.36.0 failure showed the same pattern: `Segment N accepted runtime
split at 'K'` after the quit, with no matching `new child segment` line,
and the checkpoint jumping over the child's range. No run in either build
produced duplicate rows or overlapping ranges.

**Fix.** Both reactor exits drain `split_rx` into the pending children
(which the resume ranges include), the completion check requires the
pending list to be empty, and segments stop accepting splits once the run
is quitting.

## 2. Throughput (list and diff)

**Setup.** A Rust mock serving pre-rendered ListObjectsV2 pages (1M objects,
the key shape of the v0.36 parser note), 21-segment hints file, 4 vCPU,
release builds. Median of 7 alternating runs; CPU is user+sys from
`getrusage`. Outputs compared: Parquet rows (sorted), `.ks` byte-identical,
TSV/NDJSON lines (sorted) — identical in every case.

Changes measured together: split probes parse with the fast Contents parser
(their leaf-level pages went through the SDK's per-object deserializer, ~16%
of list CPU); parser micro-optimizations (table-driven ETag hex, one
`memchr2` per value, no scratch copy when nothing is escaped, ASCII fast
paths — 816 → 388 ns per object in isolation); the diff lookahead window.

| Scenario | CPU (s) | Wall (s) | RSS |
|---|---|---|---|
| list → Parquet | 3.00 → 1.91 (−36%) | 0.98 → 0.72 (−26%) | 132 → 153 MB (noise range) |
| list → TSV | 2.06 → 1.50 (−27%) | 0.69 → 0.55 (−19%) | – |
| list → NDJSON | 2.08 → 1.60 (−23%) | 0.69 → 0.57 (−18%) | – |
| diff (identical sides) | 5.32 → 3.93 (−26%) | 1.84 → 1.36 (−26%) | 174 → 87 MB |

**Diff lookahead window.** With 4 batches per segment channel, a segment
behind the merge stalled after four pages, so a side with a few large
segments listed about one page per round trip; finished segments also kept
their batches queued, so buffered memory grew with the segment count.
The window is 32 batches per segment and a side starts at most 16 segments
ahead of the one the merge reads.

| Case | Before | After |
|---|---|---|
| 200k flat keys, 9 segments per side, 20 ms latency (wall) | 3.75 s | 0.67 s |
| 3M-object diff (peak RSS) | 390–410 MB | 100 MB |

## Not changed

- mimalloc as the global allocator was measured and rejected: TSV CPU −30%
  but Parquet CPU +31% and RSS 158 → 277 MB.
- Replacing the SDK call with a hand-built signed request was estimated at
  10–20% CPU and not attempted (endpoint, addressing, error and retry
  semantics would all have to be re-implemented).
