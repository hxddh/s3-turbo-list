# Flat-Namespace Cuts at the Middle of the Key Range

**Date:** 2026-09-29
**Scope:** local measurements only (no real cloud endpoint was contacted).
**Baseline:** v0.36.0 (`cde565f`). **Change:** new `src/flat_cut.rs`, used by
startup flat bisection (`src/auto_hints.rs`) and runtime flat splitting
(`src/tasks_s3.rs`).

## Problem

Flat cut candidates were built by truncating the range's start key at a few
depths (1/2, 3/4, 7/8, 1/4 of its tail) and bumping one character. For keys
with a long constant suffix after the varying part —
`obj-000000123.snappy.parquet`, `data/part-000000123-c000.snappy.parquet` —
every bump position fell inside the suffix, so the first key after each
candidate was the key right after the range start:

- startup bisection of a 200k-key bucket produced 63 boundaries at
  `obj-000000001` … `obj-000000063` and one segment with all other keys;
- runtime split proposals landed about one page ahead of the cursor and were
  always rejected by the segment ("0 runtime splits").

## Design

- The candidate comes from the first position where the range's low key
  (bisection start / runtime cursor) and its upper end differ, and is
  truncated right after it, so the suffix plays no part; a probe with
  `start-after=<candidate>` (`max-keys=1`) returns the next real key, which
  becomes the boundary.
- A maximal digit run through that position is read as a number (runs of
  unequal width are right-padded, which keeps lexicographic order).
  Other alphanumerics use a base-R midpoint over the alphabet the two keys
  use (digits, and `a`/`A` up to the largest letter present, so hex keys get
  `0-9a-f`), taking one more position when the leading characters are
  adjacent (`9c…`/`a0…` → `9e`). Anything else uses a code-point midpoint.
- Upper end: the next boundary for a bounded range. For an open-ended range
  (the bisection root, the last runtime segment) an estimated highest key is
  found first: a k-ary search over character positions for where the
  remaining keys stop sharing the low key's characters, then a climb to the
  largest character present at that position and the next two (digits in one
  round, non-ASCII with geometric steps). Every round issues up to 8 probes
  concurrently; bisection bounds all probes in flight to 64. The estimate is
  reused by later open-ended ranges (bisection) and by later split probes of
  the same run (runtime).
- If the midpoint probe finds no key inside a bounded range (keys cluster
  below the midpoint), the high-key estimate runs bounded by the range's end
  and the cut is retried at the midpoint below it.
- Invariants unchanged: boundaries are real observed keys, strictly increasing
  and strictly inside their range; boundary keys belong to the preceding
  segment; diff merge and hints semantics untouched. Nothing on the listing
  hot path changed.

## Methodology

- Host: 4 vCPU Linux container; both binaries `cargo build --release`.
- Endpoint: a local Rust ListObjectsV2 mock on `127.0.0.1` with 200,000 keys,
  1,000 per page, a fixed 20 ms delay per request (listing pages and probes
  alike), Contents with AWS-shaped fields. Diff uses the same key set on both
  sides. Default configuration (concurrency 100, so list and diff bisect to
  64 boundaries), `--output-parquet-file` and `--output-ks-file`; `list` runs start without a hints cache (startup
  discovery → bisection).
- Key shapes: `obj-%09d.snappy.parquet`; `data/part-%09d-c000.snappy.parquet`
  listed with `--prefix data/`; 16-hex-digit hashed keys `%016x.bin`; and the
  hierarchical `data/tenant=NNN/dt=2024-05-DD/part-…-c000.snappy.parquet`.
- Outputs compared row by row (sorted for list, ordered for diff) and the
  `.ks` files byte for byte: identical for every run below.
- Balance: segment sizes computed from the cached boundaries against the
  sorted key set (`max/mean` = largest segment over the mean).

## Results

Wall time in seconds (range over 2–3 runs); probes = `max-keys=1` requests.

| Shape | Mode | v0.36.0 | This change |
| --- | --- | --- | --- |
| `obj-…snappy.parquet` | list | 8.63–8.78 | 0.59–0.68 |
| | diff | 8.62–8.77 | 0.78–0.98 |
| | boundaries, max/mean | 63, 63.98 | 64, 1.34 |
| | list `--no-auto-hints` (runtime splitting only) | 4.48, 0 splits | 1.28, 15 splits |
| | list with 63 evenly spaced `--hints-file` boundaries (reference) | 0.27 | 0.25 |
| `data/part-…` `--prefix data/` | list | 8.58–8.72 (275 probes) | 0.62–0.64 (115 probes) |
| | diff | 8.61–8.63 (506 probes) | 0.84–0.89 (230 probes) |
| | boundaries, max/mean | 63, 63.98 | 64, 1.34 |
| hex `%016x.bin` | list | 9.97 (337 probes) | 0.63–0.67 (141–145 probes) |
| | diff | 9.93–9.99 (630 probes) | 1.26–1.27 (270 probes) |
| | boundaries, max/mean | 63, 63.98 | 64, 2.24 |
| hierarchical | list | 0.42–0.43 | 0.37–0.43 |
| | diff | 0.55–0.56 | 0.54–0.58 |
| | list `--no-auto-hints` | 2.47–2.58, 12 splits | 2.48–2.50, 12 splits |

Notes:

- The remaining gap to the evenly spaced hints reference (~0.35 s) is
  discovery latency: one structural probe, one first-key probe, the high-key
  estimate (~5 rounds) and one round per bisection level (~7).
- Hierarchical listings partition by `CommonPrefixes` and are unaffected; in
  runtime-split-only runs some leaf-directory splits now fall back to the
  flat search and issue more single-key probes (27–90 vs 6–20 per run), with
  no wall-time change.
- `data/part-…` listed *without* `--prefix` is not flat to startup
  discovery: it finds the single `CommonPrefix` `data/` and uses it as the
  only boundary, so bisection never runs (list relies on runtime splitting,
  1.4–1.5 s; diff stays one serial segment per side, 4.5 s). That is a
  structural-discovery limitation outside this change.
