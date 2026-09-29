# Startup Partitioning Under Sparse Structure, Multi-Way Cut Waves, Single-Pass Page Scan

**Date:** 2026-09-29
**Scope:** local measurements only (no real cloud endpoint was contacted).
**Baseline:** v0.37.0 (`408abd8`). **Change:** `src/auto_hints.rs`,
`src/flat_cut.rs`, `src/app/run.rs` (startup / diff-side partitioning) and
`src/list_page.rs` (page pre-check).

## Problems

1. **One prefix, many keys.** A bucket laid out as
   `data/part-%09d-c000.snappy.parquet` listed *without* `--prefix data/` is
   not flat to startup discovery: the root probe returns the single
   `CommonPrefix` `data/`, which became the only boundary, so bisection never
   ran. List relied on runtime splitting (1.36 s at 20 ms/request, 200k keys);
   each diff side listed as one serial segment (4.47 s). The same keys with
   `--prefix data/` took 0.58 s / 0.83 s. (Recorded as a limitation in
   `flat-cut-midpoint-bisection-20260929.md`.)
2. **One round-trip per bisection level.** Flat bisection cut every range
   once per wave, so 64 boundaries took ~7 waves after the high-key
   estimate — most of the ~0.33 s startup of a flat 200k-key listing at
   20 ms/request.
3. **Three passes over every page before parsing.** The fast Contents parser
   ran a UTF-8 validation, a forbidden-control scan and a `]]>` search over
   each ~340 KB page.

## Design

- **Flat leaves.** Structural discovery now also reports each probed prefix
  below the root whose page was truncated and held no `CommonPrefixes` (a
  flat directory with more than one page of keys). When discovery found
  fewer boundaries than the flat-bisection target (list: one per worker,
  up to 64; diff: at least 8, up to 64), those leaves are bisected
  concurrently with the existing flat partitioner, sharing the remaining
  budget, and their boundaries — real keys inside each leaf — are merged
  with the structural set. This is one helper used by both the list startup
  path and the diff per-side resolver; structured buckets that already have
  enough boundaries, and flat buckets, take the same path as before.
- **Multi-way waves.** Each wave now cuts every open range at up to seven
  candidates at once (one `max-keys=1` probe each, all ranges concurrently,
  still at most 64 probes in flight). Candidates sit at `j/(k+1)` of the
  range, read the same way as the midpoint (a digit run as a number, other
  alphanumerics over the keys' alphabet, with enough positions to resolve
  `k+1` steps); when that reading does not apply, they are picked evenly from
  an in-order walk of the midpoint tree. The remaining budget is spread over
  the ranges; once it fits with at most one extra cut for some ranges it is
  handed out exactly, so the final wave reaches the target. A range whose
  probes find fewer distinct keys than asked (adjacent keys, a failed probe)
  leaves the rest to one more wave: **the boundary count is exactly the
  target** whenever the keys allow it (an earlier prototype stopped one or
  two boundaries short to save that round-trip; that was rejected, and the
  exact-count assertions in `test_flat_boundaries_balance_suffix_heavy_keys`
  and `local_mock_list_flat_suffix_heavy_namespace_partitions_evenly` are
  unchanged). A 64-boundary partition now takes two waves (7, then 57).
- **Single-pass page pre-check.** One branch-free pass per 64-byte chunk
  (so it vectorizes) finds forbidden C0 controls, whether any byte is
  non-ASCII and whether any `]` occurs. UTF-8 validation and the
  U+FFFE/U+FFFF search then run only on pages with non-ASCII bytes, and the
  `]]>` search only on pages containing `]`.
- Invariants unchanged: boundaries are real observed keys, strictly
  increasing and strictly inside their range; boundary keys belong to the
  preceding segment; diff merge, checkpoint and hints semantics untouched.
  The parser accepts and rejects exactly the pages it did before.

## Methodology

- Host: 4 vCPU Linux container; both binaries `cargo build --release`
  (v0.37.0 from `git archive 408abd8`, and this change).
- Endpoint: a local Rust ListObjectsV2 mock on `127.0.0.1`, 1,000 keys per
  page, Contents with AWS-shaped fields (Key, LastModified, ETag,
  ChecksumAlgorithm, ChecksumType, Size, StorageClass). Diff lists the same
  key set on both sides.
- Latency runs: 200,000 keys, a fixed 20 ms delay per request (pages and
  probes alike), default configuration, `--output-parquet-file` and
  `--output-ks-file`, 7 runs per cell alternating the two binaries; medians
  reported.
- CPU runs: 1,000,000 `obj-…` keys, no delay, 7–9 alternating runs.
- Key shapes: `obj-%09d.snappy.parquet`; `data/part-%09d-c000.snappy.parquet`
  with and without `--prefix data/`; 16-hex-digit hashed keys `%016x.bin`;
  hierarchical `data/tenant=NNN/dt=2024-05-DD/part-…-c000.snappy.parquet`.
- Startup = time from the run's first request to its first listing-page
  request (from the mock's request log). Requests = all requests of the run.
- Outputs compared for every run: Parquet rows (sorted for list, in order for
  diff) and the `.ks` files byte for byte — identical in every cell below.
- Balance: segment sizes computed from the run's startup boundaries (debug
  log) against the sorted key set; `max/mean` = largest segment over the mean.

## Results

200k keys, 20 ms per request, median of 7 alternating runs.

| Shape | Mode | Wall v0.37.0 → this (s) | Startup (ms) | Requests | Boundaries, max/mean (list) |
| --- | --- | --- | --- | --- | --- |
| `obj-…` | list | 0.545 → **0.455** | 331 → 240 | 372 → 367 | 64, 1.34 → 64, 1.34 |
| | diff | 0.823 → **0.752** | 347 → 260 | 744 → 734 | |
| `data/part-…` (no `--prefix`) | list | 1.359 → **0.486** | 50 → 266 | 325 → 374 | 1, 2.00 → 64, 1.34 |
| | diff | 4.469 → **0.740** | 57 → 281 | 406 → 748 | |
| `data/part-…` `--prefix data/` | list | 0.578 → **0.454** | 344 → 244 | 373 → 368 | 64, 1.34 → 64, 1.34 |
| | diff | 0.830 → **0.772** | 351 → 265 | 746 → 736 | |
| hex `%016x.bin` | list | 0.662 → **0.554** | 382 → 291 | 379 → 386 | 64, 2.24 → 64, 1.08 |
| | diff | 1.097 → **0.801** | 393 → 303 | 740 → 772 | |
| hierarchical | list | 0.371 → 0.364 | 91 → 85 | 311 → 311 | 105, 1.06 → 105, 1.06 |
| | diff | 0.609 → 0.612 | 69 → 69 | 622 → 622 | |

Notes:

- `data/part-…` without `--prefix` now partitions like the `--prefix data/`
  run (64 boundaries, same balance); its startup is longer than v0.37.0's
  only because v0.37.0 skipped partitioning entirely. The extra requests in
  diff are the 65 segments' final partial pages.
- Hex keys balance better (max/mean 2.24 → 1.08): the `j/(k+1)` candidates
  are resolved over enough hex positions for their step, so the cuts are
  more evenly spaced than repeated midpoints.
- Hierarchical listings have enough `CommonPrefixes` boundaries and take the
  unchanged path.

1M `obj-…` keys, no delay (CPU = user + sys seconds, medians):

| Mode | CPU v0.37.0 → this | Wall v0.37.0 → this (s) |
| --- | --- | --- |
| list (Parquet) | 1.75 → 1.77; repeat (n=9) 1.77 → 1.78 | 1.01 → 1.13; repeat 0.80 → 0.88 |
| diff (Parquet) | 3.07 → 2.89 | 1.21 → 1.16 |
| list (TSV) | 1.57 → 1.57 | 0.62 → 0.65 |

Without latency, end-to-end CPU and wall are within run-to-run noise on this
host (individual list walls ranged 0.71–1.23 s for both binaries). The
startup at zero latency is ~10–20 ms longer (57 concurrent probes in one
wave on a 4-vCPU host shared with the mock, instead of smaller serial
waves); at any real round-trip time the fewer waves win, as the first table
shows.

Page pre-check in isolation (standalone release micro-benchmark with the
same `memchr` crate, one 340 KB ASCII page of 1,000 AWS-shaped Contents):
UTF-8 check + control scan + `]]>` search 56.5 µs/page → single pass
41.5–43.7 µs/page (−25%). That is ~15 ms per million objects, about 1% of
list CPU — real but below the end-to-end noise floor here.

## What did not help

(From the evaluation that preceded this change.)

- A naive single-pass scan (a short-circuiting `fold` with a `bool`
  accumulator per chunk, early exit on a control) lost auto-vectorisation:
  about +50% end-to-end CPU. The adopted scan is branch-free per 64-byte
  chunk with byte accumulators.
- Skipping UTF-8 validation of ETag values: no measurable gain.
- Uncompressed output: no faster.
- Reusing the first key from the discovery probe's page instead of a
  separate first-key probe (one round-trip) was not done: it needs the first
  key threaded through the flat-bisection entry points and their call sites.
