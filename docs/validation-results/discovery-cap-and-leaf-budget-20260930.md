# Discovery Cap, Size-Aware Flat-Run Budget, Leaner Dependencies

**Date:** 2026-09-30
**Scope:** local measurements only (no real cloud endpoint was contacted).
**Baseline:** v0.38.0 (`cd9c381`).
**Change:** `src/auto_hints.rs`, `src/flat_cut.rs`, the startup-boundary
functions of `src/app/run.rs`, the split probe in `src/tasks_s3.rs`,
`Cargo.toml` / `Cargo.lock`.

## Problems

1. **Wide trees overshot the boundary target by orders of magnitude.**
   Startup discovery adds every CommonPrefix a probed level returns.  A
   tree of 20 top-level prefixes with 1,000 subdirectories each produced
   20,020 boundaries for a target of 200 (two per worker).  Every segment
   costs at least one page request, and its last page is mostly keys past
   its end, so list spent 17–18 s of CPU parsing those overlapping pages and
   diff took 30 s.
2. **Flat runs shared the budget evenly.**  When discovery finds too few
   boundaries, flat directories it probed are bisected with the remaining
   budget — split evenly between them.  With 90% of the keys in one of
   20 flat directories, that directory got 2–3 of 43 cuts and its segments
   held ~60,000 keys (max/mean 19.7).  Files sitting directly next to
   folders (at the listing root, or in any probed directory) were never
   bisected at all: 180,000 root files beside ten small folders listed as
   one segment (max/mean 61.8), serially in diff.
3. **Startup parsed discovery pages with the SDK deserializer.**  A level
   of twenty 1,000-key delimiter pages cost ~100 ms of CPU (five idle
   round-trips on a 20 ms mock) before bisection could start.
4. **Two HTTP stacks were linked.**  `aws-sdk-s3`'s default features include
   the legacy `rustls` feature (hyper 0.14 + rustls 0.21) next to the default
   HTTPS client (hyper 1 + rustls 0.23); tokio used `full`; env_logger pulled
   `regex`.

## Design

- **Cap** (`auto_hints::cap_boundaries`): after the BFS, a set larger than
  the target is thinned to `target` boundaries at even index spacing.  Any
  subset of real boundaries is a valid partition.
- **Flat runs** (`auto_hints::FlatRun`): a probed page that was truncated
  and holds at least two keys is a run — a flat directory below the root, or
  the page's files when it also has CommonPrefixes and those files share
  text past the parent prefix (`obj-` in `obj-000000123.snappy.parquet`);
  such a run is bisected under that text, so the cuts stay among the files.
  Files whose names share nothing past the parent (`a.txt`, `b/`, `c.txt`)
  are not a run.
- **Sizing** (`flat_cut::RunScale`, `flat_cut::estimate_run_keys`): the
  first and last key of the page discovery already fetched are read as
  numbers (a digit run, or an alphanumeric run over its inferred alphabet,
  as the flat cut candidates are), from the start of the run through a few
  positions past where they differ.  The run is probed once at 2, 4, 16, 64,
  256, 1,024 and 4,096 page spans past its first key, all concurrently (one
  round; at most 256 probes in flight); the farthest point that still finds
  a key under the run's text bounds its size, and the estimate is the
  geometric middle of that step.  Runs whose keys have no such reading are
  counted as the median of the others (an even split when none has one).  A
  single run skips sizing.
- **Budget** (`auto_hints::share_budget`): largest-remainder allocation in
  proportion to the estimates; a run is capped at its estimated page count,
  and the capped remainder is shared again.  The total never exceeds the
  budget.  Each run is then bisected by the existing flat partitioner with
  its share, anchored on the first key of discovery's page (no anchor
  probe).  Boundaries remain real keys, strictly increasing, inside their
  run; they are merged with the structural set and deduplicated.
- **Fast parser for discovery**: discovery's delimiter probes attach the
  same `FastContentsInterceptor` listing pages use; only the first and last
  key and the key count are kept (the SDK path still applies to any page the
  fast parser declines).
- **Runtime split probes** keep the rung pages' keys: when the ancestor
  ladder reaches the listing prefix, no rung has a CommonPrefix inside the
  segment's range, and no rung page is truncated, those pages hold every
  remaining key of the range, so the split is their (lower) median with no
  flat-cut probes.  Otherwise the flat cut runs as before.
- **Dependencies**: `aws-sdk-s3` with `default-features = false` and
  `default-https-client`, `rt-tokio`, `sigv4a` (kept by maintainer
  decision), `http-1x` (feature names verified against aws-sdk-s3 1.150.0);
  tokio `rt-multi-thread`, `macros`, `sync`, `time`, `fs`, `io-util`,
  `io-std`; env_logger `auto-color`, `humantime` (without `regex`, a
  `RUST_LOG=…/pattern` message filter is a substring match).  `aws-config`
  is unchanged; the resolved tree no longer contains hyper 0.14 or
  rustls 0.21.

## Methodology

- Host: 4 vCPU Linux container.  Both binaries `cargo build --release`
  (profile from `Cargo.toml`): v0.38.0 and this change.
- Endpoint: a local Rust ListObjectsV2 mock on `127.0.0.1`, 200,000 keys,
  1,000 per page, a fixed 20 ms delay per request (pages and probes alike),
  AWS-shaped Contents.  Diff lists the same key set on both sides.  Default
  configuration (concurrency 100: structural target 200, flat target 64),
  `--output-parquet-file`, `-l`; no hints file.
- Shapes:
  - `wide`: `t=NN/d=NNNN/part-N.parquet`, 20 × 1,000 directories, 10 keys each;
  - `skew`: `data/leaf=NN/part-N-c000.snappy.parquet`, 20 flat
    directories, 90% of the keys in `leaf=07`, ~1,050 in each other one;
  - `mixed`: 180,000 root files `obj-N.snappy.parquet` beside ten folders
    (`a00/`…`a04/`, `z05/`…`z09/`) of 2,000 keys each;
  - `obj` (`obj-N.snappy.parquet`), `part` (`data/part-N-c000…`), `hex`
    (16 hex digits `.bin`), `hier`
    (`data/tenant=NNN/dt=2024-05-DD/part-…`) as in the earlier notes.
- Five runs per binary, alternating the order each run; medians shown.
- Outputs compared on the last run of each: Parquet rows sorted (list) or in
  order (diff), and the `.ks` files byte for byte — **identical for every
  shape and mode below**.
- Balance: segment sizes of the startup boundaries (from the debug log)
  against the sorted key set.

## Results

Wall time / CPU time in seconds, median of 5.

| Shape | Mode | v0.38.0 wall | v0.38.0 CPU | This change wall | This change CPU |
| --- | --- | --- | --- | --- | --- |
| `wide` | list | 6.61 | 18.50 | **0.33** | 0.53 |
| | diff | 30.09 | 39.92 | **0.72** | 0.87 |
| `skew` | list | 1.55 | 0.82 | **0.58** | 0.54 |
| | diff | 3.39 | 1.40 | **0.87** | 0.93 |
| `mixed` | list | 2.02 | 0.64 | **0.63** | 0.52 |
| | diff | 4.63 | 0.96 | **0.98** | 0.91 |
| `obj` | list | 0.44 | 0.47 | 0.41 | 0.44 |
| | diff | 0.75 | 0.79 | 0.71 | 0.78 |
| `part` | list | 0.47 | 0.45 | 0.44 | 0.44 |
| | diff | 0.72 | 0.80 | 0.70 | 0.78 |
| `hex` | list | 0.52 | 0.55 | 0.49 | 0.53 |
| | diff | 0.77 | 0.81 | 0.75 | 0.81 |
| `hier` | list | 0.37 | 0.59 | 0.34 | 0.58 |
| | diff | 0.60 | 0.87 | 0.61 | 0.87 |

The flat-namespace shapes gain ~0.02–0.04 s from reusing discovery's first
key (one probe round-trip) and from the fast parser on discovery's page;
`hier` is unchanged within noise.

Startup boundary balance (list):

| Shape | v0.38.0 boundaries, max/mean | This change boundaries, max/mean |
| --- | --- | --- |
| `wide` | 20,020, 1.00 (10-key segments) | 200, 1.00 |
| `skew` | 64, 19.72 | 64, 2.14 |
| `mixed` | 64, 61.83 | 64, 4.79 |

Reference: with 64 evenly spaced `--hints-file` boundaries, `skew` lists in
0.22–0.24 s and `mixed` in 0.22–0.24 s.  The remaining gap to those is
startup round-trips (discovery 3, sizing 1, the largest run's high-key
estimate ~4–5, two cut waves), not segment balance.

Dependencies (`cargo tree -e normal --prefix none | sort -u | wc -l`):
359 → 329 lines.  Release binary (x86_64 Linux, stripped): 23,383,552 →
20,141,400 bytes (−13.9%).  Proxy and TLS, checked locally with both
binaries: a list with `HTTP_PROXY` pointing at the mock and the endpoint on
a closed port succeeds (the request went through the proxy), and fails to
connect with `NO_PROXY=127.0.0.1`; a list over `https://127.0.0.1` through a
local TLS terminator, with a test CA trusted via `SSL_CERT_FILE`, completes
over TLS 1.3.  `doctor` and `--dry-run` output is identical.

## What did not help

Measured during this round of investigation and left out:

- AWS SDK configuration knobs (retry, stalled-stream protection, identity
  cache and similar): each under 0.5% of CPU.
- Registering the fast Contents interceptor once at client level instead of
  per request: +4.5% diff CPU.
- glibc malloc tunables (arena count, trim/mmap thresholds): +7% to +34% CPU.
- Skipping split-probe rungs already known to be flat: no measurable gain.
