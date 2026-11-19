# How s3tl partitions and lists

There is nothing to tune: no config file, no hints, no codec flags. This
page explains what the defaults do, so you can read a run's behaviour (and
a `--trace`) and know when `-c` matters.

## Segmented listing

Parallelism comes from listing key-space *segments* concurrently — each
segment is one ListObjectsV2 continuation chain over `(start, end]` — not
from parallelism inside a chain. A segment's keys are those after its start
(`start-after`) up to and including its end, so a boundary that is a real
key belongs to the segment before it and is never dropped. Every boundary is
a real key or `CommonPrefix` the endpoint returned, never a synthetic guess,
which is what keeps the partition exact whatever the heuristics choose.

A run starts listing at once, as one segment from the start of the range,
and runs startup partitioning alongside it. Partitioning hands its
boundaries over round by round — the root probe's `CommonPrefixes` a round
trip in, the cuts of the flat runs under them once those are sized — and
each batch becomes cuts of the running segments (the same path a runtime
split takes). A listing that finishes first does not wait for the rest.
Segments come from, in order:

1. **Startup structural discovery** (recursive listings). A bounded
   breadth-first set of delimiter probes (at most 3 levels deep, at most 64
   per level, spread over the level's prefixes) finds real `CommonPrefixes`
   boundaries. The root probe follows up to 8 pages, so a bucket with
   thousands of top-level prefixes is partitioned past the first 1,000;
   deeper probes read one page. Whatever a probe could not page through —
   the rest of a listing still truncated after its pages — is bisected like
   a flat namespace (below), anchored on the last entry seen, instead of
   being left to one segment. A wide tree can find far more boundaries than
   useful, so the set is thinned to the target (two per worker), evenly
   spaced: every segment costs at least one request, and its last page is
   mostly keys past its end.
2. **Startup bisection.** When discovery finds no structure and the listing
   spans more than one page, the key range is partitioned by `max-keys=1`
   probes. Each cut aims at the middle of its range: the candidate comes from
   the first position where the range's low key and its upper end differ (a
   digit run there is read as a number, other characters over the alphabet
   the keys use), so long constant suffixes such as
   `obj-000000123.snappy.parquet` do not skew it. Bisection runs in waves (up
   to seven candidates per open range per wave, all ranges concurrently).
   A flat namespace gets one segment per worker (`-c` − 1 cuts): one more
   would wait for a free worker and then list its whole range alone at the
   end of the run. Where the keys read as numbers (`obj-000000123…`, or an
   alphanumeric run), the cuts take two rounds instead: one round of probes
   at growing distances along the number finds how far the run reaches, and
   one round at evenly spread positions (two candidates per cut) returns the
   cuts. Bisection is the fallback. The same bisection runs *inside*
   prefixes when discovery found large flat directories (`data/part-…`
   under a single `data/`) or files listed next to subdirectories: up to one
   segment per worker and at least one cut per run, shared by each run's
   estimated size (one round of seven probes at growing distances along the
   keys' number), so a directory holding 90% of the keys gets most of the
   cuts. A sized run gets at least four pages per segment.
3. **One segment** for listings that fit in one page (probing would cost
   more than listing) and for `--delimiter` listings (a page's
   `CommonPrefixes` are not range-bounded, so a split would re-list them).

`--dry-run` reports which in `plan.partitioning`.

## Runtime splitting

While a run lists, the reactor watches for segments that turn out to hold
most of the remaining data. When workers are idle and nothing is queued, it
probes the busiest segments for split points, several segments at once, and
shares the idle workers between them: a segment alone with 31 idle workers is
cut into up to 16 pieces by one probe, not halved once per probe. A child is
itself a candidate after its first page.

- where the remaining range has structure, at a `CommonPrefix` found by a
  delimiter probe up the cursor's directory ladder;
- when those probe pages together hold every key left in the range (the tail
  of a leaf directory), at their quantiles, each child at least a page — and
  not at all when one page holds them, or when the deepest page already
  reaches past the segment's end (the segment's own next page finishes the
  range);
- when the deepest of those pages is a full page of the range's keys (a flat
  run, in a directory or a flat namespace), along the run's number, scoped to
  its directory, as startup cuts runs: a range ending inside the run is
  measured off the number and cut so each child gets at least two pages (not
  yet below four: it is measured again after more pages); an open-ended one
  is sized first, each child at least four pages;
- otherwise near the middle between the cursor and the segment's end (or the
  listing's estimated highest key), validated by single-key probes.

Before any probe, a segment whose range ends inside the flat run it is
listing is cut from its own latest page: the page's first and last key give
the run's number scale, the range is measured along it, and the cuts are
evenly spread positions on it (at least two pages a child). These cuts cost
no request and no round trip; a cut need not be a key (a segment is a
contiguous range either way), and one in a gap costs its child one empty
request, what validating it would have cost.

The segment keeps `(start, cut]` and a new child lists `(cut, end]`,
recursively. A cut takes effect as soon as it is found — the segment's page
in flight is trimmed at its new end when it arrives — so a segment waiting on
a slow page hands the rest of its range over at once. Splitting fills the
side's request window (below), not `-c`.

## The request window

Each listing side has one window — a list run one, a diff one per side, so
an endpoint that throttles never slows the other side — shared by every
listing page and partitioning probe of that side. It is the only
concurrency control. With `-c` it opens at `-c`, which is also its cap.
Without `-c` it opens at 100 and, once the side's first *quiet* pages (sent
while at most four pages were in flight, such as the first segment's pages
while discovery runs) give the endpoint's latency, its cap rises to what
keeps the client's cores busy at that latency — 1,000 pages a second a core
times the latency, up to 1,024: about 130 at 30 ms on four cores, 800 at
200 ms, and 100 on a fast local endpoint. A listing page takes a slot, a
probe a third of one:

- a throttle (`SlowDown`, 429, 503) or a timed-out request shrinks it to 70%,
  at most once per round trip;
- every window's worth of answers without either grows it by one again.

Latency alone does not move the window. An endpoint that queues instead of
throttling serves the same throughput whatever the queue; what such a queue
used to cost was partitioning probes — four candidates per cut, and runtime
split probes cutting a directory's run across the whole listing's key space
— and those now take two candidates per cut and stay inside the run they
cut. (Reading rising latency as a queue
was tried and measured: latency also rises with jitter, with the client's
own CPU and at the end of a run, and shrinking for those made runs slower —
`docs/validation-results/capacity-sizing-20261118.md`.)

On an endpoint that never pushes back, the window stays at `-c`. On one with
a request-rate ceiling it settles just under the ceiling instead of queueing
and retrying above it (`docs/validation-results/request-window-20261117.md`).
Segments beyond the window are cheap — they wait for a slot. The result
reports `stats.avg_concurrency` and `stats.peak_concurrency` (a diff adds
its sides').

`diff` splits each side the same way, each side with its own window (and at
most 64 segments listing at once). A side's segments are read by the merge
in key order: a split child is linked right after its parent, and the merge
moves past a segment only once it can no longer gain children (see
`src/segment_order.rs`). The side keeps listing ahead of the merge:

- each segment buffers at most 8 pages; one whose buffer is full hands the
  rest of its range to a child and ends there, so no segment waits for the
  merge with part of its range unlisted;
- a segment waiting for the merge sends no requests and does not count
  against the side's concurrency;
- a side starts segments only within 128 of where the merge reads, so it
  buffers at most 1,024 pages whatever the bucket size;
- runtime cuts go to the segments lowest in key order (the ones the merge
  reads next), checked every 50 ms.

(Before 1.6, segments with full buffers kept their slots and their unlisted
ranges, and a large diff listed one page per round trip: 5M keys took 56 s at
30 ms.)

Partitioning probes (discovery, bisection, split probes) take their slots
from the same window. A probe is optional work: it gives up after four
typical latencies (at least a second), and a probe that failed or gave up is
a missing answer, not a failed round, so one slow request does not hold
discovery back. Runtime splitting waits for startup discovery at most two
seconds. Runtime probes stay within about one per eight listed pages (plus
two per slot). A throttled probe is retried once after 100 ms.
A failed probe only means "no cut there": it can cost
parallelism, never correctness. Probes appear in `--trace` as `Probe`
events, and the result counts them (`stats.probes`).

## Requests and retries

| Setting | Value | |
|---|---:|---|
| Retry budget | 10 | Per segment, counted in *consecutive* failures: an attempt that advances the segment refunds it. |
| Backoff | 1 s, doubling, capped at 30 s | For errors; an attempt that advanced retries at once. |
| Throttle backoff | 100 ms, doubling | A throttle always pauses; the request window has already slowed the run. |
| Request timeout | 10× the typical latency, at least 10 s, at most 60 s | Per page. |
| Connect timeout | 60 s | |
| Concurrency | `-c`; default 100, raised up to 1,024 by latency | The cap of each side's request window. |
| Worker threads | CPU cores | |

The SDK makes one attempt per call: retries are the segment loop's own, so
the real error (and its classification) always reaches it. A page that says
`IsTruncated=true` without a usable token, or hands back the token it was
sent, is a truncated page that cannot be followed — not the end of the
listing: the segment retries after the last key it received with
`start-after`. A page that says `IsTruncated=false` ends the chain even if
it carries a token. A listing whose keys arrive out of order or repeat fails
with `endpoint.inconsistent_listing`.

Every request sends `encoding-type=url`; keys and prefixes are decoded when
the response says `<EncodingType>url</EncodingType>`, and read as-is from an
endpoint that ignores the parameter.

## The request-rate ceiling

Throughput is ultimately bounded by how many ListObjectsV2 requests per
second the provider serves for one bucket:

```
objects/sec ≈ requests/sec × 1000
```

Once enough segments run to saturate that rate, more `-c` only adds waiting
workers. In third-party testing on Alibaba Cloud OSS, `-c 8` and `-c 64`
reached the same ~50K objects/sec. AWS S3 scales request rate per key-space
prefix, so well-distributed prefixes — which segmented listing exploits —
reach a much higher ceiling. The request window finds the ceiling on its
own; `stats.throttled > 0` with a low `stats.avg_concurrency` is the sign of
a rate-limited bucket, and a lower `-c` changes little.

## Output pipeline

Parquet output starts with one writer and adds writers (up to the core
count, at most 32) only while the writers are CPU-bound encoding and
compressing; on a rate-limited store it stays at one part file. Each writer
streams its own part (`part-00000.parquet`, `part-00001.parquet`, …), zstd
level 1, 100,000-row row groups, chunk-level statistics, no dictionary for
the near-unique `key` and `etag` columns. List rows are not sorted across
parts; diff output is in key order.

With `-o s3://`, each writer streams its part into a multipart upload in
16 MiB upload parts, four in flight per writer, so memory stays bounded and
no local disk is used; the uploads complete only at commit.

`--versions` lists each segment with ListObjectVersions instead of
ListObjectsV2 (partitioning is unchanged: boundaries are keys). A key's
versions can span pages, so a segment holds the trailing key of a truncated
page back until the next page: every batch ends on a key boundary, and a
retry or `--resume` restarts at the next whole key.

`diff --compare checksum` sends one HeadObject per side only for same-size
pairs whose ETags differ, a few dozen at a time, as the merge reaches them.

The binary uses mimalloc as its allocator: a listing allocates per key and
per request, and on the local mock it cut client CPU per page by 20–28%
(`docs/validation-results/startup-overlap-and-cpu-20261117.md`).

ListObjectsV2 pages are parsed by a fast path (`src/list_page.rs`): for an
HTTP 200 `ListBucketResult`, the engine builds its rows from the response
bytes in one pass and hands the SDK the document without the `<Contents>`
elements. Rows are identical to the SDK's (a differential test checks), and
anything outside a strict XML subset falls back to the SDK per page. Locally
this cut listing CPU about 3x
(`docs/validation-results/listobjectsv2-fast-contents-parser-20260929.md`).

## Reading a trace

```bash
s3tl list s3://my-bucket/ -o out/ --trace trace.jsonl
jq -r .operation trace.jsonl | sort | uniq -c        # requests by operation
jq 'select(.s3_error_code != null)' trace.jsonl      # errors
python3 examples/inspect-trace.py trace.jsonl        # summary
```

Field reference: [agent-usage.md](agent-usage.md#trace-events---trace).
