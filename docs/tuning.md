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
   end of the run. The same bisection runs *inside* prefixes when discovery
   found large flat directories (`data/part-…` under a single `data/`) or
   files listed next to subdirectories, up to the structural target, shared
   by each run's estimated size (one round of seven probes at growing
   distances along the keys' number), so a directory holding 90% of the
   keys gets most of the cuts.
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
  of a leaf directory), at the median of those keys, with no more probes;
- in a flat range, near the middle between the cursor and the segment's end
  (or the listing's estimated highest key), validated by single-key probes.

The segment keeps `(start, cut]` and a new child lists `(cut, end]`,
recursively. Fan-out is **throughput-aware**: the reactor only keeps
splitting while added concurrency still raises run-wide page throughput, so
a bucket at its request-rate ceiling is not oversubscribed. `-c` is the upper
bound; the effective fan-out settles at the level that saturates the bucket
and reopens when throughput climbs again. A lowered ceiling held for three
seconds with every slot busy and the rate flat is raised by half to try
again (and lowered back if that does not help), so one noisy sample cannot
pin a run at low concurrency.

`diff` splits each side the same way. A side's segments are read by the
merge in key order: a split child is linked right after its parent, and the
merge moves past a segment only once it can no longer gain children (see
`src/segment_order.rs`). Each segment buffers at most 32 pages ahead of the
merge, and a side starts at most 32 static segments ahead of the one being
merged (its concurrency limit), so memory stays bounded whatever the bucket
size (at most 32 × 32 pages buffered per side). A side held back by that
lookahead still splits its running segments, including the one the merge is
waiting on.

Partitioning probes (discovery, bisection, split probes) share one
concurrency limit, three per `-c` slot (at most 512), and a throttled probe is
retried once after a pause. A failed probe only means "no cut there": it
can cost parallelism, never correctness. Probes are not traced.

## Requests and retries

| Setting | Value | |
|---|---:|---|
| Retry budget | 10 | Per segment, counted in *consecutive* failures: an attempt that advances the segment refunds it. |
| Backoff | 1 s, doubling, capped at 30 s | A throttle (`SlowDown`, 429) always pauses, even right after progress. |
| Request timeout | 5 s | Per page; also the SDK's attempt timeout. |
| Connect timeout | 60 s | |
| Concurrency | `-c`, default 100 | Upper bound. |
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
reach a much higher ceiling. If the result shows `stats.throttled > 0`, a
lower `-c` usually finishes sooner than retrying harder.

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
