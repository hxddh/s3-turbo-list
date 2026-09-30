# Tuning Reference

This page lists runtime defaults and advanced configuration knobs that matter
for large listings.  Values come from `src/config.rs`.

## How segmented listing works

Parallelism happens between key-space segments, not inside a single
ListObjectsV2 continuation chain.  `--concurrency` only helps when there are
enough segment boundaries to keep workers busy.

Where boundaries come from, in precedence order:

1. **Explicit `--hints-file`** — pins exact boundaries for repeated
   inventories.
2. **Startup structural discovery** (recursive list runs — the default;
   hierarchical `--delimiter '/'` runs skip it) — a bounded set of delimiter
   probes (one ListObjectsV2 page each, at most 3 levels deep) finds real
   `CommonPrefixes` boundaries at run start.  It runs on every run and costs
   at most a second or two of startup, so first runs list in parallel with no
   prior steps.  A probed level adds every prefix it finds, so a wide tree
   (20 prefixes of 1,000 subdirectories each) can find far more boundaries
   than the target of two per worker; the set is then thinned to the target,
   evenly spaced.  Every segment costs at least one page request, and its
   last page is mostly keys past its end, so tens of thousands of tiny
   segments would cost far more requests than the listing itself.  Nothing
   is cached: there is no hints file in the working directory (0.37 removed
   the `<region>_<bucket>[_<hash>]_hints.toml` cache).
3. **Startup bisection** — when discovery finds no `CommonPrefixes` (a flat
   namespace) and the listing spans more than one page, the key range is
   partitioned up front by single-key `max-keys=1` probes.  Each cut aims at
   the middle of its range: the candidate comes from the first position where
   the range's low key and its upper end differ (a digit run there is read as
   a number, other characters over the alphabet the keys use), truncated
   right after it, so long constant suffixes such as
   `obj-000000123.snappy.parquet` do not skew it.  The first, open-ended range
   estimates the namespace's highest key with a few concurrent probe rounds
   (its low end is the first key of discovery's own page, so finding it
   costs no probe).
   Bisection then runs in waves: each wave cuts every open range at up to
   seven evenly spaced candidates at once (one probe each, all ranges
   concurrently), so a 64-boundary partition takes two waves instead of
   seven levels; the last wave hands out exactly the boundaries still
   missing, and a range whose probes find fewer keys than asked leaves the
   rest to one more wave, so the target is always reached when the keys
   allow it.  The boundaries are real observed keys.  List mode targets one
   boundary per worker, since runtime splitting still covers mid-run skew.

   The same bisection also runs *inside* prefixes when discovery finds
   structure but too little of it: when discovery yields fewer boundaries
   than one per worker and a probed page shows a flat run of keys longer
   than a page, the runs are bisected concurrently and their boundaries
   merged with the structural ones.  A run is either a flat directory (its
   page was truncated and held no `CommonPrefixes` — `data/part-…` under a
   single top-level `data/`) or files listed next to subdirectories
   (`obj-000000123.snappy.parquet` beside `logs/`); the latter are bisected
   under the text they share before their number (`obj-`), so the cuts stay
   among the files.  The remaining budget is shared by each run's
   **estimated size**, not evenly: the page discovery already fetched spans
   some distance along the keys' number, and one concurrent round of seven
   `max-keys=1` probes (at 2, 4, 16, … 4,096 times that span) tells how far
   the run reaches — within a factor of two to four, which is enough when
   one directory holds 90% of the keys.  A run never gets more cuts than it
   has pages; runs whose keys have no number to read along count as the
   median of the others.  Listing such a bucket with or without
   `--prefix data/` partitions the same way.
4. **Single segment** — listings that fit in one page (nothing to partition,
   and probing would cost more requests than the listing), and runs with
   `--start-after` or `--delimiter`, which are never split.  `--no-auto-hints` (supported,
   hidden from `--help`) also starts from one segment, which runtime
   splitting still fans out.

The dry-run plan's `hints.source` names the choice: `explicit`,
`startup_discovery`, `disabled_single_segment_fallback` (`--no-auto-hints`),
`delimiter_single_segment`, or `single_chain` (`--start-after`); `diff` plans
report `diff_per_side_automatic`.

`diff` partitions each side the same way (startup discovery, flat prefixes
bisected when discovery found too few boundaries, or bisection); because diff
has no runtime splitting to fall back on, its bisection targets at least eight
boundaries. Each side lists its segments concurrently; the
segment set stays static (no runtime splitting) so the merge can consume
segments in key order.

Boundaries are also adjusted **at runtime**: when a list run has idle
concurrency and one segment proves to be a long tail, the segment splits
cooperatively — the right half becomes a new parallel child segment,
recursively.  Split points come from a delimiter probe when the remaining
range has `CommonPrefixes` structure.  When none of those probes finds a
prefix inside the range but their pages, together, hold every key left in it
(the ladder reached the listing prefix and no page was truncated — the tail of
a leaf directory), the cut is the median of those keys, with no further
probes.  Otherwise, for flat ranges (no `/` structure), the
cut is placed near the middle of the range between the segment's cursor and
its end (or the listing's estimated highest key, for the last segment) the
same way startup bisection places it, and validated with single-key probes, so
the boundary is always a real observed key.  The reactor
probes the busiest long-tail segments as soon as slots are idle (not on a fixed
once-per-second tick) and fans out several at once, so a flat namespace whose
startup bisection under-partitioned it ramps in a few page round-trips rather
than one segment per second.  Fan-out is **throughput-aware**: the reactor watches
run-wide page throughput and only keeps splitting while added concurrency is
still raising it, so a single bucket at its request-rate ceiling (see below) is
not oversubscribed past the point where more in-flight segments only add
latency.  `--concurrency` is the upper bound; the effective fan-out settles at
whatever lower number saturates the bucket, and reopens automatically if
throughput climbs again (for example as long-tail segments finish and free
slots).  Splitting never applies to `diff` (static segments by design) or
`--start-after` runs.  Split segments are checkpointed like any other: an
interrupted run records the exact key ranges not yet written, including those
of runtime-split children, and `--resume` lists only those ranges.

**Defaults are designed to be the right choice**: `worker_threads` follows
the machine's CPU count, and `--concurrency` only needs raising when a very
large bucket on a fast network leaves workers idle.  Hand-tuning `-c`, or the
`-T/--threads` option (supported, hidden from `--help`), is rarely
worthwhile.

Hints boundaries are lexicographic cut points, not directories.  A boundary
may also be a real object key; it is treated as part of the preceding segment
so adjacent `start-after` segments do not drop it.  Folder marker objects such
as `logs/` are ordinary keys for correctness purposes.

## Hints files

`--hints-file` (list only; `diff` does not take it) accepts two formats:

**Plain text** (one boundary per line):

```
alpha/
beta/
logs/
```

**TOML** (the format earlier releases cached; an optional `prefix` records
the range the boundaries partition and is verified on load):

```toml
bucket = "my-bucket"
region = "us-east-2"
boundaries = ["alpha/", "beta/", "logs/"]
generated_at = "2026-05-14T12:00:00Z"
```

Older hints files may carry extra fields (such as `total_objects` or
`scan_mode`); they are accepted and ignored on load.

Validate a hints file locally (no S3 access) with `doctor --hints-file
hints.toml`. Hints are entirely optional: startup discovery and runtime
splitting partition buckets automatically, so `--hints-file` is only for
pinning exact boundaries on repeated inventories.

## Core Defaults

| Config key | Default | Notes |
|---|---:|---|
| `s3.max_attempts` | `10` | Retry budget per segment, counted in *consecutive* failures: an attempt that advances the segment's resume point refunds it. Retries are the segment loop's own; the SDK is configured for a single attempt per call so the error it holds reaches that loop instead of being cut off by a timeout. |
| `s3.initial_backoff_secs` | `1` | Seed for the pause between consecutive retries of a segment, doubling per attempt and capped at 30s. `0` disables the pause. |
| `s3.connect_timeout_secs` | `60` | Connection timeout. |
| `s3.operation_timeout_secs` | `5` | Per-page ListObjectsV2 watchdog and SDK operation/read/attempt timeout. |
| `runtime.worker_threads` | CPU cores | Tokio worker threads; CLI override: `-T`, `--threads`. |
| `runtime.max_concurrency` | `100` | Max concurrent list operations; CLI override: `-c`, `--concurrency`. |
| `channel.capacity` | `64` | Bounded channel capacity between list tasks and data-map output. |
| `output.row_group_size` | `100000` | Parquet max row group size. |
| `output.compression` | `zstd` | Parquet compression codec; CLI override: `--compression`. |
| `output.compression_level` | `1` | Compression level for codecs that support levels; CLI override: `--compression-level`. |

For high-latency or cross-region endpoints, consider raising
`s3.operation_timeout_secs` to `30` or `60` to reduce retry churn.

A page that reports `IsTruncated=true` without a usable
`NextContinuationToken`, or hands back the continuation token it was just
sent, is a truncated page the listing cannot follow — not the end of the
listing.  It counts as a retryable failure of the segment: the retry resumes
after the last key received, with `start-after`, and refunds the budget while
it advances.  An endpoint that keeps doing this without progress exhausts
`s3.max_attempts` and fails the run, rather than the run exiting 0 with short
output.

## The single-bucket request-rate ceiling

List throughput is ultimately bounded by how many `ListObjectsV2` requests
per second the provider serves for one bucket, not by anything on the client.
Each request returns at most 1000 keys, so the ceiling is roughly:

```
max objects/sec ≈ (requests/sec the provider allows) × 1000
```

Once enough segments are running to saturate that request rate, adding more
`--concurrency` or `--threads` does nothing — the extra workers just wait on
the provider.  In third-party testing on Alibaba Cloud OSS, `-c 8` and `-c 64`
reached the same ~50K objects/sec because the bucket's request rate, not the
tool, was the limit.  AWS S3 scales request rate per key-space prefix, so
well-distributed prefixes (which segmented listing already exploits) reach a
much higher ceiling than a single hot prefix.

Practical guidance:

- Raise `--concurrency` until throughput stops improving, then stop; past
  that point you are only adding idle workers.
- If you are throttled (HTTP 503 `SlowDown`), the run reports it: the manifest
  carries `metrics.throttled_responses` and the `metrics.http_error_statuses`
  histogram behind it. Retries then back off, so the run rides out a transient
  throttle instead of answering back-pressure with more requests. Sustained
  throttling still ends the run once a segment burns its consecutive-failure
  budget without advancing — lowering request pressure (fewer concurrent
  segments) helps more than retrying harder.
- The largest wins come from spreading load across prefixes, which segmented
  listing does automatically.

## Adaptive Parquet output

The single-bucket request-rate ceiling means listing is usually I/O-bound, and
one task encoding and compressing Parquet keeps up easily.  But on a store that
is *not* rate-limited (well-distributed AWS prefixes, a large self-hosted
MinIO/Ceph cluster, a LAN) on a many-core machine, listing can feed faster than
one writer can encode+compress — the writer becomes the bottleneck.

Parquet list output adapts automatically.  It starts with one writer and, only
while the writers are CPU-bound (busy encoding most of the time rather than
waiting for input), adds more writers up to the machine's core count.  Each
extra writer streams to its own part-file (`<name>.part1.parquet`,
`<name>.part2.parquet`, …) alongside the primary `<name>.parquet`.  There is no
flag — on a rate-limited store the writers idle, the pool stays at one writer,
and the output is a single file exactly as before.

When output does scale to multiple part-files, read the base file and its
parts together — the run manifest lists each as a `parquet` artifact, or use a
glob such as `duckdb.sql("SELECT * FROM 'out/name*.parquet'")` /
`pq.ParquetDataset(glob.glob("out/name*.parquet"))`.  Do not read the whole
output directory: it also holds the `.ks` CSV (not Parquet) and the files of
any other run written there.  A run removes stale `.partN` files that an
earlier, wider run of the same output path left behind.  The companion `.ks`
counts are kept once by the coordinator for the whole run, and all run metrics
are merged across the parts into one set.

Response parsing is the other per-object CPU cost.  Deserializing a
ListObjectsV2 page through the AWS SDK tokenizes every `<Contents>` element,
copies each field into owned strings (and, before that, checks whether the 200
response is really an `<Error>` document, which UTF-8-validates and tokenizes
the page again); on an unthrottled local store that was ~80% of the listing's
CPU.  The list and diff engines therefore parse `<Contents>` themselves: each
ListObjectsV2 request carries its own interceptor that, for an HTTP 200
`ListBucketResult` body, builds the engine's rows directly from the response
bytes in one forward pass, and gives the SDK the same document with the
`<Contents>` elements removed.  The SDK still parses everything else —
`IsTruncated`, `NextContinuationToken`, `KeyCount`, `CommonPrefixes`, and error
responses — as before, over a few hundred bytes instead of the whole page.  The
rows are identical to the SDK path's (same key unescaping, ETag, size, and
`LastModified` rules), so output does not change; locally this cut listing CPU
about 3x (see `docs/validation-results/listobjectsv2-fast-contents-parser-20260929.md`).

The fast parser only accepts a strict subset of XML, and falls back per page,
automatically, to the unmodified SDK path when a page steps outside it: CDATA,
comments, processing instructions other than the XML declaration, a DOCTYPE,
namespace-prefixed element names, attributes on elements other than the root,
non-UTF-8 input or characters XML forbids, an unknown entity or invalid
character reference, nested markup inside a field, duplicate fields, a
`<Contents>` without a `<Key>`, or a field value the SDK would reject.
Runtime split probes use the fast parser too (their rows are dropped; only
`CommonPrefixes` matter).  Startup discovery probes and `compat-probe` stay on
the plain SDK path.

Streaming TSV/NDJSON to stdout and `diff` output stay single-writer by nature
(one pipe / one file).  TSV/NDJSON rows arrive in segment-completion order, not
key order; sort downstream if order matters.  Diff Parquet output is in key
order.

## Config File Settings

Some advanced settings are easiest to keep in a TOML config file passed with
`--config`.  Command-line options take precedence where both exist.

```toml
[s3]
operation_timeout_secs = 30
max_attempts = 10
initial_backoff_secs = 5
connect_timeout_secs = 60

[runtime]
worker_threads = 8
max_concurrency = 32

[output]
row_group_size = 100000
compression = "zstd"
compression_level = 1

[channel]
capacity = 128
```

Keys are checked: an unknown section or key (a typo such as
`max_concurency`) fails config loading with exit code `2` and names the
expected keys, instead of silently running on the default.  The full key
list, the search path, and a custom-endpoint example are in
[`providers.md`](providers.md#config-file).

## Trace-Driven Inspection

Long-tail segments are split at runtime automatically, so no offline
rebalancing workflow is needed.  For the raw per-page and per-segment events,
pass `--trace-compat trace.jsonl` to a run (`--trace-compat -` writes to
stderr).  `--trace-compat` combines with `--run-manifest`: use the manifest
for final status and aggregate metrics, the trace for per-request endpoint
behavior.

The trace records every listing page request (with its retries) and one
summary event per completed segment, plus each `compat-probe` request.  The
planning probes are not traced: startup structural discovery, flat-namespace
bisection, and runtime split probes (a few `max-keys=1` or delimiter
requests each).

```bash
s3-turbo-list list --bucket my-bucket --region us-east-1 --output-dir out \
  --trace-compat trace.jsonl --run-manifest run.json

jq -r .operation trace.jsonl | sort | uniq -c          # count by operation
jq 'select(.s3_error_code != null)' trace.jsonl        # errors
python3 examples/inspect-trace.py trace.jsonl          # summary
```

### Trace event fields

Each line is one `S3CompatEvent`.  Optional fields (`?`) are omitted when
absent.  `profile` is deprecated (0.38; removed in 0.39): read `provider`.

| Field | Type | Description |
|---|---|---|
| `timestamp` | string | ISO 8601 wall-clock time of the call. |
| `operation` | string | S3 operation (e.g. `"ListObjectsV2"`, `"HeadBucket"`), or `"ListObjectsV2SegmentSummary"` for a completed segment. |
| `provider` | string? | Provider preset name (e.g. `"bos"`, `"minio"`). |
| `profile` | string? | Deprecated: the same value as `provider`. |
| `endpoint_url` | string | Endpoint URL used for the request. |
| `region` | string? | Region the request was signed for. |
| `addressing_style` | string | `"path"`, `"virtual"`, or `"auto"`. |
| `bucket` | string | Target bucket. |
| `prefix` | string | Listing prefix. |
| `delimiter` | string? | Delimiter sent with the request.  The listing default is `""` (recursive), which is omitted from requests and from the event; hierarchical runs and structural probes send `"/"`. |
| `start_after` | string? | `start-after` parameter, if sent. |
| `max_keys` | int? | `max-keys` parameter, if sent. |
| `continuation_token` | string? | Continuation token the request sent (absent on a chain's first page). |
| `http_status` | uint16 | HTTP response status. |
| `s3_error_code` | string? | S3 error code (e.g. `"NoSuchBucket"`). |
| `s3_error_message` | string? | Error message body. |
| `request_id` | string? | `x-amz-request-id` or equivalent. |
| `request_id_2` | string? | `x-amz-id-2` (extended request ID). |
| `retry_attempt` | uint32 | Zero-indexed retry count. |
| `latency_ms` | uint64 | Round-trip latency in milliseconds. |
| `retryable` | bool | Whether the error is classified as retryable. |
| `fatal` | bool | Whether the error is classified as fatal. |
| `is_truncated` | bool | Whether the ListObjectsV2 response was truncated. |
| `next_continuation_token` | string? | Continuation token for the next page. |
| `key_count` | int? | `KeyCount` from the response. |
| `contents_count` | int? | Number of `Contents` entries. |
| `common_prefixes_count` | int? | Number of `CommonPrefixes` entries. |
| `next_continuation_token_present` | bool? | Whether the response included a next continuation token. |
| `first_key` | string? | First object key in the page. |
| `last_key` | string? | Last object key in the page. |
| `segment_index` | uint? | Segment index (segment summary events). |
| `end_before` | string? | Upper segment boundary, when present. |
| `segment_pages` | uint32? | Pages read by a completed segment. |
| `segment_objects` | uint? | Objects emitted by a completed segment. |
| `segment_common_prefixes` | uint? | CommonPrefixes seen by a completed segment. |
| `ended_by` | string? | Segment completion reason, such as `"pagination"` or `"boundary"`. |
| `truncated_raw_body` | string? | First 512 bytes of an error response body. |
