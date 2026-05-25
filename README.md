# s3-turbo-list

High-performance S3-compatible bucket listing, tracing, diffing, checkpoint/resume,
and Parquet export for large object stores.

## Project positioning

s3-turbo-list scans S3-compatible buckets much faster than a sequential `aws s3 ls`
by partitioning the key space into segments and listing them concurrently.  It
targets operations teams and data engineers who routinely work with millions of
objects across multiple S3-compatible providers and need fast, reliable, auditable
listings.

**What it is:**
- A single static binary that lists buckets, diffs two buckets, auto-discovers
  key-space segments, and exports results to Parquet.
- A **tracing layer** that records every S3 API call as structured JSONL —
  making provider behaviour observable and debuggable.
- A **compat-probe** that validates an endpoint before full-scale listing, so
  you know whether a provider works before you spend time on it.

**Why it exists:**
Standard S3 listing tools (`aws s3 ls`, `rclone ls`, `s5cmd ls`) are
single-threaded by design.  On buckets with hundreds of millions of objects a
single listing pass can take hours.  s3-turbo-list slices the work into
independent segments, runs them in parallel, and assembles the result.

**What problems it solves:**
- **Slow listings** — concurrent segmented listing beats sequential.
- **Provider uncertainty** — compat-probe + trace JSONL tell you exactly
  which operations an endpoint supports and how it behaves.
- **Unreliable long runs** — checkpoint/resume saves progress so an
  interrupted scan picks up where it left off.
- **Opaque diffing** — bi-directional diff with DiffFlag annotations in
  Parquet tells you what changed between two buckets.
- **Ad-hoc analysis** — Parquet output drops directly into pandas, duckdb,
  or any data tool with zero parsing overhead.

## Core features

| Feature | Description |
|---|---|
| **ListObjectsV2 scanning** | High-performance concurrent listing via the S3 ListObjectsV2 API. |
| **Multi-threaded / segmented listing** | Auto-discovered key-space hints split a bucket into parallel segments, or supply your own hints file. |
| **Prefix discovery** | `discover-prefixes` paginates ListObjectsV2 `CommonPrefixes` locally into prefix hints. |
| **Streaming Parquet output** | List mode writes received batches directly to configurable compressed Parquet, keeping memory bounded for large buckets. |
| **KS keyspace output** | Companion CSV showing per-prefix object counts. |
| **Trace JSONL output** | Every S3 API call recorded as structured JSONL for observability and debugging. |
| **Compat-probe** | Quick validation of any S3-compatible endpoint before full-scale work. |
| **Checkpoint / resume** | Interrupted scans resume from last-saved segment. Identity-verified to prevent mismatches. |
| **Diff mode** | Bi-directional diff between two buckets with per-object DiffFlag. Equal rows optionally included. |
| **Agent-friendly JSON** | Local dry-run plans, config inspection, doctor checks, stable exit codes, and run manifests for automation. |
| **Beginner-friendly local UX** | `init-config`, `quickstart`, `recipes`, `cheatsheet`, `--output-dir`, and compact `doctor` output. |
| **Local protocol harness** | Integration-test-only S3 mock verifies protocol correctness without contacting real cloud endpoints. |
| **Endpoint validation workflow** | Compat-probe → listing → trace review. Documented in `docs/validation-results/`. |

## Installation

Download the binary for your platform from the GitHub release, verify it with
`SHA256SUMS`, install it into your `PATH`, then configure AWS-compatible
credentials through the standard AWS SDK credential chain.

See [INSTALL.md](INSTALL.md) for platform-specific installation and AWS S3 / BOS / MinIO configuration examples.

## Quick start

### 30 seconds

```bash
# After installing the release binary:
s3-turbo-list doctor --local-only --simple
export AWS_PROFILE=default
s3-turbo-list --dry-run --agent --output-dir out --delimiter '' \
  list --region us-east-2 --bucket my-bucket
s3-turbo-list --output-dir out --delimiter '' \
  list --region us-east-2 --bucket my-bucket

# Count objects and bytes without writing Parquet/KS outputs
s3-turbo-list --summary-only --delimiter '' \
  list --region us-east-2 --bucket my-bucket

# Stream rows to shell tools without writing Parquet/KS outputs
s3-turbo-list --delimiter '' list --region us-east-2 --bucket my-bucket \
  --output-format ndjson > objects.ndjson
s3-turbo-list --run-manifest run.json --delimiter '' \
  list --region us-east-2 --bucket my-bucket --output-format tsv | wc -l
s3-turbo-list manifest-summary run.json
s3-turbo-list manifest-summary run.json --check
```

`init-config`, `quickstart`, `recipes`, and `cheatsheet` are local-only. They do
not contact S3 and do not edit AWS credentials. Use `AWS_PROFILE` for
credentials; use `--profile` only for endpoint compatibility presets such as
`minio`, `bos`, `r2`, `b2`, or `oss`.

### Build from source

```bash
cargo fmt --check
cargo build
cargo clippy --all-targets -- -D warnings

# Release build (see BUILD.md for aarch64 workaround)
cargo build --release
```

### First local preflight

```bash
s3-turbo-list doctor --local-only --simple
s3-turbo-list init-config --output s3-turbo-list.toml
s3-turbo-list quickstart aws
```

### Basic list command

```bash
# Dry-run first; does not contact S3
s3-turbo-list --dry-run --agent --output-dir out --delimiter '' \
  list --region us-east-2 --bucket my-bucket

# Real run; creates out/ and writes Parquet + KeySpace files
s3-turbo-list --output-dir out --delimiter '' \
  list --region us-east-2 --bucket my-bucket

# Limit parallelism
cargo run -- --delimiter '' list --region us-east-2 --bucket my-bucket --prefix logs/ -T 4 -c 20
```

The default delimiter is `/`, which performs hierarchical listing and returns
top-level objects plus `CommonPrefixes`.  Use `--delimiter ''` for a recursive
full-bucket object inventory.

`--continuation-token` is a single-chain ListObjectsV2 resume tool.  Use it
only with `list` and `--no-auto-hints`; it is intentionally rejected with
multi-segment hints, `diff`, checkpoint resume, or `--start-after`.

Output files (auto-named):
- `<region>_<bucket>_<timestamp>.parquet` — object listing
- `<region>_<bucket>_<timestamp>.ks` — keyspace counts

### Object filters

`--filter` applies a local object filter after S3 listing and before output.
It does not reduce S3 ListObjectsV2 requests; use `--prefix`, `--delimiter`,
and `--max-keys` for request-side shaping.

Supported filter expressions are intentionally small:

- Variables: `SOURCE` in `list`; `SOURCE` and `TARGET` in `diff`.
- Properties: `size` and `last_modified`.
- Operators: numeric comparison, arithmetic, `&&`, `||`, and `!`.

Examples:

```bash
# Keep objects larger than 1 GiB
s3-turbo-list --filter 'SOURCE.size > 1073741824' \
  --delimiter '' list --region us-east-2 --bucket my-bucket

# Keep recently modified objects by epoch seconds
s3-turbo-list --filter 'SOURCE.last_modified >= 1715700000' \
  --delimiter '' list --region us-east-2 --bucket my-bucket

# Diff-only: compare source and target sizes
s3-turbo-list --filter 'SOURCE.size != TARGET.size' \
  diff --bucket left-bucket --target-bucket right-bucket
```

Filters reject functions, methods, strings, arrays, maps, indexing, statements,
and long or deeply nested expressions before any listing run.  Rejected filters
exit with code `2`.

### Choosing an output mode

- Default `list`: scans S3 and writes Parquet + KeySpace artifacts.  Use this
  for audit records, DuckDB/pandas analysis, and repeatable inventory files.
- `list --output-format tsv`: scans S3 and streams
  `<key><TAB><size><TAB><last_modified_epoch_secs>` rows to stdout.  It does
  not write Parquet or KeySpace outputs.
- `list --output-format ndjson`: scans S3 and streams one JSON object per row
  to stdout, using compact keys: `k` for key, `s` for size, and `m` for
  last-modified epoch seconds.  It does not write Parquet or KeySpace outputs.
- `--summary-only`: scans S3 and reports aggregate metrics such as object
  count, total bytes, and top prefixes.  It does not write Parquet or KeySpace
  outputs.
- `--dry-run`: does not contact S3.  It only resolves inputs, planned outputs,
  local hints, checkpoint identity, and warnings.

TSV and NDJSON are list-only formats.  They reserve stdout for rows, so use
`--run-manifest run.json` plus `s3-turbo-list manifest-summary run.json --json`
when automation also needs a structured run summary.

### Validating a completed run

`manifest-summary` is local-only: it reads a saved `--run-manifest` JSON file
and checks recorded artifact paths on the local filesystem without contacting S3.

```bash
s3-turbo-list --run-manifest run.json --output-dir out --delimiter '' \
  list --region us-east-2 --bucket my-bucket
s3-turbo-list manifest-summary run.json --check
```

`--check` exits non-zero when the manifest reports a failed run, a non-zero
exit code, fatal/output errors, a Parquet row-count mismatch, or a missing
recorded artifact.  When recorded metadata is present, `--check` also verifies
current artifact file size, SHA256, and Parquet row/schema metadata.  For
`summary-only`, `tsv`, and `ndjson` runs, Parquet row equality is intentionally
reported as not applicable.

`manifest-summary --check --json` includes a stable `check` summary with
machine-readable pass/fail counts, artifact counts, and row/schema/exit-code
status values for CI and agent workflows.

### Read Parquet with Python / pyarrow

```python
import pyarrow.parquet as pq

t = pq.read_table("us-east-2_my-bucket_20260514120000.parquet")
print(t.schema)
# Key: string
# Size: uint64
# LastModified: uint64
# ETag: string
# DiffFlag: uint8

df = t.to_pandas()
print(df.head())
print(f"{len(df)} objects")
```

### Inspect trace JSONL

```bash
# Human-readable
cat trace.jsonl | jq '.'

# Count by operation
cat trace.jsonl | jq -r .operation | sort | uniq -c

# Find errors
cat trace.jsonl | jq 'select(.s3_error_code != null)'
```

## Usage examples

### AWS S3

```bash
# Standard listing
s3-turbo-list --delimiter '' list --region us-east-2 --bucket my-bucket

# With virtual-hosted addressing
s3-turbo-list --delimiter '' list --region us-east-2 --bucket my-bucket \
  --addressing-style virtual

# Enable trace
s3-turbo-list --delimiter '' list --region us-east-2 --bucket my-bucket \
  --trace-compat trace.jsonl

# Use traditional gzip output for older downstream readers
s3-turbo-list --delimiter '' --compression gzip --compression-level 6 \
  list --region us-east-2 --bucket my-bucket
```

### MinIO

```bash
# Local MinIO
s3-turbo-list --delimiter '' list --bucket my-bucket \
  --endpoint-url http://localhost:9000 \
  --profile minio

# Or via config file (s3-turbo-list.toml):
#   [s3]
#   endpoint_url = "http://localhost:9000"
#   profile = "minio"
```

### Endpoint profiles

Endpoint profiles are optional presets for common S3-compatible providers.
They are explicit: the default path remains standard S3 behavior unless you
set `--profile` or `profile = "..."` in config.

```bash
# Local-only profile discovery
s3-turbo-list profiles list
s3-turbo-list profiles show r2 --json

# Cloudflare R2 example; endpoint is account-specific
s3-turbo-list --delimiter '' list --bucket my-bucket \
  --endpoint-url https://<account-id>.r2.cloudflarestorage.com \
  --region auto \
  --profile r2
```

Built-in profiles: `aws`, `minio`, `bos`, `r2`, `b2`, and `oss`.
Profiles only fill safe defaults such as addressing style or documented
endpoint defaults when the user has not supplied an explicit value.  They do
not change output schema, diff semantics, or pagination behavior.
Profiles whose endpoints are account, bucket, or region specific will warn
during `doctor` and dry-run until `--endpoint-url` or `s3.endpoint_url` is set.
Starter config placeholders such as `<account-id>` are also reported locally
before a real run.

### BOS (Baidu Object Storage)

BOS recommends bucket virtual hosting (`bucket.s3.<region>.bcebos.com`).
s3-turbo-list supports this with virtual-hosted addressing:

```bash
# Virtual-hosted (recommended for BOS)
s3-turbo-list --delimiter '' list --region bj --bucket my-bos-bucket \
  --profile bos
```

The built-in `bos` profile preset uses the BOS S3-compatible endpoint and
virtual-hosted addressing.  You can still pass `--endpoint-url` explicitly
when validating a specific regional endpoint.

Path-style is supported but intended only for legacy compatibility,
diagnostics, or controlled validation:

```bash
# Path-style (legacy / diagnostic only)
s3-turbo-list --delimiter '' list --region bj --bucket my-bos-bucket \
  --profile bos \
  --addressing-style path
```

### Trace / debug

```bash
# Write trace to file
s3-turbo-list --delimiter '' list --region us-east-2 --bucket my-bucket \
  --trace-compat trace.jsonl

# Stream trace events to stderr
s3-turbo-list --delimiter '' list --region us-east-2 --bucket my-bucket \
  --debug-s3
```

### Agent / automation mode

Use local-only JSON commands before allowing an agent or CI job to run a scan:

```bash
s3-turbo-list config-inspect --json
s3-turbo-list doctor --local-only --json

s3-turbo-list --dry-run --agent list \
  --region us-east-2 \
  --bucket my-bucket \
  --output-parquet-file out/list.parquet \
  --output-ks-file out/list.ks
```

The JSON preflight reports include `config_source`, so agents can see which
TOML file was loaded and which global CLI flags overrode configuration values.

For a real run, write a machine-readable manifest:

```bash
s3-turbo-list --run-manifest run.json list \
  --region us-east-2 \
  --bucket my-bucket \
  --output-parquet-file out/list.parquet \
  --output-ks-file out/list.ks
```

See [`docs/agent-usage.md`](docs/agent-usage.md) for JSON fields and stable
exit-code classes.  Run manifests include artifact summaries with SHA256,
file sizes, line counts, and Parquet row/schema metadata.

Trace output is one JSON object per line (JSONL). Each line contains:
operation, endpoint, addressing style, profile, region, bucket, prefix,
delimiter, max-keys, start-after, continuation token, HTTP status, S3
error code, request ID, latency, pagination details, optional per-page
`first_key`/`last_key` samples when tracing is enabled, and truncated error
body excerpts (first 512 bytes).

Local trace and hints tooling is available for agents and CI without contacting
S3:

```bash
s3-turbo-list trace-summary trace.jsonl --output-format json
s3-turbo-list hints-merge hints-a.toml hints-b.txt --output merged.toml --machine-readable
s3-turbo-list hints-rebalance \
  --trace trace.jsonl \
  --hints-file merged.toml \
  --output rebalanced.toml \
  --emit-manifest rebalance.manifest.json \
  --machine-readable
```

These commands parse local files only.  They do not alter `list`/`diff`
concurrency, do not add S3 requests, and do not enable provider-specific
pagination workarounds.

For concise local help:

```bash
s3-turbo-list recipes
s3-turbo-list recipes summary
s3-turbo-list recipes filter
s3-turbo-list recipes verify
s3-turbo-list recipes release-check
s3-turbo-list recipes diff-safe
s3-turbo-list recipes large-bucket
s3-turbo-list cheatsheet
```

### Local benchmark and CLI docs

```bash
# Synthetic local streaming-output benchmark; does not contact S3
s3-turbo-list --compression zstd --compression-level 3 \
  benchmark-local --objects 100000 --batch-size 5000 --output-format parquet --json

# Measure the local NDJSON row formatter without contacting S3
s3-turbo-list benchmark-local \
  --objects 100000 --batch-size 5000 --output-format ndjson --json

# Shell completions and man page
s3-turbo-list completions bash > s3-turbo-list.bash
s3-turbo-list completions zsh > _s3-turbo-list
s3-turbo-list man > s3-turbo-list.1
```

The helper script `scripts/benchmark-local.sh` writes a machine-readable
local benchmark report.  See [`docs/benchmarking.md`](docs/benchmarking.md)
and [`docs/endpoint-profiles.md`](docs/endpoint-profiles.md).

The local S3 protocol mock used by integration tests is documented in
[`docs/local-s3-mock.md`](docs/local-s3-mock.md).  It validates CLI behavior
against local XML fixtures and never contacts real cloud endpoints.

### Diff mode

```bash
# Compare two buckets
s3-turbo-list diff \
  --region us-east-2 --bucket source-bucket \
  --target-region us-west-2 --target-bucket target-bucket

# In current releases, diff mode always outputs Equal rows.
# Filter DiffFlag=0 downstream with pandas or duckdb if needed.

# With trace for both sides
s3-turbo-list diff \
  --region us-east-2 --bucket source-bucket \
  --target-region us-west-2 --target-bucket target-bucket \
  --trace-compat diff-trace.jsonl
```

Diff mode intentionally uses authoritative single-segment listing in current
releases.  Conventional hints caches are ignored for `diff`, and explicit
`--hints-file` is rejected before any S3 request.  Paired-segment hinted diff
coordination is planned for `v0.2.x`; use hints with `list` only for now.
Unlike list-mode Parquet output, current diff mode holds the comparison map in
memory until both sides finish so it can classify equal, left-only, right-only,
and changed objects.  For very large bucket-to-bucket comparisons, plan memory
capacity around the combined key count or split the comparison externally.

### Checkpoint / resume

```bash
# Start a listing with checkpoint save
s3-turbo-list list --region us-east-2 --bucket my-bucket

# If interrupted (Ctrl-C, crash, network), resume:
s3-turbo-list list --region us-east-2 --bucket my-bucket --resume
```

Checkpoint identity includes bucket, region, prefix, delimiter, max-keys,
addressing style, profile, and mode — resume is safe across restarts.
Saves progress every 30 seconds and on graceful shutdown.

### Hints file

Hints files split a bucket into segments for parallel listing.  Two formats
are supported:

**Plain text** (one key-space boundary per line):

```
alpha/
beta/
logs/
```

**TOML** (auto-hints cache, generated by `auto-hints`):
```toml
bucket = "my-bucket"
region = "us-east-2"
total_objects = 50000
boundaries = [
    "alpha/",
    "beta/",
    "logs/",
]
generated_at = "2026-05-14T12:00:00Z"
scan_mode = "full"
estimate_mode = "full"

[[segment_estimates]]
start_after = ""
end_before = "alpha/"
estimated_objects = 12000

[[segment_estimates]]
start_after = "alpha/"
end_before = "beta/"
estimated_objects = 18000
```

Generate hints automatically:
```bash
s3-turbo-list auto-hints --region us-east-2 --bucket my-bucket -o hints.toml
```

Generate hints for one prefix subtree:
```bash
s3-turbo-list --prefix logs/2026/ auto-hints \
  --region us-east-2 \
  --bucket my-bucket \
  -o logs-2026-hints.toml
```

Discover delimiter-based CommonPrefixes before building manual hints:
```bash
s3-turbo-list --prefix logs/ --delimiter / discover-prefixes \
  --region us-east-2 \
  --bucket my-bucket \
  -o logs-prefixes.txt
```

For very large buckets, generate an estimated hints file from a bounded sample:
```bash
s3-turbo-list auto-hints --region us-east-2 --bucket my-bucket \
  -o hints.sampled.toml \
  --sample-limit 1000000 \
  --max-pages 1000
```

Hints boundaries are lexicographic cut points, not directories.  A boundary may
also be a real object key; v0.1.10 treats that object as part of the preceding
segment so adjacent `start-after` segments do not drop it.  Folder marker
objects such as `logs/` are ordinary keys for correctness purposes.

In sampled mode, the TOML `total_objects` field is the sampled object count,
not the full bucket total.  Prefix-scoped hints generated with `--prefix` are
valid for that prefix subtree, not for the whole bucket.  The cache also records
`scan_mode`, `sampled_objects`, `sampled_pages`, `sample_limit`, `max_pages`,
and per-segment estimates marked as sampled/estimated rather than authoritative
bucket-wide statistics.

`auto-hints` performs one sequential object scan.  `--prefix` and `--max-keys`
apply to that scan; `--threads` and `--concurrency` do not make it parallel.
Use `discover-prefixes` for delimiter/CommonPrefixes discovery instead of
mixing delimiter folding into object-count estimates.

Validate a hints file locally before a cloud run:
```bash
s3-turbo-list hints-validate --hints-file hints.toml
```

Merge multiple local hints files into one sorted TOML cache:
```bash
s3-turbo-list hints-merge base.toml prefixes.txt --output merged.toml
```

Analyze a previous trace and generate conservative next-run hints for observed
long-tail segments:
```bash
s3-turbo-list hints-rebalance \
  --trace trace.jsonl \
  --hints-file merged.toml \
  --output rebalanced.toml \
  --explain
```

Use a hints file:
```bash
s3-turbo-list list --region us-east-2 --bucket my-bucket -H hints.toml
```

Advanced runtime and auto-hints tuning is documented in
[`docs/tuning.md`](docs/tuning.md).  Some knobs, including
`auto_hints.sample_threshold`, `auto_hints.max_prefix_depth`,
`auto_hints.min_segment_size`, and `auto_hints.max_prefix_entries`, are
TOML-only settings and do not have CLI flags.

### How segmented listing works

Parallelism happens between key-space segments, not inside a single
ListObjectsV2 continuation chain.  `--concurrency` only helps when hints provide
enough boundaries to keep workers busy.  With no hints, or after
`--no-auto-hints`, listing falls back to one segment and follows one paginator
chain.

## Output schema

### Parquet columns

| Column | Type | Description |
|---|---|---|
| `Key` | `Utf8` | Object key (full path). |
| `Size` | `UInt64` | Object size in bytes. |
| `LastModified` | `UInt64` | Unix timestamp (seconds since epoch). |
| `ETag` | `Utf8` | S3 ETag (hex-encoded MD5, with optional multipart part count suffix). |
| `DiffFlag` | `UInt8` | Diff classification (see below). |

### DiffFlag meaning

| Value | Name | Meaning |
|---|---|---|
| `0` | Equal | Object exists in both buckets with identical size and ETag. |
| `1` | Left-only (Plus) | Object exists only in the left (source) bucket. |
| `2` | Right-only (Minus) | Object exists only in the right (target) bucket. |
| `3` | Asterisk | Object exists in both buckets but differs in size, ETag, or data. |

In list mode (single bucket), all rows carry `DiffFlag = 0`.

### KS keyspace file

CSV with two columns — prefix and object count:
```
"alpha/","150"
"beta/","3200"
"logs/","42"
```

## Trace fields

Trace events are written as JSONL.  Each line is one `S3CompatEvent` with
the following fields:

| Field | Type | Description |
|---|---|---|
| `timestamp` | string | ISO 8601 wall-clock time of the call. |
| `operation` | string | S3 operation name (e.g. `"ListObjectsV2"`, `"HeadBucket"`). |
| `profile` | string? | Vendor/profile name (e.g. `"bos"`, `"minio"`, `null`). |
| `endpoint_url` | string | Full endpoint URL used for the request. |
| `region` | string? | AWS region or vendor region. |
| `addressing_style` | string | `"path"`, `"virtual"`, or `"auto"`. |
| `bucket` | string | Target bucket name. |
| `prefix` | string | Listing prefix. |
| `delimiter` | string? | S3 delimiter (default `"/"`). |
| `start_after` | string? | `start-after` parameter if set. |
| `max_keys` | int? | `max-keys` parameter if set. |
| `continuation_token` | string? | Continuation token sent in the request. |
| `http_status` | uint16 | HTTP response status code. |
| `s3_error_code` | string? | S3 error code (e.g. `"NoSuchBucket"`). |
| `s3_error_message` | string? | Error message body. |
| `request_id` | string? | `x-amz-request-id` or equivalent header. |
| `request_id_2` | string? | `x-amz-id-2` (AWS extended request ID). |
| `retry_attempt` | uint32 | Zero-indexed retry count. |
| `latency_ms` | uint64 | Round-trip latency in milliseconds. |
| `retryable` | bool | Whether the error is classified as retryable. |
| `fatal` | bool | Whether the error is classified as fatal. |
| `is_truncated` | bool | Whether the ListObjectsV2 response was truncated. |
| `next_continuation_token` | string? | Continuation token for the next page. |
| `key_count` | int? | `KeyCount` from the ListObjectsV2 response. |
| `contents_count` | int? | Number of `Contents` entries in the response. |
| `common_prefixes_count` | int? | Number of `CommonPrefixes` entries. |
| `next_continuation_token_present` | bool? | Whether the response explicitly included a next continuation token. |
| `segment_index` | uint? | Segment index for `ListObjectsV2SegmentSummary` events. |
| `end_before` | string? | Upper segment boundary, when present. |
| `segment_pages` | uint32? | Pages read by a completed segment summary. |
| `segment_objects` | uint? | Objects emitted by a completed segment summary. |
| `segment_common_prefixes` | uint? | CommonPrefixes seen by a completed segment summary. |
| `ended_by` | string? | Segment completion reason, such as `"pagination"` or `"boundary"`. |
| `truncated_raw_body` | string? | First 512 bytes of the error response body. |

## Endpoint compatibility matrix

| Endpoint | Status | Addressing styles | Notes |
|---|---|---|---|
| **AWS S3** (`us-east-2`) | ✅ Validated | path, virtual-hosted | Full compatibility. Baseline reference. |
| **MinIO** (v2025-09-07) | ✅ Validated | path, virtual-hosted | Full compatibility. Local and remote. |
| **BOS** (Baidu Object Storage) | ✅ Validated | virtual-hosted (recommended), path (legacy/diagnostic) | See `docs/validation-results/` for endpoint-specific details. |
| **Cloudflare R2** | 📋 Profile documented | provider-specific | Use `profiles show r2`; run `compat-probe` before production use. |
| **Backblaze B2** | 📋 Profile documented | provider-specific | Use `profiles show b2`; run `compat-probe` before production use. |
| **Alibaba OSS** | 📋 Profile documented | provider-specific | Use `profiles show oss`; run `compat-probe` before production use. |

Validation details:
- [`docs/validation-results/v0.1.1-code-review-fixes-20260516.md`](docs/validation-results/v0.1.1-code-review-fixes-20260516.md)
- [`docs/validation-results/final-validation-summary-20260514.md`](docs/validation-results/final-validation-summary-20260514.md)
- [`docs/validation-results/aws-s3-baseline-20260514.md`](docs/validation-results/aws-s3-baseline-20260514.md)
- [`docs/validation-results/bos-s3-compatible-20260514.md`](docs/validation-results/bos-s3-compatible-20260514.md)
- [`docs/validation-results/bos-listobjects-v2-pagination-compatibility-note-20260514.md`](docs/validation-results/bos-listobjects-v2-pagination-compatibility-note-20260514.md)

## Known limitations

1. **Hinted multi-segment diff paired coordination is deferred.**  Diff mode
   uses authoritative single-segment listing in current releases.  Conventional
   hints caches are ignored for `diff`, and `diff --hints-file` is rejected.
   Multi-segment diff coordination across left/right segment pairs is planned
   for `v0.2.x`.  Current diff mode also retains its comparison map in memory
   until both sides complete; list-mode streaming output remains the bounded
   memory path for very large single-bucket inventories.

2. **Release build on Ubuntu 20.04 arm64 may require an `aws-lc-sys` workaround**
   documented in [`BUILD.md`](BUILD.md).  The `aws-lc-sys` crate detects a
   known GCC < 10 memcmp bug on aarch64 and aborts the build.  Workarounds:
   use clang, GCC 10+, or disable ASM.  Debug builds are unaffected.

## Validation status

All validation is complete.  The test suite passes on a clean working tree.

Validation covered three endpoints (MinIO, AWS S3, BOS) across 15 test
categories per endpoint: compat-probe, standard listing, prefix filter,
delimiter, pagination, start-after, encoding-type, continuation token,
error behaviour, checkpoint/resume, identity verification, diff mode,
Parquet output, addressing styles, and trace event completeness.

8 tool-level fixes were delivered during validation; 2 BOS-side
incompatibilities were documented.  Full details in
[`docs/validation-results/final-validation-summary-20260514.md`](docs/validation-results/final-validation-summary-20260514.md).

## Roadmap

| Priority | Item | Status |
|---|---|---|
| ✅ Done | README / docs polish | This document |
| ✅ Done | Validation (MinIO, AWS S3, BOS) | Complete |
| ✅ Done | Release packaging | v0.1.1+ multi-platform release assets published |
| ✅ Done | Release / compat hardening | Versioned workflow, checks, compat-probe, output config |
| ✅ Done | Large-run readiness | data_map batch insertion metrics, hints validation, sampled auto-hints |
| ✅ Done | Streaming readiness | list-mode streaming Parquet output, segment estimates, release/test hardening |
| ✅ Done | Benchmark harness | Local synthetic streaming-output benchmark plus JSON report |
| ✅ Done | CLI help polish | Shell completions and man page generation |
| ✅ Done | Optional endpoint compatibility profiles | Per-provider presets and local profile inspection |
| ✅ Done | Local S3 protocol mock | Local correctness harness for ListObjectsV2, compat-probe, retry, and checkpoint/resume |
| ✅ Done | Hints correctness / prefix discovery | Boundary key correctness, `--no-auto-hints`, prefix-scoped auto-hints, CommonPrefixes discovery |
| ✅ Done | Trace-driven hints tooling | Local hints merge, trace summary, conservative rebalance, and agent-readable manifests |
| ✅ Done | Beginner-friendly local UX | init-config, quickstart, recipes, cheatsheet, output-dir, doctor simple output |
| 📋 Planned | Paired-segment diff coordination | Multi-segment diff with proper per-segment DiffFlag |
| 📋 Later | Real endpoint benchmark templates | Cloud runs remain opt-in and require explicit authorization |
