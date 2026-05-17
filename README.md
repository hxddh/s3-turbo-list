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
| **Streaming Parquet output** | List mode writes received batches directly to configurable compressed Parquet, keeping memory bounded for large buckets. |
| **KS keyspace output** | Companion CSV showing per-prefix object counts. |
| **Trace JSONL output** | Every S3 API call recorded as structured JSONL for observability and debugging. |
| **Compat-probe** | Quick validation of any S3-compatible endpoint before full-scale work. |
| **Checkpoint / resume** | Interrupted scans resume from last-saved segment. Identity-verified to prevent mismatches. |
| **Diff mode** | Bi-directional diff between two buckets with per-object DiffFlag. Equal rows optionally included. |
| **Agent-friendly JSON** | Local dry-run plans, config inspection, doctor checks, stable exit codes, and run manifests for automation. |
| **Endpoint validation workflow** | Compat-probe → listing → trace review. Documented in `docs/validation-results/`. |

## Installation

Download the binary for your platform from the GitHub release, verify it with
`SHA256SUMS`, install it into your `PATH`, then configure AWS-compatible
credentials through the standard AWS SDK credential chain.

See [INSTALL.md](INSTALL.md) for platform-specific installation and AWS S3 / BOS / MinIO configuration examples.

## Quick start

### Build

```bash
cargo build

# Release build (see BUILD.md for aarch64 workaround)
cargo build --release
```

### Basic list command

```bash
# List a bucket with default settings
cargo run -- list --region us-east-2 --bucket my-bucket

# Limit parallelism
cargo run -- list --region us-east-2 --bucket my-bucket --prefix logs/ -T 4 -c 20
```

Output files (auto-named):
- `<region>_<bucket>_<timestamp>.parquet` — object listing
- `<region>_<bucket>_<timestamp>.ks` — keyspace counts

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
s3-turbo-list list --region us-east-2 --bucket my-bucket

# With virtual-hosted addressing
s3-turbo-list list --region us-east-2 --bucket my-bucket \
  --addressing-style virtual

# Enable trace
s3-turbo-list list --region us-east-2 --bucket my-bucket \
  --trace-compat trace.jsonl
```

### MinIO

```bash
# Local MinIO
s3-turbo-list list --bucket my-bucket \
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
s3-turbo-list list --bucket my-bucket \
  --endpoint-url https://<account-id>.r2.cloudflarestorage.com \
  --region auto \
  --profile r2
```

Built-in profiles: `aws`, `minio`, `bos`, `r2`, `b2`, and `oss`.
Profiles only fill safe defaults such as addressing style or documented
endpoint defaults when the user has not supplied an explicit value.  They do
not change output schema, diff semantics, or pagination behavior.

### BOS (Baidu Object Storage)

BOS recommends bucket virtual hosting (`bucket.s3.<region>.bcebos.com`).
s3-turbo-list supports this with virtual-hosted addressing:

```bash
# Virtual-hosted (recommended for BOS)
s3-turbo-list list --region bj --bucket my-bos-bucket \
  --profile bos
```

The built-in `bos` profile preset uses the BOS S3-compatible endpoint and
virtual-hosted addressing.  You can still pass `--endpoint-url` explicitly
when validating a specific regional endpoint.

Path-style is supported but intended only for legacy compatibility,
diagnostics, or controlled validation:

```bash
# Path-style (legacy / diagnostic only)
s3-turbo-list list --region bj --bucket my-bos-bucket \
  --profile bos \
  --addressing-style path
```

### Trace / debug

```bash
# Write trace to file
s3-turbo-list list --region us-east-2 --bucket my-bucket \
  --trace-compat trace.jsonl

# Stream trace events to stderr
s3-turbo-list list --region us-east-2 --bucket my-bucket \
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
error code, request ID, latency, pagination details, and truncated error
body excerpts (first 512 bytes).

### Local benchmark and CLI docs

```bash
# Synthetic local streaming-output benchmark; does not contact S3
s3-turbo-list benchmark-local --objects 100000 --batch-size 5000 --json

# Shell completions and man page
s3-turbo-list completions bash > s3-turbo-list.bash
s3-turbo-list completions zsh > _s3-turbo-list
s3-turbo-list man > s3-turbo-list.1
```

The helper script `scripts/benchmark-local.sh` writes a machine-readable
local benchmark report.  See [`docs/benchmarking.md`](docs/benchmarking.md)
and [`docs/endpoint-profiles.md`](docs/endpoint-profiles.md).

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

For very large buckets, generate an estimated hints file from a bounded sample:
```bash
s3-turbo-list auto-hints --region us-east-2 --bucket my-bucket \
  -o hints.sampled.toml \
  --sample-limit 1000000 \
  --max-pages 1000
```

In sampled mode, the TOML `total_objects` field is the sampled object count,
not the full bucket total.  The cache also records `scan_mode`,
`sampled_objects`, `sampled_pages`, `sample_limit`, `max_pages`, and
per-segment estimates marked as sampled/estimated rather than authoritative
bucket-wide statistics.

Validate a hints file locally before a cloud run:
```bash
s3-turbo-list hints-validate --hints-file hints.toml
```

Use a hints file:
```bash
s3-turbo-list list --region us-east-2 --bucket my-bucket -H hints.toml
```

Advanced runtime and auto-hints tuning is documented in
[`docs/tuning.md`](docs/tuning.md).  Some knobs, including
`auto_hints.sample_threshold`, `auto_hints.max_prefix_depth`, and
`auto_hints.min_segment_size`, are TOML-only settings and do not have CLI
flags.

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
   has been validated for single-segment operations (no hints).  Multi-segment
   diff coordination across left/right segment pairs is planned but not yet
   implemented.

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
| 📋 Planned | Paired-segment diff coordination | Multi-segment diff with proper per-segment DiffFlag |
| 📋 Later | Real endpoint benchmark templates | Cloud runs remain opt-in and require explicit authorization |
