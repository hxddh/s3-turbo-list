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
| **Parquet output** | Results written to compressed Parquet (gzip level 6) — drop into any analytics tool. |
| **KS keyspace output** | Companion CSV showing per-prefix object counts. |
| **Trace JSONL output** | Every S3 API call recorded as structured JSONL for observability and debugging. |
| **Compat-probe** | Quick validation of any S3-compatible endpoint before full-scale work. |
| **Checkpoint / resume** | Interrupted scans resume from last-saved segment. Identity-verified to prevent mismatches. |
| **Diff mode** | Bi-directional diff between two buckets with per-object DiffFlag. Equal rows optionally included. |
| **Endpoint validation workflow** | Compat-probe → listing → trace review. Documented in `docs/validation-results/`. |

## Installation

Download the binary for your platform from the GitHub release, verify it with `SHA256SUMS`, install it into your `PATH`, then configure an AWS-compatible profile.

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

### BOS (Baidu Object Storage)

BOS recommends bucket virtual hosting (`bucket.s3.<region>.bcebos.com`).
s3-turbo-list supports this with virtual-hosted addressing:

```bash
# Virtual-hosted (recommended for BOS)
s3-turbo-list list --region bj --bucket my-bos-bucket \
  --endpoint-url https://s3.bj.bcebos.com \
  --addressing-style virtual
```

> **Note on `--profile bos`:** The built-in `bos` profile preset currently
> defaults to path-style addressing for legacy compatibility.  Override it
> with `--addressing-style virtual` to use the recommended virtual-hosted
> mode:
>
> ```bash
> s3-turbo-list list --region bj --bucket my-bos-bucket \
>   --profile bos --addressing-style virtual
> ```

Path-style is supported but intended only for legacy compatibility,
diagnostics, or controlled validation:

```bash
# Path-style (legacy / diagnostic only)
s3-turbo-list list --region bj --bucket my-bos-bucket \
  --profile bos
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

Trace output is one JSON object per line (JSONL). Each line contains:
operation, endpoint, addressing style, profile, region, bucket, prefix,
delimiter, max-keys, start-after, continuation token, HTTP status, S3
error code, request ID, latency, pagination details, and truncated error
body excerpts (first 512 bytes).

### Diff mode

```bash
# Compare two buckets
s3-turbo-list diff \
  --region us-east-2 --bucket source-bucket \
  --target-region us-west-2 --target-bucket target-bucket

# Include equal rows in output
# (controlled by internal config; equal rows default to included)

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
```

Generate hints automatically:
```bash
s3-turbo-list auto-hints --region us-east-2 --bucket my-bucket -o hints.toml
```

Use a hints file:
```bash
s3-turbo-list list --region us-east-2 --bucket my-bucket -H hints.toml
```

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

Validation details:
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

All validation is complete. 107/107 tests pass on a clean working tree.

Validation covered three endpoints (MinIO, AWS S3, BOS) across 13+ test
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
| ✅ Done | Validation (MinIO, AWS S3, BOS) | Complete — 107/107 tests |
| 🔜 Next | Release packaging | Cross-compile static binary; publish |
| 🔜 Next | Benchmark harness | Throughput benchmarks across endpoints |
| 🔜 Next | CLI help polish | Expanded --help, man page, shell completions |
| 📋 Planned | Paired-segment diff coordination | Multi-segment diff with proper per-segment DiffFlag |
| 📋 Later | Optional endpoint compatibility profiles | Per-provider presets (Cloudflare R2, Backblaze B2, etc.) |
