# s3-turbo-list

High-performance listing, diffing, and Parquet export for large
S3-compatible buckets.

`s3-turbo-list` lists a bucket far faster than a sequential `aws s3 ls` by
splitting the key space into segments and listing them concurrently. The
first run probes the bucket's prefix structure automatically, so parallel
listing works with no flags and no prior setup. A third-party benchmark on
Alibaba Cloud OSS (1M objects) measured a single-stream listing at ~55s and
the same bucket at `-c 8` in ~19s.

## What it does

- **Fast listings** — concurrent segmented listing, several-fold to an order
  of magnitude faster than a sequential scan, depending on bucket structure
  and the provider's request-rate limit.
- **Diffing** — bi-directional bucket diff with a per-object `DiffFlag` in
  Parquet output.
- **Reliable long runs** — checkpoint/resume continues an interrupted scan.
- **Provider validation** — `compat-probe` and trace JSONL show exactly how an
  S3-compatible endpoint behaves before you commit to it.
- **Analysis-ready output** — Parquet drops straight into pandas or duckdb.

## Install

Download the binary for your platform from the
[GitHub release](https://github.com/hxddh/s3-turbo-list/releases), verify it
against `SHA256SUMS`, and put it on your `PATH`. Credentials come from the
standard AWS SDK chain (`AWS_PROFILE`, environment variables, or instance
roles). Platform-specific steps and provider configuration are in
[INSTALL.md](INSTALL.md). Build from source with `cargo build --release`
(see [BUILD.md](BUILD.md) for the Ubuntu 20.04 aarch64 workaround).

## Quick start

```bash
# Local preflight — no S3 access
s3-turbo-list doctor --simple

# Full recursive inventory → Parquet + keyspace CSV in out/
export AWS_PROFILE=default
s3-turbo-list --output-dir out list --region us-east-2 --bucket my-bucket

# Count objects and bytes without writing files
s3-turbo-list --summary-only list --region us-east-2 --bucket my-bucket

# Stream rows to shell tools instead of Parquet
s3-turbo-list list --region us-east-2 --bucket my-bucket \
  --output-format ndjson > objects.ndjson
```

Listing is recursive by default. Use `--delimiter '/'` for a hierarchical
listing (top-level objects plus `CommonPrefixes`). Preview any run without
contacting S3 by adding `--dry-run --agent`. `guide` prints command examples,
`init-config` writes a starter config, and `completions`/`man` generate shell
completions and a man page.

Output files are auto-named:

- `<region>_<bucket>_<timestamp>.parquet` — the object listing
- `<region>_<bucket>_<timestamp>.ks` — per-prefix object counts (CSV)

## Output

| Mode | Command | Writes |
|---|---|---|
| Parquet (default) | `list` | Parquet + KS files; streaming, bounded memory. |
| TSV | `list --output-format tsv` | `key<TAB>size<TAB>epoch` on stdout. |
| NDJSON | `list --output-format ndjson` | `{"k":…,"s":…,"m":…}` on stdout. |
| Summary | `--summary-only` | Aggregate metrics only (objects, bytes, top prefixes). |
| Dry run | `--dry-run` | Plan only; no S3 requests. |

The Parquet schema is five columns:

| Column | Type | Description |
|---|---|---|
| `Key` | `Utf8` | Object key. |
| `Size` | `UInt64` | Size in bytes. |
| `LastModified` | `UInt64` | Unix timestamp (seconds). |
| `ETag` | `Utf8` | Hex MD5, with optional multipart part-count suffix. |
| `DiffFlag` | `UInt8` | `0` equal, `1` left-only, `2` right-only, `3` differs. |

In list mode every row carries `DiffFlag = 0`. The companion `.ks` file is a
two-column CSV of prefix and object count.

Parquet output parallelizes itself: on a fast (non-rate-limited) store and a
multi-core machine, when one writer can't keep up it automatically scales to
several writers, each streaming a part-file (`<name>.part1.parquet`, …) — read
the directory with pandas/duckdb/pyarrow. On a rate-limited store it stays a
single file. No flag; details in [`docs/tuning.md`](docs/tuning.md).

```python
import pyarrow.parquet as pq
df = pq.read_table("us-east-2_my-bucket_20260514120000.parquet").to_pandas()
```

## Filters

`--filter` applies locally after listing, before output (it does not reduce S3
requests — use `--prefix` for that). The language is deliberately small:
`SOURCE`/`TARGET` (diff only), properties `size` and `last_modified`, numeric
comparison and arithmetic, and `&&` `||` `!`. An invalid filter is rejected
before any S3 request with exit code `2`.

```bash
s3-turbo-list --filter 'SOURCE.size > 1073741824' \
  list --region us-east-2 --bucket my-bucket
s3-turbo-list --filter 'SOURCE.size != TARGET.size' \
  diff --bucket left-bucket --target-bucket right-bucket
```

## Performance

Recursive list runs parallelize across key-space segments. Boundaries come
from, in precedence order:

1. `--hints-file` — explicit control for repeated inventories.
2. The conventional hints cache (`<region>_<bucket>_hints.toml`) written by a
   previous run.
3. **Startup structural discovery** (automatic) — a handful of delimiter
   probes find real `CommonPrefixes` boundaries and cache them. First runs are
   parallel with zero flags.
4. A single segment for flat namespaces; runtime splitting then fans it out.

Segments also **split at runtime**: when one segment turns out to hold most of
the data, the run probes its remaining range and fans it across idle workers —
using `CommonPrefixes` boundaries where the range has structure, and
cursor-derived single-key probes where it is flat. Fan-out is throughput-aware:
it stops adding segments once a bucket is at its request-rate ceiling, so
`--concurrency` acts as an upper bound rather than a target. Hints formats,
boundary semantics, and tuning knobs (including validating a hints file with
`doctor --hints-file`) are in [`docs/tuning.md`](docs/tuning.md).

## Diff

```bash
s3-turbo-list diff \
  --region us-east-2 --bucket source-bucket \
  --target-region us-west-2 --target-bucket target-bucket
```

Diff lists both buckets, partitioning and listing each side's segments in
parallel, then merges the outputs in key order and streams one Parquet with a
`DiffFlag` per object (equal rows included; filter `DiffFlag != 0`
downstream). Memory stays bounded regardless of bucket size, and any ordering
violation or segment failure fails the run loudly rather than producing a wrong
diff. `--hints-file` and `--resume` are rejected for diff.

## Checkpoint / resume

```bash
s3-turbo-list list --region us-east-2 --bucket my-bucket            # interrupted…
s3-turbo-list list --region us-east-2 --bucket my-bucket --resume   # picks up
```

Progress is saved every 30 seconds and on graceful shutdown. The checkpoint
identity covers bucket, region, prefix, delimiter, max-keys, addressing style,
profile, mode, and segment count — a mismatched checkpoint is discarded with a
warning rather than mis-resuming.

## Providers

Works against any S3-compatible endpoint via `--endpoint-url` and
`--addressing-style`. Optional presets fill safe endpoint and addressing
defaults for common providers — they never touch credentials. Use
`AWS_PROFILE` for credentials; `--profile` selects an *endpoint* preset only.

```bash
s3-turbo-list guide oss              # quickstart + endpoint-compatibility facts

# Region-derived endpoints need no --endpoint-url:
s3-turbo-list --profile oss list --region oss-cn-beijing --bucket my-bucket

# Deployment-specific endpoints stay explicit:
s3-turbo-list --profile minio --endpoint-url http://localhost:9000 \
  list --bucket my-bucket
```

Built-in presets: `aws`, `minio`, `bos`, `r2`, `b2`, `oss`. Per-provider notes
(including BOS addressing guidance) are in
[`docs/endpoint-profiles.md`](docs/endpoint-profiles.md).

| Endpoint | Status |
|---|---|
| AWS S3 | ✅ Validated (path + virtual-hosted) |
| MinIO | ✅ Validated (path + virtual-hosted) |
| BOS | ✅ Validated (virtual-hosted recommended) |
| Cloudflare R2 / Backblaze B2 / Alibaba OSS | 📋 Preset documented; run `compat-probe` first |

Validate any endpoint before a full run:

```bash
s3-turbo-list --endpoint-url https://endpoint compat-probe --region r --bucket b
```

## Automation

For CI and agents, every surface has a machine-readable form:

- `--dry-run --agent` emits a JSON plan without contacting S3.
- `doctor --json` reports environment and resolved config; add `--hints-file`
  to lint a hints file.
- `--run-manifest run.json` records artifacts with SHA256 and Parquet
  row/schema metadata; `manifest-summary run.json --check` verifies a completed
  run locally.
- `--trace-compat trace.jsonl` records every S3 API call as JSONL
  ([`docs/trace-reference.md`](docs/trace-reference.md)).

Exit-code classes are stable. Full reference:
[`docs/agent-usage.md`](docs/agent-usage.md).

## Known limitations

1. **Diff segments are static** — each side is partitioned up front (structured
   sides by discovery, flat sides by a single-key bisection) and not re-split
   mid-run, so a segment that turns out skewed cannot rebalance the way list
   mode does. List mode remains the fastest path for one-bucket inventories.
2. **Release builds on Ubuntu 20.04 arm64** may need the `aws-lc-sys`
   workaround in [BUILD.md](BUILD.md).

## Project principles

One binary; list and diff done extremely fast, with observability and
automation hooks. The CLI surface is intentionally small — new subcommands or
global flags need an exceptional case (see [CONTRIBUTING.md](CONTRIBUTING.md)).
Performance work targets the default path, not new knobs.
