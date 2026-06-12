# s3-turbo-list

High-performance S3-compatible bucket listing, diffing, checkpoint/resume,
and Parquet export for large object stores.

s3-turbo-list scans S3-compatible buckets much faster than a sequential
`aws s3 ls` by partitioning the key space into segments and listing them
concurrently. On first run it probes the bucket's prefix structure
automatically, so parallel listing works out of the box — no tuning, no
prior steps.

**What it solves:**

- **Slow listings** — concurrent segmented listing beats sequential scans by
  an order of magnitude on large buckets.
- **Unreliable long runs** — checkpoint/resume picks an interrupted scan up
  where it left off.
- **Opaque diffing** — bi-directional diff with per-object `DiffFlag`
  annotations in Parquet.
- **Provider uncertainty** — `compat-probe` and trace JSONL show exactly how
  an endpoint behaves before you commit to it.
- **Ad-hoc analysis** — Parquet output drops directly into pandas or duckdb
  with zero parsing.

## Installation

Download the binary for your platform from the GitHub release, verify it with
`SHA256SUMS`, install it into your `PATH`, then configure credentials through
the standard AWS SDK credential chain.

See [INSTALL.md](INSTALL.md) for platform-specific installation and
AWS S3 / MinIO / BOS configuration examples.

## Quick start

```bash
# Local preflight (no S3 access)
s3-turbo-list doctor --local-only --simple

# Full recursive bucket inventory → Parquet + keyspace CSV in out/
export AWS_PROFILE=default
s3-turbo-list --output-dir out \
  list --region us-east-2 --bucket my-bucket

# Count objects and bytes without writing files
s3-turbo-list --summary-only \
  list --region us-east-2 --bucket my-bucket

# Stream rows to shell tools instead of Parquet
s3-turbo-list list --region us-east-2 --bucket my-bucket \
  --output-format ndjson > objects.ndjson
```

Listing is recursive by default — the high-performance path: the first run
probes the bucket's prefix structure, caches the discovered key-space
boundaries, and lists the segments in parallel. Use `--delimiter '/'` for a
hierarchical listing (top-level objects plus `CommonPrefixes` only).

Preview any run without contacting S3 by adding `--dry-run --agent`.
`init-config`, `quickstart`, `recipes`, and `cheatsheet` provide local help;
`completions` and `man` generate shell completions and a man page.

Output files (auto-named):

- `<region>_<bucket>_<timestamp>.parquet` — object listing
- `<region>_<bucket>_<timestamp>.ks` — per-prefix object counts (CSV)

### Build from source

```bash
cargo build --release   # see BUILD.md for the Ubuntu 20.04 aarch64 workaround
```

## Output modes

| Mode | Command | Writes |
|---|---|---|
| Parquet (default) | `list` | Parquet + KS files; bounded memory, streaming. |
| TSV | `list --output-format tsv` | `key<TAB>size<TAB>epoch` rows on stdout. |
| NDJSON | `list --output-format ndjson` | `{"k":…,"s":…,"m":…}` rows on stdout. |
| Summary | `--summary-only` | Aggregate metrics only (objects, bytes, top prefixes). |
| Dry run | `--dry-run` | Plan only; no S3 requests. |

### Parquet schema

| Column | Type | Description |
|---|---|---|
| `Key` | `Utf8` | Object key (full path). |
| `Size` | `UInt64` | Object size in bytes. |
| `LastModified` | `UInt64` | Unix timestamp (seconds). |
| `ETag` | `Utf8` | Hex MD5, optional multipart part-count suffix. |
| `DiffFlag` | `UInt8` | `0` equal, `1` left-only, `2` right-only, `3` differs. |

In list mode (single bucket), all rows carry `DiffFlag = 0`. The companion
`.ks` file is a two-column CSV of prefix and object count.

```python
import pyarrow.parquet as pq
df = pq.read_table("us-east-2_my-bucket_20260514120000.parquet").to_pandas()
```

## Object filters

`--filter` applies a local filter after listing and before output (it does not
reduce S3 requests; use `--prefix` for request-side shaping). The expression
language is deliberately tiny: `SOURCE`/`TARGET` (diff only), properties
`size` and `last_modified`, numeric comparison, arithmetic, `&&`, `||`, `!`.
Invalid filters are rejected before any S3 request with exit code `2`.

```bash
s3-turbo-list --filter 'SOURCE.size > 1073741824' \
  list --region us-east-2 --bucket my-bucket
s3-turbo-list --filter 'SOURCE.size != TARGET.size' \
  diff --bucket left-bucket --target-bucket right-bucket
```

## Performance and hints

Recursive list runs (the default) parallelise across key-space segments. The
boundary sources, in precedence order:

1. `--hints-file` — explicit control for repeated inventories.
2. The conventional hints cache (`<region>_<bucket>_hints.toml`) written by
   `auto-hints` or by a previous run's startup discovery.
3. **Startup structural discovery** (automatic): a handful of delimiter
   probes at run start find real `CommonPrefixes` boundaries and cache them.
   First runs are parallel with zero flags.
4. Single-segment fallback for flat namespaces with no `/` structure.

Segments also **split at runtime**: when one segment turns out to hold most
of the data, the run probes its remaining range and fans it out across idle
workers automatically. Skewed buckets no longer serialize behind their
largest prefix.

For precise object-count-balanced segments on very large buckets, generate
hints explicitly:

```bash
s3-turbo-list auto-hints --region us-east-2 --bucket my-bucket -o hints.toml
s3-turbo-list --hints-file hints.toml \
  list --region us-east-2 --bucket my-bucket
```

Hints file formats, boundary semantics, advanced generation workflows
(`discover-prefixes`, `hints-validate`, `hints-merge`), and all runtime
tuning knobs are documented in [`docs/tuning.md`](docs/tuning.md).

## Diff mode

```bash
s3-turbo-list diff \
  --region us-east-2 --bucket source-bucket \
  --target-region us-west-2 --target-bucket target-bucket
```

Diff lists both buckets and writes one Parquet with `DiffFlag` per object
(equal rows included; filter `DiffFlag != 0` downstream). Diff is
authoritative single-segment by design — hints are ignored and
`--hints-file`/`--resume` are rejected — so left-only and right-only objects
cannot be hidden by mismatched segment boundaries. The two ordered streams
are merged on the fly and rows stream straight to Parquet, so memory stays
bounded regardless of bucket size.

## Checkpoint / resume

```bash
s3-turbo-list list --region us-east-2 --bucket my-bucket            # interrupted…
s3-turbo-list list --region us-east-2 --bucket my-bucket --resume   # picks up
```

Progress is saved every 30 seconds and on graceful shutdown. Checkpoint
identity covers bucket, region, prefix, delimiter, max-keys, addressing
style, profile, mode, and segment count — a mismatched checkpoint is
discarded with a warning rather than silently mis-resuming.

## Providers

Works against any S3-compatible endpoint via `--endpoint-url` and
`--addressing-style`. Optional presets exist for common providers:

```bash
s3-turbo-list profiles list
s3-turbo-list list --bucket my-bucket \
  --endpoint-url http://localhost:9000 --profile minio
```

Built-in profiles: `aws`, `minio`, `bos`, `r2`, `b2`, `oss`. Profiles only
fill safe defaults (endpoint, addressing style) when you haven't supplied
explicit values. Use `AWS_PROFILE` for credentials; `--profile` is only the
endpoint preset. Details and per-provider notes (including BOS
virtual-hosted addressing guidance):
[`docs/endpoint-profiles.md`](docs/endpoint-profiles.md).

| Endpoint | Status |
|---|---|
| AWS S3 | ✅ Validated (path + virtual-hosted) |
| MinIO | ✅ Validated (path + virtual-hosted) |
| BOS | ✅ Validated (virtual-hosted recommended) |
| Cloudflare R2 / Backblaze B2 / Alibaba OSS | 📋 Profile documented; run `compat-probe` first |

Validate any endpoint before full-scale work:

```bash
s3-turbo-list compat-probe --endpoint https://endpoint --region r --bucket b
```

Full validation reports live in
[`docs/validation-results/`](docs/validation-results/).

## Observability and automation

Every S3 API call can be recorded as structured JSONL:

```bash
s3-turbo-list list --region us-east-2 --bucket my-bucket \
  --trace-compat trace.jsonl
s3-turbo-list trace-summary trace.jsonl --output-format json
```

Field-by-field trace schema: [`docs/trace-reference.md`](docs/trace-reference.md).

For CI and agents: `--dry-run --agent` emits a JSON plan without contacting
S3; `--run-manifest run.json` records artifacts with SHA256 and Parquet
row/schema metadata; `manifest-summary run.json --check` verifies a completed
run locally. Exit-code classes are stable. Full reference:
[`docs/agent-usage.md`](docs/agent-usage.md).

## Known limitations

1. **Diff is authoritative single-segment by design** (see Diff mode
   above); each side follows one ListObjectsV2 chain.
2. **Release builds on Ubuntu 20.04 arm64** may need the `aws-lc-sys`
   workaround documented in [BUILD.md](BUILD.md).

## Project principles

The product stays small: one binary, list and diff done extremely fast, with
observability and automation hooks. The CLI surface is frozen — new
subcommands or global flags need an exceptional case (see
[CONTRIBUTING.md](CONTRIBUTING.md)). Performance work targets the default
path, not new knobs.
