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
roles). Platform-specific steps are in [INSTALL.md](INSTALL.md). Build from
source with `cargo build --release` (see
[`docs/releasing.md`](docs/releasing.md) for the Ubuntu 20.04 aarch64
workaround).

## Quick start

```bash
# Local preflight — no S3 access
s3-turbo-list doctor

# Full recursive inventory → Parquet + keyspace CSV in out/
export AWS_PROFILE=default
s3-turbo-list list --bucket my-bucket --region us-east-2 --output-dir out

# Preview the same run as a JSON plan, without contacting S3
s3-turbo-list list --bucket my-bucket --region us-east-2 --output-dir out --dry-run

# Count objects and bytes without writing files
s3-turbo-list list --bucket my-bucket --region us-east-2 --output-format summary

# Stream rows to shell tools instead of Parquet
s3-turbo-list list --bucket my-bucket --region us-east-2 \
  --output-format ndjson > objects.ndjson
```

Options follow the command name, and each command's `--help` lists only
what it takes. `--config`, `--provider`, `--endpoint-url` and
`--addressing-style` are global and may go on either side of it.

Listing is recursive by default; `--prefix logs/2026/` narrows it. Use
`--delimiter '/'` for a hierarchical listing: the objects at that level plus
one row per `CommonPrefix` ("folder"), whose `Key` ends with the delimiter and
whose `Size`/`LastModified` are 0 and `ETag` empty. Folder rows count in
`streamed_rows` but not in the KS object counts or `bytes_total`. `guide`
prints a quickstart overview and per-provider pages, and `completions`/`man`
generate shell completions and a man page.

Output files are auto-named in `--output-dir` (else the working directory):

- `<region>_<bucket>_<timestamp>.parquet` — the object listing
- `<region>_<bucket>_<timestamp>.ks` — per-prefix object counts (CSV)

A `--prefix` run adds a short hash of the prefix
(`…_<bucket>_p1a2b3c4d_<timestamp>`), and a name already taken gets a `_N`
suffix; names are reserved atomically, so concurrent runs never share one.
With `--output-parquet-file out/list.parquet` the KeySpace file is written
beside it as `out/list.ks`. `--log` writes `<name>.log` beside the outputs.

## Output

| Mode | Command | Writes |
|---|---|---|
| Parquet (default) | `list` | Parquet + KS files; streaming, bounded memory. |
| TSV | `list --output-format tsv` | `key<TAB>size<TAB>epoch` on stdout. |
| NDJSON | `list --output-format ndjson` | `{"k":…,"s":…,"m":…}` on stdout. |
| Summary | `list --output-format summary` | Aggregate metrics only (objects, bytes, top prefixes). |
| Dry run | `--dry-run` | JSON plan on stdout; no S3 requests. |

The Parquet schema is five columns:

| Column | Type | Description |
|---|---|---|
| `Key` | `Utf8` | Object key. |
| `Size` | `UInt64` | Size in bytes. |
| `LastModified` | `UInt64` | Unix timestamp (seconds). |
| `ETag` | `Utf8` | Hex MD5, with optional multipart part-count suffix. |
| `DiffFlag` | `UInt8` | `0` equal, `1` left-only, `2` right-only, `3` differs. |

In list mode every row carries `DiffFlag = 0`. The companion `.ks` file is a
two-column CSV of prefix and object count; the prefix is the key's directory
with its trailing `/` (as S3 writes a CommonPrefix), and `""` for top-level
keys.

Parquet output parallelizes itself: on a fast (non-rate-limited) store and a
multi-core machine, when one writer can't keep up it automatically scales to
several writers, each streaming a part-file (`<name>.part1.parquet`, …) beside
`<name>.parquet`. Read the base file plus its parts — the run manifest lists
each one as a `parquet` artifact, or glob `<name>*.parquet` — rather than the
whole directory, which also holds the `.ks` CSV and other runs' files. A run
removes stale part files an earlier run of the same path left behind. On a
rate-limited store it stays a single file. No flag; details in
[`docs/tuning.md`](docs/tuning.md).

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

In diff, the filter applies to every row, including the one-sided `+`/`-`
rows for keys present on only one side. Such a row binds the side that exists
to `SOURCE`; a predicate naming `TARGET` cannot be evaluated for it and keeps
the row, so `SOURCE.size != TARGET.size` still reports every one-sided
difference. Rows the filter excludes are counted as `ignored`.

The filter is part of a run's identity: `--resume` rejects a checkpoint
written under a different `--filter` rather than splicing two differently
filtered populations into one output file, and the run manifest records the
expression under `inputs.filter`.

```bash
s3-turbo-list list --bucket my-bucket --region us-east-2 \
  --filter 'SOURCE.size > 1073741824'
s3-turbo-list diff --bucket left-bucket --region us-east-1 \
  --target-bucket right-bucket --target-region us-east-1 \
  --filter 'SOURCE.size != TARGET.size'
```

## Performance

Recursive list runs parallelize across key-space segments. Boundaries come
from, in precedence order:

1. `--hints-file` — pins exact boundaries for repeated inventories.
2. **Startup structural discovery** (automatic, every run) — a handful of
   delimiter probes find real `CommonPrefixes` boundaries. Every run is
   parallel with zero flags; nothing is cached in the working directory.
3. **Startup bisection** (automatic) — a flat namespace with no
   `CommonPrefixes` is partitioned by single-key probes instead, so it also
   starts parallel. Runtime splitting still covers mid-run skew.
4. A single segment for listings that fit in one page (nothing to partition)
   and for `--start-after` and `--delimiter` runs.

Segments also **split at runtime**: when one segment turns out to hold most of
the data, the run probes its remaining range and fans it across idle workers —
using `CommonPrefixes` boundaries where the range has structure, and
single-key probes near the middle of the remaining keys where it is flat.
Fan-out is throughput-aware:
it stops adding segments once a bucket is at its request-rate ceiling, so
`--concurrency` acts as an upper bound rather than a target. Hints formats,
boundary semantics, and tuning knobs (including validating a hints file with
`doctor --hints-file`) are in [`docs/tuning.md`](docs/tuning.md).

## Diff

```bash
s3-turbo-list diff --bucket source-bucket --region us-east-2 \
  --target-bucket target-bucket --target-region us-west-2 --output-dir out
```

Diff lists both buckets, partitioning and listing each side's segments in
parallel, then merges the outputs in key order and streams one Parquet with a
`DiffFlag` per object (equal rows included; filter `DiffFlag != 0`
downstream). Memory stays bounded regardless of bucket size, and any ordering
violation or segment failure fails the run loudly rather than producing a wrong
diff. `--target-region` defaults to `--region`. Diff takes no `--hints-file`
or `--resume`.

## Checkpoint / resume

```bash
s3-turbo-list list --bucket my-bucket --region us-east-2 --output-dir out            # interrupted (Ctrl-C / SIGTERM, exit 7)…
s3-turbo-list list --bucket my-bucket --region us-east-2 --output-dir out --resume   # …lists only what is left
```

Every interrupted `list` run saves a checkpoint; `--resume` only reads it.
The checkpoint is `<region>_<bucket>[_<prefix-hash>]_checkpoint.toml` in
`--output-dir` when one is given (else the working directory), so resume with
the same `--output-dir`. It is saved when the run is interrupted gracefully,
after the outputs are finalized, and records exactly the key ranges not yet
written — each unfinished segment (including ones split at runtime) is cut at
its last key whose rows reached the output — so the resumed run lists those
ranges and nothing else. There are no mid-run saves: a crash or failed write
leaves the previous checkpoint unchanged. If the checkpoint cannot be written,
the run says so and the exit line does not promise a resume. `diff` and
`--start-after` runs do not checkpoint.

The checkpoint identity covers bucket, region, endpoint, prefix, delimiter,
max-keys, addressing style, provider, mode and filter; a checkpoint written
for another identity is ignored with a warning, and is never deleted or
overwritten by a run it does not belong to. Checkpoints written before 0.36
are discarded with a warning and the run starts over. A run that lists its
whole key space removes its checkpoint on the way out.

**A resumed run's output covers only the ranges it listed.** The rest are in
the output of the run that was interrupted — combine the two (auto-named
outputs get fresh names, so neither overwrites the other; pointing both runs
at the same `--output-parquet-file` leaves only the second run's part). A
resumed run says so on stderr, and the run manifest records it under
`checkpoint.resumed_segments_skipped`.

## Providers

Works against any S3-compatible endpoint via `--endpoint-url` and
`--addressing-style`. Optional `--provider` presets fill safe endpoint and
addressing defaults for common providers — they never touch credentials. Use
`AWS_PROFILE` for credentials; `--provider` selects an *endpoint* preset only.

```bash
s3-turbo-list guide oss              # provider quickstart, local only

# Region-derived endpoints need no --endpoint-url:
s3-turbo-list --provider oss list --bucket my-bucket --region oss-cn-beijing --output-dir out

# Deployment-specific endpoints stay explicit:
s3-turbo-list --provider minio --endpoint-url http://localhost:9000 \
  list --bucket my-bucket --region us-east-1 --output-dir out
```

Built-in presets: `aws`, `minio`, `bos`, `r2`, `b2`, `oss`. Presets, the
config file (for a custom endpoint you use repeatedly), and the
`compat-probe` report are in [`docs/providers.md`](docs/providers.md).

| Endpoint | Status |
|---|---|
| AWS S3 | ✅ Validated (path + virtual-hosted) |
| MinIO | ✅ Validated (path + virtual-hosted) |
| BOS | ✅ Validated (virtual-hosted recommended; hinted multi-segment supported) |
| Cloudflare R2 / Backblaze B2 / Alibaba OSS | 📋 Preset documented; run `compat-probe` first |

Behind an HTTP proxy, the standard `HTTPS_PROXY` / `HTTP_PROXY` /
`ALL_PROXY` / `NO_PROXY` variables apply, as with curl and the AWS CLI. A
local endpoint (for example MinIO on `http://localhost:9000`) goes through
`HTTP_PROXY` too unless its host is in `NO_PROXY`. `doctor` reports whether a
proxy applies to an explicit path-style endpoint, and every run names the proxy
it uses for each side's resolved endpoint before its first request.

Validate any endpoint before a full run:

```bash
s3-turbo-list compat-probe --endpoint-url https://endpoint --bucket my-bucket
```

## Automation

For CI and agents, every surface has a machine-readable form:

- `--dry-run` prints a JSON plan without contacting S3 (`> plan.json` to
  keep it).
- `doctor --json` reports environment and resolved config; add `--hints-file`
  to lint a hints file.
- `--run-manifest run.json` records artifacts with SHA256 and Parquet
  row/schema metadata, and `--agent` prints the manifest on stdout;
  `manifest-summary run.json --check` verifies a completed run locally.
- `--trace-compat trace.jsonl` records every S3 API call as JSONL
  (`--trace-compat -` for stderr; fields in
  [`docs/tuning.md`](docs/tuning.md#trace-event-fields)).

Exit-code classes are stable. Full reference:
[`docs/agent-usage.md`](docs/agent-usage.md).

## Known limitations

1. **Diff segments are static** — each side is partitioned up front (structured
   sides by discovery, flat sides by a single-key bisection) and not re-split
   mid-run, so a segment that turns out skewed cannot rebalance the way list
   mode does. List mode remains the fastest path for one-bucket inventories.
2. **Release builds on Ubuntu 20.04 arm64** may need the `aws-lc-sys`
   workaround in [`docs/releasing.md`](docs/releasing.md).

## Project principles

One binary; list and diff done extremely fast, with observability and
automation hooks. The CLI surface is intentionally small — new subcommands or
global flags need an exceptional case (see [CONTRIBUTING.md](CONTRIBUTING.md)).
Performance work targets the default path, not new knobs.

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). Releases through
v0.17 were published under the MIT license.
