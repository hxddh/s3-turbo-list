# Tuning Reference

This page lists runtime defaults and advanced configuration knobs that matter
for large listings.  Values come from `src/config.rs`.

## How segmented listing works

Parallelism happens between key-space segments, not inside a single
ListObjectsV2 continuation chain.  `--concurrency` only helps when there are
enough segment boundaries to keep workers busy.

Where boundaries come from, in precedence order:

1. **Explicit `--hints-file`** — full control for repeated inventories.
2. **Cached hints** at the conventional path
   (`<region>_<bucket>_hints.toml`), written by `auto-hints` or by startup
   discovery on a previous run.
3. **Startup structural discovery** (recursive list runs — the default;
   hierarchical `--delimiter '/'` runs skip it) —
   a bounded set of delimiter probes (one ListObjectsV2 page each, at most
   3 levels deep) finds real `CommonPrefixes` boundaries at run start and
   caches them at the conventional path.  Costs at most a second or two of
   startup; first runs list in parallel with no prior steps.
4. **Single segment** — flat namespaces with no `/` structure, runs with
   `--no-auto-hints`, `--start-after`, or `--continuation-token`, and `diff`
   (always single-segment by design).

Boundaries are also adjusted **at runtime**: when a list run has idle
concurrency and one segment proves to be a long tail, a delimiter probe on
the segment's remaining range finds a real `CommonPrefixes` boundary and the
segment splits cooperatively — the right half becomes a new parallel child
segment, recursively.  Skewed buckets no longer serialize behind their
largest prefix.  Splitting never applies to `diff`, `--start-after`, or
`--continuation-token` runs, and ranges without prefix structure simply keep
running unsplit.  Split segments conservatively do not record checkpoint
progress, so `--resume` re-lists the original segment.

Hints boundaries are lexicographic cut points, not directories.  A boundary
may also be a real object key; it is treated as part of the preceding segment
so adjacent `start-after` segments do not drop it.  Folder marker objects such
as `logs/` are ordinary keys for correctness purposes.

## Hints files

Two formats are accepted by `--hints-file` and the conventional cache path:

**Plain text** (one boundary per line):

```
alpha/
beta/
logs/
```

**TOML** (written by `auto-hints`, `discover-prefixes --toml`, startup
discovery, and `hints-merge`):

```toml
bucket = "my-bucket"
region = "us-east-2"
total_objects = 50000
boundaries = ["alpha/", "beta/", "logs/"]
generated_at = "2026-05-14T12:00:00Z"
scan_mode = "full"
```

Generation workflows:

```bash
# Object-count-aware hints from a full or sampled sequential scan
s3-turbo-list auto-hints --region us-east-2 --bucket my-bucket -o hints.toml
s3-turbo-list auto-hints --region us-east-2 --bucket my-bucket \
  -o hints.sampled.toml --sample-limit 1000000 --max-pages 1000

# Delimiter-based CommonPrefixes discovery
s3-turbo-list --prefix logs/ --delimiter / discover-prefixes \
  --region us-east-2 --bucket my-bucket -o logs-prefixes.txt

# Validate / merge locally (no S3 access)
s3-turbo-list hints-validate --hints-file hints.toml
s3-turbo-list hints-merge base.toml prefixes.txt --output merged.toml
```

In sampled mode the TOML `total_objects` field is the sampled count, not the
bucket total, and segment estimates are marked as sampled.  Prefix-scoped
hints generated with `--prefix` are valid for that subtree only.  `auto-hints`
performs one sequential scan; `--threads`/`--concurrency` do not parallelise
it.

## Core Defaults

| Config key | Default | Notes |
|---|---:|---|
| `s3.max_attempts` | `10` | Retry budget per segment. |
| `s3.initial_backoff_secs` | `30` | Initial SDK retry backoff. |
| `s3.connect_timeout_secs` | `60` | Connection timeout. |
| `s3.operation_timeout_secs` | `5` | Per-page ListObjectsV2 watchdog and SDK operation/read/attempt timeout. |
| `runtime.worker_threads` | CPU cores | Tokio worker threads; CLI override: `-T`, `--threads`. |
| `runtime.max_concurrency` | `100` | Max concurrent list operations; CLI override: `-c`, `--concurrency`. |
| `channel.capacity` | `64` | Bounded channel capacity between list tasks and data-map output. |
| `output.row_group_size` | `100000` | Parquet max row group size. |
| `output.compression` | `zstd` | Parquet compression codec; CLI override: `--compression`. |
| `output.compression_level` | `1` | Compression level for codecs that support levels; CLI override: `--compression-level`. |
| `auto_hints.sample_threshold` | `10000` | Prefix count threshold used by auto-hints splitting. |
| `auto_hints.max_prefix_depth` | `5` | Maximum prefix depth considered by auto-hints splitting. |
| `auto_hints.max_prefix_entries` | `1000000` | Maximum unique parent prefixes retained during auto-hints counting before bounded mode. |

For high-latency or cross-region endpoints, consider raising
`s3.operation_timeout_secs` to `30` or `60` to reduce retry churn.

## Config File Settings

Some advanced settings are easiest to keep in a TOML config file and pass with
`--config`.  CLI flags take precedence for settings that have both forms.

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

[auto_hints]
sample_threshold = 10000
max_prefix_depth = 5
max_prefix_entries = 1000000

[channel]
capacity = 128
```

CLI flags exist for common runtime controls such as `--threads`,
`--concurrency`, `--endpoint-url`, `--profile`, `--addressing-style`,
`--max-keys`, `--start-after`, output file paths, `--sample-limit`, and
`--max-pages`.  The `auto_hints.sample_threshold`,
`auto_hints.max_prefix_depth`, and `auto_hints.max_prefix_entries` values
are TOML-only.

## Auto-Hints Scope

`auto-hints` performs one sequential ListObjectsV2 object scan.  `--prefix`
restricts that scan to a subtree, and `--max-keys` controls page size.  The
resulting TOML records the prefix when one was used.  Do not reuse
prefix-scoped hints as if they described the entire bucket.

Use `discover-prefixes` for delimiter-based `CommonPrefixes` discovery.  That
command writes prefix candidates; it does not claim object-count-balanced
segments.

## Trace-Driven Hints Iteration

For repeated inventories, hints can still be curated locally:

```bash
s3-turbo-list trace-summary trace.jsonl
s3-turbo-list hints-merge base.toml prefixes.txt --output merged.toml
```

These commands read local files only; they do not contact S3.  Long-tail
segments are split at runtime automatically, so no offline rebalancing
workflow is needed.
