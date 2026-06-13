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
   (`<region>_<bucket>_hints.toml`), written by startup discovery on a
   previous run.
3. **Startup structural discovery** (recursive list runs — the default;
   hierarchical `--delimiter '/'` runs skip it) —
   a bounded set of delimiter probes (one ListObjectsV2 page each, at most
   3 levels deep) finds real `CommonPrefixes` boundaries at run start and
   caches them at the conventional path.  Costs at most a second or two of
   startup; first runs list in parallel with no prior steps.
4. **Single segment** — flat namespaces with no `/` structure, and runs
   with `--no-auto-hints`, `--start-after`, or `--continuation-token`.

`diff` uses the same automatic sources per side (cached hints or startup
discovery) and lists each side's segments in parallel; its segment set
stays static (no runtime splitting) so the merge can consume segments in
key order.

Boundaries are also adjusted **at runtime**: when a list run has idle
concurrency and one segment proves to be a long tail, the segment splits
cooperatively — the right half becomes a new parallel child segment,
recursively.  Split points come from a delimiter probe when the remaining
range has `CommonPrefixes` structure; for flat ranges (no `/` structure),
candidate cuts are derived from the segment's cursor and validated with
single-key probes, so the boundary is always a real observed key.  The reactor
probes the busiest long-tail segments as soon as slots are idle (not on a fixed
once-per-second tick) and fans out several at once up to the idle-slot budget,
so a flat namespace — where this is the only fan-out mechanism — reaches full
concurrency in a few page round-trips rather than one segment per second.
Skewed and flat buckets alike fan out until concurrency is used.  Splitting
never applies to `diff` (static segments by design), `--start-after`, or
`--continuation-token` runs.  Split segments conservatively do not record
checkpoint progress, so `--resume` re-lists the original segment.

**Defaults are designed to be the right choice**: `worker_threads` follows
the machine's CPU count, and `--concurrency` only needs raising when a very
large bucket on a fast network leaves workers idle.  Hand-tuning `-c`/`-T`
is rarely worthwhile.

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

**TOML** (written by startup discovery):

```toml
bucket = "my-bucket"
region = "us-east-2"
total_objects = 50000
boundaries = ["alpha/", "beta/", "logs/"]
generated_at = "2026-05-14T12:00:00Z"
scan_mode = "full"
```

Local tooling (no S3 access):

```bash
s3-turbo-list hints-validate --hints-file hints.toml
```

The `auto-hints` and `discover-prefixes` scan commands were **removed in
v0.8.0**: startup discovery plus runtime splitting cover their use cases
automatically and without a separate sequential scan.  Existing hints
caches and `--hints-file` workflows keep working unchanged.

## Core Defaults

| Config key | Default | Notes |
|---|---:|---|
| `s3.max_attempts` | `10` | Retry budget per segment. |
| `s3.initial_backoff_secs` | `1` | Initial SDK retry backoff. A single throttle (HTTP 503 SlowDown) costs ~1s of wall-clock instead of stalling the whole run; the SDK still grows the delay exponentially across `max_attempts`. |
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
- If you are throttled (HTTP 503 `SlowDown`), the low default
  `initial_backoff_secs` keeps each retry cheap; lowering request pressure
  (fewer concurrent segments) helps more than retrying harder.
- The largest wins come from spreading load across prefixes, which segmented
  listing does automatically.

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

[channel]
capacity = 128
```

CLI flags exist for common runtime controls such as `--threads`,
`--concurrency`, `--endpoint-url`, `--profile`, `--addressing-style`,
`--max-keys`, `--start-after`, and output file paths.

## Trace-Driven Inspection

After a run, inspect segment balance from the `--trace-compat` JSONL locally:

```bash
s3-turbo-list trace-summary trace.jsonl
```

This reads local files only and does not contact S3.  Long-tail segments are
split at runtime automatically, so no offline rebalancing workflow is needed.
