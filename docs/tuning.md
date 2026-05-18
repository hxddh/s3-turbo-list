# Tuning Reference

This page lists runtime defaults and advanced configuration knobs that matter
for large listings.  Values come from `src/config.rs`.

## Core Defaults

| Config key | Default | Notes |
|---|---:|---|
| `s3.max_attempts` | `10` | Retry budget per segment. |
| `s3.initial_backoff_secs` | `30` | Initial SDK retry backoff. |
| `s3.connect_timeout_secs` | `60` | Connection timeout. |
| `s3.operation_timeout_secs` | `5` | Per-page ListObjectsV2 watchdog and SDK operation/read/attempt timeout. |
| `runtime.worker_threads` | `10` | Tokio worker threads; CLI override: `-T`, `--threads`. |
| `runtime.max_concurrency` | `100` | Max concurrent list operations; CLI override: `-c`, `--concurrency`. |
| `channel.capacity` | `64` | Bounded channel capacity between list tasks and data-map output. |
| `output.row_group_size` | `10000` | Parquet max row group size. |
| `output.compression` | `gzip` | Parquet compression codec. |
| `output.compression_level` | `6` | Compression level for codecs that support levels. |
| `auto_hints.sample_threshold` | `10000` | Prefix count threshold used by auto-hints splitting. |
| `auto_hints.max_prefix_depth` | `5` | Maximum prefix depth considered by auto-hints splitting. |
| `auto_hints.min_segment_size` | `1000` | Reserved segment-size tuning value. |
| `auto_hints.max_prefix_entries` | `1000000` | Maximum unique parent prefixes retained during auto-hints counting before bounded mode. |

For high-latency or cross-region endpoints, consider raising
`s3.operation_timeout_secs` to `30` or `60` to reduce retry churn.

## TOML-Only Settings

Some advanced settings do not have CLI flags.  Put them in a TOML config file
and pass it with `--config`.

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
row_group_size = 50000
compression = "zstd"
compression_level = 3

[auto_hints]
sample_threshold = 10000
max_prefix_depth = 5
min_segment_size = 1000
max_prefix_entries = 1000000

[channel]
capacity = 128
```

CLI flags exist for common runtime controls such as `--threads`,
`--concurrency`, `--endpoint-url`, `--profile`, `--addressing-style`,
`--max-keys`, `--start-after`, output file paths, `--sample-limit`, and
`--max-pages`.  The `auto_hints.sample_threshold`,
`auto_hints.max_prefix_depth`, `auto_hints.min_segment_size`, and
`auto_hints.max_prefix_entries` values are TOML-only.

## Auto-Hints Scope

`auto-hints` performs one sequential ListObjectsV2 object scan.  `--prefix`
restricts that scan to a subtree, and `--max-keys` controls page size.  The
resulting TOML records the prefix when one was used.  Do not reuse
prefix-scoped hints as if they described the entire bucket.

Use `discover-prefixes` for delimiter-based `CommonPrefixes` discovery.  That
command writes prefix candidates; it does not claim object-count-balanced
segments.
