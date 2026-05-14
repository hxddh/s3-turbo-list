# MinIO Part A — Endpoint Validation Results

**Date**: 2026-05-14
**Scope**: MinIO local endpoint (`localhost:9000`)
**Status**: ✅ All sections passed — no blockers

---

## 1. Environment

| Item | Value |
|---|---|
| MinIO server | `RELEASE.2025-09-07T16-13-09Z` (go1.24.6 linux/arm64) |
| MinIO client (mc) | `RELEASE.2025-08-13T08-35-41Z` |
| s3-turbo-list binary | `/home/ubuntu/s3-turbo-list/target/debug/s3-turbo-list` |
| Validation directory | `/tmp/s3tl-validation-20260514-042634` |
| Test bucket | `s3tl-validation` |
| Endpoint URL | `http://localhost:9000` |
| Credentials | `AWS_ACCESS_KEY_ID=minioadmin` / `AWS_SECRET_ACCESS_KEY=minioadmin` |
| Addressing style | `path` (default), `virtual` (tested separately) |
| Profile | `minio` |
| Rustc / cargo | 1.94.0 |

Tools: `python3` + `pyarrow` + `toml`, `jq`, `mc`, `minio`, `cargo`, `clang`.

---

## 2. Summary

All MinIO Part A sections passed after two rounds of runtime fixes:

1. **Runtime fixes** (commits `884d9b9`, `06a0cfc`, `44617e1`):
   - Reactor starvation in `flat_reactor_task` — JoinSet + await-driven polling
   - `--debug-s3` trace writer discarded — CompositeTraceWriter
   - Stream timeout infinite retry — excluded from `continue_on_error`

2. **S3 compatibility fixes** (commits `f23e051`, `b825e07`, `5cb5988`):
   - `--start-after` wired into ListObjectsV2 requests
   - `delimiter`, `max_keys`, `start_after` populated in trace events
   - Final checkpoint save on fast successful runs
   - Doc expectations updated (DiffFlag=1, virtual-hosted behavior)

**Tests**: 88/88 passing after all fixes.

**Timeouts**: None — all commands completed in <10 seconds.

---

## 3. Results Table

### A.3 Basic Listings

| # | Test | EXIT | Rows | Notes |
|---|---|---|---|---|
| A.3.1 | Default listing | 0 | 19 | DiffFlag=1, KS 6 lines, 5 columns (Key, Size, LastModified, ETag, DiffFlag) |
| A.3.2 | Prefix `a/` | 0 | 5 | All keys start with `a/` |
| A.3.3 | Delimiter `/` | 0 | 1 | `common_prefixes=4,1`; `contents=1,0` (top.txt + 5 prefixes) |
| A.3.4 | max_keys=2 | 0 | 19 | 11 trace pages, all objects returned |
| A.3.5 | start_after `b/` | 0 | 14 | All keys ≥ `b/`, zero `a/` objects |
| A.3.6 | encoding-type=url | 0 | 2 | Spaces and plus signs preserved in keys |

### A.4 Trace Compatibility

| # | Test | Events | Fields verified |
|---|---|---|---|
| A.4.1 | `--debug-s3` stderr | 5 | `delimiter`, `max_keys`, `start_after` in every event |
| A.4.2 | `--trace-compat` file | 5 | Same schema as stderr |
| A.4.3 | Both simultaneously | 8 file | Composite writer fans to both outputs |

All trace events contain: `operation`, `endpoint_url`, `addressing_style`, `profile`,
`http_status`, `latency_ms`, `is_truncated`, `key_count`, `contents_count`,
`common_prefixes_count`, `delimiter`, `max_keys`, `start_after`.

### A.5–A.9

| # | Test | EXIT | Result |
|---|---|---|---|
| A.5.1 | Virtual-hosted style | 0 | Works against MinIO (environment-dependent) |
| A.5.2 | Path style | — | Confirmed in all A.3/A.4 traces |
| A.6 | Pagination (max_keys=1) | 0 | 19 rows, 21 trace events |
| A.7 | Parquet validity | — | 4 files verified: schema × columns, correct types |
| A.8 | Object filter `SOURCE.size > 0` | 0 | 19 rows, all Size > 0 |
| A.9.1 | Checkpoint fast save | 0 | `Final checkpoint saved: 1/1` |
| A.9.3 | Identity mismatch | 0 | `WARN … mismatch on field(s): delimiter` |

---

## 4. Interpretation Notes

- **DiffFlag=1**: Expected in list mode — all objects are "left-side present" (DiffFlag=0 only used in diff mode for equal objects).
- **max_keys**: Controls S3 page size, not final output row count. With `max_keys=2`, 19 objects require 10 pages of 2 + 1 final page.
- **Delimiter `/`**: Causes hierarchical listing — only top-level objects appear as contents; subdirectories appear as common prefixes. Use `--delimiter ""` for flat listing.
- **Virtual-hosted style**: Succeeds against this MinIO version — behavior is environment-dependent and does not imply BOS compatibility.
- **Checkpoint fast save**: Final checkpoint is saved on successful completion when `--resume` is enabled, regardless of run duration.
- **Trace fields**: `delimiter`, `max_keys`, and `start_after` are populated in every `S3CompatEvent` emitted via `--debug-s3` or `--trace-compat`.

---

## 5. Artifact Inventory

### Parquet files
```
minio-basic.parquet        (19 rows)
minio-delim.parquet        (1 row)
minio-maxkeys2.parquet     (19 rows)
minio-start-after.parquet  (14 rows)
minio-prefix-a.parquet     (5 rows)
minio-encode.parquet       (2 rows)
minio-filter.parquet       (19 rows)
minio-paginate.parquet     (19 rows)
minio-resume.parquet       (19 rows)
```

### Trace JSONL files
```
minio-paginate.jsonl       (21 events)
minio-trace-file.jsonl     (5 events)
minio-trace-both.jsonl     (8 events)
smoke-trace.jsonl          (2 events)
```

### KeySpace files
```
minio-basic.ks             (6 lines)
```

### Stderr / debug logs
```
logs/minio-delim.stderr
logs/minio-trace-fields.stderr
logs/minio-trace-both.stderr
logs/minio-debug-after-fix.stderr
logs/minio-server.log
```

### Config
```
minio/s3-turbo-list.toml   (reference config)
```

---

## 6. Follow-Up

- **Recommended next phase**: AWS S3 baseline (Part B in `docs/endpoint-validation-plan.md`).
- **BOS**: Defer until AWS S3 baseline completes.
- **Pre-existing issues**: `--continuation-token` CLI flag is defined but not wired (same class as `--start-after` was before fix).
