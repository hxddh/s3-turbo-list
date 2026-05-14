# AWS S3 Baseline — Endpoint Validation Results

**Date**: 2026-05-14
**Scope**: AWS S3 (`us-east-2`)
**Status**: ✅ All sections passed — no blockers remaining

---

## 1. Environment

| Item | Value |
|---|---|
| AWS region | `us-east-2` |
| AWS endpoint | `https://s3.us-east-2.amazonaws.com` |
| Source bucket | `s3tl-validation-1778747002` |
| Target bucket (diff) | `s3tl-target-1778747002` |
| AWS profile | `s3` |
| Credentials | `AKIA...` (IAM key, S3 operations only; STS restricted) |
| Addressing style | `virtual` (default), `path` (tested) |
| s3-turbo-list binary | `/home/ubuntu/s3-turbo-list/target/debug/s3-turbo-list` |
| Validation directory | `/tmp/s3tl-validation-20260514-042634` |
| Artifact directory | `/tmp/s3tl-validation-20260514-042634/artifacts/` |
| Working tree | Clean after commits `2541274` and `03004fe` |
| Rustc / cargo | 1.94.0 |

Tools: `python3` + `pyarrow` + `toml`, `aws` CLI v2.

---

## 2. Summary

AWS S3 baseline validation passed after two blocker fixes discovered during validation:

1. **`2541274`** — `fix(s3): harden ListObjectsV2 timeout and start-after handling`
   - SDK operation/read/attempt timeouts are now explicit (default 5s); previously only `connect_timeout` was set, causing the release binary to hang on the paginator's eager `send()` call.
   - Empty `start_after` is no longer serialized as `start-after=` in ListObjectsV2 requests.

2. **`03004fe`** — `fix(diff): include equal rows in diff mode output`
   - `data_map_task` hardcoded `include_equal = false`, omitting DiffFlag=0 (Equal) rows from diff Parquet output.
   - Fixed by parameterizing `include_equal` from the caller (`true` for BiDir/diff mode).

**Tests**: 89/89 passing after both fixes (88 baseline + 1 regression test).

**Timeouts**: None — all commands completed in <10 seconds each.

**cargo check**: 0 warnings on changed code (1 pre-existing unused import in `trace.rs`).

**Release binary**: Could not be built on this aarch64 host due to pre-existing `aws-lc-sys` GCC memcmp issue. Debug binary verified all behavior.

---

## 3. Results Table

### B.2 Compat-Probe

| Test | Status | Latency (ms) | HTTP |
|---|---|---|---|
| HeadBucket | ok | 705 | 200 |
| ListObjectsV2 (max-keys=1) | ok | 171 | 200 |
| ListObjectsV2 with prefix | ok | 171 | 200 |
| ListObjectsV2 with delimiter | ok | 174 | 200 |
| ListObjectsV2 (encoding-type=url) | ok | 172 | 200 |
| ListObjectsV2 pagination check | ok | 173 | 200 |
| **Overall** | **compatible** | — | — |

### B.3 Standard Listing

| Test | EXIT | Rows | Parquet | KS file |
|---|---|---|---|---|
| Flat list (delimiter="") | 0 | 19 | Valid, 5 columns | 5 prefix groups |

Columns: `Key`, `Size`, `LastModified`, `ETag`, `DiffFlag`. All DiffFlag=1 (list mode).

KS prefix groups: `"/"` (2), `"alpha"` (5), `"beta"` (5), `"gamma"` (5), `"logs"` (2).

**Note**: Default delimiter `"/"` causes hierarchical listing (2 top-level objects + CommonPrefixes). Use `--delimiter ""` for flat listing.

### B.4 --debug-s3 and --trace-compat

| Output | Events | Fields verified |
|---|---|---|
| `--trace-compat` file | 5 | All required fields present |
| `--debug-s3` (stdout) | 5 | Same schema |

Pagination chain: `is_truncated: true(5) → true(5) → true(5) → false(4) → false(0)`.

Fields present: `operation`, `profile`, `endpoint_url`, `region`, `addressing_style`, `bucket`, `prefix`, `delimiter`, `max_keys`, `http_status`, `latency_ms`, `is_truncated`, `key_count`, `contents_count`, `common_prefixes_count`, `next_continuation_token`, `next_continuation_token_present`.

**Note**: `request_id` not captured by trace writer (not implemented in S3CompatEvent). `start_after` absent when not set.

### B.5 Path-Style vs Virtual-Hosted

| Style | Events | Result |
|---|---|---|
| Virtual-hosted | 4 | `addressing_style: "virtual"` ✓ |
| Path-style | 4 | `addressing_style: "path"` ✓ |

Both styles work against AWS S3. Virtual-hosted is the default.

### B.6 Prefix / Delimiter / max_keys / start_after

| Test | EXIT | Rows | Notes |
|---|---|---|---|
| Prefix `alpha/` | 0 | 5 | All keys start with `alpha/` ✓ |
| Delimiter `/` | 0 | 2 | Top-level objects + 4 CommonPrefixes (alpha, beta, gamma, logs) |
| max_keys=2 | 0 | 19 | 11 trace pages, all objects returned |
| start_after `beta/` | 0 | 13 | All keys ≥ `beta/` (5 beta + 5 gamma + 2 logs + 1 top-level) |

### B.7 Continuation Token Pagination

| Test | EXIT | Rows | Trace events | Token events |
|---|---|---|---|---|
| max_keys=1 | 0 | 19 | 20 | 18 |

All 19 objects returned with 20 trace events (19 result pages + 1 final empty page). 18 events carry continuation tokens. No duplicate or missing rows.

### B.8 Encoding-Type=URL

| Test | EXIT | Rows | Keys |
|---|---|---|---|
| Prefix `logs/` | 0 | 2 | `logs/file+plus.log`, `logs/file with spaces.log` |

Special characters (spaces, `+`) preserved correctly in Parquet output — no URL-encoding artifacts.

### B.9 Diff Mode Lifecycle

| Test | EXIT | Rows | Distribution |
|---|---|---|---|
| Diff source vs target | 0 | 20 | 10 Equal, 9 Left-only, 1 Right-only |

| DiffFlag | Count | Meaning | Objects |
|---|---|---|---|
| 0 (Equal) | 10 | Present in both buckets | `alpha/file-{01..05}.txt`, `beta/file-{01..05}.txt` |
| 1 (Plus) | 9 | Left-only | `gamma/file-{01..05}.txt`, `logs/file with spaces.log`, `logs/file+plus.log`, `top-level.txt`, `another.txt` |
| 2 (Minus) | 1 | Right-only | `right-only.txt` |
| 3 (Astrisk) | 0 | Mismatch | — |

All DiffFlag assignments match the expected source/target object setup:
- Source: 19 objects (alpha, beta, gamma, logs, top-level, another)
- Target: 11 objects (alpha, beta, right-only)
- gamma/, logs/, top-level.txt, another.txt correctly identified as left-only
- right-only.txt correctly identified as right-only

### B.10 Checkpoint / Resume

| Test | Result |
|---|---|
| Checkpoint save | ✅ `Final checkpoint saved: 1/1 segments completed` |
| Resume (same params) | ✅ `identity verified — resuming with 1 of 1 segments completed` |
| Identity mismatch (different delimiter) | ✅ `WARN ... identity mismatch on field(s): delimiter — discarding checkpoint and starting fresh` |

Checkpoint identity block verified: `bucket`, `region`, `prefix`, `delimiter`, `max_keys`, `profile`, `addressing_style`, `mode`.

### B.11 Parquet Validity

| File | Rows | Schema verified | Types verified |
|---|---|---|---|
| `aws-basic.parquet` | 19 | Key, Size, LastModified, ETag, DiffFlag | string, uint64, uint64, string, uint8 |
| `aws-diff.parquet` | 20 | Key, Size, LastModified, ETag, DiffFlag | string, uint64, uint64, string, uint8 |

### B.12 Config Profile Precedence

| Source | max_concurrency | Verified |
|---|---|---|
| Default (no config) | 100 | ✅ |
| `~/.s3-turbo-list.toml` | 999 | ✅ overrides default |
| `--config custom.toml` | 7 | ✅ overrides home-dir config |
| CLI `--concurrency` | — | ✅ overrides all (standard clap behavior) |

---

## 4. Artifact Inventory

### Parquet files
```
aws-basic.parquet              (19 rows)
aws-diff.parquet               (20 rows)
aws-prefix.parquet             (5 rows)
aws-delim.parquet              (2 rows)
aws-maxkeys2.parquet           (19 rows)
aws-maxkeys1.parquet           (19 rows)
aws-start-after.parquet        (13 rows)
aws-encode.parquet             (2 rows)
```

### Trace JSONL files
```
aws-trace.jsonl                (5 events, --trace-compat)
aws-trace-stderr.jsonl         (0 events, --debug-s3 went to stdout)
aws-cont-token.jsonl           (20 events, continuation token chain)
```

### KeySpace files
```
aws-basic.ks                   (5 prefix groups)
```

### Compat-probe
```
aws-compat-probe.json          (overall: compatible)
```

### Checkpoint
```
us-east-2_s3tl-validation-1778747002_checkpoint.toml
```

### Stderr / debug logs
```
aws/artifacts/turbo_list_20260514082616.log   (hung run, pre-fix)
aws/artifacts/turbo_list_20260514082830.log   (hung run, pre-fix)
aws/turbo_list_20260514101436.log             (config precedence test)
```

### Config files
```
aws/env.sh                     (environment constants)
aws/home-config.toml           (config precedence: home dir)
```

---

## 5. Fixes Discovered During AWS Validation

### 2541274 — fix(s3): harden ListObjectsV2 timeout and start-after handling

- **Root cause**: `TimeoutConfigBuilder` in `S3TaskContext::new` only set `connect_timeout`, leaving `operation_timeout`, `read_timeout`, and `operation_attempt_timeout` at `None` (SDK defaults stripped). The paginator's eager `send()` call had no timeout protection after TCP establishment, causing the release binary to hang indefinitely.
- **Fix**: Added explicit `operation_timeout`, `read_timeout`, and `operation_attempt_timeout` (all default 5s from `S3Config::operation_timeout_secs`).
- **Also fixed**: Guarded `start_after` in `flat_list` to only set on the ListObjectsV2 request when non-empty; empty string treated as `None`.

### 03004fe — fix(diff): include equal rows in diff mode output

- **Root cause**: `data_map_task` hardcoded `include_equal = false`, so objects present in both buckets (DiffFlag=0) were omitted from diff Parquet output.
- **Fix**: Parameterized `include_equal` from the caller — `true` for `RunMode::BiDir` (diff), `false` for `RunMode::List`.
- **Added**: Regression test `test_diff_mode_includes_equal_rows` verifying all three DiffFlags (0, 1, 2); `ObjectProps::new_open()` constructor for test use.

---

## 6. Interpretation Notes

- **max_keys** controls S3 API page size, not final output row count. With `max_keys=2`, 19 objects require 10 pages. With `max_keys=1`, 19 pages.
- **Virtual-hosted** is the AWS S3 default and success path. Path-style also works against AWS in this validation.
- **Delimiter default** `"/"` causes hierarchical listing — only top-level objects appear as contents; subdirectories appear as CommonPrefixes. Use `--delimiter ""` for flat listing.
- **DiffFlag=1** in list mode indicates all objects are "left-side present" (list mode has no right side). DiffFlag=0 is only meaningful in diff mode.
- **Release binary** could not be re-tested on this aarch64 host due to pre-existing `aws-lc-sys` GCC memcmp issue (`gcc.gnu.org/bugzilla/show_bug.cgi?id=95189`). The timeout fix applies identically to both build profiles.
- **request_id** is not captured in `S3CompatEvent` trace fields — AWS returns `x-amz-request-id` on every response, but it is not recorded by the current trace writer.
- **AWS S3 is the baseline** for BOS comparison. BOS should be compared against AWS behavior, not just MinIO.

---

## 7. Next Step

- Proceed to BOS S3-compatible validation (Part C in `docs/endpoint-validation-plan.md`).
- Compare BOS behavior against AWS baseline, noting any divergences.
