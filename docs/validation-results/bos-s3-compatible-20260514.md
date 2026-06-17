# BOS S3-Compatible Validation — s3-turbo-list

> **Note (2026-06-17):** Finding #2 (hinted multi-segment diff dropped
> objects on BOS) was a manifestation of the BOS service-side pagination bug,
> now **resolved by BOS**. See
> [`bos-listobjectsv2-compatibility-resolved-20260617.md`](bos-listobjectsv2-compatibility-resolved-20260617.md).
> Conclusions below are retained unchanged as historical evidence.

**Date**: 2026-05-14
**Endpoint**: `https://s3.bj.bcebos.com`
**Region**: `bj`
**Profile**: `bos`
**Addressing style**: `path` (primary; virtual-hosted also tested)

**Source bucket**: `s3tl-bos-1778750400`
**Target bucket** (diff mode): `s3tl-bos-target-1778750400`
**Object tree**: 19-object layout matching MinIO/AWS baseline (alpha/×5, beta/×5, gamma/×5, top-level.txt, another.txt, logs/file with spaces.log, logs/file+plus.log)

**Binary**: `target/debug/s3-turbo-list`
**Artifacts**: `/tmp/s3tl-validation-20260514-042634/bos/`

---

## Pass/Fail Matrix

| # | Test | Result | Details |
|---|---|---|---|
| 1 | **Compat-probe** | ✅ PASS | `overall_status: compatible`, all 6 tests ok (HeadBucket, ListObjectsV2 × 5 variants) |
| 2 | **Standard listing** | ✅ PASS | 19 rows, Parquet + KS output (`--delimiter ""` required) |
| 3 | **Trace compatibility** | ✅ PASS | All 18 required fields present; `profile=bos`, `addressing_style=path` |
| 4 | **Path-style** | ✅ PASS | Primary success path |
| 5 | **Virtual-hosted** | ⚠️ UNEXPECTED PASS | Succeeds (plan assumed failure). `addressing_style=virtual`, HTTP 200 |
| 6 | **Prefix filter** (`alpha/`) | ✅ PASS | 5 rows, all start with `alpha/` |
| 7 | **Delimiter** (`/`) | ✅ PASS | CommonPrefixes: 2 per page, top-level contents: 2 |
| 8 | **max_keys pagination** | ✅ PASS | max_keys=2 → 11 events, 19 rows |
| 9 | **start_after** (`beta/`) | ✅ PASS | 13 rows, all ≥ `beta/` (excludes alpha/ + another.txt) |
| 10 | **Continuation token** | ✅ PASS | max_keys=1 → 19 unique rows, 18 unique tokens, clean chain |
| 11 | **encoding-type=url** | ✅ PASS | Spaces and `+` preserved correctly in Parquet |
| 12 | **Error behavior (404)** | ✅ PASS | `NoSuchBucket`, HTTP 404, `truncated_raw_body` (262 chars XML), `request_id` populated |
| 13 | **Checkpoint save** | ✅ PASS | Single-segment checkpoint saved (1/1 segments) |
| 14 | **Checkpoint resume** | ✅ PASS | Identity verified, "all 1 segments already completed" |
| 15 | **Identity mismatch** | ✅ PASS | `delimiter` mismatch correctly rejected |
| 16 | **Diff mode** | ✅ PASS | 10 Equal, 9 Left-only, 1 Right-only (single-segment; see finding #2) |
| 17 | **Parquet validity** | ✅ PASS | All output files: correct schema (Key, Size, LastModified, ETag, DiffFlag), correct types |

---

## BOS vs AWS Behavior Matrix

| Area | BOS | AWS S3 |
|---|---|---|
| **Virtual-hosted style** | ✅ Supported | ✅ Supported |
| **Path-style** | ✅ Supported | ✅ Supported |
| **Server header** | `BceBos` | `AmazonS3` |
| **Request ID headers** | `x-amz-request-id` + `x-bce-request-id` (both) | `x-amz-request-id` only |
| **Delimiter `""`** | Required for flat listing with prefix `/` | Required (consistent) |
| **Compat-probe** | Compatible (6/6 ok) | Compatible (6/6 ok) |
| **Continuation tokens** | Key names as tokens (correct) | Same |
| **encoding-type=url** | Spaces and `+` preserved | Same |
| **Error XML body** | Standard S3 XML format | Same |
| **start_after** | Works correctly with valid keys | Same |
| **ListObjectsV2 pagination** | Correct | Same |
| **Diff mode (single-segment)** | 10/9/1 distribution matches AWS | 10/9/1 |

---

## Incompatibilities Found

### Finding #1: TOML syntax leaked as S3 `start-after` values (tool-level)

**Classification**: Tool bug — hints-file TOML parsing feeds raw TOML syntax (`'    "alpha",'`) as the S3 `start-after` parameter. BOS rejects these with `SignatureDoesNotMatch` (403). AWS would likely also reject them.

**Evidence** (preserved in `finding-1-*` under the bos artifact directory):

Command:
```bash
s3-turbo-list list \
  --bucket s3tl-bos-1778750400 \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --region bj \
  --addressing-style path \
  --prefix "/" \
  --delimiter "" \
  --max-keys 5 \
  --threads 2 --concurrency 2 \
  --hints-file bj_s3tl-bos-1778750400_hints.toml \
  --resume \
  --debug-s3 \
  --trace-compat finding-1-trace.jsonl
```

Hints file (`bj_s3tl-bos-1778750400_hints.toml`):
```toml
bucket = "s3tl-bos-1778750400"
region = "bj"
total_objects = 19
boundaries = [
    "alpha",
    "beta",
    "gamma",
    "logs",
]
generated_at = "2026-05-14T11:02:50.615210921+00:00"
```

**What BOS received as `start-after`** (from trace JSONL):

| `start-after` value | HTTP | S3 error | Request ID |
|---|---|---|---|
| `'    "alpha",'` | 403 | `SignatureDoesNotMatch` | `c1405fc6-b399-4803-a3a5-64f2377a8cc0` |
| `'    "beta",'` | 403 | `SignatureDoesNotMatch` | `0dda3fd0-d7d4-41ed-840e-09911be02489` |
| `'    "gamma",'` | 403 | `SignatureDoesNotMatch` | `81798254-1eb7-484c-a2c4-8b48292eceb9` |
| `'    "logs",'` | 403 | `SignatureDoesNotMatch` | `e370ff37-a140-4833-bb6d-11b2e85d6618` |
| `'boundaries = ['` | 200 | — | (BOS ignores unknown key prefix — no matching objects) |
| `']'` | 200 | — | (BOS ignores unknown key prefix) |
| `'bucket = "s3tl-bos-1778750400"'` | 200 | — | (BOS ignores unknown key prefix) |
| `NONE` (root, no start_after) | 200 | — | (correct — no start_after on root segment) |

**Each error body** (identical pattern):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>SignatureDoesNotMatch</Code>
    <Message>The request signature we calculated does not match the signature
    you provided. Check your Secret Access Key and signing method.
    Consult the service documentation for details.</Message>
    <Resource>/s3tl-bos-1778750400</Resource>
    <RequestId>c1405fc6-b399-4803-a3a5-64f2377a8cc0</RequestId>
</Error>
```

`x-bce-request-id` matches `x-amz-request-id` on all events (BOS returns both).

**Artifacts preserved**:
- `finding-1-command.sh` — exact command
- `finding-1-trace.jsonl` — full trace JSONL (11 events)
- `finding-1-stderr.log` — stderr/debug output
- `finding-1-hints.toml` — hints file used
- `finding-1-analysis.txt` — event-by-event breakdown

**Root cause**: The hints TOML file parser reads raw lines from the `boundaries` array as `start_after` strings instead of extracting the quoted boundary values (`alpha`, `beta`, `gamma`, `logs`). The indented TOML array entries (`'    "alpha",'`) include literal whitespace, quotes, and commas, which are sent as the S3 `start-after` parameter, corrupting the AWS Signature V4 canonical request and causing BOS to reject the request.

**Impact**: All segments with `start_after` boundaries from hints fail with 403 when checkpoint/resume is in use. The root segment (no `start_after`) succeeds because it does not use a boundary value. Pagination continuation tokens (correctly formatted S3 keys) work correctly.

**Recommendation**: Fix the hints TOML parser in `src/auto_hints.rs` to extract boundary values properly (trim whitespace, strip quotes, strip trailing commas). This is not BOS-specific — AWS would also reject these malformed `start-after` values.

---

### Finding #2: Hinted multi-segment diff may drop objects (tool-level)

**Classification**: Tool limitation — hinted multi-segment diff with non-overlapping target bucket key space can produce incomplete results compared to single-segment fallback.

**Evidence**: During an earlier validation run, hinted diff produced 17 rows (10 Equal, 6 Left-only, 1 Right-only), missing `logs/file with spaces.log`, `logs/file+plus.log`, and `top-level.txt` (3 source-only objects). The expected single-segment result is 20 rows (10 Equal, 9 Left-only, 1 Right-only), which matches the AWS baseline.

The most recent run produced the correct 20 rows, indicating this is non-deterministic and likely depends on segment scheduling order, token freshness, and whether all segments complete before the data_map task finalizes.

**Expected baseline** (AWS single-segment diff):
- 10 Equal (DiffFlag=0): alpha/×5, beta/×5
- 9 Left-only (DiffFlag=1): gamma/×5, logs/×2, top-level.txt, another.txt
- 1 Right-only (DiffFlag=2): right-only.txt

**Recommendation**: Do not use hinted multi-segment diff as authoritative until paired-segment coordination is implemented. Single-segment diff (fallback, `--no-auto-hints`) produces correct results for BOS.

---

## Recommendations

1. **BOS single-segment list/diff/checkpoint path is usable** for production. Use `--no-auto-hints` (or ensure hints are not present) to stay on the single-segment code path.

2. **BOS hinted multi-segment list with checkpoint/resume needs further investigation** — the TOML parsing bug (Finding #1) prevents any segment with a `start_after` boundary from working. Fix the parser first, then re-validate.

3. **Hinted multi-segment diff should not be used as authoritative** until paired-segment coordination is implemented. Objects in key ranges not present in the target bucket may be silently dropped.

4. **`--delimiter ""` is required** for flat listing with `--prefix "/"` on BOS. This is consistent with AWS behavior and is not BOS-specific.

5. **BOS supports virtual-hosted addressing** in addition to path-style. This was not expected per the original validation plan but was confirmed in testing.

---

## Artifact Inventory

All preserved under `/tmp/s3tl-validation-20260514-042634/bos/`:

```
bos-basic.parquet            19 rows — standard listing
bos-basic.ks                 5 segments — keyspace output
bos-diff.parquet             20 rows — diff mode (single-segment)
bos-encode.parquet           2 rows — encoding test
bos-maxkeys1.parquet         19 rows — max_keys=1 pagination
bos-maxkeys2.parquet         19 rows — max_keys=2 pagination
bos-prefix-alpha.parquet     5 rows — prefix filter
bos-start-after.parquet      13 rows — start_after filter
bos-trace.jsonl              12 events — trace compat
bos-paginate.jsonl           20 events — pagination trace
bos-virtual-fail.jsonl       13 events — virtual-hosted trace (unexpected success)
bos-error-trace.jsonl        1 error event — 404 NoSuchBucket
compat-probe.json            overall: compatible
verify-creds.json            overall: compatible
env.sh                       environment variables

finding-1-command.sh         exact command for 403 reproduction
finding-1-trace.jsonl        11 trace events (4×403, 4×200 with TOML-corrupted start_after)
finding-1-stderr.log         stderr/debug output
finding-1-hints.toml         hints TOML file used
finding-1-analysis.txt       event-by-event breakdown with request IDs
finding-2-hinted-diff.parquet 20 rows (hinted diff — correct on this run)
```

---

## Cleanup Commands

After review and approval, remove the two test buckets:

```bash
aws s3 rm s3://s3tl-bos-1778750400 --recursive \
  --endpoint-url https://s3.bj.bcebos.com --profile bos --region bj
aws s3 rb s3://s3tl-bos-1778750400 \
  --endpoint-url https://s3.bj.bcebos.com --profile bos --region bj

aws s3 rm s3://s3tl-bos-target-1778750400 --recursive \
  --endpoint-url https://s3.bj.bcebos.com --profile bos --region bj
aws s3 rb s3://s3tl-bos-target-1778750400 \
  --endpoint-url https://s3.bj.bcebos.com --profile bos --region bj
```
