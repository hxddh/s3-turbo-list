# BOS ListObjectsV2 Pagination Compatibility Note

> **SUPERSEDED (2026-06-17):** BOS has since fixed this service-side
> incompatibility. See
> [`bos-listobjectsv2-compatibility-resolved-20260617.md`](bos-listobjectsv2-compatibility-resolved-20260617.md).
> This note is retained as historical evidence of the original behavior.

**Date:** 2026-05-14
**Discovered during:** Minimal BOS hints revalidation after commit `4b0abd2` (hints TOML parser fix)

## Context

After fixing a bug where TOML hints-file syntax leaked into S3 `start_after` parameters
(commit `4b0abd2`), a minimal BOS hints revalidation was performed against bucket
`s3tl-bos-hints-1778759715` using the Baidu Object Storage (BOS) endpoint
`https://s3.bj.bcebos.com` with path-style addressing.

The parser fix was confirmed: `start_after` values are now clean (no TOML indentation,
quotes, or trailing commas), and no 403 SignatureDoesNotMatch errors occur.

However, the revalidation exposed a pre-existing BOS compatibility issue with
ListObjectsV2 pagination when `start_after` is combined with `continuation-token`.

## BOS Behavior Difference

### Expected (AWS S3-compatible) behavior

When a ListObjectsV2 request includes both `start_after` and `continuation-token`,
the `continuation-token` takes precedence. The service resumes the listing from
where the previous page left off, ignoring `start_after`.

The AWS SDK Rust paginator sends the original `start_after` value on every
subsequent page (it is part of the request template). On AWS S3, this is harmless.

### Actual BOS behavior

BOS **restarts the listing from `start_after`** when both parameters are present,
effectively ignoring the `continuation-token`. This causes the paginator to
receive the same first-page results repeatedly, then falsely report `key_count=0`.

## Minimal Reproduction

```bash
BUCKET="s3tl-bos-hints-1778759715"
EP="https://s3.bj.bcebos.com"

# Get a continuation token from page 1
TOKEN=$(aws s3api list-objects-v2 --bucket "$BUCKET" \
  --endpoint-url "$EP" --region bj --profile bos \
  --start-after "beta" --max-keys 2 \
  --query 'NextContinuationToken' --output text)

# --- continuation_token alone: WORKS ---
aws s3api list-objects-v2 --bucket "$BUCKET" \
  --endpoint-url "$EP" --region bj --profile bos \
  --continuation-token "$TOKEN" --max-keys 2
# → returns logs/file with spaces.log, logs/file+plus.log  (correct)

# --- start_after + continuation_token together: BROKEN ---
aws s3api list-objects-v2 --bucket "$BUCKET" \
  --endpoint-url "$EP" --region bj --profile bos \
  --start-after "beta" --continuation-token "$TOKEN" --max-keys 2
# → returns beta/file-01.txt, beta/file-02.txt  (RESTARTS from start_after!)
```

## Impact on s3-turbo-list

| Mode | Status |
|------|--------|
| Single-segment listing (no hints, or empty hints) | ✅ Works — no `start_after` is set |
| Hinted multi-segment listing on AWS S3 | ✅ Works — S3 handles both params correctly |
| Hinted multi-segment listing on MinIO | ✅ Works — MinIO handles both params correctly |
| Hinted multi-segment listing on BOS | ⚠️ Non-first segments can miss later pages |
| Standard `--start-after` / `--max-keys` / `--prefix` / `--delimiter` on BOS | ✅ Works — no segmented pagination involved |

**Specific failure mode on BOS:** Every hinted segment with a non-empty `start_after`
(all segments except the first, segment index 0) will only retrieve its first
`--max-keys` objects. Any objects beyond the first page in that segment are silently
dropped. The first segment (index 0, empty `start_after`) paginates correctly.

## Recommendation

- **BOS should fix ListObjectsV2 compatibility** to respect `continuation-token`
  when `start_after` is also present, matching AWS S3 behavior.

- **s3-turbo-list should not add an endpoint-specific workaround** at this time.
  Adding manual pagination that strips `start_after` on continuation requests for
  BOS-only would compromise the high-performance design and add maintenance burden.

- Until BOS fixes this, **avoid hinted multi-segment listing on BOS for
  authoritative scans**. Use single-segment fallback or AWS-compatible endpoints
  (AWS S3, MinIO) for hinted multi-segment validation.

## Artifacts

All revalidation artifacts are preserved:

```
/tmp/s3tl-validation-20260514-042634/bos-hints-recheck/
  trace.jsonl              — main hinted listing trace (10 events, all HTTP 200)
  trace-ascii.jsonl        — ASCII-only hints trace
  trace-no-hints.jsonl     — single-segment baseline (7 objects found)
  trace-plain.jsonl        — plain-text hints trace
  trace-no-logs.jsonl      — no-"logs"-boundary hints trace
  run.log                  — application log
  result.ks                — output KeySpace file
  result.parquet           — output Parquet (6 rows, readable)
  ascii_hints.toml         — test TOML hints file
  hints_no_logs.toml       — test TOML hints (no "logs" boundary)
  plain_hints.txt          — test plain-text hints
```

Bucket `s3tl-bos-hints-1778759715` with 7 test objects is still live (pending cleanup).
