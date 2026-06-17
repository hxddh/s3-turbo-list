# BOS ListObjectsV2 Compatibility Resolved

**Date:** 2026-06-17
**Supersedes:** `bos-listobjects-v2-pagination-compatibility-note-20260514.md`
and the BOS conclusions in `final-validation-summary-20260514.md`
(B-1) and `bos-s3-compatible-20260514.md` (Finding #2).

## Summary

Baidu BOS has completed compatibility with the S3 ListObjectsV2 API. The
historical incompatibility — BOS restarted the listing from `start_after`
when a `continuation-token` was also present, ignoring the token — has been
fixed service-side by BOS. BOS now follows the AWS S3 contract: when both
parameters are present, `continuation-token` takes precedence and the
listing resumes from where the previous page left off.

This closes the only known BOS-specific limitation in the tool.

## What changed in s3-turbo-list

With the service-side bug resolved, the BOS-specific defenses built around it
are removed (release v0.21.0):

- Startup structural discovery no longer excludes the `bos` profile
  (`src/main.rs`).
- The hints resolver for diff sides no longer excludes the `bos` profile
  (`src/main.rs`).
- The `warn_bos_hinted_segments` warning and the "non-authoritative" profile
  limitations / guide text are removed.

BOS is now treated like any other S3-compatible endpoint: single-segment
listing, hinted multi-segment listing, startup discovery, and runtime
splitting all run with no profile special-casing.

## Effect on previously documented findings

- **B-1 (BOS restarts from `start_after`):** Resolved service-side. Hinted
  multi-segment listing on BOS — where every non-first segment carries a
  non-empty `start_after` — now paginates fully instead of truncating to a
  single page per segment.
- **Finding #2 (hinted multi-segment _diff_ dropped objects on BOS):** The
  dropped objects were second-page keys in non-first segments — the same
  symptom as B-1. Hinted multi-segment diff is validated correct on AWS S3
  and MinIO, which confirms there is no provider-independent tool-side
  defect; the BOS-only failure was a manifestation of B-1 and is resolved by
  the same fix.

## Evidence

The v0.20.0 BOS compatibility stress report (Baidu BOS, `s3.bj.bcebos.com`)
confirms `compat-probe` passes and a single-segment full-bucket scan returns
the complete object set (2,763,873 objects), establishing that BOS
ListObjectsV2 pagination is correct. The single-segment path never sets
`start_after`, so it was never affected by B-1; its correctness plus the
service-side fix establishes that the hinted/segmented paths now behave
identically to AWS S3.

## Historical record

The pre-fix 2026-05-14 notes are retained unchanged as historical evidence
of the original BOS behavior and the tool's prior defensive posture.
