# ListObjectsV2 Fast `<Contents>` Parser and Truncation Guard

**Date:** 2026-09-29
**Scope:** local measurements only (no real cloud endpoint was contacted).
**Baseline:** v0.35.1 (`281ff80`). **Change:** the listing page path in
`src/tasks_s3.rs` (`flat_list`) plus `src/list_page.rs`.

## What changed

1. **Fast `<Contents>` parser.** `flat_list` no longer uses the SDK paginator;
   it issues one ListObjectsV2 request per page, each with its own
   `FastContentsInterceptor`. For an HTTP 200 `ListBucketResult` body the
   interceptor builds the engine's `(ObjectKey, ObjectProps)` rows directly
   from the response bytes (one forward pass, memchr-based) and hands the SDK
   the same document with the `<Contents>` elements removed, so the SDK still
   parses `IsTruncated`, `NextContinuationToken`, `KeyCount`,
   `CommonPrefixes`, and errors. Any page outside the parser's strict XML
   subset falls back, per page, to the unmodified SDK path (see
   `docs/tuning.md` for the list of fallback triggers).
2. **Truncation guard.** A page reporting `IsTruncated=true` without a usable
   `NextContinuationToken`, or repeating the token it was sent, used to end
   the paginator stream, which `flat_list` treated as segment completion (exit
   0, short output). It now fails the attempt as a retryable error at the
   last key seen; the retry loop resumes with `start-after`.

## Methodology

- Host: 4 vCPU Linux container. Both binaries built with
  `cargo build --release` (the repository's release profile: thin LTO,
  `codegen-units = 1`).
- Endpoint: a local Python `ThreadingHTTPServer` mock of ListObjectsV2 on
  `127.0.0.1` serving 1,000,000 objects (keys like
  `data/tenant=NNN/dt=2024-05-DD/part-NNNNNNNNN-c000.snappy.parquet`), 1,000
  objects per page. Every object carries `Key`, `LastModified`
  (`2024-05-17T01:MM:SS.000Z`), `ETag` (`&quot;<32 hex>&quot;`),
  `ChecksumAlgorithm`, `ChecksumType`, `Size`, `StorageClass` — the shape AWS
  S3 returns. Page bodies are built once and cached by request path, so after
  a warm-up run the mock only serves cached bytes.
- Run: `list --bucket b --hints-file hints.txt` (20 boundaries, one per
  tenant, so 21 segments list in parallel) with `--output-parquet-file` and
  `--output-ks-file`, proxies cleared, path-style addressing.
- Measurement: wall time, and the child's CPU (user + sys) and max RSS from
  `getrusage(RUSAGE_CHILDREN)`. One warm-up run, then three runs of each
  binary, alternating.
- Output check: Parquet rows of both runs loaded with pyarrow, sorted, and
  compared (`Key`, `Size`, `LastModified`, `ETag`, `DiffFlag`); `.ks` files
  compared byte for byte.

## Results (1,000,000 objects)

| Run | Binary | Wall (s) | CPU user+sys (s) | Max RSS (MB) |
|---|---|---:|---:|---:|
| 1 | v0.35.1 | 3.07 | 9.19 | 133 |
| 1 | fast path | 1.11 | 3.15 | 121 |
| 2 | v0.35.1 | 2.73 | 9.01 | 134 |
| 2 | fast path | 1.06 | 3.13 | 132 |
| 3 | v0.35.1 | 2.86 | 9.13 | 145 |
| 3 | fast path | 1.02 | 2.99 | 125 |

Mean: wall 2.89 s → 1.06 s (**2.7x**), CPU 9.11 s → 3.09 s (**2.9x**),
max RSS 137 MB → 126 MB. On this 4-vCPU host the Python mock shares the CPUs
with the tool, so the wall-time ratio is a lower bound on a faster endpoint.

Output: 1,000,000 rows from each binary, identical after sorting; the `.ks`
files are byte-identical.

## Correctness evidence

- Unit tests of the parser (`src/list_page/tests.rs`): entity and
  character-reference keys, Unicode and whitespace keys, multipart and
  malformed ETags, missing fields, pretty-printed pages, self-closing/empty
  elements, extra children (`Owner`, `ChecksumAlgorithm`, `StorageClass`,
  `RestoreStatus`, unknown elements), and every fallback trigger.
- Timestamp and ETag rules are checked against the SDK's own
  `DateTime::from_str` and `ObjectProps::from(&Object)`.
- A differential test drives the real `aws-sdk-s3` client (in-process HTTP
  connector, bodies streamed in random chunks) over 600 generated pages —
  random keys with `& < > " '`, character references, control characters via
  references, random fields and orders, CommonPrefixes, error documents, 503s,
  and deliberately out-of-subset pages — through both the SDK path and the
  fast path, and requires identical rows, `IsTruncated`, next token,
  `KeyCount`, and CommonPrefixes, and identical failure for pages the SDK
  rejects. One run: 381 fast pages, 60 fallback pages, 159 error pages,
  5,791 rows compared.
- Local mock integration tests (`tests/s3_mock_integration.rs`): a truncated
  page with no token and a repeated continuation token both now list all six
  keys exactly once (v0.35.1: 2 of 6 and 4 of 6 keys, exit 0); an endpoint
  that never advances fails the run after `s3.max_attempts`.
