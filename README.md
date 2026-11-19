# s3tl

Fast listing and diffing of S3-compatible buckets, to Parquet, with a
machine-readable result for every run.

```bash
s3tl list s3://my-bucket/logs/ -o out/          # Parquet dataset in out/
s3tl list s3://my-bucket/ -o - | jq .key        # NDJSON rows on stdout
s3tl list s3://my-bucket/                       # count objects and bytes only
s3tl list s3://my-bucket/ -o s3://reports/inv/  # Parquet dataset written to S3
s3tl list s3://my-bucket/ --versions -o v/      # every version and delete marker
s3tl diff s3://src/data/ s3://dst/data/ -o d/   # keys that differ, to Parquet
s3tl diff s3://src/ s3://dst/ --compare checksum  # settle ETag mismatches by checksum
s3tl check s3://my-bucket/                      # can I list this, and how?
```

`s3tl` splits the key space into segments and lists them concurrently, so it
is several times to an order of magnitude faster than a sequential
`aws s3 ls`, with no flags and no prior setup. A run probes the bucket's
prefix structure at startup, partitions flat namespaces by sampling keys,
and keeps splitting segments that turn out to be large while it runs, in
both `list` and `diff`.

## Install

Download the binary for your platform from the
[releases](https://github.com/hxddh/s3-turbo-list/releases), verify it
against `SHA256SUMS`, and put it on your `PATH` ([INSTALL.md](INSTALL.md)),
or build it with `cargo build --release`.

Credentials, region and endpoint come from the standard AWS SDK chain
(`AWS_PROFILE`, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, instance roles,
`AWS_ENDPOINT_URL_S3`). On AWS the bucket's region is detected
automatically.

## Commands

| Command | What it does |
|---|---|
| `s3tl list S3_URL` | List every object under `s3://bucket[/prefix]`. |
| `s3tl diff S3_URL S3_URL` | Compare two prefixes key by key (size and ETag, or see `--compare`). |
| `s3tl check [S3_URL]` | Show the credentials, region, endpoint and proxy a run would use; with an S3 URL, probe the endpoint with a few requests. |

Options:

| Option | Meaning |
|---|---|
| `-o DIR` | Write a Parquet dataset to `DIR` (must be absent or empty). |
| `-o s3://bucket/prefix/` | Write the Parquet dataset to an S3 prefix (must hold no objects); committed only when the run succeeds. |
| `-o -` | `list`: stream NDJSON rows to stdout. |
| *(no `-o`)* | Statistics only: object count, bytes, top prefixes (a diff: the counts). |
| `--delimiter /` | `list`: one level only, with one row per folder. |
| `--resume` | `list`: continue an interrupted run into the same `-o DIR`. |
| `--versions` | `list`: every version and delete marker (ListObjectVersions), newest first within a key. |
| `--compare etag\|size\|checksum` | `diff`: how keys on both sides compare (default `etag`; see [Output](#output)). |
| `--endpoint-url URL` | An S3-compatible endpoint (MinIO, R2, OSS, BOS, ...). |
| `--target-endpoint-url URL` | `diff`: the right side's endpoint, for cross-provider diffs. |
| `--path-style` | Path-style addressing (automatic for IP and `localhost` endpoints). |
| `--region REGION` | Signing region; defaults to the bucket's region on AWS, else `AWS_REGION`, else `us-east-1`. |
| `-c N` | Maximum concurrent requests per side. An upper bound: the run backs off on its own when the endpoint throttles or slows down. Without `-c`, a side starts at 100 and raises it, up to 1,024, when the endpoint's latency calls for it (a remote endpoint). |
| `--dry-run` | Print the plan as the result, without contacting S3. |
| `--trace FILE` | Record every listing request as JSONL (`-` for stderr). |
| `-v`, `-vv` | Progress and diagnostics on stderr. |

## The result

stdout always carries exactly one JSON document, the result of the run
(`s3tl.result/1`). Every key is always present, `null` when it does not
apply:

```json
{
  "schema": "s3tl.result/1", "version": "1.5.0", "command": "list",
  "ok": true, "status": "success", "exit_code": 0, "error": null,
  "source": {"url": "s3://my-bucket/logs/", "bucket": "my-bucket", "prefix": "logs/",
             "region": "us-east-2", "endpoint_url": null, "path_style": false},
  "target": null,
  "output": {"format": "parquet", "path": "out", "rows": 1048576,
             "files": [{"path": "out/part-00000.parquet", "rows": 1048576, "bytes": 31457280}]},
  "stats": {"objects": 1048576, "folders": 0, "bytes": 52428800000, "elapsed_secs": 4.2,
            "throttled": 0, "timeouts": 0, "top_prefixes": [...],
            "requests": 1060, "probes": 214, "segments": 99,
            "avg_concurrency": 96.4, "peak_concurrency": 100},
  "diff": null, "resume": {"resumed": false, "checkpoint": null, "remaining_ranges": 0},
  "plan": null, "check": null, "warnings": []
}
```

With `-o -` stdout carries the rows instead, and the result is the last line
of stderr. A failed run sets `ok: false` and explains itself:

```json
"error": {"code": "auth.expired_token", "message": "...", "retryable": false,
          "hint": "refresh the session credentials (e.g. `aws sso login`)"}
```

| Exit | Meaning | |
|---|---|---|
| 0 | Success | |
| 1 | Internal error (a bug) | `internal.*` |
| 2 | Usage error: fix the command line | `usage.*` |
| 3 | Credentials, bucket, region or endpoint are wrong | `auth.*`, `config.*`, `endpoint.*` |
| 4 | Network, throttling or server fault: retrying may succeed (with `--resume` when the result has a checkpoint) | `net.*` |
| 5 | Writing the output failed | `output.*` |
| 130 | Interrupted; `list --resume` continues | `interrupted` |

The full contract — every field and error code — is in
[docs/agent-usage.md](docs/agent-usage.md).

## Output

`-o DIR` writes a Parquet dataset: `part-00000.parquet`, `part-00001.parquet`,
… (more than one when the run scales its writers to keep up with a fast
store) and `_result.json`, a copy of the result. Parts are written under
hidden temporary names and renamed into place when the run commits, so a
reader that globs `DIR/part-*.parquet` never sees a partial file:

```python
import duckdb
duckdb.sql("select count(*), sum(size) from 'out/part-*.parquet'")
```

`list` columns (`s3tl.list/1`, recorded in each file's metadata):

| Column | Type | |
|---|---|---|
| `key` | string | |
| `size` | uint64 | bytes |
| `last_modified` | timestamp (ms, UTC) | null for folder rows |
| `etag` | string | hex MD5, with `-N` for multipart uploads; another vendor's form verbatim (quotes removed); null when the endpoint sent none |
| `storage_class` | string | null when the endpoint sent none |

A `--delimiter` listing adds one row per folder: the key ends with the
delimiter, size is 0, and the other columns are null.

`list --versions` (`s3tl.list-versions/1`) adds three columns: `version_id`
(string; `"null"` for an object written before versioning was enabled),
`is_latest` (bool) and `is_delete_marker` (bool). A delete marker has size
0 and a null `etag` and `storage_class`. Rows of a key are newest first.

`diff` compares keys relative to each side's prefix, so
`s3tl diff s3://a/2025/ s3://b/archive/2025/` matches `a/2025/x` with
`b/archive/2025/x`. It writes only the keys that differ (`s3tl.diff/1`):
`key` (relative to the prefixes), `status`
(`left_only`, `right_only` or `changed`), then `left_size`,
`left_last_modified`, `left_etag`, `right_size`, `right_last_modified`,
`right_etag` (null for the side a key is missing from). Equal keys are only
counted, in the result's `diff.equal`. `--compare` decides when a key is
`changed`:

- `etag` (default): its size or ETag differs, or either side has no ETag.
- `size`: its size differs (ETags are ignored).
- `checksum`: like `etag`, then each same-size pair whose ETags differ is
  settled by the checksums the store keeps (one HeadObject per side with
  checksum mode on). Multipart uploads and SSE-KMS give the same bytes
  different ETags; their full-object checksums still agree. A pair proven
  equal counts as equal (and in `diff.verified_equal`); a pair the
  checksums cannot settle (no algorithm in common, or composite per-part
  checksums) stays `changed` and counts in `diff.unverified`.

NDJSON rows (`-o -`) have the `list` columns:
`{"key":"logs/a.gz","size":123,"last_modified":"2026-05-17T00:00:00.123Z","etag":"…","storage_class":"STANDARD"}`;
with `--versions` they add `"version_id"`, `"is_latest"` and
`"is_delete_marker"`.

`-o s3://bucket/prefix/` writes the same dataset to S3: each part streams
into a multipart upload while it is written (no local disk needed), and
nothing is visible until the run succeeds. The commit completes every
upload, all or nothing, and writes `_result.json` last; a failed or
interrupted run aborts its uploads. The prefix must hold no objects, the
output bucket uses the source's endpoint and credentials, and an S3 output
is not resumable. A bucket lifecycle rule that aborts incomplete multipart
uploads covers a run that is killed outright.

## Interrupt and resume

Ctrl-C or SIGTERM stops a `list -o DIR` run gracefully: the parts written so
far are committed, the key ranges not yet written are saved in
`DIR/_checkpoint.json`, and the run exits 130. A run that fails on the
network or on its setup (exit 4 or 3: throttled until the retry budget ran
out, an expired token) after listing something keeps its progress the same
way. Rerunning the same command with `--resume` lists exactly those ranges,
adds its parts after the existing ones, and leaves `DIR` holding the
complete listing; resuming a dataset that already finished reports its
result again (exit 0):

```bash
s3tl list s3://my-bucket/ -o out/            # interrupted: exit 130
s3tl list s3://my-bucket/ -o out/ --resume   # out/ now holds every key once
```

A checkpoint is only accepted by the run it belongs to (same bucket, prefix,
endpoint and options, `--versions` included). Any other failed run — before
it listed anything, or on its output — leaves the directory as it was before
it started. The dataset is complete when `_result.json` says `ok: true` and
there is no `_checkpoint.json`. `diff` and outputs other than a local
directory do not checkpoint.

## Performance

Parallelism comes from listing key-space segments concurrently. Segments
come from, in order:

1. **Startup discovery**: a few delimiter probes find real prefix
   boundaries, across the whole key space even with thousands of top-level
   prefixes. The listing starts at once and is split when they arrive.
2. **Startup bisection**: flat namespaces (one segment per worker), large
   flat directories under too few prefixes, and whatever a prefix listing
   could not page through are partitioned by single-key probes.
3. **Runtime splitting**: a segment that turns out to hold most of the data
   is split while it runs — into as many pieces as there are idle workers —
   at prefix boundaries where the range has structure and spread over its
   remaining keys where it is flat. `diff` splits each side the same way
   while reading both sides in key order.

One adaptive request window per side, capped at `-c`, paces every request:
it backs off when the endpoint throttles or times out and grows back when it
does not, so a rate-limited bucket is listed just under its ceiling instead
of above it. Partitioning takes few probes (two candidates per cut, runtime
splits scoped to the directory they cut), and its boundaries reach the
listing round by round. Listings that
fit in one page, and `--delimiter` listings, are a single request chain.
Details in [docs/tuning.md](docs/tuning.md).

## Providers

Any endpoint that implements ListObjectsV2 works (`--versions` needs
ListObjectVersions, `-o s3://` multipart uploads, `--compare checksum`
HeadObject checksums). Examples for AWS, MinIO,
Cloudflare R2, Backblaze B2, Alibaba OSS and Baidu BOS are in
[docs/providers.md](docs/providers.md). Before a large run against a new
endpoint, `s3tl check s3://bucket/` sends a handful of requests and reports
how it behaves.

`s3tl` sends `encoding-type=url`, so keys with characters XML cannot carry
still list. A listing that breaks the S3 ordering contract (keys out of
order, or repeated) fails with `endpoint.inconsistent_listing` instead of
producing a silently wrong output.

Behind a proxy, `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` / `NO_PROXY` apply
as with curl and the AWS CLI; `s3tl check` names the proxy a request would
use.

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
