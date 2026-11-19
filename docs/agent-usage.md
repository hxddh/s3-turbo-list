# The s3tl contract

Everything an automated caller (a script, CI job or AI agent) can rely on.
Within major version 1, fields and error codes are only ever added; a field
is never renamed, retyped or removed, and an exit code never changes
meaning.

## Streams

- **stdout** carries exactly one JSON document, the *result*, for every
  invocation: success, failure, usage error, dry run, `check`.
- With `list -o -`, stdout carries NDJSON rows instead, and the result is the
  **last line of stderr** (compact JSON). Rows are in no particular order. A
  reader that closes the pipe early (`| head`) stops the run with exit 5
  (`output.write_failed`).
- Everything else on stderr is for humans: a one-line summary of a failure
  (`s3tl: failed (exit 3): auth.expired_token: … (hint: …)`), and with `-v`
  progress logs. Do not parse it.
- `s3tl` never prompts.
- `--help` and `--version` print text and exit 0 (no result document).

## Exit codes

| Exit | Class | Error codes | What to do |
|---|---|---|---|
| 0 | success | | |
| 1 | internal | `internal.error`, `internal.panic` | A bug; report it. |
| 2 | usage | `usage.*` | Fix the command line; rerunning unchanged fails again. |
| 3 | setup | `auth.*`, `config.*`, `endpoint.*` | Fix credentials, bucket, region or endpoint. |
| 4 | retryable | `net.*` | Retry later (with `--resume` when `resume.checkpoint` is set). |
| 5 | output | `output.*` | Fix the output location (space, permissions, closed pipe). |
| 130 | interrupted | `interrupted` | `list -o DIR`: rerun with `--resume` (an `-o s3://` run: rerun). |

`error.retryable` is `true` exactly for exit 4 and 130.

**Continuing a run.** Whenever `resume.checkpoint` is a path, the same
command plus `--resume` continues where the run stopped: after an interrupt,
and (since 1.6) after a `list -o DIR` that failed with exit 3 or 4 once it had
listed something. `--resume` of a dataset that already finished reports that
dataset's `_result.json` again and exits 0, so retrying a resume is safe.

## The result (`s3tl.result/1`)

Every key below is always present; `null` means "does not apply".

| Key | Type | |
|---|---|---|
| `schema` | string | `"s3tl.result/1"` |
| `version` | string | the s3tl version |
| `command` | string | `list`, `diff`, `check` (`unknown` for an unparsable command line) |
| `ok` | bool | `exit_code == 0` |
| `status` | string | `success`, `failed`, `interrupted`, or `planned` (a `--dry-run`) |
| `exit_code` | int | see above |
| `error` | object \| null | `{code, message, retryable, hint}`; `hint` may be null |
| `source` | object \| null | the listed side (left side of a diff) |
| `target` | object \| null | the right side of a diff |
| `output` | object \| null | what was written |
| `stats` | object \| null | what was listed |
| `diff` | object \| null | diff counts |
| `resume` | object \| null | checkpoint state (`list -o DIR`) |
| `plan` | object \| null | `--dry-run` only |
| `check` | object \| null | `check` only |
| `warnings` | string[] | non-fatal issues |

`source` / `target`:

| Key | |
|---|---|
| `url` | `s3://bucket/prefix` |
| `bucket`, `prefix` | |
| `region` | the signing region used (`null` in a plan when it is detected at run time) |
| `endpoint_url` | the endpoint, credentials and query removed; `null` means AWS S3 |
| `path_style` | path-style addressing |

`output`:

| Key | |
|---|---|
| `format` | `parquet`, `ndjson` or `none` |
| `path` | the dataset directory, `s3://bucket/prefix/`, `-`, or `null` |
| `files` | committed parts of the dataset, including earlier resumed runs': `[{path, rows, bytes}]` (`s3://` URLs for an S3 output) |
| `rows` | rows this run wrote |

`stats`:

| Key | |
|---|---|
| `objects` | objects listed (a diff: distinct keys compared; `--versions`: versions and delete markers) |
| `folders` | folder rows of a `--delimiter` listing |
| `bytes` | total object size (a diff: of the differing keys) |
| `elapsed_secs` | wall time |
| `throttled` | requests the endpoint rejected for rate (`SlowDown`, 429, …) |
| `timeouts` | requests that timed out and were retried |
| `top_prefixes` | the 10 directories (key prefix up to the last `/`) with the most objects: `[{prefix, objects, bytes}]` |
| `requests` | listing requests sent (pages, retries included) |
| `probes` | partitioning probes sent (startup discovery and runtime splits) |
| `segments` | segments listed (startup partitions and split children) |
| `avg_concurrency` | request slots in use on average over the run (a listing page takes a slot, a probe a third; a diff adds its sides') |
| `peak_concurrency` | most request slots in use at once (at most `-c`; a diff adds its two sides' peaks) |

`diff`: `{left_only, right_only, changed, equal, verified_equal,
unverified}` — key counts. `verified_equal` (included in `equal`) and
`unverified` (included in `changed`) are the same-size pairs with different
ETags that `--compare checksum` proved equal or could not settle; both are 0
in the other modes.

`resume`: `{resumed, checkpoint, remaining_ranges}` — `resumed` is true when
this run continued a checkpoint; `checkpoint` is the path of the checkpoint
left for `--resume` (null when the listing is complete). After a resume,
`stats` describe the last run; the dataset's rows are the sum of
`output.files[].rows`.

`plan` (dry run): `partitioning` (`startup_discovery`, `single_segment`,
`checkpoint`), `partitioning_reason`, `concurrency` (what each side starts
with: `-c`, or 100; without `-c` a side raises it, up to 1,024, when the
endpoint's latency calls for it, and `stats.peak_concurrency` shows what it
reached), `compare` (`etag`,
`size`, `checksum`; null for `list`), `versions` (bool; null for `diff`),
`max_keys`,
`resume_ranges`, `first_part`, `region` (how the region will be chosen).

`check`: `credentials` (`{found, expires_at}` or `{found: false, error}`),
`region`, `endpoint_url`, `path_style`, `request_url` (the URL a listing
request goes to), `proxy` (the proxy it goes through, or null), and `probe`
(with an S3 URL): `{endpoint_url, region, bucket, addressing_style,
overall_status: compatible|partial|incompatible, tests: [...]}`.
`check` fails (exit 3 or 4) when no credentials are found, when the probe is
`incompatible`, or when every ListObjectsV2 test fails (even if HeadBucket
answered); `source` describes the URL it checked.

## Error codes

| Code | Exit | Meaning |
|---|---|---|
| `usage.invalid_argument` | 2 | The command line does not parse. |
| `usage.invalid_url` | 2 | Not `s3://bucket[/prefix]`. |
| `usage.invalid_endpoint` | 2 | The endpoint is not an `http(s)://` URL. |
| `usage.invalid_output` | 2 | `-o` value not allowed here (e.g. `diff -o -`). |
| `usage.output_not_a_directory` | 2 | `-o` names an existing file. |
| `usage.output_not_empty` | 2 | `-o DIR` is not empty (the hint says whether it holds a resumable run), or `-o s3://…` already holds objects. |
| `usage.resume_needs_output` | 2 | `--resume` without a local `-o DIR` (an S3 output is not resumable). |
| `usage.no_checkpoint` | 2 | `--resume` into a directory without a checkpoint that did not finish either (a finished one is reported, exit 0). |
| `usage.checkpoint_mismatch` | 2 | The checkpoint belongs to another run (bucket, prefix, endpoint or options); the message names both values. Endpoints compare without case, default port or trailing slash. |
| `usage.invalid_trace` | 2 | `--trace` names stdout, which carries the result. |
| `usage.checkpoint_unreadable` | 2 | The checkpoint cannot be read. |
| `auth.missing_credentials` | 3 | No credentials in the SDK chain. |
| `auth.expired_token` | 3 | Expired or invalid session token. |
| `auth.invalid_access_key` | 3 | The access key does not exist on this endpoint. |
| `auth.access_denied` | 3 | Not allowed to list the bucket. |
| `auth.signature_mismatch` | 3 | Wrong secret key, region or addressing style. |
| `config.wrong_region` | 3 | The bucket is in another region. |
| `config.missing_region` | 3 | No region could be resolved. |
| `config.no_such_bucket` | 3 | The bucket does not exist on this endpoint. |
| `endpoint.inconsistent_listing` | 3 | The endpoint returned keys out of order or repeated them. |
| `endpoint.unreachable` | 3 | The connection was refused or the host did not resolve before the endpoint ever answered (wrong `--endpoint-url`, DNS or proxy). Reported at once. |
| `net.throttled` | 4 | Rate limited until the retry budget ran out. |
| `net.timeout` | 4 | Requests kept timing out. |
| `net.unavailable` | 4 | The endpoint kept failing (5xx). |
| `net.error` | 4 | Other transport or unclassified service errors. |
| `output.write_failed` | 5 | Writing the dataset (a file or an S3 upload), stdout or the result file failed. |
| `internal.error`, `internal.panic` | 1 | A bug. |
| `interrupted` | 130 | SIGINT / SIGTERM. |

## Dataset directory

`-o DIR` must be absent or empty, except with `--resume`. After the run:

| File | |
|---|---|
| `part-NNNNN.parquet` | committed parts (zero-padded index) |
| `_result.json` | the result document |
| `_checkpoint.json` | present only when a stopped run can be resumed (`s3tl.checkpoint/1`) |
| `.part-NNNNN.parquet.tmp` | parts being written; never read these |

A `list` run that is interrupted, or that fails on the network or on its
setup (exit 3 or 4) after listing something, commits what it listed and
leaves `_checkpoint.json` for `--resume`. Any other failed run removes its
temporary parts and leaves the directory as it was (and removes it if it
created it and it is empty). **The dataset is complete exactly when
`_result.json` says `ok: true` and there is no `_checkpoint.json`.** List
rows are in no particular order (segments are written as they finish); a
diff dataset is in key order. The Parquet schemas
are in the [README](../README.md#output); each file's key-value metadata has
`s3tl.schema` = `s3tl.list/1`, `s3tl.list-versions/1` or `s3tl.diff/1`.

## S3 output (`-o s3://bucket/prefix/`)

The prefix must hold no objects. Parts are multipart uploads while the run
writes them and become objects only at commit: a successful run completes
them all (all or nothing) and then writes `prefix/_result.json`, so a reader
that waits for `_result.json` never sees a partial dataset. A failed or
interrupted run aborts its uploads and writes nothing. There is no
checkpoint and no `--resume`.

## Recipes

```bash
# Branch on the class, not the message.
out=$(s3tl list s3://b/p/ -o run/); code=$?
case $code in
  0) jq -r '.output.files[].path' <<<"$out" ;;
  4|130)                                                 # retryable, or interrupted
    if jq -e '.resume.checkpoint' <<<"$out" >/dev/null; then
      s3tl list s3://b/p/ -o run/ --resume              # continue where it stopped
    else
      sleep 60; s3tl list s3://b/p/ -o run/             # nothing was kept: start over
    fi ;;
  *) jq -r '.error.code, .error.hint' <<<"$out"; exit $code ;;
esac

# Are two prefixes identical?
s3tl diff s3://a/p/ s3://b/p/ | jq -e '.ok and .diff.left_only + .diff.right_only + .diff.changed == 0'

# Plan without touching S3.
s3tl list s3://b/p/ -o run/ --dry-run | jq .plan
```

## Trace events (`--trace`)

One JSON object per line, per listing request: `timestamp`, `operation`
(`ListObjectsV2`, or `ListObjectVersions` with `--versions`; `Probe` for a
partitioning probe, which names no request: it omits `endpoint_url`,
`bucket` and `addressing_style`, has an empty `prefix`, and reports its
outcome in `latency_ms`, `http_status` and `s3_error_code`), `endpoint_url` (redacted),
`region`, `addressing_style`, `bucket`, `prefix`, `delimiter` (always
present; `""` for a recursive listing), `start_after`, `max_keys`,
`continuation_token`, `http_status`, `s3_error_code`, `s3_error_message`,
`request_id`, `request_id_2`, `retry_attempt`, `latency_ms`, `retryable`,
`fatal`, `is_truncated`, `next_continuation_token`, `key_count`,
`contents_count`, `common_prefixes_count`, `first_key`, `last_key`, and a
`ListObjectsV2SegmentSummary` event per segment (`segment_index`,
`end_before`, `segment_pages`, `segment_objects`, `ended_by`). Optional
fields are omitted when empty. Fields are only ever added.
