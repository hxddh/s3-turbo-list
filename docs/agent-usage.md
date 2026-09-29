# Agent-Friendly Usage

This document describes the machine-readable surfaces intended for AI agents,
CI jobs, and shell automation.  The default human CLI remains unchanged.

## No-cloud preflight

These commands do not contact S3 endpoints:

```bash
s3-turbo-list doctor --json
s3-turbo-list doctor --simple --fix-suggestions
s3-turbo-list init-config --output s3-turbo-list.toml
s3-turbo-list guide agent-safe
s3-turbo-list guide summary
s3-turbo-list guide pipe
s3-turbo-list guide filter
s3-turbo-list guide verify
s3-turbo-list guide release-check
s3-turbo-list guide diff-safe
s3-turbo-list --dry-run --agent --output-dir out list --bucket my-bucket --region us-east-1
s3-turbo-list --dry-run --agent --summary-only list --bucket my-bucket --region us-east-1
s3-turbo-list manifest-summary run.json --json
s3-turbo-list manifest-summary run.json --check
s3-turbo-list doctor --hints-file hints.toml --json
```

`doctor --json` prints the resolved local configuration (under
`resolved_config`) after TOML, CLI overrides, profile presets, and
addressing-style normalization, alongside its environment checks. It also
includes `config_source`, which reports the explicit `--config` path when
present, the config file actually loaded, the searched paths, the source kind
(`explicit`, `workspace`, `home`, or `none`), and global CLI config overrides
such as `compression` or `endpoint_url`.
An explicit `--config` path that does not exist is an error (exit `2`), for
every command that loads config — including `doctor` and `--dry-run`.  It used
to fall back to built-in defaults, which would list the same-named bucket on
AWS instead of the configured endpoint.  `doctor --json` prints its JSON
report (with a `config_parse` `error` check) on every config error, so stdout
is never empty.

`doctor --json` checks the binary version, current working
directory, config parse status, local config file presence, `AWS_PROFILE`,
endpoint compatibility profile status, local output parent directories, and
explicitly marks network probing as skipped.  An endpoint problem that stops
every real run — a profile that requires an explicit endpoint URL, or an
endpoint still containing template placeholders — is an `error` check, and
`doctor` then exits `3`, the same code the real run would exit with.  Other
`error` checks exit `2`.  `doctor --simple` is intended for
compact human output; agents should prefer `--json`.

`--dry-run` resolves command inputs, planned output paths, hints source,
checkpoint identity, output parent directories, and local file conflicts
without creating Parquet/KS files and without making S3 requests.
`--output-dir` is safe in dry-run: it plans Parquet/KS paths but does not create
the directory until a real `list` or `diff` run.
Compression choices are also visible in `resolved_config`: agents can use
`--compression` and `--compression-level` for one-off Parquet output runs
without editing TOML.  The default is `zstd(1)`; use
`--compression gzip --compression-level 6` when a downstream reader requires
traditional gzip output.

`init-config` and `guide`
are local tooling commands.  They are handled before S3 config loading, do not
require cloud credentials, and do not change list/diff hot-path behavior.
`doctor --hints-file hints.toml` validates a hints file locally and embeds the
report under `hints` in its JSON output.

## Dry-run plan files

Use `--plan-json` when stdout should stay quiet:

```bash
s3-turbo-list --dry-run \
  --plan-json plan.json \
  --output-parquet-file out/list.parquet \
  --output-ks-file out/list.ks \
  list --bucket my-bucket --region us-east-1
```

The plan JSON includes:

- `schema_version`
- `tool_version`
- `status`
- `command`
- `network`
- `inputs`
- `outputs`
- `config_source`
- `resolved_config`
- `hints`
- `checkpoint`
- `file_conflicts`
- `warnings`

`status` is `ok`, or `blocked` when a provider setup problem would stop the
real run with exit code `3` (the reason is in `warnings`).  A blocked dry run
still prints or writes the plan, then exits `3`, so the dry run predicts the
run's exit class.

`hints.source` for `list` says how the run will partition its key space:

| Value | Meaning |
|---|---|
| `explicit` | `--hints-file` boundaries. |
| `auto_cache` | Boundaries cached by an earlier run's startup discovery. |
| `startup_discovery` | No cache yet: the run probes the bucket structure at startup, partitions from it, and caches the boundaries. |
| `disabled_single_segment_fallback` | `--no-auto-hints`: one starting segment, fanned out by runtime splitting. |
| `delimiter_single_segment` | A `--delimiter` run: one hierarchical segment, never split. |
| `single_chain` | `--start-after` / `--continuation-token`: one sequential chain, never split. |

Only the last two add the "single ListObjectsV2 chain" warning; the others
partition the run, so `--concurrency` adds parallelism to them.

Agents should treat `network` as authoritative for dry-run behavior.  Current
dry-run reports `none: dry-run only resolves local configuration and planned
paths`.

The `command` array preserves the invoked argument shape for diagnostics, but
sensitive option values are redacted.  Current redactions include
`--endpoint-url`, `--endpoint`, and `--continuation-token` values.

Agents should also inspect `warnings`.  For example, a warning that `--profile`
is only an endpoint compatibility preset means credentials still come from
`AWS_PROFILE` or the standard AWS SDK credential chain.

When a hints file is present, `hints` includes parse status, format, boundary
count, and warnings.  When `--resume` is set and a
checkpoint file exists, `checkpoint` reports parse status, completed/total
segments, and identity match details.

`checkpoint.resumed_segments_skipped` is the field to branch on after a
resumed run.  `null` means the run did not resume; a number greater than zero
means the run skipped that many segments because a checkpoint recorded them
complete, so **its artifacts describe only the rest of the key space** — the
remainder is in the output of the run that was interrupted, and the two must
be combined.  Reusing one output path across both runs leaves only the second
run's half; the run also emits a warning saying so.  Read this field rather
than the checkpoint file: a run that lists its whole key space removes the
checkpoint before exiting, so the file on disk cannot answer the question.

For large buckets, agents should prefer the simple high-throughput path:
run `list` directly with the default concurrency: it is an upper bound the run
settles below on its own, so pinning `-c` lower only caps a large bucket.
Lower it only when the endpoint throttles (`metrics.throttled_responses`).
Key-space partitioning is automatic — startup discovery probes the bucket
structure and caches boundaries on the first run, and runtime splitting fans
out long-tail segments — so no separate hints-generation step is needed.
Empty delimiter is omitted from ListObjectsV2 requests, which keeps recursive
listing compatible with providers that reject `delimiter=`.

For `diff`, dry-run reports `hints.source = "diff_per_side_automatic"`.  Explicit
`diff --hints-file` exits with code `2` before any S3 request, but each side is
still partitioned and listed in parallel — by cached or startup-discovered
`CommonPrefixes`, or by an up-front single-key bisection when a side is flat.
The segment set is fixed up front (not re-split mid-run the way list mode does)
so the ordered merge can consume it in key order.  `diff --resume` is
intentionally unsupported because partial paired checkpointing can hide
left-only or right-only objects.  Diff streams an ordered merge of the two
sides, so memory stays bounded regardless of bucket size — agents do not need to
plan memory capacity from the combined key count.

## Run manifests

For real listing runs, write a final manifest:

```bash
s3-turbo-list --run-manifest run.json list \
  --bucket my-bucket \
  --region us-east-1 \
  --output-parquet-file out/list.parquet \
  --output-ks-file out/list.ks
```

With `--agent`, the same manifest is also printed to stdout at the end:

```bash
s3-turbo-list --agent --run-manifest run.json list \
  --bucket my-bucket \
  --region us-east-1
```

The manifest includes:

- `status`: `success`, `failed`, or `interrupted`
- `exit_code`
- `started_at`, `finished_at`, `elapsed_secs`
- `inputs`
- `outputs`
- `artifacts`
- `metrics`
- `checkpoint`
- `warnings`

The `metrics` object includes data-map counters such as received batches,
received objects, streamed rows, unique prefixes, Parquet rows, KS entries,
total bytes, top prefixes, fatal listing errors, and output write errors.

Two fields report endpoint pushback, so a run that was slow because it was
being rate-limited can be told apart from one that was slow because the
bucket is large:

- `throttled_responses`: responses the endpoint rejected for rate
  (`SlowDown`, `TooManyRequests`, `ThrottlingException`).  Keyed on the S3
  error code, not the HTTP status, because 503 also carries plain
  `ServiceUnavailable`.
- `http_error_statuses`: the `{status, count}` histogram of error responses
  the run saw, empty when there were none.

Retries are not failures: a run can finish with `status: success`,
`fatal_errors: 0` and a large `throttled_responses`.  That combination means
the listing completed but the endpoint was the constraint.

`inputs.filter` records the `--filter` expression the run applied, or `null`
for no filter.  Two runs over one bucket under different filters produce
different artifacts; this field is what tells their manifests apart.

The manifest `command` array uses the same sensitive value redaction as dry-run
plans.  Use `inputs`, `outputs`, and `config_source` for exact structured run
details instead of trying to recover secret-adjacent values from `command`.

Use `--summary-only` when an agent needs aggregate object count, byte count, or
top-prefix distribution without writing Parquet/KS artifacts.  This is not a
dry-run: it scans S3 unless combined with `--dry-run`.

Use `list --output-format ndjson` when an agent needs object rows as a stream:

```bash
s3-turbo-list --run-manifest run.json \
  list --bucket my-bucket --region us-east-1 --output-format ndjson > objects.ndjson
s3-turbo-list manifest-summary run.json --json
```

TSV and NDJSON reserve stdout for rows.  Do not combine them with `--agent`;
write `--run-manifest` to a file and summarize that manifest locally instead.
`manifest-summary` is local-only and does not load credentials or contact S3.

Use `manifest-summary --check` when an agent needs a single local validation
exit code.  It checks the saved manifest status, exit code, fatal/output error
counters, Parquet row equality when Parquet output applies, and recorded
artifact paths on the local filesystem.  When the manifest includes artifact
metadata, it also verifies current file size, SHA256, and Parquet row/schema
metadata.  For `summary-only`, `tsv`, and `ndjson` manifests, Parquet row
equality is reported as not applicable rather than a failure.

With `--json`, the top-level `check` object gives agents stable pass/fail
counts, artifact counts, and row/schema/exit-code status values without parsing
human text.  Its `parquet_schema_check` reports the worst status across every
Parquet artifact, so a pooled run's part-files cannot hide behind the base
file.

Individual checks in `checks` are named `<check>:<kind>`.  When a run records
several artifacts of one kind — a pooled list run writes one `parquet` entry
per writer — the first keeps the bare name and the rest are suffixed with
their index (`artifact_sha256:parquet#1`), so every check name in a report is
unique and attributable to one file.

Run manifest `warnings` use the same guardrail wording as dry-run plans so
agents can compare preflight and completed runs consistently.
Run manifests also include `config_source`, so a saved manifest is enough to
see which TOML file and CLI overrides shaped the completed run.

Endpoint compatibility profiles that require provider-specific endpoints are
reported by dry-run and `doctor` until an endpoint URL is configured.
Placeholder endpoints from starter configs, such as `<account-id>` or
`<region>`, are also reported locally before a real run.  These are
deterministic provider setup problems: real cloud-facing commands stop with
exit code `3`, and so do `doctor` and `--dry-run` (plan `status: blocked`).

For `diff` with a region-templated profile (`bos`, `b2`, `oss`) and no explicit
endpoint, each side uses the profile's endpoint for its own region: the target
side lists against the `--target-region` endpoint, which the dry-run plan names
in `warnings`.  An explicit `--endpoint-url` / `s3.endpoint_url` applies to
both sides.

Object filters are validated before any listing run.  Agents can use simple
numeric predicates such as:

```bash
s3-turbo-list --filter 'SOURCE.size > 1073741824' \
  --run-manifest run.json \
  list --bucket my-bucket --region us-east-1
```

In `list`, `SOURCE.size` and `SOURCE.last_modified` are available.  In `diff`,
`TARGET.size` and `TARGET.last_modified` are also available.  Function calls,
methods, strings, arrays, maps, indexing, statements, and large or deeply nested
expressions are rejected with exit code `2`.  Treat a filter rejection as a
local configuration error, not a network or provider failure.

The `artifacts` array describes generated files:

- `kind`: `parquet`, `ks`, `hints`, `trace`, or `log`
- `path`
- `exists`
- `size_bytes`
- `sha256`
- `line_count` for line-oriented files
- `parquet.row_count`, `parquet.row_group_count`, and `parquet.schema_fields`
  for Parquet outputs

A pooled list run scales to several Parquet writers, each streaming its own
part-file, so the array can hold more than one `parquet` entry: the base path
plus `<name>.partN.parquet` for each extra writer.  Read all of them — each
carries its own `sha256` and `row_count`, and `metrics.parquet_rows` is the
total across the set.

## Stable exit codes

| Code | Meaning |
|---:|---|
| 0 | Success |
| 1 | Unexpected internal error |
| 2 | CLI/config/filter validation error |
| 3 | Auth/profile/region/provider setup error — detected before the scan (e.g. no region resolved), or returned by the endpoint as a permanent error (`AccessDenied`, `NoSuchBucket`, `SignatureDoesNotMatch`, `AuthorizationHeaderMalformed`, `PermanentRedirect`). Re-running unchanged will not help. |
| 4 | Network timeout, retry exhaustion, or other fatal listing error |
| 5 | Output filesystem, manifest, Parquet, or KS write error |
| 6 | Data validation, schema, or checksum error |
| 7 | Interrupted; checkpoint may be available |

Agents should branch on exit codes first, then read `run.json` if it exists.
Every non-zero exit also prints one `s3-turbo-list: run <status> (exit N): <reason>`
line on stderr, and a failed listing records its first fatal error (S3 error
code, HTTP status, message) in the manifest's `warnings`.  SIGTERM (as sent by
`timeout(1)` and most harnesses) is handled like Ctrl-C: exit 7, with the
manifest and any checkpoint written.

A non-zero exit means the output artifacts are not trustworthy, even when they
exist and parse.  A `list` run that failed mid-listing (exit 4) leaves a
structurally valid Parquet file holding only the keys it got to.  A `diff`
whose side failed removes its output file instead, because a partial merge
cannot describe both sides — an aborted diff leaves no Parquet and no KS file,
so their absence next to a non-zero exit is expected.  Only treat outputs as
complete on exit 0.

## S3 API trace

`--trace-compat trace.jsonl` records every S3 API call as JSONL — endpoint
behavior, request IDs, HTTP status, S3 error codes, pagination metadata, and
retry details. It is separate from `--run-manifest`, and the two combine:

```bash
s3-turbo-list --trace-compat trace.jsonl --run-manifest run.json list \
  --bucket my-bucket \
  --region us-east-1
```

Use the manifest for final run status and aggregate metrics; use the trace for
per-request endpoint behavior. Long-tail segments split at runtime, so the
trace is for inspection only — there is no offline rebalancing step. The field
schema is documented in [trace-reference.md](trace-reference.md).

## Safety expectations

- `doctor` and `--dry-run` are local-only.
- `list`, `diff`, and `compat-probe` can contact S3 unless combined with
  `--dry-run`.
- Provider-specific caveats still apply; `--agent` does not change hot-path
  listing behavior.
