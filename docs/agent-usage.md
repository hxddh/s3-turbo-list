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
endpoint compatibility profile status, the proxy that applies to the
configured endpoint (`proxy`: decided from `HTTP(S)_PROXY` / `ALL_PROXY` /
`NO_PROXY` exactly as the run's HTTP client decides it, for an explicit
path-style endpoint; when the request host depends on the bucket and region
— virtual-hosted or AWS — the check is `skipped`, and the run log names the
proxy for each side's resolved endpoint before its first request; credentials
in the proxy URL are never printed), local output parent directories, and
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

`status` is `ok`, or `blocked` when a problem would stop the real run: a
provider setup problem (exit code `3`) or an output file it cannot create —
a path under an existing file (e.g. `--output-dir` naming a file) or in a
read-only directory (exit code `5`); the reason is in `warnings`.  A blocked
dry run still prints or writes the plan, then exits with that code, so the dry
run predicts the run's exit class.  An existing output file that the run would
overwrite gets a warning, and a `--prefix` starting with `/` (which ordinary
S3 keys never match) gets one too.  A missing region is `blocked` when the instance metadata
service is disabled (`AWS_EC2_METADATA_DISABLED=true`) and neither the
environment nor the AWS profile file names one; otherwise it is a warning,
because the SDK may still resolve it from IMDS at run time.  Local input errors
exit `2`, as they would in the real run: a filter that does not compile, or two
outputs (Parquet and its `.partN` names, KS, log, trace, run manifest, plan
JSON) that resolve to the same file, stop before a plan is printed; an explicit
`--hints-file` that cannot be loaded exits `2` after the plan (whose `hints`
section carries the parse error) is printed or written.

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
means a checkpoint recorded that many key ranges as already written (in whole
or in part, by the interrupted run or a chain of earlier resumes), so **its
artifacts describe only the rest of the key space** — the remainder is in the
output of the interrupted run(s), and the outputs must be combined; together
they hold every key exactly once.  A dry-run plan reports how many ranges a
resume would list in `checkpoint.remaining_ranges`.  Reusing one output path across both runs leaves only the second
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

For `diff`, dry-run reports `hints.source = "diff_per_side_automatic"`, or
`single_chain` / `delimiter_single_segment` / `disabled_single_segment_fallback`
when `--start-after`, `--delimiter` or `--no-auto-hints` leave each side as one
serial segment (diff never splits at runtime, so these also carry the
single-chain warning).  Explicit
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
metadata; a Parquet artifact recorded without that metadata (its footer was
unreadable when the run ended) fails, and `artifact_parquet_rows_total`
checks that the Parquet artifacts' row counts add up to
`metrics.parquet_rows`.  For `summary-only`, `tsv`, and `ndjson` manifests,
Parquet row equality is reported as not applicable rather than a failure.

Relative artifact paths are resolved against the manifest's `cwd` (the
working directory of the run), then the current directory, then the
manifest's own directory, so `--check` works from wherever the agent runs it.

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

`diff` without `--target-region` lists the target in `--region` (the plan's
`inputs.target_region` shows it); it used to fall back to the ambient
`AWS_REGION`.  For `diff` with a region-templated profile (`bos`, `b2`, `oss`)
and no explicit endpoint, each side uses the profile's endpoint for its own
region: the target
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
- `line_count` for line-oriented files (for KS, the number of CSV records)
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
Every non-zero exit of `list`, `diff` or `compat-probe` also prints one
`s3-turbo-list: run <status> (exit N): <reason>` line on stderr — including a
run that stops before listing (a filter that does not compile, no region, an
output it cannot create), which under `--agent` also prints a minimal JSON
result (`status`, `exit_code`, `error`) on stdout instead of a manifest.  A
failed listing records its first fatal error (S3 error code, HTTP status,
message) in the manifest's `warnings`; a diff side's failure (for example
`AccessDenied` on the target bucket) exits with that side's class (`3` / `4`),
not as an output failure.  SIGTERM (as sent by
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
