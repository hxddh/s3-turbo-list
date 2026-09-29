# Agent-Friendly Usage

The machine-readable surfaces for AI agents, CI jobs, and shell automation:
dry-run plans, `doctor --json`, run manifests, `manifest-summary`, exit
codes, and traces.  The [README](../README.md) covers the commands
themselves; this page is the contract.

Options follow the command name
(`s3-turbo-list list --bucket b --region r --output-dir out`).  Only
`--config`, `--provider`, `--endpoint-url`, and `--addressing-style` are
global and may appear on either side.  The pre-0.37 spelling with other
options before the command name is still accepted until 0.39, with a
deprecation warning (see [Deprecations](#deprecations)).

## Deprecations

A deprecated spelling keeps working for one release and says so twice: a
`warning: deprecated …; it will be removed in 0.39` line on stderr, and the
same text (without `warning: `) in the JSON a consumer reads — the plan's
and the manifest's `warnings`, `config_source.warnings` for config keys, and
a `deprecated` `warn` check in `doctor`.  Under `--agent` the stderr line is
left out (stderr stays quiet); the JSON still carries it.  Deprecated in
0.38, removed in 0.39:

- Options other than the global ones written before the command name
  (`s3-turbo-list --output-dir out list …`): write them after it.
- The config keys `s3.profile` (use `s3.provider`) and
  `s3.force_path_style` (use `s3.addressing_style = "path"`).
- `--endpoint` (use `--endpoint-url`), `doctor --agent` and
  `manifest-summary --agent` (use `--json`), and the no-op
  `doctor --simple` and `doctor --fix-suggestions`.

## No-cloud preflight

None of these contact S3:

```bash
s3-turbo-list doctor --json
s3-turbo-list doctor --json --hints-file hints.toml --output-dir out
s3-turbo-list guide
s3-turbo-list list --bucket my-bucket --region us-east-1 --output-dir out --dry-run
s3-turbo-list list --bucket my-bucket --region us-east-1 --output-format summary --dry-run
s3-turbo-list diff --bucket left --target-bucket right --region us-east-1 --dry-run
s3-turbo-list manifest-summary run.json --json
s3-turbo-list manifest-summary run.json --check
```

### doctor

`doctor --json` checks the binary version, working directory, config parse
status and file, `AWS_PROFILE`, the provider preset, the endpoint, the proxy
that applies to the endpoint, and any outputs, trace file, hints file
(`--hints-file`, report embedded under `hints`), or filter (`--filter`) you
pass it; network probing is always `skipped`.  It prints the resolved
configuration under `resolved_config` (TOML, CLI overrides, provider preset,
and addressing-style normalization applied) and `config_source`: the
explicit `--config` path, the file actually loaded, the searched paths, the
source kind (`explicit`, `workspace`, `home`, or `none`), and CLI overrides.

The `proxy` check is decided from `HTTP(S)_PROXY` / `ALL_PROXY` / `NO_PROXY`
exactly as the run's HTTP client decides it, for an explicit path-style
endpoint.  When the request host depends on the bucket and region
(virtual-hosted or AWS) it is `skipped`, and the run log names the proxy for
each side's resolved endpoint before its first request.  Credentials in a
proxy URL are never printed.

Exit codes: an endpoint problem that stops every real run (a preset that
needs an explicit endpoint URL, or an endpoint with template placeholders) is
an `error` check and exits `3`, the code the run would exit with; any other
`error` check exits `2`.  `doctor --json` always prints JSON on stdout —
including for a config error (a `config_parse` `error` check), a hints file
it cannot read (`hints`), and a command-line usage error such as an unknown
option (`cli`), all exit `2`.  Such an early exit prints a shorter report:
`schema_version`, `tool_version`, `status: "error"`, `cwd`, and `checks`
holding the one `error` check, plus `config_source` and `resolved_config`
once the config file has been read (a usage error or a config file that
does not parse comes before that).  The message is also printed once on
stderr.  The human format is one compact list; the pre-0.37 `--simple` and
`--fix-suggestions` are deprecated no-ops.

An explicit `--config` path that does not exist is an error (exit `2`) for
every command that loads config, `doctor` and `--dry-run` included.

## Dry-run plans

`--dry-run` resolves inputs, planned output paths, the hints source,
checkpoint identity, output parent directories, and file conflicts, and
prints the plan JSON on stdout without creating files or making S3 requests.
`--output-dir` is safe in a dry run: it plans paths but does not create the
directory.  To keep the plan in a file, redirect stdout (`--plan-json` was
removed in 0.38):

```bash
s3-turbo-list list --bucket my-bucket --region us-east-1 \
  --output-parquet-file out/list.parquet --dry-run > plan.json
```

Top-level fields: `schema_version`, `tool_version`, `status`, `command`,
`network`, `inputs`, `outputs`, `config_source`, `resolved_config`, `hints`,
`checkpoint`, `file_conflicts`, `warnings`.

`status` is `ok`, or `blocked` when a problem would stop the real run: an
explicit `--hints-file` it cannot load (exit `2`; `hints.valid: false`, the
reason in `hints.warnings`), a provider setup problem (exit `3`) or an output
the run cannot create — a path under an existing file, or in a read-only
directory (exit `5`); the reason is in `warnings`.  A blocked plan is still
printed, then the dry run exits with that code and prints
`s3-turbo-list: run blocked (exit N): <reason>` on stderr, so it predicts
the run's exit class.  `warnings` is empty in the normal case: it carries
deprecations, problems, and notes about options that have no effect (such
as an explicit `--delimiter ''`, or output paths with
`--output-format summary`).  Writability is
judged with `access(2)`, so it is right for root as well.  Warnings also flag
an existing output file the run would overwrite, a `--prefix` starting with
`/`, and a missing region (`blocked` when `AWS_EC2_METADATA_DISABLED=true`
and nothing names a region; otherwise a warning, because IMDS may still
supply one).  Local input errors exit `2` as in the real run: a filter that
does not compile, or two outputs that resolve to the same file, stop before a
plan is printed (with the run line and, under `--agent`, the minimal JSON
result below); an explicit `--hints-file` that cannot be loaded exits `2`
after the plan (`status: blocked`, and the `hints` section carries the
error).

`network` is authoritative for dry-run behavior; it currently reads
`none: dry-run only resolves local configuration and planned paths`.

`hints.source` says how the run will partition its key space:

| Value | Meaning |
|---|---|
| `explicit` | `--hints-file` boundaries. |
| `startup_discovery` | The run probes the bucket structure at startup and partitions from it (every run; nothing is cached). |
| `disabled_single_segment_fallback` | `--no-auto-hints`: one starting segment (list mode fans it out by runtime splitting). |
| `delimiter_single_segment` | A `--delimiter` run: one hierarchical segment, never split. |
| `single_chain` | `--start-after`: one sequential chain, never split. |
| `diff_per_side_automatic` | `diff`: each side partitioned up front and listed in parallel. |
| `not_applicable` | `compat-probe`: a fixed set of probe requests, no partitioning. |

For `diff`, `single_chain`, `delimiter_single_segment`, and
`disabled_single_segment_fallback` leave each side one serial segment (diff
never splits at runtime), and carry the single-chain warning.

`checkpoint` describes resumability:

| Field | Meaning |
|---|---|
| `enabled` | The run saves a checkpoint at `path` if it is interrupted — `true` for every `list` run except `--start-after`; `false` for `diff`. |
| `resume` | The run reads the checkpoint at `path` (`--resume`). |
| `path` | `<region>_<bucket>[_<prefix-hash>]_checkpoint.toml`, in `--output-dir` when given, else the working directory. |
| `exists`, `valid`, `identity_matches`, `identity_mismatches` | State of the file at `path` and whether its identity matches this run. |
| `remaining_ranges` | Key ranges a resume would list. |
| `identity_fields` | The fields that make up the checkpoint identity (`provider` was `profile` up to 0.37; checkpoints written by 0.37 still load, and the preset name compares case-insensitively). |
| `resumed_segments_skipped` | See [Resumed runs](#resumed-runs). |

`outputs` lists every file the run writes: `parquet_file`, `ks_file`,
`hints_file`, `trace_compat`, `log_file`, and `report_file` (compat-probe's
`-o` report; `null` otherwise).  `inputs.provider` and
`resolved_config.s3.provider` hold the preset's canonical lowercase name.

The `command` array preserves the invoked argument shape with sensitive
values redacted (`--endpoint-url`, `--endpoint`, `--continuation-token`, and
`user:password@` in endpoint URLs; `resolved_config` redacts the latter too).
Use `inputs`, `outputs`, and `config_source` for exact values.

### Deprecated fields

Kept for one release, then removed; do not branch on them:

- `inputs.profile` (same value as `inputs.provider`) and
  `inputs.continuation_token` (always `null`: `--continuation-token` was
  removed in 0.38).
- `resolved_config.s3.profile_known` and `resolved_config.s3.profile_warnings`
  (use `provider_known` and `provider_warnings`).
- The trace event field `profile` (use `provider`).

Removed in 0.38 (deprecated in 0.37): `resolved_config.s3.profile`,
`resolved_config.s3.force_path_style`, `resolved_config.s3.debug_s3`, and
`checkpoint.completed_segments` / `checkpoint.total_segments`.

## Run manifests

```bash
s3-turbo-list list --bucket my-bucket --region us-east-1 --output-dir out \
  --run-manifest run.json
s3-turbo-list list --bucket my-bucket --region us-east-1 --output-dir out --agent
```

`--run-manifest` writes the manifest to a file; `--agent` prints it on stdout
at the end and keeps stderr quiet: no warnings, no log lines (the stderr log
filter is off unless `RUST_LOG` is set; a `--log` file keeps its normal
level), only the final run line on a non-zero exit.  The manifest includes
`status`
(`success`, `failed`, or `interrupted`), `exit_code`, `started_at`,
`finished_at`, `elapsed_secs`, `command` (redacted as in plans), `cwd`,
`inputs`, `outputs`, `config_source`, `artifacts`, `metrics`, `checkpoint`,
and `warnings`
(using the same wording as dry-run plans).  `inputs.filter` records the
`--filter` expression, or `null`.

`metrics` holds the data-map counters (received batches and objects,
streamed rows, unique prefixes, Parquet rows, KS entries, total bytes, top
prefixes, fatal listing errors, output write errors) plus endpoint pushback:
`throttled_responses` (keyed on the S3 error codes `SlowDown`,
`TooManyRequests`, `ThrottlingException`, not the HTTP status) and
`http_error_statuses` (a `{status, count}` histogram).  Retries are not
failures: `status: success` with `fatal_errors: 0` and many
`throttled_responses` means the endpoint was the constraint.

`artifacts` describes each generated file: `kind` (`parquet`, `ks`, `hints`,
`trace`, or `log`), `path`, `exists`, `size_bytes`, `sha256`, `line_count`
for line-oriented files (for KS, CSV records), and `parquet.row_count`,
`parquet.row_group_count`, `parquet.schema_fields` for Parquet.  A pooled
list run writes several Parquet part-files, one `parquet` entry each;
`metrics.parquet_rows` is the total across them.

TSV and NDJSON reserve stdout for rows, so they cannot be combined with
`--agent` in a real run (exit `2`); write `--run-manifest` to a file instead:

```bash
s3-turbo-list list --bucket my-bucket --region us-east-1 \
  --output-format ndjson --run-manifest run.json > objects.ndjson
s3-turbo-list manifest-summary run.json --json
```

`--output-format summary` returns aggregate counts without Parquet/KS files;
it still scans the bucket unless combined with `--dry-run`.

### manifest-summary

`manifest-summary` is local-only.  `--check` gives a single validation exit
code (`6` on a mismatch): it checks the saved status, exit code, fatal and
output error counters, Parquet row equality where Parquet output applies, and
each recorded artifact on disk — current size, SHA256, and Parquet
row/schema metadata.  A Parquet artifact recorded without that metadata
fails, and `artifact_parquet_rows_total` checks that the Parquet artifacts'
rows add up to `metrics.parquet_rows`.  For `summary`, `tsv`, and `ndjson`
manifests, Parquet row equality is reported as not applicable.

Relative artifact paths resolve against the manifest's `cwd`, then the
current directory, then the manifest's directory.  With `--json`, the
top-level `check` object gives pass/fail counts, artifact counts, and
row/schema/exit-code status values; its `parquet_schema_check` is the worst
status across every Parquet artifact.  Individual checks in `checks` are
named `<check>:<kind>`; when several artifacts share a kind, the first keeps
the bare name and the rest get an index suffix (`artifact_sha256:parquet#1`).

## Resumed runs

Every interrupted `list` run (Ctrl-C or SIGTERM, exit `7`) saves a
checkpoint; `--resume` reads it.  `checkpoint.resumed_segments_skipped` in the
manifest is the field to branch on afterwards: `null` means the run did not
resume; a number greater than zero means the checkpoint recorded that many
key ranges as already written, so **this run's artifacts cover only the rest
of the key space** and must be combined with the interrupted run's outputs —
together they hold every key exactly once.  Reusing one output path for both
runs leaves only the second half, and the run warns about it.  Read this
field, not the checkpoint file: a run that lists its whole key space removes
the checkpoint before exiting.

`diff` never checkpoints (`diff --resume` is a usage error): partial paired
checkpointing could hide left-only or right-only objects.

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | Success |
| 1 | Unexpected internal error |
| 2 | CLI/config/filter validation error (including command-line usage errors) |
| 3 | Auth/provider/region/endpoint setup error — detected before the scan (e.g. no region resolved) or returned by the endpoint as a permanent error (`AccessDenied`, `NoSuchBucket`, `SignatureDoesNotMatch`, `AuthorizationHeaderMalformed`, `PermanentRedirect`). Re-running unchanged will not help. |
| 4 | Network timeout, retry exhaustion, or other fatal listing error |
| 5 | Output filesystem, manifest, Parquet, or KS write error |
| 6 | Data validation, schema, or checksum error (`manifest-summary --check`) |
| 7 | Interrupted; a checkpoint is saved for `list` runs |

Branch on the exit code first, then read the manifest if it exists.  Every
non-zero exit of `list`, `diff`, or `compat-probe` — dry runs included —
prints one `s3-turbo-list: run <status> (exit N): <reason>` line on stderr
(`<status>` is `failed`, `interrupted`, or `blocked` for a dry run whose plan
was printed).  A command-line usage error (unknown or misplaced option) is
reported by the argument parser in its own format; it adds the run line only
under `--agent`.  A run that stops before listing — a usage error, a filter
that does not compile, no region, an output it cannot create — still
prints, under `--agent`, a minimal JSON result on stdout instead of a
manifest (a dry run whose plan was printed prints no second document):

```json
{
  "error": "unexpected argument '--bogus' found",
  "exit_code": 2,
  "schema_version": "s3-turbo-list.agent.v1",
  "status": "failed",
  "tool_version": "…"
}
```

A failed listing records its first fatal error (S3 error code, HTTP status,
message) in the manifest's `warnings`; a diff side's failure (for example
`AccessDenied` on the target bucket) exits with that side's class (`3` /
`4`).  SIGTERM (as sent by `timeout(1)` and most harnesses) is handled like
Ctrl-C: exit `7`, with the manifest and checkpoint written.

A non-zero exit means the outputs are not trustworthy even if they exist and
parse.  A `list` run that failed mid-listing (exit `4`) leaves a structurally
valid Parquet file holding only the keys it reached.  A failed `diff` removes
its outputs, since a partial merge cannot describe both sides.  Only treat
outputs as complete on exit `0`.

## Filters

`--filter` is validated before any request.  In `list`, `SOURCE.size` and
`SOURCE.last_modified` are available; `diff` adds `TARGET.size` and
`TARGET.last_modified`.  Function calls, methods, strings, arrays, maps,
indexing, statements, and large or deeply nested expressions are rejected
with exit `2` — a local configuration error, not a provider failure.
`doctor --filter '<expr>'` checks an expression without running anything.

## S3 API trace

`--trace-compat trace.jsonl` records every listing page request as JSONL —
request IDs, HTTP status, S3 error codes, pagination metadata, retry details
— plus one summary event per completed segment, and combines with
`--run-manifest`.  Startup discovery, flat-namespace bisection, and runtime
split probes are not traced.  `--trace-compat -` writes to stderr.  The
field reference is in [tuning.md](tuning.md#trace-event-fields).

## Safety expectations

- `doctor`, `guide`, `manifest-summary`, and `--dry-run` are local-only.
- `list`, `diff`, and `compat-probe` contact S3 unless run with `--dry-run`.
- `--agent` changes only what is printed, never listing behavior.
