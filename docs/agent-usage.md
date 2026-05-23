# Agent-Friendly Usage

This document describes the machine-readable surfaces intended for AI agents,
CI jobs, and shell automation.  The default human CLI remains unchanged.

## No-cloud preflight

These commands do not contact S3 endpoints:

```bash
s3-turbo-list config-inspect --json
s3-turbo-list doctor --local-only --json
s3-turbo-list doctor --local-only --simple --fix-suggestions
s3-turbo-list init-config --output s3-turbo-list.toml
s3-turbo-list recipes agent-safe
s3-turbo-list recipes summary
s3-turbo-list recipes pipe
s3-turbo-list recipes filter
s3-turbo-list recipes verify
s3-turbo-list recipes release-check
s3-turbo-list recipes diff-safe
s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list --bucket my-bucket --region us-east-1
s3-turbo-list --dry-run --agent --summary-only --delimiter '' list --bucket my-bucket --region us-east-1
s3-turbo-list manifest-summary run.json --json
s3-turbo-list manifest-summary run.json --check
s3-turbo-list trace-summary trace.jsonl --machine-readable
s3-turbo-list hints-merge hints-a.toml hints-b.txt --output merged.toml --machine-readable
s3-turbo-list hints-rebalance --trace trace.jsonl --hints-file merged.toml --dry-run --machine-readable
```

`config-inspect --json` prints the resolved local configuration after TOML,
CLI overrides, profile presets, and addressing-style normalization.

`doctor --local-only --json` checks the binary version, current working
directory, config parse status, local config file presence, `AWS_PROFILE`,
endpoint compatibility profile status, local output parent directories, and
explicitly marks network probing as skipped.  `doctor --simple` is intended for
compact human output; agents should prefer `--json`.

`--dry-run` resolves command inputs, planned output paths, hints source,
checkpoint identity, output parent directories, and local file conflicts
without creating Parquet/KS files and without making S3 requests.
`--output-dir` is safe in dry-run: it plans Parquet/KS paths but does not create
the directory until a real `list` or `diff` run.

`init-config`, `recipes`, `quickstart`, `cheatsheet`, `trace-summary`,
`hints-merge`, and `hints-rebalance` are local tooling commands.  They are
handled before S3 config loading, do not require cloud credentials, and do not
change list/diff hot-path behavior.

## Dry-run plan files

Use `--plan-json` when stdout should stay quiet:

```bash
s3-turbo-list --dry-run \
  --plan-json plan.json \
  --delimiter '' \
  --output-parquet-file out/list.parquet \
  --output-ks-file out/list.ks \
  list --bucket my-bucket --region us-east-1
```

The plan JSON includes:

- `schema_version`
- `tool_version`
- `command`
- `network`
- `inputs`
- `outputs`
- `resolved_config`
- `hints`
- `checkpoint`
- `file_conflicts`
- `warnings`

Agents should treat `network` as authoritative for dry-run behavior.  Current
dry-run reports `none: dry-run only resolves local configuration and planned
paths`.

Agents should also inspect `warnings`.  For example, a warning that `--profile`
is only an endpoint compatibility preset means credentials still come from
`AWS_PROFILE` or the standard AWS SDK credential chain.

When a hints file is present, `hints` includes parse status, format, boundary
count, warnings, and estimate summary metadata.  When `--resume` is set and a
checkpoint file exists, `checkpoint` reports parse status, completed/total
segments, and identity match details.

For `diff`, dry-run reports `hints.source =
"disabled_for_diff_single_segment"`.  Conventional hints caches are
intentionally ignored, and explicit `diff --hints-file` exits with code `2`
before any S3 request.  Agents should treat current `diff` runs as
authoritative single-segment comparisons until paired-segment diff coordination
lands in `v0.2.x`.  Current diff mode retains its comparison map in memory until
both sides complete; for very large bucket-to-bucket comparisons, agents should
plan memory capacity from the combined key count or split the comparison into
smaller external ranges.

## Run manifests

For real listing runs, write a final manifest:

```bash
s3-turbo-list --run-manifest run.json --delimiter '' list \
  --bucket my-bucket \
  --region us-east-1 \
  --output-parquet-file out/list.parquet \
  --output-ks-file out/list.ks
```

With `--agent`, the same manifest is also printed to stdout at the end:

```bash
s3-turbo-list --agent --run-manifest run.json --delimiter '' list \
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

Use `--summary-only` when an agent needs aggregate object count, byte count, or
top-prefix distribution without writing Parquet/KS artifacts.  This is not a
dry-run: it scans S3 unless combined with `--dry-run`.

Use `list --output-format ndjson` when an agent needs object rows as a stream:

```bash
s3-turbo-list --run-manifest run.json --delimiter '' \
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
human text.

Run manifest `warnings` use the same guardrail wording as dry-run plans so
agents can compare preflight and completed runs consistently.

Endpoint compatibility profiles that require provider-specific endpoints warn
in dry-run and `doctor` until an endpoint URL is configured.  Placeholder
endpoints from starter configs, such as `<account-id>` or `<region>`, are also
reported locally before a real run.

Object filters are validated before any listing run.  Agents can use simple
numeric predicates such as:

```bash
s3-turbo-list --filter 'SOURCE.size > 1073741824' \
  --run-manifest run.json --delimiter '' \
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

## Stable exit codes

| Code | Meaning |
|---:|---|
| 0 | Success |
| 1 | Unexpected internal error |
| 2 | CLI/config/filter validation error |
| 3 | Auth/profile/region/provider setup error |
| 4 | Network timeout, retry exhaustion, or tracked fatal listing error |
| 5 | Output filesystem, manifest, Parquet, or KS write error |
| 6 | Data validation, schema, or checksum error |
| 7 | Interrupted; checkpoint may be available |

Agents should branch on exit codes first, then read `run.json` if it exists.

## S3 API trace

`--trace-compat trace.jsonl` remains the per-request S3 API trace.  It is
separate from `--run-manifest`:

```bash
s3-turbo-list --trace-compat trace.jsonl --run-manifest run.json list \
  --bucket my-bucket \
  --region us-east-1
```

Use the manifest for final run status and aggregate metrics.  Use trace JSONL
for endpoint behavior, request IDs, HTTP status, S3 error codes, pagination
metadata, and retry details.

## Trace-driven hints tooling

The local hints tooling commands support machine-readable output for agents:

```bash
s3-turbo-list trace-summary trace.jsonl --output-format json

s3-turbo-list hints-merge \
  base.toml prefixes.txt \
  --output merged.toml \
  --emit-manifest merge.manifest.json \
  --machine-readable

s3-turbo-list hints-rebalance \
  --trace trace.jsonl \
  --hints-file merged.toml \
  --output rebalanced.toml \
  --emit-manifest rebalance.manifest.json \
  --machine-readable
```

`--machine-readable` is an alias for JSON report output on these commands.
Warnings and recommendations are JSON fields, so agents should not scrape human
text.  `--emit-manifest` records input/output file hashes for reproducibility.

`hints-rebalance` is conservative.  It only adds boundaries from observed
per-page trace key samples and only for segments that exceed the configured
long-tail threshold.  If a trace was produced by an older binary and lacks
`last_key` samples, the command reports the long-tail segments but does not
guess synthetic boundaries.

## Safety expectations

- `config-inspect`, `doctor --local-only`, and `--dry-run` are local-only.
- `trace-summary`, `hints-merge`, and `hints-rebalance` are local-only.
- `list`, `diff`, `auto-hints`, `discover-prefixes`, and `compat-probe` can contact S3 unless
  combined with `--dry-run`.
- Provider-specific caveats still apply; `--agent` does not enable BOS
  pagination workarounds or change hot-path listing behavior.
