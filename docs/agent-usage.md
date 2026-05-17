# Agent-Friendly Usage

This document describes the machine-readable surfaces intended for AI agents,
CI jobs, and shell automation.  The default human CLI remains unchanged.

## No-cloud preflight

These commands do not contact S3 endpoints:

```bash
s3-turbo-list config-inspect --json
s3-turbo-list doctor --local-only --json
s3-turbo-list --dry-run --agent list --bucket my-bucket --region us-east-1
```

`config-inspect --json` prints the resolved local configuration after TOML,
CLI overrides, profile presets, and addressing-style normalization.

`doctor --local-only --json` checks the binary version, current working
directory, config parse status, local output parent directories, and explicitly
marks network probing as skipped.

`--dry-run` resolves command inputs, planned output paths, hints source,
checkpoint identity, output parent directories, and local file conflicts
without creating Parquet/KS files and without making S3 requests.

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

When a hints file is present, `hints` includes parse status, format, boundary
count, warnings, and estimate summary metadata.  When `--resume` is set and a
checkpoint file exists, `checkpoint` reports parse status, completed/total
segments, and identity match details.

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
fatal listing errors, and output write errors.

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

## Safety expectations

- `config-inspect`, `doctor --local-only`, and `--dry-run` are local-only.
- `list`, `diff`, `auto-hints`, and `compat-probe` can contact S3 unless
  combined with `--dry-run`.
- Provider-specific caveats still apply; `--agent` does not enable BOS
  pagination workarounds or change hot-path listing behavior.
