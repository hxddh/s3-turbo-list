# s3-turbo-list examples

These scripts are **templates**, not validation scripts: each shows one
invocation pattern.  Adapt them to your environment.  For a first listing
against AWS, MinIO, BOS, R2, B2, or OSS, use the quickstart that
`s3-turbo-list guide <provider>` prints.

## Safety

- No script deletes buckets or objects, and none embeds credentials.
- Bucket names come from environment variables.
- `agent-dry-run.sh` is local-only; every other shell script contacts S3
  (`agent-run-with-manifest.sh` refuses to unless `RUN_REAL_S3=1`).

## Prerequisites

| Tool | Required | Notes |
|---|---|---|
| `cargo` or `s3-turbo-list` binary | **Required** | Set `S3_TURBO_LIST_BIN` to use a pre-built binary. |
| `jq` | Optional | Handy for trace JSONL. |
| Python 3 + `pandas` + `pyarrow` | Optional | Only for `read-parquet.py` (`pip install pandas pyarrow`). |

## Environment variables

- `S3_TURBO_LIST_BIN` — path to the binary.  Unset, scripts use
  `cargo run --`, which builds from source in the current workspace.
- `OUTDIR` — output directory; each script defaults to
  `./artifacts/<example-name>`.
- `ENDPOINT_URL` — a custom S3 endpoint (omit for AWS); `PROVIDER` selects a
  provider preset (`minio`, `bos`, `r2`, `b2`, `oss`) in the agent scripts.
- `AWS_PROFILE` — the standard AWS SDK credentials profile.  s3-turbo-list's
  `--provider` selects an endpoint preset only; it does not pick credentials.

Listing is a recursive full-bucket inventory by default.  Pass
`--delimiter '/'` for a hierarchical listing (top-level objects plus one row
per `CommonPrefix`).

## Suggested order

1. Read the [README](../README.md) and run the local preflight:
   ```bash
   s3-turbo-list doctor
   s3-turbo-list guide
   ./examples/agent-dry-run.sh
   ```
2. Explore **diff**, **checkpoint/resume**, **trace**, and **hints**.
3. For a local-only throughput check, run `./scripts/benchmark-local.sh`
   from the repository root (the `bench_local` example; synthetic data, no
   S3 — see [`docs/development.md`](../docs/development.md)).

## File index

| Script | Purpose | Output |
|---|---|---|
| `agent-dry-run.sh` | Local-only dry-run plan (`--dry-run > plan.json`) | `plan.json` |
| `agent-run-with-manifest.sh` | List with a run manifest and trace; requires `RUN_REAL_S3=1` | `.parquet`, `.ks`, `run.json`, `trace.jsonl` |
| `checkpoint-resume.sh` | Interrupt a listing, then `--resume` it | `.parquet`, `.ks` (two sets), `run.json` |
| `diff-basic.sh` | Diff two buckets | `.parquet`, `.ks` |
| `hints-file-toml.sh` | Validate (`doctor --hints-file`) and list with a TOML hints file | `.parquet`, `.ks`, `hints.toml` |
| `trace-debug.sh` | List with an S3 request trace and a run log | `.parquet`, `.ks`, `.log`, `trace.jsonl` |
| `read-parquet.py` | Read a Parquet file with pandas | stdout |
| `inspect-trace.py` | Summarize a trace JSONL file | stdout |
| `read_manifest.py` | Summarize a run manifest | stdout |
| `bench_local.rs` | Local synthetic output benchmark (`cargo run --release --example bench_local -- --help`) | JSON report |
