# s3-turbo-list examples

These scripts are **templates** — not validation scripts.  They demonstrate
canonical invocation patterns for each supported use case.  Adapt them to
your environment.

## Safety

- No script deletes buckets or objects.
- No script embeds credentials.
- All bucket names must be supplied via environment variables.
- Scripts print the command before executing it.

## Prerequisites

| Tool | Required | Notes |
|---|---|---|
| `cargo` or `s3-turbo-list` binary | **Required** | Set `S3_TURBO_LIST_BIN` to use a pre-built binary. |
| `jq` | Optional | Useful for inspecting trace JSONL, but not required. |
| Python 3 + `pandas` + `pyarrow` | Optional | Only needed for `read-parquet.py` and `inspect-trace.py`. |

Install Python deps:
```bash
pip install pandas pyarrow
```

## Environment variables

### `S3_TURBO_LIST_BIN`

Path to the s3-turbo-list binary.  If unset, scripts fall back to
`cargo run --`, which builds from source in the current workspace.

```bash
# Use a pre-built binary
export S3_TURBO_LIST_BIN=/usr/local/bin/s3-turbo-list

# Or build from source (default)
unset S3_TURBO_LIST_BIN
```

### `OUTDIR`

Output directory for artifacts.  Each script defaults to
`./artifacts/<example-name>` if `OUTDIR` is unset.

```bash
export OUTDIR=/tmp/s3tl-examples/aws-basic
```

### `AWS_PROFILE`

Use the standard AWS SDK environment variable for non-default credential
profiles:

```bash
export AWS_PROFILE=my-aws-profile
```

The s3-turbo-list `--profile` flag is reserved for endpoint compatibility
presets such as `minio`, `bos`, `r2`, `b2`, and `oss`.

## Recommended order

1. Read [`README.md`](../README.md) first — understand the tool.
2. Run **MinIO** locally (no cloud credentials needed):
   ```bash
   # Start MinIO, create a bucket, then:
   BUCKET=my-test-bucket ./examples/minio-basic-list.sh
   ```
3. Run **AWS S3** with an explicit bucket:
   ```bash
   BUCKET=my-real-bucket REGION=us-east-2 ./examples/aws-basic-list.sh
   ```
4. Run **BOS** (virtual-hosted by default, as recommended by BOS):
   ```bash
   BUCKET=my-bos-bucket ./examples/bos-basic-list.sh
   ```
5. Explore **diff**, **checkpoint/resume**, **trace**, and **hints**.
6. For a local-only throughput check, run `./scripts/benchmark-local.sh`
   from the repository root.  It uses synthetic data and does not contact S3.

## BOS default guidance

All BOS examples default to **virtual-hosted** addressing, following BOS
official guidance.  Path-style is available only in the diagnostic example
(`bos-path-style-diagnostic.sh`) and is clearly labeled as legacy/diagnostic
mode — not the recommended default.

The known BOS ListObjectsV2 pagination limitation (start_after +
continuation_token interaction) is documented in the main
[`README.md`](../README.md#known-limitations).  Hinted multi-segment scans
on BOS should wait for a BOS-side fix or use single-segment fallback.

## File index

| Script | Purpose | Output |
|---|---|---|
| `aws-basic-list.sh` | List an AWS S3 bucket | `.parquet`, `.ks` |
| `minio-basic-list.sh` | List a MinIO bucket | `.parquet`, `.ks` |
| `bos-basic-list.sh` | List a BOS bucket (virtual-hosted) | `.parquet`, `.ks` |
| `bos-path-style-diagnostic.sh` | List a BOS bucket (path-style, diagnostic) | `.parquet`, `.ks` |
| `trace-debug.sh` | List with trace JSONL + debug stderr | `.parquet`, `.ks`, `trace.jsonl`, `debug-stderr.log` |
| `diff-basic.sh` | Diff two buckets | `.parquet` |
| `checkpoint-resume.sh` | List with checkpoint save and resume | `.parquet`, `.ks` |
| `hints-file-toml.sh` | List with a TOML hints file | `.parquet`, `.ks`, `hints.toml` |
| `hints-validate.sh` | Validate TOML or plain-text hints locally | stdout |
| `auto-hints-sampled.sh` | Generate sampled hints with segment estimates | `hints.sampled.toml` |
| `agent-dry-run.sh` | Produce local-only agent plan JSON | `plan.json` |
| `agent-run-with-manifest.sh` | Run list with manifest and trace outputs; requires `RUN_REAL_S3=1` | `.parquet`, `.ks`, `run.json`, `trace.jsonl` |
| `read-parquet.py` | Read a Parquet file with pandas | stdout |
| `inspect-trace.py` | Summarize a trace JSONL file | stdout |
| `read_manifest.py` | Summarize a run manifest | stdout |
