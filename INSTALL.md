# Installing s3-turbo-list

## Overview

To get started with s3-turbo-list:

1. Download the correct release binary for your platform.
2. Download `SHA256SUMS`.
3. Verify the checksum.
4. Install the binary into your `PATH`.
5. Configure AWS-compatible credentials.
6. Run a first listing command.

All release assets are published at the [GitHub releases page](https://github.com/hxddh/s3-turbo-list/releases).

## 30-second first run

After installing the binary and configuring credentials, run a local check,
then dry-run the command before the real listing:

```bash
s3-turbo-list doctor --local-only --simple
export AWS_PROFILE=default

s3-turbo-list --dry-run --agent --output-dir out --delimiter '' \
  list --bucket my-bucket --region us-east-1

s3-turbo-list --output-dir out --delimiter '' \
  list --bucket my-bucket --region us-east-1
```

`--delimiter ''` performs a recursive full-bucket object inventory.  The
default delimiter is `/`, which is hierarchical and only returns top-level
objects plus `CommonPrefixes`.

If you only need object count, total bytes, and top-prefix distribution, use
summary-only mode.  It still scans S3, but it does not write Parquet or
KeySpace files:

```bash
s3-turbo-list --summary-only --run-manifest summary.json --delimiter '' \
  list --bucket my-bucket --region us-east-1
s3-turbo-list manifest-summary summary.json
```

If you want rows directly in a shell pipeline or an agent-readable stream, use
TSV or NDJSON list output.  These modes still scan S3, but they write rows to
stdout instead of writing Parquet or KeySpace files:

```bash
s3-turbo-list --delimiter '' list --bucket my-bucket --region us-east-1 \
  --output-format tsv | wc -l

s3-turbo-list --run-manifest run.json --delimiter '' \
  list --bucket my-bucket --region us-east-1 --output-format ndjson > objects.ndjson
s3-turbo-list manifest-summary run.json --json
s3-turbo-list manifest-summary run.json --check
```

Output mode guide:

| Need | Command shape |
|---|---|
| Repeatable inventory for DuckDB/pandas/audit | default `list` Parquet output |
| Object count, bytes, and top prefixes only | `--summary-only --run-manifest run.json` |
| Shell pipeline rows | `list --output-format tsv` |
| Agent/JQ-readable rows | `list --output-format ndjson` |
| Local result validation | `manifest-summary run.json --check` |

`manifest-summary --check` is local-only.  It validates the saved manifest and
recorded artifact paths on the local filesystem.  When recorded metadata is
available, it also verifies current artifact size, SHA256, and Parquet
row/schema metadata.  It does not contact S3 or prove the remote bucket has not
changed since the run.

## Choose the correct binary

| Platform | Binary |
|---|---|
| Linux x86_64 | `s3-turbo-list-0.1.26-linux-x86_64` |
| Linux ARM64 / aarch64 | `s3-turbo-list-0.1.26-linux-aarch64` |
| macOS Apple Silicon | `s3-turbo-list-0.1.26-macos-aarch64` |
| macOS Intel | `s3-turbo-list-0.1.26-macos-x86_64` |

To identify your platform:

```bash
uname -s   # Linux or Darwin
uname -m   # x86_64 or aarch64
```

## Verify SHA256SUMS

Download the selected binary and `SHA256SUMS` into the same flat directory.
Do not nest the files inside subdirectories — `SHA256SUMS` uses bare filenames.

Verify on Linux:

```bash
sha256sum -c SHA256SUMS
```

Verify on macOS:

```bash
shasum -a 256 -c SHA256SUMS
```

The command should print `OK` next to each binary you downloaded.  If you
see `FAILED` or `No such file or directory`, make sure both files are in
the same directory and that the SHA256SUMS file does not contain a `dist/`
prefix.

## Install on Linux

### Linux x86_64

```bash
chmod +x s3-turbo-list-0.1.26-linux-x86_64
sudo install -m 0755 s3-turbo-list-0.1.26-linux-x86_64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
s3-turbo-list --help
```

### Linux ARM64 / aarch64

```bash
chmod +x s3-turbo-list-0.1.26-linux-aarch64
sudo install -m 0755 s3-turbo-list-0.1.26-linux-aarch64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

If `/usr/local/bin` is not writable, install into `~/.local/bin` or another
directory on your `PATH`.

## Install on macOS

### Apple Silicon

```bash
chmod +x s3-turbo-list-0.1.26-macos-aarch64
xattr -d com.apple.quarantine ./s3-turbo-list-0.1.26-macos-aarch64 2>/dev/null || true
sudo install -m 0755 s3-turbo-list-0.1.26-macos-aarch64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

### Intel

```bash
chmod +x s3-turbo-list-0.1.26-macos-x86_64
xattr -d com.apple.quarantine ./s3-turbo-list-0.1.26-macos-x86_64 2>/dev/null || true
sudo install -m 0755 s3-turbo-list-0.1.26-macos-x86_64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

macOS may apply a quarantine attribute to manually downloaded binaries.
The `xattr -d com.apple.quarantine` command removes it.  If you see
*"app cannot be opened because the developer cannot be verified"*, run
the `xattr` command above.

If `/usr/local/bin` is not writable, install into `~/.local/bin` or another
directory on your `PATH`.

## Optional shell completions and man page

Generate shell completions locally after installing the binary:

```bash
s3-turbo-list completions bash > s3-turbo-list.bash
s3-turbo-list completions zsh > _s3-turbo-list
s3-turbo-list completions fish > s3-turbo-list.fish
s3-turbo-list man > s3-turbo-list.1
```

These commands only write to stdout and do not contact S3.

## Configure AWS S3 credentials

s3-turbo-list uses the standard AWS SDK credential chain.  Run
`aws configure` to set up credentials:

```bash
aws configure --profile default
```

For a non-default AWS credentials profile, use the AWS SDK environment:

```bash
export AWS_PROFILE=my-aws-profile
```

Then run a first listing:

```bash
s3-turbo-list doctor --local-only --simple
s3-turbo-list init-config --output s3-turbo-list.toml

s3-turbo-list --dry-run --agent --output-dir out \
  --delimiter '' list --bucket my-bucket --region us-east-1

s3-turbo-list --output-dir out \
  --delimiter '' list --bucket my-bucket --region us-east-1
```

The s3-turbo-list `--profile` flag is for endpoint compatibility presets such
as `minio`, `bos`, `r2`, `b2`, or `oss`; it is not a substitute for
`AWS_PROFILE`.

## Agent-safe local preflight

Automation can inspect configuration and planned outputs without contacting S3:

```bash
s3-turbo-list config-inspect --json
s3-turbo-list doctor --local-only --json
s3-turbo-list doctor --local-only --simple --fix-suggestions
s3-turbo-list recipes aws-basic
s3-turbo-list recipes summary
s3-turbo-list recipes pipe
s3-turbo-list recipes verify
s3-turbo-list recipes release-check
s3-turbo-list recipes diff-safe

s3-turbo-list --dry-run --agent --output-dir out --delimiter '' list \
  --bucket my-bucket \
  --region us-east-1
```

For full details on machine-readable plans, manifests, and exit codes, see
[`docs/agent-usage.md`](docs/agent-usage.md).  Manifest artifact summaries
include SHA256, file sizes, line counts, and Parquet metadata.
Provider profiles that need account- or region-specific endpoints warn locally
until `--endpoint-url` or `s3.endpoint_url` is set.  Replace any starter config
placeholder such as `<account-id>` before a real listing run.

## Configure BOS

BOS (Baidu Object Storage) is supported through its S3-compatible endpoint.
BOS is **virtual-hosted-first** — use `--addressing-style virtual` for
normal BOS usage.

```bash
aws configure --profile bos
export AWS_PROFILE=bos
```

Normal BOS usage:

```bash
mkdir -p out

# The bos profile preset sets the BOS endpoint and virtual-hosted addressing.
s3-turbo-list list \
  --delimiter '' \
  --bucket your-bos-bucket \
  --region bj \
  --profile bos \
  --output-parquet-file out/bos-basic.parquet \
  --output-ks-file out/bos-basic.ks
```

Path-style is **not the recommended BOS default**.  Use it only for
legacy compatibility or diagnostic checks:

```bash
# Path-style (legacy / diagnostic only)
s3-turbo-list list \
  --delimiter '' \
  --bucket your-bos-bucket \
  --region bj \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --addressing-style path \
  --output-parquet-file out/bos-path-diagnostic.parquet \
  --output-ks-file out/bos-path-diagnostic.ks
```

## Configure MinIO

```bash
aws configure --profile minio
```

Example local MinIO dev credentials:

```
AWS Access Key ID: minioadmin
AWS Secret Access Key: minioadmin
Default region name: us-east-1
```

First run:

```bash
mkdir -p out

s3-turbo-list list \
  --delimiter '' \
  --bucket my-minio-bucket \
  --region us-east-1 \
  --profile minio \
  --endpoint-url http://localhost:9000 \
  --addressing-style path \
  --output-parquet-file out/minio-basic.parquet \
  --output-ks-file out/minio-basic.ks
```

## Output files

- **Parquet output** (`--output-parquet-file`): Object listing result with
  Key, Size, LastModified, ETag, and DiffFlag columns.  Suitable for pandas,
  duckdb, or any Parquet-compatible tool.  The default compression is
  `gzip(6)`; use `--compression zstd --compression-level 3` for faster local
  write-heavy runs when your downstream tools support zstd.
- **KS output** (`--output-ks-file`): Keyspace CSV showing per-prefix object
  counts.
- **JSONL trace** (`--trace-compat`): Structured compatibility/debug trace
  with 28 fields per S3 API call.

Current `diff` runs use authoritative single-segment listing.  Conventional
hints caches are ignored for `diff`, and `diff --hints-file` fails before any
S3 request until paired-segment diff coordination is implemented in `v0.2.x`.

Example with all output types:

```bash
s3-turbo-list list \
  --bucket my-bucket \
  --region us-east-1 \
  --debug-s3 \
  --trace-compat out/trace.jsonl \
  --output-parquet-file out/output.parquet \
  --output-ks-file out/output.ks \
  2> out/debug-stderr.log
```

## Inspect Parquet output

Use the bundled example script:

```bash
python3 examples/read-parquet.py out/output.parquet
```

Or with a Python snippet:

```python
import pandas as pd

df = pd.read_parquet("out/output.parquet")
print(df.head())
```

## Checkpoint / resume

s3-turbo-list saves progress every 30 seconds and on graceful shutdown.
Resume picks up where an interrupted scan left off:

```bash
s3-turbo-list list \
  --bucket my-bucket \
  --region us-east-1 \
  --resume \
  --output-parquet-file out/resume.parquet \
  --output-ks-file out/resume.ks
```

Resume uses checkpoint identity validation.  If bucket, region, prefix,
delimiter, max_keys, profile, addressing_style, or mode change, the
checkpoint is rejected to prevent mismatched data.

## Hints file

Hints files split a bucket into segments for parallel listing.  TOML format:

```toml
boundaries = [
  "alpha/",
  "beta/",
  "logs/file with spaces.log",
  "logs/file+plus.log",
  "中文/"
]
```

Run with a hints file:

```bash
s3-turbo-list list \
  --bucket my-bucket \
  --region us-east-1 \
  --hints-file ./hints.toml \
  --output-parquet-file out/hints.parquet \
  --output-ks-file out/hints.ks
```

TOML hints are parsed as TOML.  Plain line-by-line hints are also
supported.  Malformed TOML-like hints fail before any S3 requests are
sent — no partial work or wasted API calls.

Validate a hints file locally:

```bash
s3-turbo-list hints-validate --hints-file ./hints.toml
```

For very large buckets, `auto-hints` can produce an estimated hints cache
from a bounded sample:

```bash
s3-turbo-list auto-hints \
  --bucket my-bucket \
  --region us-east-1 \
  --sample-limit 1000000 \
  --max-pages 1000 \
  --output ./hints.sampled.toml
```

`auto-hints` can also be scoped to a subtree:

```bash
s3-turbo-list --prefix logs/2026/ auto-hints \
  --bucket my-bucket \
  --region us-east-1 \
  --output ./logs-2026-hints.toml
```

Use `discover-prefixes` when you want delimiter-based `CommonPrefixes` as a
starting point for manual hints:

```bash
s3-turbo-list --prefix logs/ --delimiter / discover-prefixes \
  --bucket my-bucket \
  --region us-east-1 \
  --output ./logs-prefixes.txt
```

In sampled mode, `total_objects` means sampled objects, not the full bucket
total.  Prefix-scoped hints describe only the selected subtree.  Segment
estimates in the hints cache are sampled/estimated, not authoritative
bucket-wide statistics.  Use full-scan `auto-hints` when you need observed
bucket-wide prefix statistics.

For runtime defaults and advanced TOML-only settings, see
[`docs/tuning.md`](docs/tuning.md).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Permission denied | Binary is not executable | Run `chmod +x` |
| command not found | Install directory not in `PATH` | Move binary to `/usr/local/bin` or update `PATH` |
| macOS says app cannot be opened | Quarantine attribute | Run `xattr -d com.apple.quarantine` |
| AccessDenied / auth failure | Wrong profile or credentials | Check `aws configure --profile ...` |
| Wrong region or endpoint | Region/endpoint mismatch | Check `--region` and `--endpoint-url` |
| BOS path-style issue | Using legacy path-style | Use `--addressing-style virtual` |
| SHA256SUMS fails with missing file | Files not in same flat directory or old checksum format | Download binary and SHA256SUMS into one directory |
| Empty output | Prefix/delimiter/filter mismatch | Re-check prefix, delimiter, and object filter |

## Security note

- Do not put access keys or secret keys in shell history, docs, issues, PRs,
  or trace files.
- Prefer named profiles or environment-managed credentials.
- Sanitize bucket names and account identifiers before sharing logs.
