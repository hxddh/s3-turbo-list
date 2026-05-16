# Installing s3-turbo-list

## Overview

To get started with s3-turbo-list:

1. Download the correct release binary for your platform.
2. Download `SHA256SUMS`.
3. Verify the checksum.
4. Install the binary into your `PATH`.
5. Configure an AWS-compatible profile.
6. Run a first listing command.

All release assets are published at the [GitHub releases page](https://github.com/hxddh/s3-turbo-list/releases).

## Choose the correct binary

| Platform | Binary |
|---|---|
| Linux x86_64 | `s3-turbo-list-0.1.3-linux-x86_64` |
| Linux ARM64 / aarch64 | `s3-turbo-list-0.1.3-linux-aarch64` |
| macOS Apple Silicon | `s3-turbo-list-0.1.3-macos-aarch64` |
| macOS Intel | `s3-turbo-list-0.1.3-macos-x86_64` |

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
chmod +x s3-turbo-list-0.1.3-linux-x86_64
sudo install -m 0755 s3-turbo-list-0.1.3-linux-x86_64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
s3-turbo-list --help
```

### Linux ARM64 / aarch64

```bash
chmod +x s3-turbo-list-0.1.3-linux-aarch64
sudo install -m 0755 s3-turbo-list-0.1.3-linux-aarch64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

If `/usr/local/bin` is not writable, install into `~/.local/bin` or another
directory on your `PATH`.

## Install on macOS

### Apple Silicon

```bash
chmod +x s3-turbo-list-0.1.3-macos-aarch64
xattr -d com.apple.quarantine ./s3-turbo-list-0.1.3-macos-aarch64 2>/dev/null || true
sudo install -m 0755 s3-turbo-list-0.1.3-macos-aarch64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

### Intel

```bash
chmod +x s3-turbo-list-0.1.3-macos-x86_64
xattr -d com.apple.quarantine ./s3-turbo-list-0.1.3-macos-x86_64 2>/dev/null || true
sudo install -m 0755 s3-turbo-list-0.1.3-macos-x86_64 /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

macOS may apply a quarantine attribute to manually downloaded binaries.
The `xattr -d com.apple.quarantine` command removes it.  If you see
*"app cannot be opened because the developer cannot be verified"*, run
the `xattr` command above.

If `/usr/local/bin` is not writable, install into `~/.local/bin` or another
directory on your `PATH`.

## Configure AWS S3 credentials

s3-turbo-list uses the standard AWS credential and profile configuration.
Run `aws configure` to set up a profile:

```bash
aws configure --profile default
```

Then run a first listing:

```bash
mkdir -p out

s3-turbo-list list \
  --bucket my-bucket \
  --region us-east-1 \
  --profile default \
  --output-parquet-file out/aws-basic.parquet \
  --output-ks-file out/aws-basic.ks
```

## Configure BOS

BOS (Baidu Object Storage) is supported through its S3-compatible endpoint.
BOS is **virtual-hosted-first** — use `--addressing-style virtual` for
normal BOS usage.

```bash
aws configure --profile bos
```

Normal BOS usage:

```bash
mkdir -p out

s3-turbo-list list \
  --bucket your-bos-bucket \
  --region bj \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --addressing-style virtual \
  --output-parquet-file out/bos-basic.parquet \
  --output-ks-file out/bos-basic.ks
```

Path-style is **not the recommended BOS default**.  Use it only for
legacy compatibility or diagnostic checks:

```bash
# Path-style (legacy / diagnostic only)
s3-turbo-list list \
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
  duckdb, or any Parquet-compatible tool.
- **KS output** (`--output-ks-file`): Keyspace CSV showing per-prefix object
  counts.
- **JSONL trace** (`--trace-compat`): Structured compatibility/debug trace
  with 28 fields per S3 API call.

Example with all output types:

```bash
s3-turbo-list list \
  --bucket my-bucket \
  --region us-east-1 \
  --profile default \
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
  --profile default \
  --resume \
  --output-parquet-file out/resume.parquet \
  --output-ks-file out/resume.ks
```

Resume uses checkpoint identity validation.  If bucket, region, endpoint,
delimiter, max_keys, addressing_style, or other identity fields change,
the checkpoint is rejected to prevent mismatched data.

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
  --profile default \
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
  --profile default \
  --sample-limit 1000000 \
  --max-pages 1000 \
  --output ./hints.sampled.toml
```

In sampled mode, `total_objects` means sampled objects, not the full bucket
total.  Use full-scan `auto-hints` when you need authoritative bucket-wide
prefix statistics.

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
