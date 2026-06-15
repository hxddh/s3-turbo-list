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
s3-turbo-list doctor --simple
export AWS_PROFILE=default

s3-turbo-list --dry-run --agent --output-dir out \
  list --bucket my-bucket --region us-east-1

s3-turbo-list --output-dir out \
  list --bucket my-bucket --region us-east-1
```

Listing is a recursive full-bucket inventory by default.  Use
`--delimiter '/'` for a hierarchical listing that only returns top-level
objects plus `CommonPrefixes`.  Other output modes (summary-only, TSV,
NDJSON, run manifests) are covered in the [README](README.md#output).

## Choose the correct binary

| Platform | Binary |
|---|---|
| Linux x86_64 | `s3-turbo-list-<version>-linux-x86_64` |
| Linux ARM64 / aarch64 | `s3-turbo-list-<version>-linux-aarch64` |
| macOS Apple Silicon | `s3-turbo-list-<version>-macos-aarch64` |
| macOS Intel | `s3-turbo-list-<version>-macos-x86_64` |

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
VERSION=<version>
chmod +x "s3-turbo-list-${VERSION}-linux-x86_64"
sudo install -m 0755 "s3-turbo-list-${VERSION}-linux-x86_64" /usr/local/bin/s3-turbo-list
s3-turbo-list --version
s3-turbo-list --help
```

### Linux ARM64 / aarch64

```bash
VERSION=<version>
chmod +x "s3-turbo-list-${VERSION}-linux-aarch64"
sudo install -m 0755 "s3-turbo-list-${VERSION}-linux-aarch64" /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

If `/usr/local/bin` is not writable, install into `~/.local/bin` or another
directory on your `PATH`.

## Install on macOS

### Apple Silicon

```bash
VERSION=<version>
chmod +x "s3-turbo-list-${VERSION}-macos-aarch64"
xattr -d com.apple.quarantine "./s3-turbo-list-${VERSION}-macos-aarch64" 2>/dev/null || true
sudo install -m 0755 "s3-turbo-list-${VERSION}-macos-aarch64" /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

### Intel

```bash
VERSION=<version>
chmod +x "s3-turbo-list-${VERSION}-macos-x86_64"
xattr -d com.apple.quarantine "./s3-turbo-list-${VERSION}-macos-x86_64" 2>/dev/null || true
sudo install -m 0755 "s3-turbo-list-${VERSION}-macos-x86_64" /usr/local/bin/s3-turbo-list
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

You are ready to run — see the **30-second first run** above, or write a
starter config first with `s3-turbo-list init-config --output
s3-turbo-list.toml`.

The s3-turbo-list `--profile` flag is for endpoint compatibility presets such
as `minio`, `bos`, `r2`, `b2`, or `oss`; it is not a substitute for
`AWS_PROFILE`.

## Agent and CI use

Every cloud-facing run has a local-only preflight: `doctor --json` reports the
environment and resolved config, `--dry-run --agent` emits a JSON plan, and
`guide <topic>` prints command recipes — none contact S3. Machine-readable
plans, run manifests, and stable exit codes are documented in
[`docs/agent-usage.md`](docs/agent-usage.md).

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

## After installation

Output formats and the Parquet schema, checkpoint/resume, hints, and
performance tuning are covered in the [README](README.md) and
[`docs/tuning.md`](docs/tuning.md). Two common follow-ups:

```bash
# Resume an interrupted scan (identity-verified)
s3-turbo-list --resume list --bucket my-bucket --region us-east-1

# Inspect the Parquet output
python3 examples/read-parquet.py out/*.parquet
```

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
