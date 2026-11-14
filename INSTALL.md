# Installing s3tl

1. Download the binary for your platform and `SHA256SUMS` from the
   [GitHub releases page](https://github.com/hxddh/s3-turbo-list/releases).
2. Verify the checksum.
3. Install the binary into your `PATH` as `s3tl`.
4. Configure AWS credentials, then run `s3tl check`.

To build from source instead: `cargo build --release` (see
[`docs/releasing.md`](docs/releasing.md) for the Ubuntu 20.04 arm64
workaround).

## Choose the binary

| Platform | Binary |
|---|---|
| Linux x86_64 | `s3tl-<version>-linux-x86_64` |
| Linux ARM64 / aarch64 | `s3tl-<version>-linux-aarch64` |
| macOS Apple Silicon | `s3tl-<version>-macos-aarch64` |
| macOS Intel | `s3tl-<version>-macos-x86_64` |

`uname -s` prints `Linux` or `Darwin`; `uname -m` prints `x86_64` or
`aarch64` (`arm64` on Apple Silicon).

## Verify SHA256SUMS

Download the binary and `SHA256SUMS` into the same directory (`SHA256SUMS`
uses bare file names):

```bash
sha256sum -c --ignore-missing SHA256SUMS        # Linux
shasum -a 256 -c --ignore-missing SHA256SUMS    # macOS
```

Expect `OK` next to the binary you downloaded. If it reports `FAILED`,
download again.

## Install

```bash
VERSION=<version>
TARGET=linux-x86_64        # linux-aarch64, macos-aarch64, macos-x86_64
chmod +x "s3tl-${VERSION}-${TARGET}"
xattr -d com.apple.quarantine "s3tl-${VERSION}-${TARGET}" 2>/dev/null || true   # macOS
sudo install -m 0755 "s3tl-${VERSION}-${TARGET}" /usr/local/bin/s3tl
s3tl --version
```

If `/usr/local/bin` is not writable, install into `~/.local/bin` or another
directory on your `PATH`.

## Credentials

`s3tl` uses the standard AWS SDK chain: environment variables,
`~/.aws/credentials` and `~/.aws/config` profiles, web identity, and
instance or container roles.

```bash
export AWS_PROFILE=my-profile
s3tl check                      # what a run would use; sends nothing
s3tl check s3://my-bucket/      # a few probe requests
```

For other S3-compatible endpoints see [`docs/providers.md`](docs/providers.md).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Permission denied | Binary is not executable | `chmod +x` |
| command not found | Install directory not on `PATH` | Move the binary or update `PATH` |
| macOS refuses to open it | Quarantine attribute | `xattr -d com.apple.quarantine` |
| `auth.*` error (exit 3) | Wrong profile or credentials | Check `AWS_PROFILE`; `s3tl check` |
| `config.wrong_region` / `config.no_such_bucket` (exit 3) | Region or endpoint mismatch | Omit `--region` on AWS; check `--endpoint-url` |
| `usage.output_not_empty` (exit 2) | `-o` points at a used directory | Use a new directory, or `--resume` an interrupted run |

## Security

- Do not put access keys in shell history, docs, issues or trace files.
- Prefer named profiles or environment-managed credentials.
- Results and traces redact credentials in endpoint URLs; bucket names and
  keys are not redacted, so sanitize them before sharing.
