# Installing s3-turbo-list

1. Download the release binary for your platform and `SHA256SUMS` from the
   [GitHub releases page](https://github.com/hxddh/s3-turbo-list/releases).
2. Verify the checksum.
3. Install the binary into your `PATH`.
4. Configure AWS-compatible credentials, then follow the
   [README quick start](README.md#quick-start) or `s3-turbo-list guide`.

To build from source instead, see [`docs/releasing.md`](docs/releasing.md).

## Choose the correct binary

| Platform | Binary |
|---|---|
| Linux x86_64 | `s3-turbo-list-<version>-linux-x86_64` |
| Linux ARM64 / aarch64 | `s3-turbo-list-<version>-linux-aarch64` |
| macOS Apple Silicon | `s3-turbo-list-<version>-macos-aarch64` |
| macOS Intel | `s3-turbo-list-<version>-macos-x86_64` |

`uname -s` prints `Linux` or `Darwin`; `uname -m` prints `x86_64` or
`aarch64` (`arm64` on Apple Silicon).

## Verify SHA256SUMS

Download the binary and `SHA256SUMS` into the same flat directory —
`SHA256SUMS` uses bare filenames.

```bash
sha256sum -c SHA256SUMS          # Linux
shasum -a 256 -c SHA256SUMS      # macOS
```

Expect `OK` next to the binary you downloaded (lines for binaries you did not
download report missing files).  If it reports `FAILED`, download again; if
it cannot find your binary, make sure both files are in one directory.

## Install on Linux

```bash
VERSION=<version>
ARCH=x86_64        # or aarch64
chmod +x "s3-turbo-list-${VERSION}-linux-${ARCH}"
sudo install -m 0755 "s3-turbo-list-${VERSION}-linux-${ARCH}" /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

## Install on macOS

```bash
VERSION=<version>
ARCH=aarch64       # Apple Silicon; x86_64 for Intel
chmod +x "s3-turbo-list-${VERSION}-macos-${ARCH}"
xattr -d com.apple.quarantine "./s3-turbo-list-${VERSION}-macos-${ARCH}" 2>/dev/null || true
sudo install -m 0755 "s3-turbo-list-${VERSION}-macos-${ARCH}" /usr/local/bin/s3-turbo-list
s3-turbo-list --version
```

macOS applies a quarantine attribute to downloaded binaries; the `xattr`
command removes it.  If you see *"app cannot be opened because the developer
cannot be verified"*, run it.

On either platform, if `/usr/local/bin` is not writable, install into
`~/.local/bin` or another directory on your `PATH`.

## Shell completions and man page

```bash
s3-turbo-list completions bash > s3-turbo-list.bash
s3-turbo-list completions zsh > _s3-turbo-list
s3-turbo-list completions fish > s3-turbo-list.fish
s3-turbo-list man > s3-turbo-list.1
```

These write to stdout only and do not contact S3.  The man page covers the
top-level command and lists the subcommands; each command's options are in
`s3-turbo-list <command> --help` (clap_mangen writes per-command pages only
to a directory, which `man` does not take).

## Credentials

s3-turbo-list uses the standard AWS SDK credential chain: environment
variables, `~/.aws/credentials` profiles, or instance roles.

```bash
aws configure --profile default      # or: aws configure --profile my-profile
export AWS_PROFILE=my-profile        # select a non-default profile
```

`--provider` (`aws`, `minio`, `bos`, `r2`, `b2`, `oss`) selects an
S3-compatible *endpoint* preset; it is not a substitute for `AWS_PROFILE`.
`s3-turbo-list guide <provider>` prints a quickstart for each, and
[`docs/providers.md`](docs/providers.md) covers the presets and the config
file.  Before the first real run, `s3-turbo-list doctor` checks the local
setup and `--dry-run` previews the run; neither contacts S3.  Automation
surfaces are in [`docs/agent-usage.md`](docs/agent-usage.md).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Permission denied | Binary is not executable | Run `chmod +x` |
| command not found | Install directory not in `PATH` | Move the binary to `/usr/local/bin` or update `PATH` |
| macOS says app cannot be opened | Quarantine attribute | Run `xattr -d com.apple.quarantine` |
| AccessDenied / auth failure (exit 3) | Wrong profile or credentials | Check `AWS_PROFILE` / `aws configure --profile ...` |
| Wrong region or endpoint (exit 3) | Region/endpoint mismatch | Check `--region`, `--provider` and `--endpoint-url`; run `compat-probe` |
| SHA256SUMS cannot find the binary | Files not in one flat directory | Download binary and `SHA256SUMS` into one directory |
| Empty output | Prefix/delimiter/filter mismatch | Re-check `--prefix`, `--delimiter`, and `--filter` |

## Security note

- Do not put access keys or secret keys in shell history, docs, issues, PRs,
  or trace files.
- Prefer named profiles or environment-managed credentials.
- Sanitize bucket names and account identifiers before sharing logs.
