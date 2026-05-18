# Building a Release

This document describes how to build a release binary of s3-turbo-list
and package it into `dist/`.

## Quick start

```bash
# 1. Check the environment
./scripts/check-release-env.sh

# 2. Build and package
BUILD_MODE=default ./scripts/build-release.sh
```

## Overview

`scripts/build-release.sh` handles the full pipeline:

1. Build the binary with `cargo build --release` (or a workaround mode).
2. Copy the binary to `dist/` with a versioned name:
   `s3-turbo-list-<version>-<os>-<arch>`
3. Generate a SHA256 checksum.
4. Verify the binary by running `--help` and `--version`.

No network calls, no cloud endpoints, no GitHub release creation.

## Build modes

### `default`

Standard release build.  Works on x86_64 Linux, macOS, and aarch64 with
GCC >= 10 or clang available.

```bash
BUILD_MODE=default ./scripts/build-release.sh
```

### `clang`

Use clang instead of the default C compiler.  Required on Ubuntu 20.04
arm64 where GCC 9.4 triggers an `aws-lc-sys` memcmp bug detection.

```bash
# Install clang first (if needed):
# sudo apt install clang

BUILD_MODE=clang ./scripts/build-release.sh
```

### `gcc10`

Use GCC 10 specifically instead of the system default.

```bash
# Install gcc-10 first (if needed):
# sudo apt install gcc-10

BUILD_MODE=gcc10 ./scripts/build-release.sh
```

### `no-asm`

Disable assembly optimisations in `aws-lc-sys`, falling back to the
Rust/C implementation.  Works on any platform but may produce a slightly
slower binary.

```bash
BUILD_MODE=no-asm ./scripts/build-release.sh
```

## Ubuntu 20.04 arm64 / GCC 9.4 / aws-lc-sys

The `aws-lc-sys` crate (a dependency of `aws-smithy-runtime`) detects
GCC < 10 on aarch64 and aborts the build with:

```
error: failed to run custom build command for `aws-lc-sys v...`
  This environment (GCC 9.4.0, aarch64) uses a known buggy memcmp
  implementation. Aborting build.
```

Use one of the workarounds above (`clang`, `gcc10`, or `no-asm`).  The
`scripts/check-release-env.sh` script will warn if a workaround is needed.

Debug builds (`cargo build`, `cargo test`) are unaffected — this only
applies to `--release`.

## Expected output in `dist/`

```
dist/
├── s3-turbo-list-0.1.10-linux-x86_64
├── s3-turbo-list-0.1.10-linux-x86_64.sha256
├── s3-turbo-list-0.1.10-linux-aarch64
└── s3-turbo-list-0.1.10-linux-aarch64.sha256
```

## Cross-compilation

Set the `TARGET` environment variable to cross-compile:

```bash
TARGET=x86_64-unknown-linux-gnu BUILD_MODE=default ./scripts/build-release.sh
```

## Verifying the binary

After the build completes, verify the binary locally (no cloud endpoints):

```bash
./dist/s3-turbo-list-0.1.10-linux-aarch64 --version
./dist/s3-turbo-list-0.1.10-linux-aarch64 --help
```

## What NOT to do

- **Do not embed credentials** in the binary, build scripts, or output.
- **Do not run endpoint validation** as part of the release build — the
  binary verification step uses only `--help` and `--version`.
- **Do not publish a GitHub release** before all checks pass:
  `cargo fmt --check`, `cargo check`, `cargo test`, `cargo build`,
  examples static QA, secret scan.

## See also

- [`BUILD.md`](../BUILD.md) — general build instructions and workarounds.
- [`docs/release-checklist.md`](release-checklist.md) — full release process.
- [`scripts/check-release-env.sh`](../scripts/check-release-env.sh) — environment checker.
- [`scripts/build-release.sh`](../scripts/build-release.sh) — release build script.
