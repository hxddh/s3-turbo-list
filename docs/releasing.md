# Building and Releasing

How to build s3-turbo-list from source, package a release binary into
`dist/`, and publish a GitHub release.  The release source of truth is the
latest git tag / GitHub Release.

## Development build

```bash
cargo build
cargo test
```

Debug builds are unaffected by the `aws-lc-sys` issue below; it only applies
to `--release`.

## Ubuntu 20.04 arm64 / GCC 9 / aws-lc-sys

The `aws-lc-sys` crate (a dependency of `aws-smithy-runtime`) detects GCC < 10
on aarch64 and aborts a release build with:

```
error: failed to run custom build command for `aws-lc-sys v...`
Caused by:
  process didn't exit successfully: ...
  --- stderr
  ...
  This environment (GCC 9.4.0, aarch64) uses a known buggy memcmp
  implementation. Aborting build.
```

Use one of these workarounds (each has a matching `BUILD_MODE` for
`scripts/build-release.sh`, below):

```bash
# Option 1: clang                     (BUILD_MODE=clang)
sudo apt install clang
export CC=clang
cargo build --release

# Option 2: GCC 10+                   (BUILD_MODE=gcc10)
sudo apt install gcc-10
export CC=gcc-10
cargo build --release

# Option 3: disable ASM (Rust/C fallback, slightly slower; BUILD_MODE=no-asm)
export AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1
cargo build --release
```

`scripts/check-release-env.sh` warns when a workaround is needed.

## Release build script

```bash
./scripts/check-release-env.sh            # OS, arch, toolchain, compilers, git state
BUILD_MODE=default ./scripts/build-release.sh
```

`scripts/build-release.sh` builds with `cargo build --release` (or a
workaround mode), copies the binary to `dist/` as
`s3-turbo-list-<version>-<os>-<arch>`, writes a SHA256 checksum, and verifies
the binary with `--help` and `--version`.  It makes no network calls, contacts
no cloud endpoint, and creates no GitHub release.

| `BUILD_MODE` | Use |
|---|---|
| `default` | Standard build: x86_64 Linux, macOS, and aarch64 with GCC >= 10 or clang. |
| `clang` | Build with clang (required on Ubuntu 20.04 arm64 with GCC 9.4). |
| `gcc10` | Build with GCC 10 instead of the system default. |
| `no-asm` | Disable `aws-lc-sys` assembly; works anywhere, slightly slower binary. |

Cross-compile by setting `TARGET`:

```bash
TARGET=x86_64-unknown-linux-gnu BUILD_MODE=default ./scripts/build-release.sh
```

Expected output:

```
dist/
├── s3-turbo-list-<version>-linux-x86_64
├── s3-turbo-list-<version>-linux-x86_64.sha256
├── s3-turbo-list-<version>-linux-aarch64
└── s3-turbo-list-<version>-linux-aarch64.sha256
```

Verify locally (no cloud endpoints):

```bash
VERSION=$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
./dist/s3-turbo-list-${VERSION}-linux-aarch64 --version
./dist/s3-turbo-list-${VERSION}-linux-aarch64 --help
```

Never embed credentials in the binary, build scripts, or output, and never
run endpoint validation as part of the release build.

## Release checklist

### 1. Local pre-release checks

- [ ] Working tree clean (`git status --short` empty).
- [ ] On the correct branch (typically `main` for release publication).
- [ ] All commits intended for this release are present, and `CHANGELOG.md`
      has a section for the version in `Cargo.toml`.
- [ ] `scripts/check-release-env.sh` reports no blockers.

### 2. Secret scan

```bash
git grep -nE 'AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_(access_key_id|secret_access_key|session_token)\s*=|AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY)=|BEGIN (RSA|OPENSSH|EC|PRIVATE) KEY|ghp_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+' || true

# Real bucket-looking names (manual review)
git grep -n -E 's3tl-|my-real-|prod-' -- examples docs
```

Every hit must be a placeholder or false positive: no real credentials or
production bucket names.

### 3. Code quality

```bash
cargo fmt --check
cargo check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build
```

All must pass with no errors and no unexpected warnings.

GitHub Actions installs the current stable Rust toolchain for CI and release
asset builds, so local `cargo clippy` can lag or lead it.  If a release
workflow fails on a new lint, make the smallest source fix, push `main`, move
the release tag to the fixed commit, replace the local linux-aarch64 asset,
and rerun the release asset workflow.

The CI workflow runs the full Ubuntu check suite plus a macOS test job.  The
release asset workflow repeats source validation before building platform
artifacts.

### 4. Examples static QA

```bash
for f in examples/*.sh; do bash -n "$f" || exit 1; done
for f in examples/*.py; do python3 -m py_compile "$f" || exit 1; done
```

### 5. Docs link check (by inspection)

- [ ] `README.md` and `examples/README.md` internal links resolve.
- [ ] No broken internal links across root and `docs/` Markdown files.

### 6. Release build

Build the linux-aarch64 asset (or any platform asset built outside CI) with
`scripts/build-release.sh` as described above, using a `BUILD_MODE`
workaround on Ubuntu 20.04 arm64.

- [ ] `dist/s3-turbo-list-<version>-<os>-<arch>` and its `.sha256` exist.
- [ ] `--help` runs and `--version` prints the correct version.

### 7. Create and push the tag

```bash
git tag -a "v${VERSION}" -m "Release v${VERSION}"
git push origin main
git push origin "v${VERSION}"
```

If the environment cannot push tags (managed environments allow branch
pushes only), trigger the `release-tag.yml` workflow instead:

```bash
gh workflow run release-tag.yml --repo hxddh/s3-turbo-list -f tag="v${VERSION}"
```

The `tag` input is optional and defaults to the Cargo.toml version on main.
The workflow requires a matching CHANGELOG section, is idempotent (existing
tags are a no-op), and dispatches the release-assets build after tagging, so
one dispatch releases the version prepared on main.

`release-tag.yml` also runs on a schedule as release-on-version-bump
(maintainer-authorized): once main carries a Cargo.toml version with a
matching CHANGELOG section and no tag, the next scheduled run creates the tag
and dispatches the asset build.  Pushing a prepared release to main is
therefore sufficient to release.  **A version bump on main is treated as a
release instruction — keep unreleased version bumps off main.**

### 8. Build release assets

```bash
gh workflow run release-assets.yml --repo hxddh/s3-turbo-list -f tag="v${VERSION}"
RUN_ID="$(gh run list --repo hxddh/s3-turbo-list --workflow release-assets.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
gh run watch "$RUN_ID" --repo hxddh/s3-turbo-list --exit-status
```

The workflow validates the release source, then builds four platform assets:
Linux x86_64, Linux aarch64 (native `ubuntu-24.04-arm` runner), macOS Apple
Silicon, and macOS Intel.  The finalize job creates the GitHub release with
notes extracted from `CHANGELOG.md`, generates the combined `SHA256SUMS` plus
the linux-aarch64 single-file checksum, verifies the combined checksum file,
and uploads the complete asset set.

If arm64 runners are unavailable, build the linux-aarch64 asset on an
external arm64 host (with the `aws-lc-sys` workaround above on older
toolchains), upload it with `gh release upload`, and rerun the workflow — the
finalize job accepts a pre-uploaded linux-aarch64 asset as a fallback.

If a workflow appears stuck, inspect its jobs:

```bash
gh run view "$RUN_ID" --repo hxddh/s3-turbo-list --json status,conclusion,jobs \
  --jq '.status + " " + (.conclusion // ""), (.jobs[] | [.name,.status,.conclusion] | @tsv)'
```

### 9. Post-release verification

```bash
./scripts/verify-release-assets.sh "v${VERSION}"
git rev-parse main origin/main "v${VERSION}^{}"
```

- [ ] Release is not draft and not prerelease.
- [ ] Release contains four platform binaries, `SHA256SUMS`, and
      `s3-turbo-list-${VERSION}-linux-aarch64.sha256`.
- [ ] `sha256sum -c SHA256SUMS` reports `OK` for all four binaries.
- [ ] The current-platform binary prints the correct version, and `--help`
      runs without cloud access.
- [ ] `main`, `origin/main`, and the dereferenced release tag point to the
      intended commit.

### 10. Private repository dry run

Before pushing to a public repository:

- [ ] Create a private test repository on GitHub and push the release branch.
- [ ] Verify CI passes there.
- [ ] Download the CI artifact and verify the binary runs.

## See also

- [`scripts/check-release-env.sh`](../scripts/check-release-env.sh) — environment checker.
- [`scripts/build-release.sh`](../scripts/build-release.sh) — release build script.
- [`scripts/verify-release-assets.sh`](../scripts/verify-release-assets.sh) — published release asset verifier.
