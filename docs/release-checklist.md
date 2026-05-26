# Release Checklist

Follow these steps to publish a release of s3-turbo-list.

## 0. Environment check

Run the release environment checker first:

```bash
./scripts/check-release-env.sh
s3-turbo-list recipes release-check
```

It prints OS, architecture, Rust toolchain, C compilers, git state,
and warns if an `aws-lc-sys` release build workaround is needed.
The recipe prints the local-only command sequence without running cloud
endpoint validation.

## 1. Local pre-release checks

- [ ] Working tree clean (`git status --short` empty).
- [ ] On the correct branch (typically `main` for release publication).
- [ ] All commits intended for this release are present.
- [ ] `scripts/check-release-env.sh` reports no blockers.

## 2. Secret scan

Run a local hygiene scan on the full working tree:

```bash
# Credential patterns
rg -in 'AKIA|aws_access_key|aws_secret|secret_key|session_token' \
   --glob '!target/' --glob '!.git/' .

# Private keys
rg -l '-----BEGIN.*PRIVATE KEY-----' --glob '!target/' --glob '!.git/' .

# Real bucket-looking names (manual review)
rg -n 's3tl-|my-real-|prod-' examples/ docs/ 2>/dev/null
```

All hits must be either placeholders or false positives — no real
credentials or production bucket names.

## 3. Code quality

```bash
cargo fmt --check
cargo check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build
```

All must pass with zero errors and zero unexpected warnings.

GitHub Actions installs the current stable Rust toolchain during CI and release
asset builds.  Local `cargo clippy` can lag or lead the GitHub stable toolchain;
if a release workflow fails on a new lint, make the smallest source fix, push
`main`, move the release tag to the fixed commit, replace the local
linux-aarch64 asset, and rerun the release asset workflow.

## 4. Examples static QA

```bash
# Shell syntax
for f in examples/*.sh; do bash -n "$f" || exit 1; done

# Python syntax
python3 -m py_compile examples/read-parquet.py
python3 -m py_compile examples/inspect-trace.py
```

## 5. Docs link check (by inspection)

- [ ] `README.md` internal links resolve.
- [ ] `examples/README.md` internal links resolve.
- [ ] Cross-references to `docs/validation-results/` files exist.

## 6. Release build

Use the release build script (see [`docs/build-release.md`](build-release.md)):

```bash
# Standard build
BUILD_MODE=default ./scripts/build-release.sh

# On Ubuntu 20.04 arm64, use a workaround:
BUILD_MODE=clang   ./scripts/build-release.sh   # if clang is installed
BUILD_MODE=gcc10   ./scripts/build-release.sh   # if gcc-10 is installed
BUILD_MODE=no-asm  ./scripts/build-release.sh   # fallback (no ASM)
```

The script handles binary naming, checksum generation, and `--help`/`--version`
verification automatically.  Output lands in `dist/`.

- [ ] Binary at `dist/s3-turbo-list-<version>-<os>-<arch>` exists.
- [ ] `dist/s3-turbo-list-<version>-<os>-<arch>.sha256` exists.
- [ ] `./dist/<binary> --help` runs successfully.
- [ ] `./dist/<binary> --version` prints the correct version.

## 7. Create and Push the Tag

```bash
git tag -a "v${VERSION}" -m "Release v${VERSION}"
git push origin main
git push origin "v${VERSION}"
```

## 8. Create Release and Upload linux-aarch64

On the Oracle arm64 host, upload the locally built Linux ARM64 binary first:

```bash
gh release create "v${VERSION}" \
  "dist/s3-turbo-list-${VERSION}-linux-aarch64" \
  "dist/s3-turbo-list-${VERSION}-linux-aarch64.sha256" \
  --repo hxddh/s3-turbo-list \
  --verify-tag \
  --title "v${VERSION}" \
  --notes-file /tmp/s3-turbo-list-v${VERSION}-notes.md
```

If the release already exists after a rerun or fix, replace just the local
linux-aarch64 assets:

```bash
gh release upload "v${VERSION}" \
  "dist/s3-turbo-list-${VERSION}-linux-aarch64" \
  "dist/s3-turbo-list-${VERSION}-linux-aarch64.sha256" \
  --repo hxddh/s3-turbo-list \
  --clobber
```

## 9. Build Cross-Platform Release Assets

Trigger the repository workflow after the linux-aarch64 asset exists:

```bash
gh workflow run release-assets.yml --repo hxddh/s3-turbo-list -f tag="v${VERSION}"
gh run watch --repo hxddh/s3-turbo-list --exit-status
```

The workflow first validates the release source once on Linux, then builds
Linux x86_64, macOS Apple Silicon, and macOS Intel assets.  The finalize job
downloads the existing linux-aarch64 asset, generates the combined
`SHA256SUMS`, verifies it, and uploads the complete release asset set.

If a workflow appears stuck, inspect individual job steps:

```bash
gh run view "$RUN_ID" \
  --repo hxddh/s3-turbo-list \
  --json status,conclusion,jobs
```

## 10. Post-Release Verification

Download and verify the published assets from GitHub:

```bash
./scripts/verify-release-assets.sh "v${VERSION}"
git rev-parse main origin/main "v${VERSION}^{}"
```

- [ ] Release is not draft and not prerelease.
- [ ] Release contains four platform binaries plus `SHA256SUMS`.
- [ ] `sha256sum -c SHA256SUMS` reports `OK` for all four binaries.
- [ ] Current-platform binary prints the correct version.
- [ ] Current-platform binary `--help` runs without cloud access.
- [ ] `main`, `origin/main`, and the dereferenced release tag point to the
      intended commit.

## 11. GitHub private repo dry run

Before pushing to a public repository:

- [ ] Create a private test repository on GitHub.
- [ ] Push the release branch.
- [ ] Verify CI passes on the private repo.
- [ ] Download the CI artifact and verify the binary runs.
