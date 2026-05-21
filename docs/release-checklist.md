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

## 9. GitHub private repo dry run

Before pushing to a public repository:

- [ ] Create a private test repository on GitHub.
- [ ] Push the release branch.
- [ ] Verify CI passes on the private repo.
- [ ] Download the CI artifact and verify the binary runs.

## 10. Tag creation

```bash
git tag -a "v${VERSION}" -m "s3-turbo-list v${VERSION}"
git push origin "v${VERSION}"
```

## 11. GitHub release draft

- [ ] Create a GitHub Release from the tag.
- [ ] Attach the binary and checksum file.
- [ ] Copy the relevant section from `CHANGELOG.md` into the release notes.

## 12. Post-release verification

- [ ] Download the release binary from GitHub.
- [ ] Verify checksum matches.
- [ ] Run `s3-turbo-list --version`.
- [ ] Run `s3-turbo-list --help` (no cloud endpoints).
- [ ] Confirm the version string in `Cargo.toml` matches the tag.
