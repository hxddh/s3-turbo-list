# Release Checklist

Follow these steps to publish a release of s3-turbo-list.

## 1. Local pre-release checks

- [ ] Working tree clean (`git status --short` empty).
- [ ] On the correct branch (typically `master`).
- [ ] All commits intended for this release are present.

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

```bash
# Standard release build
cargo build --release

# On Ubuntu 20.04 arm64, use the aws-lc-sys workaround:
# export CC=clang && cargo build --release
# See BUILD.md for details.
```

## 7. Binary naming

Copy the release binary with a versioned name:

```bash
VERSION=$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
cp target/release/s3-turbo-list "s3-turbo-list-${VERSION}-${OS}-${ARCH}"
```

## 8. Checksum

```bash
sha256sum "s3-turbo-list-${VERSION}-${OS}-${ARCH}" > "s3-turbo-list-${VERSION}-${OS}-${ARCH}.sha256"
```

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
