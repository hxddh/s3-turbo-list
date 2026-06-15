# Contributing to s3-turbo-list

## Local setup

```bash
# Clone the repo
git clone https://github.com/hxddh/s3-turbo-list.git
cd s3-turbo-list

# Build
cargo build

# Run tests
cargo test

# Check formatting
cargo fmt --check

# Type check
cargo check
```

## Development workflow

1. **Fork and branch.**  Create a feature branch from `main`.
2. **Write tests.**  Add or update tests for any new behaviour.
3. **Run the full suite before pushing:**
   ```bash
   cargo fmt --check
   cargo check
   cargo test
   cargo build
   ```
4. **Open a pull request.**  Use the PR template checklist.

## Credential and data safety

- **Never commit credentials, access keys, secret keys, session tokens,
  signed URLs, or private keys.**
- **Never use real production bucket names** in examples, docs, issues,
  PR descriptions, commit messages, or test fixtures.
- Sanitise bucket names in trace excerpts before sharing.
- Endpoint validation **must use temporary buckets only** — not production
  buckets and not buckets shared with other workloads.
- **Clean up all cloud resources** after validation runs (delete temporary
  buckets, objects, and credentials).

## Endpoint compatibility

s3-turbo-list targets the S3 ListObjectsV2 API as implemented by AWS S3.
It is validated against MinIO and BOS as well, but not every S3-compatible
provider behaves identically.

- **Do not add endpoint-specific workarounds** without documenting the
  compatibility rationale in `docs/validation-results/`.
- **Prefer a compat-probe + trace** approach: validate the endpoint first,
  document findings, and update the compatibility matrix in `README.md`.
- If a provider deviates from S3 semantics, open an issue and classify it
  as a provider-side incompatibility — do not silently paper over it in
  the tool.

## Product scope: the CLI surface is frozen

s3-turbo-list deliberately stays small.  The subcommand list and the global
flag set are **frozen**: pull requests adding new subcommands, new global
flags, or new configuration knobs need an exceptional, documented case and
maintainer sign-off before any implementation work.

- Prefer making the **default path** faster or smarter over adding an option.
  (Example: startup structural discovery replaced the need for a "run
  auto-hints first" step instead of adding a flag for it.)
- Prefer removing or consolidating options over extending them.
- Performance claims need numbers: a `benchmark-local` comparison or a real
  endpoint measurement in the PR description.

## Code style

- Follow `rustfmt` defaults (`cargo fmt`).
- Run `cargo clippy --all-targets -- -D warnings`; it is enforced in CI.
- Keep functions focused; prefer the module structure already in place.
- Write doc comments (`///`) for public APIs.

## Questions?

Open an issue or start a discussion.  Before opening a bug report, check
the [known limitations](README.md#known-limitations) in the README.

## License

By contributing, you agree that your contributions are licensed under the
project's [Apache-2.0](LICENSE) license.
