# Contributing to s3tl

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
   cargo clippy --all-targets -- -D warnings
   cargo test
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

s3tl targets the S3 ListObjectsV2 API as implemented by AWS S3.
It is validated against MinIO and BOS as well, but not every S3-compatible
provider behaves identically.

- **Do not add endpoint-specific workarounds** without documenting the
  compatibility rationale in `docs/validation-results/`.
- **Prefer a `s3tl check` + `--trace`** approach: validate the endpoint first,
  document findings, and update the compatibility matrix in `README.md`.
- If a provider deviates from S3 semantics, open an issue and classify it
  as a provider-side incompatibility — do not silently paper over it in
  the tool.

## Product scope: a minimal surface and a contract that only grows

s3tl deliberately stays small: three commands (`list`, `diff`, `check`), a
handful of options, no config file. 1.0 removed everything that another
tool does as well (filters, statistics files, manifests, provider presets);
read the Parquet output with duckdb or pandas instead.

- New commands, options or knobs need an exceptional, documented case and
  maintainer sign-off before any implementation work. Prefer making the
  default path faster or smarter over adding an option.
- The result contract (`docs/agent-usage.md`) — result fields, error codes,
  exit codes, the Parquet schemas, the checkpoint format, trace fields —
  only grows within a major version: add, never rename, retype or remove.
  A new failure mode gets a stable `error.code` and a test.
- Performance claims need numbers: `cargo run --release --example
  bench_local` and/or an A/B run against `scripts/mock-s3.py` (see
  [`docs/development.md`](docs/development.md)), or a real endpoint
  measurement in the PR description.

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
