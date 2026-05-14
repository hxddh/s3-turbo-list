# Contributing to s3-turbo-list

## Local setup

```bash
# Clone the repo
git clone https://github.com/YOUR_ORG/s3-turbo-list.git
cd s3-turbo-list

# Build
cargo build

# Run tests
cargo test

# Check formatting
cargo fmt --check

# Static analysis
cargo check
```

## Development workflow

1. **Fork and branch.**  Create a feature branch from `master`.
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

## Code style

- Follow `rustfmt` defaults (`cargo fmt`).
- Use `cargo clippy` for lint guidance (not yet enforced in CI).
- Keep functions focused; prefer the module structure already in place.
- Write doc comments (`///`) for public APIs.

## Questions?

Open an issue or start a discussion.  Before opening a bug report, check
the [known limitations](../README.md#known-limitations) in the README.
