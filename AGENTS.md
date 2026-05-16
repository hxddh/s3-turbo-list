# AGENTS.md - s3-turbo-list

## Project Context

`s3-turbo-list` is a Rust CLI for high-performance S3-compatible bucket listing, tracing, diffing, checkpoint/resume, and Parquet export.

The project prioritizes a clean, high-performance architecture for standards-compatible S3 behavior. Do not compromise the core architecture or performance model to work around non-standard provider behavior.

Repository:

- Local path: `/home/ubuntu/s3-turbo-list`
- GitHub: `https://github.com/hxddh/s3-turbo-list`
- Main branch: `main`
- Current release tag: `v0.1.2`

## Required Reading

Before non-trivial code changes, read:

- `README.md`
- `INSTALL.md`
- `docs/validation-results/final-validation-summary-20260514.md`
- `docs/validation-results/bos-listobjects-v2-pagination-compatibility-note-20260514.md`

For architecture or larger changes, also consult:

- `/home/ubuntu/thoughts/shared/designs/2026-05-13_11-59-14_s3-turbo-list.md`
- `/home/ubuntu/thoughts/shared/plans/2026-05-13_12-52-37_s3-turbo-list.md`

## Development Rules

- Prefer existing architecture and module boundaries.
- Keep changes narrowly scoped to the requested task.
- Preserve the high-performance S3-compatible design as the primary product identity.
- Provider-specific compatibility handling must be opt-in and must not alter the standard S3-compatible hot path.
- Do not add provider-specific workarounds that complicate or slow the standard S3-compatible hot path without explicit approval.
- Do not implement BOS-specific pagination workarounds by default.
- Prefer documentation and guardrails for BOS-specific caveats.
- Do not rewrite unrelated code.
- Do not remove or weaken existing tests unless explicitly requested.
- Do not change public CLI behavior without updating README, INSTALL, and examples where relevant.
- Treat `docs/validation-results/` as historical evidence. If behavior changes, add a new dated note instead of silently editing old validation conclusions.
- Do not present BOS as fully unrestricted for hinted multi-segment listing unless BOS fixes its service-side compatibility behavior and this has been revalidated.

## BOS Compatibility Position

BOS has a documented ListObjectsV2 compatibility issue when both `start_after` and `continuation-token` are present.

Project position:

- This is considered a BOS service-side S3 compatibility issue, not a general `s3-turbo-list` architecture problem.
- Single-segment BOS listing is supported and validated.
- Hinted multi-segment listing is safe on AWS S3 and MinIO.
- Hinted multi-segment listing is not authoritative on BOS until BOS fixes its ListObjectsV2 continuation-token + start_after compatibility.
- Do not compromise the tool's architecture, SDK paginator usage, or high-performance listing model to compensate for BOS-specific behavior.
- If needed, add warnings, documentation, or explicit user guardrails rather than endpoint-specific pagination rewrites.

## Cloud Safety Rules

Do not run real cloud commands unless the user explicitly authorizes that specific action.

This includes:

- `aws s3 ...`
- `aws s3api ...`
- `bcecmd ...`
- `mc ...` against non-local endpoints
- creating, deleting, or modifying buckets
- uploading or deleting objects
- running endpoint validation against AWS S3, BOS, OSS, R2, B2, Spaces, or other real providers

Allowed without extra confirmation:

- local source inspection
- Rust build/test commands
- unit/integration tests that do not contact real cloud endpoints
- MinIO-only local testing if a local MinIO service is already running and the command is clearly local

## Secrets And Sensitive Data

Never print, copy, commit, or expose credentials.

Sensitive local files include:

- `/home/ubuntu/.aws/credentials`
- `/home/ubuntu/.aws/config`
- `/home/ubuntu/.bos_env`
- `/home/ubuntu/.duckdb/setup_s3_secrets.sql`
- `/tmp/s3tl-validation-20260514-042634/aws/env.sh`
- `/tmp/s3tl-validation-20260514-042634/bos/env.sh`

Before commits or publication-related work, run a secret scan:

```bash
git grep -nE 'AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_(access_key_id|secret_access_key|session_token)\s*=|AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY)=|BEGIN (RSA|OPENSSH|EC|PRIVATE) KEY|ghp_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+' || true
```

## Build And Test Commands

Run these after code changes:

```bash
cargo fmt --check
cargo check
cargo test
cargo build
```

For docs-only changes, at minimum run:

```bash
cargo fmt --check
cargo check
```

Useful targeted tests:

```bash
cargo test --test checkpoint_integration
cargo test --test data_map_integration
cargo test --test diff_integration
cargo test --test trace_integration
cargo test --test cli_integration
```

## Release Build Note

On Ubuntu 20.04 arm64, release builds may hit the `aws-lc-sys` / GCC 9 memcmp issue.

Use the documented workarounds in:

- `BUILD.md`
- `docs/build-release.md`

Typical workaround:

```bash
export CC=clang
cargo build --release
```

Other documented options include GCC 10+ or disabling ASM.

## Known Technical Constraints

- BOS has a documented ListObjectsV2 compatibility issue when both `start_after` and `continuation-token` are present.
- Single-segment BOS listing works.
- Hinted multi-segment listing is safe on AWS S3 and MinIO.
- Hinted multi-segment listing is not authoritative on BOS until BOS fixes the service-side pagination compatibility issue.
- Single-segment diff is validated.
- Multi-segment diff coordination/testing remains an engineering priority.
- Release artifacts in `dist/` are generated and ignored.
- `target/` is generated and ignored.

## Useful Local Reference Material

Historical design and planning docs:

- `/home/ubuntu/thoughts/shared/designs/2026-05-13_11-59-14_s3-turbo-list.md`
- `/home/ubuntu/thoughts/shared/plans/2026-05-13_12-52-37_s3-turbo-list.md`

Validation artifacts:

- `/tmp/s3tl-validation-20260514-042634/`

Related compatibility research:

- `/home/ubuntu/s3-compatibility-test`
- `/home/ubuntu/headobj-compat-test`

Do not assume these external directories are safe to publish. Review for credentials, bucket names, endpoint details, and agent transcript residue before copying anything into the repository.

## Git Hygiene

Check status before editing:

```bash
git status --short --branch
```

Rules:

- Do not revert user changes unless explicitly requested.
- Do not create tags, releases, or make the repository public unless explicitly requested.
- Do not push unless explicitly requested.
- Keep commits focused and use conventional commit messages when committing.
