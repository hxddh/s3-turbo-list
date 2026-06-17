# AGENTS.md - s3-turbo-list

## Project Context

`s3-turbo-list` is a Rust CLI for high-performance S3-compatible bucket
listing, diffing, checkpoint/resume, and Parquet export.  GitHub:
`https://github.com/hxddh/s3-turbo-list`, main branch `main`.
The release source of truth is the latest git tag / GitHub Release.

The project prioritizes a clean, high-performance architecture for
standards-compatible S3 behavior.  Do not compromise the core architecture
or performance model to work around non-standard provider behavior.

Before non-trivial code changes, read `README.md`, `docs/tuning.md`, and the
relevant notes in `docs/validation-results/`.

## Development Rules

- **The CLI surface is frozen** (see CONTRIBUTING.md): no new subcommands,
  global flags, or config knobs without an exceptional, documented case.
- Prefer existing architecture and module boundaries; keep changes narrowly
  scoped to the requested task; do not rewrite unrelated code.
- Provider-specific compatibility handling must be opt-in and must not alter
  or slow the standard S3-compatible hot path.  Prefer documentation and
  guardrails over endpoint-specific pagination rewrites.
- Do not remove or weaken existing tests unless explicitly requested.
- Do not change public CLI behavior without updating README and docs.
- Treat `docs/validation-results/` as historical evidence: if behavior
  changes, add a new dated note instead of editing old conclusions.

## BOS Compatibility Position

BOS is fully compatible with the S3 ListObjectsV2 API. The historical
incompatibility — BOS restarted listing from `start_after` when a
`continuation-token` was also present — has been resolved service-side by
BOS. Consequences:

- BOS is treated like any other S3-compatible endpoint. Single-segment
  listing, hinted multi-segment listing, startup structural discovery, and
  runtime splitting all run for the `bos` profile with no special-casing.
- There is no BOS-specific gating in code. Discovery and the hints resolver
  no longer exclude the `bos` profile. Runtime splitting is controlled only
  by mode/flags (`allow_split` keys on diff-mode / `--start-after` /
  `--continuation-token`, never on the profile).
- Do not reintroduce BOS-specific pagination workarounds or profile gates;
  the service now behaves per the S3 contract.
- See `docs/validation-results/` for the dated note recording BOS's fix; the
  pre-fix 2026-05-14 notes are retained as historical evidence.

## Other Design Constraints

- Diff partitions each side automatically and merges the ordered segment
  streams; `diff --hints-file` and `diff --resume` are intentionally
  rejected, and any segment failure or ordering violation fails the run.
- `dist/` and `target/` are generated and ignored.

## Cloud Safety Rules

Do not run real cloud commands unless the user explicitly authorizes that
specific action — including `aws s3 ...`, `bcecmd ...`, `mc ...` against
non-local endpoints, bucket/object mutations, and endpoint validation
against real providers.

Allowed without extra confirmation: local source inspection, Rust
build/test commands, tests that do not contact real cloud endpoints, and
clearly-local MinIO testing against an already-running local service.

## Secrets

Never print, copy, commit, or expose credentials.  Before commits or
publication-related work, run a secret scan:

```bash
git grep -nE 'AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_(access_key_id|secret_access_key|session_token)\s*=|AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY)=|BEGIN (RSA|OPENSSH|EC|PRIVATE) KEY|ghp_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+' || true
```

## Build And Test

After code changes:

```bash
cargo fmt --check
cargo check
cargo clippy --all-targets -- -D warnings
cargo test
```

For docs-only changes, `cargo fmt --check` and `cargo check` suffice.  The
local S3 protocol mock lives in `tests/s3_mock_integration.rs` and never
contacts real endpoints.

Release builds on Ubuntu 20.04 arm64 may hit the `aws-lc-sys` / GCC 9
memcmp issue; use the workarounds in `BUILD.md` (clang, GCC 10+, or no-ASM).

## Git Hygiene

- Do not revert user changes unless explicitly requested.
- Do not create tags, releases, or make the repository public unless
  explicitly requested.
- Do not push unless explicitly requested.
- Keep commits focused; use conventional commit messages.
