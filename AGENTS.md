# AGENTS.md - s3-turbo-list

## Project Context

`s3tl` (repository `s3-turbo-list`) is a Rust CLI for high-performance
S3-compatible bucket listing and diffing to Parquet, with one
machine-readable result document per run.  GitHub:
`https://github.com/hxddh/s3-turbo-list`, main branch `main`.
The release source of truth is the latest git tag / GitHub Release.

The project prioritizes a clean, high-performance architecture for
standards-compatible S3 behavior.  Do not compromise the core architecture
or performance model to work around non-standard provider behavior.

Before non-trivial code changes, read `README.md`, `docs/tuning.md`, and the
relevant notes in `docs/validation-results/`.

## Development Rules

- **The CLI surface is minimal by design** (see CONTRIBUTING.md): three
  commands (`list`, `diff`, `check`), a handful of options, no config file.
  New commands, options or knobs need an exceptional, documented case.
- **The result contract only grows** (`docs/agent-usage.md`): within major
  version 1, result fields, error codes, exit codes, the Parquet schemas and
  the checkpoint format may gain members but never rename, retype or remove
  one.  New error conditions get a stable `error.code`.
- Prefer existing architecture and module boundaries; keep changes narrowly
  scoped to the requested task; do not rewrite unrelated code.
- Provider-specific compatibility handling must be opt-in and must not alter
  or slow the standard S3-compatible hot path.  Prefer documentation and
  guardrails over endpoint-specific pagination rewrites.
- Do not remove or weaken existing tests unless explicitly requested.
- Do not change public CLI behavior without updating README, docs and
  CHANGELOG.
- Treat `docs/validation-results/` as historical evidence: if behavior
  changes, add a new dated note instead of editing old conclusions.

## BOS Compatibility Position

BOS is fully compatible with the S3 ListObjectsV2 API. The historical
incompatibility — BOS restarted listing from `start_after` when a
`continuation-token` was also present — has been resolved service-side by
BOS. Consequences:

- BOS is treated like any other S3-compatible endpoint: startup structural
  discovery, bisection and runtime splitting all apply, with no
  special-casing (1.0 has no provider presets at all).
- There is no BOS-specific gating in code. Runtime splitting is controlled
  only by the listing shape (`--delimiter` listings are never split).
- Do not reintroduce BOS-specific pagination workarounds or profile gates;
  the service now behaves per the S3 contract.
- See `docs/validation-results/` for the dated note recording BOS's fix; the
  pre-fix 2026-05-14 notes are retained as historical evidence.

## Other Design Constraints

- Diff partitions and splits each side like list mode and merges the sides
  in key order (`src/segment_order.rs`); diff does not checkpoint, and any
  segment failure or ordering violation fails the run.
- Output is a dataset directory committed atomically (temp parts renamed on
  commit). A list run that is interrupted, or fails on the network or its
  setup after listing something, commits what it listed with a checkpoint
  for `--resume` (since 1.6); any other failed run leaves the directory as it
  was.
- `dist/` and `target/` are generated and ignored.

## Cloud Safety Rules

Do not run real cloud commands unless the user explicitly authorizes that
specific action — including `aws s3 ...`, `bcecmd ...`, `mc ...` against
non-local endpoints, bucket/object mutations, and endpoint validation
against real providers.

Allowed without extra confirmation: local source inspection, Rust
build/test commands, tests that do not contact real cloud endpoints,
`scripts/mock-s3.py` on 127.0.0.1, and clearly-local MinIO testing against
an already-running local service.

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
memcmp issue; use the workarounds in `docs/releasing.md` (clang, GCC 10+, or no-ASM).

## Git Hygiene

- Do not revert user changes unless explicitly requested.
- Do not create tags, releases, or make the repository public unless
  explicitly requested.
- Do not push unless explicitly requested.
- Keep commits focused; use conventional commit messages.
