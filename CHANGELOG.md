# Changelog

All notable changes to s3-turbo-list will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `manifest-summary --check --json` now includes a stable `check` summary with
  pass/fail counts, artifact counts, and row/schema/exit-code status values.
- `recipes release-check` for local-only pre-release verification commands.
- CI now runs clippy and an advisory Rust 1.75 MSRV check.

### Changed
- Cheatsheet, release documentation, and PR guidance now include the local
  release-check and clippy verification paths.
- `clap_mangen` is pinned to a Rust 1.75-compatible release while the broader
  AWS SDK transitive dependency MSRV remains under review.

## [0.1.20] - 2026-05-21

### Added
- Resource and syntax guardrails for `--filter` expressions: bounded expression
  length, AST size, operation count, expression depth, and literal/container
  sizes.

### Changed
- Object filters now accept only simple property comparisons and boolean/operator
  combinations over `SOURCE` and `TARGET`, rejecting function calls, method
  calls, indexing, arrays, maps, strings, and statements before any listing run.
- README and agent documentation now describe supported `--filter` expressions
  and rejection behavior.
- `recipes filter` now provides local filter examples and the supported syntax
  boundary.

## [0.1.19] - 2026-05-21

### Added
- Guardrails for `diff --resume`, which is deferred until paired diff/resume
  coordination is implemented.

### Changed
- Parquet output errors are now treated as output failures instead of warnings.
- Default checkpoint and conventional hints cache paths now sanitize bucket and
  region path components.
- Resume cursor advancement now uses the last processed object key instead of
  relying on provider `KeyCount`.

### Fixed
- `auto-hints` now applies `max_prefix_depth` to generated boundaries instead of
  accepting deeper prefixes unconditionally.
- Local hints merge/rebalance outputs no longer write local-tool names into the
  `scan_mode` field.

## [0.1.18] - 2026-05-20

### Added
- Dry-run and run-manifest warnings now state that `diff` uses authoritative
  single-segment mode until paired-segment diff coordination is implemented.
- Dry-run plans report `hints.source = "disabled_for_diff_single_segment"` for
  `diff` so agents can tell that conventional hints caches are intentionally
  ignored.
- `recipes diff-safe` for the recommended local preflight and manifest-check
  workflow around bucket diffing.

### Changed
- `diff` now ignores conventional auto-hints caches by default and stays on the
  single-segment authoritative path.

### Fixed
- `diff --hints-file` is now rejected before any S3 request instead of allowing
  an unsupported hinted multi-segment diff path.

## [0.1.17] - 2026-05-20

### Added
- Local endpoint-profile guardrails for profiles that require an explicit
  endpoint URL and for starter config endpoints that still contain template
  placeholders.
- `doctor` endpoint URL checks for missing provider-specific endpoints and
  unedited placeholder endpoint values.
- `manifest-summary --check` artifact integrity checks for recorded file size,
  SHA256, and Parquet row/schema metadata when those fields are present in the
  run manifest.

### Changed
- `init-config --profile r2` and `init-config --profile b2` now use the same
  path-style addressing defaults as the endpoint profile registry.

## [0.1.16] - 2026-05-20

### Added
- `manifest-summary --check` for local-only run validation with stable exit
  codes for CI and agents.
- Manifest summary check details for run status, exit code, fatal/output error
  counters, Parquet row consistency, and local recorded artifact path existence.
- `recipes verify` for concise manifest validation workflows.

### Changed
- `manifest-summary` now treats Parquet row equality as not applicable for
  `summary-only`, `tsv`, and `ndjson` runs instead of reporting a misleading
  mismatch.
- Documentation now includes a compact output-mode and validation guide.

### Fixed
- `--continuation-token` is now wired into single-chain `list` requests and
  guarded against confusing combinations with hints, checkpoint resume, diff,
  or `--start-after`.

## [0.1.15] - 2026-05-20

### Added
- `list --output-format parquet|tsv|ndjson`, with `parquet` remaining the
  default artifact-writing mode and TSV/NDJSON streaming rows to stdout.
- Local-only `manifest-summary` for turning run manifest JSON into human or
  machine-readable summaries without contacting S3.
- `recipes pipe` for shell, jq, and agent-friendly list workflows.

### Changed
- Dry-run plans and run manifests now record the selected list output format.
- TSV/NDJSON list runs skip Parquet and KeySpace artifact paths while retaining
  aggregate manifest metrics.
- Release asset workflow now uses Node 24-native GitHub Actions versions for
  checkout, upload-artifact, and download-artifact.

## [0.1.14] - 2026-05-20

### Added
- `--summary-only` for `list` runs that scan S3 and report aggregate metrics
  without writing Parquet or KeySpace outputs.
- Manifest metrics for `bytes_total`, `top_prefixes`, and `summary_only`.
- `recipes summary` for concise object-count and byte-count workflows.

### Changed
- List metrics now include aggregate byte and top-prefix summaries for agent
  manifests.
- `examples/read_manifest.py` now prints summary-only metrics and top prefixes.
- Documentation now distinguishes default Parquet output, summary-only scans,
  and dry-run planning.

## [0.1.13] - 2026-05-19

### Added
- Runtime and dry-run warnings when an endpoint compatibility `--profile` such
  as `bos`, `minio`, `r2`, `b2`, or `oss` may be confused with an AWS
  credentials profile.
- `doctor` hint when `AWS_PROFILE` is set to a name that also matches an
  endpoint compatibility preset.

### Changed
- `list --help`, README, INSTALL, recipes, quickstart, and cheatsheet now make
  the recursive full-bucket `--delimiter ''` path more visible while preserving
  the existing default delimiter behavior.
- Run manifests now include the same guardrail warnings exposed in dry-run
  plans, giving agents a consistent preflight and post-run signal.

## [0.1.12] - 2026-05-19

### Added
- Local `init-config` command for writing starter TOML configs without touching
  AWS credentials, shell configuration, or global files.
- Local `recipes`, `quickstart`, and `cheatsheet` commands for concise beginner
  and copy-paste workflows.
- Global `--output-dir` shortcut for `list` and `diff` Parquet/KeySpace output
  paths, with real runs creating the requested directory.
- `doctor --simple` and `doctor --fix-suggestions` for compact local
  diagnostics and next-step commands.
- `--overwrite` for local generated config/hints outputs.

### Changed
- Successful human-mode runs now print a compact `Wrote:` summary with generated
  output paths.
- `doctor` now reports local config file presence, `AWS_PROFILE`, and endpoint
  compatibility profile status.

## [0.1.11] - 2026-05-19

### Added
- Local `hints-merge` command for combining TOML and plain hints files into a
  sorted, deduplicated TOML hints cache without contacting S3.
- Local `trace-summary` command for summarizing `--trace-compat` JSONL files
  into human text, markdown, or JSON for agents and CI.
- Conservative local `hints-rebalance` command that uses trace segment
  summaries and per-page key samples to propose or write next-run hints for
  long-tail segments.
- Agent-friendly `--output-format json`, `--machine-readable`, and
  `--emit-manifest` surfaces for the new local hints tooling commands.
- Optional per-page `first_key` and `last_key` trace fields when S3 tracing is
  enabled, giving offline rebalance tooling real observed key cut points.

### Changed
- Local hints/trace tooling is handled before config loading and before runtime
  setup, keeping these commands no-cloud and independent of S3 credentials.
- Documentation now describes trace-driven hints workflows for agents and
  large-bucket tuning.

## [0.1.10] - 2026-05-18

### Added
- `discover-prefixes` command to page through ListObjectsV2 `CommonPrefixes`
  and write prefix hints locally, without relying on external cloud CLIs.
- Auto-hints metadata for prefix-scoped scans, max-keys, prefix map bounds, and
  bounded prefix-count mode.
- Auto-hints heartbeat logging during long scans.
- Trace `ListObjectsV2SegmentSummary` events with per-segment pages, object
  count, CommonPrefixes count, elapsed time, boundaries, and completion reason.
- Local mock coverage for boundary-key correctness, `--no-auto-hints`,
  prefix/max-keys auto-hints, paginated CommonPrefixes discovery, and segment
  summary traces.

### Changed
- `auto-hints` now honors global `--prefix` and `--max-keys` for its sequential
  object scan and uses the resolved S3 retry/timeout configuration.
- `--no-auto-hints` now skips conventional hints-cache loading when no explicit
  `--hints-file` is provided, forcing single-segment fallback.
- Documentation now explains segmented listing parallelism, hints boundary
  caveats, prefix-scoped sampled estimates, compression choices, and endpoint
  profile maturity.

### Fixed
- Hinted listing no longer drops an object whose key is exactly equal to a
  segment boundary.  Segment ranges now include the right boundary because the
  adjacent segment starts strictly after that same key.

## [0.1.9] - 2026-05-17

### Added
- Local S3 protocol mock integration harness covering ListObjectsV2 pagination,
  continuation tokens, `start-after`, `prefix`, `delimiter`, `max-keys`, XML
  error responses, SDK retry of transient list failures, `compat-probe`, and
  checkpoint/resume segment identity without contacting real cloud endpoints.
- Documentation for the local mock harness scope, safety boundaries, and
  maintenance expectations.

### Changed
- Resume now preserves original key-space segment starts when filtering out
  completed checkpoint segments, preventing resumed hinted runs from reusing a
  synthetic single segment that starts at the beginning of the bucket.
- Release asset workflow opts into GitHub Actions Node.js 24 execution ahead of
  the Node.js 20 runner migration.

## [0.1.8] - 2026-05-17

### Added
- Local synthetic `benchmark-local` command for list-mode streaming output throughput reports without contacting S3.
- `scripts/benchmark-local.sh` wrapper and benchmarking documentation with machine-readable JSON output.
- Local-only endpoint profile inspection via `profiles list` and `profiles show`, covering `aws`, `minio`, `bos`, `r2`, `b2`, and `oss`.
- Shell completion generation via `completions <shell>` and man page generation via `man`.
- Agent/config summaries now include whether the selected endpoint profile is known and its documented warnings.

### Changed
- Endpoint profile defaults are now backed by a shared registry instead of hard-coded `bos`/`minio` branches.
- README roadmap now marks benchmark harness, CLI help polish, and optional endpoint compatibility profiles as delivered.

## [0.1.7] - 2026-05-17

### Added
- Run manifests now include artifact summaries for Parquet, KS, hints, trace, and log outputs, including size, SHA256, line counts, and Parquet metadata.
- Dry-run plans now summarize local hints files and checkpoint compatibility without contacting S3.
- Dry-run file conflict entries now report output parent directory existence and writability.
- Synthetic list-mode streaming integration coverage for 20k local objects across multiple prefixes and batches.
- Agent examples for local dry-run planning, guarded manifest-producing runs, and manifest inspection.

### Changed
- Agent documentation now describes artifact summaries and the expanded dry-run plan fields.
- Data-map integration tests now share a local correctness helper for Parquet schema, row count, list-mode DiffFlag, and KS totals.

## [0.1.6] - 2026-05-17

### Added
- Agent-friendly machine-readable CLI surfaces: `--agent`, `--dry-run`, `--plan-json`, and `--run-manifest`.
- Local-only `config-inspect --json` and `doctor --local-only --json` commands for automation preflight checks without contacting S3.
- Stable exit-code classes for config, output, network/fatal, interrupted, and internal-error outcomes.
- Run manifest and dry-run plan JSON schemas with resolved config, inputs, planned outputs, checkpoint metadata, and runtime metrics.
- Agent usage documentation covering no-cloud planning, local doctor checks, manifests, and retry interpretation.

### Changed
- Data-map final metrics are now recorded into shared runtime state so agent manifests can report Parquet rows, KS entries, batches, objects, and prefix counts.
- Fatal list-task failures now increment a tracked runtime error counter for machine-readable final status.

## [0.1.5] - 2026-05-17

### Added
- Tuning reference documentation for core default values and TOML-only advanced knobs.
- Release workflow assertions for the complete four-platform asset set and SHA256SUMS entries.
- Integration coverage for `hints-validate --json` estimate metadata.
- Integration coverage for list-mode streaming KS ordering and Parquet compression configuration.

### Changed
- Documentation now describes checkpoint identity fields consistently across README and INSTALL.
- Diff-mode documentation now states that Equal rows are always emitted in current releases.
- Validation status wording no longer hard-codes a stale test count.

## [0.1.4] - 2026-05-16

### Added
- List mode now streams received object batches directly to Parquet while maintaining lightweight KS prefix counts, reducing memory pressure on large buckets.
- Auto-hints TOML caches now include per-segment object estimates with sampled/full metadata.
- `hints-validate` now reports estimate summaries, including sampled status, count, min/max/sum, and preview estimates.
- Local integration coverage for list-mode streaming output and `DiffFlag = 0` semantics.

### Changed
- Diff mode continues to use the existing `PrefixMap` matching path; only list mode uses streaming output.
- CLI integration tests now execute Cargo's prebuilt test binary instead of repeatedly shelling out through `cargo run`.
- Release asset workflow now creates the GitHub Release when missing before uploading assets, while still requiring the externally built linux-aarch64 asset.
- Documentation refreshed for v0.1.4 streaming readiness and release hardening.

## [0.1.3] - 2026-05-16

### Added
- `hints-validate` command for local TOML/plain-text hints inspection without making cloud requests.
- Auto-hints bounded sampling via `--sample-limit` and `--max-pages`, with sampled-scan metadata written to TOML caches.
- Runtime data-map metrics for received batches, received objects, prefix/object counts, throughput, Parquet rows, KS entries, and write elapsed time.
- Example scripts for sampled auto-hints and hints validation.

### Changed
- Data-map ingestion now groups each received batch by prefix before calling `bulk_insert`, reducing hot-path per-object allocation and lookup overhead.
- Auto-hints output now reports scan mode, scanned objects/pages, unique prefixes, boundary count, and preview boundaries.
- Documentation refreshed for v0.1.3 large-run readiness work.

## [0.1.2] - 2026-05-16

### Added
- Release consistency checks for versioned docs, changelog entries, release workflow defaults, and stale agent release guidance.
- BOS guardrail warnings when `profile = "bos"` is combined with hinted multi-segment listing or auto-hints generation.
- Compat-probe pagination verification now follows the first continuation token and records pagination metadata in the JSON report.

### Changed
- Release asset workflow now derives asset names from the input tag instead of hard-coding versioned filenames.
- Compat-probe now uses configured S3 retry and timeout settings.
- Parquet writer now honors output `row_group_size`, `compression`, and `compression_level` config values.
- Documentation refreshed for the v0.1.2 release and current BOS virtual-hosted-first guidance.

## [0.1.1] - 2026-05-16

### Added
- Publication readiness: README with usage and compatibility guidance.
- Publication readiness: runnable example scripts under `examples/`.
- Publication readiness: `.gitignore`, `LICENSE` (MIT), `CONTRIBUTING.md`, `SECURITY.md`.
- Publication readiness: GitHub CI workflow, PR template, issue templates.
- Publication readiness: `docs/release-checklist.md`.

### Changed
- BOS documentation updated to recommend virtual-hosted addressing (per BOS official guidance).
- `--profile bos` now defaults to virtual-hosted addressing instead of path-style.
- `--endpoint-url` no longer implicitly forces path-style addressing; use `--addressing-style path` or `--force-path-style` when path-style is required.

### Fixed
- ListObjectsV2 per-page watchdog now honors `operation_timeout_secs` instead of a hard-coded 5 seconds.
- ListObjectsV2 stream timeouts are retried within the configured `max_attempts` budget instead of being treated as immediately fatal.

## [0.1.0] — 2026-05-14

### Added
- High-performance concurrent S3-compatible bucket listing via ListObjectsV2.
- Multi-threaded / key-space segmented listing with auto-discovered or user-supplied hints.
- Parquet output (gzip-compressed) with Key, Size, LastModified, ETag, DiffFlag columns.
- KS keyspace output (CSV with per-prefix object counts).
- Structured trace JSONL output (S3CompatEvent — 28 fields per API call).
- Bi-directional diff mode with DiffFlag annotations (Equal, Left-only, Right-only, Asterisk).
- Checkpoint / resume with identity verification (bucket, region, prefix, delimiter, max-keys, addressing, profile, mode).
- Compat-probe for S3-compatible endpoint validation (6 test categories).
- Endpoint validation workflows for MinIO, AWS S3, and BOS (Baidu Object Storage).

### Fixed
- TOML hints parser: safe deserialisation with boundary validation (prevents TOML syntax from leaking into S3 `start-after` values).

### Documentation
- Validation results: MinIO Part A, AWS S3 baseline, BOS S3-compatible.
- BOS ListObjectsV2 pagination compatibility note (start_after + continuation_token interaction).

### Known Limitations
- BOS hinted multi-segment authoritative scans should wait for a BOS-side ListObjectsV2 compatibility fix, or use single-segment fallback.
- Hinted multi-segment diff paired coordination is deferred.
- Release build on Ubuntu 20.04 arm64 may require `aws-lc-sys` workaround (documented in `BUILD.md`).
