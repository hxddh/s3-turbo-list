# Changelog

All notable changes to s3-turbo-list will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
