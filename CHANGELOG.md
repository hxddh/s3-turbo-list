# Changelog

All notable changes to s3-turbo-list will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Publication readiness: README with usage and compatibility guidance.
- Publication readiness: runnable example scripts under `examples/`.
- Publication readiness: `.gitignore`, `LICENSE` (MIT), `CONTRIBUTING.md`, `SECURITY.md`.
- Publication readiness: GitHub CI workflow, PR template, issue templates.
- Publication readiness: `docs/release-checklist.md`.

### Changed
- BOS documentation updated to recommend virtual-hosted addressing (per BOS official guidance).

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
