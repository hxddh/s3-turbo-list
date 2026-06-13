# Changelog

All notable changes to s3-turbo-list will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] - 2026-06-13

### Removed
- **Removed the `auto-hints` and `discover-prefixes` subcommands** (deprecated
  in 0.7.0). Startup structural discovery and runtime splitting partition
  buckets automatically with zero flags, so the separate sequential-scan
  commands no longer earned their surface area. Their config section
  `[auto_hints]` (`sample_threshold`, `max_prefix_depth`, `max_prefix_entries`)
  is gone as well. Existing hints caches, `--hints-file`, `hints-validate`,
  and `hints-merge` keep working unchanged.

### Changed
- **Reduced the default retry backoff** `s3.initial_backoff_secs` from `30` to
  `1`. A single throttle (HTTP 503 `SlowDown`) previously added 30+ seconds of
  wall-clock to a run; the SDK still grows the delay exponentially across
  `max_attempts`, so transient throttling recovers quickly without a
  tail-latency spike.

### Documentation
- Documented the single-bucket `ListObjectsV2` request-rate ceiling in
  `docs/tuning.md`: once enough segments saturate the provider's per-bucket
  request rate, additional `--concurrency`/`--threads` cannot raise throughput.
  Added tuning guidance and corrected stale auto-hints references across the
  README, tuning, and agent-usage docs.

## [0.7.0] - 2026-06-13

### Changed
- **Diff lists both sides in parallel.** Each side partitions
  automatically (cached hints or startup discovery, the same sources as
  list mode) and lists its segments concurrently; the merge consumes each
  side's segment outputs in key order, so the ordered merge-join engine —
  including its loud-failure ordering guard — is unchanged. Memory stays
  bounded by the per-segment prefetch windows (4 batches each, at most 32
  segments in flight per side). Diff on a 1M-object bucket previously paid
  full sequential listing time per side. `--hints-file`/`--resume` remain
  rejected for diff; the dry-run hints plan `source` for diff is now
  `diff_per_side_automatic`.
- Deprecated `auto-hints` and `discover-prefixes`: startup discovery and
  runtime splitting cover both commands' use cases, and auto-hints' full
  sequential scan is often slower than simply running the listing. Both
  warn on invocation and will be removed in a future release;
  `--hints-file`, `hints-validate`, and `hints-merge` stay.


## [0.6.1] - 2026-06-12

Replaces v0.6.0, whose release pipeline was halted by a timing-flaky CI
test (the tag exists but no assets were ever published). No functional
difference from the intended 0.6.0; the flat-split integration test now
uses a wide enough window for slow CI runners.

### Added
- Flat-range runtime splitting: ranges without CommonPrefix structure can
  now split too. Candidate cuts are derived from the segment's cursor — a
  real key inside the live region — by bumping one character at several
  tail depths; each candidate costs one max_keys=1 request and the first
  real key returned inside the range becomes the cut. Third-party OSS
  benchmarks showed structureless prefixes capping real parallelism at the
  bucket's top-level prefix count (2.3x with 80% of concurrency idle);
  flat and skewed buckets alike now fan out until concurrency is used.
- Region-derived profile endpoints: `--profile oss/bos/b2` now template
  the endpoint from `--region` (`{region}.aliyuncs.com`,
  `s3.{region}.bcebos.com`, `s3.{region}.backblazeb2.com`), removing
  `--endpoint-url` from everyday commands for these providers. This also
  fixes `bos` previously hard-coding the `bj` endpoint for every region.
  Explicit endpoint values always win; the generic no-profile path is
  unchanged.

### Changed
- `runtime.worker_threads` defaults to the machine's CPU count instead of
  a hard-coded 10; benchmarks showed oversubscribing a 2-vCPU host
  degrades throughput.
- `compat-probe` reads the global `--endpoint-url` flag, fixing the flag
  inconsistency third-party testers reported; the subcommand-local
  `--endpoint` remains as an override and is now optional.
- Documentation refreshed: defaults-first tuning guidance, region-derived
  profile examples, benchmark scenario descriptions aligned with the
  streaming merge engine.

## [0.5.0] - 2026-06-12

### Changed
- **Recursive listing is now the default** (`--delimiter ''`). The S3
  ListObjectsV2 API itself defaults to no delimiter, the performance path
  (startup discovery, runtime splitting, parallel segments) only engages on
  recursive runs, and every documented example carried the boilerplate
  override. Hierarchical listing remains available via `--delimiter '/'`.
  Checkpoints recorded with the old default no longer match the new
  identity and are discarded with a warning, never mis-resumed.
- **Diff streams as an ordered merge-join.** Each side is an authoritative
  single segment, so its keys arrive in S3 lexicographic order; merging the
  two ordered streams classifies every key on the fly and streams rows
  straight to Parquet. Memory is bounded by channel buffers instead of the
  combined object count (2M-object diff: 375 MB → 63 MB peak RSS, flat as
  buckets grow), and local diff benchmarks run +65% (diff-map) to +148%
  (diff-output) faster. Classification semantics, DiffFlag values, equal-row
  output, and KS counts are unchanged. A side returning keys out of
  lexicographic order now fails the run loudly instead of producing a wrong
  diff. `benchmark-local --benchmark diff-map` now measures the merge plus
  row encoding against a null writer (its `parquet_rows` metric is no
  longer zero).

### Removed
- `hints-rebalance` (deprecated in 0.4.0): runtime long-tail splitting
  covers its use case. Also removes its five tuning flags, the example
  script, and docs references.
- The dead `auto_hints.min_segment_size` TOML knob (never read by any
  algorithm) and its config-inspect/plan JSON field.
- The legacy in-memory diff machinery (PrefixMap/ObjectMap and the match
  state machine), ~500 lines net.
- `docs/endpoint-validation-plan.md` moved into `docs/validation-results/`
  as a dated historical document.

## [0.4.0] - 2026-06-12

### Added
- Runtime long-tail segment splitting: when a list run has idle concurrency
  and one segment proves to be a long tail, a delimiter probe over the
  cursor's ancestor directories finds a real CommonPrefix boundary and the
  segment splits cooperatively — the right half becomes a new parallel
  child segment, recursively.  Skewed buckets no longer serialize behind
  their largest prefix.  Splitting never applies to diff,
  `--start-after`, `--continuation-token`, or `bos`-profile runs, and
  ranges without prefix structure keep running unsplit.  Split segments
  conservatively do not record checkpoint progress.

### Changed
- Removed the last per-object allocation from the list ingest hot path by
  moving SDK-owned key Strings into batches instead of copying them.
- Deprecated `hints-rebalance`: runtime splitting covers its use case
  without a manual trace-analysis loop.  The command still works but warns,
  and will be removed in a future release.
- Slimmed INSTALL.md to installation, credentials, and provider setup;
  output/hints/checkpoint details now live in the README and
  `docs/tuning.md` (also fixes stale compression and diff notes).
  Tightened AGENTS.md and removed machine-specific paths.
- Removed the ineffective `release-dispatch.yml` workflow.

### Fixed
- `--dry-run` now validates `--filter` expressions, failing with exit
  code 2 at plan time instead of only at run time.

## [0.3.0] - 2026-06-11

### Added
- Startup structural discovery: flat (`--delimiter ''`) list runs without
  hints now probe the bucket's CommonPrefix structure at startup (a bounded
  BFS of single-page delimiter probes) and list the discovered segments in
  parallel.  First runs no longer require a prior `auto-hints` invocation.
  Discovered boundaries are persisted to the conventional hints cache so
  subsequent runs — including `--resume` — reload identical segments.
  Disabled automatically for `--no-auto-hints`, explicit `--hints-file`,
  `--start-after`, `--continuation-token`, diff mode, and the `bos` profile
  (documented provider-side pagination incompatibility).

### Changed
- Replaced the Rhai-based `--filter` engine with a direct expression
  parser/evaluator.  The accepted grammar, validation errors, and exit-code
  behavior are unchanged; per-object evaluation no longer pays interpreter
  dispatch or per-call scope allocations, and dropping the dependency makes
  the binary smaller and builds faster.
- Removed per-object allocations from the list ingest hot path: resume keys
  are tracked once per page, diff-mode batches are grouped by contiguous
  prefix runs without an intermediate HashMap, and key decoding reuses the
  key's allocation.
- Relaxed atomic memory ordering on informational counters (error/timeout
  tallies, data metrics, HTTP status tracker).
- Hid the developer-only `benchmark-local` subcommand from `--help` output;
  it remains available and unchanged for benchmarking scripts.
- Slimmed README to the product surface; moved the trace schema to
  `docs/trace-reference.md` and segmented-listing/hints details to
  `docs/tuning.md`.  Documented the frozen CLI surface in CONTRIBUTING.

### Fixed
- Resume journals whose segment count no longer matches the current hints
  are discarded with a warning instead of silently skipping the wrong
  segments.

## [0.2.20] - 2026-06-11

### Changed
- Treat `--delimiter ""` as recursive ListObjectsV2 listing by omitting the
  delimiter request parameter, improving compatibility with S3-compatible
  providers that reject `delimiter=`.
- Added dry-run guidance when a list run falls back to a single ListObjectsV2
  chain, making the hints-based high-throughput path easier to identify.
- Documented third-party OSS hints benchmarks and the recommended
  `auto-hints` plus moderate-concurrency list workflow for large buckets.

## [0.2.19] - 2026-06-11

### Changed
- Improved default list-mode Parquet throughput by using larger row groups and
  the faster default `zstd(1)` compression level.
- Reduced list-mode Parquet writer overhead by accumulating rows into
  row-group-sized Arrow batches before writing.
- Avoided disabled S3 trace event construction on the list path unless
  compatibility tracing or S3 debug logging is enabled.

## [0.2.18] - 2026-06-11

### Changed
- Reduced list-mode Parquet ETag rendering overhead by formatting ETags into a
  fixed stack buffer before appending them to Arrow string builders.

## [0.2.17] - 2026-06-07

### Changed
- Reduced list-mode output filtering overhead by using a lightweight list-only
  inclusion check instead of the diff-oriented final status state machine.

## [0.2.16] - 2026-06-06

### Changed
- Reduced list-mode Parquet ingestion overhead by folding prefix/byte
  accounting into the writer selection pass.
- Removed the Parquet writer's per-batch key-length pre-scan, keeping list
  output on a single main batch traversal.

## [0.2.15] - 2026-06-06

### Changed
- Reduced list-mode Parquet output overhead by building Arrow arrays directly
  from incoming batches and avoiding the intermediate filtered row vector.
- Reused an ETag rendering buffer in the Parquet writer, avoiding per-row ETag
  string allocation on the list output hot path.

## [0.2.14] - 2026-06-06

### Changed
- Benchmark wrapper scripts now use the release build script when the default
  release binary is missing, so `BUILD_MODE` workarounds are consistent with
  release builds.
- Benchmark wrappers now fail clearly when an explicit `BIN` points to a
  non-executable path instead of silently building another binary.
- Documented benchmark `BUILD_MODE` and `BIN` usage, and added benchmark smoke
  commands to the release-check recipe.

## [0.2.13] - 2026-06-06

### Changed
- Reduced release-version maintenance by replacing versioned install/build
  examples with template-based asset names.
- Release asset workflow now requires an explicit tag and checks it against
  `Cargo.toml` before building.
- Release preflight checks now flag stale hard-coded release asset versions
  instead of requiring docs and agent notes to repeat the current version.

## [0.2.12] - 2026-06-05

### Changed
- Inlined the remaining diff lifecycle log messages and removed the placeholder
  `diff` library module plus its trivial integration test.
- Replaced the long README roadmap table with a compact current-direction note.

## [0.2.11] - 2026-06-05

### Changed
- Clarified that `diff` is authoritative single-segment by design and removed
  roadmap language that implied future hinted multi-segment diff support.
- Release asset finalization now generates and uploads the linux-aarch64
  single-file checksum expected by the published release verifier.

## [0.2.10] - 2026-06-05

### Added
- Added `benchmark-local --diff-shape mixed|all-equal|all-changed` for local
  diff-output benchmarks, keeping the benchmark-only shape selection separate
  from real S3 diff execution.

## [0.2.9] - 2026-05-31

### Added
- Added `benchmark-local --benchmark diff-output` to measure local diff
  data-map construction plus Parquet/KeySpace output with mixed synthetic
  equal, plus, minus, and changed objects without cloud requests.
- Added large mixed diff output regression coverage for DiffFlag distribution,
  Parquet row counts, and sorted KeySpace counts.

### Changed
- Reduced diff dump-stage allocation by classifying per-prefix objects directly
  from the object map instead of first cloning a full object snapshot.

## [0.2.8] - 2026-05-31

### Added
- Added `benchmark-local --benchmark diff-map` to measure local diff data-map
  construction throughput with synthetic left/right inputs and no cloud
  requests.

### Changed
- Replaced the single-consumer diff data-map's `DashMap` storage with
  `HashMap`-backed maps guarded by local mutexes, reducing per-insert overhead
  without changing diff output semantics.

## [0.2.7] - 2026-05-31

### Added
- Added `benchmark-local --producers` and producer send-wait reporting to help
  measure local data-map channel backpressure without contacting S3 endpoints.

### Changed
- Reduced list output hot-path prefix accounting overhead by borrowing object
  prefixes, avoiding discarded object-name allocations, and using hash-based
  prefix aggregation while preserving sorted KeySpace output.

## [0.2.6] - 2026-05-26

### Added
- Added stable `compat-probe` diagnostic codes and next-step recommendations
  for common S3 service errors, HTTP status classes, transport failures,
  timeouts, invalid responses, and pagination metadata inconsistencies.
- Added reproducibility metadata to `scripts/benchmark-output-formats.sh`
  reports, including git commit, dirty state, platform, Rust host, build
  profile, binary path, compression settings, timestamp, and command template.

### Fixed
- Strengthened stdout row-format regression coverage for empty TSV/NDJSON
  list results so empty buckets do not emit blank rows.

## [0.2.5] - 2026-05-26

### Added
- Added `scripts/benchmark-output-formats.sh` to run repeated local
  `benchmark-local` comparisons across Parquet, TSV, and NDJSON output formats
  and write JSON plus Markdown median summaries.
- Added macOS test coverage back to CI and release source validation after the
  v0.2.4 release workflow split.

### Changed
- `manifest-summary --check` now explicitly reports that Parquet row/schema
  checks are not applicable for stdout row formats while artifact size/hash
  checks still apply when artifacts are recorded.
- Benchmarking documentation now covers the combined output-format benchmark
  script.

### Fixed
- Strengthened TSV/NDJSON stdout regression coverage for row preservation,
  TSV control-character escaping, and NDJSON line parseability.

## [0.2.4] - 2026-05-26

### Added
- Added `scripts/verify-release-assets.sh` to verify a published GitHub
  release's asset set, combined `SHA256SUMS`, and current-platform binary
  locally without contacting S3 endpoints.
- Added a minimal `compat-probe` JSON report example covering success and
  modeled service-error diagnostics.

### Changed
- Split the release asset workflow into a single Linux source-validation job
  and platform-specific release build jobs to avoid repeating the full test
  suite on every asset platform.
- Expanded release documentation with workflow step inspection and dereferenced
  annotated tag verification.
- Documented the v0.2.3 stdout formatter benchmark gains in the benchmarking
  guide.

### Fixed
- Strengthened compat-probe integration coverage for the stable diagnostic
  fields `http_status`, `s3_error_code`, `error_kind`, `request_id`, and
  `request_id_2`.

## [0.2.3] - 2026-05-25

### Added
- Added a documented `compat-probe` JSON report contract, including stable
  guidance for structured diagnostics fields.
- `compat-probe` reports now include `error_kind` and the secondary S3 request
  ID (`request_id_2`) when available from SDK service errors.
- Added a v0.2.3 local stdout formatter benchmark note comparing TSV/NDJSON
  output against the published v0.2.2 binary.

### Changed
- Reduced list TSV/NDJSON stdout hot-path write overhead by rendering each
  received batch into a reusable byte buffer before a single async write.
- Release documentation now reflects the actual tag, linux-aarch64 upload,
  cross-platform asset workflow, and post-release verification process.
- The release environment checker now notes whether the repo pins a Rust
  toolchain and documents that GitHub release workflows use current stable.

## [0.2.2] - 2026-05-25

### Fixed
- `compat-probe` now fails before network setup with provider setup exit code
  `3` when its explicit `--endpoint` still contains template placeholders.
- `compat-probe` reports structured HTTP status, S3 error code, and request ID
  metadata for modeled S3 service errors when the SDK exposes them.
- `compat-probe --output` write failures now return the output-write exit code
  instead of panicking.

## [0.2.1] - 2026-05-25

### Added
- `benchmark-local` can now measure `parquet`, `tsv`, and `ndjson` output
  formats with synthetic local data, including streamed row throughput and
  text output byte metrics for stdout row formats.
- Release environment checks now validate more version metadata, including the
  package version recorded in `Cargo.lock` and documented release asset names.

### Changed
- The local benchmark wrapper accepts `OUTPUT_FORMAT` for repeatable stdout
  formatter benchmark runs without contacting S3.

### Fixed
- Added redaction regression coverage for endpoint alias arguments and dangling
  sensitive flags in agent-facing command diagnostics.
- Added compat-probe overall-status regression coverage after the module split.

## [0.2.0] - 2026-05-25

### Added
- Dry-run plans and run manifests now redact sensitive command argument values
  such as endpoint URLs and continuation tokens while preserving the command
  shape for agent diagnostics.

### Changed
- Moved compat-probe implementation out of `main.rs` into a dedicated library
  module without changing CLI behavior.
- Reduced per-row allocation in list TSV/NDJSON stdout output paths.

## [0.1.29] - 2026-05-25

### Added
- Run manifests now include `config_source`, matching dry-run plans and
  config inspection reports.
- `config_source` now carries warnings, including when an explicit `--config`
  path was not found.

### Changed
- Real cloud-facing commands now fail before network setup with exit code `3`
  for deterministic provider setup errors such as endpoint profiles that
  require an explicit endpoint URL or endpoint URLs that still contain template
  placeholders.
- Config source classification now uses explicit search-path kinds instead of
  relying on a fallback branch for home config files.

## [0.1.28] - 2026-05-24

### Added
- `config-inspect --json` and `--agent --dry-run` reports now include
  `config_source`, showing the explicit config path, loaded config path,
  searched paths, source kind, and CLI config overrides.

### Changed
- Human `config-inspect` output now shows the loaded config file path, or `-`
  when the command is using built-in defaults only.

## [0.1.27] - 2026-05-24

### Changed
- Changed the default Parquet output compression from `gzip(6)` to `zstd(3)`
  based on the v0.1.26 local compression benchmark.
- Updated starter configs and documentation to show the new `zstd(3)` default
  and the explicit `--compression gzip --compression-level 6` fallback for
  traditional gzip output.

## [0.1.26] - 2026-05-24

### Added
- Added `scripts/benchmark-compression.sh` to compare `gzip(6)`, `zstd(3)`,
  `zstd(6)`, `lz4`, and `snappy` with local synthetic data only.
- `benchmark-local` now reports per-object byte counts, local output MiB/sec
  rates, and the retained artifact directory when `--keep-artifacts` is used.

### Changed
- Expanded benchmarking documentation with a repeatable compression matrix while
  keeping the default Parquet compression at `gzip(6)`.

## [0.1.25] - 2026-05-24

### Added
- Added global `--compression` and `--compression-level` flags for one-off
  Parquet output codec selection without editing TOML.
- `benchmark-local` reports the resolved Parquet compression codec and level,
  and `scripts/benchmark-local.sh` accepts `COMPRESSION` and
  `COMPRESSION_LEVEL` environment overrides.

### Changed
- Documented compression selection for local analytics workflows while keeping
  the default Parquet compression at `gzip(6)`.

## [0.1.24] - 2026-05-24

### Changed
- Checkpoint progress now records only segments that finished successfully.
- Final checkpoint saves are skipped when a run has fatal listing errors or
  output-write errors, avoiding completed checkpoints for unreliable artifacts.

### Fixed
- Failed segments can no longer be marked complete simply because their task
  joined.
- Output failures during resume-enabled runs no longer create a checkpoint that
  would cause a later resume to skip the scan.

## [0.1.23] - 2026-05-23

### Changed
- Plain-text hints now allow object keys that contain `=` or bracketed names
  when they do not look like the tool's TOML hints schema.
- Runtime initialization failures now exit with the stable internal-error code
  instead of panicking.

### Fixed
- Fixed false TOML-syntax rejections for common partition-style hints such as
  `dt=2026-05-23/part=0/`.
- Fixed false TOML parsing for plain hints containing bracketed object keys such
  as `[backups]`.
- Unexpected object state in the output path is now logged and ignored instead
  of aborting the process with a Rust panic exit code.
- Removed an obsolete checkpoint helper whose segment-boundary semantics did
  not match the production resume path.

## [0.1.22] - 2026-05-22

### Changed
- S3 client setup now loads the AWS SDK config once in the async startup path
  and reuses it for source/target clients instead of blocking inside
  `S3TaskContext::new`.
- Checkpoint saves now merge existing and newly completed segment indices and
  write them in deterministic sorted order.

### Fixed
- Resume checkpoints no longer drop already completed segment indices when a
  resumed run completes additional segments.
- Added local mock regressions for continuation-token/start-after rejection,
  missing `KeyCount` resume-on-error cursor advancement, and multi-segment
  boundary-key inclusion.

## [0.1.21] - 2026-05-22

### Added
- `manifest-summary --check --json` now includes a stable `check` summary with
  pass/fail counts, artifact counts, and row/schema/exit-code status values.
- `recipes release-check` for local-only pre-release verification commands.
- CI now runs clippy.

### Changed
- Cheatsheet, release documentation, and PR guidance now include the local
  release-check and clippy verification paths.

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
