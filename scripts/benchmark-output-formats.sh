#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${BIN+x}" ]]; then
  BIN_WAS_SET=1
else
  BIN_WAS_SET=0
  BIN="$ROOT/target/release/s3-turbo-list"
fi
OBJECTS="${OBJECTS:-100000}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
PREFIXES="${PREFIXES:-512}"
PRODUCERS="${PRODUCERS:-1}"
RUNS="${RUNS:-3}"
OUT="${OUT:-$ROOT/benchmark-results-output-formats.json}"
MARKDOWN="${MARKDOWN:-${OUT%.json}.md}"
FORMATS="${FORMATS:-parquet tsv ndjson}"
COMPRESSION="${COMPRESSION:-zstd}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-3}"
KEEP_ARTIFACTS="${KEEP_ARTIFACTS:-false}"
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
GIT_COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)"
GIT_DIRTY="false"
if ! git -C "$ROOT" diff --quiet --ignore-submodules -- 2>/dev/null || \
  ! git -C "$ROOT" diff --cached --quiet --ignore-submodules -- 2>/dev/null; then
  GIT_DIRTY="true"
fi
OS_NAME="$(uname -s)"
ARCH_NAME="$(uname -m)"
RUSTC_HOST=""
if command -v rustc >/dev/null 2>&1; then
  RUSTC_HOST="$(rustc -vV 2>/dev/null | awk '/^host:/ { print $2 }')"
fi
BUILD_PROFILE="${BUILD_PROFILE:-release}"

if [[ ! -x "$BIN" ]]; then
  if [[ "$BIN_WAS_SET" == "1" ]]; then
    echo "ERROR: BIN is set but is not executable: $BIN" >&2
    exit 1
  fi
  BUILD_MODE="${BUILD_MODE:-default}" "$ROOT/scripts/build-release.sh" >/dev/null
  BIN="$ROOT/target/release/s3-turbo-list"
fi

TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/s3-turbo-list-output-formats.XXXXXX")"
trap 'rm -rf "$TMPDIR"' EXIT

run_case() {
  local format="$1"
  local run="$2"
  local output="$TMPDIR/${format}-${run}.json"
  local args=(
    --compression "$COMPRESSION"
    --compression-level "$COMPRESSION_LEVEL"
    benchmark-local
    --objects "$OBJECTS"
    --batch-size "$BATCH_SIZE"
    --prefixes "$PREFIXES"
    --producers "$PRODUCERS"
    --output-format "$format"
    --output "$output"
    --json
  )
  if [[ "$KEEP_ARTIFACTS" == "true" ]]; then
    args+=("--keep-artifacts")
  fi
  "$BIN" "${args[@]}" >/dev/null
}

for format in $FORMATS; do
  for run in $(seq 1 "$RUNS"); do
    run_case "$format" "$run"
  done
done

python3 - "$TMPDIR" "$OUT" "$MARKDOWN" "$FORMATS" "$BIN" "$RUN_STARTED_AT" \
  "$GIT_COMMIT" "$GIT_DIRTY" "$OS_NAME" "$ARCH_NAME" "$RUSTC_HOST" \
  "$BUILD_PROFILE" "$COMPRESSION" "$COMPRESSION_LEVEL" "$KEEP_ARTIFACTS" \
  "$PRODUCERS" <<'PY'
import json
import pathlib
import statistics
import sys

tmpdir = pathlib.Path(sys.argv[1])
out_path = pathlib.Path(sys.argv[2])
md_path = pathlib.Path(sys.argv[3])
formats = sys.argv[4].split()
bin_path = pathlib.Path(sys.argv[5])
started_at = sys.argv[6]
git_commit = sys.argv[7] or None
git_dirty = sys.argv[8] == "true"
os_name = sys.argv[9]
arch_name = sys.argv[10]
rustc_host = sys.argv[11] or None
build_profile = sys.argv[12]
compression = sys.argv[13]
compression_level = int(sys.argv[14])
keep_artifacts = sys.argv[15] == "true"
producers = int(sys.argv[16])

def median(values):
    return statistics.median(values) if values else 0

format_results = []
for fmt in formats:
    runs = []
    for path in sorted(tmpdir.glob(f"{fmt}-*.json")):
        runs.append(json.loads(path.read_text()))
    if not runs:
        continue
    format_results.append({
        "output_format": fmt,
        "runs": runs,
        "median_elapsed_secs": median([item["elapsed_secs"] for item in runs]),
        "median_objects_per_sec": median([item["objects_per_sec"] for item in runs]),
        "median_output_mib_per_sec": median([item["output_mib_per_sec"] for item in runs]),
        "median_output_bytes_per_object": median([item["output_bytes_per_object"] for item in runs]),
    })

first = format_results[0]["runs"][0] if format_results else {}
command_template = (
    "{bin} --compression {compression} --compression-level {level} "
    "benchmark-local --objects {objects} --batch-size {batch_size} "
    "--prefixes {prefixes} --producers {producers} "
    "--output-format <format> --output <tmp-json> --json"
).format(
    bin=str(bin_path),
    compression=compression,
    level=compression_level,
    objects=first.get("objects", 0),
    batch_size=first.get("batch_size", 0),
    prefixes=first.get("prefixes", 0),
    producers=producers,
)
if keep_artifacts:
    command_template += " --keep-artifacts"
summary = {
    "schema_version": "s3-turbo-list.output-format-benchmark.v1",
    "tool_version": first.get("tool_version"),
    "network": "none: synthetic local data only",
    "started_at": started_at,
    "git_commit": git_commit,
    "git_dirty": git_dirty,
    "os": os_name,
    "arch": arch_name,
    "rustc_host": rustc_host,
    "build_profile": build_profile,
    "binary": str(bin_path),
    "compression": compression,
    "compression_level": compression_level,
    "keep_artifacts": keep_artifacts,
    "command_template": command_template,
    "objects": first.get("objects", 0),
    "batch_size": first.get("batch_size", 0),
    "prefixes": first.get("prefixes", 0),
    "producers": first.get("producers", producers),
    "runs_per_format": len(format_results[0]["runs"]) if format_results else 0,
    "results": format_results,
}

out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(summary, indent=2) + "\n")

lines = [
    "# Output Format Benchmark",
    "",
    f"- Tool version: `{summary['tool_version']}`",
    f"- Network: `{summary['network']}`",
    f"- Started at: `{summary['started_at']}`",
    f"- Git commit: `{summary['git_commit']}`",
    f"- Git dirty: `{str(summary['git_dirty']).lower()}`",
    f"- Platform: `{summary['os']} {summary['arch']}`",
    f"- Rust host: `{summary['rustc_host']}`",
    f"- Build profile: `{summary['build_profile']}`",
    f"- Binary: `{summary['binary']}`",
    f"- Compression: `{summary['compression']}:{summary['compression_level']}`",
    f"- Objects: `{summary['objects']}`",
    f"- Batch size: `{summary['batch_size']}`",
    f"- Prefixes: `{summary['prefixes']}`",
    f"- Producers: `{summary['producers']}`",
    f"- Runs per format: `{summary['runs_per_format']}`",
    "",
    "| Format | Median seconds | Median objects/sec | Median output MiB/sec | Median bytes/object |",
    "|---|---:|---:|---:|---:|",
]
for item in format_results:
    lines.append(
        "| {fmt} | {elapsed:.4f} | {ops:.0f} | {mib:.2f} | {bpo:.2f} |".format(
            fmt=item["output_format"],
            elapsed=item["median_elapsed_secs"],
            ops=item["median_objects_per_sec"],
            mib=item["median_output_mib_per_sec"],
            bpo=item["median_output_bytes_per_object"],
        )
    )

md_path.parent.mkdir(parents=True, exist_ok=True)
md_path.write_text("\n".join(lines) + "\n")
PY

echo "wrote $OUT"
echo "wrote $MARKDOWN"
