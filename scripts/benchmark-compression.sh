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
OUT="${OUT:-$ROOT/benchmark-results-compression.json}"
MARKDOWN="${MARKDOWN:-${OUT%.json}.md}"
KEEP_ARTIFACTS="${KEEP_ARTIFACTS:-false}"

if [[ ! -x "$BIN" ]]; then
  if [[ "$BIN_WAS_SET" == "1" ]]; then
    echo "ERROR: BIN is set but is not executable: $BIN" >&2
    exit 1
  fi
  BUILD_MODE="${BUILD_MODE:-default}" "$ROOT/scripts/build-release.sh" >/dev/null
  BIN="$ROOT/target/release/s3-turbo-list"
fi

TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/s3-turbo-list-compression.XXXXXX")"
trap 'rm -rf "$TMPDIR"' EXIT

run_case() {
  local name="$1"
  local codec="$2"
  local level="$3"
  local output="$TMPDIR/${name}.json"
  local args=(
    --compression "$codec"
    benchmark-local
    --objects "$OBJECTS"
    --batch-size "$BATCH_SIZE"
    --prefixes "$PREFIXES"
    --output "$output"
    --json
  )

  if [[ -n "$level" ]]; then
    args=(--compression-level "$level" "${args[@]}")
  fi
  if [[ "$KEEP_ARTIFACTS" == "true" ]]; then
    args+=("--keep-artifacts")
  fi

  "$BIN" "${args[@]}" >/dev/null
}

run_case gzip-6 gzip 6
run_case zstd-3 zstd 3
run_case zstd-6 zstd 6
run_case lz4 lz4 ""
run_case snappy snappy ""

python3 - "$TMPDIR" "$OUT" "$MARKDOWN" <<'PY'
import json
import pathlib
import sys

tmpdir = pathlib.Path(sys.argv[1])
out_path = pathlib.Path(sys.argv[2])
md_path = pathlib.Path(sys.argv[3])
order = ["gzip-6", "zstd-3", "zstd-6", "lz4", "snappy"]

results = []
for name in order:
    data = json.loads((tmpdir / f"{name}.json").read_text())
    results.append(data)

summary = {
    "schema_version": "s3-turbo-list.compression-benchmark.v1",
    "tool_version": results[0]["tool_version"] if results else None,
    "network": "none: synthetic local data only",
    "objects": results[0]["objects"] if results else 0,
    "batch_size": results[0]["batch_size"] if results else 0,
    "prefixes": results[0]["prefixes"] if results else 0,
    "results": results,
}

out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(summary, indent=2) + "\n")

lines = [
    "# Compression Benchmark",
    "",
    f"- Tool version: `{summary['tool_version']}`",
    f"- Network: `{summary['network']}`",
    f"- Objects: `{summary['objects']}`",
    f"- Batch size: `{summary['batch_size']}`",
    f"- Prefixes: `{summary['prefixes']}`",
    "",
    "| Codec | Level | Seconds | Objects/sec | Parquet bytes | Bytes/object | Parquet MiB/sec |",
    "|---|---:|---:|---:|---:|---:|---:|",
]
for item in results:
    lines.append(
        "| {codec} | {level} | {elapsed:.4f} | {ops:.0f} | {bytes} | {bpo:.2f} | {mib:.2f} |".format(
            codec=item["compression"],
            level=item["compression_level"],
            elapsed=item["elapsed_secs"],
            ops=item["objects_per_sec"],
            bytes=item["parquet_bytes"],
            bpo=item["parquet_bytes_per_object"],
            mib=item["parquet_mib_per_sec"],
        )
    )
md_path.parent.mkdir(parents=True, exist_ok=True)
md_path.write_text("\n".join(lines) + "\n")
PY

echo "wrote $OUT"
echo "wrote $MARKDOWN"
