#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT/target/release/s3-turbo-list}"
OBJECTS="${OBJECTS:-100000}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
PREFIXES="${PREFIXES:-512}"
PRODUCERS="${PRODUCERS:-1}"
BENCHMARK="${BENCHMARK:-list-output}"
DIFF_SHAPE="${DIFF_SHAPE:-mixed}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-parquet}"
COMPRESSION="${COMPRESSION:-}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-}"
OUT="${OUT:-$ROOT/benchmark-results-local.json}"

if [[ ! -x "$BIN" ]]; then
  cargo build --release --manifest-path "$ROOT/Cargo.toml"
fi

args=(
  benchmark-local
  --benchmark "$BENCHMARK"
  --objects "$OBJECTS"
  --batch-size "$BATCH_SIZE"
  --prefixes "$PREFIXES"
  --producers "$PRODUCERS"
  --diff-shape "$DIFF_SHAPE"
  --output-format "$OUTPUT_FORMAT"
  --output "$OUT"
  --json
)

if [[ -n "$COMPRESSION" ]]; then
  args=(--compression "$COMPRESSION" "${args[@]}")
fi

if [[ -n "$COMPRESSION_LEVEL" ]]; then
  args=(--compression-level "$COMPRESSION_LEVEL" "${args[@]}")
fi

"$BIN" "${args[@]}"

echo "wrote $OUT"
