#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT/target/release/s3-turbo-list}"
OBJECTS="${OBJECTS:-100000}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
PREFIXES="${PREFIXES:-512}"
OUT="${OUT:-$ROOT/benchmark-results-local.json}"

if [[ ! -x "$BIN" ]]; then
  cargo build --release --manifest-path "$ROOT/Cargo.toml"
fi

"$BIN" benchmark-local \
  --objects "$OBJECTS" \
  --batch-size "$BATCH_SIZE" \
  --prefixes "$PREFIXES" \
  --output "$OUT" \
  --json

echo "wrote $OUT"
