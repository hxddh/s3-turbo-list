#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${BIN+x}" ]]; then
  BIN_WAS_SET=1
else
  BIN_WAS_SET=0
  BIN="$ROOT/target/release/examples/bench_local"
fi
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
  if [[ "$BIN_WAS_SET" == "1" ]]; then
    echo "ERROR: BIN is set but is not executable: $BIN" >&2
    exit 1
  fi
  # The benchmark is the bench_local example (it left the shipped binary in
  # 0.37); BUILD_MODE picks the same compiler workarounds as build-release.sh.
  case "${BUILD_MODE:-default}" in
    clang) (cd "$ROOT" && CC=clang CXX=clang++ cargo build --release --example bench_local) >/dev/null ;;
    gcc10) (cd "$ROOT" && CC=gcc-10 CXX=g++-10 cargo build --release --example bench_local) >/dev/null ;;
    no-asm) (cd "$ROOT" && AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1 cargo build --release --example bench_local) >/dev/null ;;
    *) (cd "$ROOT" && cargo build --release --example bench_local) >/dev/null ;;
  esac
  BIN="$ROOT/target/release/examples/bench_local"
fi

args=(
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
