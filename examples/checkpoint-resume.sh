#!/usr/bin/env bash
# s3-turbo-list — checkpoint / resume example
# ---------------------------------------------------------------------------
# Demonstrates checkpoint save and resume.  The example runs two passes:
#   1. First pass — creates a checkpoint on completion.
#   2. Second pass — resumes from the saved checkpoint.
#
# Checkpoint identity includes: bucket, region, prefix, delimiter, max_keys,
# addressing style, profile, and mode.  Mismatched identity causes the
# checkpoint to be rejected, preventing accidental corruption.
#
# Note: This script intentionally runs to completion.  It does NOT simulate
# an interruption.  For interrupt testing, Ctrl-C the first pass mid-run,
# then run the second pass with --resume.
#
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — AWS region (default: us-east-1)
#   PROFILE       — AWS named profile (default: default)
#   ENDPOINT_URL  — custom S3 endpoint (optional; omit for AWS)
#   OUTDIR        — output directory (default: ./artifacts/checkpoint-resume)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
PROFILE="${PROFILE:-default}"
OUTDIR="${OUTDIR:-./artifacts/checkpoint-resume}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

build_args() {
  set -- \
    list \
    --region "$REGION" \
    --profile "$PROFILE" \
    --bucket "$BUCKET" \
    --output-parquet-file "$OUTDIR/checkpoint.parquet" \
    --output-ks-file "$OUTDIR/checkpoint.ks"
  if [ -n "${ENDPOINT_URL:-}" ]; then
    set -- "$@" --endpoint-url "$ENDPOINT_URL"
  fi
  echo "$@"
}

ARGS=$(build_args)

# ── Pass 1: initial run ────────────────────────────────────
echo "==> Pass 1: initial listing (checkpoint saved on completion)"
echo "    Command: $S3TL $ARGS --resume"
$S3TL $ARGS --resume

echo ""
echo "==> Pass 2: resume from checkpoint"
echo "    Command: $S3TL $ARGS --resume"
$S3TL $ARGS --resume

echo ""
echo "==> Done.  Both passes should produce identical output."
echo "    On the second pass, the tool logs 'Resuming checkpoint: N of M segments completed'."
echo "    Parquet: $OUTDIR/checkpoint.parquet"
echo "    KS:      $OUTDIR/checkpoint.ks"
