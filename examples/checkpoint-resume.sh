#!/usr/bin/env bash
# s3-turbo-list — checkpoint / resume example
# ---------------------------------------------------------------------------
# Every interrupted list run (Ctrl-C or SIGTERM, exit 7) saves a checkpoint
# in --output-dir; --resume reads it and lists only the key ranges the
# interrupted run had not written.
#
# This script runs pass 1 under `timeout` so it is interrupted after
# INTERRUPT_AFTER seconds (SIGTERM is handled like Ctrl-C), then resumes.
# If pass 1 finishes first, it removes its checkpoint and pass 2 lists the
# whole bucket again.
#
# The checkpoint identity covers bucket, region, endpoint, prefix, delimiter,
# max-keys, addressing style, provider, mode and filter: a checkpoint written
# under another identity is ignored with a warning.
#
# Pass 2's output covers only the remaining ranges — combine it with pass 1's
# output.  Auto-named outputs get fresh names, so neither overwrites the other.
#
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — region (default: us-east-1)
#   AWS_PROFILE   — AWS SDK credentials profile (optional)
#   ENDPOINT_URL  — custom S3 endpoint (optional; omit for AWS)
#   INTERRUPT_AFTER — seconds before pass 1 is interrupted (default: 10)
#   OUTDIR        — output directory (default: ./artifacts/checkpoint-resume)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
INTERRUPT_AFTER="${INTERRUPT_AFTER:-10}"
OUTDIR="${OUTDIR:-./artifacts/checkpoint-resume}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

set -- \
  list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --output-dir "$OUTDIR" \
  --run-manifest "$OUTDIR/run.json"
if [ -n "${ENDPOINT_URL:-}" ]; then
  set -- "$@" --endpoint-url "$ENDPOINT_URL"
fi

echo "==> Pass 1: listing, interrupted after ${INTERRUPT_AFTER}s"
echo "    Command: timeout $INTERRUPT_AFTER $S3TL $*"
status=0
timeout "$INTERRUPT_AFTER" $S3TL "$@" || status=$?
echo "    Pass 1 exit code: $status (7 = interrupted, checkpoint saved)"

echo ""
echo "==> Pass 2: resume from the checkpoint in $OUTDIR"
echo "    Command: $S3TL $* --resume"
$S3TL "$@" --resume

echo ""
echo "==> Done.  Outputs in $OUTDIR:"
ls -1 "$OUTDIR"
echo "    run.json's checkpoint.resumed_segments_skipped > 0 means pass 2 covers"
echo "    only the ranges pass 1 had not written: read both Parquet files."
