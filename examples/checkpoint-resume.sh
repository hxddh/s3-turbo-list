#!/usr/bin/env bash
# s3tl — interrupt a listing, then resume it into the same dataset.
#
# Pass 1 runs under `timeout` (SIGTERM is handled like Ctrl-C): it commits
# the parts written so far, saves OUTDIR/_checkpoint.json and exits 130.
# Pass 2 (--resume) lists only the remaining key ranges and adds its parts
# to OUTDIR, which then holds every key exactly once.
#
# Env: S3_URL (required, s3://bucket[/prefix]), ENDPOINT_URL, INTERRUPT_AFTER
# (seconds, default 10), OUTDIR (default ./artifacts/checkpoint-resume),
# S3TL (binary, default: cargo run --release --).
set -euo pipefail

: "${S3_URL:?set S3_URL to s3://bucket[/prefix]}"
OUTDIR="${OUTDIR:-./artifacts/checkpoint-resume}"
S3TL="${S3TL:-cargo run --release --}"
set -- list "$S3_URL" -o "$OUTDIR"
if [ -n "${ENDPOINT_URL:-}" ]; then set -- "$@" --endpoint-url "$ENDPOINT_URL"; fi

status=0
timeout "${INTERRUPT_AFTER:-10}" $S3TL "$@" > pass1.json || status=$?
echo "pass 1: exit $status ($(jq -r '.status' pass1.json))"
if [ "$status" -eq 130 ]; then
  $S3TL "$@" --resume | jq '{status, rows: .output.rows, files: [.output.files[].rows]}'
fi
