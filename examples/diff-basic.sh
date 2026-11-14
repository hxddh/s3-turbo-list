#!/usr/bin/env bash
# s3tl — compare two prefixes and write the differing keys to Parquet.
#
# Keys are compared relative to each side's prefix; the output holds only
# keys that differ (status left_only / right_only / changed, with both
# sides' size, last_modified and etag). The result's .diff has the counts,
# including equal keys.
#
# Env: LEFT, RIGHT (required, s3://bucket[/prefix]), ENDPOINT_URL,
# TARGET_ENDPOINT_URL (right side, for cross-provider diffs), OUTDIR
# (default ./artifacts/diff-basic), S3TL (default: cargo run --release --).
set -euo pipefail

: "${LEFT:?set LEFT to s3://bucket[/prefix]}"
: "${RIGHT:?set RIGHT to s3://bucket[/prefix]}"
OUTDIR="${OUTDIR:-./artifacts/diff-basic}"
S3TL="${S3TL:-cargo run --release --}"
set -- diff "$LEFT" "$RIGHT" -o "$OUTDIR"
if [ -n "${ENDPOINT_URL:-}" ]; then set -- "$@" --endpoint-url "$ENDPOINT_URL"; fi
if [ -n "${TARGET_ENDPOINT_URL:-}" ]; then set -- "$@" --target-endpoint-url "$TARGET_ENDPOINT_URL"; fi

$S3TL "$@" | jq '{status, diff, files: [.output.files[].path]}'
