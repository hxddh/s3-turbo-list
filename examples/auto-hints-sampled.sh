#!/usr/bin/env bash
# s3-turbo-list - sampled auto-hints example
# ---------------------------------------------------------------------------
# Generates an estimated hints cache from a bounded sample.  This is useful
# when a full auto-hints scan would be too expensive for an initial large-bucket
# run.  The output TOML records scan_mode="sampled" and sampling limits.
#
# Required env vars:
#   BUCKET        - S3 bucket name
# Optional env vars:
#   REGION        - AWS region (default: us-east-1)
#   AWS_PROFILE   - AWS SDK credentials profile (optional)
#   ENDPOINT_URL  - custom S3 endpoint (optional; omit for AWS)
#   SAMPLE_LIMIT  - max objects to scan (default: 1000000)
#   MAX_PAGES     - max ListObjectsV2 pages to scan (default: 1000)
#   OUTDIR        - output directory (default: ./artifacts/auto-hints-sampled)
#   S3_TURBO_LIST_BIN - path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
SAMPLE_LIMIT="${SAMPLE_LIMIT:-1000000}"
MAX_PAGES="${MAX_PAGES:-1000}"
OUTDIR="${OUTDIR:-./artifacts/auto-hints-sampled}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

set -- \
  auto-hints \
  --region "$REGION" \
  --bucket "$BUCKET" \
  --sample-limit "$SAMPLE_LIMIT" \
  --max-pages "$MAX_PAGES" \
  --output "$OUTDIR/hints.sampled.toml"

if [ -n "${ENDPOINT_URL:-}" ]; then
  set -- "$@" --endpoint-url "$ENDPOINT_URL"
fi

echo "==> Generating sampled hints for bucket: $BUCKET"
echo "    sample-limit=$SAMPLE_LIMIT max-pages=$MAX_PAGES"

$S3TL "$@"

echo "==> Validate generated hints:"
echo "    S3_TURBO_LIST_BIN=\"${S3_TURBO_LIST_BIN:-}\" HINTS_FILE=\"$OUTDIR/hints.sampled.toml\" ./examples/hints-validate.sh"
