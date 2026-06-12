#!/usr/bin/env bash
# s3-turbo-list — BOS path-style diagnostic example
# ---------------------------------------------------------------------------
# ⚠️  THIS IS NOT THE RECOMMENDED BOS DEFAULT.
# Use only for diagnostics, legacy compatibility testing, or controlled
# validation.  For normal BOS usage, prefer bos-basic-list.sh (virtual-hosted).
#
# Required env vars:
#   BUCKET        — BOS bucket name
# Optional env vars:
#   ENDPOINT_URL  — BOS endpoint (default: https://s3.bj.bcebos.com)
#   REGION        — BOS region (default: bj)
#   ENDPOINT_PROFILE — endpoint compatibility profile name (default: bos)
#   OUTDIR        — output directory (default: ./artifacts/bos-path-style-diagnostic)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the BOS bucket name}"
ENDPOINT_URL="${ENDPOINT_URL:-https://s3.bj.bcebos.com}"
REGION="${REGION:-bj}"
ENDPOINT_PROFILE="${ENDPOINT_PROFILE:-bos}"
OUTDIR="${OUTDIR:-./artifacts/bos-path-style-diagnostic}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

echo "==> [DIAGNOSTIC] Listing BOS bucket with path-style addressing"
echo "    Bucket:      $BUCKET"
echo "    Endpoint:    $ENDPOINT_URL"
echo "    Region:      $REGION"
echo "    ⚠️  Path-style is LEGACY/DIAGNOSTIC only — not the recommended default."
echo "    Output: $OUTDIR/bos-path-style.parquet"
echo "            $OUTDIR/bos-path-style.ks"

$S3TL list \
  --endpoint-url "$ENDPOINT_URL" \
  --region "$REGION" \
  --profile "$ENDPOINT_PROFILE" \
  --bucket "$BUCKET" \
  --addressing-style path \
  --output-parquet-file "$OUTDIR/bos-path-style.parquet" \
  --output-ks-file "$OUTDIR/bos-path-style.ks"

echo "==> Done.  Inspect with:"
echo "    python examples/read-parquet.py $OUTDIR/bos-path-style.parquet"
echo "    cat $OUTDIR/bos-path-style.ks"
