#!/usr/bin/env bash
# s3-turbo-list — BOS (Baidu Object Storage) basic list example
# ---------------------------------------------------------------------------
# BOS recommends virtual-hosted / bucket virtual hosting style.
# Path-style is legacy/diagnostic only — see bos-path-style-diagnostic.sh.
#
# Avoid hinted multi-segment authoritative scans on BOS until BOS fixes the
# ListObjectsV2 start_after + continuation_token compatibility issue.
# Use single-segment fallback (no hints file, or empty hints).
#
# Required env vars:
#   BUCKET        — BOS bucket name
# Optional env vars:
#   ENDPOINT_URL  — BOS endpoint (default: https://s3.bj.bcebos.com)
#   REGION        — BOS region (default: bj)
#   PROFILE       — profile name (default: bos)
#   OUTDIR        — output directory (default: ./artifacts/bos-basic)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the BOS bucket name}"
ENDPOINT_URL="${ENDPOINT_URL:-https://s3.bj.bcebos.com}"
REGION="${REGION:-bj}"
PROFILE="${PROFILE:-bos}"
OUTDIR="${OUTDIR:-./artifacts/bos-basic}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

echo "==> Listing BOS bucket: $BUCKET (endpoint=$ENDPOINT_URL, region=$REGION)"
echo "    Addressing style: virtual-hosted (BOS recommended default)"
echo "    Output: $OUTDIR/bos-basic.parquet"
echo "            $OUTDIR/bos-basic.ks"

$S3TL list \
  --endpoint-url "$ENDPOINT_URL" \
  --region "$REGION" \
  --profile "$PROFILE" \
  --bucket "$BUCKET" \
  --addressing-style virtual \
  --output-parquet-file "$OUTDIR/bos-basic.parquet" \
  --output-ks-file "$OUTDIR/bos-basic.ks"

echo "==> Done.  Inspect with:"
echo "    python examples/read-parquet.py $OUTDIR/bos-basic.parquet"
echo "    cat $OUTDIR/bos-basic.ks"
