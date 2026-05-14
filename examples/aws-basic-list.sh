#!/usr/bin/env bash
# s3-turbo-list — AWS S3 basic list example
# ---------------------------------------------------------------------------
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — AWS region (default: us-east-1)
#   PROFILE       — AWS named profile (default: default)
#   OUTDIR        — output directory (default: ./artifacts/aws-basic)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
PROFILE="${PROFILE:-default}"
OUTDIR="${OUTDIR:-./artifacts/aws-basic}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

echo "==> Listing AWS S3 bucket: $BUCKET (region=$REGION, profile=$PROFILE)"
echo "    Output: $OUTDIR/aws-basic.parquet"
echo "            $OUTDIR/aws-basic.ks"

$S3TL list \
  --region "$REGION" \
  --profile "$PROFILE" \
  --bucket "$BUCKET" \
  --output-parquet-file "$OUTDIR/aws-basic.parquet" \
  --output-ks-file "$OUTDIR/aws-basic.ks"

echo "==> Done.  Inspect with:"
echo "    python examples/read-parquet.py $OUTDIR/aws-basic.parquet"
echo "    cat $OUTDIR/aws-basic.ks"
