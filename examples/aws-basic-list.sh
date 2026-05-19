#!/usr/bin/env bash
# s3-turbo-list — AWS S3 basic list example
# ---------------------------------------------------------------------------
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — AWS region (default: us-east-1)
#   AWS_PROFILE   — AWS SDK credentials profile (optional)
#   OUTDIR        — output directory (default: ./artifacts/aws-basic)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
OUTDIR="${OUTDIR:-./artifacts/aws-basic}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

echo "==> Listing AWS S3 bucket: $BUCKET (region=$REGION, AWS_PROFILE=${AWS_PROFILE:-default})"
echo "    Output: $OUTDIR/aws-basic.parquet"
echo "            $OUTDIR/aws-basic.ks"

$S3TL list \
  --delimiter '' \
  --region "$REGION" \
  --bucket "$BUCKET" \
  --output-parquet-file "$OUTDIR/aws-basic.parquet" \
  --output-ks-file "$OUTDIR/aws-basic.ks"

echo "==> Done.  Inspect with:"
echo "    python examples/read-parquet.py $OUTDIR/aws-basic.parquet"
echo "    cat $OUTDIR/aws-basic.ks"
