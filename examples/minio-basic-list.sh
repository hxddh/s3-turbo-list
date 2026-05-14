#!/usr/bin/env bash
# s3-turbo-list — MinIO basic list example
# ---------------------------------------------------------------------------
# Required env vars:
#   BUCKET        — MinIO bucket name
# Optional env vars:
#   ENDPOINT_URL  — MinIO endpoint (default: http://localhost:9000)
#   REGION        — region (default: us-east-1)
#   PROFILE       — profile name (default: minio)
#   OUTDIR        — output directory (default: ./artifacts/minio-basic)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the MinIO bucket name}"
ENDPOINT_URL="${ENDPOINT_URL:-http://localhost:9000}"
REGION="${REGION:-us-east-1}"
PROFILE="${PROFILE:-minio}"
OUTDIR="${OUTDIR:-./artifacts/minio-basic}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

echo "==> Listing MinIO bucket: $BUCKET (endpoint=$ENDPOINT_URL)"
echo "    Output: $OUTDIR/minio-basic.parquet"
echo "            $OUTDIR/minio-basic.ks"

$S3TL list \
  --endpoint-url "$ENDPOINT_URL" \
  --region "$REGION" \
  --profile "$PROFILE" \
  --bucket "$BUCKET" \
  --addressing-style path \
  --output-parquet-file "$OUTDIR/minio-basic.parquet" \
  --output-ks-file "$OUTDIR/minio-basic.ks"

echo "==> Done.  Inspect with:"
echo "    python examples/read-parquet.py $OUTDIR/minio-basic.parquet"
echo "    cat $OUTDIR/minio-basic.ks"
