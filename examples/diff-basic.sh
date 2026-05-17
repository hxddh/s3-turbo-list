#!/usr/bin/env bash
# s3-turbo-list — bi-directional diff example
# ---------------------------------------------------------------------------
# Required env vars:
#   LEFT_BUCKET   — source (left) bucket name
#   RIGHT_BUCKET  — target (right) bucket name
# Optional env vars:
#   REGION        — left region (default: us-east-1)
#   TARGET_REGION — right region (default: same as REGION)
#   AWS_PROFILE   — AWS SDK credentials profile (optional)
#   ENDPOINT_URL  — custom S3 endpoint (optional; omit for AWS)
#   OUTDIR        — output directory (default: ./artifacts/diff-basic)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
#
# Parquet DiffFlag legend:
#   0 — Equal     (same object in both buckets)
#   1 — Left-only (only in LEFT_BUCKET)
#   2 — Right-only (only in RIGHT_BUCKET)
#   3 — Asterisk  (both buckets, but size/ETag differ)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${LEFT_BUCKET:?set LEFT_BUCKET to the source (left) bucket name}"
: "${RIGHT_BUCKET:?set RIGHT_BUCKET to the target (right) bucket name}"
REGION="${REGION:-us-east-1}"
TARGET_REGION="${TARGET_REGION:-$REGION}"
OUTDIR="${OUTDIR:-./artifacts/diff-basic}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

echo "==> Diffing buckets"
echo "    Left:  $LEFT_BUCKET  (region=$REGION)"
echo "    Right: $RIGHT_BUCKET (region=$TARGET_REGION)"
echo "    Output: $OUTDIR/diff.parquet"

set -- \
  diff \
  --region "$REGION" \
  --bucket "$LEFT_BUCKET" \
  --target-region "$TARGET_REGION" \
  --target-bucket "$RIGHT_BUCKET" \
  --output-parquet-file "$OUTDIR/diff.parquet"

if [ -n "${ENDPOINT_URL:-}" ]; then
  set -- "$@" --endpoint-url "$ENDPOINT_URL"
fi

$S3TL "$@"

echo "==> Done.  Inspect with:"
echo "    python examples/read-parquet.py $OUTDIR/diff.parquet"
echo ""
echo "    # Count by DiffFlag:"
echo "    python -c \""
echo "import pandas as pd"
echo "df = pd.read_parquet('$OUTDIR/diff.parquet')"
echo "print(df['DiffFlag'].value_counts().sort_index())"
echo "\""
