#!/usr/bin/env bash
# s3-turbo-list — TOML hints file example
# ---------------------------------------------------------------------------
# TOML hints split a bucket into segments for parallel listing.  The parser
# detects TOML structure automatically (boundaries = [...], table headers,
# key=value assignments) and deserialises via toml::from_str.
#
# Plain text hints (one boundary per line) are also supported.  Malformed
# TOML-looking hints are rejected before any S3 request is sent.
#
# Valid key characters — spaces, +, /, %, Unicode — are preserved.
#
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — AWS region (default: us-east-1)
#   PROFILE       — AWS named profile (default: default)
#   ENDPOINT_URL  — custom S3 endpoint (optional; omit for AWS)
#   OUTDIR        — output directory (default: ./artifacts/hints-file-toml)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
PROFILE="${PROFILE:-default}"
OUTDIR="${OUTDIR:-./artifacts/hints-file-toml}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

# Create a TOML hints file with sample boundaries.
# Replace these with actual key-space boundaries from your bucket.
cat > "$OUTDIR/hints.toml" << 'TOML_EOF'
bucket = "example-bucket"
region = "us-east-1"
total_objects = 50000
boundaries = [
    "alpha/",
    "beta/",
    "logs/file with spaces.log",
    "logs/file+plus.log",
    "中文/",
]
generated_at = "2026-05-14T12:00:00Z"
TOML_EOF

echo "==> Created hints file: $OUTDIR/hints.toml"
echo "    Boundaries:"
grep -A999 'boundaries = \[' "$OUTDIR/hints.toml" | grep '"' || true

echo ""
echo "==> Listing with TOML hints file"
echo "    Bucket:      $BUCKET"
echo "    Output:      $OUTDIR/hints-output.parquet"
echo "                $OUTDIR/hints-output.ks"

set -- \
  list \
  --region "$REGION" \
  --profile "$PROFILE" \
  --bucket "$BUCKET" \
  --hints-file "$OUTDIR/hints.toml" \
  --output-parquet-file "$OUTDIR/hints-output.parquet" \
  --output-ks-file "$OUTDIR/hints-output.ks"

if [ -n "${ENDPOINT_URL:-}" ]; then
  set -- "$@" --endpoint-url "$ENDPOINT_URL"
fi

$S3TL "$@"

echo "==> Done.  Inspect with:"
echo "    python examples/read-parquet.py $OUTDIR/hints-output.parquet"
echo "    cat $OUTDIR/hints-output.ks"
