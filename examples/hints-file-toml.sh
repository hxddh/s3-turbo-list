#!/usr/bin/env bash
# s3-turbo-list — TOML hints file example
# ---------------------------------------------------------------------------
# Hints are optional: startup discovery and runtime splitting partition a
# bucket automatically on every run.  A hints file pins exact segment
# boundaries for repeated inventories (list only; diff does not take one).
#
# The parser detects TOML structure automatically (boundaries = [...], table
# headers, key=value assignments); plain text (one boundary per line) is also
# accepted.  Malformed TOML-looking hints are rejected before any S3 request.
# Valid key characters — spaces, +, /, %, Unicode — are preserved.
#
# The file is validated locally first with `doctor --hints-file` (no S3).
#
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — region (default: us-east-1)
#   AWS_PROFILE   — AWS SDK credentials profile (optional)
#   ENDPOINT_URL  — custom S3 endpoint (optional; omit for AWS)
#   OUTDIR        — output directory (default: ./artifacts/hints-file-toml)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
OUTDIR="${OUTDIR:-./artifacts/hints-file-toml}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

# Create a TOML hints file with sample boundaries.
# Replace these with actual key-space boundaries from your bucket.
cat > "$OUTDIR/hints.toml" << 'TOML_EOF'
bucket = "example-bucket"
region = "us-east-1"
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
echo "==> Validating it locally (no S3 access)"
$S3TL doctor --hints-file "$OUTDIR/hints.toml"

echo ""
echo "==> Listing with the TOML hints file"
echo "    Bucket:  $BUCKET"
echo "    Output:  $OUTDIR/hints-output.parquet (+ hints-output.ks)"

set -- \
  list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --hints-file "$OUTDIR/hints.toml" \
  --output-parquet-file "$OUTDIR/hints-output.parquet"

if [ -n "${ENDPOINT_URL:-}" ]; then
  set -- "$@" --endpoint-url "$ENDPOINT_URL"
fi

$S3TL "$@"

echo "==> Done.  Inspect with:"
echo "    python3 examples/read-parquet.py $OUTDIR/hints-output.parquet"
echo "    cat $OUTDIR/hints-output.ks"
