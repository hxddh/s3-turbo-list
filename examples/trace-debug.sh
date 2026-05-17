#!/usr/bin/env bash
# s3-turbo-list — trace + debug output example
# ---------------------------------------------------------------------------
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — AWS region (default: us-east-1)
#   AWS_PROFILE   — AWS SDK credentials profile (optional)
#   ENDPOINT_URL  — custom S3 endpoint (optional; omit for AWS)
#   OUTDIR        — output directory (default: ./artifacts/trace-debug)
#   S3_TURBO_LIST_BIN — path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${BUCKET:?set BUCKET to the S3 bucket name}"
REGION="${REGION:-us-east-1}"
OUTDIR="${OUTDIR:-./artifacts/trace-debug}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

mkdir -p "$OUTDIR"

echo "==> Listing with trace + debug output"
echo "    Bucket:      $BUCKET"
echo "    Trace:       $OUTDIR/trace.jsonl"
echo "    Debug stderr: $OUTDIR/debug-stderr.log"
echo "    Parquet:     $OUTDIR/trace-output.parquet"

# Build CLI args; conditionally include --endpoint-url.
set -- \
  list \
  --region "$REGION" \
  --bucket "$BUCKET" \
  --debug-s3 \
  --trace-compat "$OUTDIR/trace.jsonl" \
  --output-parquet-file "$OUTDIR/trace-output.parquet"

if [ -n "${ENDPOINT_URL:-}" ]; then
  set -- "$@" --endpoint-url "$ENDPOINT_URL"
fi

# Run and redirect stderr (debug output) to a log file.
$S3TL "$@" 2>"$OUTDIR/debug-stderr.log"

echo "==> Done.  Inspect trace:"
echo ""
echo "    # HTTP status distribution:"
echo "    cat $OUTDIR/trace.jsonl | jq -r .http_status | sort | uniq -c | sort -rn"
echo ""
echo "    # Operations:"
echo "    cat $OUTDIR/trace.jsonl | jq -r .operation | sort | uniq -c"
echo ""
echo "    # Events with start_after or continuation_token:"
echo "    cat $OUTDIR/trace.jsonl | jq 'select(.start_after != null or .continuation_token != null)'"
echo ""
echo "    # Full debug stderr:"
echo "    less $OUTDIR/debug-stderr.log"
