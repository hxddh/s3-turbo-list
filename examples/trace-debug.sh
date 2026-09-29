#!/usr/bin/env bash
# s3-turbo-list — S3 request trace example
# ---------------------------------------------------------------------------
# --trace-compat FILE records every listing page request as JSONL (startup
# discovery and split probes are not traced); --trace-compat - writes the
# same events to stderr.
# Field reference: docs/tuning.md, "Trace event fields".
#
# Required env vars:
#   BUCKET        — S3 bucket name
# Optional env vars:
#   REGION        — region (default: us-east-1)
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

echo "==> Listing with an S3 request trace and a run log"
echo "    Bucket:   $BUCKET"
echo "    Trace:    $OUTDIR/trace.jsonl"
echo "    Parquet:  $OUTDIR/trace-output.parquet (+ .ks, .log beside it)"

# Build CLI args; conditionally include --endpoint-url.
set -- \
  list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --output-parquet-file "$OUTDIR/trace-output.parquet" \
  --trace-compat "$OUTDIR/trace.jsonl" \
  --log

if [ -n "${ENDPOINT_URL:-}" ]; then
  set -- "$@" --endpoint-url "$ENDPOINT_URL"
fi

$S3TL "$@"

echo "==> Done.  Inspect the trace:"
echo ""
echo "    python3 examples/inspect-trace.py $OUTDIR/trace.jsonl"
echo ""
echo "    # HTTP status distribution:"
echo "    jq -r .http_status $OUTDIR/trace.jsonl | sort | uniq -c | sort -rn"
echo ""
echo "    # Operations:"
echo "    jq -r .operation $OUTDIR/trace.jsonl | sort | uniq -c"
echo ""
echo "    # Events with start_after or continuation_token:"
echo "    jq 'select(.start_after != null or .continuation_token != null)' $OUTDIR/trace.jsonl"
echo ""
echo "    # Run log:"
echo "    less $OUTDIR/trace-output.log"
