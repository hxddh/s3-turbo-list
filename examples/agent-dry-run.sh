#!/usr/bin/env bash
set -euo pipefail

# Local-only example: this command does not contact S3.
#
# Optional env vars:
#   BUCKET, REGION     — bucket and region to plan (defaults are placeholders)
#   PROVIDER           — provider preset: minio, bos, r2, b2, oss (optional)
#   ENDPOINT_URL       — custom S3 endpoint (optional)
#   OUTDIR             — output directory (default: ./artifacts/agent-dry-run)
#   S3_TURBO_LIST_BIN  — path to binary (default: cargo run --)

BIN="${S3_TURBO_LIST_BIN:-cargo run --}"
OUTDIR="${OUTDIR:-./artifacts/agent-dry-run}"
BUCKET="${BUCKET:-example-bucket}"
REGION="${REGION:-us-east-1}"
PROVIDER="${PROVIDER:-}"

mkdir -p "$OUTDIR"

cmd=(
  $BIN
  list
  --bucket "$BUCKET"
  --region "$REGION"
  --output-dir "$OUTDIR"
  --dry-run
)

if [[ -n "$PROVIDER" ]]; then
  cmd+=(--provider "$PROVIDER")
fi
if [[ -n "${ENDPOINT_URL:-}" ]]; then
  cmd+=(--endpoint-url "$ENDPOINT_URL")
fi

printf 'Running local-only dry-run:\n'
printf '  %q' "${cmd[@]}"
printf '\n'
# The plan goes to stdout; a blocked plan is still written, then the
# command exits with the code the real run would (3 or 5).
status=0
"${cmd[@]}" > "$OUTDIR/plan.json" || status=$?

printf '\nPlan written to %s (exit %s)\n' "$OUTDIR/plan.json" "$status"
exit "$status"
