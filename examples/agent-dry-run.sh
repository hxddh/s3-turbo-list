#!/usr/bin/env bash
set -euo pipefail

# Local-only example: this command does not contact S3.

BIN="${S3_TURBO_LIST_BIN:-cargo run --}"
OUTDIR="${OUTDIR:-./artifacts/agent-dry-run}"
BUCKET="${BUCKET:-example-bucket}"
REGION="${REGION:-us-east-1}"
PROFILE="${PROFILE:-default}"

mkdir -p "$OUTDIR"

cmd=(
  $BIN
  --dry-run
  --agent
  --plan-json "$OUTDIR/plan.json"
  --output-parquet-file "$OUTDIR/list.parquet"
  --output-ks-file "$OUTDIR/list.ks"
  list
  --bucket "$BUCKET"
  --region "$REGION"
  --profile "$PROFILE"
)

printf 'Running local-only dry-run:\n'
printf '  %q' "${cmd[@]}"
printf '\n'
"${cmd[@]}"

printf '\nPlan written to %s\n' "$OUTDIR/plan.json"
