#!/usr/bin/env bash
set -euo pipefail

# Real-run example: this contacts S3.  It refuses to execute unless
# RUN_REAL_S3=1 is set explicitly.

if [[ "${RUN_REAL_S3:-}" != "1" ]]; then
  cat >&2 <<'EOF'
This example would contact an S3-compatible endpoint.
Set RUN_REAL_S3=1 plus BUCKET/REGION and AWS_PROFILE to run it intentionally.
Use ENDPOINT_PROFILE only for endpoint compatibility presets such as minio,
bos, r2, b2, or oss.
EOF
  exit 2
fi

BIN="${S3_TURBO_LIST_BIN:-cargo run --}"
OUTDIR="${OUTDIR:-./artifacts/agent-run}"
BUCKET="${BUCKET:?set BUCKET}"
REGION="${REGION:?set REGION}"
ENDPOINT_PROFILE="${ENDPOINT_PROFILE:-}"

mkdir -p "$OUTDIR"

cmd=(
  $BIN
  --run-manifest "$OUTDIR/run.json"
  --trace-compat "$OUTDIR/trace.jsonl"
  --output-parquet-file "$OUTDIR/list.parquet"
  --output-ks-file "$OUTDIR/list.ks"
  --delimiter ''
  list
  --bucket "$BUCKET"
  --region "$REGION"
)

if [[ -n "$ENDPOINT_PROFILE" ]]; then
  cmd+=(--profile "$ENDPOINT_PROFILE")
fi

printf 'Running S3 listing:\n'
printf '  %q' "${cmd[@]}"
printf '\n'
"${cmd[@]}"

printf '\nManifest written to %s\n' "$OUTDIR/run.json"
