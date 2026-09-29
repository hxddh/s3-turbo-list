#!/usr/bin/env bash
set -euo pipefail

# Real-run example: this contacts S3.  It refuses to execute unless
# RUN_REAL_S3=1 is set explicitly.

if [[ "${RUN_REAL_S3:-}" != "1" ]]; then
  cat >&2 <<'EOF'
This example would contact an S3-compatible endpoint.
Set RUN_REAL_S3=1 plus BUCKET/REGION and AWS_PROFILE to run it intentionally.
Use PROVIDER only for S3-compatible provider presets such as minio,
bos, r2, b2, or oss.
EOF
  exit 2
fi

BIN="${S3_TURBO_LIST_BIN:-cargo run --}"
OUTDIR="${OUTDIR:-./artifacts/agent-run}"
BUCKET="${BUCKET:?set BUCKET}"
REGION="${REGION:?set REGION}"
PROVIDER="${PROVIDER:-}"

mkdir -p "$OUTDIR"

cmd=(
  $BIN
  list
  --bucket "$BUCKET"
  --region "$REGION"
  --output-parquet-file "$OUTDIR/list.parquet"
  --run-manifest "$OUTDIR/run.json"
  --trace-compat "$OUTDIR/trace.jsonl"
)

if [[ -n "$PROVIDER" ]]; then
  cmd+=(--provider "$PROVIDER")
fi
if [[ -n "${ENDPOINT_URL:-}" ]]; then
  cmd+=(--endpoint-url "$ENDPOINT_URL")
fi

printf 'Running S3 listing:\n'
printf '  %q' "${cmd[@]}"
printf '\n'
# The KeySpace file is written beside the Parquet file as list.ks.
"${cmd[@]}"

printf '\nManifest written to %s\n' "$OUTDIR/run.json"
printf 'Verify it locally with:\n  %s manifest-summary %q --check\n' "$BIN" "$OUTDIR/run.json"
