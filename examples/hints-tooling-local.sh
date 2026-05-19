#!/usr/bin/env bash
set -euo pipefail
set -x

# Local-only trace-driven hints workflow.  These commands parse files already
# present on disk and do not contact S3 endpoints.

TRACE_FILE="${TRACE_FILE:-trace.jsonl}"
BASE_HINTS="${BASE_HINTS:-hints.toml}"
EXTRA_HINTS="${EXTRA_HINTS:-prefixes.txt}"
MERGED_HINTS="${MERGED_HINTS:-merged-hints.toml}"
NEXT_HINTS="${NEXT_HINTS:-rebalanced-hints.toml}"

s3-turbo-list trace-summary "$TRACE_FILE" --machine-readable

s3-turbo-list hints-merge \
  "$BASE_HINTS" "$EXTRA_HINTS" \
  --output "$MERGED_HINTS" \
  --emit-manifest merge.manifest.json \
  --machine-readable

s3-turbo-list hints-rebalance \
  --trace "$TRACE_FILE" \
  --hints-file "$MERGED_HINTS" \
  --output "$NEXT_HINTS" \
  --emit-manifest rebalance.manifest.json \
  --machine-readable
