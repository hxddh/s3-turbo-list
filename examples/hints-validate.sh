#!/usr/bin/env bash
# s3-turbo-list - hints validation example
# ---------------------------------------------------------------------------
# Validates a local TOML or plain-text hints file without contacting S3.
#
# Required env vars:
#   HINTS_FILE    - path to hints file
# Optional env vars:
#   S3_TURBO_LIST_BIN - path to binary (default: cargo run --)
# ---------------------------------------------------------------------------
set -euo pipefail

: "${HINTS_FILE:?set HINTS_FILE to a TOML or plain-text hints file}"
S3TL="${S3_TURBO_LIST_BIN:-cargo run --}"

echo "==> Validating hints file: $HINTS_FILE"

$S3TL --hints-file "$HINTS_FILE" doctor --local-only

echo "==> Hints file is structurally valid."
