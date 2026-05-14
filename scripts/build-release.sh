#!/usr/bin/env bash
# s3-turbo-list — release build and packaging script
# ---------------------------------------------------------------------------
# Builds a release binary and packages it into dist/.
# No network commands, no cloud endpoints, no GitHub release creation.
#
# Environment variables:
#   RELEASE_PROFILE — cargo profile (default: release)
#   TARGET          — cross-compilation target triple (optional)
#   BUILD_MODE      — compiler workaround mode:
#                       default  → cargo build --release
#                       clang    → CC=clang cargo build --release
#                       gcc10    → CC=gcc-10 cargo build --release
#                       no-asm   → AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1
#   BINARY_NAME     — output binary name (default: s3-turbo-list)
#   STRIP           — if set to 1, strip the binary after build
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

RELEASE_PROFILE="${RELEASE_PROFILE:-release}"
TARGET="${TARGET:-}"
BUILD_MODE="${BUILD_MODE:-default}"
BINARY_NAME="${BINARY_NAME:-s3-turbo-list}"
STRIP="${STRIP:-0}"

# ── Determine OS/arch for naming ───────────────────────────
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
VERSION="$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)"

if [ -n "$TARGET" ]; then
  SUFFIX="$TARGET"
else
  SUFFIX="${OS}-${ARCH}"
fi

DIST_DIR="$REPO_DIR/dist"
mkdir -p "$DIST_DIR"

OUTPUT_NAME="${BINARY_NAME}-${VERSION}-${SUFFIX}"

echo "=== s3-turbo-list Release Build ==="
echo "  Profile:      $RELEASE_PROFILE"
echo "  Build mode:   $BUILD_MODE"
echo "  Target:       ${TARGET:-<host>}"
echo "  Binary name:  $BINARY_NAME"
echo "  Output:       $DIST_DIR/$OUTPUT_NAME"
echo ""

# ── Build ──────────────────────────────────────────────────
case "$BUILD_MODE" in
  default)
    echo "==> cargo build --profile $RELEASE_PROFILE"
    if [ -n "$TARGET" ]; then
      cargo build --profile "$RELEASE_PROFILE" --target "$TARGET"
    else
      cargo build --profile "$RELEASE_PROFILE"
    fi
    ;;
  clang)
    if ! command -v clang &>/dev/null; then
      echo "ERROR: BUILD_MODE=clang but clang is not installed." >&2
      exit 1
    fi
    echo "==> CC=clang cargo build --profile $RELEASE_PROFILE"
    if [ -n "$TARGET" ]; then
      CC=clang cargo build --profile "$RELEASE_PROFILE" --target "$TARGET"
    else
      CC=clang cargo build --profile "$RELEASE_PROFILE"
    fi
    ;;
  gcc10)
    if ! command -v gcc-10 &>/dev/null; then
      echo "ERROR: BUILD_MODE=gcc10 but gcc-10 is not installed." >&2
      exit 1
    fi
    echo "==> CC=gcc-10 cargo build --profile $RELEASE_PROFILE"
    if [ -n "$TARGET" ]; then
      CC=gcc-10 cargo build --profile "$RELEASE_PROFILE" --target "$TARGET"
    else
      CC=gcc-10 cargo build --profile "$RELEASE_PROFILE"
    fi
    ;;
  no-asm)
    echo "==> AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1 cargo build --profile $RELEASE_PROFILE"
    if [ -n "$TARGET" ]; then
      AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1 cargo build --profile "$RELEASE_PROFILE" --target "$TARGET"
    else
      AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1 cargo build --profile "$RELEASE_PROFILE"
    fi
    ;;
  *)
    echo "ERROR: unknown BUILD_MODE '$BUILD_MODE'.  Valid: default, clang, gcc10, no-asm" >&2
    exit 1
    ;;
esac

echo ""

# ── Locate the built binary ────────────────────────────────
if [ -n "$TARGET" ]; then
  SRC_BIN="target/${TARGET}/${RELEASE_PROFILE}/${BINARY_NAME}"
else
  SRC_BIN="target/${RELEASE_PROFILE}/${BINARY_NAME}"
fi

if [ ! -f "$SRC_BIN" ]; then
  echo "ERROR: binary not found at $SRC_BIN" >&2
  exit 1
fi

# ── Strip (optional) ───────────────────────────────────────
if [ "$STRIP" = "1" ]; then
  echo "==> Stripping $SRC_BIN"
  strip "$SRC_BIN"
fi

# ── Copy to dist/ ──────────────────────────────────────────
cp "$SRC_BIN" "$DIST_DIR/$OUTPUT_NAME"
echo "==> Copied to $DIST_DIR/$OUTPUT_NAME"

# ── Binary info ────────────────────────────────────────────
SIZE=$(stat -c%s "$DIST_DIR/$OUTPUT_NAME" 2>/dev/null || stat -f%z "$DIST_DIR/$OUTPUT_NAME" 2>/dev/null || echo "?")
echo "    Size: $SIZE bytes"

# ── Checksum ───────────────────────────────────────────────
cd "$DIST_DIR"
if command -v sha256sum &>/dev/null; then
  sha256sum "$OUTPUT_NAME" > "${OUTPUT_NAME}.sha256"
elif command -v shasum &>/dev/null; then
  shasum -a 256 "$OUTPUT_NAME" > "${OUTPUT_NAME}.sha256"
else
  echo "WARNING: no sha256sum or shasum available — skipping checksum" >&2
fi

echo "    SHA256: ${OUTPUT_NAME}.sha256"
echo ""

# ── Verify binary ──────────────────────────────────────────
echo "==> Verifying binary: --help"
"$DIST_DIR/$OUTPUT_NAME" --help 2>&1 | head -5
echo ""
echo "==> Verifying binary: --version"
"$DIST_DIR/$OUTPUT_NAME" --version 2>&1

echo ""
echo "=== Release build complete ==="
echo "  Binary:   $DIST_DIR/$OUTPUT_NAME"
if [ -f "$DIST_DIR/${OUTPUT_NAME}.sha256" ]; then
  echo "  Checksum: $DIST_DIR/${OUTPUT_NAME}.sha256"
fi
