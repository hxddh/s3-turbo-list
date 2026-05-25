#!/usr/bin/env bash
# s3-turbo-list — release environment checker
# ---------------------------------------------------------------------------
# Checks local prerequisites for a release build.  Does not install anything,
# does not run cloud commands, does not modify system packages.
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

echo "=== s3-turbo-list Release Environment ==="
echo ""

# ── OS information ────────────────────────────────────────
echo "--- OS ---"
echo "uname:   $(uname -a)"
if command -v lsb_release &>/dev/null; then
  echo "distro:  $(lsb_release -ds 2>/dev/null || echo 'unknown')"
fi
echo "arch:    $(uname -m)"

# ── Rust toolchain ─────────────────────────────────────────
echo ""
echo "--- Rust ---"
echo "rustc:   $(rustc --version 2>/dev/null || echo 'NOT FOUND')"
echo "cargo:   $(cargo --version 2>/dev/null || echo 'NOT FOUND')"

# ── C compilers ────────────────────────────────────────────
echo ""
echo "--- C Compilers ---"

if command -v cc &>/dev/null; then
  echo "cc:      $(cc --version 2>/dev/null | head -1 || echo 'present (version unknown)')"
else
  echo "cc:      NOT FOUND"
fi

if command -v gcc &>/dev/null; then
  GCC_VER=$(gcc --version 2>/dev/null | head -1 || echo 'unknown')
  echo "gcc:     $GCC_VER"
  GCC_MAJOR=$(echo "$GCC_VER" | grep -oP '\d+' | head -1 || echo "0")
else
  echo "gcc:     NOT FOUND"
  GCC_MAJOR=0
fi

if command -v clang &>/dev/null; then
  echo "clang:   $(clang --version 2>/dev/null | head -1 || echo 'present (version unknown)')"
  HAVE_CLANG=1
else
  echo "clang:   NOT FOUND"
  HAVE_CLANG=0
fi

if command -v gcc-10 &>/dev/null; then
  echo "gcc-10:  $(gcc-10 --version 2>/dev/null | head -1 || echo 'present (version unknown)')"
  HAVE_GCC10=1
else
  echo "gcc-10:  NOT FOUND"
  HAVE_GCC10=0
fi

# ── OpenSSL / pkg-config ───────────────────────────────────
echo ""
echo "--- Libraries ---"

if command -v pkg-config &>/dev/null; then
  echo "pkg-config: $(pkg-config --version 2>/dev/null || echo 'present')"
else
  echo "pkg-config: NOT FOUND"
fi

if pkg-config --exists openssl 2>/dev/null; then
  echo "openssl:  $(pkg-config --modversion openssl 2>/dev/null || echo 'present')"
else
  echo "openssl:  NOT FOUND (via pkg-config)"
fi

# ── Git state ──────────────────────────────────────────────
echo ""
echo "--- Git ---"
echo "commit:   $(git rev-parse HEAD 2>/dev/null || echo 'NOT A REPO')"
echo "branch:   $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')"

DIRTY=$(git status --short 2>/dev/null)
if [ -z "$DIRTY" ]; then
  echo "status:   clean"
else
  echo "status:   DIRTY"
  echo "$DIRTY"
fi

# ── Release consistency ───────────────────────────────────
echo ""
echo "--- Release Consistency ---"

VERSION=$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
TAG="v${VERSION}"
CHECK_FAILED=0

echo "version: $VERSION"
echo "tag:     $TAG"

check_contains() {
  local file="$1"
  local pattern="$2"
  local label="$3"
  if grep -qE "$pattern" "$file"; then
    echo "ok:      $label"
  else
    echo "MISSING: $label"
    CHECK_FAILED=1
  fi
}

check_not_contains() {
  local file="$1"
  local pattern="$2"
  local label="$3"
  if grep -qE "$pattern" "$file"; then
    echo "STALE:   $label"
    CHECK_FAILED=1
  else
    echo "ok:      $label"
  fi
}

check_contains "CHANGELOG.md" "^## \\[${VERSION//./\\.}\\]" "CHANGELOG section for $VERSION"
check_contains "Cargo.lock" "^version = \"${VERSION//./\\.}\"$" "Cargo.lock contains crate version $VERSION"
check_contains "INSTALL.md" "s3-turbo-list-${VERSION}-linux-x86_64" "INSTALL linux x86_64 asset"
check_contains "INSTALL.md" "s3-turbo-list-${VERSION}-linux-aarch64" "INSTALL linux aarch64 asset"
check_contains "docs/build-release.md" "s3-turbo-list-${VERSION}-linux-aarch64" "build-release linux aarch64 asset"
check_contains "AGENTS.md" "Current release tag: \`${TAG}\`" "AGENTS current release tag"
check_contains ".github/workflows/release-assets.yml" "default: '${TAG}'" "release workflow default tag"
check_not_contains ".github/workflows/release-assets.yml" "s3-turbo-list-[0-9]+\\.[0-9]+\\.[0-9]+" "hard-coded versioned release asset names"

if awk '
  $0 == "name = \"s3-turbo-list\"" { in_pkg=1; next }
  in_pkg && $0 ~ /^version = / { print; exit }
' Cargo.lock | grep -qx "version = \"$VERSION\""; then
  echo "ok:      Cargo.lock s3-turbo-list package version"
else
  echo "MISSING: Cargo.lock s3-turbo-list package version $VERSION"
  CHECK_FAILED=1
fi

if [ "$CHECK_FAILED" -ne 0 ]; then
  echo ""
  echo "ERROR: release consistency checks failed." >&2
  exit 1
fi

# ── Output directories ─────────────────────────────────────
echo ""
echo "--- Output ---"

if [ -d "target/release" ]; then
  if [ -w "target/release" ]; then
    echo "target/release: writable"
  else
    echo "target/release: EXISTS but NOT WRITABLE"
  fi
else
  echo "target/release: does not exist (will be created by cargo)"
fi

if [ -d "dist" ]; then
  if [ -w "dist" ]; then
    echo "dist:           writable"
  else
    echo "dist:           EXISTS but NOT WRITABLE"
  fi
else
  mkdir -p dist 2>/dev/null && rmdir dist 2>/dev/null
  if [ $? -eq 0 ]; then
    echo "dist:           can be created"
  else
    echo "dist:           CANNOT CREATE (permissions?)"
  fi
fi

# ── aws-lc-sys workaround guidance ─────────────────────────
echo ""
echo "--- aws-lc-sys Release Build on aarch64 ---"

ARCH=$(uname -m)
NEEDS_WORKAROUND=0

if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
  if [ "$GCC_MAJOR" -lt 10 ] 2>/dev/null && [ "$HAVE_CLANG" -eq 0 ] && [ "$HAVE_GCC10" -eq 0 ]; then
    NEEDS_WORKAROUND=1
  fi
fi

if [ "$NEEDS_WORKAROUND" -eq 1 ]; then
  echo ""
  echo "  ⚠️  This system (aarch64, GCC < 10, no clang, no gcc-10)"
  echo "     may fail 'cargo build --release' due to an aws-lc-sys"
  echo "     GCC memcmp bug detection."
  echo ""
  echo "  Workarounds (pick one):"
  echo "    - Install clang:   sudo apt install clang"
  echo "    - Install gcc-10:  sudo apt install gcc-10"
  echo "    - Disable ASM:     AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1"
  echo ""
  echo "  Use scripts/build-release.sh with BUILD_MODE:"
  echo "    BUILD_MODE=clang   ./scripts/build-release.sh"
  echo "    BUILD_MODE=gcc10   ./scripts/build-release.sh"
  echo "    BUILD_MODE=no-asm  ./scripts/build-release.sh"
else
  if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    if [ "$HAVE_CLANG" -eq 1 ]; then
      echo "  ✅ clang available — recommended BUILD_MODE=clang"
    elif [ "$HAVE_GCC10" -eq 1 ]; then
      echo "  ✅ gcc-10 available — recommended BUILD_MODE=gcc10"
    else
      echo "  ✅ GCC >= 10 detected — default build should work"
    fi
  else
    echo "  ✅ Not aarch64 — default build should work"
  fi
fi

echo ""
echo "=== Check complete ==="
