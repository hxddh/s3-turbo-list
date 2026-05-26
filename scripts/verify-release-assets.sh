#!/usr/bin/env bash
# s3-turbo-list — published release asset verifier
# ---------------------------------------------------------------------------
# Downloads a GitHub release into /tmp, verifies the expected asset set and
# SHA256SUMS, then runs --version and --help for the current platform binary.
# It does not contact S3 or any cloud storage endpoint.
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

TAG="${1:-}"
if [ -z "$TAG" ]; then
  VERSION="$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)"
  TAG="v${VERSION}"
fi

VERSION="${TAG#v}"
REPO="${REPO:-hxddh/s3-turbo-list}"
VERIFY_DIR="${VERIFY_DIR:-/tmp/s3tl-${TAG}-verify}"

case "$(uname -s)" in
  Linux) OS="linux" ;;
  Darwin) OS="macos" ;;
  *)
    echo "ERROR: unsupported OS: $(uname -s)" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) ARCH="x86_64" ;;
  aarch64 | arm64) ARCH="aarch64" ;;
  *)
    echo "ERROR: unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

CURRENT_ASSET="s3-turbo-list-${VERSION}-${OS}-${ARCH}"
EXPECTED_ASSETS=(
  "s3-turbo-list-${VERSION}-linux-aarch64"
  "s3-turbo-list-${VERSION}-linux-aarch64.sha256"
  "s3-turbo-list-${VERSION}-linux-x86_64"
  "s3-turbo-list-${VERSION}-macos-aarch64"
  "s3-turbo-list-${VERSION}-macos-x86_64"
  "SHA256SUMS"
)

echo "=== Verify s3-turbo-list release assets ==="
echo "repo:       ${REPO}"
echo "tag:        ${TAG}"
echo "directory:  ${VERIFY_DIR}"
echo ""

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh is required." >&2
  exit 1
fi

RELEASE_STATE="$(gh release view "$TAG" \
  --repo "$REPO" \
  --json isDraft,isPrerelease \
  --jq '"draft=\(.isDraft) prerelease=\(.isPrerelease)"')"
echo "release:   ${RELEASE_STATE}"

if [ "$RELEASE_STATE" != "draft=false prerelease=false" ]; then
  echo "ERROR: release must be published and non-prerelease." >&2
  exit 1
fi

ASSET_LIST="$(gh release view "$TAG" --repo "$REPO" --json assets --jq '.assets[].name')"
for asset in "${EXPECTED_ASSETS[@]}"; do
  if printf '%s\n' "$ASSET_LIST" | grep -Fxq "$asset"; then
    echo "ok:        asset ${asset}"
  else
    echo "ERROR: missing release asset: ${asset}" >&2
    exit 1
  fi
done

rm -rf "$VERIFY_DIR"
mkdir -p "$VERIFY_DIR"
gh release download "$TAG" --repo "$REPO" --dir "$VERIFY_DIR"

cd "$VERIFY_DIR"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum -c SHA256SUMS
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 -c SHA256SUMS
else
  echo "ERROR: sha256sum or shasum is required." >&2
  exit 1
fi

if [ ! -x "$CURRENT_ASSET" ]; then
  chmod +x "$CURRENT_ASSET"
fi

VERSION_OUTPUT="$(./"$CURRENT_ASSET" --version)"
echo "version:   ${VERSION_OUTPUT}"
if [ "$VERSION_OUTPUT" != "s3-turbo-list ${VERSION}" ]; then
  echo "ERROR: unexpected version output: ${VERSION_OUTPUT}" >&2
  exit 1
fi

./"$CURRENT_ASSET" --help >/tmp/s3tl-${TAG}-help.txt
echo "help:      ok"
echo ""
echo "=== Release asset verification complete ==="
