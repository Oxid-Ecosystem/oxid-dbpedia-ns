#!/usr/bin/env bash
# Build (and optionally push) the self-contained demo image: OxidDB with the
# t50 tier already loaded and classified.
#
#   ./deploy/demo-image/build.sh                    # build linux/amd64 locally
#   ./deploy/demo-image/build.sh --push             # build and push to Scaleway
#   VERSION=0.9.9-t50-1.1.0 ./deploy/demo-image/build.sh --push
#
# Needs dist/t50.oxdb-bundle first: make bundle
set -euo pipefail

cd "$(dirname "$0")/../.."

BASE="${BASE_IMAGE:-moonlightarray/oxid-db:0.9.9}"
VERSION="${VERSION:-0.9.9-t50demo}"   # no second hyphen: Oxid-Cloud hides tags that have one
REGISTRY="${REGISTRY:-rg.nl-ams.scw.cloud/oxid-db}"
IMAGE="${IMAGE:-oxid-db}"
PLATFORM="${PLATFORM:-linux/amd64}"   # Scaleway instances are amd64
BUNDLE="${BUNDLE:-dist/t50.oxdb-bundle}"
CTX="dist/demo-context"
REF="$REGISTRY/$IMAGE:$VERSION"

[ -f "$BUNDLE" ] || { echo "no bundle at $BUNDLE — run 'make bundle' first" >&2; exit 1; }

echo "==> seeding the build context from $BUNDLE"
rm -rf "$CTX/seed"
mkdir -p "$CTX/dataset"
docker run --rm -v "$PWD/$(dirname "$BUNDLE"):/work" --entrypoint oxd "$BASE" \
  restore --data-dir "/work/$(basename "$CTX")/seed" "/work/$(basename "$BUNDLE")" >/dev/null

echo "==> collecting dataset provenance (CC BY-SA 4.0 travels with the data)"
cp out/t50/ATTRIBUTION.md out/t50/manifest.json LICENSE-DATA "$CTX/dataset/"

echo "==> building $REF ($PLATFORM)"
if [ "${1:-}" = "--push" ]; then
  # Refuse to clobber a tag that already exists. This repo is the one the
  # Oxid-Cloud control plane provisions from and its house rule is that tags
  # are never deleted, so a wrong tag here is permanent.
  if docker buildx imagetools inspect "$REF" >/dev/null 2>&1; then
    echo "REFUSING: $REF already exists in the registry. Bump VERSION." >&2
    exit 1
  fi
  docker buildx build --platform "$PLATFORM" --provenance=false --sbom=false \
    -f deploy/demo-image/Dockerfile -t "$REF" --push "$CTX"
  echo "pushed $REF"
else
  docker buildx build --platform "$PLATFORM" --provenance=false --sbom=false \
    -f deploy/demo-image/Dockerfile -t "$IMAGE:$VERSION" --load "$CTX"
  echo "built $IMAGE:$VERSION (local only; pass --push to publish)"
fi
