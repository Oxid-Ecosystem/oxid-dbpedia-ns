#!/usr/bin/env bash
# Materialise a shipped bundle into the demo box's volume and start serving it.
# Run this ON THE DEMO BOX, from the deploy/ directory, with deploy/.env present.
#
#   ./restore.sh t50.oxdb-bundle
#
set -euo pipefail

BUNDLE="${1:?usage: ./restore.sh <bundle>}"
[ -f "$BUNDLE" ] || { echo "no such bundle: $BUNDLE" >&2; exit 1; }
[ -f .env ] || { echo "deploy/.env missing (copy .env.example and set OXD_API_TOKEN)" >&2; exit 1; }
# shellcheck disable=SC1091
set -a; . ./.env; set +a
IMAGE="${OXD_IMAGE:-moonlightarray/oxid-db:0.9.9}"
PORT="${OXD_PORT:-7878}"
UI_PORT="${OXD_UI_PORT:-7880}"

if [ -f "$BUNDLE.sha256" ]; then
  echo "==> checking sha256"
  shasum -a 256 -c "$BUNDLE.sha256" 2>/dev/null || sha256sum -c "$BUNDLE.sha256"
fi

echo "==> verifying the bundle before it touches the volume"
docker run --rm -v "$PWD:/in" --entrypoint oxd "$IMAGE" \
  verify --backup "/in/$BUNDLE" --scratch-dir /tmp

echo "==> stopping any running server"
docker compose down --remove-orphans >/dev/null 2>&1 || true

# Let Compose create the network and volume (but not start anything), then ask
# it what the volume is actually called, rather than guessing the project prefix.
docker compose up --no-start >/dev/null
VOLUME=$(docker inspect oxid-t50 \
  --format '{{range .Mounts}}{{if eq .Destination "/app/data"}}{{.Name}}{{end}}{{end}}')
[ -n "$VOLUME" ] || { echo "could not resolve the data volume" >&2; exit 1; }

echo "==> restoring into $VOLUME"
docker run --rm -v "$VOLUME:/app/data" -v "$PWD:/in" --entrypoint oxd "$IMAGE" \
  restore --data-dir /app/data "/in/$BUNDLE" --force

echo "==> starting"
docker compose up -d

printf '==> waiting for /health'
for _ in $(seq 1 60); do
  if curl -sf -m 2 http://127.0.0.1:$PORT/health >/dev/null 2>&1; then
    echo " ok"; break
  fi
  printf '.'; sleep 1
done

echo
echo "collections:"
curl -s -H "Authorization: Bearer $OXD_API_TOKEN" http://127.0.0.1:$PORT/collections
echo
echo "resident memory (anon — not 'docker stats', which counts page cache):"
docker exec oxid-t50 sh -c "awk '/^anon /{printf \"  %.0f MiB\n\", \$2/1048576}' /sys/fs/cgroup/memory.stat"
echo
echo "Expect ~50000 vectors and roughly 400 MiB. Reach it from your laptop with:"
echo "  ssh -N -L $PORT:127.0.0.1:$PORT -L $UI_PORT:127.0.0.1:$UI_PORT \$USER@<host>"
