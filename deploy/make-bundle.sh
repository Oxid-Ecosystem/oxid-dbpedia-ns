#!/usr/bin/env bash
# Build a verified, full-fidelity backup bundle from a LOCAL, loaded OxidDB
# container, ready to ship to the demo box.
#
#   ./deploy/make-bundle.sh [container] [output]
#
# compact/backup/verify all open the data dir directly and refuse one held by a
# live server, so the container is stopped for the duration and restarted after.
set -euo pipefail

CONTAINER="${1:-oxid-dbpedia-ns}"
OUT="${2:-dist/t50.oxdb-bundle}"

command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
docker inspect "$CONTAINER" >/dev/null 2>&1 || {
  echo "no such container: $CONTAINER" >&2; exit 1; }

IMAGE=$(docker inspect "$CONTAINER" --format '{{.Config.Image}}')
VOLUME=$(docker inspect "$CONTAINER" \
  --format '{{range .Mounts}}{{if eq .Destination "/app/data"}}{{.Name}}{{end}}{{end}}')
[ -n "$VOLUME" ] || { echo "$CONTAINER has no named volume at /app/data" >&2; exit 1; }

OUT_DIR=$(cd "$(dirname "$OUT")" 2>/dev/null && pwd || { mkdir -p "$(dirname "$OUT")" && cd "$(dirname "$OUT")" && pwd; })
OUT_NAME=$(basename "$OUT")
SCRATCH="$OUT_DIR/.verify-scratch"
mkdir -p "$SCRATCH"

# Run an offline oxd against the same volume, with the server down.
oxd_offline() {
  docker run --rm \
    -v "$VOLUME:/app/data" \
    -v "$OUT_DIR:/out" \
    --entrypoint oxd "$IMAGE" "$@"
}

WAS_RUNNING=$(docker inspect "$CONTAINER" --format '{{.State.Running}}')
cleanup() {
  rm -rf "$SCRATCH"
  if [ "$WAS_RUNNING" = "true" ]; then
    echo "==> restarting $CONTAINER"
    docker start "$CONTAINER" >/dev/null
  fi
}
trap cleanup EXIT

if [ "$WAS_RUNNING" = "true" ]; then
  echo "==> stopping $CONTAINER (offline file operations)"
  docker stop "$CONTAINER" >/dev/null
fi

echo "==> compacting (folds live deltas into a fresh base)"
oxd_offline compact --data-dir /app/data

echo "==> writing bundle"
# --bundle is REQUIRED: the single-file backup drops cold vectors, and this
# collection is cold_f32 with a ~192 MiB .vecs file beside the checkpoint.
oxd_offline backup create --data-dir /app/data --bundle "/out/$OUT_NAME"

echo "==> verifying the bundle (six checks, including a fresh reclassify)"
oxd_offline verify --backup "/out/$OUT_NAME" --scratch-dir /out/.verify-scratch

( cd "$OUT_DIR" && shasum -a 256 "$OUT_NAME" > "$OUT_NAME.sha256" )

echo
echo "bundle : $OUT_DIR/$OUT_NAME  ($(du -h "$OUT_DIR/$OUT_NAME" | cut -f1))"
echo "sha256 : $(cut -d' ' -f1 < "$OUT_DIR/$OUT_NAME.sha256")"
echo
echo "next: scp it to the demo box, then run deploy/restore.sh there."
