#!/usr/bin/env bash
# TUI/websocket end-to-end verification: the shared gateway owner + a ws client + a real watch.
#
#   ./run-tui.sh [data-dir]
#
# Build the image first (once):
#   podman build --ignorefile ignore.txt -t hermes-e2e:local -f Containerfile "$HERMES_CHECKOUT"
#
# The container runs `hermes gateway run` (via the same bootstrap the Ink TUI uses), connects a
# WebSocket client that declares source=tui, and asserts the background A2A result is injected
# into that session as a new turn. Evidence lands in $DATA/evidence-tui.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA="${1:-/tmp/e2e-plugin-tui-data}"
PLUGIN="${E2E_PLUGIN:-$(cd "$HERE/../.." && pwd)}"
IMAGE="${E2E_IMAGE:-hermes-e2e:local}"

mkdir -p "$DATA"
DATA_REAL="$(realpath -m "$DATA")"
case "$DATA_REAL" in
  /|/home|/usr|/etc|/var|/tmp|"$HOME"|"$PLUGIN"|"$PLUGIN"/*|"$HERE"|"$HERE"/*)
    echo "[run-tui] refusing to use '$DATA_REAL' as an e2e data directory" >&2
    exit 2
    ;;
esac
if [ -n "$(ls -A "$DATA_REAL" 2>/dev/null)" ] && [ ! -f "$DATA_REAL/.e2e-data" ]; then
  echo "[run-tui] refusing to clear '$DATA_REAL': not an e2e directory (missing .e2e-data marker)" >&2
  exit 2
fi
touch "$DATA_REAL/.e2e-data"
find "$DATA_REAL" -mindepth 1 -maxdepth 1 ! -name '.e2e-data' -exec rm -rf {} +
echo "[run-tui] image=$IMAGE data=$DATA_REAL plugin=$PLUGIN"

set +e
podman run --rm \
  --name hermes-e2e-tui \
  -v "$DATA":/data:z \
  -v "$PLUGIN":/plugin:ro,z \
  -v "$HERE":/harness:ro,z \
  -e E2E_DATA=/data \
  -e E2E_PLUGIN=/plugin \
  -e E2E_HERMES_SRC=/src \
  -e PYTHONPATH=/src \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e TZ=UTC \
  -e LC_ALL=C.UTF-8 \
  "$IMAGE" /harness/tui_driver.py 2>&1 | tee "$DATA/driver.log"
status=${PIPESTATUS[0]}
set -e

echo
if [ "$status" -eq 0 ]; then
  echo "[run-tui] PASS"
else
  echo "[run-tui] FAIL (exit $status)"
  echo "--- evidence ---"
  ls -la "$DATA/evidence-tui" 2>/dev/null || true
  echo "--- gateway log tail ---"
  tail -n 40 "$DATA/home/.hermes/logs/gateway.log" 2>/dev/null || true
fi
exit "$status"
