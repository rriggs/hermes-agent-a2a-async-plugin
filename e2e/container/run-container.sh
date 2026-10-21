#!/usr/bin/env bash
# Classic-CLI end-to-end check: background mode must refuse when the host cannot display a turn.
#
#   ./run-container.sh [data-dir]
#
# Builds nothing here: build the image first with
#   podman build --ignorefile ignore.txt -t hermes-e2e:local -f Containerfile "$HERMES_CHECKOUT"
#
# The container runs a real `hermes chat` (autostarted gateway + CLI client), a scripted
# OpenAI-compatible model endpoint and a scripted A2A peer. The driver asserts the refusal
# reached the model, no watch was persisted and the peer was never polled. For the delivery
# path use ./run-tui.sh (real gateway owner + `source=tui` websocket client).
# Evidence lands in $DATA/evidence.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA="${1:-/tmp/e2e-plugin-completions-data}"
PLUGIN="${E2E_PLUGIN:-$(cd "$HERE/../.." && pwd)}"
IMAGE="${E2E_IMAGE:-hermes-e2e:local}"

mkdir -p "$DATA"
DATA_REAL="$(realpath -m "$DATA")"
# This script clears its output directory. Only a dedicated e2e directory may be cleared: never
# /, $HOME, the repository or the harness, and never a directory that holds something the script
# did not create (marker file). A typo such as `./run-container.sh .` must fail, not delete code.
case "$DATA_REAL" in
  /|/home|/usr|/etc|/var|/tmp|"$HOME"|"$PLUGIN"|"$PLUGIN"/*|"$HERE"|"$HERE"/*)
    echo "[run] refusing to use '$DATA_REAL' as an e2e data directory" >&2
    exit 2
    ;;
esac
if [ -n "$(ls -A "$DATA_REAL" 2>/dev/null)" ] && [ ! -f "$DATA_REAL/.e2e-data" ]; then
  echo "[run] refusing to clear '$DATA_REAL': not an e2e directory (missing .e2e-data marker)" >&2
  exit 2
fi
touch "$DATA_REAL/.e2e-data"
find "$DATA_REAL" -mindepth 1 -maxdepth 1 ! -name '.e2e-data' -exec rm -rf {} +
echo "[run] image=$IMAGE data=$DATA_REAL plugin=$PLUGIN"

set +e
podman run --rm \
  --name hermes-e2e-plugin-completions \
  -v "$DATA":/data:z \
  -v "$PLUGIN":/plugin:ro,z \
  -v "$HERE":/harness:ro,z \
  -e E2E_DATA=/data \
  -e E2E_PLUGIN=/plugin \
  -e E2E_HERMES=/opt/venv/bin/hermes \
  "$IMAGE" /harness/driver.py 2>&1 | tee "$DATA/driver.log"
status=${PIPESTATUS[0]}
set -e

echo
if [ "$status" -eq 0 ]; then
  echo "[run] PASS"
  python3 - "$DATA/evidence/summary.json" <<'PY'
import json, sys
with open(sys.argv[1]) as fh:
    print(json.dumps(json.load(fh), indent=2))
PY
else
  echo "[run] FAIL (exit $status)"
  echo "--- evidence ---"
  ls -la "$DATA/evidence" 2>/dev/null || true
  echo "--- gateway tail ---"
  tail -n 30 "$DATA/hermes/logs/gateway.log" 2>/dev/null || true
fi
exit "$status"