#!/usr/bin/env bash
# End-to-end verification of plugin completion publishing + a2a background watch in a container.
#
#   ./run-container.sh [data-dir]
#
# Builds nothing here: build the image first with
#   podman build --ignorefile ignore.txt -t hermes-e2e:local -f Containerfile "$HERMES_CHECKOUT"
#
# The container runs a real `hermes chat` (autostarted gateway + CLI client), a scripted
# OpenAI-compatible model endpoint and a scripted A2A peer; the driver asserts delivery into the
# originating session and the durable ledger ack. Evidence lands in $DATA/evidence.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA="${1:-/tmp/e2e-plugin-completions-data}"
PLUGIN="${E2E_PLUGIN:-$(cd "$HERE/../.." && pwd)}"
IMAGE="${E2E_IMAGE:-hermes-e2e:local}"

mkdir -p "$DATA"
echo "[run] image=$IMAGE data=$DATA plugin=$PLUGIN"
rm -rf "$DATA"/*

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
