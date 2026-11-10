#!/usr/bin/env bash
# Full plugin suite in the container, including the official a2a-sdk
# cross-implementation tests.
#
#   ./run-suite.sh [data-dir]
#
# Build the image first (see README.md):
#   podman build --ignorefile ignore.txt -t hermes-e2e:local -f Containerfile "$HERMES_CHECKOUT"
#
# What this does that a bare `pytest tests` in the container does not:
#   * builds the export from `git archive HEAD` (plus the tracked worktree
#     changes, unless E2E_CLEAN=1), so uncommitted work is what gets tested;
#   * installs and enables the plugin in a disposable HERMES_HOME and
#     validates it (plugins list / validate / doctor);
#   * builds the pinned a2a-sdk venv at a writable path (the export is
#     mounted read-only) and exports A2A_SDK_VENV, so the three
#     interoperability tests RUN instead of skipping;
#   * fails the run if those three tests skipped, which is the whole point
#     of wiring the venv in.
#
# Env:
#   E2E_IMAGE        image (default hermes-e2e:local)
#   E2E_EXPORT       use a prebuilt export directory instead of git archive
#   E2E_CLEAN=1      archive HEAD only, ignore worktree changes
#   E2E_EXTRA_FILES  space-separated untracked files to copy into the export
#                    (e.g. a new test file that is not committed yet)
#   E2E_SDK_VENV     host path to cache the SDK venv (default: per-run in $DATA)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
DATA="${1:-/tmp/e2e-a2a-suite}"
DATA_REAL="$(realpath -m "$DATA")"
IMAGE="${E2E_IMAGE:-hermes-e2e:local}"
EXPORT="${E2E_EXPORT:-$DATA_REAL/export}"

if [ -n "${E2E_EXPORT:-}" ]; then
  case "$EXPORT" in
    "$DATA_REAL"/*) echo "[suite] E2E_EXPORT must live outside the data directory" >&2; exit 2 ;;
  esac
fi

# This script clears its output directory unless E2E_EXPORT points outside it.
# Same guard as run-container.sh: never /, $HOME, the repository or the harness.
case "$DATA_REAL" in
  /|/home|/usr|/etc|/var|/tmp|"$HOME"|"$REPO"|"$REPO"/*|"$HERE"|"$HERE"/*)
    echo "[suite] refusing to use '$DATA_REAL' as a suite data directory" >&2
    exit 2
    ;;
esac
if [ -n "$(ls -A "$DATA_REAL" 2>/dev/null)" ] && [ ! -f "$DATA_REAL/.e2e-data" ]; then
  echo "[suite] refusing to clear '$DATA_REAL': not an e2e directory (missing .e2e-data marker)" >&2
  exit 2
fi
mkdir -p "$DATA_REAL"
touch "$DATA_REAL/.e2e-data"
find "$DATA_REAL" -mindepth 1 -maxdepth 1 ! -name '.e2e-data' -exec rm -rf {} +

if [ -z "${E2E_EXPORT:-}" ]; then
  echo "[suite] exporting HEAD of $REPO to $EXPORT"
  mkdir -p "$EXPORT"
  git -C "$REPO" archive HEAD | tar -x -C "$EXPORT"
  if [ "${E2E_CLEAN:-0}" != "1" ]; then
    git -C "$REPO" diff --name-only --no-renames HEAD | while IFS= read -r path; do
      if [ -f "$REPO/$path" ]; then
        mkdir -p "$EXPORT/$(dirname "$path")"
        cp "$REPO/$path" "$EXPORT/$path"
        cmp "$REPO/$path" "$EXPORT/$path"
      else
        rm -f -- "$EXPORT/$path"
      fi
    done
    for path in ${E2E_EXTRA_FILES:-}; do
      mkdir -p "$EXPORT/$(dirname "$path")"
      cp "$REPO/$path" "$EXPORT/$path"
    done
  fi
else
  echo "[suite] using prebuilt export $EXPORT"
fi

VENV_ARGS=(-e A2A_SDK_VENV=/data/sdk-venv)
if [ -n "${E2E_SDK_VENV:-}" ]; then
  mkdir -p "$E2E_SDK_VENV"
  VENV_ARGS=(-v "$E2E_SDK_VENV":/sdk-venv:z -e A2A_SDK_VENV=/sdk-venv)
fi

echo "[suite] image=$IMAGE data=$DATA_REAL export=$EXPORT"
set +e
podman run --rm --init --name hermes-e2e-a2a-suite --entrypoint /bin/bash \
  -v "$EXPORT":/plugin:ro,z \
  -v "$DATA_REAL":/data:z \
  "${VENV_ARGS[@]}" \
  -e HERMES_HOME=/data/hermes-home \
  -e HERMES_SRC=/src \
  -e A2A_INTEROP_WORK_DIR=/data/interop \
  -e PYTHONPATH=/plugin \
  -e A2A_LIVE_PEER=dev-a \
  -e A2A_LIVE_PEER_PROFILE=/data/hermes-home \
  -e TEST_PYTHON=/opt/venv/bin/python \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e TZ=UTC -e LC_ALL=C.UTF-8 -e LANG=C.UTF-8 \
  "$IMAGE" -lc '
set -euo pipefail
mkdir -p /home/hermes/.hermes
ln -sfn /src /home/hermes/.hermes/hermes-agent
ln -sfn /opt/venv /src/.venv
mkdir -p "$HERMES_HOME/plugins"
printf "%s\n" "a2a_agents:" "  dev-a:" "    url: http://127.0.0.1:1" > "$HERMES_HOME/config.yaml"
cp -a /plugin "$HERMES_HOME/plugins/a2a-async"
/opt/venv/bin/hermes plugins enable a2a-async --no-allow-tool-override
/opt/venv/bin/hermes plugins list --json > /data/plugins-list.json
/opt/venv/bin/python - /data/plugins-list.json <<'PY'
import json, sys
plugins = json.load(open(sys.argv[1]))
entry = next((p for p in plugins if p.get("name") == "a2a-async"), None)
assert entry and entry.get("status") == "enabled" and entry.get("source") == "user", entry
PY
echo "[suite] plugins validate"
/opt/venv/bin/hermes plugins validate /plugin
echo "[suite] plugins doctor"
/opt/venv/bin/hermes plugins doctor a2a-async 2>&1 | tail -3

echo "[suite] building the a2a-sdk venv at ${A2A_SDK_VENV:-/plugin/tests/interoperability/.venv-sdk}"
if [ -n "${A2A_SDK_VENV:-}" ]; then
  SDK_VENV_DIR="$A2A_SDK_VENV" TEST_PYTHON=/opt/venv/bin/python /plugin/tests/interoperability/setup_sdk_env.sh
else
  TEST_PYTHON=/opt/venv/bin/python /plugin/tests/interoperability/setup_sdk_env.sh
fi

echo "[suite] a2a-sdk compatibility tests"
/opt/venv/bin/python -m pytest /plugin/tests/interoperability -q -p no:cacheprovider | tee /data/interop.log
if ! grep -Eq "^3 passed" /data/interop.log; then
  echo "[suite] FAIL: the a2a-sdk compatibility tests did not run (expected 3 passed)" >&2
  exit 1
fi

echo "[suite] full suite"
/opt/venv/bin/python -m pytest /plugin/tests -q -p no:cacheprovider -rs | tee /data/suite.log
' 2>&1 | tee "$DATA_REAL/suite-console.log"
status=${PIPESTATUS[0]}
set -e

echo
if [ "$status" -eq 0 ]; then
  echo "[suite] PASS"
  grep -E "passed|skipped" "$DATA_REAL/suite.log" | tail -1
else
  echo "[suite] FAIL (exit $status)"
  tail -n 40 "$DATA_REAL/suite-console.log"
fi
exit "$status"
