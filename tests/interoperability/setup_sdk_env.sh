#!/usr/bin/env bash
# tests/interoperability/setup_sdk_env.sh
#
# Build the dedicated a2a-sdk venv used by the Phase 5 / Task 12
# interoperability test. The SDK is pinned to a single version so the
# test is reproducible across hosts and CI images. The venv is kept
# under tests/interoperability/.venv-sdk/ and is .gitignore-d; we never
# install the SDK into the Hermes runtime .venv (the plugin's own
# runtime must not depend on a2a-sdk per HANDOFF.md).
#
# Pin rationale:
#   a2a-sdk 1.1.2 — last 1.1.x line; Python 3.10+; includes
#   http-server extras (starlette + sse-starlette) and grpc extras
#   (grpcio + grpcio-tools) for the cross-implementation tests. We
#   pin exactly so the tests/interoperability/README.md trace table
#   and the v1.0/legacy split assertions stay deterministic.
#
# Usage:
#   ./tests/interoperability/setup_sdk_env.sh
#
# Idempotent: re-running after the venv exists is a no-op (the venv
# already has the pinned SDK). A venv whose pin doesn't match
# `tests/interoperability/README.md` is re-installed.
#
# Environment:
#   TEST_PYTHON — python interpreter to use for the venv (default:
#                 the python3 on PATH; the hermes runtime's
#                 /home/hermes/.hermes/hermes-agent/.venv/bin/python
#                 works too but is NOT required).
#   SDK_VERSION — override the a2a-sdk pin (default 1.1.2).
#   SDK_EXTRAS  — pip extras to add (default "http-server").
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${HERE}/.venv-sdk"
SDK_VERSION="${SDK_VERSION:-1.1.2}"
SDK_EXTRAS="${SDK_EXTRAS:-http-server}"
PYTHON_BIN="${TEST_PYTHON:-python3}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "setup_sdk_env.sh: ${PYTHON_BIN} not found on PATH" >&2
    exit 1
fi

# Sanity: the venv should live under tests/interoperability/. If the
# operator points this script at the hermes runtime by mistake, the
# .venv-sdk marker won't be there and we abort.
case "${VENV}" in
    "${HERE}/"*) ;;
    *)
        echo "setup_sdk_env.sh: refusing to create venv outside ${HERE}: ${VENV}" >&2
        exit 1
        ;;
esac

# The hermes runtime's python 3.14 venv leaks onto PYTHONPATH during
# subagent execution and breaks the SDK's pydantic_core ABI. Strip
# every reference to the host's bundled Python env (anything under
# /home/hermes/.hermes/installs/ or /home/hermes/.local/) before
# invoking pip.
LEAKY_PATH_RE='/home/hermes/\.hermes/installs/|/home/hermes/\.local/'

sanitize_env() {
    if [ -n "${PYTHONPATH:-}" ]; then
        # Strip path entries that match the leak pattern.
        local IFS=':'
        local cleaned=""
        for entry in ${PYTHONPATH}; do
            case "${entry}" in
                *${LEAKY_PATH_RE}*) ;;
                *) cleaned="${cleaned:+${cleaned}:}${entry}" ;;
            esac
        done
        if [ -z "${cleaned}" ]; then
            unset PYTHONPATH
        else
            export PYTHONPATH="${cleaned}"
        fi
    fi
}

# Build the venv if missing.
if [ ! -d "${VENV}" ]; then
    echo "setup_sdk_env.sh: creating ${VENV} with ${PYTHON_BIN}"
    sanitize_env
    "${PYTHON_BIN}" -m venv "${VENV}"
    "${VENV}/bin/pip" install --upgrade pip
fi

# Install the pinned SDK (idempotent — pip is a no-op if version matches).
# We assemble the requirement string carefully: ``a2a-sdk[extras]==VERSION``
# is the standard pip requirement syntax. The order matters — pip
# rejects ``a2a-sdk==VERSION[extras]`` (the version specifier must
# follow the extras marker), and bash word-splitting would otherwise
# split VERSION from the extras and produce a malformed requirement.
if [ -n "${SDK_EXTRAS}" ]; then
    SDK_REQ="a2a-sdk[${SDK_EXTRAS}]==${SDK_VERSION}"
else
    SDK_REQ="a2a-sdk==${SDK_VERSION}"
fi
echo "setup_sdk_env.sh: installing ${SDK_REQ}"
sanitize_env
"${VENV}/bin/pip" install "${SDK_REQ}"

# Install uvicorn so the SDK server side has a real ASGI runner.
# The version is unpinned: uvicorn's API is stable and the SDK does
# not bind to a specific version.
sanitize_env
"${VENV}/bin/pip" install uvicorn httpx

# Print the venv python and the resolved SDK version so the operator
# can paste it into the verification report.
echo
echo "setup_sdk_env.sh: done"
echo "  venv python: ${VENV}/bin/python"
echo "  a2a-sdk version:"
sanitize_env
"${VENV}/bin/python" -c "import a2a, a2a.types; print('    a2a-sdk at:', a2a.__file__)"
"${VENV}/bin/python" -c "import importlib.metadata as m; print('    a2a-sdk version:', m.version('a2a-sdk'))"
