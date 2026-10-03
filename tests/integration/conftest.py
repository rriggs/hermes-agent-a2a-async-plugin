"""Hermetic env for the Phase 5 / Task 11 integration test.

Forward the sven profile's HERMES_HOME (so ``a2a_agents.dev-a`` is
discoverable) and any A2A_BEARER_TOKEN / A2A_PEER_TOKENS the operator
has set (so the live-peer (c) tests can authenticate). The plugin's
``_load_config`` is keyed off ``get_active_profile_name()``, which in
turn reads ``HERMES_HOME`` — without forwarding, the integration test
sees the default profile's empty a2a_agents and (c) cannot resolve
``dev-a``.

The bearer token is read from a single, narrowly-scoped file path
(``tests/integration/.a2a_live_token``) that the test runner can
optionally populate — the file is excluded from version control. This
keeps secrets out of HANDOFF.md and out of test code, while letting
the (c) live-peer tests authenticate when the operator wants the
full evidence (and skip cleanly when they do not).

This is intentionally opt-in: a fresh CI without the live peer simply
skips (c) per the plan's guard.
"""
from __future__ import annotations

import os
from pathlib import Path


_HERE = Path(__file__).resolve().parent
_SVEN_PROFILE_HOME = "/home/hermes/.hermes/profiles/sven"
_LIVE_TOKEN_FILE = _HERE / ".a2a_live_token"
_PASS_THROUGH_KEYS = (
    "A2A_BEARER_TOKEN", "A2A_PEER_TOKENS", "A2A_HOST", "A2A_PORT",
    "A2A_LIVE_PEER",
)


def _maybe_load_live_token() -> str | None:
    """Read the bearer token from the test-local file. The file is a
    single line containing the token; it is created on-demand by the
    operator (not by tests). Returns None if the file is absent —
    ``(c)`` will skip cleanly in that case.
    """
    if not _LIVE_TOKEN_FILE.exists():
        return None
    try:
        text = _LIVE_TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return text or None


def pytest_configure(config):
    """Forward env vars BEFORE any test collection that may load the
    plugin (so module-level imports of ``a2a_async_plugin`` see the
    right HERMES_HOME and don't pollute the operator's real home).
    """
    if os.path.isdir(_SVEN_PROFILE_HOME):
        # Only override when the operator hasn't pinned a custom HERMES_HOME.
        current = os.environ.get("HERMES_HOME", "")
        if not current or current == os.path.expanduser("~/.hermes"):
            os.environ["HERMES_HOME"] = _SVEN_PROFILE_HOME

    # Read the live-peer bearer token (if the operator dropped it next
    # to this conftest) and export it under the standard A2A_BEARER_TOKEN
    # name. The conftest never invents a token; it only propagates one
    # the operator explicitly provided.
    if not os.environ.get("A2A_BEARER_TOKEN") and not os.environ.get("A2A_PEER_TOKENS"):
        token = _maybe_load_live_token()
        if token:
            os.environ["A2A_BEARER_TOKEN"] = token
    for key in _PASS_THROUGH_KEYS:
        # Already set in the operator's env? Then leave it.
        pass
