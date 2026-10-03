"""Hermetic env for the Phase 5 / Task 11 integration test.

Live-peer (c) tests are OPT-IN through an explicit profile gate:
set ``A2A_LIVE_PEER_PROFILE`` to a profile home directory (absolute
path) and the conftest will forward that profile's identity to the
test process (HERMES_HOME) so ``a2a_agents.<peer>`` resolves. Without
the gate nothing is forwarded: a suite run on ANY host - with or
without a Hermes install - cannot silently adopt another profile's
peer configuration, and the live tests skip cleanly per the plan's
"skip if the peer probe is unreachable or not authed" guard.

Credentials are never read from profile files by this conftest. The
only two credential channels are (both require the gate):

1. the operator exports ``A2A_BEARER_TOKEN`` / ``A2A_PEER_TOKENS``
   themselves (forwarded only when the gate is set); or
2. a single, narrowly-scoped file the operator creates next to this
   conftest: ``tests/integration/.a2a_live_token`` (one line, the
   token; excluded from version control).

This closes the review finding (Amy, 2026-10-03, area B): the
conftest previously redirected HERMES_HOME to the authoring host's
sven profile and forwarded its bearer token whenever that profile
happened to exist, which is not portable and reaches real peer
config by default.
"""
from __future__ import annotations

import os
from pathlib import Path


_HERE = Path(__file__).resolve().parent
_LIVE_TOKEN_FILE = _HERE / ".a2a_live_token"
_GATE_ENV = "A2A_LIVE_PEER_PROFILE"
_PASS_THROUGH_KEYS = ("A2A_BEARER_TOKEN", "A2A_PEER_TOKENS", "A2A_HOST", "A2A_PORT", "A2A_LIVE_PEER")
_DEFAULT_HERMES_HOMES = ("", os.path.join(os.path.expanduser("~"), ".hermes"))


def _resolve_session_home() -> Path | None:
    """Which profile home to use for this pytest session.

    Resolves A2A_LIVE_PEER_PROFILE to an absolute directory. Relative
    values are resolved against the current working directory at test
    start (documented; the operator should pass an absolute path).
    Returns None when the gate is unset or points nowhere usable.
    """
    gate = os.environ.get(_GATE_ENV, "").strip()
    if not gate:
        return None
    candidate = Path(os.path.abspath(os.path.expanduser(gate)))
    if candidate.is_dir():
        return candidate
    return None


def pytest_configure(config):
    """Apply the live-peer gate BEFORE any test collection that may
    load the plugin (so module-level imports of ``a2a_async_plugin``
    see the right HERMES_HOME and don't pollute the operator's real
    home).
    """
    session_home = _resolve_session_home()

    if session_home is not None:
        # Gate is explicitly open: adopt the gated profile's identity.
        current = os.environ.get("HERMES_HOME", "")
        if current in _DEFAULT_HERMES_HOMES:
            os.environ["HERMES_HOME"] = str(session_home)

        # Credential channels, in order: operator-set env wins, then
        # the operator-created token file. Never invented, never read
        # from other profile config. The operator's exported values in
        # os.environ ARE the test env -- they are honored as-is.
        if not os.environ.get("A2A_BEARER_TOKEN") and not os.environ.get("A2A_PEER_TOKENS"):
            token = _load_token_file()
            if token:
                os.environ["A2A_BEARER_TOKEN"] = token
    # Gate closed: forward NOTHING. The (c) live tests resolve their
    # skip guards themselves (peer unresolvable / unreachable /
    # unauthed -> pytest.skip) and the (a)/(b) tests are hermetic and
    # do not need profile identity.


def _load_token_file() -> str | None:
    """Read the bearer token from the test-local token file (single
    line). Absent, unreadable or empty file -> None: the (c) tests
    then skip cleanly under their auth guard.
    """
    try:
        text = _LIVE_TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return text or None