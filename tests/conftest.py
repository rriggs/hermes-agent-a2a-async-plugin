"""
Top-level conftest for the entire ``tests/`` tree.

The Phase 1 launcher harness (``gateway``, ``isolated_profile``,
``free_port``) is defined in ``tests/recovery/conftest.py`` and is
the single source of truth for those fixtures. The conformance
test suite (``tests/conformance/``) is a sibling directory that
needs the same fixtures.

Pytest auto-discovers ``conftest.py`` files walking DOWN the
directory tree, so the recovery fixtures are NOT visible from
``tests/conformance/`` by default. We bridge this by importing
the recovery conftest here and re-decorating the fixtures in
this conftest's namespace, which is auto-discovered from this
directory level. The recovery conftest remains the source of
truth — this file just re-exports the same fixture objects.

The plan permits modifying this file: Task 1's acceptance
("Modify only if needed: tests/conftest.py") covers exactly this
case — Phase 6 needs the harness visible to a sibling test
directory.

We deliberately do NOT use ``pytest_plugins = ["tests.recovery.conftest"]``
because pytest auto-discovers ``tests/recovery/conftest.py``
when it walks the recovery directory, which causes a "Plugin
already registered" collection error. Importing the module
object directly (without registering it as a plugin) sidesteps
the conflict.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


# Resolve the recovery conftest path once at import time.
_HERE = Path(__file__).resolve().parent
_RECOVERY_CONFTEST = _HERE / "recovery" / "conftest.py"
_RECOVERY_MODULE_NAME = "_tests_recovery_conftest_singleton"


def _import_recovery_conftest() -> object | None:
    """Import the recovery conftest as a plain module and cache it.

    Returns the module object, or None if the recovery conftest
    cannot be loaded (in which case the fixtures below simply
    won't be re-exported and conformance tests that need them
    will fail with a clear fixture-not-found error).
    """
    if _RECOVERY_MODULE_NAME in sys.modules:
        return sys.modules[_RECOVERY_MODULE_NAME]
    if not _RECOVERY_CONFTEST.exists():
        return None
    spec = importlib.util.spec_from_file_location(
        _RECOVERY_MODULE_NAME, str(_RECOVERY_CONFTEST)
    )
    if spec is None or spec.loader is None:
        return None
    try:
        module = importlib.util.module_from_spec(spec)
        sys.modules[_RECOVERY_MODULE_NAME] = module
        spec.loader.exec_module(module)
        return module
    except Exception:
        # If the harness fails to load (e.g. Hermes source tree
        # missing or .venv not present), the conformance tests
        # will surface a clear fixture-not-found error. We do
        # not crash collection here.
        sys.modules.pop(_RECOVERY_MODULE_NAME, None)
        return None


# Import the recovery conftest and re-export the harness fixtures
# in this conftest's namespace so pytest's auto-discovery picks
# them up for the entire tests/ tree (including the conformance
# sibling).
#
# A pytest ``Fixture`` object is itself callable (it implements
# ``__call__``); assigning it to a new name in this conftest's
# module namespace is enough for pytest's plugin manager to
# register the fixture for tests in any subdirectory. We do
# NOT re-decorate with ``@pytest.fixture`` — pytest raises
# "fixture applied more than once" if the same function is
# wrapped twice.
_recovery = _import_recovery_conftest()
if _recovery is not None:
    gateway = _recovery.gateway
    isolated_profile = _recovery.isolated_profile
    free_port = _recovery.free_port


# ──────────────────────────────────────────────────────────────────
# Per-test env restoration. See tests/recovery/NOTES.md #36 and #38.
# ──────────────────────────────────────────────────────────────────

import pytest  # imported after the module-level fixture re-exports
import os as _os  # local alias to keep the import block tidy

# Env keys the recovery teardown pops and test configs may set
# process-wide. Restored around every test (review finding: the
# restore previously covered only HERMES_HOME while credential keys
# stayed asymmetric and latent).
_RESTORED_ENV_KEYS = ("HERMES_HOME", "A2A_BEARER_TOKEN", "A2A_PEER_TOKENS")


@pytest.fixture(autouse=True)
def _restore_hermes_home_per_test():
    """Snap the session-shaping env keys back to their pre-test
    values after each test in this ``tests/`` tree runs.

    The recovery-harness teardown (in ``isolated_profile``) pops
    ``HERMES_HOME`` from ``os.environ``. Without this restore, a
    sibling-directory test that runs after a recovery test sees a
    missing HERMES_HOME and the operator's peer config doesn't
    resolve. The session-start integration conftest only sets
    HERMES_HOME once; it does not re-set after the recovery tests
    pop it. ``A2A_BEARER_TOKEN`` / ``A2A_PEER_TOKENS`` are restored
    for symmetry: today only the integration conftest sets them,
    but a future test that does so must not leak into neighbors.

    Because this is an autouse fixture at the ``tests/`` level (not
    the conformance-only level), it covers every test in every
    subdirectory that pytest auto-discovers below this conftest.
    """
    saved = {key: _os.environ.get(key, "") for key in _RESTORED_ENV_KEYS}
    yield
    for key, value in saved.items():
        if value and key not in _os.environ:
            _os.environ[key] = value
        elif not value and key in _os.environ:
            # Pre-test state was "absent" (e.g. recovery teardown or
            # a closed live-peer gate) -- keep it absent instead of
            # letting a prior test's set leak forward.
            _os.environ.pop(key, None)
