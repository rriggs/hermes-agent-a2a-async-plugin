"""Resolve the real core publisher for the cross-repo tests.

The full suite also imports an operator's Hermes install: ``tests/integration`` loads this plugin
into the installed tree's plugin loader. When two different Hermes trees are imported into one
process, ``agent.plugin_completions`` comes from this checkout while its storage layer
(``hermes_state_common``) may come from the other one, so capture fails with "state.db cannot store
plugin completion targets" -- a test-process artifact, not a product defect. Skip with that
explanation instead of reporting a false failure; run the test alone to exercise it.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def load_real_publisher():
    """Return ``agent.plugin_completions`` when this process holds exactly one Hermes tree."""
    pc = pytest.importorskip("agent.plugin_completions")
    try:
        import hermes_state_common as state_common
    except Exception:
        pytest.skip("Hermes state storage is not importable on this host")
    checkout = Path(pc.__file__).resolve().parents[1]
    if checkout != Path(state_common.__file__).resolve().parent:
        pytest.skip(
            "two Hermes trees were imported into this test process (the suite loads the "
            "operator's install); run this test alone with PYTHONPATH=<checkout>")
    return pc
