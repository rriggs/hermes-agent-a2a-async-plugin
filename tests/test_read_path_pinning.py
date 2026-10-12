"""GitHub review finding 2 (issue #3 follow-through, amy review of v0.1.9):

On a multiplexed gateway, `a2a_history` and `a2a_list kind=conversations`
do NOT take a dir override — they resolve the conversation dir contextually
via `_conv_dir()`. If a tool handler executes OUTSIDE the profile's runtime
scope (the ContextVar gap that split the artifacts in v0.1.8), reads miss
the adapter-pinned transcripts even though writes landed correctly.

Guarded here, in a real subprocess whose environment simulates a gateway
worker WITHOUT the profile ContextVar but WITH the documented deploy knob:

1. The adapter writes a transcript under the profile-scoped dir (init
   capture — pinned explicitly at persist, issue #3's mechanism).
2. `tools.a2a_history` / `tools.a2a_list kind=conversations` — invoked
   with NO dir argument, like a live tool handler — resolve the SAME dir
   the adapter pinned (env override), and read the written transcript.
3. The framed replay of the peer turn comes back through the full
   subprocess path (issue #5's guarantee end-to-end).

Also guards the invariant that, without the knob, tools and adapter agree
in a single-profile deployment where HERMES_HOME is the same context.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HERMES_SRC = os.environ.get("HERMES_SRC", "/home/hermes/.hermes/hermes-agent")

_PROBE = textwrap.dedent("""
    import json, os, sys
    from pathlib import Path

    repo = sys.argv[1]
    mode = sys.argv[2]          # "knob" | "shared"
    sys.path.insert(0, repo)
    if os.environ.get("HERMES_SRC"):
        sys.path.insert(0, os.environ["HERMES_SRC"])

    from a2a_async_plugin import protocol
    from a2a_async_plugin import tools as a2a_tools
    from a2a_async_plugin import adapter as adapter_mod

    home = os.environ["HERMES_HOME"]
    profile_conv_dir = Path(home) / "a2a_conversations"

    if mode == "knob":
        # Simulate the multiplex gap: the tool handler's ambient context
        # resolves a DIFFERENT hermes home than the adapter's scope.
        os.environ["HERMES_HOME"] = str(Path(home).parent / "NOT-THE-PROFILE")
        # The documented deploy knob makes the resolution deterministic
        # for BOTH writer and reader regardless of scope.
        os.environ["A2A_CONVERSATIONS_DIR"] = str(profile_conv_dir)

    # Adapter init captures the dir (same resolution as the task DB root's
    # context on a real gateway).
    captured = adapter_mod._profile_scoped_conversations_dir()
    assert captured == profile_conv_dir, f"adapter capture wrong: {captured}"

    # Write the way the inbound adapter does (callee inbound peer turn,
    # filtered + peer-attributed at persist).
    protocol.persist_message("ctx-probe-1", "user",
                             "hello from the adapter", "task-probe-1",
                             peer="dean", conv_dir=captured)

    # And the way the caller tools do (local outbound turn, no peer).
    protocol.persist_message("ctx-probe-1", "agent", "reply text",
                             "task-probe-1", conv_dir=captured)

    # Now the reader: a tool handler with NO dir argument — exactly how
    # a2a_history / a2a_list invoke them in production.
    msgs = protocol.load_conversation("ctx-probe-1", limit=10)
    assert len(msgs) == 2, f"tool read missed the adapter-pinned transcript: {msgs!r}"

    listed = protocol.list_conversations()
    assert "ctx-probe-1" in listed, f"a2a_list read missed the pinned dir: {listed!r}"

    # Full framed replay through the tool itself.
    out = a2a_tools.a2a_history({"context_id": "ctx-probe-1"})
    assert "[peer:dean" in out and "untrusted remote content" in out, out
    # The LOCAL turn stays unframed.
    assert "[agent] reply text" in out, out
    print("PROBE_OK")
""")


def _run_probe(tmp_path: Path, mode: str, monkeypatch: pytest.MonkeyPatch) -> subprocess.CompletedProcess:
    monkeypatch.delenv("A2A_CONVERSATIONS_DIR", raising=False)
    # The subprocess gets a hermetic HERMES_HOME (a profile home shape).
    env = dict(os.environ)
    env["HERMES_HOME"] = str(tmp_path / "profile_home")
    env["HERMES_SRC"] = HERMES_SRC
    Path(env["HERMES_HOME"]).mkdir(parents=True, exist_ok=True)
    script = tmp_path / f"probe_{mode}.py"
    script.write_text(_PROBE)
    return subprocess.run(
        [sys.executable, str(script), str(ROOT), mode],
        capture_output=True, text=True, env=env, timeout=120,
    )


def test_tools_read_the_adapter_pinned_dir_via_env_knob(monkeypatch, tmp_path):
    """Multiplex-gap simulation: the reader's ambient hermes home differs
    from the profile; the A2A_CONVERSATIONS_DIR knob pins reads to the
    profile dir where the adapter wrote."""
    result = _run_probe(tmp_path, "knob", monkeypatch)
    assert result.returncode == 0, (
        f"probe failed (rc={result.returncode}):\nstdout:{result.stdout}\nstderr:{result.stderr[-2000:]}"
    )
    assert "PROBE_OK" in result.stdout


def test_tools_and_adapter_agree_in_shared_single_profile_home(monkeypatch, tmp_path):
    """Single-profile deployment: no knob needed; HERMES_HOME is the same
    context for adapter capture and tool reads."""
    result = _run_probe(tmp_path, "shared", monkeypatch)
    assert result.returncode == 0, (
        f"probe failed (rc={result.returncode}):\nstdout:{result.stdout}\nstderr:{result.stderr[-2000:]}"
    )
    assert "PROBE_OK" in result.stdout