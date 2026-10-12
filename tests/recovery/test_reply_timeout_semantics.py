"""
GitHub issue #1 / §3.2.2 blocking-send semantics.

Issue #1 (amy gateway, v0.1.8): a blocking ``message/send`` whose
session was busy for longer than 300s was false-FAILED with
``[agent did not reply in time]`` even though the agent was healthy
and still working the original task. ``TaskStore.complete()`` is
idempotent on terminal states, so nothing could ever repair the row.

Per A2A v1.0 §3.2.2, a blocking send MUST wait until the task reaches
a terminal (or interrupted) state. The fix (amy's spec-conformance
comment) removes the artificial cap from the blocking waiter:

* Default (``A2A_REPLY_TIMEOUT`` unset): the blocking path waits with
  NO cap; the wait is bounded only by connection lifetime (SSE
  keepalive death / gateway shutdown).
* Opt-in cap (``A2A_REPLY_TIMEOUT`` set): expiry yields CANCELED, not
  FAILED — the record asserts what actually happened (we stopped
  waiting and withdrew the task). FAILED would be a false assertion
  about the agent; CANCELED is truthful.
* Late real reply after a caller-side timeout resolves no waiter (the
  future is popped) and does NOT contradict the terminal record.

The subprocess profile-forward path keeps a hard bounded timeout
(``_dispatch_timeout``): expiry kills the child process, which truly
cancels the work, so its FAILED outcome is truthful.

These tests drive the REAL ``A2AAdapter`` and REAL ``TaskStore``
in-process (the same pattern as ``test_callee_restart.py``), with
``_loop`` stubbed to a loopback object that holds futures.
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from a2a_async_plugin import adapter as adapter_module
from a2a_async_plugin import protocol
from a2a_async_plugin.protocol import (
    STATE_CANCELED,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_WORKING,
)

from .test_callee_restart import _instantiate_adapter  # reuse the fixture helper


def _isolated_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    home = tmp_path / "hermes_home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_TASKS_DB", str(home / "a2a_tasks.db"))
    return home


# ──────────────────────────────────────────────────────────────────
# _reply_timeout / _dispatch_timeout parsing semantics.
# ──────────────────────────────────────────────────────────────────


def test_reply_timeout_unset_means_no_cap(monkeypatch):
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)
    assert adapter_module._reply_timeout() is None


def test_reply_timeout_set_returns_float(monkeypatch):
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "3.5")
    assert adapter_module._reply_timeout() == 3.5


def test_reply_timeout_garbage_falls_back_to_no_cap(monkeypatch):
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "not-a-number")
    assert adapter_module._reply_timeout() is None


def test_dispatch_timeout_defaults_even_when_reply_cap_unset(monkeypatch):
    """The subprocess path MUST stay bounded regardless of the waiter cap."""
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)
    assert adapter_module._dispatch_timeout() == 300.0
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "7")
    assert adapter_module._dispatch_timeout() == 7.0


# ──────────────────────────────────────────────────────────────────
# Blocking waiter semantics: no cap by default; cap cancels, never
# false-FAILs a healthy wait.
# ──────────────────────────────────────────────────────────────────


def test_blocking_wait_without_cap_holds_until_reply(monkeypatch, tmp_path):
    """§3.2.2 MUST-wait: with no A2A_REPLY_TIMEOUT the blocking waiter
    does not expire — it holds until the future resolves, however long
    that takes (we use ~1s to prove >300s-default semantics in
    miniature: a cap of even 0.5s would have fired FAILED here).
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)

    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("rw-nocap", "ctx-rw-nocap", peer="peer-rw",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"],
            "context_id": rec["context_id"],
            "peer": "peer-rw",
            "future": fut,
            "created_iso": rec["created_iso"],
            "started": time.time(),
        }

        def _late_reply():
            time.sleep(1.0)
            adp._resolve_task(rec["task_id"], STATE_COMPLETED, "finally done")

        t = threading.Thread(target=_late_reply)
        t.start()
        state, reply = adp._await_reply(pending)
        t.join()
        assert state == STATE_COMPLETED
        assert reply == "finally done"
    finally:
        adp._watchdog_stop.set()


def test_blocking_wait_with_optin_cap_cancels_never_fails(monkeypatch, tmp_path):
    """Opt-in cap expiry yields CANCELED with an honest marker — NOT the
    issue #1 false FAILED (the agent may still be working; FAILED would
    assert an outcome that never happened).
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "0.5")

    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("rw-cap", "ctx-rw-cap", peer="peer-rw",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"],
            "context_id": rec["context_id"],
            "peer": "peer-rw",
            "future": fut,
            "created_iso": rec["created_iso"],
            "started": time.time() - 5.0,  # cap already in the past
        }
        state, reply = adp._await_reply(pending)
        assert state == STATE_CANCELED, f"expected CANCELED, got {state}"
        # Honest marker only: must NOT assert a false agent failure state.
        assert reply not in ("", "[agent processing failed]")
    finally:
        adp._watchdog_stop.set()


def test_capped_expiry_finalizes_as_canceled_row(monkeypatch, tmp_path):
    """The capped expired blocking send persists a CANCELED row, not FAILED."""
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "0.5")

    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("rw-cap-row", "ctx-rw-row", peer="peer-rw",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"],
            "context_id": rec["context_id"],
            "peer": "peer-rw",
            "future": fut,
            "created_iso": rec["created_iso"],
            "started": time.time() - 5.0,
        }
        state, reply = adp._await_reply(pending)
        state, reply = adp._finalize_task(pending, state, reply)

        after = adp.tasks.get(rec["task_id"])
        assert after["state"] == STATE_CANCELED
        assert after["completed_at"] is not None
    finally:
        adp._watchdog_stop.set()


def test_late_real_reply_after_capped_expiry_does_not_contradict_row(
    monkeypatch, tmp_path
):
    """Core issue #1 repair property: after the capped caller gives up
    (CANCELED), the agent eventually answers — and the late reply must
    NOT overwrite the terminal record. The future is popped with the
    waiter, so ``_resolve_oldest_for_context`` finds nothing and drops.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "0.5")

    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("rw-late", "ctx-rw-late", peer="peer-rw",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"],
            "context_id": rec["context_id"],
            "peer": "peer-rw",
            "future": fut,
            "created_iso": rec["created_iso"],
            "started": time.time() - 5.0,
        }
        state, reply = adp._await_reply(pending)
        state, reply = adp._finalize_task(pending, state, reply)
        assert state == STATE_CANCELED

        # The late real reply arrives after the caller stopped waiting.
        resolved = adp._resolve_oldest_for_context(rec["context_id"], STATE_COMPLETED, "the real answer")
        assert not resolved, "late reply resolved a popped waiter"

        # The terminal record stands, uncontradicted.
        after = adp.tasks.get(rec["task_id"])
        assert after["state"] == STATE_CANCELED
        assert after["reply"] != "the real answer"
    finally:
        adp._watchdog_stop.set()


def test_genuine_failure_still_reports_failed(monkeypatch, tmp_path):
    """A waiter that observes an exception (real agent failure via
    disconnect/shutdown resolution) still yields FAILED — only cap
    expiry is CANCELED. We drive this with the real shutdown path.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)

    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("rw-fail", "ctx-rw-fail", peer="peer-rw",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"],
            "context_id": rec["context_id"],
            "peer": "peer-rw",
            "future": fut,
            "created_iso": rec["created_iso"],
            "started": time.time(),
        }

        # The real shutdown path resolves the pending future with FAILED.
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(adp.disconnect())
        finally:
            loop.close()

        state, reply = adp._await_reply(pending)
        assert state == STATE_FAILED
        assert reply == "[agent shutting down]"
    finally:
        adp._watchdog_stop.set()


# ──────────────────────────────────────────────────────────────────
# SSE variant: no cap still detects client disconnect through keepalive.
# ──────────────────────────────────────────────────────────────────


def test_sse_wait_detects_client_disconnect_without_cap(monkeypatch, tmp_path):
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)

    adp = _instantiate_adapter(port=_free_port())
    _SSE_KEEPALIVE_ORIG = adapter_module._SSE_KEEPALIVE
    adapter_module._SSE_KEEPALIVE = 0.2
    try:
        rec = adp.tasks.create("rw-sse", "ctx-rw-sse", peer="peer-rw",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"],
            "context_id": rec["context_id"],
            "peer": "peer-rw",
            "future": fut,
            "created_iso": rec["created_iso"],
            "started": time.time(),
        }
        state, reply = adp._await_reply(pending, keepalive=lambda: (_ for _ in ()).throw(RuntimeError("client gone")))
        assert state == STATE_CANCELED
        assert "disconnect" in reply.lower() or "client" in reply.lower()
    finally:
        adapter_module._SSE_KEEPALIVE = _SSE_KEEPALIVE_ORIG
        adp._watchdog_stop.set()


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()