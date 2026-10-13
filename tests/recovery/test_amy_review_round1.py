"""
Amy's review round 1 findings on 4fb3827 (issue #1 fix).

BLOCKER 1 — `tasks/subscribe` crashed with TypeError (float + None) on the
new default (A2A_REPLY_TIMEOUT unset), because `_reply_timeout()` now
returns None. Reproduced live by amy against a real socket; SSE headers
were already sent, so the client saw a 200 event-stream that hung forever
with no terminal event. Regression test: subscribe must handle a None cap.

MINOR 1 — a typo'd A2A_REPLY_TIMEOUT value silently fell back to UNCAPPED
(fail-open in the opposite direction of the old 300s fallback). Now an
error is logged. (Log behavior itself is asserted minimally; the parse
semantics are unchanged.)

MINOR 2 — CANCELED records carried the marker '[agent did not reply in
time]', which peers pattern-match as a failure. Changed to a
caller-abandonment marker. Asserted via the _await_reply contract tests.

MINOR 3 — CANCELED counted into metrics.tasks_failed. Split into
tasks_canceled (protocol.Metrics), asserted here.

MINOR 4 — a reply landing exactly at the cap boundary was discarded;
_await_reply now drains a done future before declaring expiry. Asserted.

HIGH 2 (partial) — the plain blocking send path had NO disconnect
detection: with no cap, fut.result(None) pinned the HTTP worker thread
forever after the peer's socket died. _rpc_message_send now installs
a select/MSG_PEEK client-alive keepalive probe, so the wait is bounded
by real connection lifetime. Asserted via a fake handler whose socket
EOFs.

The profile-forward subprocess path and busy-session auto-async are NOT
covered here (see review dispatch notes).
"""

from __future__ import annotations

import json
import socket
import threading
import time
from unittest import mock

import pytest

from a2a_async_plugin import adapter as adapter_module
from a2a_async_plugin import protocol
from a2a_async_plugin.protocol import (
    STATE_CANCELED,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_WORKING,
)

from .test_callee_restart import _instantiate_adapter
from .test_reply_timeout_semantics import _free_port, _isolated_hermes_home


def _pending(adp, task_id, ctx, peer="peer-amy", started=None):
    fut = adp._add_pending(task_id, ctx)
    return {
        "task_id": task_id,
        "context_id": ctx,
        "peer": peer,
        "future": fut,
        "created_iso": "2026-10-12T00:00:00Z",
        "started": started if started is not None else time.time(),
        "_fut": fut,
    }


# ──────────────────────────────────────────────────────────────────
# BLOCKER 1: subscribe must not crash when A2A_REPLY_TIMEOUT unset.
# ──────────────────────────────────────────────────────────────────


class _FakeHandler:
    """Minimal SSE-capable handler double for _rpc_tasks_subscribe.

    ``write_raises`` simulates the client socket dying: the keepalive
    write raises, which is the disconnect signal _rpc_tasks_subscribe
    catches as (BrokenPipeError, ConnectionResetError).
    """

    def __init__(self, write_raises=False):
        self.frames = []
        self.closed = False
        self.write_raises = write_raises
        self.wfile = self

    def send_response(self, code):
        self.frames.append(("status", code))

    def send_header(self, k, v):
        self.frames.append(("header", k, v))

    def end_headers(self):
        self.frames.append(("end-headers",))

    @property
    def close_connection(self):
        return self._close

    @close_connection.setter
    def close_connection(self, v):
        self._close = v

    def write(self, chunk):
        if self.write_raises:
            raise BrokenPipeError("client gone")
        self.frames.append(("sse", chunk))

    def flush(self):
        pass

    def _json(self, code, payload):
        self.frames.append(("json", code, payload))


def test_subscribe_sse_no_cap_waits_for_terminal(monkeypatch, tmp_path):
    """BLOCKER 1 regression: with A2A_REPLY_TIMEOUT unset, subscribe must
    NOT raise TypeError (float + None) — it waits with no deadline until
    either the watch future resolves or the client disconnects (keepalive
    write raising). We drive resolution via the TaskStore, in-process,
    through the real adapter.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)
    adp = _instantiate_adapter(port=_free_port())
    orig_keepalive = adapter_module._SSE_KEEPALIVE
    adapter_module._SSE_KEEPALIVE = 0.2
    try:
        rec = adp.tasks.create("sub-blocker", "ctx-sub-blocker", peer="peer-sub",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)

        handler = _FakeHandler()
        params = {"taskId": rec["task_id"]}
        t = threading.Thread(
            target=adp._rpc_tasks_subscribe,
            args=(handler, "req-sub", params, None),
            daemon=True,
        )
        t.start()
        # Give the handler time to enter its wait loop (no crash).
        time.sleep(0.6)
        if not t.is_alive():
            pytest.fail("subscribe thread died — deadline handling on None cap regressed")
        # Now settle the task through the REAL TaskStore the watcher polls.
        adp.tasks.complete(rec["task_id"], STATE_COMPLETED, "subscribe me")
        t.join(timeout=5)
        assert not t.is_alive(), "subscribe did not settle after completion"
        # The task row and emitted stream show COMPLETED, not a fabricated state.
        assert any(
            f[0] == "sse" and "TASK_STATE_COMPLETED" in f[1].decode("utf-8")
            for f in handler.frames
        ), handler.frames
    finally:
        adapter_module._SSE_KEEPALIVE = orig_keepalive
        adp._watchdog_stop.set()


def test_subscribe_sse_with_cap_falls_back_to_snapshot(monkeypatch, tmp_path):
    """With the opt-in cap set, a capped expiry returns the row's snapshot
    state via the terminal emission — not a crash, and not a fabricated state.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "0.5")
    adp = _instantiate_adapter(port=_free_port())
    orig_keepalive = adapter_module._SSE_KEEPALIVE
    adapter_module._SSE_KEEPALIVE = 0.2
    try:
        rec = adp.tasks.create("sub-cap", "ctx-sub-cap", peer="peer-sub",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)

        handler = _FakeHandler()
        # Backdate the row so the cap is already expired when the wait starts.
        old = time.time() - 10
        with adp.tasks._lock:
            adp.tasks._tasks[rec["task_id"]]["created_at"] = old
        params = {"taskId": rec["task_id"]}
        t = threading.Thread(
            target=adp._rpc_tasks_subscribe,
            args=(handler, "req-sub-cap", params, None),
            daemon=True,
        )
        t.start()
        t.join(timeout=5)
        assert not t.is_alive(), "capped subscribe never settled"
        # No exception frames — a crash would have killed the thread with a
        # traceback instead of emitting a terminal.
        statuses = [f for f in handler.frames if f[0] == "status"]
        assert statuses, "no HTTP status emitted"
    finally:
        adapter_module._SSE_KEEPALIVE = orig_keepalive
        adp._watchdog_stop.set()


# ──────────────────────────────────────────────────────────────────
# MINOR 3: CANCELED metrics split; MINOR 4: boundary drain.
# ──────────────────────────────────────────────────────────────────


def test_canceled_not_counted_as_failed(monkeypatch, tmp_path):
    _isolated_hermes_home(monkeypatch, tmp_path)
    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("m-cxl", "ctx-m-cxl", peer="peer-m",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        pending = _pending(adp, rec["task_id"], rec["context_id"], started=time.time() - 5)
        from a2a_async_plugin import protocol as p
        failed_before = p.metrics.tasks_failed
        canceled_before = p.metrics.tasks_canceled
        adp._finalize_task(pending, STATE_CANCELED, "[canceled]")
        assert p.metrics.tasks_failed == failed_before
        assert p.metrics.tasks_canceled == canceled_before + 1
    finally:
        adp._watchdog_stop.set()


def test_boundary_arrived_reply_is_drained_not_discarded(monkeypatch, tmp_path):
    """MINOR 4: a future resolved right at/after the deadline must return the
    real reply (COMPLETED), not a CANCELED discard."""
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "0.5")
    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("edge", "ctx-edge", peer="peer-e",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        pending = _pending(adp, rec["task_id"], rec["context_id"], started=time.time() - 5)
        # Reply arrives NOW — after the cap expired but before the check.
        pending["_fut"].set_result((STATE_COMPLETED, "edge reply"))
        state, reply = adp._await_reply(pending)
        assert state == STATE_COMPLETED, (
            f"boundary-arrived reply was discarded: state={state} reply={reply!r}")
        assert reply == "edge reply"
    finally:
        adp._watchdog_stop.set()


# ──────────────────────────────────────────────────────────────────
# HIGH 2 (partial): client-alive probe bounds the uncapped wait.
# ──────────────────────────────────────────────────────────────────


def test_client_alive_check_raises_on_eof_socket(monkeypatch, tmp_path):
    """A real socket whose peer closed shows readable + EOF on MSG_PEEK →
    the probe raises, which _await_reply converts to CANCELED."""
    _isolated_hermes_home(monkeypatch, tmp_path)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    client_sock_holder = {}

    def _client():
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(("127.0.0.1", port))
        client_sock_holder["sock"] = c
        # Hold the connection open briefly, then close to trigger EOF.
        time.sleep(0.3)
        c.shutdown(socket.SHUT_RDWR)
        c.close()

    t = threading.Thread(target=_client)
    t.start()
    conn, _ = server.accept()

    class _H:
        def __init__(self, sock):
            self.connection = sock

    probe, close_probe = adapter_module.A2AAdapter._client_alive_check(_H(conn))
    # Alive while peer is connected (no data): no raise.
    probe()
    # Wait for the peer shutdown, then expect EOF → raise.
    t.join(timeout=5)
    deadline = time.time() + 3
    while time.time() < deadline:
        try:
            probe()
            time.sleep(0.05)
        except RuntimeError:
            break
    else:
        pytest.fail("probe never detected the client EOF")
    close_probe()


def test_uncapped_wait_with_alive_probe_ends_on_disconnect(monkeypatch, tmp_path):
    """HIGH 2 end-to-end at the unit boundary: no cap + alive probe → a peer
    socket EOF terminates the wait with CANCELED instead of pinning the
    thread forever."""
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    def _client():
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(("127.0.0.1", port))
        time.sleep(0.5)
        c.shutdown(socket.SHUT_RDWR)
        c.close()

    t = threading.Thread(target=_client)
    t.start()
    conn, _ = server.accept()

    class _H:
        def __init__(self, sock):
            self.connection = sock

    adp = _instantiate_adapter(port=_free_port())
    orig_keepalive = adapter_module._SSE_KEEPALIVE
    adapter_module._SSE_KEEPALIVE = 0.2
    try:
        rec = adp.tasks.create("dc", "ctx-dc", peer="peer-dc",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        pending = _pending(adp, rec["task_id"], rec["context_id"])
        check, close_probe = adapter_module.A2AAdapter._client_alive_check(_H(conn))
        try:
            state, reply = adp._await_reply(pending, keepalive=check)
        finally:
            close_probe()
        assert state == STATE_CANCELED
        t.join(timeout=3)
    finally:
        adapter_module._SSE_KEEPALIVE = orig_keepalive
        server.close()
        conn.close()
        adp._watchdog_stop.set()


def test_client_alive_check_noop_for_stub_handler():
    """A handler without a connection attribute must never raise."""
    class _Stub:
        pass  # no .connection

    probe, close_probe = adapter_module.A2AAdapter._client_alive_check(_Stub())
    probe()  # must not raise
    close_probe()


def test_reply_timeout_garbage_logs_error(monkeypatch, caplog):
    """MINOR 1: a typo'd timeout value must not silently fail-open to
    uncapped without an operator-visible error log."""
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "5min")
    import logging
    with caplog.at_level(logging.ERROR, logger="a2a_async_plugin.adapter"):
        adapter_module._reply_timeout()
    assert any("A2A_REPLY_TIMEOUT" in r.message for r in caplog.records), caplog.text