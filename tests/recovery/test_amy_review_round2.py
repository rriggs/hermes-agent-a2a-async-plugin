"""
Amy's review round 2 findings on PR #7 (commit 5756177).

MAJOR 1 — the client-alive probe used MSG_DONTWAIT:
  (a) it does not exist on Windows (plugin.yaml declares windows), and
  (b) on Linux it made recv non-blocking, so a legal HTTP keep-alive
      follow-up (a second request buffered on the socket) showed up as
      readable-with-data at the first keepalive tick and was misread:
      the wait was falsely CANCELED for live keep-alive clients.
  Fix: sock.setblocking(False) ONCE at probe creation + plain MSG_PEEK
  recv. EOF (b'') = disconnect; a pending byte = alive (left unconsumed);
  BlockingIOError = alive and idle. Portable, no(MSG_DONTWAIT).

MAJOR 2 — the round-1 CANCELED marker still embedded the old failure
marker as a substring ('[agent did not reply in time; task canceled by
caller]' contains '[agent did not reply in time]', which the plugin's
own _failed_profile_reply() substring-matches). Fix: markers reworded to
'[canceled by caller: ...]' with no overlap with any failure marker,
asserted by test.

Minor riders also fixed per amy: ValueError (closed socket object, fd -1
raises ValueError from select, not OSError) has its own probe branch; the
conformance row's stale line-number anchors switched to symbol references.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from a2a_async_plugin import adapter as adapter_module
from a2a_async_plugin.protocol import STATE_CANCELED, STATE_COMPLETED, STATE_WORKING

from .test_reply_timeout_semantics import _free_port, _isolated_hermes_home
from .test_callee_restart import _instantiate_adapter


# ──────────────────────────────────────────────────────────────────
# MAJOR 1 — probe mechanism: portable, keep-alive-safe.
# ──────────────────────────────────────────────────────────────────


def _pair_with_peer(close_after: float = 0.4, send_byte: bool = False):
    """Socketpair with a controlled peer. Returns (conn, peer, thread)."""
    a, b = socket.socketpair()
    def _peer():
        time.sleep(close_after)
        if send_byte:
            try:
                b.send(b"X")  # a 'follow-up request' byte
            except OSError:
                pass
        else:
            try:
                b.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        b.close()
    t = threading.Thread(target=_peer)
    t.start()
    return a, b, t


class _H:
    def __init__(self, sock):
        self.connection = sock


def test_probe_detects_eof_even_without_msg_dontwait(monkeypatch):
    """MAJOR 1 (a) regression guard: the probe must work with MSG_DONTWAIT
    deleted from the socket module (the Windows shape). If the code
    references the constant, this test fails with AttributeError."""
    monkeypatch.delattr(socket, "MSG_DONTWAIT", raising=False)
    conn, peer, t = _pair_with_peer(close_after=0.3)
    try:
        probe = adapter_module.A2AAdapter._client_alive_check(_H(conn))
        # Alive while peer connected: no raise, no false cancel.
        probe()
        t.join(timeout=3)
        deadline = time.time() + 3
        while time.time() < deadline:
            try:
                probe()
                time.sleep(0.05)
            except RuntimeError:
                break
        else:
            pytest.fail("probe never detected peer EOF without MSG_DONTWAIT")
    finally:
        conn.close()


def test_probe_does_not_cancel_live_keepalive_traffic():
    """MAJOR 1 (b) regression: a buffered follow-up byte on a KEEP-ALIVE
    socket (peer alive, one extra request buffered) must NOT be read as a
    disconnect. recv(MSG_PEEK) sees data (not EOF) → the probe stays
    silent and the byte is left unconsumed for the HTTP layer."""
    conn, peer, t = _pair_with_peer(close_after=0.3, send_byte=True)
    try:
        probe = adapter_module.A2AAdapter._client_alive_check(_H(conn))
        t.join(timeout=3)
        time.sleep(0.2)
        # Must NOT raise across several ticks.
        for _ in range(20):
            probe()
            time.sleep(0.02)
        # The byte was peeked but not consumed — the HTTP layer still has it.
        assert conn.recv(1, socket.MSG_PEEK) == b"X"
    finally:
        conn.close()


def test_probe_idle_blocking_socket_does_not_stall(monkeypatch, tmp_path):
    """The probe must not block the waiter on an idle BLOCKABLE socket:
    setblocking(False) is applied at probe creation, so MSG_PEEK on an
    idle socket raises BlockingIOError immediately (handled as alive)."""
    _isolated_hermes_home(monkeypatch, tmp_path)
    a, b = socket.socketpair()
    try:
        b.send(b"Z")  # data buffered: socket is readable
        probe = adapter_module.A2AAdapter._client_alive_check(_H(a))
        start = time.time()
        probe()  # must return fast, not block on recv
        elapsed = time.time() - start
        assert elapsed < 1.0, f"probe blocked {elapsed:.2f}s on readable socket"
    finally:
        a.close()
        b.close()


def test_probe_closed_socket_object_raises(monkeypatch, tmp_path):
    """ amy's minor: select on a CLOSED socket object raises ValueError
    (fd −1), not OSError. The probe handles ValueError explicitly and
    raises RuntimeError (the disconnect contract)."""
    a, b = socket.socketpair()
    probe = adapter_module.A2AAdapter._client_alive_check(_H(a))
    a.close()  # close the Python object: select now raises ValueError
    try:
        with pytest.raises(RuntimeError):
            probe()
    finally:
        b.close()


# ──────────────────────────────────────────────────────────────────
# MAJOR 2 — CANCELED markers carry no failure-marker substring.
# ──────────────────────────────────────────────────────────────────


def test_canceled_markers_do_not_match_failure_classifier():
    """MAJOR 2 regression: every CANCELED marker used by _await_reply must
    NOT be classified as failure by _failed_profile_reply (substring
    matching), including when the reply text round-trips through the
    profile-forward classifier."""
    canceled_markers = [
        "[canceled by caller: reply timeout expired]",
        "[canceled by caller: blocked send abandoned, reply timeout expired]",
        "[client disconnected]",
    ]
    for marker in canceled_markers:
        assert not adapter_module._failed_profile_reply(marker), (
            f"CANCELED marker {marker!r} is substring-matched as failure")
    # And the failure classifier still catches the REAL failure markers.
    assert adapter_module._failed_profile_reply("[agent did not reply in time]")
    assert adapter_module._failed_profile_reply("[profile did not reply in time]")


def test_await_reply_canceled_message_has_no_failure_marker(monkeypatch, tmp_path):
    """The CANCELED text produced at cap expiry must not contain any
    _failed_profile_reply marker substring."""
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "0.3")
    adp = _instantiate_adapter(port=_free_port())
    try:
        rec = adp.tasks.create("r2-cxl", "ctx-r2-cxl", peer="peer-r2",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"], "context_id": rec["context_id"],
            "peer": "peer-r2", "future": fut,
            "created_iso": rec["created_iso"], "started": time.time() - 5,
        }
        state, reply = adp._await_reply(pending)
        assert state == STATE_CANCELED
        assert not adapter_module._failed_profile_reply(reply), reply
        assert "canceled by caller" in reply, reply
    finally:
        adp._watchdog_stop.set()