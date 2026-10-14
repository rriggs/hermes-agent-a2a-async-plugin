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


def _probe(conn):
    """Unpack the (check, close) probe tuple; round-3 API."""
    check, close = adapter_module.A2AAdapter._client_alive_check(_H(conn))
    return check, close


def test_probe_detects_eof_even_without_msg_dontwait(monkeypatch):
    """MAJOR 1 (a) regression guard: the probe must work with MSG_DONTWAIT
    deleted from the socket module (the Windows shape). If the code
    references the constant, this test fails with AttributeError."""
    monkeypatch.delattr(socket, "MSG_DONTWAIT", raising=False)
    conn, peer, t = _pair_with_peer(close_after=0.3)
    try:
        probe, _close = _probe(conn)
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
        probe, _close = _probe(conn)
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
        probe, _close = _probe(a)
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
    probe, _close = _probe(a)
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


# ──────────────────────────────────────────────────────────────────
# ROUND 3 — REAL HTTP LAYER recovery tests. The socketpair unit tests
# structurally cannot see the two round-3 blockers (both come from the
# probe mutating the LIVE handler socket that BaseHTTPRequestHandler
# continues to use after the wait). Drive a REAL BaseHTTPRequestHandler
# through a real TCP connection.
# ──────────────────────────────────────────────────────────────────


def _fake_adapter_on_real_http():
    """A real ThreadingHTTPServer + BaseHTTPRequestHandler whose GET /
    triggers the probe path and responds with a LARGE body afterward —
    the round-3 blocker-1 shape (large unbuffered write after the probe
    ran). Returns (server, adapter, handler_obj_holder)."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class _MiniAdapter:
        """Just enough surface: _client_alive_check is a staticmethod the
        handler can call exactly as _rpc_message_send does."""
        _client_alive_check = staticmethod(adapter_module.A2AAdapter._client_alive_check)

    holder = {}
    probe_re = {}  # set when request handled

    class _H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive: reuse the connection

        def do_GET(self):  # noqa: N802
            adapter = _MiniAdapter()
            # EXACTLY the call shape in _rpc_message_send's blocking wait.
            check, close_probe = adapter._client_alive_check(self)
            try:
                # Simulate the blocked wait: probe ticks for ~0.3s while
                # the peer stays connected.
                deadline = time.time() + 0.3
                while time.time() < deadline:
                    try:
                        check()
                    except RuntimeError:
                        break
                    time.sleep(0.05)
            finally:
                close_probe()  # the round-3 fix: restore blocking mode
            body = b"R" * 512 * 1024  # 512KB — the response amy showed truncated
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            holder.setdefault("written", 0)
            holder["written"] = len(body)

        def log_message(self, format, *args):  # noqa: A002,N802  # silence
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), _H)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    return srv, holder


def test_probe_restore_large_response_not_truncated():
    """Round-3 BLOCKER 1 (real HTTP): a large (512KB) response written by
    the handler AFTER a probe handled the wait must arrive COMPLETE —
    with the round-2 bug (setblocking(False) never restored), the unbuffered
    wfile short-sends (~146KB received) with no exception, and the client
    hangs on declared Content-Length."""
    srv, holder = _fake_adapter_on_real_http()
    try:
        port = srv.server_address[1]
        s = socket.create_connection(("127.0.0.1", port), timeout=10)
        try:
            s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            buf = b""
            deadline = time.time() + 15
            while time.time() < deadline:
                chunk = s.recv(1 << 16)
                if not chunk:
                    break
                buf += chunk
                # Header end + full body?
                if b"\r\n\r\n" in buf:
                    head, _, body_part = buf.partition(b"\r\n\r\n")
                    clen = int(next(l.split(b":")[1].strip() for l in head.split(b"\r\n")
                                    if l.lower().startswith(b"content-length")))
                    if len(body_part) >= clen:
                        break
            head, _, body_part = buf.partition(b"\r\n\r\n")
            clen = int(next(l.split(b":")[1].strip() for l in head.split(b"\r\n")
                            if l.lower().startswith(b"content-length")))
            assert len(body_part) == clen, (
                f"RESPONSE TRUNCATED: got {len(body_part)} of {clen} bytes "
                f"(round-3 blocker 1 shape)")
        finally:
            s.close()
    finally:
        srv.shutdown()
        srv.server_close()


def test_probe_restore_keepalive_connection_survives():
    """Round-3 BLOCKER 2 (real HTTP): a SECOND request on the same pooled
    keep-alive connection must succeed after the first request's wait was
    probe-handled — with the round-2 bug, the next request's
    rfile.readline() hit a non-blocking socket and read EOF b'' (server
    closes the live connection)."""
    srv, holder = _fake_adapter_on_real_http()
    try:
        port = srv.server_address[1]
        s = socket.create_connection(("127.0.0.1", port), timeout=10)
        try:
            for i in (1, 2):  # keep-alive: same connection, two requests
                s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                buf = b""
                deadline = time.time() + 15
                while time.time() < deadline:
                    chunk = s.recv(1 << 16)
                    if not chunk:
                        pytest.fail(f"keep-alive request {i}: connection closed "
                                    f"by server after {len(buf)} bytes "
                                    f"(round-3 blocker 2 shape)")
                    buf += chunk
                    if b"\r\n\r\n" in buf:
                        head, _, body_part = buf.partition(b"\r\n\r\n")
                        clen = int(next(l.split(b":")[1].strip() for l in head.split(b"\r\n")
                                        if l.lower().startswith(b"content-length")))
                        if len(body_part) >= clen:
                            break
                head, _, body_part = buf.partition(b"\r\n\r\n")
                clen = int(next(l.split(b":")[1].strip() for l in head.split(b"\r\n")
                                if l.lower().startswith(b"content-length")))
                assert len(body_part) == clen, f"request {i} truncated"
        finally:
            s.close()
    finally:
        srv.shutdown()
        srv.server_close()


def test_rpc_message_send_restores_socket_mode_after_wait(monkeypatch, tmp_path):
    """Direct check of the integration point: after _rpc_message_send's
    blocking wait runs with a handler, the handler socket is back in
    blocking mode — even when the wait ended via disconnect (the finally
    ran)."""
    _isolated_hermes_home(monkeypatch, tmp_path)
    monkeypatch.delenv("A2A_REPLY_TIMEOUT", raising=False)
    adp = _instantiate_adapter(port=_free_port())

    a, b = socket.socketpair()
    try:
        b.send(b"PING")  # buffered: probe sees readable-with-data (alive)
        class _H:
            connection = a

        rec = adp.tasks.create("restore", "ctx-restore", peer="peer-restore",
                               direction="inbound", ownership="local")
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])
        pending = {
            "task_id": rec["task_id"], "context_id": rec["context_id"],
            "peer": "peer-restore", "future": fut,
            "created_iso": rec["created_iso"], "started": time.time(),
        }

        params = {"message": {"role": "ROLE_USER", "contextId": rec["context_id"],
                              "parts": [{"text": "hi", "mediaType": "text/plain"}],
                              "messageId": "msg-restore"}}
        # _prepare_task dispatches self.handle_message(..., self._loop) — the
        # connect() loop is closed in the test harness; stub the dispatch so
        # the wait is controlled solely by our Timer resolution.
        import asyncio as _asyncio

        async def _noop_handle(event):
            return None
        monkeypatch.setattr(adp, "handle_message", _noop_handle, raising=False)
        monkeypatch.setattr(adp, "_message_handler", object(), raising=False)
        # The connect() loop is closed in the harness; run_coroutine_threadsafe
        # onto it raises 'Event loop is closed' — give the adapter (and the
        # dispatch) a fresh running loop instead.
        new_loop = _asyncio.new_event_loop()
        loop_thread = threading.Thread(target=new_loop.run_forever, daemon=True)
        loop_thread.start()
        monkeypatch.setattr(adp, "_loop", new_loop, raising=False)
        # Resolve whatever task _rpc_message_send actually creates (params
        # carry no taskId, so it generates a fresh id) from another thread,
        # so the wait ENDS with a real reply.
        import asyncio as _aio

        # NOTE: a pre-created pending ('restore') exists; _rpc_message_send adds
        # its OWN pending for the NEW task id. Resolve only the newly-added one.
        pre_ids = set(adp._pending.keys())

        def _resolve_when_present():
            deadline = time.time() + 10
            while time.time() < deadline:
                with adp._pending_lock:
                    fresh = [tid for tid in adp._pending if tid not in pre_ids]
                if fresh:
                    with adp._pending_lock:
                        _ctx, fut = adp._pending[fresh[0]]
                        if not fut.done():
                            fut.set_result((STATE_COMPLETED, "done"))
                    return
                time.sleep(0.05)
        threading.Timer(0.5, _resolve_when_present).start()
        result = adp._rpc_message_send("req-restore", params, "peer-restore",
                                       v1_response=False, handler=_H())
        # v1_response=False returns the bare Task dict.
        task = result if "status" in result else result.get("result", result)
        state_val = task["status"]["state"]
        reply_dbg = (task["status"].get("message") or "")
        assert state_val in ("TASK_STATE_COMPLETED", STATE_COMPLETED), (
            f"unexpected state: {state_val} reply={reply_dbg!r}")
        # THE FIX: blocking mode restored even after normal wait completion.
        assert a.gettimeout() == -1.0 or a.gettimeout() is None, (
            f"socket still non-blocking after wait (timeout={a.gettimeout()})")
    finally:
        a.close()
        b.close()
        adp._watchdog_stop.set()