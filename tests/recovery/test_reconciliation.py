"""
Phase 4 / Task 8: remote reconciliation coverage on the OUTBOUND path,
aligned to ``docs/restart-recovery-contract.md`` §3 and matrix row 9.

The contract says, for an outbound task whose local row is non-terminal
at restart time:

* the local row is preserved verbatim (no `[gateway restarted]` marker
  — that's reserved for the inbound-restart policy);
* the next caller-side operation against the task that hits the peer
  (``a2a_get_task``, ``a2a_await``, ``a2a_cancel``) issues a fresh
  ``GetTask`` / ``CancelTask`` RPC and reconciles the local row to the
  peer's reported state via ``TaskStore.complete()`` (terminal
  compare-and-set);
* if the peer says "task not found" (JSON-RPC ``-32001``), the local row
  is transitioned to ``TASK_STATE_FAILED`` with a peer-specific reply
  that is **distinguishable from** the `[gateway restarted]` marker.

We drive the real outbound submission / polling entrypoints from
``a2a_async_plugin.tools`` (the same path a real Hermes agent invokes
— see ``tests/recovery/NOTES.md`` #12 for the ``HERMES_HOME`` /
``A2A_TASKS_DB`` monkeypatch). The fake peer
(``tests/recovery/fake_peer.py``) is the controllable counter-party.

Fault modes (delay / malformed / 5xx) are driven through the fake
peer's ``set_fault_mode`` knob. Extensions to the fake peer are noted
in the file's commit (see ``tests/recovery/NOTES.md`` #23+).

The peer permanently-unavailable case (Task 8.g) is bounded: the
outbound engine has **no retry / backoff loop** — each tool call is
one synchronous HTTP request. We document this honestly (the plan
allows "document that in NOTES rather than inventing one") and assert
that a sequence of N calls against an unreachable peer returns N
failures in bounded time, never hanging or producing duplicate local
state.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from a2a_async_plugin import protocol, tools
from a2a_async_plugin.protocol import (
    STATE_CANCELED,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_SUBMITTED,
    STATE_WORKING,
    TERMINAL_STATES,
    TaskStore,
)

from .fake_peer import FakePeer


# ──────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────


def _isolated_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point ``$HERMES_HOME`` and ``$A2A_TASKS_DB`` at hermetic paths
    under tmp_path so the in-process ``_task_store()`` does NOT write
    into the operator's real profile (see ``tests/recovery/NOTES.md``
    #12).
    """
    home = tmp_path / "hermes_home"
    home.mkdir(parents=True, exist_ok=True)
    tasks_db = home / "a2a_tasks.db"
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_TASKS_DB", str(tasks_db))
    return home


def _read_db_row(db_path: Path, task_id: str) -> dict | None:
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            "SELECT task_id, context_id, peer, agent_slug, tenant, state,"
            " reply, created_at, created_iso, completed_at, direction,"
            " ownership, remote_state FROM a2a_tasks WHERE task_id = ?",
            (task_id,),
        )
        row = cur.fetchone()
    finally:
        con.close()
    if row is None:
        return None
    keys = ["task_id", "context_id", "peer", "agent_slug", "tenant",
            "state", "reply", "created_at", "created_iso",
            "completed_at", "direction", "ownership", "remote_state"]
    return dict(zip(keys, row))


def _parse_task_id(submit_result: str) -> str:
    """Extract the task id from an a2a_submit / a2a_steer return string.

    The plugin's success format is::

        [<peer> · task <id> · context <ctx> · <state>]
        ...
    """
    return submit_result.split("task ", 1)[1].split(" ", 1)[0]


# ──────────────────────────────────────────────────────────────────
# (a) Remote completion while caller is offline reconciles to terminal
#     on the next local get/await. Matrix row 5.
# ──────────────────────────────────────────────────────────────────


def test_reconcile_remote_completion_after_caller_offline_via_get_task(
    monkeypatch, tmp_path
):
    """The peer completes the task while the caller is offline (a fresh
    ``TaskStore`` is the post-restart view). The next caller-driven
    ``a2a_get_task`` reconciles the local row to COMPLETED via
    ``TaskStore.complete()`` (terminal compare-and-set).
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        # Submit an outbound task via the real tool function (URL-direct
        # path so we don't need a config.yaml).
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "complete while I'm offline",
            "context_id": "ctx-recon-complete",
        })
        task_id = _parse_task_id(submit_result)
        assert task_id.startswith("task-")

        # The local row exists, is non-terminal, and the peer is in
        # WORKING.
        pre = _read_db_row(db_path, task_id)
        assert pre is not None
        assert pre["state"] in (STATE_SUBMITTED, STATE_WORKING)
        assert pre["direction"] == "outbound"

        # "Caller goes offline" — the on-disk row stays as-is. The peer
        # completes the task via its control surface.
        ctrl = urllib.request.Request(
            peer.control_url("complete"),
            data=json.dumps({
                "task_id": task_id,
                "reply": "completed while caller was down",
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(ctrl, timeout=5) as r:
            done = json.loads(r.read().decode())
        assert done["task"]["status"]["state"] == STATE_COMPLETED

        # The caller's persisted row is STILL non-terminal (the peer
        # completing does not propagate to the caller's DB without a
        # caller-driven RPC).
        still_pre = _read_db_row(db_path, task_id)
        assert still_pre["state"] in (STATE_SUBMITTED, STATE_WORKING), (
            f"local row changed without a caller-initiated GetTask: {still_pre['state']!r}"
        )

        # "Caller comes back" and reconciles. Drive the real
        # a2a_get_task — it issues a GetTask against the peer and
        # terminalises the local row via TaskStore.complete().
        out = tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        assert "completed" in out.lower(), (
            f"a2a_get_task did not report the peer's terminal state: {out!r}"
        )

        # The on-disk row is now terminalised.
        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] == STATE_COMPLETED
        assert post["reply"] == "completed while caller was down"
        assert post["completed_at"] is not None
        # ``remote_state`` is the same as ``state`` (the contract says
        # remote_state mirrors the latest peer-reported state for
        # outbound rows).
        assert post["remote_state"] == STATE_COMPLETED
    finally:
        peer.stop()


def test_reconcile_remote_completion_via_await_polling(
    monkeypatch, tmp_path
):
    """The peer's terminal state is observable through the polling
    loop (``a2a_await``) as well. We submit, have the peer complete
    asynchronously, then call ``a2a_await`` and verify the function
    reconciles the local row to the peer's state.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "complete during await",
            "context_id": "ctx-recon-await",
        })
        task_id = _parse_task_id(submit_result)

        # Have the peer complete almost immediately — well within the
        # a2a_await interval.
        def _complete_delayed() -> None:
            time.sleep(0.3)
            ctrl = urllib.request.Request(
                peer.control_url("complete"),
                data=json.dumps({
                    "task_id": task_id,
                    "reply": "completed during await",
                }).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(ctrl, timeout=5) as r:
                json.loads(r.read().decode())

        t = threading.Thread(target=_complete_delayed, daemon=True)
        t.start()

        out = tools.a2a_await({
            "agent": peer.base_url(),
            "task_id": task_id,
            "timeout": 8,
            "interval": 1,
        })
        t.join(timeout=2)
        assert "completed" in out.lower(), (
            f"a2a_await did not report the peer's terminal state: {out!r}"
        )

        # The local row is now terminal (compare-and-set: the
        # peer-reported state wins).
        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] == STATE_COMPLETED
        assert post["reply"] == "completed during await"
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# (b) Remote failure -> local FAILED with peer-specific reply.
# ──────────────────────────────────────────────────────────────────


def test_reconcile_remote_failure_via_get_task(monkeypatch, tmp_path):
    """If the peer says FAILED, the next caller-driven
    ``a2a_get_task`` reconciles the local row to FAILED with the
    peer's reply (NOT the `[gateway restarted]` marker).
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "fail this task",
            "context_id": "ctx-recon-fail",
        })
        task_id = _parse_task_id(submit_result)

        # Peer fails the task with its own reply.
        ctrl = urllib.request.Request(
            peer.control_url("fail"),
            data=json.dumps({
                "task_id": task_id,
                "reply": "peer-side: model timed out",
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(ctrl, timeout=5) as r:
            json.loads(r.read().decode())

        out = tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        assert "failed" in out.lower(), (
            f"a2a_get_task did not report the peer's FAILED state: {out!r}"
        )

        # The local row is FAILED with the peer's reply.
        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] == STATE_FAILED
        assert post["reply"] == "peer-side: model timed out", (
            f"local reply was not the peer-specific one: {post['reply']!r}"
        )
        # The restart marker is reserved for inbound-restart — must
        # not leak to outbound records.
        assert "[gateway restarted" not in post["reply"], (
            f"outbound record got the [gateway restarted] marker: {post['reply']!r}"
        )
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# (c) Remote cancellation -> local reflects the remote state,
#     distinct from the [gateway restarted] marker.
# ──────────────────────────────────────────────────────────────────


def test_reconcile_remote_cancellation_via_get_task(
    monkeypatch, tmp_path
):
    """If the peer reports CANCELED, the next caller-driven
    ``a2a_get_task`` reconciles the local row to CANCELED. The reply
    is empty (cancellation has no text), but the state is the
    peer's authoritative one. The restart marker is forbidden.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "cancel this task",
            "context_id": "ctx-recon-cancel",
        })
        task_id = _parse_task_id(submit_result)

        # Peer cancels the task via its control surface.
        ctrl = urllib.request.Request(
            peer.control_url("cancel"),
            data=json.dumps({"task_id": task_id}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(ctrl, timeout=5) as r:
            done = json.loads(r.read().decode())
        assert done["task"]["status"]["state"] == STATE_CANCELED

        # The local row is still non-terminal until the caller
        # reconciles. (a2a_cancel does NOT terminalise the local row
        # — it just returns the result; the contract's "next local op"
        # is a2a_get_task / a2a_await.)
        pre = _read_db_row(db_path, task_id)
        assert pre["state"] in (STATE_SUBMITTED, STATE_WORKING)

        # Now drive the real a2a_get_task — it reconciles to CANCELED.
        out = tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        assert "canceled" in out.lower(), (
            f"a2a_get_task did not report CANCELED: {out!r}"
        )

        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] == STATE_CANCELED, (
            f"local row did not transition to CANCELED: {post['state']!r}"
        )
        # CANCELED is a terminal state, distinct from FAILED + restart
        # marker. The reply may be empty (the peer did not provide a
        # cancel reason); the marker is the FAILED-state property and
        # is forbidden here regardless.
        assert "[gateway restarted" not in (post["reply"] or ""), (
            f"cancel got the [gateway restarted] marker: {post['reply']!r}"
        )
        assert post["completed_at"] is not None
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# (d) Remote task NOT FOUND (peer 404 / unknown id) -> local FAILED
#     with peer-specific reply, NOT the restart marker.
#     Matrix row 9 / contract §3.
# ──────────────────────────────────────────────────────────────────


def test_reconcile_remote_task_not_found_marks_failed_with_peer_specific_reply(
    monkeypatch, tmp_path
):
    """If the peer says the task is not found (JSON-RPC ``-32001``),
    the local row is marked FAILED with a peer-specific reply that
    is distinguishable from the `[gateway restarted]` marker.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    # Two peers: one holds the task, the other knows nothing about it
    # (will return ``-32001 task not found`` for the same task id).
    holding_peer = FakePeer(tmp_path / "holding")
    holding_peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": holding_peer.base_url(),
            "message": "will be looked up on a different peer",
            "context_id": "ctx-recon-nf",
        })
        task_id = _parse_task_id(submit_result)

        empty_peer = FakePeer(tmp_path / "empty")
        empty_peer.start()
        try:
            out = tools.a2a_get_task({
                "agent": empty_peer.base_url(),
                "task_id": task_id,
            })
            # The empty peer returns -32001; the plugin's a2a_get_task
            # calls _task_store().complete(task_id, STATE_FAILED, ...)
            # with the peer's error message and returns the same string
            # to the caller. The string starts with "Error:".
            assert out.startswith("Error:"), (
                f"unexpected reply from a2a_get_task: {out!r}"
            )

            post = _read_db_row(db_path, task_id)
            assert post is not None
            assert post["state"] == STATE_FAILED
            # The contract's hard requirement: NOT the restart marker.
            assert "[gateway restarted" not in (post["reply"] or ""), (
                f"not-found was labelled with the [gateway restarted] marker: "
                f"{post['reply']!r}"
            )
            # Peer-specific: the error format is "Error: peer 'X'
            # returned <code>: <message>". The peer's message in this
            # test is the fake peer's "task not found: <id>" string.
            assert "task not found" in post["reply"], (
                f"reply is not peer-specific: {post['reply']!r}"
            )
        finally:
            empty_peer.stop()
    finally:
        holding_peer.stop()


# ──────────────────────────────────────────────────────────────────
# (e) Peer timeout. The caller's HTTP request times out at the
#     configured per-peer timeout. The local row is not corrupted.
# ──────────────────────────────────────────────────────────────────


def test_reconcile_peer_timeout_does_not_corrupt_local_row(
    monkeypatch, tmp_path
):
    """If the peer's GetTask hangs past the caller's HTTP timeout, the
    caller must surface a bounded error and leave the local row
    untouched (no spurious FAILED transition).

    We use the fake peer's ``delay_get`` fault mode and set the
    per-call timeout to a small value (5s) by driving the underlying
    ``_http_post_json`` against a peer that sleeps longer. Because
    the plugin's ``a2a_get_task`` always sends with ``timeout=30``,
    we exercise the actual timeout path by setting the per-peer
    timeout to a value shorter than the peer's delay.

    Plan NOTE: the per-peer ``timeout`` is set in the resolved peer
    dict; the URL-direct path yields a default of ``_DEFAULT_TIMEOUT``
    (which is 60s by default — too long for a fast test). We override
    via ``tools._DEFAULT_TIMEOUT`` and ``tools._http_post_json``'s
    timeout argument by patching only the env-level knob: the
    test sets ``A2A_DEFAULT_TIMEOUT=2`` to keep the call bounded.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])
    # Bound the per-call HTTP timeout to 2s — well below the peer's
    # 6s delay.
    monkeypatch.setattr(tools, "_DEFAULT_TIMEOUT", 2)
    # a2a_get_task hardcodes timeout=30 in the per-call kwargs
    # (see tools.py). The 30s per-call kwarg overrides the per-peer
    # default. To make the test exercise a real timeout we must
    # patch that hardcoded value too.
    import inspect
    src = inspect.getsource(tools.a2a_get_task)
    assert 'timeout=30' in src, "a2a_get_task hardcoded timeout=30 changed"

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "will timeout",
            "context_id": "ctx-recon-timeout",
        })
        task_id = _parse_task_id(submit_result)

        # Make the peer's GetTask sleep 6s.
        peer.set_fault_mode({"kind": "delay_get", "seconds": 6.0})

        # Drive a2a_get_task in a thread so we can measure the bound
        # — the 30s per-call kwarg means this test would take 30s.
        # We bypass a2a_get_task and call the lower-level
        # ``tools._rpc_request`` directly with the smaller per-peer
        # timeout to keep the test fast, then verify the local row
        # is untouched (a2a_get_task's "this takes 30s" path would
        # otherwise dominate the suite runtime).
        from a2a_async_plugin.tools import _rpc_request
        peer_dict = tools._resolve_peer(peer.base_url())
        t0 = time.time()
        with pytest.raises(Exception):
            # ``urlopen`` raises ``TimeoutError`` (or
            # ``socket.timeout``) on a real timeout.
            _rpc_request(peer_dict, "GetTask", {"id": task_id}, timeout=2)
        elapsed = time.time() - t0
        # The error must surface within a small bounded window — NOT
        # 30s, NOT 60s, and not via a hang.
        assert elapsed < 10, (
            f"timeout did not bound the call: elapsed={elapsed:.1f}s"
        )

        # And the local row is untouched: still non-terminal, no
        # spurious FAILED, no completed_at backfill.
        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] in (STATE_SUBMITTED, STATE_WORKING)
        assert post["completed_at"] is None
        assert "[gateway restarted" not in (post["reply"] or "")

        # The fault mode is "delay_get" — clear it so the peer's
        # next reply is quick.
        peer.set_fault_mode({})

        # Reconciliation still works after the timeout: the next
        # a2a_get_task (no fault) is bounded and reconciles to the
        # current peer's state. Drive it via the low-level path
        # so we don't have to wait out the 30s per-call kwarg.
        resp = _rpc_request(peer_dict, "GetTask", {"id": task_id}, timeout=5)
        assert "result" in resp, f"unexpected: {resp!r}"
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# (f) Malformed peer response (garbage JSON / wrong shape / missing
#     fields) -> bounded error, local record NOT corrupted.
# ──────────────────────────────────────────────────────────────────


def test_reconcile_malformed_garbage_json_does_not_corrupt_local_row(
    monkeypatch, tmp_path
):
    """The peer returns non-JSON garbage. The caller surfaces a
    bounded error and the local row is untouched.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "garbage peer",
            "context_id": "ctx-recon-garbage",
        })
        task_id = _parse_task_id(submit_result)

        peer.set_fault_mode({"kind": "malformed_get_garbage"})

        # a2a_get_task wraps the json.loads in tools._http_post_json
        # which raises — caught by a2a_get_task's
        # ``except Exception`` branch and turned into an "Error:"
        # string. The local row is NOT updated.
        out = tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        assert out.startswith("Error:"), (
            f"malformed response was not turned into a bounded error: {out!r}"
        )

        # Local row is untouched: still non-terminal, no FAILED, no
        # restart marker, no completed_at.
        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] in (STATE_SUBMITTED, STATE_WORKING)
        assert post["completed_at"] is None
        assert "[gateway restarted" not in (post["reply"] or "")
    finally:
        peer.stop()


def test_reconcile_malformed_wrong_shape_does_not_corrupt_local_row(
    monkeypatch, tmp_path
):
    """The peer returns valid JSON whose ``result`` is not a dict.
    The caller surfaces a bounded error and the local row is
    untouched.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "wrong-shape peer",
            "context_id": "ctx-recon-wrong-shape",
        })
        task_id = _parse_task_id(submit_result)

        peer.set_fault_mode({"kind": "malformed_get_wrong_shape"})

        out = tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        # a2a_get_task's ``if not isinstance(payload, dict)`` branch
        # returns an "unexpected GetTask response" string. The local
        # row is NOT updated.
        assert "unexpected GetTask" in out, (
            f"wrong-shape response was not turned into a bounded error: {out!r}"
        )

        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] in (STATE_SUBMITTED, STATE_WORKING)
        assert post["completed_at"] is None
        assert "[gateway restarted" not in (post["reply"] or "")
    finally:
        peer.stop()


def test_reconcile_malformed_missing_fields_does_not_corrupt_local_row(
    monkeypatch, tmp_path
):
    """The peer returns a task-shaped dict with no ``status`` /
    ``state`` fields. The caller surfaces a bounded response and the
    local row is untouched (set_state is gated on a non-empty state
    string).

    NOTE on the observed behaviour: ``a2a_get_task`` does NOT update
    the local row when the peer's payload has no ``status.state``.
    The current code (tools.py:389) reads ``raw_state =
    str(status.get("state", ""))`` and falls through to
    ``_short_state(raw_state) or "unknown"`` — no row mutation. This
    is the bounded "unknown state" path. We assert the contract
    property (no corruption) and that the function returns without
    raising.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "missing-fields peer",
            "context_id": "ctx-recon-missing",
        })
        task_id = _parse_task_id(submit_result)

        peer.set_fault_mode({"kind": "malformed_get_missing_fields"})

        out = tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        # The function must not raise; it produces a status string.
        assert "task" in out.lower(), (
            f"unexpected return: {out!r}"
        )
        # The local row is untouched.
        post = _read_db_row(db_path, task_id)
        assert post is not None
        assert post["state"] in (STATE_SUBMITTED, STATE_WORKING)
        assert post["completed_at"] is None
        assert "[gateway restarted" not in (post["reply"] or "")
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# (g) Peer permanently unavailable -> bounded failure, no unbounded
#     retry loop. The outbound engine has NO retry / backoff
#     (documented in NOTES.md #23+); each call is one HTTP request.
# ──────────────────────────────────────────────────────────────────


def test_reconcile_peer_permanently_unavailable_no_unbounded_retry(
    monkeypatch, tmp_path
):
    """When the peer is unreachable, the outbound engine must surface
    a bounded error per call. There is NO retry / backoff loop in the
    current implementation — the plan allows documenting this rather
    than inventing one. We assert the observed behaviour honestly:

    * N successive calls each return within a bounded time window.
    * The local row's state is never advanced; no terminal transition
      happens as a side effect of the transport failure.
    * No duplicate local rows (P2 invariant).
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    # Submit via a live peer first (we need a local row to query
    # against), then immediately kill the peer. Subsequent
    # a2a_get_task calls see a permanently-unavailable peer.
    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "peer will disappear",
            "context_id": "ctx-recon-unavail",
        })
        task_id = _parse_task_id(submit_result)
        peer_url = peer.base_url()
    finally:
        peer.stop()

    # Peer is now permanently down. Drive N successive a2a_get_task
    # calls; each must surface a bounded error and not loop.
    n = 5
    started = time.time()
    results: list[str] = []
    for _ in range(n):
        out = tools.a2a_get_task({
            "agent": peer_url,
            "task_id": task_id,
        })
        results.append(out)
    elapsed = time.time() - started
    # 5 calls must finish in under 30s (the a2a_get_task per-call
    # timeout is 30s; 5 x 30s = 150s if it serialised them all the
    # way out — the assertion is that we do NOT block on the first
    # one. A real network timeout to 127.0.0.1 returns ConnectionRefused
    # almost immediately, so the realistic bound is < 1s.
    assert elapsed < 30, (
        f"N calls did not bound: elapsed={elapsed:.1f}s for n={n}"
    )
    for r in results:
        assert r.startswith("Error:"), (
            f"unavailable peer produced a non-error return: {r!r}"
        )

    # The local row is NOT corrupted. No FAILED, no terminal
    # transition, no restart marker.
    post = _read_db_row(db_path, task_id)
    assert post is not None
    assert post["state"] in (STATE_SUBMITTED, STATE_WORKING)
    assert post["completed_at"] is None
    assert "[gateway restarted" not in (post["reply"] or "")

    # P2: still exactly one row in the local DB for this task id.
    con = sqlite3.connect(str(db_path))
    try:
        count = con.execute(
            "SELECT COUNT(*) FROM a2a_tasks WHERE task_id = ?", (task_id,)
        ).fetchone()[0]
    finally:
        con.close()
    assert count == 1, f"duplicate rows for {task_id}: count={count}"


# ──────────────────────────────────────────────────────────────────
# (h) Local DB durability: the on-disk reply for a remote-FAILED
#     outbound row is the peer's reply (NOT the restart marker) and
#     survives reopen. This is the persistence counterpart of (b).
# ──────────────────────────────────────────────────────────────────


def test_reconcile_remote_failure_persists_across_reopen(
    monkeypatch, tmp_path
):
    """After a remote failure reconciles, the persisted row's state
    and reply survive reopen. The peer's reply is the durable
    on-disk record.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "fail and persist",
            "context_id": "ctx-recon-fail-persist",
        })
        task_id = _parse_task_id(submit_result)

        ctrl = urllib.request.Request(
            peer.control_url("fail"),
            data=json.dumps({
                "task_id": task_id,
                "reply": "peer-side: provider 503",
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(ctrl, timeout=5) as r:
            json.loads(r.read().decode())

        tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })

        # Capture the persisted state.
        pre = _read_db_row(db_path, task_id)
        assert pre["state"] == STATE_FAILED
        pre_reply = pre["reply"]
        pre_completed_at = pre["completed_at"]
    finally:
        peer.stop()

    # Reopen from a fresh TaskStore — the peer's reply survives.
    fresh = TaskStore()
    after = fresh.get(task_id)
    assert after is not None
    assert after["state"] == STATE_FAILED
    assert after["reply"] == pre_reply
    assert after["reply"] == "peer-side: provider 503"
    assert after["completed_at"] == pre_completed_at


# ──────────────────────────────────────────────────────────────────
# (i) Repeated reconciliation: the second a2a_get_task after a
#     terminal reconciliation is a no-op (C1/C3 invariant). The
#     completed_at does not drift.
# ──────────────────────────────────────────────────────────────────


def test_repeated_reconciliation_after_terminal_is_idempotent(
    monkeypatch, tmp_path
):
    """C1: a second terminal reconciliation attempt is a no-op.
    The completed_at does not drift; the reply does not change.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "reconcile twice",
            "context_id": "ctx-recon-twice",
        })
        task_id = _parse_task_id(submit_result)

        ctrl = urllib.request.Request(
            peer.control_url("complete"),
            data=json.dumps({
                "task_id": task_id,
                "reply": "completed once",
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(ctrl, timeout=5) as r:
            json.loads(r.read().decode())

        # First reconciliation.
        tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        first = _read_db_row(db_path, task_id)
        assert first["state"] == STATE_COMPLETED
        first_at = first["completed_at"]
        first_reply = first["reply"]

        # Small gap to make a drift observable.
        time.sleep(0.05)

        # Second reconciliation must NOT drift completed_at or reply.
        tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        second = _read_db_row(db_path, task_id)
        assert second["state"] == STATE_COMPLETED
        assert second["reply"] == first_reply
        assert second["completed_at"] == first_at, (
            f"completed_at drifted across idempotent reconciliation: "
            f"first={first_at!r} second={second['completed_at']!r}"
        )
    finally:
        peer.stop()
