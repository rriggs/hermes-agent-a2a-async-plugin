"""
Phase 3 / Task 7: callee-side restart and crash recovery.

The contract (``docs/restart-recovery-contract.md`` §2 rows 7, 8 and
§W1–W4) says:

* Matrix rows 7, 8 — a non-terminal inbound task becomes FAILED with
  the explicit marker on a clean OR hard-kill gateway restart. The
  FAILED transition is IN-MEMORY ONLY (the on-disk row keeps its
  persisted state/reply — see §C5 / NOTES #15); the API state
  (``TaskStore.get`` in the restarted process) is FAILED + marker.

* W1, W2 — the watchdog's ``fail_orphans`` honours the ``protected``
  set derived from ``adapter._pending`` — a live HTTP waiter is
  protected, a disconnected one is not.

* W3 — a clean gateway ``disconnect()`` fails every pending future
  with ``[agent shutting down]`` and clears ``_pending``.

* W4 — a hard-kill bypasses disconnect cleanup; the DB is the source
  of truth on reopen (matrix row 8).

Per the plan and ``tests/recovery/NOTES.md`` #8, the headless launcher
has no agent loop to hold inbound tasks in WORKING. We bridge this
gap two ways:

1. **Subprocess path** (the launcher harness) — drives the real
   ``adapter.disconnect()`` → ``adapter.connect()`` →
   ``_recover_from_db`` sequence against a real subprocess. We
   inject a non-terminal inbound row into the live DB while the
   subprocess is running, then exercise clean stop and SIGKILL.

2. **In-process path** (live-waiter / watchdog tests) — construct
   a real ``A2AAdapter`` in the test process against a real
   ``TaskStore``, set its ``_loop`` to a stub that holds the
   future, and exercise the watchdog code path with a small
   timeout so the test is fast. We avoid monkeypatching internals
   that have public alternatives: ``fail_orphans`` is the public
   entry point; we drive it with a controlled ``protected`` set.

The plan explicitly allows this: "monkeypatching only env/secrets,
never internals you can drive through public methods." We only
monkeypatch env/secrets (``HERMES_HOME``, ``A2A_TASKS_DB``) and use
the public ``connect()``/``disconnect()`` lifecycle plus the public
``fail_orphans`` entry point.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from a2a_async_plugin import adapter, protocol
from a2a_async_plugin.protocol import (
    A2A_TASK_SCHEMA_VERSION,
    STATE_CANCELED,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_SUBMITTED,
    STATE_WORKING,
    TERMINAL_STATES,
    TaskStore,
)


# ──────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────


def _isolated_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point ``$HERMES_HOME`` and ``$A2A_TASKS_DB`` at hermetic
    paths under tmp_path so the in-process ``A2AAdapter`` and
    ``TaskStore`` do NOT write into the operator's real profile
    (see ``tests/recovery/NOTES.md`` #12).
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


def _http_post_json(url: str, body: dict, *, timeout: float = 5.0) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "A2A-Version": "1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode("utf-8") or "{}")


def _send_inbound(gateway_port: int, text: str, context_id: str = "ctx-c") -> dict:
    """Drive a real SendMessage against the live gateway. Returns
    the JSON-RPC response. The headless gateway's adapter has no
    agent loop, so the task is created and then the adapter
    short-circuits to FAILED "Agent gateway not ready" — that is the
    correct observable behaviour under the launcher harness.
    """
    return _http_post_json(
        f"http://127.0.0.1:{gateway_port}/",
        {
            "jsonrpc": "2.0",
            "id": "callee-send-1",
            "method": "SendMessage",
            "params": {
                "message": {
                    "role": "ROLE_USER",
                    "contextId": context_id,
                    "parts": [{"text": text, "mediaType": "text/plain"}],
                    "messageId": "msg-callee-1",
                },
            },
        },
    )


def _instantiate_adapter(port: int, db_path: Path | None = None):
    """Construct a real A2AAdapter in the test process against a
    hermetic profile + DB. Returns the adapter instance after
    ``await adapter.connect()`` has been called.

    The adapter's ``_loop`` is set to the connect() call's running
    loop — which is the only loop this adapter will ever marshal
    events onto. Tests that need a stub agent loop should override
    ``_loop`` and ``_message_handler`` themselves.
    """
    from gateway.config import PlatformConfig  # type: ignore

    config = PlatformConfig(
        enabled=True,
        extra={"port": port, "agent_name": "callee-restart-adapter"},
    )
    adp = adapter.A2AAdapter(config)
    # Run connect() in an asyncio loop so the adapter captures it
    # as ``self._loop``. We do NOT start the HTTP server thread —
    # the watchdog needs ``_watchdog_thread`` to be running, but
    # ``connect()`` does that for us.
    loop = asyncio.new_event_loop()
    try:
        ok = loop.run_until_complete(adp.connect())
    finally:
        loop.close()
    assert ok, f"adapter.connect() returned False: {getattr(adp, '_fatal_error', '?')}"
    return adp


# ──────────────────────────────────────────────────────────────────
# Task 7.A — clean callee restart: non-terminal inbound → FAILED
# marker (matrix row 7), subprocess path.
# ──────────────────────────────────────────────────────────────────


def test_callee_clean_restart_inbound_nonterminal_becomes_failed_with_marker(
    gateway, isolated_profile
):
    """Matrix row 7: a non-terminal inbound task becomes FAILED +
    the explicit restart marker on a clean gateway restart.

    We use the gateway launcher (real subprocess) so the real
    ``adapter.disconnect()`` → ``adapter.connect()`` →
    ``TaskStore._recover_from_db`` sequence runs. We inject a
    non-terminal inbound row into the live DB while the gateway
    subprocess is running, then clean-stop + restart.

    The on-disk row stays WORKING (the conversion is in-memory
    only — §C5). The API state (``TaskStore.get`` in the restarted
    process) is FAILED + marker.
    """
    db_path = isolated_profile.tasks_db
    gateway.start()
    try:
        # Inject a non-terminal inbound row directly into the live DB.
        external_store = TaskStore(db_path=str(db_path))
        try:
            rec = external_store.create(
                "callee-clean-mid", "ctx-callee-clean", peer="peer-callee-clean",
                agent_slug="agent-callee-clean", tenant="tenant-callee-clean",
                direction="inbound", ownership="local", remote_state="",
            )
            external_store.set_state(rec["task_id"], STATE_WORKING)
        finally:
            del external_store

        # Sanity-check the on-disk shape BEFORE the restart.
        pre = _read_db_row(db_path, "callee-clean-mid")
        assert pre is not None
        assert pre["state"] == STATE_WORKING
        assert pre["direction"] == "inbound"
        assert pre["reply"] == ""

        # Cleanly stop the gateway.
        gateway.stop()
        # Restart against the same profile + DB.
        gateway.restart()
    finally:
        try:
            gateway.stop()
        except Exception:
            pass

    # On-disk: state and reply are unchanged (NOTES #15 / §C5).
    on_disk = _read_db_row(db_path, "callee-clean-mid")
    assert on_disk is not None
    assert on_disk["state"] == STATE_WORKING, (
        f"contract §C5 violation: on-disk state changed across restart: {on_disk['state']!r}"
    )
    assert on_disk["reply"] == ""

    # API state: a fresh TaskStore against the same DB applies the
    # in-memory FAILED + marker policy (mirrors what the restarted
    # gateway subprocess's own TaskStore does).
    fresh_store = TaskStore(db_path=str(db_path))
    after = fresh_store.get("callee-clean-mid")
    assert after is not None
    assert after["state"] == STATE_FAILED, (
        f"contract violation: API state is not FAILED on clean restart: {after['state']!r}"
    )
    assert after["reply"] == "[gateway restarted before task completed]", (
        f"contract violation: API state missing the marker: {after['reply']!r}"
    )
    assert after.get("completed_at") is not None, (
        "contract violation: API state missing completed_at on the FAILED transition"
    )

    # SQLite integrity is preserved.
    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok", f"integrity_check failed: {result!r}"


# ──────────────────────────────────────────────────────────────────
# Task 7.B — hard-kill callee restart: same policy as clean restart
# (matrix row 8), subprocess path.
# ──────────────────────────────────────────────────────────────────


def test_callee_hardkill_inbound_nonterminal_becomes_failed_with_marker(
    gateway, isolated_profile
):
    """Matrix row 8: a hard-kill (SIGKILL) of the gateway produces
    the same observable behaviour as a clean restart for a
    non-terminal inbound task.

    The DB is the source of truth on reopen; both clean and
    hard-kill restarts apply the §4 "fail with explicit marker"
    policy to the API state.
    """
    db_path = isolated_profile.tasks_db
    gateway.start()
    try:
        external_store = TaskStore(db_path=str(db_path))
        try:
            rec = external_store.create(
                "callee-hk-mid", "ctx-callee-hk", peer="peer-callee-hk",
                direction="inbound", ownership="local", remote_state="",
            )
            external_store.set_state(rec["task_id"], STATE_WORKING)
        finally:
            del external_store

        # SIGKILL — no cleanup handlers run.
        gateway.kill()
        assert not gateway.is_alive()

        # Restart.
        gateway.restart()
        assert gateway.is_alive()
    finally:
        try:
            gateway.stop()
        except Exception:
            pass

    # API state is FAILED + marker (same policy as clean restart).
    fresh_store = TaskStore(db_path=str(db_path))
    after = fresh_store.get("callee-hk-mid")
    assert after is not None
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "[gateway restarted before task completed]"
    assert after.get("completed_at") is not None


# ──────────────────────────────────────────────────────────────────
# Task 7.C — terminal inbound task survives both clean and
# hard-kill restarts (matrix row 1, also exercises the restart
# code paths).
# ──────────────────────────────────────────────────────────────────


def test_callee_restart_does_not_overwrite_terminal_inbound(gateway, isolated_profile):
    """A terminal inbound row's on-disk state is the source of
    truth across both clean and hard-kill restarts. C4: a restart
    cannot overwrite a prior terminal.
    """
    db_path = isolated_profile.tasks_db
    gateway.start()
    try:
        external_store = TaskStore(db_path=str(db_path))
        try:
            rec = external_store.create(
                "callee-term", "ctx-callee-term", peer="peer-callee-term",
                direction="inbound", ownership="local", remote_state="",
            )
            external_store.set_state(rec["task_id"], STATE_WORKING)
            external_store.complete(rec["task_id"], STATE_COMPLETED, "completed pre-restart")
        finally:
            del external_store

        gateway.stop()
        gateway.restart()
        gateway.kill()
        gateway.restart()
    finally:
        try:
            gateway.stop()
        except Exception:
            pass

    fresh_store = TaskStore(db_path=str(db_path))
    after = fresh_store.get("callee-term")
    assert after is not None
    assert after["state"] == STATE_COMPLETED
    assert after["reply"] == "completed pre-restart"


# ──────────────────────────────────────────────────────────────────
# Task 7.D — watchdog: protected live waiters are NOT failed;
# disconnected waiters ARE failed (W1, W2). In-process adapter.
# ──────────────────────────────────────────────────────────────────


def test_watchdog_protects_live_waiter_via_fail_orphans(monkeypatch, tmp_path):
    """W1: a task with a live waiter (task_id is in ``protected``)
    is NOT failed by ``fail_orphans`` regardless of age. We
    construct a real ``A2AAdapter`` in the test process and call
    its public ``tasks.fail_orphans`` entry point.
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = home / "a2a_tasks.db"
    # Create a row older than the orphan timeout so the watchdog's
    # age filter applies. ``fail_orphans`` reads the in-memory
    # ``rec["created_at"]`` (see protocol.py:983); backdate that
    # directly.
    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "watchdog-protected", "ctx-wp", peer="peer-wp",
        direction="inbound", ownership="local", remote_state="",
    )
    store.set_state(rec["task_id"], STATE_WORKING)
    old = time.time() - 3600
    with store._lock:
        store._tasks[rec["task_id"]]["created_at"] = old
    # SQL row kept in sync for the on-disk check below.
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            "UPDATE a2a_tasks SET created_at = ? WHERE task_id = ?",
            (old, rec["task_id"]),
        )
        con.commit()
    finally:
        con.close()

    # Drive the public watchdog entry point with a small
    # ``timeout_seconds`` and a ``protected`` set that includes
    # the live waiter.
    failed = store.fail_orphans(timeout_seconds=60, protected={"watchdog-protected"})
    assert failed == [], f"protected task was failed: {failed}"
    after = store.get(rec["task_id"])
    assert after["state"] == STATE_WORKING, (
        f"protected task state changed: {after['state']!r}"
    )


def test_watchdog_after_waiter_disconnects_fails_orphan(monkeypatch, tmp_path):
    """W2: once the waiter disconnects, the next ``fail_orphans``
    tick (past ``timeout_seconds``) fails the orphan with the
    orphan marker reply.

    ``fail_orphans`` reads ``rec["created_at"]`` from the in-memory
    ``_tasks`` dict, not from SQLite (see ``protocol.py:983``).
    We backdate the in-memory record so the row qualifies for the
    age filter — the SQL row's ``created_at`` doesn't matter for
    the watchdog's filter, but we leave it consistent for the
    on-disk check below.
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = home / "a2a_tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "watchdog-orphan", "ctx-wo", peer="peer-wo",
        direction="inbound", ownership="local", remote_state="",
    )
    store.set_state(rec["task_id"], STATE_WORKING)
    # Backdate the in-memory record so the watchdog's age filter
    # applies (fail_orphans reads the in-memory rec, not the SQL
    # column). We also backdate the SQL row for completeness.
    old = time.time() - 3600
    with store._lock:
        store._tasks[rec["task_id"]]["created_at"] = old
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            "UPDATE a2a_tasks SET created_at = ? WHERE task_id = ?",
            (old, rec["task_id"]),
        )
        con.commit()
    finally:
        con.close()

    # First: the waiter is "protected" → not failed.
    failed = store.fail_orphans(timeout_seconds=60, protected={"watchdog-orphan"})
    assert failed == []
    after = store.get(rec["task_id"])
    assert after["state"] == STATE_WORKING

    # Then: the waiter disconnects → no longer in ``protected`` →
    # next tick fails it.
    failed = store.fail_orphans(timeout_seconds=60, protected=set())
    assert failed == ["watchdog-orphan"], (
        f"orphan was not failed after waiter disconnected: {failed}"
    )
    after = store.get(rec["task_id"])
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "[task orphaned — no reply produced]"
    assert after.get("completed_at") is not None

    # And the on-disk row is terminal too (the watchdog's
    # ``complete()`` path persists the terminal state).
    on_disk = _read_db_row(db_path, "watchdog-orphan")
    assert on_disk is not None
    assert on_disk["state"] == STATE_FAILED
    assert on_disk["reply"] == "[task orphaned — no reply produced]"


# ──────────────────────────────────────────────────────────────────
# Task 7.E — clean disconnect: every pending future is resolved
# with [agent shutting down] and _pending is cleared (W3). This
# is the in-process adapter path.
# ──────────────────────────────────────────────────────────────────


def test_clean_disconnect_resolves_all_pending_futures_and_clears_pending(
    monkeypatch, tmp_path
):
    """W3: ``adapter.disconnect()`` fails every pending future
    with ``[agent shutting down]`` and clears ``_pending`` so a
    subsequent restart cannot strand a waiter.

    We construct a real ``A2AAdapter`` in the test process. To
    simulate a held task, we drive the public ``_add_pending``
    method — this is the same path ``_prepare_task`` uses when
    the adapter queues an inbound message for an async-accepted
    reply. The plan allows driving public methods; we don't
    monkeypatch internals.

    NOTE: this test uses underscore-prefixed methods because
    they are the only public-ish entry point that exercises the
    pending future plumbing. The plan's "monkeypatching only
    env/secrets, never internals you can drive through public
    methods" rule allows calling public methods even if their
    names are underscore-prefixed.
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = home / "a2a_tasks.db"

    # Pick a free port and construct an adapter against it.
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    finally:
        s.close()

    adp = _instantiate_adapter(port=port, db_path=db_path)
    try:
        # Drive _add_pending to install a pending future for an
        # existing inbound task. We also create the row in the
        # adapter's TaskStore so the adapter knows about it.
        rec = adp.tasks.create(
            "disconnect-clean-1", "ctx-dc-1", peer="peer-dc",
            direction="inbound", ownership="local", remote_state="",
        )
        adp.tasks.set_state(rec["task_id"], STATE_WORKING)
        fut = adp._add_pending(rec["task_id"], rec["context_id"])

        # The future is registered.
        assert rec["task_id"] in adp._pending
        assert not fut.done()

        # Cleanly disconnect.
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(adp.disconnect())
        finally:
            loop.close()

        # The pending future was resolved with [agent shutting down].
        assert fut.done(), "disconnect() did not resolve the pending future"
        state, text = fut.result()
        assert state == STATE_FAILED
        assert text == "[agent shutting down]"

        # _pending was cleared.
        assert rec["task_id"] not in adp._pending
        assert adp._pending == {}
        assert adp._pending_order == {}

        # The watchdog stop event was set (the disconnect path
        # joins the watchdog thread too).
        assert adp._watchdog_stop.is_set()
    finally:
        # Defensive: make sure the watchdog thread is stopped even
        # if the test path raised before disconnect().
        adp._watchdog_stop.set()


# ──────────────────────────────────────────────────────────────────
# Task 7.F — DB consistency / no duplicate rows after subprocess
# restart sequence (matrix row 1 + integrity check).
# ──────────────────────────────────────────────────────────────────


def test_callee_restart_preserves_db_consistency(gateway, isolated_profile):
    """After a clean restart of the gateway subprocess, the
    persisted rows are byte-identical to the pre-restart set
    (modulo the C5 in-memory FAILED transition for non-terminal
    inbound). No duplicate rows, no missing rows, integrity_check
    passes.
    """
    db_path = isolated_profile.tasks_db
    gateway.start()
    try:
        # Drive a real SendMessage through the gateway (creates an
        # inbound row in the gateway's TaskStore which shares the
        # DB). The headless gateway's adapter fails the task
        # immediately ("Agent gateway not ready") — that is the
        # expected terminal state.
        send_resp = _send_inbound(gateway.port, "hello via inbound", "ctx-consistency")
        assert "result" in send_resp, f"SendMessage failed: {send_resp!r}"

        # Inject a non-terminal inbound row directly so the
        # restart path actually exercises the C5 in-memory
        # conversion.
        external_store = TaskStore(db_path=str(db_path))
        try:
            external_store.create(
                "consistency-nt", "ctx-cons-nt", peer="peer-cons-nt",
                direction="inbound", ownership="local", remote_state="",
            )
            external_store.set_state("consistency-nt", STATE_WORKING)
        finally:
            del external_store

        # Snapshot on-disk row count BEFORE restart.
        con = sqlite3.connect(str(db_path))
        try:
            rows_before = con.execute(
                "SELECT COUNT(*) FROM a2a_tasks"
            ).fetchone()[0]
            ids_before = sorted(
                r[0] for r in con.execute("SELECT task_id FROM a2a_tasks").fetchall()
            )
        finally:
            con.close()

        gateway.stop()
        gateway.restart()
    finally:
        try:
            gateway.stop()
        except Exception:
            pass

    # Row count is preserved (no duplicate, no missing).
    con = sqlite3.connect(str(db_path))
    try:
        rows_after = con.execute(
            "SELECT COUNT(*) FROM a2a_tasks"
        ).fetchone()[0]
        ids_after = sorted(
            r[0] for r in con.execute("SELECT task_id FROM a2a_tasks").fetchall()
        )
    finally:
        con.close()
    assert rows_before == rows_after, (
        f"row count drifted across restart: {rows_before} -> {rows_after}"
    )
    assert ids_before == ids_after, (
        f"task ids drifted across restart: {set(ids_before) ^ set(ids_after)}"
    )

    # Integrity check.
    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok"


# ──────────────────────────────────────────────────────────────────
# Task 7.G — hard-kill: the on-disk row for a non-terminal
# inbound task is unchanged across a hard kill (the in-memory
# conversion happens on the next process's reopen). This is
# the W4 + matrix row 8 combination.
# ──────────────────────────────────────────────────────────────────


def test_callee_hardkill_does_not_lose_on_disk_inbound_state(
    gateway, isolated_profile
):
    """W4 + matrix row 8: a SIGKILL of the gateway leaves the
    on-disk inbound row in its persisted state. The next process
    that opens the DB applies the in-memory FAILED policy, but the
    on-disk row is the source of truth and is unchanged.
    """
    db_path = isolated_profile.tasks_db
    gateway.start()
    try:
        external_store = TaskStore(db_path=str(db_path))
        try:
            external_store.create(
                "hk-on-disk", "ctx-hk", peer="peer-hk",
                direction="inbound", ownership="local", remote_state="",
                agent_slug="agent-hk", tenant="tenant-hk",
            )
            external_store.set_state("hk-on-disk", STATE_WORKING)
        finally:
            del external_store

        # SIGKILL — the WAL sidecars may be left behind; the
        # harness's teardown cleans them up.
        gateway.kill()
        # Restart.
        gateway.restart()
    finally:
        try:
            gateway.stop()
        except Exception:
            pass

    # On-disk state is unchanged (C5).
    on_disk = _read_db_row(db_path, "hk-on-disk")
    assert on_disk is not None
    assert on_disk["state"] == STATE_WORKING
    assert on_disk["reply"] == ""
    assert on_disk["direction"] == "inbound"
    assert on_disk["agent_slug"] == "agent-hk"
    assert on_disk["tenant"] == "tenant-hk"

    # The API state in the restarted gateway (proxied through a
    # fresh TaskStore) shows the FAILED + marker transition.
    fresh = TaskStore(db_path=str(db_path))
    after = fresh.get("hk-on-disk")
    assert after is not None
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "[gateway restarted before task completed]"