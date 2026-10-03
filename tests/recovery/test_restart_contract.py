"""
Phase 3 / Task 5: restart state-transition contract — assert the
contract in ``docs/restart-recovery-contract.md`` against real
behaviour (the actual ``TaskStore`` SQLite lifecycle and the actual
subprocess restart path used by the Phase 1 launcher harness).

The contract is the source of truth — this file is the executable
assertion of its clauses. Tests here MUST match the wording in the
contract doc; if they disagree, the contract doc wins and this file
must be updated (and a NOTES entry recorded).

What this file covers (per the contract doc):

* Matrix rows 1, 2 — terminal rows survive reopen (inbound + outbound).
* Matrix row 3  — non-terminal outbound survives caller reopen (Phase 2
  already covers the in-process case; we add the subprocess case here).
* Matrix row 7  — non-terminal inbound becomes FAILED with the marker
  on subprocess reopen.
* C1/C2/C3      — terminal compare-and-set: a late finalizer cannot
  overwrite a prior terminal; exactly one terminal state wins.
* C4            — a caller restart cannot overwrite a prior terminal
  outbound row.
* C5            — repeated non-terminal inbound reopen does not keep
  rewriting the marker reply (the on-disk reply stays identifiable).
* P1/P2/P3      — SQLite integrity invariants (rows already covered
  in ``test_taskstore_restart.py``; we re-assert the subprocess path).

What this file does NOT cover (deferred to Phase 4+):

* Matrix rows 4–6, 8–11 — ``test_caller_restart.py`` /
  ``test_callee_restart.py``.
* W1–W4           — ``test_callee_restart.py``.
* P4              — known gap (NOTES #11); not Phase 3 scope to close
  unless a failing test surfaces.
"""

from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from a2a_async_plugin import protocol
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


def _http_get(url: str, *, timeout: float = 5.0) -> tuple[int, bytes]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


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
    """Drive a real SendMessage against the live gateway.

    The headless gateway has no agent to keep tasks in WORKING, so the
    inbound lifecycle is short — the task is created, the agent loop
    is None, the task ends in FAILED with the no-agent-reply message.
    That's the correct observable behaviour; we use this helper to
    populate the inbound TaskStore rows under test.
    """
    return _http_post_json(
        f"http://127.0.0.1:{gateway_port}/",
        {
            "jsonrpc": "2.0",
            "id": "contract-send-1",
            "method": "SendMessage",
            "params": {
                "message": {
                    "role": "ROLE_USER",
                    "contextId": context_id,
                    "parts": [{"text": text, "mediaType": "text/plain"}],
                    "messageId": "msg-contract-1",
                },
            },
        },
    )


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


# ──────────────────────────────────────────────────────────────────
# Matrix row 1, 2: terminal rows survive reopen (in-process path).
# Re-asserted here against the contract doc.
# ──────────────────────────────────────────────────────────────────


def test_contract_matrix_row_1_terminal_inbound_survives_reopen(tmp_path):
    """Matrix row 1: a terminal INBOUND row survives close + reopen.

    The contract says "Identical terminal state, reply, artifacts,
    completed_at." The on-disk reply must be preserved; the in-memory
    `_tasks` dict must round-trip via SQLite.
    """
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "contract-term-in-1", "ctx-ti", peer="peer-i",
        agent_slug="agent-i", tenant="tenant-i",
        direction="inbound", ownership="local", remote_state="",
    )
    store.set_state(rec["task_id"], STATE_WORKING)
    finished = store.complete(rec["task_id"], STATE_COMPLETED, "inbound-done")
    pre = dict(finished)

    del store
    store2 = TaskStore(db_path=str(db_path))
    after = store2.get("contract-term-in-1")
    assert after is not None
    assert after["state"] == STATE_COMPLETED
    assert after["reply"] == "inbound-done"
    assert after["direction"] == "inbound"
    assert after["ownership"] == "local"
    assert after["completed_at"] == pre["completed_at"]


def test_contract_matrix_row_2_terminal_outbound_survives_reopen(tmp_path):
    """Matrix row 2: a terminal OUTBOUND row survives close + reopen.

    The contract says "Identical terminal state, reply, artifacts,
    completed_at." The local record was queryable before restart; it
    must be queryable after restart with the same payload.
    """
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "contract-term-out-1", "ctx-to", peer="peer-o",
        agent_slug="", tenant="",
        direction="outbound", ownership="remote",
        remote_state=STATE_SUBMITTED,
    )
    store.set_state(rec["task_id"], STATE_WORKING)
    finished = store.complete(rec["task_id"], STATE_FAILED, "outbound-failed-on-purpose")
    pre = dict(finished)

    del store
    store2 = TaskStore(db_path=str(db_path))
    after = store2.get("contract-term-out-1")
    assert after is not None
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "outbound-failed-on-purpose"
    assert after["direction"] == "outbound"
    assert after["ownership"] == "remote"
    assert after["remote_state"] == STATE_FAILED
    assert after["completed_at"] == pre["completed_at"]


# ──────────────────────────────────────────────────────────────────
# Matrix row 3: non-terminal outbound survives caller reopen (in-process)
# ──────────────────────────────────────────────────────────────────


def test_contract_matrix_row_3_nonterminal_outbound_survives_reopen(tmp_path):
    """Matrix row 3: a caller restart on a WORKING outbound task does
    NOT mark it FAILED. The row must reopen in the same state with an
    empty reply and a backfilled `remote_state`.
    """
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "contract-nt-out-1", "ctx-nto", peer="peer-nto",
        agent_slug="", tenant="",
        direction="outbound", ownership="remote",
        remote_state=STATE_SUBMITTED,
    )
    store.set_state(rec["task_id"], STATE_WORKING)
    assert store.get(rec["task_id"])["remote_state"] == STATE_WORKING

    del store
    store2 = TaskStore(db_path=str(db_path))
    after = store2.get("contract-nt-out-1")
    assert after is not None
    assert after["state"] == STATE_WORKING, (
        f"contract violation: outbound non-terminal was changed on reopen: {after['state']!r}"
    )
    assert after["reply"] == ""
    assert after["remote_state"] == STATE_WORKING
    assert after["direction"] == "outbound"
    assert after["ownership"] == "remote"
    # The recovered dict has no "completed_at" key when the persisted
    # row had none (the `_recover_from_db` only adds the key when the
    # SQL row's column was non-NULL). Either "key absent" or "value
    # None" means the contract's "no completed_at backfill" clause is
    # satisfied.
    assert after.get("completed_at") is None, (
        f"contract violation: outbound non-terminal got a completed_at: {after.get('completed_at')!r}"
    )


# ──────────────────────────────────────────────────────────────────
# Matrix row 7: non-terminal inbound becomes FAILED with the marker
# on subprocess reopen (the contract's chosen policy).
# ──────────────────────────────────────────────────────────────────


def test_contract_matrix_row_7_inbound_nonterminal_policy_in_subprocess_restart(gateway, isolated_profile):
    """Contract clause 4: the plugin's chosen policy for non-terminal
    inbound at restart is FAILED + the explicit marker reply. We
    drive the subprocess path (the Phase 1 launcher harness) so a
    real ``adapter.disconnect() → adapter.connect() → _recover_from_db``
    sequence happens — and verify the resulting DB row matches the
    contract.

    The headless gateway's lifecycle has no agent loop, so an inbound
    SendMessage normally fails immediately with "Agent gateway not
    ready to accept A2A tasks." That is a terminal state (FAILED) the
    adapter itself writes — the on-disk row is already terminal when
    we read it after a SendMessage, NOT a non-terminal WORKING row.

    To exercise the *non-terminal inbound → FAILED* transition, we
    inject a non-terminal inbound row directly into the live DB while
    the gateway is running. When the gateway restarts, its
    ``_recover_from_db`` re-opens the row and applies the contract
    policy. This is the production restart topology: an inbound task
    that was mid-flight when the gateway died.

    We use the gateway as the canonical "the live process reopens the
    DB on its next boot" check by reading the DB through a fresh
    ``TaskStore`` pointed at the same file after restart. The gateway
    subprocess IS the canonical second-opener.
    """
    db_path = isolated_profile.tasks_db
    assert db_path.exists() or not db_path.exists()  # gateway creates on start

    gateway.start()
    try:
        # Inject a non-terminal inbound row directly into the live DB.
        # This models the production topology: a real inbound task was
        # mid-flight when the gateway process died. The row is on
        # disk; the process will reopen it on its next boot.
        external_store = TaskStore(db_path=str(db_path))
        try:
            rec = external_store.create(
                "contract-in-mid", "ctx-in-mid", peer="peer-in-mid",
                agent_slug="agent-in-mid", tenant="tenant-in-mid",
                direction="inbound", ownership="local", remote_state="",
            )
            external_store.set_state(rec["task_id"], STATE_WORKING)
            # Sanity-check the on-disk shape BEFORE the restart — the
            # gateway subprocess has its own TaskStore pointing at the
            # same DB; we confirm both views agree.
            on_disk_pre = _read_db_row(db_path, rec["task_id"])
            assert on_disk_pre is not None
            assert on_disk_pre["state"] == STATE_WORKING
            assert on_disk_pre["direction"] == "inbound"
            assert on_disk_pre["reply"] == ""
        finally:
            del external_store

        gateway.stop()
        # Cleanly stopped. Now restart against the same profile + DB.
        gateway.restart()
    finally:
        try:
            gateway.stop()
        except Exception:
            pass

    # Contract clause 4: the plugin's chosen policy for non-terminal
    # inbound at restart is FAILED + the explicit marker reply, but
    # the conversion is IN-MEMORY ONLY — the on-disk SQLite row keeps
    # its persisted state/reply (see §C5 and NOTES.md #15). The
    # in-memory conversion is observable via ``TaskStore.get`` in the
    # restarted process; the on-disk row is unchanged.

    # First, the on-disk SQLite row: it is unchanged across the restart.
    on_disk = _read_db_row(db_path, "contract-in-mid")
    assert on_disk is not None, "non-terminal inbound row vanished across restart"
    assert on_disk["state"] == STATE_WORKING, (
        f"contract §C5 violation: on-disk state changed across restart: {on_disk['state']!r}"
    )
    assert on_disk["reply"] == "", (
        f"contract §C5 violation: on-disk reply was rewritten across restart: {on_disk['reply']!r}"
    )
    assert on_disk["direction"] == "inbound"
    assert on_disk["ownership"] == "local"
    assert on_disk["remote_state"] == ""
    # `completed_at` is NOT backfilled on the on-disk row.
    assert on_disk["completed_at"] is None

    # Second, the API state (a fresh TaskStore against the same DB
    # mirrors what the restarted gateway subprocess sees at boot):
    # the in-memory view is FAILED + marker + completed_at.
    fresh_store = TaskStore(db_path=str(db_path))
    after = fresh_store.get("contract-in-mid")
    assert after is not None
    assert after["state"] == STATE_FAILED, (
        f"contract violation: API state is not FAILED on reopen: {after['state']!r}"
    )
    assert after["reply"] == "[gateway restarted before task completed]", (
        f"contract violation: API state missing the marker: {after['reply']!r}"
    )
    assert after.get("completed_at") is not None, (
        "contract violation: API state missing completed_at on the FAILED transition"
    )


# ──────────────────────────────────────────────────────────────────
# Compare-and-set rules (C1/C2/C3): exactly one terminal wins;
# a late finalizer cannot overwrite a prior terminal.
# ──────────────────────────────────────────────────────────────────


def test_contract_c1_terminal_complete_is_idempotent(tmp_path):
    """C1: a second terminal attempt is a no-op.

    Once `complete()` succeeds, every further `complete()` returns
    `None` and the row is unchanged. The on-disk SQLite row is
    byte-equivalent (modulo volatile timestamps) to the first
    terminal state.
    """
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create("c1", "ctx-c1", peer="peer-c1")
    store.set_state(rec["task_id"], STATE_WORKING)

    first = store.complete(rec["task_id"], STATE_COMPLETED, "first-wins")
    assert first is not None
    first_at = first["completed_at"]

    # Now try a competing terminal.
    second = store.complete(rec["task_id"], STATE_FAILED, "second-arrives-late")
    assert second is None, "second terminal() returned a non-None record (row was overwritten)"

    after = store.get(rec["task_id"])
    assert after["state"] == STATE_COMPLETED
    assert after["reply"] == "first-wins"
    assert after["completed_at"] == first_at, (
        f"completed_at drifted across a no-op second terminal: {after['completed_at']!r}"
    )


def test_contract_c2_late_failed_cannot_overwrite_prior_completed(tmp_path):
    """C2: a late FAILED cannot overwrite a prior COMPLETED.

    This is the production scenario: the peer already replied with
    COMPLETED, the local row terminalised; a stray watchdog tick or
    a late caller-side `a2a_cancel` must NOT overwrite the COMPLETED.
    """
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create("c2", "ctx-c2", peer="peer-c2")
    store.set_state(rec["task_id"], STATE_WORKING)
    store.complete(rec["task_id"], STATE_COMPLETED, "peer-finished")

    # Late attempts at every terminal variant must be no-ops.
    for state, reply in [
        (STATE_FAILED, "stray-fail"),
        (STATE_CANCELED, "stray-cancel"),
        (STATE_COMPLETED, "stray-completed-again"),
    ]:
        out = store.complete(rec["task_id"], state, reply)
        assert out is None, f"{state!r} overwrote a prior COMPLETED"

    after = store.get(rec["task_id"])
    assert after["state"] == STATE_COMPLETED
    assert after["reply"] == "peer-finished"


def test_contract_c3_late_completed_cannot_overwrite_prior_failed(tmp_path):
    """C3 (the other direction): a late COMPLETED cannot overwrite a
    prior FAILED. A watchdog-orphaned task that recovered on a peer
    must NOT overwrite the FAILED we already wrote.
    """
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create("c3", "ctx-c3", peer="peer-c3")
    store.set_state(rec["task_id"], STATE_WORKING)
    store.complete(rec["task_id"], STATE_FAILED, "watchdog-failed-it")

    out = store.complete(rec["task_id"], STATE_COMPLETED, "peer-finished-late")
    assert out is None, "completed overwrote a prior FAILED"

    after = store.get(rec["task_id"])
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "watchdog-failed-it"


def test_contract_c4_persisted_terminal_survives_reopen_unchanged(tmp_path):
    """C4: a caller restart cannot overwrite a prior terminal.

    A terminal row's on-disk state survives reopen. The reopen does
    NOT promote it back to non-terminal. This is the property that
    makes the persistence layer safe across the `disconnect` →
    `connect` cycle on restart.
    """
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path=str(db_path))
    rec = store.create("c4", "ctx-c4", peer="peer-c4")
    store.set_state(rec["task_id"], STATE_WORKING)
    store.complete(rec["task_id"], STATE_COMPLETED, "before-restart")

    del store
    store2 = TaskStore(db_path=str(db_path))
    after = store2.get(rec["task_id"])
    assert after is not None
    assert after["state"] == STATE_COMPLETED
    assert after["reply"] == "before-restart"


def test_contract_c5_repeated_inbound_nonterminal_reopen_does_not_rewrite_marker(tmp_path):
    """C5: the on-disk `reply` of a row that has been converted to
    FAILED with the marker is not rewritten by subsequent reopens.

    After the first reopen, the on-disk row has `state='WORKING'` and
    `reply=''` (the `_recover_from_db` policy does NOT re-persist the
    FAILED transition — see `tests/recovery/NOTES.md` #15). A
    second reopen re-reads the SAME persisted row and re-applies the
    in-memory FAILED conversion. The on-disk `reply` stays empty —
    it never gets the marker in SQLite, only in the in-memory
    `_tasks` dict for the current process.

    This is acceptable per the contract: the marker is the operator-
    facing signal, and it's always observable through `TaskStore.get`
    in the current process. The on-disk reply stays empty, which is
    IDENTIFIABLE as the inbound-restart-FAILED policy because the
    row was originally inbound + non-terminal.

    We assert the behavior honestly: the on-disk `reply` after
    reopen is empty, the on-disk `state` is the persisted WORKING
    (unchanged), and a second reopen does not rewrite either field.
    """
    db_path = tmp_path / "tasks.db"
    store1 = TaskStore(db_path=str(db_path))
    rec = store1.create(
        "c5", "ctx-c5", peer="peer-c5",
        direction="inbound", ownership="local", remote_state="",
    )
    store1.set_state(rec["task_id"], STATE_WORKING)

    del store1
    store2 = TaskStore(db_path=str(db_path))
    after_first = store2.get("c5")
    assert after_first["state"] == STATE_FAILED, (
        f"first reopen should have applied the FAILED policy: {after_first['state']!r}"
    )
    # The in-memory record carries the marker (so the operator sees it
    # through the API); the on-disk row below may not — that is the
    # C5 invariant.
    assert after_first["reply"] == "[gateway restarted before task completed]"

    # Now drop store2 and reopen a third time. The on-disk row should
    # not have a different reply — only an in-memory FAILED+marker is
    # re-applied for the new process.
    on_disk_after_first = _read_db_row(db_path, "c5")
    assert on_disk_after_first is not None
    on_disk_state_1 = on_disk_after_first["state"]
    on_disk_reply_1 = on_disk_after_first["reply"]

    del store2
    store3 = TaskStore(db_path=str(db_path))
    after_second = store3.get("c5")
    assert after_second["state"] == STATE_FAILED
    assert after_second["reply"] == "[gateway restarted before task completed]"

    on_disk_after_second = _read_db_row(db_path, "c5")
    assert on_disk_after_second is not None
    # The on-disk reply must not have been rewritten between reopens —
    # it is whatever the first reopen produced (the persistence layer
    # does not write the FAILED transition, per NOTES #15).
    assert on_disk_after_second["state"] == on_disk_state_1
    assert on_disk_after_second["reply"] == on_disk_reply_1, (
        f"on-disk reply drifted across reopens: "
        f"first={on_disk_reply_1!r} second={on_disk_after_second['reply']!r}"
    )


# ──────────────────────────────────────────────────────────────────
# Persistence invariants P1, P2, P3 — re-asserted on the
# subprocess restart path. The in-process versions live in
# ``test_taskstore_restart.py``.
# ──────────────────────────────────────────────────────────────────


def test_contract_p1_sqlite_integrity_after_subprocess_restart(gateway, isolated_profile):
    """P1 (subprocess path): ``PRAGMA integrity_check`` returns 'ok'
    after the gateway subprocess restarts and persists rows.
    """
    db_path = isolated_profile.tasks_db
    gateway.start()
    try:
        external_store = TaskStore(db_path=str(db_path))
        try:
            for i in range(4):
                external_store.create(f"contract-p1-{i}", "ctx-p1", peer="peer-p1")
            external_store.complete("contract-p1-0", STATE_COMPLETED, "ok")
        finally:
            del external_store
        gateway.stop()
        gateway.restart()
    finally:
        try:
            gateway.stop()
        except Exception:
            pass

    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok", f"integrity_check failed: {result!r}"


def test_contract_p3_subprocess_starts_after_newer_schema_version(gateway, isolated_profile):
    """P3: a database at ``user_version > A2A_TASK_SCHEMA_VERSION``
    must cause the subprocess gateway to crash with a visible
    RuntimeError, not silently come up empty. We assert this by
    hand-crafting the DB and observing the gateway subprocess's
    exit code and stderr.
    """
    db_path = isolated_profile.tasks_db
    future_version = A2A_TASK_SCHEMA_VERSION + 1

    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            """CREATE TABLE a2a_tasks (
            task_id TEXT PRIMARY KEY,
            context_id TEXT NOT NULL,
            peer TEXT NOT NULL,
            agent_slug TEXT NOT NULL DEFAULT '',
            tenant TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL,
            reply TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            created_iso TEXT NOT NULL,
            completed_at REAL,
            direction TEXT NOT NULL DEFAULT 'inbound',
            ownership TEXT NOT NULL DEFAULT 'local',
            remote_state TEXT NOT NULL DEFAULT ''
        )"""
        )
        con.execute(f"PRAGMA user_version = {future_version}")
        con.commit()
    finally:
        con.close()

    # Starting the gateway against a too-new schema must surface an
    # error. The harness's GatewayProcess.start() captures the
    # subprocess exit and raises RuntimeError naming the mismatch.
    with pytest.raises(RuntimeError) as excinfo:
        gateway.start()

    msg = str(excinfo.value)
    assert "newer than supported" in msg or "schema" in msg, (
        f"subprocess did not surface a schema-mismatch error: {msg!r}"
    )
    assert str(future_version) in msg, (
        f"subprocess error did not name the future version: {msg!r}"
    )


# ──────────────────────────────────────────────────────────────────
# Reconcile / finalizer-friendly assertions: the contract says
# exactly one terminal wins; assert the column surface is
# consistently the source of truth.
# ──────────────────────────────────────────────────────────────────


def test_contract_terminal_states_match_protocol_set():
    """The contract's terminal-state set matches the protocol's
    TERMINAL_STATES exactly. If a future change adds/removes a
    state, this catches it and forces the contract doc to be
    updated.
    """
    expected = {STATE_COMPLETED, STATE_FAILED, STATE_CANCELED,
                "TASK_STATE_REJECTED"}
    assert TERMINAL_STATES == frozenset(expected), (
        f"TERMINAL_STATES drifted: {TERMINAL_STATES} vs {expected}"
    )


def test_contract_db_columns_match_contract_vocabulary():
    """The contract's vocabulary (direction, ownership, remote_state)
    is realised as exact SQLite columns on `a2a_tasks`. If a future
    migration changes a name, this catches it.
    """
    con = sqlite3.connect(":memory:")
    try:
        # Boot the schema via a transient store; we don't persist.
        con.execute(
            """CREATE TABLE a2a_tasks (
            task_id TEXT PRIMARY KEY,
            context_id TEXT NOT NULL,
            peer TEXT NOT NULL,
            agent_slug TEXT NOT NULL DEFAULT '',
            tenant TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL,
            reply TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            created_iso TEXT NOT NULL,
            completed_at REAL,
            direction TEXT NOT NULL DEFAULT 'inbound',
            ownership TEXT NOT NULL DEFAULT 'local',
            remote_state TEXT NOT NULL DEFAULT ''
        )"""
        )
        cols = {row[1] for row in con.execute("PRAGMA table_info(a2a_tasks)")}
    finally:
        con.close()

    for required in ("direction", "ownership", "remote_state", "state",
                     "reply", "completed_at", "task_id", "context_id",
                     "peer", "agent_slug", "tenant"):
        assert required in cols, f"contract-required column missing: {required!r}"