"""
Phase 3 / Task 6: caller-side restart semantics on the OUTBOUND path.

The plan defines a "caller" as the gateway / agent process that owns an
outbound A2A task. A caller restart is the most common restart we
expect to see — every time the agent gateway reboots, every outbound
task it had outstanding against a peer is exposed to the
``_recover_from_db`` path in ``a2a_async_plugin.protocol.TaskStore``.

The contract (``docs/restart-recovery-contract.md`` §2 rows 3, 4, 5,
11) says:

* Matrix row 3 / 4 — a caller clean OR hard-kill restart of a
  non-terminal outbound task leaves the row WORKING (in memory and
  on disk). The `[gateway restarted]` marker is FORBIDDEN on an
  outbound record. This is the hard requirement.

* Matrix row 5 — the next caller-side GetTask against the peer
  reconciles to whatever the peer currently has (terminal or
  non-terminal). The peer's state is authoritative for outbound
  records.

* Matrix row 11 — a caller restart that follows a remote completion
  cannot overwrite the prior terminal. The persisted state survives.

* Matrix row 9 — if the peer says "task not found", the local row is
  marked FAILED with a peer-specific reply (NOT the restart marker).

We drive the outbound path via the plugin's real client tools
(``a2a_async_plugin.tools.a2a_submit``,
``a2a_async_plugin.tools.a2a_get_task``, ``a2a_await``,
``a2a_cancel``) so the test exercises the same call shapes a real
Hermes agent would invoke. The fake peer is a real HTTP server (the
Phase 1 ``tests/recovery/fake_peer.py`` helper) so the wire is the
real A2A JSON-RPC binding, not a mock.

Setup follows ``tests/recovery/NOTES.md`` #12: ``HERMES_HOME`` and
``A2A_TASKS_DB`` are pointed at hermetic paths so the in-process
``_task_store()`` does not write into the operator's real profile.

Following NOTES #8: outbound tasks are not held by the headless
gateway (it has no agent to drive them); we drive ``a2a_submit``
from the test process itself, which exercises the same code path a
real Hermes agent would invoke.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from a2a_async_plugin import protocol, tools
from a2a_async_plugin.protocol import (
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_SUBMITTED,
    STATE_WORKING,
    TaskStore,
)

from .fake_peer import FakePeer


# ──────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────


def _isolated_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point ``$HERMES_HOME`` and ``$A2A_TASKS_DB`` at hermetic
    paths under tmp_path so the in-process ``_task_store()`` does
    NOT write into the operator's real profile. See
    ``tests/recovery/NOTES.md`` #12.
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


# ──────────────────────────────────────────────────────────────────
# Task 6.A — caller restart on a non-terminal outbound task
# preserves the row (in-process TaskStore reopen).
# ──────────────────────────────────────────────────────────────────


def test_caller_clean_restart_outbound_task_remains_queryable_and_nonterminal(
    monkeypatch, tmp_path
):
    """Matrix row 3: a clean caller restart on a WORKING outbound
    task leaves the row queryable AND non-terminal.

    The hard requirement from the plan: "a caller restart alone
    NEVER creates a '[gateway restarted]' failure on an OUTBOUND
    record." Asserted here against the real ``TaskStore``
    ``_recover_from_db`` path.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        # 1. Submit an outbound task via the real tool function. Use
        # the URL-direct path so we don't need a config.yaml.
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "hold this while I restart",
            "context_id": "ctx-caller-clean",
        })
        # The string return encodes the task_id. Pull it out.
        assert "task-" in submit_result, (
            f"a2a_submit did not return a task id: {submit_result!r}"
        )
        # The format is "[<peer> · task <id> · context <ctx> · <state>]\n..."
        task_id = submit_result.split("task ", 1)[1].split(" ", 1)[0]
        assert task_id.startswith("task-"), (
            f"could not parse task id from: {submit_result!r}"
        )

        # 2. The local DB row was created by a2a_submit with the
        # outbound/remote metadata. Read it directly.
        db_path = Path(os.environ["A2A_TASKS_DB"])
        pre = _read_db_row(db_path, task_id)
        assert pre is not None
        assert pre["direction"] == "outbound"
        assert pre["ownership"] == "remote"
        assert pre["state"] in (STATE_SUBMITTED, STATE_WORKING), (
            f"outbound task did not start non-terminal: {pre['state']!r}"
        )
        assert pre["reply"] == ""
        assert pre["remote_state"] in (STATE_SUBMITTED, STATE_WORKING)

        # 3. SIMULATE a caller restart: drop the in-memory TaskStore
        # in this process by deleting the ``_task_store`` cached
        # singleton, and force a new TaskStore against the same DB
        # file. (In production this is exactly what happens when the
        # agent gateway process restarts: a brand-new ``TaskStore``
        # instance opens the same SQLite file and runs
        # ``_recover_from_db``.)
        # The plugin's tools._task_store is a module-level function
        # that constructs a new TaskStore every call — no in-process
        # cache to evict. Just construct a fresh TaskStore directly.
        del pre
        fresh_store = TaskStore()
        after = fresh_store.get(task_id)

        # 4. The row survives and is still non-terminal. The
        # `[gateway restarted]` marker is FORBIDDEN on outbound.
        assert after is not None, "outbound row vanished across caller reopen"
        assert after["state"] not in (STATE_FAILED,), (
            f"contract violation: outbound row was forced FAILED on caller restart: {after['state']!r}"
        )
        assert "[gateway restarted" not in (after.get("reply") or ""), (
            f"contract violation: outbound row has the [gateway restarted] marker: {after['reply']!r}"
        )
        assert after["direction"] == "outbound"
        assert after["ownership"] == "remote"
        # `completed_at` is not backfilled on a non-terminal reopen.
        assert after.get("completed_at") is None

        # 5. The DB row also survives (durable source of truth).
        on_disk = _read_db_row(db_path, task_id)
        assert on_disk is not None
        assert on_disk["state"] not in (STATE_FAILED,), (
            f"contract violation: on-disk outbound was forced FAILED: {on_disk['state']!r}"
        )
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# Task 6.B — caller HARD-KILL restart on a non-terminal outbound
# task preserves the row (subprocess-level, SIGKILL).
# ──────────────────────────────────────────────────────────────────


def test_caller_hardkill_outbound_task_remains_queryable_and_nonterminal(
    monkeypatch, tmp_path
):
    """Matrix row 4: a hard-kill caller restart on a WORKING outbound
    task leaves the row queryable AND non-terminal — the same
    guarantee as a clean restart.

    A SIGKILL of the caller must not be distinguishable in DB rows
    from a clean restart. We drive a real subprocess that uses the
    plugin's real ``_task_store()`` + a real SendMessage wire call,
    SIGKILL the subprocess mid-flight, then read the DB from the
    test process.
    """
    hermes_home = _isolated_hermes_home(monkeypatch, tmp_path)
    tasks_db = hermes_home / "a2a_tasks.db"
    python_path = sys.executable

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    peer_url = peer.base_url()

    # Worker script — runs in its own process; SIGKILL'd by the test.
    # The script does ONLY:
    #   1. POST SendMessage to peer
    #   2. Construct TaskStore and persist the outbound row
    #   3. Sit idle (the test will SIGKILL it)
    worker = tmp_path / "caller_worker.py"
    # Use sys.executable's directory + the plugin repo + the
    # hermes-agent tree (which the parent pytest inherited) as the
    # worker's import path. We can't rely on the parent's
    # PYTHONPATH because monkeypatch may have replaced it; the
    # worker's PYTHONPATH must include both the hermes source tree
    # (for ``hermes_constants`` etc.) and the plugin repo (for
    # ``a2a_async_plugin``).
    plugin_repo = "/home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin"
    hermes_tree = "/home/hermes/.hermes/hermes-agent"
    worker.write_text(f"""
import os, json, time, sys, urllib.request
# Ensure the plugin repo is importable in the worker process —
# the parent pytest's PYTHONPATH doesn't always include it.
sys.path.insert(0, {json.dumps(plugin_repo)})
sys.path.insert(0, {json.dumps(hermes_tree)})
sys.path.insert(0, {json.dumps(str(Path(__file__).parent))})
# We must NOT import the plugin in the worker — it does too much
# (config lookup, registry). Just inline the SendMessage + DB write.
from a2a_async_plugin import protocol
from a2a_async_plugin.protocol import TaskStore, STATE_WORKING, STATE_SUBMITTED

peer_url = {json.dumps(peer_url)}

# Drive the wire (same envelope a2a_submit emits).
body = {{
    "jsonrpc": "2.0", "id": "caller-hardkill-1", "method": "SendMessage",
    "params": {{
        "message": {{
            "role": "ROLE_USER",
            "contextId": "ctx-caller-hardkill",
            "parts": [{{"text": "hold during SIGKILL", "mediaType": "text/plain"}}],
            "messageId": "msg-caller-hardkill-1",
        }},
        "configuration": {{"returnImmediately": True}},
    }},
}}
req = urllib.request.Request(
    peer_url + "/", data=json.dumps(body).encode(),
    headers={{"Content-Type": "application/json", "A2A-Version": "1.0"}},
)
with urllib.request.urlopen(req, timeout=10) as r:
    result = json.loads(r.read().decode())
task_id = result["result"]["task"]["id"]

# Persist locally (mirrors a2a_submit's TaskStore.create call).
store = TaskStore()
store.create(task_id, "ctx-caller-hardkill", peer_url,
             direction="outbound", ownership="remote",
             remote_state=result["result"]["task"]["status"]["state"])
# Print the task_id so the test can read it.
print(task_id)
sys.stdout.flush()
# Sit idle — the test SIGKILLs us.
time.sleep(60)
""", encoding="utf-8")

    proc = subprocess.Popen(
        [python_path, "-u", str(worker)],
        env={**os.environ,
              "HERMES_HOME": str(hermes_home),
              "A2A_TASKS_DB": str(tasks_db)},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        # Read the task_id the worker printed. We use a polling
        # loop instead of a single readline so a slow module import
        # or DNS lookup doesn't trip the test.
        import select
        deadline = time.time() + 8.0
        buf = b""
        while time.time() < deadline:
            rl, _, _ = select.select([proc.stdout], [], [], 0.2)
            if rl:
                chunk = os.read(proc.stdout.fileno(), 4096)
                if not chunk:
                    break
                buf += chunk
                if b"\n" in buf:
                    break
        line = buf.decode().strip().splitlines()[0] if buf else ""
        if not line.startswith("task-"):
            stderr = (proc.stderr.read() if proc.stderr else b"").decode(
                "utf-8", errors="replace"
            )
            raise AssertionError(
                f"worker did not print a task id: stdout={line!r} stderr={stderr!r}"
            )
        task_id = line

        # SIGKILL the worker — no cleanup handlers run.
        proc.kill()
        proc.wait(timeout=5)

        # Reopen the DB from the test process.
        on_disk = _read_db_row(tasks_db, task_id)
        assert on_disk is not None, "outbound row vanished after SIGKILL of caller"
        assert on_disk["direction"] == "outbound"
        assert on_disk["ownership"] == "remote"
        assert on_disk["state"] not in (STATE_FAILED,), (
            f"contract violation: hard-kill forced outbound FAILED: {on_disk['state']!r}"
        )
        assert "[gateway restarted" not in (on_disk.get("reply") or ""), (
            f"contract violation: hard-kill wrote the [gateway restarted] marker "
            f"to an outbound record: {on_disk['reply']!r}"
        )

        # A fresh TaskStore against the same DB also confirms the
        # row is non-terminal (the in-memory view mirrors what the
        # restarted caller's first ``get`` call returns).
        fresh = TaskStore()
        after = fresh.get(task_id)
        assert after is not None
        assert after["state"] not in (STATE_FAILED,), (
            f"contract violation: API view is FAILED on hard-kill reopen: {after['state']!r}"
        )
        assert "[gateway restarted" not in (after.get("reply") or ""), (
            f"contract violation: API view has the restart marker on an outbound record"
        )
    finally:
        try:
            proc.kill()
            proc.wait(timeout=3)
        except Exception:
            pass
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# Task 6.C — peer completes the task while caller is down; caller
# reconciles on next GetTask (matrix row 5).
# ──────────────────────────────────────────────────────────────────


def test_peer_completes_while_caller_is_down_then_caller_reconciles(
    monkeypatch, tmp_path
):
    """Matrix row 5: peer completes the task while the caller is
    down; on the caller's next ``a2a_get_task`` against the peer,
    the local row reconciles to the peer's terminal state.

    The local row at restart moment is unchanged (still WORKING /
    SUBMITTED). Reconciliation is a wire-level concern: the next
    ``GetTask`` call against the peer drives ``complete()`` on the
    local row (terminal compare-and-set).
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        # 1. Submit an outbound task via the real tool function.
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "hold this while caller restarts",
            "context_id": "ctx-caller-recon",
        })
        task_id = submit_result.split("task ", 1)[1].split(" ", 1)[0]
        assert task_id.startswith("task-")

        # 2. The peer still holds the task. Confirm the row exists
        # locally.
        pre = _read_db_row(db_path, task_id)
        assert pre is not None
        pre_state = pre["state"]

        # 3. SIMULATE the caller going down: close the test-side
        # TaskStore. The "down" period is just a wall-clock gap;
        # there's no in-process restart here.
        # Tell the peer to complete the task NOW while the caller
        # would be down (in production, this is the peer's own
        # delayed-work loop finishing after the caller died).
        # We hit the peer's control port directly.
        peer_ctrl = peer.control_url("complete")
        req = urllib.request.Request(
            peer_ctrl,
            data=json.dumps({"task_id": task_id, "reply": "completed while caller was down"}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            done = json.loads(r.read().decode())
        assert done["task"]["status"]["state"] == STATE_COMPLETED

        # 4. The caller "comes back up": a fresh in-process
        # ``TaskStore`` reads the SAME on-disk row (still the
        # pre-completion state — the local row didn't observe the
        # peer's transition).
        fresh = TaskStore()
        after_reopen = fresh.get(task_id)
        assert after_reopen is not None
        # The local row is whatever it was at submit time — the
        # peer's terminalisation is NOT yet mirrored locally.
        assert after_reopen["state"] == pre_state, (
            f"local row changed without a caller-initiated GetTask: {after_reopen['state']!r}"
        )

        # 5. Now drive the real ``a2a_get_task`` tool handler — it
        # issues a GetTask against the peer and reflects the
        # terminal state into the local TaskStore via
        # ``TaskStore.complete()``.
        get_result = tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })
        assert "completed" in get_result.lower(), (
            f"a2a_get_task did not report the peer's terminal state: {get_result!r}"
        )

        # 6. The local row is now terminal (compare-and-set: the
        # peer-reported state wins). The DB row reflects the new
        # terminal state — a fresh TaskStore against the DB
        # confirms it.
        con2 = sqlite3.connect(str(db_path))
        try:
            db_row = con2.execute(
                "SELECT state, reply, completed_at FROM a2a_tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        finally:
            con2.close()
        assert db_row is not None
        assert db_row[0] == STATE_COMPLETED, (
            f"DB state was not terminal after a2a_get_task: {db_row[0]!r}"
        )
        assert db_row[1] == "completed while caller was down"
        assert db_row[2] is not None
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# Task 6.D — caller restart AFTER peer remote completion does NOT
# overwrite the prior terminal (matrix row 11).
# ──────────────────────────────────────────────────────────────────


def test_caller_restart_does_not_overwrite_prior_remote_terminal(
    monkeypatch, tmp_path
):
    """Matrix row 11 / C4: a caller restart that follows a remote
    terminal completion cannot overwrite the prior terminal. The
    persisted terminal state survives reopen; the peer's terminal
    is the source of truth for outbound records.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        # 1. Submit + reconcile to terminal.
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "will complete before restart",
            "context_id": "ctx-caller-prior",
        })
        task_id = submit_result.split("task ", 1)[1].split(" ", 1)[0]

        # Have the peer complete BEFORE the caller restart.
        req = urllib.request.Request(
            peer.control_url("complete"),
            data=json.dumps({"task_id": task_id, "reply": "completed pre-restart"}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            done = json.loads(r.read().decode())
        assert done["task"]["status"]["state"] == STATE_COMPLETED

        # Reconcile.
        tools.a2a_get_task({
            "agent": peer.base_url(),
            "task_id": task_id,
        })

        # 2. Capture the persisted terminal state.
        pre = _read_db_row(db_path, task_id)
        assert pre is not None
        assert pre["state"] == STATE_COMPLETED
        assert pre["reply"] == "completed pre-restart"
        pre_completed_at = pre["completed_at"]

        # 3. Caller restart: fresh TaskStore.
        fresh = TaskStore()
        after = fresh.get(task_id)
        assert after is not None
        assert after["state"] == STATE_COMPLETED, (
            f"contract violation: caller restart overwrote prior terminal: {after['state']!r}"
        )
        assert after["reply"] == "completed pre-restart"
        assert after["completed_at"] == pre_completed_at, (
            f"contract violation: completed_at drifted across restart: "
            f"pre={pre_completed_at!r} post={after['completed_at']!r}"
        )

        # 4. C2 sanity: a late attempt to mark the row FAILED must
        # not overwrite the COMPLETED.
        out = fresh.complete(task_id, STATE_FAILED, "stray-fail")
        assert out is None
        again = fresh.get(task_id)
        assert again["state"] == STATE_COMPLETED
        assert again["reply"] == "completed pre-restart"
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# Task 6.E — remote task not-found yields a peer-specific reply,
# NOT the [gateway restarted] marker (matrix row 9).
# ──────────────────────────────────────────────────────────────────


def test_remote_task_not_found_does_not_get_restart_marker(
    monkeypatch, tmp_path
):
    """Matrix row 9: if the peer says the task is not found, the
    local row is marked FAILED with a peer-specific reply. It is
    NOT marked with the `[gateway restarted]` marker — that marker
    is reserved for the callee-restart policy on inbound rows.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        # 1. Submit an outbound task. The peer holds it.
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "will be deleted on peer",
            "context_id": "ctx-caller-nf",
        })
        task_id = submit_result.split("task ", 1)[1].split(" ", 1)[0]

        # 2. Now construct a second FakePeer that knows NOTHING
        # about this task — it will respond "not found" to any
        # GetTask on this id.
        empty_peer_dir = tmp_path / "empty_peer"
        empty_peer = FakePeer(empty_peer_dir)
        empty_peer.start()
        try:
            # Direct wire-level GetTask against the empty peer —
            # the plugin's a2a_get_task will translate the peer's
            # "task not found" error into a local FAILED transition
            # (see ``a2a_get_task`` in tools.py — it calls
            # ``_task_store().complete(task_id, STATE_FAILED, ...)``
            # on the error branch).
            resp = _http_post_json(
                empty_peer.base_url() + "/",
                {"jsonrpc": "2.0", "id": "nf-1", "method": "GetTask",
                 "params": {"id": task_id}},
            )
            assert "error" in resp, f"empty peer should reject GetTask: {resp!r}"

            # Now drive the plugin's own a2a_get_task against the
            # empty peer — it must convert the peer's not-found
            # into a local FAILED + the plugin's error-format reply
            # (NOT the restart marker).
            out = tools.a2a_get_task({
                "agent": empty_peer.base_url(),
                "task_id": task_id,
            })
            assert "[gateway restarted" not in out, (
                f"contract violation: not-found was labelled with the "
                f"[gateway restarted] marker: {out!r}"
            )
            # The plugin formats the error as
            # ``Error: peer '...' returned <code>: <message>``.
            assert out.startswith("Error:"), f"unexpected reply: {out!r}"

            # The local DB row was marked FAILED.
            db_path = Path(os.environ["A2A_TASKS_DB"])
            on_disk = _read_db_row(db_path, task_id)
            assert on_disk is not None
            assert on_disk["state"] == STATE_FAILED
            assert "[gateway restarted" not in (on_disk.get("reply") or ""), (
                f"on-disk reply was labelled with the restart marker for an "
                f"outbound not-found failure: {on_disk['reply']!r}"
            )
        finally:
            empty_peer.stop()
    finally:
        peer.stop()


# ──────────────────────────────────────────────────────────────────
# Task 6.F — local metadata identifies outbound direction/peer
# (DB columns direction/ownership/remote_state are populated
# correctly).
# ──────────────────────────────────────────────────────────────────


def test_local_metadata_identifies_outbound_direction_and_peer(
    monkeypatch, tmp_path
):
    """Per the plan: "Local metadata identifies outbound direction
    and remote peer." Drive a real ``a2a_submit`` and inspect the
    DB row to confirm ``direction='outbound'``,
    ``ownership='remote'``, and ``peer`` carries the peer's URL.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    peer = FakePeer(tmp_path / "peer")
    peer.start()
    try:
        submit_result = tools.a2a_submit({
            "agent": peer.base_url(),
            "message": "metadata check",
            "context_id": "ctx-meta-out",
        })
        task_id = submit_result.split("task ", 1)[1].split(" ", 1)[0]

        # Direct DB read.
        on_disk = _read_db_row(db_path, task_id)
        assert on_disk is not None
        assert on_disk["direction"] == "outbound"
        assert on_disk["ownership"] == "remote"
        # ``peer`` is the configured peer label (here, the URL
        # because we used the URL-direct path).
        assert on_disk["peer"] == peer.base_url()
        # ``remote_state`` carries the latest peer-reported state.
        assert on_disk["remote_state"] in (STATE_SUBMITTED, STATE_WORKING)
        # Sanity: agent_slug / tenant are empty for outbound (we
        # didn't set them; the tool's contract is "this gateway is
        # the caller, not the agent").
        assert on_disk["agent_slug"] == ""
        assert on_disk["tenant"] == ""

        # And via the in-memory TaskStore view.
        fresh = TaskStore()
        rec = fresh.get(task_id)
        assert rec is not None
        assert rec["direction"] == "outbound"
        assert rec["ownership"] == "remote"
        assert rec["peer"] == peer.base_url()
    finally:
        peer.stop()