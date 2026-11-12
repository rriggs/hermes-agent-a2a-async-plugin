"""Deferred completion for inbound A2A tasks (issue #25).

An inbound task used to complete on the first turn-final reply, so an agent that
started background work and answered with a progress note closed the task before
the work finished. The later report had no open task to land in and was dropped
with a debug log and a success return.

The fix is the ``[WORKING]`` deferral marker: the reply is recorded as
``TASK_STATE_WORKING`` with the progress text in ``status.message``, the reply
waiter stays armed, and the next turn-final reply in the context completes the
task. When the waiter is gone, the durable row is completed instead.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from concurrent.futures import Future

import pytest

from a2a_async_plugin import adapter as adapter_mod
from a2a_async_plugin import protocol
from a2a_async_plugin import security


CTX = "ctx-deferred-1"


@pytest.fixture
def isolated_home(monkeypatch, tmp_path):
    home = tmp_path / "hermes_home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_TASKS_DB", str(home / "a2a_tasks.db"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    return home


@pytest.fixture
def store(isolated_home):
    return protocol.TaskStore()


def _make_adapter(store, monkeypatch) -> "adapter_mod.A2AAdapter":
    from gateway.config import PlatformConfig  # type: ignore

    config = PlatformConfig(enabled=True, extra={"port": 0, "agent_name": "deferred-test"})
    adp = adapter_mod.A2AAdapter(config)
    adp.tasks = store
    adp.gateway_runner = None
    return adp


def _add_waiter(adp, task_id: str, context_id: str = CTX, *,
                output_modes=None, accepted_output_modes=None) -> Future:
    fut = adp._add_pending(
        task_id, context_id,
        output_modes=output_modes, accepted_output_modes=accepted_output_modes)
    adp.tasks.create(task_id, context_id, "peer-a")
    return fut


def _rec(store, task_id: str) -> dict:
    rec = store.get(task_id)
    assert rec is not None
    return rec


class _Event:
    def __init__(self, task_id: str = "", context_id: str = CTX, *, internal: bool = False):
        self.message_id = task_id
        self.internal = internal
        self.source = type("Source", (), {"chat_id": context_id})()


# ---------------------------------------------------------------------------
# The deferral itself
# ---------------------------------------------------------------------------


def test_working_marker_keeps_the_task_non_terminal(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    waiter = _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")

    asyncio.run(adp.send(
        CTX, "[WORKING] build at 15%, will report the resource table",
        metadata={"notify": True},
    ))

    assert not waiter.done(), "a deferred reply must leave the caller's waiter armed"
    rec = _rec(adp.tasks, "task-1")
    assert rec["state"] == protocol.STATE_WORKING
    assert rec["reply"] == "build at 15%, will report the resource table"
    assert rec["deferred_at"] > 0
    task = protocol.TaskStore.to_task(rec)
    assert "artifacts" not in task
    assert task["status"]["message"]["parts"][0]["text"] == (
        "build at 15%, will report the resource table")
    assert adp._deferred[CTX] == ["task-1"]


def test_later_report_completes_the_deferred_task(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    waiter = _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")
    asyncio.run(adp.send(CTX, "[WORKING] working", metadata={"notify": True}))

    # The background-completion wake is a synthetic event with no message id, so
    # this send has no turn record. It must land on the deferred task.
    asyncio.run(adp.send(CTX, "the real report", metadata={"notify": True}))

    assert waiter.done()
    state, reply = waiter.result(timeout=0)
    assert (state, reply) == (protocol.STATE_COMPLETED, "the real report")

    # The async finalizer thread does this after the future resolves.
    adp._finalize_task(
        {"task_id": "task-1", "context_id": CTX, "peer": "peer-a",
         "started": time.time()},
        state, reply,
    )
    rec = _rec(adp.tasks, "task-1")
    assert rec["state"] == protocol.STATE_COMPLETED
    assert rec["reply"] == "the real report"
    assert rec["deferred_at"] == 0
    task = protocol.TaskStore.to_task(rec)
    assert task["artifacts"][0]["parts"][0]["text"] == "the real report"
    assert adp._deferred == {}


def test_later_report_completes_a_deferred_row_without_a_waiter(isolated_home, monkeypatch):
    """A blocking caller may disconnect; the row must still take the report."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")
    asyncio.run(adp.send(CTX, "[WORKING] working", metadata={"notify": True}))
    adp._pop_pending("task-1")

    asyncio.run(adp.send(CTX, "final report", metadata={"notify": True}))

    rec = _rec(adp.tasks, "task-1")
    assert rec["state"] == protocol.STATE_COMPLETED
    assert rec["reply"] == "final report"
    assert rec["deferred_at"] == 0


def test_the_durable_row_completes_the_report_without_in_memory_bookkeeping(
        isolated_home, monkeypatch):
    """An adapter reload loses _deferred; the durable deferred row still takes it."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")
    asyncio.run(adp.send(CTX, "[WORKING] working", metadata={"notify": True}))
    adp._pop_pending("task-1")
    adp._deferred.clear()

    asyncio.run(adp.send(CTX, "final report", metadata={"notify": True}))

    rec = _rec(adp.tasks, "task-1")
    assert rec["state"] == protocol.STATE_COMPLETED
    assert rec["reply"] == "final report"


def test_the_async_finalizer_waits_through_a_deferral(isolated_home, monkeypatch):
    """The real finalizer thread must not finalize on the progress note."""
    monkeypatch.setenv("A2A_ASYNC_TIMEOUT", "5")
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    fut = adp._add_pending("task-1", CTX)
    adp.tasks.create("task-1", CTX, "peer-a")
    adp.tasks.set_state("task-1", protocol.STATE_WORKING)
    pending = {
        "task_id": "task-1", "context_id": CTX, "peer": "peer-a",
        "future": fut, "created_iso": protocol.now_iso(), "started": time.time(),
    }
    finalizer = threading.Thread(target=adp._finalize_in_background, args=(pending,),
                                 daemon=True)
    finalizer.start()
    try:
        adp._note_turn_start(CTX, "task-1")
        asyncio.run(adp.send(CTX, "[WORKING] working", metadata={"notify": True}))
        time.sleep(0.15)
        assert finalizer.is_alive(), "the finalizer finalized the progress note"
        assert _rec(adp.tasks, "task-1")["state"] == protocol.STATE_WORKING

        asyncio.run(adp.send(CTX, "the real report", metadata={"notify": True}))
        finalizer.join(timeout=2.0)
    finally:
        if finalizer.is_alive():
            adp._pop_pending("task-1")

    assert not finalizer.is_alive()
    rec = _rec(adp.tasks, "task-1")
    assert rec["state"] == protocol.STATE_COMPLETED
    assert rec["reply"] == "the real report"


def test_caller_walk_away_does_not_cancel_a_deferred_task(isolated_home, monkeypatch):
    """The blocking wait ending is not the work ending."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")
    asyncio.run(adp.send(CTX, "[WORKING] working", metadata={"notify": True}))
    adp._pop_pending("task-1")

    state, reply = adp._finalize_task(
        {"task_id": "task-1", "context_id": CTX, "peer": "peer-a",
         "started": time.time()},
        protocol.STATE_CANCELED,
        "[canceled by caller: reply timeout expired]",
    )

    assert (state, reply) == (protocol.STATE_WORKING, "working")
    assert _rec(adp.tasks, "task-1")["state"] == protocol.STATE_WORKING


def test_working_progress_is_egress_redacted(isolated_home, monkeypatch):
    """Progress is outbound egress on its own; final-report redaction is too late."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    _add_waiter(adp, "task-progress")
    adp._note_turn_start(CTX, "task-progress")
    text = "Contact synthetic-review@example.invalid for status"
    expected = security.redact_outbound(text)
    assert expected != text, "the control must actually redact"

    asyncio.run(adp.send(CTX, "[WORKING] " + text, metadata={"notify": True}))

    rec = _rec(adp.tasks, "task-progress")
    rendered = protocol.TaskStore.to_task(rec)["status"]["message"]["parts"][0]["text"]
    assert rendered == expected
    assert "synthetic-review@example.invalid" not in rec["reply"]


def test_a_steered_arrival_does_not_steal_the_deferred_report(isolated_home, monkeypatch):
    """The wake turn's reply belongs to the arrival AND the deferred task."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    original = _add_waiter(adp, "task-original")
    adp._note_turn_start(CTX, "task-original")
    asyncio.run(adp.send(CTX, "[WORKING] building", metadata={"notify": True}))

    # Synthetic background turn: internal, no message id.
    wake = _Event("", internal=True)
    asyncio.run(adp.on_processing_start(wake))  # type: ignore[arg-type]
    arriving = _add_waiter(adp, "task-arriving")
    adp._note_arrival(CTX, "task-arriving", folded_into_turn=True)

    asyncio.run(adp.send(CTX, "the completed build report", metadata={"notify": True}))

    assert arriving.result(timeout=0) == (
        protocol.STATE_COMPLETED, "the completed build report")
    assert original.result(timeout=0) == (
        protocol.STATE_COMPLETED, "the completed build report")


@pytest.mark.parametrize("disconnect", [False, True])
def test_a_deferred_report_keeps_json_negotiation(isolated_home, monkeypatch, disconnect):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    fut = _add_waiter(
        adp, "task-json", output_modes=["application/json"],
        accepted_output_modes=["application/json"])
    adp._note_turn_start(CTX, "task-json")
    asyncio.run(adp.send(CTX, "[WORKING] building", metadata={"notify": True}))
    if disconnect:
        adp._pop_pending("task-json")

    asyncio.run(adp.send(CTX, '{"result": 42}', metadata={"notify": True}))

    if not disconnect:
        state, reply = fut.result(timeout=0)
        adp._finalize_task(
            {"task_id": "task-json", "context_id": CTX, "peer": "peer-a",
             "started": time.time()},
            state, reply,
        )
    rec = _rec(adp.tasks, "task-json")
    parts = protocol.TaskStore.to_task(rec)["artifacts"][0]["parts"]
    assert parts == [protocol.data_part({"result": 42})], parts


@pytest.mark.parametrize("disconnect", [False, True])
def test_a_deferred_report_keeps_file_parts(isolated_home, monkeypatch, disconnect):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    fut = _add_waiter(
        adp, "task-file", output_modes=["image/png"],
        accepted_output_modes=["image/png"])
    adp._note_turn_start(CTX, "task-file")
    asyncio.run(adp.send(CTX, "[WORKING] building", metadata={"notify": True}))
    if disconnect:
        adp._pop_pending("task-file")
    path = isolated_home / "report.png"
    path.write_bytes(b"synthetic-image-payload")

    asyncio.run(adp.send(CTX, f"report\nMEDIA:{path}", metadata={"notify": True}))

    if not disconnect:
        state, reply = fut.result(timeout=0)
        adp._finalize_task(
            {"task_id": "task-file", "context_id": CTX, "peer": "peer-a",
             "started": time.time()},
            state, reply,
        )
    rec = _rec(adp.tasks, "task-file")
    assert any("raw" in part for part in rec["reply_parts"]), rec["reply_parts"]


def test_a_second_working_reply_refreshes_the_progress(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    waiter = _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")
    asyncio.run(adp.send(CTX, "[WORKING] 15%", metadata={"notify": True}))

    asyncio.run(adp.send(CTX, "[WORKING] 40%", metadata={"notify": True}))

    assert not waiter.done()
    assert _rec(adp.tasks, "task-1")["reply"] == "40%"
    assert _rec(adp.tasks, "task-1")["state"] == protocol.STATE_WORKING
    assert adp._deferred[CTX] == ["task-1"]


def test_processing_complete_does_not_complete_a_deferred_task(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    waiter = _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")
    asyncio.run(adp.send(CTX, "[WORKING] working", metadata={"notify": True}))

    evt = _Event("task-1")
    asyncio.run(adp.on_processing_complete(evt, adapter_mod.ProcessingOutcome.SUCCESS))  # type: ignore[arg-type]
    assert not waiter.done(), "turn end must not resolve a deferred task"
    assert _rec(adp.tasks, "task-1")["state"] == protocol.STATE_WORKING


def test_a_plain_reply_still_completes(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    waiter = _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")

    asyncio.run(adp.send(CTX, "the answer", metadata={"notify": True}))

    assert waiter.result(timeout=0) == (protocol.STATE_COMPLETED, "the answer")


# ---------------------------------------------------------------------------
# TaskStore: durable deferral, watchdog, migration
# ---------------------------------------------------------------------------


def test_defer_marks_the_row_and_clears_on_completion(isolated_home):
    store = protocol.TaskStore()
    store.create("task-1", CTX, "peer-a")

    store.defer("task-1", protocol.STATE_WORKING, "still working")

    rec = _rec(store, "task-1")
    assert rec["state"] == protocol.STATE_WORKING
    assert rec["reply"] == "still working"
    assert rec["deferred_at"] > 0

    store.complete("task-1", protocol.STATE_COMPLETED, "done")
    assert _rec(store, "task-1")["deferred_at"] == 0


def test_fail_orphans_exempts_input_required(isolated_home):
    store = protocol.TaskStore()
    store.create("task-input", CTX, "peer-a")
    store.complete("task-input", protocol.STATE_INPUT_REQUIRED, "which branch?")
    store._tasks["task-input"]["created_at"] = time.time() - 301

    assert store.fail_orphans(300, protected=set()) == []
    assert _rec(store, "task-input")["state"] == protocol.STATE_INPUT_REQUIRED


def test_fail_orphans_exempts_a_fresh_deferred_task(isolated_home):
    store = protocol.TaskStore()
    store.create("task-deferred", CTX, "peer-a")
    store.defer("task-deferred", protocol.STATE_WORKING, "still working")
    # Old enough for the normal orphan window, but the deferral is fresh.
    store._tasks["task-deferred"]["created_at"] = time.time() - 301

    assert store.fail_orphans(300, protected=set()) == []
    assert _rec(store, "task-deferred")["state"] == protocol.STATE_WORKING


def test_fail_orphans_expires_a_deferred_task_after_the_deferred_timeout(isolated_home):
    store = protocol.TaskStore()
    store.create("task-deferred", CTX, "peer-a")
    store.defer("task-deferred", protocol.STATE_WORKING, "still working")
    store._tasks["task-deferred"]["deferred_at"] = time.time() - 101

    failed = store.fail_orphans(300, protected=set(), deferred_timeout_seconds=100)

    assert failed == ["task-deferred"]
    rec = _rec(store, "task-deferred")
    assert rec["state"] == protocol.STATE_FAILED
    assert "deferred task expired" in rec["reply"]


def test_deferred_at_survives_a_v10_migration(tmp_path):
    """An existing v10 database gains the column without losing its rows."""
    db_path = tmp_path / "tasks-v10.db"
    store = protocol.TaskStore(str(db_path))
    store.create("task-v10", "ctx-v10", "peer")
    store.complete("task-v10", protocol.STATE_COMPLETED, "legacy reply")
    with sqlite3.connect(db_path) as con:
        con.execute("ALTER TABLE a2a_tasks DROP COLUMN deferred_at")
        con.execute("ALTER TABLE a2a_tasks DROP COLUMN output_settings")
        con.execute("PRAGMA user_version = 10")

    migrated = protocol.TaskStore(str(db_path))
    rec = _rec(migrated, "task-v10")
    assert rec["reply"] == "legacy reply"
    assert rec["deferred_at"] == 0
    assert rec["output_settings"] == {}
    with sqlite3.connect(db_path) as con:
        columns = {row[1] for row in con.execute("PRAGMA table_info(a2a_tasks)")}
        version = int(con.execute("PRAGMA user_version").fetchone()[0])
    assert {"deferred_at", "output_settings"} <= columns
    assert version == protocol.A2A_TASK_SCHEMA_VERSION
