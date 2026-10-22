"""Operator commands for task ownership: list, release, transfer (issue #10).

Both adapters (``hermes a2a-tasks`` and ``/a2a tasks``) call the same functions in
``task_admin``, so these tests assert the durable effects once and the surface behavior twice.
"""

from __future__ import annotations

import importlib.util
import logging
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PKG = "a2a_task_admin_under_test"
spec = importlib.util.spec_from_file_location(
    PKG, ROOT / "a2a_async_plugin" / "__init__.py",
    submodule_search_locations=[str(ROOT / "a2a_async_plugin")])
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
sys.modules[PKG] = module
spec.loader.exec_module(module)

protocol = importlib.import_module(f"{PKG}.protocol")  # noqa: E402
ownership = importlib.import_module(f"{PKG}.ownership")  # noqa: E402
watches = importlib.import_module(f"{PKG}.watches")  # noqa: E402
task_admin = importlib.import_module(f"{PKG}.task_admin")  # noqa: E402

SESSION_A = "sess-A"
SESSION_B = "sess-B"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "a2a_tasks.db"))
    try:
        from gateway.session_context import clear_session_vars, set_session_vars
    except Exception:
        # Standalone execution (no Hermes checkout importable): the command core does not need
        # the gateway's session vars, so the tests still run.
        yield
        return
    tokens = set_session_vars(platform="telegram", source="telegram", chat_id="42",
                              chat_type="dm", session_key="agent:main:telegram:dm:42",
                              session_id=SESSION_A)
    try:
        yield
    finally:
        clear_session_vars(tokens)


def _store() -> ownership.OwnershipStore:
    return ownership.OwnershipStore()


def _task(task_id: str, peer: str = "peer", session: str = "") -> None:
    protocol.TaskStore().create(task_id, "ctx-1", peer, direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    if session:
        assert _store().acquire(task_id, session)


def _watch(session: str, tasks: list[tuple[str, str]], watch_id: str = "w1") -> None:
    watches.WatchStore().create(watch_id=watch_id, session_id=session,
                                target={"session_key": "agent:main:telegram:dm:42"},
                                tasks=tasks, ttl_s=3600)


def _pending(watch_id: str, task_id: str, event_id: str = "ev1") -> None:
    watches.WatchStore().enqueue_publication(
        watch_id=watch_id, event_id=event_id, content="queued delivery",
        metadata={"source": "a2a", "task_id": task_id, "watch_id": watch_id})


def _pending_ids() -> list[str]:
    with sqlite3.connect(_store().db_path) as con:
        return [row[0] for row in con.execute(
            "SELECT event_id FROM a2a_watch_publications WHERE published_at IS NULL")]


def _owner(task_id: str) -> str:
    return _store().owner(task_id)


def test_list_filters_by_owner_unowned_and_task_id():
    _task("t1", session=SESSION_A)
    _task("t2", session=SESSION_B)
    _task("t3")

    by_owner = task_admin.list_tasks(session=SESSION_A)
    assert by_owner.exit_code == 0
    assert [row["task_id"] for row in by_owner.rows] == ["t1"]

    unowned = task_admin.list_tasks(unowned=True)
    assert [row["task_id"] for row in unowned.rows] == ["t3"]

    exact = task_admin.list_tasks(task_ids=["t2"])
    assert [row["task_id"] for row in exact.rows] == ["t2"]
    assert "sess-B" in exact.text


def test_list_without_a_store_does_not_create_one(tmp_path, monkeypatch):
    missing = tmp_path / "missing.db"
    monkeypatch.setenv("A2A_TASKS_DB", str(missing))
    result = task_admin.list_tasks()
    assert result.exit_code == task_admin.EXIT_NOTHING
    assert not missing.exists(), "listing must not migrate or create the profile store"


def test_release_all_clears_ownership_and_keeps_the_backlog():
    _task("t1", session=SESSION_A)
    _task("t2", session=SESSION_A)
    _store().add_message("t1", "m1", "result", "queued result")
    _watch(SESSION_A, [("peer", "t1"), ("peer", "t2")])
    _pending("w1", "t1")

    result = task_admin.release_tasks(session=SESSION_A, all_tasks=True)

    assert result.exit_code == 0, result.text
    assert _owner("t1") == "" and _owner("t2") == ""
    with sqlite3.connect(_store().db_path) as con:
        rows = con.execute(
            "SELECT released_by_session_id, released_at, unowned_since FROM a2a_tasks").fetchall()
    assert all(row[0] == SESSION_A and row[1] > 0 and row[2] > 0 for row in rows)
    assert [m["text"] for m in _store().queued("t1")] == ["queued result"], (
        "a queued peer message must stay for the next owner")
    assert _pending_ids() == [], "the previous owner's pending delivery must be dropped"
    watch = watches.WatchStore().get("w1")
    assert watch["state"] == watches.WATCH_COMPLETE, (
        "releasing everything settles the session's watch without touching other tasks")


def test_release_is_atomic_when_one_task_is_not_owned():
    _task("t1", session=SESSION_A)
    _task("t2", session=SESSION_B)

    result = task_admin.release_tasks(session=SESSION_A, task_ids=["t1", "t2"])

    assert result.exit_code == task_admin.EXIT_CONFLICT
    assert "Nothing released" in result.text
    assert _owner("t1") == SESSION_A, "no partial release"
    assert _owner("t2") == SESSION_B


def test_transfer_moves_ownership_and_drops_only_the_old_owners_delivery():
    _task("t1", session=SESSION_A)
    _store().add_message("t1", "m1", "result", "queued result")
    _watch(SESSION_A, [("peer", "t1")])
    _pending("w1", "t1")

    result = task_admin.transfer_tasks(from_session=SESSION_A, to_session=SESSION_B)

    assert result.exit_code == 0, result.text
    assert _owner("t1") == SESSION_B
    with sqlite3.connect(_store().db_path) as con:
        row = con.execute(
            "SELECT released_by_session_id, released_at, unowned_since FROM a2a_tasks"
            " WHERE task_id='t1'").fetchone()
    assert row == ("", 0.0, 0.0), "transfer is not a release"
    assert [m["text"] for m in _store().queued("t1")] == ["queued result"]
    assert _pending_ids() == []
    assert watches.WatchStore().get("w1")["items"][0]["state"] == watches.ITEM_UNWATCHED


def test_transfer_refuses_when_ownership_moved_under_it():
    _task("t1", session=SESSION_A)
    assert _store().release("t1", SESSION_A)
    assert _store().acquire("t1", SESSION_B)

    result = task_admin.transfer_tasks(from_session=SESSION_A,
                                       to_session=SESSION_A + "-new",
                                       task_ids=["t1"])

    assert result.exit_code == task_admin.EXIT_CONFLICT
    assert "Nothing transferred" in result.text
    assert _owner("t1") == SESSION_B


def test_slash_release_defaults_to_the_bound_session_and_requires_yes():
    _task("t1", session=SESSION_A)

    refused = task_admin.slash_command("tasks release --all", session_id=SESSION_A)
    assert "Refusing" in refused
    assert _owner("t1") == SESSION_A

    released = task_admin.slash_command("tasks release --all --yes", session_id=SESSION_A)
    assert "Released 1 task(s)" in released
    assert _owner("t1") == ""


def test_slash_lists_only_the_bound_session_by_default():
    _task("t1", session=SESSION_A)
    _task("t2", session=SESSION_B)

    text = task_admin.slash_command("tasks list", session_id=SESSION_A)

    assert "t1" in text and "t2" not in text


def test_slash_list_unowned_does_not_default_to_the_bound_session():
    _task("t1", session=SESSION_A)
    _task("t2")

    text = task_admin.slash_command("tasks list --unowned", session_id=SESSION_A)

    assert "t2" in text and "t1" not in text


def test_slash_rejects_an_unknown_subcommand_without_changing_anything():
    _task("t1", session=SESSION_A)

    text = task_admin.slash_command("tasks frobnicate", session_id=SESSION_A)

    assert "usage:" in text
    assert _owner("t1") == SESSION_A


def test_cli_requires_yes_without_a_tty(capsys):
    _task("t1", session=SESSION_A)
    assert not sys.stdin.isatty()

    code = task_admin.dispatch_cli(["release", "--session", SESSION_A, "--all"])

    assert code == task_admin.EXIT_USAGE
    assert "refusing without --yes" in capsys.readouterr().err
    assert _owner("t1") == SESSION_A


def test_cli_and_slash_list_produce_the_same_text(capsys):
    _task("t1", session=SESSION_A)

    code = task_admin.dispatch_cli(["list", "--session", SESSION_A])
    cli_text = capsys.readouterr().out
    slash_text = task_admin.slash_command("tasks list", session_id=SESSION_A)

    assert code == 0
    assert cli_text.strip() == slash_text.strip(), "the two surfaces must not drift"


def test_release_all_leaves_a_task_acquired_after_the_release_untouched(monkeypatch):
    """A11-1: cleanup is scoped to the tasks this release moved, not the whole session."""
    _task("t1", session=SESSION_A)
    _watch(SESSION_A, [("peer", "t1")])
    real_release_many = ownership.OwnershipStore.release_many

    def release_then_acquire(self, task_ids, session_id, now=None):
        out = real_release_many(self, task_ids, session_id, now)
        _task("t3", session=SESSION_A)
        _watch(SESSION_A, [("peer", "t3")], watch_id="w3")
        return out

    monkeypatch.setattr(ownership.OwnershipStore, "release_many", release_then_acquire)

    result = task_admin.release_tasks(session=SESSION_A, all_tasks=True)

    assert result.exit_code == 0, result.text
    assert _owner("t1") == ""
    assert _owner("t3") == SESSION_A, "a task acquired after the release must stay owned"
    assert watches.WatchStore().get("w3")["state"] == watches.WATCH_ACTIVE, (
        "its watch must not be cancelled by the earlier release")


def test_release_all_does_not_touch_a_same_task_reacquired_after_the_release(monkeypatch):
    """A11-1 same-task case: cleanup is inside the release transaction, before reacquisition."""
    _task("t1", session=SESSION_A)
    _watch(SESSION_A, [("peer", "t1")])
    real_release_many = ownership.OwnershipStore.release_many

    def release_then_reacquire(self, task_ids, session_id, now=None):
        out = real_release_many(self, task_ids, session_id, now)
        _store().acquire("t1", SESSION_A)
        _watch(SESSION_A, [("peer", "t1")], watch_id="w2")
        _pending("w2", "t1", "ev2")
        return out

    monkeypatch.setattr(ownership.OwnershipStore, "release_many", release_then_reacquire)

    result = task_admin.release_tasks(session=SESSION_A, all_tasks=True)

    assert result.exit_code == 0, result.text
    assert _owner("t1") == SESSION_A, "the reacquisition must survive the release"
    assert watches.WatchStore().get("w2")["state"] == watches.WATCH_ACTIVE
    assert _pending_ids() == ["ev2"], "the reacquired task's new delivery must survive"


def test_release_rolls_back_when_watch_cleanup_fails(monkeypatch):
    """A11-2: cleanup shares the ownership transaction, so a failure changes nothing."""
    _task("t1", session=SESSION_A)

    def fail(con, session_id, task_ids, now=None):
        raise RuntimeError("watch cleanup unavailable")

    monkeypatch.setattr(watches.WatchStore, "cleanup_for_released_tasks", staticmethod(fail))

    result = task_admin.release_tasks(session=SESSION_A, all_tasks=True)

    assert result.exit_code == task_admin.EXIT_ERROR
    assert "rolled back" in result.text
    assert _owner("t1") == SESSION_A, "the release must roll back with the cleanup"


def test_slash_with_unmatched_quote_returns_usage_instead_of_raising():
    text = task_admin.slash_command('tasks list "unclosed', session_id=SESSION_A)

    assert "usage:" in text
    assert "No closing quotation" in text


def test_release_writes_an_audit_log_per_task(caplog):
    _task("t1", session=SESSION_A)

    with caplog.at_level(logging.INFO, logger=task_admin.__name__):
        result = task_admin.release_tasks(session=SESSION_A, all_tasks=True)

    assert result.exit_code == 0
    messages = [record.getMessage() for record in caplog.records]
    assert any("released task t1" in message for message in messages)
    assert any("released 1 task(s)" in message for message in messages)


def test_aggregate_publications_count_per_task_and_once_in_the_batch_total():
    """A11-4: one aggregate event is a per-task participant but one dropped delivery."""
    _task("t1", session=SESSION_A)
    _task("t2", session=SESSION_A)
    _watch(SESSION_A, [("peer", "t1"), ("peer", "t2")], watch_id="wa")
    watches.WatchStore().enqueue_publication(
        watch_id="wa", event_id="agg", content="aggregate",
        metadata={"source": "a2a", "tasks": ["peer/t1", "peer/t2"]})

    listing = task_admin.list_tasks(task_ids=["t1", "t2"])
    assert [row["pending"] for row in listing.rows] == [1, 1]

    result = task_admin.release_tasks(session=SESSION_A, all_tasks=True)

    assert result.exit_code == 0, result.text
    assert "dropped 1 pending deliver" in result.text, (
        f"the aggregate event must count once in the batch total: {result.text}")
