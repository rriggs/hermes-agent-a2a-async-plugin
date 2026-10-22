"""Operator commands for task ownership: list, release, transfer (issue #10).

Both adapters (``hermes a2a-tasks`` and ``/a2a tasks``) call the same functions in
``task_admin``, so these tests assert the durable effects once and the surface behavior twice.
"""

from __future__ import annotations

import importlib.util
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
    from gateway.session_context import set_session_vars
    tokens = set_session_vars(platform="telegram", source="telegram", chat_id="42",
                              chat_type="dm", session_key="agent:main:telegram:dm:42",
                              session_id=SESSION_A)
    try:
        yield
    finally:
        from gateway.session_context import clear_session_vars
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
    assert watch["state"] == watches.WATCH_CANCELED, (
        "releasing everything stops the session's watch outright")


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
