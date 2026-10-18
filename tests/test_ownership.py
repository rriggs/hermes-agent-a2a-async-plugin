"""A2A task ownership: one owner at a time, queued results for whoever adopts, release on end.

The model (user requirement): a task is unowned or owned by exactly one session. Await acquires an
unowned task; session end releases everything it owns; resume reclaims what it can and is told what
it lost; adoption is final; only unowned tasks can be adopted; no cross-profile adoption.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PKG = "a2a_ownership_under_test"
spec = importlib.util.spec_from_file_location(
    PKG, ROOT / "a2a_async_plugin" / "__init__.py",
    submodule_search_locations=[str(ROOT / "a2a_async_plugin")])
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
sys.modules[PKG] = module
spec.loader.exec_module(module)

protocol = importlib.import_module(f"{PKG}.protocol")
tools = importlib.import_module(f"{PKG}.tools")
ownership = importlib.import_module(f"{PKG}.ownership")

SESSION_A = "sess-A"
SESSION_B = "sess-B"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "a2a_tasks.db"))
    monkeypatch.setattr(tools, "_CTX", None)
    monkeypatch.setattr(tools, "_WATCH_MANAGER", None)
    yield


def _store() -> ownership.OwnershipStore:
    return ownership.OwnershipStore()


def _task(task_id="t1", peer="peer") -> dict:
    return protocol.TaskStore().create(task_id, "ctx-1", peer, direction="outbound",
                                       remote_state=protocol.STATE_WORKING)


def test_one_owner_at_a_time_and_release_makes_it_adoptable():
    _task()
    store = _store()
    assert store.acquire("t1", SESSION_A) is True
    assert store.owner("t1") == SESSION_A

    assert store.acquire("t1", SESSION_B) is False, "a second session must not take an owned task"
    assert store.owner("t1") == SESSION_A

    assert store.release("t1", SESSION_B) is False, "only the owner can release"
    assert store.release("t1", SESSION_A) is True
    assert store.owner("t1") == ""

    assert store.acquire("t1", SESSION_B) is True
    assert store.acquire("t1", SESSION_A) is False
    assert store.owner("t1") == SESSION_B, "adoption is final"


def test_reclaim_returns_what_is_still_unowned_and_reports_what_was_adopted():
    for task_id in ("t1", "t2"):
        protocol.TaskStore().create(task_id, "ctx", "peer", direction="outbound")
    store = _store()
    store.acquire("t1", SESSION_A)
    store.acquire("t2", SESSION_A)
    assert store.release_all(SESSION_A) == 2
    store.acquire("t2", SESSION_B)  # adopted elsewhere while A was away

    reclaimed, lost = store.reclaim(SESSION_A)

    assert reclaimed == ["t1"]
    assert lost == ["t2"]
    assert store.owner("t1") == SESSION_A
    assert store.owner("t2") == SESSION_B
    # A second resume reports nothing: the release records were consumed.
    assert store.reclaim(SESSION_A) == ([], [])


def test_messages_queue_while_unowned_and_are_delivered_once_on_adoption():
    _task()
    store = _store()
    assert store.add_message("t1", "m1", "progress", "halfway there") is True
    assert store.add_message("t1", "m1", "progress", "halfway there") is False, "message ids dedupe"
    store.add_message("t1", "m2", "result", "done")

    queued = store.queued("t1")
    assert [m["message_id"] for m in queued] == ["m1", "m2"], "oldest first"

    store.acquire("t1", SESSION_B)
    # Delivery is a publish per message; the marker is what makes it once-only.
    assert store.mark_delivered("t1", ["m1", "m2"], SESSION_B) == 2
    assert store.queued("t1") == []
    assert store.mark_delivered("t1", ["m1"], SESSION_B) == 0


def test_unowned_tasks_are_cancelled_after_a_day_and_abandoned_after_the_ttl():
    protocol.TaskStore().create("t1", "ctx", "peer", direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    store = _store()
    store.acquire("t1", SESSION_A, now=0.0)
    store.release("t1", SESSION_A, now=0.0)

    cancel, abandon = store.unowned_due(3600.0, cancel_after_s=86400.0, ttl_s=604800.0)
    assert (cancel, abandon) == ([], []), "owned too recently to touch"

    cancel, abandon = store.unowned_due(86401.0, cancel_after_s=86400.0, ttl_s=604800.0)
    assert [r["task_id"] for r in cancel] == ["t1"]
    assert abandon == []

    store.mark_cancel_requested("t1", now=86401.0)
    cancel, _ = store.unowned_due(90000.0, cancel_after_s=86400.0, ttl_s=604800.0)
    assert cancel == [], "CancelTask is requested once"

    _, abandon = store.unowned_due(604801.0, cancel_after_s=86400.0, ttl_s=604800.0)
    assert [r["task_id"] for r in abandon] == ["t1"]


def test_session_hooks_release_on_end_and_reclaim_with_a_note_on_start():
    _task()
    store = _store()
    store.acquire("t1", SESSION_A)

    class Ctx:
        def __init__(self):
            self.hooks: dict = {}
            self.notes: list[str] = []

        def subscribe(self, event, callback):
            self.hooks.setdefault(event, []).append(callback)

        def queue_context_note(self, text, key=""):
            self.notes.append(text)
            return True

        def on_unload(self, callback):
            pass

    ctx = Ctx()
    tools._install_session_hooks(ctx)
    assert {"on_session_start", "on_session_end", "on_session_reset",
            "on_session_finalize"} <= set(ctx.hooks)

    for handler in ctx.hooks["on_session_end"]:
        handler(session_id=SESSION_A)
    assert store.owner("t1") == "", "session end releases ownership"

    for handler in ctx.hooks["on_session_start"]:
        handler(session_id=SESSION_A)

    assert store.owner("t1") == SESSION_A, "resume reclaims what it released"
    [note] = ctx.notes
    assert "reclaimed" in note and "t1" in note


def test_resume_notes_a_lost_task_when_another_session_adopted_it():
    _task()
    store = _store()
    store.acquire("t1", SESSION_A)
    store.release_all(SESSION_A)
    store.acquire("t1", SESSION_B)

    class Ctx:
        def __init__(self):
            self.hooks: dict = {}
            self.notes: list[str] = []

        def subscribe(self, event, callback):
            self.hooks.setdefault(event, []).append(callback)

        def queue_context_note(self, text, key=""):
            self.notes.append(text)
            return True

        def on_unload(self, callback):
            pass

    ctx = Ctx()
    tools._install_session_hooks(ctx)
    for handler in ctx.hooks["on_session_start"]:
        handler(session_id=SESSION_A)

    assert store.owner("t1") == SESSION_B
    [note] = ctx.notes
    assert "lost" in note and "t1" in note


def test_await_refuses_a_task_owned_by_another_session():
    _task()
    store = _store()
    store.acquire("t1", SESSION_B)

    ok, refusal = tools._acquire_task("t1", SESSION_A, "peer")

    assert ok is False
    assert "owned by another session" in refusal
    assert store.owner("t1") == SESSION_B


def test_a_list_shows_ownership_per_task(monkeypatch):
    _task("t1")
    _task("t2")
    store = _store()
    store.acquire("t1", SESSION_B)

    out = tools.a2a_list({"kind": "tasks"}, session_id=SESSION_A)

    assert "t1" in out and "owned-by-another-session" in out
    assert "t2" in out and "unowned" in out
