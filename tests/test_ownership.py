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

        def register_hook(self, event, callback):
            self.hooks.setdefault(event, []).append(callback)

        def queue_context_note(self, text, key=""):
            self.notes.append(text)
            return True

        def on_unload(self, callback):
            pass

    ctx = Ctx()
    tools._install_session_hooks(ctx)
    assert {"on_session_start", "on_session_reset", "on_session_finalize"} <= set(ctx.hooks)

    for handler in ctx.hooks["on_session_reset"]:
        handler(session_id=SESSION_A)
    assert store.owner("t1") == "", "session end releases ownership"

    for handler in ctx.hooks["on_session_start"]:
        handler(session_id=SESSION_A)

    assert store.owner("t1") == SESSION_A, "resume reclaims what it released"
    [note] = ctx.notes
    assert "reclaimed" in note and "t1" in note


def test_new_releases_the_outgoing_session():
    """The host names the outgoing session old_session_id on /new and /reset."""
    protocol.TaskStore().create("t1", "ctx", "peer", direction="outbound")
    store = _store()
    store.acquire("t1", "sess-old")

    class Ctx:
        def __init__(self):
            self.hooks: dict = {}

        def register_hook(self, event, callback):
            self.hooks.setdefault(event, []).append(callback)

        def queue_context_note(self, text, key=""):
            return True

    ctx = Ctx()
    tools._install_session_hooks(ctx)
    for handler in ctx.hooks["on_session_reset"]:
        handler(session_id="sess-new", old_session_id="sess-old")

    assert store.owner("t1") == "", "the OLD session's ownership must be released"
    assert store.owner("t1") != "sess-new"


def test_a_client_detach_does_not_release_a_local_sessions_tasks():
    """A gateway-hosted local chat finalizes when its client detaches after a turn."""
    _task()
    store = _store()
    store.acquire("t1", "local-abc")

    class Ctx:
        def __init__(self):
            self.hooks: dict = {}

        def register_hook(self, event, callback):
            self.hooks.setdefault(event, []).append(callback)

        def queue_context_note(self, text, key=""):
            return True

    ctx = Ctx()
    tools._install_session_hooks(ctx)
    for handler in ctx.hooks["on_session_finalize"]:
        handler(session_id="local-abc", platform="local")
    assert store.owner("t1") == "local-abc", "a detached client does not end the session"

    for handler in ctx.hooks["on_session_finalize"]:
        handler(session_id="local-abc", platform="cli")
    assert store.owner("t1") == "", "the interactive CLI's finalize is a real session end"


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

        def register_hook(self, event, callback):
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


def test_a_refused_backlog_message_stays_queued():
    _task()
    store = _store()
    store.add_message("t1", "m1", "progress", "one")
    store.add_message("t1", "m2", "result", "two")

    class Ctx:
        def __init__(self):
            self.seen = []

        def publish_completion(self, *, target, event_id, content, metadata=None):
            if metadata["message_id"] == "m2":
                raise RuntimeError("ledger unavailable")
            self.seen.append(metadata["message_id"])
            return {"status": "queued"}

    ctx = Ctx()
    queued = store.queued("t1")
    accepted = tools._publish_queued(ctx, "t1", "peer", SESSION_A, {"target_id": "x"}, queued)
    tools._mark_backlog_delivered(
        "t1", [m for m in queued if m["message_id"] in accepted], SESSION_A)

    assert accepted == ["m1"]
    assert [m["message_id"] for m in store.queued("t1")] == ["m2"], (
        "a refused message must stay queued for the next await")


def test_blocking_await_returns_progress_it_consumed(monkeypatch):
    _task()
    monkeypatch.setattr(tools, "_resolve_peer", lambda name: {"url": "http://peer.invalid"})
    responses = iter([
        {"result": {"id": "t1", "contextId": "ctx-1",
                    "status": {"state": "TASK_STATE_WORKING",
                               "message": {"messageId": "m1",
                                           "parts": [{"text": "halfway there"}]}}}},
        {"result": {"id": "t1", "contextId": "ctx-1",
                    "status": {"state": "TASK_STATE_COMPLETED"},
                    "artifacts": [{"parts": [{"text": "done"}]}]}},
    ])
    monkeypatch.setattr(tools, "_rpc_request",
                        lambda *a, **k: next(responses, {"result": {}}))

    out = tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 10},
                          session_id=SESSION_A)

    assert "halfway there" in out, "progress observed during the wait must not be swallowed"
    assert "done" in out


def test_timer_actions_do_not_touch_a_task_adopted_after_selection():
    protocol.TaskStore().create("t1", "ctx", "peer", direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    store = _store()
    store.acquire("t1", SESSION_A)
    store.add_message("t1", "m1", "result", "the answer")
    store.release_all(SESSION_A)

    import time as _time
    now = _time.time() + 86_402.0
    cancel, abandon = store.unowned_due(now, cancel_after_s=86_400.0, ttl_s=604_800.0)
    assert [r["task_id"] for r in cancel] == ["t1"]

    store.acquire("t1", SESSION_B)  # adopted between selection and action

    assert store.mark_cancel_requested("t1") is False, "no CancelTask for an owned task"
    assert store.abandon("t1") is False, "no abandonment for an owned task"
    assert store.owner("t1") == SESSION_B
    assert [m["message_id"] for m in store.queued("t1")] == ["m1"], "the adopted backlog survives"


def test_a_background_await_of_another_sessions_task_is_refused(monkeypatch):
    """A watch for a task this session cannot deliver must not exist, and its backlog must survive."""
    _task("t1")
    _store().acquire("t1", SESSION_A)
    _store().add_message("t1", "m1", "result", "A's result")

    class Ctx:
        def capture_completion_target(self, *, session_id=None):
            return {"target_id": "pct_test"}

        def publish_completion(self, **kwargs):
            raise AssertionError("another session's backlog must never be published")

    class Manager:
        def start(self):
            return self

        def cancel_session(self, session_id, task_ids=None):
            return 0

        def add(self, *, session_id, target, tasks):
            from a2a_async_plugin.ownership import OwnershipConflict
            raise OwnershipConflict("t1", SESSION_A)

    monkeypatch.setattr(tools, "_CTX", Ctx())
    monkeypatch.setattr(tools, "_completion_capable", lambda ctx: True)
    monkeypatch.setattr(tools, "_watch_manager", lambda ctx=None: Manager())
    monkeypatch.setattr(tools, "_resolve_peer", lambda name: {"url": "http://peer.invalid"})

    out = tools._start_background_watch({"agent": "peer", "task_id": "t1"}, SESSION_B)

    assert "owned by another session" in out
    assert _store().owner("t1") == SESSION_A
    assert [m["message_id"] for m in _store().queued("t1")] == ["m1"]


def test_a_refused_background_await_claims_no_tasks(monkeypatch):
    _task("t1")

    class LegacyCtx:
        pass  # no capture/publish: the old-runtime refusal path

    class Manager:
        def start(self):
            return self

        def cancel_session(self, session_id, task_ids=None):
            return 0

    monkeypatch.setattr(tools, "_CTX", LegacyCtx())
    monkeypatch.setattr(tools, "_watch_manager", lambda ctx=None: Manager())
    monkeypatch.setattr(tools, "_resolve_peer", lambda name: {"url": "http://peer.invalid"})

    out = tools._start_background_watch({"agent": "peer", "task_id": "t1"}, SESSION_A)

    assert "needs a Hermes runtime" in out
    assert _store().owner("t1") == "", "a refused await must not claim the task"


def test_a_failed_target_capture_claims_no_tasks(monkeypatch):
    _task("t1")

    class NoDeliveryCtx:
        def capture_completion_target(self, *, session_id=None):
            raise type("CompletionDeliveryUnsupported", (RuntimeError,), {})("no later turn")

        def publish_completion(self, **kwargs):
            return {}

    class Manager:
        def start(self):
            return self

        def cancel_session(self, session_id, task_ids=None):
            return 0

    monkeypatch.setattr(tools, "_CTX", NoDeliveryCtx())
    monkeypatch.setattr(tools, "_completion_capable", lambda ctx: True)
    monkeypatch.setattr(tools, "_watch_manager", lambda ctx=None: Manager())
    monkeypatch.setattr(tools, "_resolve_peer", lambda name: {"url": "http://peer.invalid"})

    out = tools._start_background_watch({"agent": "peer", "task_id": "t1"}, SESSION_A)

    assert "cannot receive a later turn" in out
    assert _store().owner("t1") == ""


def test_a_refused_multi_task_await_rolls_back_earlier_claims(monkeypatch):
    _task("t1")
    _task("t2")
    _store().acquire("t2", SESSION_B)

    class Ctx:
        def capture_completion_target(self, *, session_id=None):
            return {"target_id": "pct_test"}

        def publish_completion(self, **kwargs):
            return {"status": "queued"}

    class Manager:
        def start(self):
            return self

        def cancel_session(self, session_id, task_ids=None):
            return 0

        def add(self, *, session_id, target, tasks):
            for _agent, task_id in tasks:
                other = _store().owner(task_id)
                if other and other != session_id:
                    raise RuntimeError(f"task {task_id} is owned by another session")
            return {"watch_id": "watch_test"}

        ttl_s = 3600.0
        interval = 30.0

    monkeypatch.setattr(tools, "_CTX", Ctx())
    monkeypatch.setattr(tools, "_completion_capable", lambda ctx: True)
    monkeypatch.setattr(tools, "_watch_manager", lambda ctx=None: Manager())
    monkeypatch.setattr(tools, "_resolve_peer", lambda name: {"url": "http://peer.invalid"})

    out = tools._start_background_watch(
        {"tasks": [{"agent": "peer", "task_id": "t1"},
                   {"agent": "peer", "task_id": "t2"}]}, SESSION_A)

    assert "owned by another session" in out
    assert _store().owner("t1") == "", "a refused batch must not leave earlier tasks claimed"
    assert _store().owner("t2") == SESSION_B


def test_a_list_shows_ownership_per_task(monkeypatch):
    _task("t1")
    _task("t2")
    store = _store()
    store.acquire("t1", SESSION_B)

    out = tools.a2a_list({"kind": "tasks"}, session_id=SESSION_A)

    assert "t1" in out and "owned-by-another-session" in out
    assert "t2" in out and "unowned" in out

def test_blocking_await_returns_every_artifact(monkeypatch):
    _task()
    monkeypatch.setattr(tools, "_resolve_peer", lambda name: {"url": "http://peer.invalid"})
    monkeypatch.setattr(tools, "_rpc_request", lambda *a, **k: {"result": {
        "id": "t1", "contextId": "ctx-1", "status": {"state": protocol.STATE_COMPLETED},
        "artifacts": [{"parts": [{"text": "first artifact"}]},
                      {"parts": [{"text": "second artifact"}]}]}})

    out = tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 10},
                          session_id=SESSION_A)

    assert "first artifact" in out and "second artifact" in out, out


def test_releasing_starts_the_maintenance_poller(monkeypatch):
    """Auto-cancel and the TTL only run in the poller; a release must start it."""
    started: list = []

    class Manager:
        def start(self):
            started.append(True)
            return self

        def cancel_session(self, session_id, task_ids=None):
            return 0

    _task()
    _store().acquire("t1", SESSION_A)
    monkeypatch.setattr(tools, "_watch_manager", lambda ctx=None: Manager())

    tools.a2a_release_task_ownership({"all": True}, session_id=SESSION_A)

    assert started, "the unowned-task timers would never run"


def test_released_tasks_count_as_work_to_resume():
    import importlib
    watches = importlib.import_module(f"{PKG}.watches")
    protocol.TaskStore().create("t1", "ctx", "peer", direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    store = _store()
    store.acquire("t1", SESSION_A)
    store.release_all(SESSION_A)

    manager = watches.WatchManager(object(), store=watches.WatchStore(),
                                   poll=lambda p, t: {}, interval=5.0, ttl_s=3600)
    assert manager.store.resumable_count_readonly() == 0
    assert store.unowned_count_readonly() == 1
