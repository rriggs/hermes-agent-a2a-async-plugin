"""Background A2A watches: capture a delivery target, poll off-turn, publish one completion.

The watch is the plugin side of ``PluginContext.capture_completion_target`` /
``publish_completion``. Each test drives the manager deterministically (injected poll, injected
clock, no thread) against a temp ``a2a_tasks.db``; the last test runs the same flow through the
REAL Hermes publisher when the core checkout is importable.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
# Load the plugin package under a private name: other test modules register the repository-root
# ``__init__.py`` as ``a2a_async_plugin`` in ``sys.modules``, and reusing that name here would
# either shadow the real package for them or inherit their shadow.
PKG = "a2a_watch_under_test"
spec = importlib.util.spec_from_file_location(
    PKG, ROOT / "a2a_async_plugin" / "__init__.py",
    submodule_search_locations=[str(ROOT / "a2a_async_plugin")])
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
sys.modules[PKG] = module
spec.loader.exec_module(module)

protocol = importlib.import_module(f"{PKG}.protocol")  # noqa: E402
tools = importlib.import_module(f"{PKG}.tools")  # noqa: E402
watches = importlib.import_module(f"{PKG}.watches")  # noqa: E402

SESSION_ID = "sess-1"


def task_payload(task_id="t1", state=protocol.STATE_WORKING, text="", context_id="ctx-1"):
    payload = {"id": task_id, "contextId": context_id, "status": {"state": state}}
    if text:
        payload["artifacts"] = [{"parts": [{"text": text}]}]
    return payload


class ScriptedPoll:
    """Poll stub: one scripted payload (or exception) per call, repeating the last one."""

    def __init__(self, *script):
        self.script = list(script)
        self.calls: list[tuple[str, str]] = []

    def __call__(self, peer: str, task_id: str) -> dict:
        self.calls.append((peer, task_id))
        step = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(step, Exception):
            raise step
        return step


class FakeCtx:
    """Minimal PluginContext: records captures/publishes, dedups by event id like the real one."""

    def __init__(self):
        self.captures: list = []
        self.published: list[dict] = []
        self.unload_callbacks: list = []
        self.event_ids: set[str] = set()

    def capture_completion_target(self, *, session_id=None):
        self.captures.append(session_id)
        return {
            "contract_version": 1, "target_id": "pct_test", "plugin_id": "a2a-async",
            "session_key": session_id or "", "session_id": session_id or "",
            "platform": "telegram", "chat_id": "42", "chat_type": "dm", "thread_id": "",
            "scope_id": "", "user_id": "", "user_name": "", "ui_session_id": "",
            "profile": "default", "profile_home": "/tmp/hermes", "created_at": 1.0,
            "expires_at": None,
        }

    def publish_completion(self, *, target, event_id, content, metadata=None):
        duplicate = event_id in self.event_ids
        self.event_ids.add(event_id)
        self.published.append({"target": target, "event_id": event_id, "content": content,
                               "metadata": metadata or {}, "duplicate": duplicate})
        return {"status": "duplicate" if duplicate else "queued", "event_id": event_id}

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)

    def register_tool(self, **_kwargs):
        pass


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    """Temp task DB, fresh module globals, and no background threads in unit tests."""
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "a2a_tasks.db"))
    monkeypatch.setattr(tools, "_CTX", None)
    monkeypatch.setattr(tools, "_WATCH_MANAGER", None)
    started: list = []
    monkeypatch.setattr(watches.WatchManager, "start",
                        lambda self: (started.append(self), self)[1])
    monkeypatch.setattr(watches, "started_managers", started, raising=False)
    yield


def _store() -> watches.WatchStore:
    return watches.WatchStore()


def _manager(poll, *, ctx=None, interval=5.0, store=None):
    return watches.WatchManager(ctx or FakeCtx(), store=store or _store(),
                                poll=poll, interval=interval, ttl_s=3600)


def _add(manager, tasks=(("peer", "t1"),), target=None):
    return manager.add(session_id=SESSION_ID, target=target or {"target_id": "pct_test"},
                       tasks=list(tasks))


# ── a2a_await(background=True) ──────────────────────────────────────────────

def test_background_await_returns_a_watch_id_and_persists_the_watch(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})

    result = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                             session_id=SESSION_ID)

    assert result.startswith("[watch watch_"), result
    assert "background" in result
    assert ctx.captures == [SESSION_ID], "the target must be captured from the invoking session"
    watch_id = result.split()[1]
    watch = _store().get(watch_id)
    assert watch["session_id"] == SESSION_ID
    assert watch["state"] == watches.WATCH_ACTIVE
    assert [(i["peer"], i["task_id"]) for i in watch["items"]] == [("peer", "t1")]


def test_background_await_accepts_a_task_list_and_rejects_duplicates(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})

    result = tools.a2a_await({"background": True, "tasks": [
        {"agent": "peer", "task_id": "t1"},
        {"agent": "peer", "task_id": "t2"},
        {"agent": "peer", "task_id": "t1"},
    ]}, session_id=SESSION_ID)

    watch_id = result.split()[1]
    items = _store().get(watch_id)["items"]
    assert [(i["peer"], i["task_id"]) for i in items] == [("peer", "t1"), ("peer", "t2")]


def test_background_await_refuses_a_runtime_without_the_publisher(monkeypatch):
    class LegacyCtx:
        pass

    monkeypatch.setattr(tools, "_CTX", LegacyCtx())
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})

    result = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                             session_id=SESSION_ID)

    assert result.startswith("Error:")
    assert "plugin completion publisher" in result


def test_background_await_refuses_a_session_that_cannot_receive_a_later_turn(monkeypatch):
    class StatelessFailure(Exception):
        pass

    class Ctx(FakeCtx):
        def capture_completion_target(self, *, session_id=None):
            raise type("CompletionDeliveryUnsupported", (Exception,), {})("no later turn")

    monkeypatch.setattr(tools, "_CTX", Ctx())
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})

    result = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                             session_id=SESSION_ID)

    assert result.startswith("Error:")
    assert "cannot receive a later turn" in result


def test_background_await_validates_peers_and_arguments(monkeypatch):
    monkeypatch.setattr(tools, "_CTX", FakeCtx())
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: None if agent == "nope" else {"url": "http://x"})

    assert tools.a2a_await({"agent": "nope", "task_id": "t1", "background": True}).startswith("Error: unknown agent")
    assert tools.a2a_await({"background": True}).startswith("Error:")
    assert tools.a2a_await({"background": True, "tasks": [{"agent": "peer"}]}).startswith("Error:")


def test_blocking_await_is_unchanged_by_background_support(monkeypatch):
    """Default a2a_await still blocks, polls, and returns the peer's terminal reply."""
    replies = [task_payload(state=protocol.STATE_WORKING),
               task_payload(state=protocol.STATE_COMPLETED, text="blocking reply")]

    def fake_rpc(_peer, method, params, timeout=None):
        assert method == "GetTask"
        return {"result": replies.pop(0)}

    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    monkeypatch.setattr(tools, "_rpc_request", fake_rpc)
    monkeypatch.setattr(tools.time, "sleep", lambda _s: None)

    result = tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 60})

    assert "completed" in result
    assert "blocking reply" in result
    assert replies == []


# ── polling and publishing ──────────────────────────────────────────────────

def test_a_terminal_state_publishes_one_completion_and_updates_the_task_record(tmp_path):
    poll = ScriptedPoll(task_payload(state=protocol.STATE_WORKING),
                        task_payload(state=protocol.STATE_COMPLETED, text="finished the job"))
    manager = _manager(poll)
    watch = _add(manager)

    assert manager.run_once(now=100.0) == 1          # working: no notification yet
    assert manager.ctx.published == []
    assert manager.run_once(now=200.0) == 1          # completed: publish
    [published] = manager.ctx.published
    assert "finished the job" in published["content"]
    assert "completed" in published["content"]
    assert published["metadata"]["peer"] == "peer"
    assert published["metadata"]["task_id"] == "t1"
    assert published["metadata"]["watch_id"] == watch["watch_id"]
    assert published["metadata"]["state"] == "completed"

    stored = _store().get(watch["watch_id"])
    assert stored["state"] == watches.WATCH_COMPLETE
    assert stored["items"][0]["state"] == watches.ITEM_DONE

    record = protocol.TaskStore(str(tmp_path / "a2a_tasks.db")).get("t1")
    assert record["state"] == protocol.STATE_COMPLETED
    assert record["reply"] == "finished the job"

    # Nothing left to poll: a later pass must not notify again.
    assert manager.run_once(now=300.0) == 0
    assert len(manager.ctx.published) == 1


def test_one_completion_per_task_while_siblings_keep_running():
    poll = ScriptedPoll(
        task_payload(task_id="t1", state=protocol.STATE_COMPLETED, text="one done"),
        task_payload(task_id="t2", state=protocol.STATE_WORKING),
        task_payload(task_id="t1", state=protocol.STATE_COMPLETED, text="one done"),
        task_payload(task_id="t2", state=protocol.STATE_COMPLETED, text="two done"),
    )
    manager = _manager(poll)
    watch = _add(manager, tasks=(("peer", "t1"), ("peer", "t2")))

    manager.run_once(now=100.0)
    assert [p["metadata"]["task_id"] for p in manager.ctx.published] == ["t1"]
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_ACTIVE

    manager.run_once(now=200.0)
    assert [p["metadata"]["task_id"] for p in manager.ctx.published] == ["t1", "t2"]
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_COMPLETE


def test_input_required_pauses_without_terminalizing():
    poll = ScriptedPoll(task_payload(state=protocol.STATE_INPUT_REQUIRED, text="which branch?"))
    manager = _manager(poll)
    watch = _add(manager)

    manager.run_once(now=100.0)

    [published] = manager.ctx.published
    assert published["metadata"]["state"] == "input-required"
    assert "which branch?" in published["content"]
    stored = _store().get(watch["watch_id"])
    assert stored["items"][0]["state"] == watches.ITEM_PAUSED
    assert stored["state"] == watches.WATCH_PAUSED
    assert protocol.TaskStore().get("t1") is None or \
        protocol.TaskStore().get("t1")["state"] != protocol.STATE_FAILED

    # A paused task is not polled again.
    assert manager.run_once(now=200.0) == 0
    assert len(manager.ctx.published) == 1


def test_repeated_poll_errors_back_off_then_give_up_without_failing_the_task():
    poll = ScriptedPoll(watches.TransientPollError("connection refused"))
    manager = _manager(poll)
    watch = _add(manager)

    now = 100.0
    for _ in range(watches.MAX_CONSECUTIVE_ERRORS - 1):
        manager.run_once(now=now)
        now += 1000.0
        assert manager.ctx.published == [], "a transient poll error is not a remote outcome"

    assert poll.calls, "the watch must retry with backoff"
    manager.run_once(now=now)

    [published] = manager.ctx.published
    assert published["metadata"]["state"] == "watch-error"
    assert "not cancelled or marked failed" in published["content"]
    stored = _store().get(watch["watch_id"])
    assert stored["items"][0]["state"] == watches.ITEM_UNWATCHED
    assert stored["state"] == watches.WATCH_COMPLETE
    record = protocol.TaskStore().get("t1")
    assert record is None or record["state"] not in protocol.TERMINAL_STATES


def test_a_stale_duplicate_observation_reuses_the_same_event_id():
    """Crash after publish, before the item is marked: the stable event id makes the core dedup."""
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")))
    watch = _add(manager)
    item = dict(_store().get(watch["watch_id"])["items"][0])

    manager._apply_payload(_store().get(watch["watch_id"]), item, task_payload(
        state=protocol.STATE_COMPLETED, text="done"), now=100.0)
    # The same pre-update item snapshot, as a second process holding a stale row would see it.
    manager._apply_payload(_store().get(watch["watch_id"]), item, task_payload(
        state=protocol.STATE_COMPLETED, text="done"), now=101.0)

    assert len(manager.ctx.published) == 2
    assert manager.ctx.published[0]["event_id"] == manager.ctx.published[1]["event_id"]
    assert manager.ctx.published[1]["duplicate"] is True, "the publisher must reject the repeat"


def test_an_expired_watch_reports_the_watcher_not_a_remote_failure():
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    watch = _add(manager)
    # Move the watch's deadline into the past without polling it.
    store = _store()
    with store._session() as con:
        con.execute("UPDATE a2a_watches SET expires_at=1 WHERE watch_id=?", (watch["watch_id"],))

    assert manager.run_once(now=10_000_000.0) == 0

    [published] = manager.ctx.published
    assert published["metadata"]["state"] == "watch-expired"
    assert "remote task was not cancelled" in published["content"].lower() or \
        "not cancelled" in published["content"]
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_EXPIRED
    record = protocol.TaskStore().get("t1")
    assert record is None or record["state"] not in protocol.TERMINAL_STATES


def test_a_publish_failure_is_not_recorded_as_published():
    class FailingCtx(FakeCtx):
        def publish_completion(self, **_kwargs):
            raise RuntimeError("publisher unavailable")

    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=FailingCtx())
    watch = _add(manager)

    manager.run_once(now=100.0)

    item = _store().get(watch["watch_id"])["items"][0]
    assert item["state"] == watches.ITEM_DONE
    assert item["published_state"] == "", "a refused publish must stay retryable, not look delivered"


# ── cancellation ────────────────────────────────────────────────────────────

def test_watch_cancel_stops_polling_and_leaves_the_remote_task_alone(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    result = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                             session_id=SESSION_ID)
    watch_id = result.split()[1]

    out = tools.a2a_watch_cancel({"watch_id": watch_id}, session_id=SESSION_ID)

    assert "canceled" in out
    assert "keep running" in out
    assert _store().get(watch_id)["state"] == watches.WATCH_CANCELED
    manager = tools._WATCH_MANAGER
    manager.poll = ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED))
    assert manager.run_once(now=100.0) == 0
    assert manager.poll.calls == []
    assert ctx.published == []


def test_watch_cancel_is_idempotent_and_refuses_an_unknown_id(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    watch_id = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                               session_id=SESSION_ID).split()[1]

    assert "canceled" in tools.a2a_watch_cancel({"watch_id": watch_id}, session_id=SESSION_ID)
    again = tools.a2a_watch_cancel({"watch_id": watch_id}, session_id=SESSION_ID)
    assert "already canceled" in again
    assert "unknown watch" in tools.a2a_watch_cancel({"watch_id": "watch_missing"})
    assert "required" in tools.a2a_watch_cancel({})


def test_a_watch_cannot_be_cancelled_from_another_session(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    watch_id = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                               session_id=SESSION_ID).split()[1]

    out = tools.a2a_watch_cancel({"watch_id": watch_id}, session_id="other-session")

    assert out.startswith("Error:")
    assert _store().get(watch_id)["state"] == watches.WATCH_ACTIVE


# ── restart / reload ────────────────────────────────────────────────────────

def test_rehydration_resumes_a_persisted_watch(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    watch_id = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                               session_id=SESSION_ID).split()[1]
    tools._WATCH_MANAGER = None  # a new process starts with no manager

    resumed = tools._watch_manager(ctx)
    resumed.poll = ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="after restart"))
    assert resumed.rehydrate() == 1
    resumed.run_once(now=100.0)

    [published] = ctx.published
    assert published["event_id"].startswith(f"a2a:{watch_id}:peer:t1:")
    assert "after restart" in published["content"]


def test_the_watch_store_follows_the_plugins_profile_home(tmp_path, monkeypatch):
    """A secondary profile's watcher must not write into the launch profile's database."""
    from types import SimpleNamespace

    profile_home = tmp_path / "profiles" / "secondary"
    profile_home.mkdir(parents=True)

    class ProfileCtx(FakeCtx):
        _manager = SimpleNamespace(home_path=profile_home)

    monkeypatch.setattr(tools, "_CTX", ProfileCtx())
    manager = tools._watch_manager()
    assert manager.store.db_path == str(profile_home / "a2a_tasks.db")


def test_register_tools_resumes_only_when_a_watch_database_exists(tmp_path):
    ctx = FakeCtx()
    # No database yet: discovery must not create one or start a poller.
    tools._WATCH_MANAGER = None
    tools._resume_watches(ctx)
    assert tools._WATCH_MANAGER.store.db_path.endswith("a2a_tasks.db")
    assert not (tmp_path / "a2a_tasks.db").exists()
    assert ctx.unload_callbacks, "the manager must stop on plugin unload"

    # A persisted watch resumes on the next load.
    _store().create(watch_id="watch_x", session_id=SESSION_ID, target={"target_id": "pct"},
                    tasks=[("peer", "t1")])
    tools._WATCH_MANAGER = None
    tools._resume_watches(ctx)
    assert tools._WATCH_MANAGER._thread is None  # start() is patched out in this suite
    assert watches.started_managers, "an active watch must start the poller"

def test_a_plugin_load_never_creates_or_migrates_the_task_database(tmp_path, monkeypatch):
    """Registration runs in every process; it must not write the profile's task database."""
    legacy = tmp_path / "legacy.db"
    con = sqlite3.connect(legacy)
    con.execute("CREATE TABLE a2a_tasks (task_id TEXT PRIMARY KEY)")
    con.execute("PRAGMA user_version=3")
    con.commit()
    con.close()

    monkeypatch.setenv("A2A_TASKS_DB", str(legacy))
    monkeypatch.setattr(tools, "_WATCH_MANAGER", None)
    tools._resume_watches(FakeCtx())

    con = sqlite3.connect(legacy)
    try:
        assert con.execute("PRAGMA user_version").fetchone()[0] == 3, "a plugin load migrated a v3 database"
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        con.close()
    assert "a2a_watches" not in tables

    missing = tmp_path / "missing.db"
    monkeypatch.setenv("A2A_TASKS_DB", str(missing))
    monkeypatch.setattr(tools, "_WATCH_MANAGER", None)
    tools._resume_watches(FakeCtx())
    assert not missing.exists(), "a plugin load created a task database with nothing to resume"


# ── the real core publisher ─────────────────────────────────────────────────

def test_watch_publishes_through_the_real_hermes_publisher(tmp_path, monkeypatch):
    """Cross-repo integration: the same flow through ``agent.plugin_completions``.

    Needs the Hermes checkout importable (``PYTHONPATH=/path/to/hermes-agent``); skipped
    otherwise, so the plugin's standalone suite stays standalone.
    """
    pc = pytest.importorskip("agent.plugin_completions")
    from gateway.session_context import clear_session_vars, set_session_vars

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    tokens = set_session_vars(platform="telegram", source="telegram", chat_id="42",
                              chat_type="dm", session_key="agent:main:telegram:dm:42",
                              session_id=SESSION_ID)
    try:
        class RealCtx:
            def capture_completion_target(self, *, session_id=None):
                return pc.capture_completion_target("a2a-async", session_id=session_id).to_dict()

            def publish_completion(self, *, target, event_id, content, metadata=None):
                return pc.publish_completion("a2a-async", target=target, event_id=event_id,
                                             content=content, metadata=metadata)

        manager = watches.WatchManager(
            RealCtx(), store=_store(),
            poll=ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="real publisher")),
            interval=5.0, ttl_s=3600)
        target = manager.ctx.capture_completion_target(session_id=SESSION_ID)
        watch = manager.add(session_id=SESSION_ID, target=target, tasks=[("peer", "t1")])

        manager.run_once(now=100.0)

        receipt_row = pc._existing_completion("a2a-async", "agent:main:telegram:dm:42",
                                              f"a2a:{watch['watch_id']}:peer:t1:completed")
        assert receipt_row is not None
        assert receipt_row["delivery_state"] == "pending", "queued for the surface, not yet delivered"
        # A restarted watcher re-observing the same terminal state gets a duplicate receipt.
        again = manager.ctx.publish_completion(
            target=target, event_id=f"a2a:{watch['watch_id']}:peer:t1:completed", content="again")
        assert again["status"] == "duplicate"
    finally:
        clear_session_vars(tokens)