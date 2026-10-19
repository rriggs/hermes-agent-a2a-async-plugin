"""Background A2A watches: capture a delivery target, poll off-turn, inject into the host.

The watch is the plugin side of ``PluginContext.inject_message``: the host routes the captured
content into the live CLI, the owning TUI/desktop session, or the messaging chat. Each test drives
the manager deterministically (injected poll, injected clock, no thread) against a temp
``a2a_tasks.db``; the ``FakeCtx`` records every accepted ``inject_message`` call so the test can
assert both the injected content and the durable outbox row.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
import threading
import time
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

# The autouse fixture patches WatchManager.start out of most tests; the lifecycle regression
# needs the real implementation.
_REAL_START = watches.WatchManager.start

SESSION_ID = "sess-1"
TARGET_SESSION_KEY = "agent:main:telegram:dm:42"


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
    """Minimal PluginContext: records every ``inject_message`` call and honours an ``accept`` flag.

    The host contract: ``inject_message(content, role="user", *, session_key=None, origin=None)``
    returns ``True`` only when a live host accepted the message. ``session_surface()`` returns a
    short string for diagnostics; the outbox row already carries the peer / task_id / state, so the
    tests no longer need the metadata dict that ``publish_completion`` used to receive.
    """

    def __init__(self):
        self.injected: list[dict] = []
        self.unload_callbacks: list = []
        self.accept = True
        self.surface = "telegram"

    def inject_message(self, content, role="user", session_key=None, origin=None):
        self.injected.append({
            "content": content,
            "role": role,
            "session_key": session_key,
            "origin": origin,
        })
        return bool(self.accept)

    def session_surface(self) -> str:
        return self.surface

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)

    def register_tool(self, **_kwargs):
        pass


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    """Temp task DB, fresh module globals, and no background threads in unit tests."""
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "a2a_tasks.db"))
    # Background delivery is addressed by the session's route key; bind one like a live surface.
    from gateway.session_context import set_session_vars
    tokens = set_session_vars(platform="telegram", source="telegram", chat_id="42",
                              chat_type="dm", session_key="agent:main:telegram:dm:42",
                              session_id=SESSION_ID)
    monkeypatch.setattr(tools, "_CTX", None)
    tools._WATCH_MANAGERS.clear()
    started: list = []
    monkeypatch.setattr(watches.WatchManager, "start",
                        lambda self: (started.append(self), self)[1])
    monkeypatch.setattr(watches, "started_managers", started, raising=False)
    try:
        yield
    finally:
        from gateway.session_context import clear_session_vars
        clear_session_vars(tokens)


def _store() -> watches.WatchStore:
    return watches.WatchStore()


def _manager(poll, *, ctx=None, interval=5.0, store=None):
    return watches.WatchManager(ctx or FakeCtx(), store=store or _store(),
                                poll=poll, interval=interval, ttl_s=3600)


def _add(manager, tasks=(("peer", "t1"),), target=None):
    return manager.add(session_id=SESSION_ID,
                       target=target or {"session_key": TARGET_SESSION_KEY},
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
    assert ctx.injected == [], (
        "the watch starts by capturing the target through _capture_target; "
        "no message is injected until a poll observes a terminal state"
    )
    watch_id = result.split()[1]
    watch = _store().get(watch_id)
    assert watch["session_id"] == SESSION_ID
    assert watch["state"] == watches.WATCH_ACTIVE
    assert watch["target"]["surface"] == ctx.surface, (
        "_capture_target must record the runtime surface so diagnostics and the "
        "outbox row agree on where the message went"
    )
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


def test_background_await_refuses_a_runtime_without_inject_message(monkeypatch):
    class LegacyCtx:
        pass

    monkeypatch.setattr(tools, "_CTX", LegacyCtx())
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})

    result = tools.a2a_await({"agent": "peer", "task_id": "t1", "background": True},
                             session_id=SESSION_ID)

    assert result.startswith("Error:")
    assert "PluginContext.inject_message" in result, (
        "the error must name the missing surface so an operator knows what to upgrade"
    )


def test_background_await_refuses_a_session_that_cannot_receive_a_later_turn(monkeypatch):
    import gateway.session_context as sc

    monkeypatch.setattr(tools, "_CTX", FakeCtx())
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    monkeypatch.setattr(sc, "async_delivery_supported", lambda: False)

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

def test_a_terminal_state_injects_one_message_and_updates_the_task_record(tmp_path):
    poll = ScriptedPoll(task_payload(state=protocol.STATE_WORKING),
                        task_payload(state=protocol.STATE_COMPLETED, text="finished the job"))
    manager = _manager(poll)
    watch = _add(manager)

    assert manager.run_once(now=100.0) == 1          # working: no injection yet
    assert manager.ctx.injected == []
    assert manager.run_once(now=200.0) == 1          # completed: inject
    [injected] = manager.ctx.injected
    assert "finished the job" in injected["content"]
    assert "returned a result" in injected["content"]
    assert injected["session_key"] == TARGET_SESSION_KEY, (
        "the injection must target the captured session, not None"
    )
    [row] = _outbox(watch["watch_id"])
    assert row["published_at"] is not None, "the outbox row must be marked delivered"
    meta = _meta(row)
    assert meta["peer"] == "peer"
    assert meta["task_id"] == "t1"
    assert meta["watch_id"] == watch["watch_id"]
    assert meta["state"] == "completed"

    stored = _store().get(watch["watch_id"])
    assert stored["state"] == watches.WATCH_COMPLETE
    assert stored["items"][0]["state"] == watches.ITEM_DONE

    record = protocol.TaskStore(str(tmp_path / "a2a_tasks.db")).get("t1")
    assert record["state"] == protocol.STATE_COMPLETED
    assert record["reply"] == "finished the job"

    # Nothing left to poll: a later pass must not inject again.
    assert manager.run_once(now=300.0) == 0
    assert len(manager.ctx.injected) == 1


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
    assert [_meta(r)["task_id"] for r in _outbox(watch["watch_id"])] == ["t1"]
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_ACTIVE

    manager.run_once(now=200.0)
    assert [_meta(r)["task_id"] for r in _outbox(watch["watch_id"])] == ["t1", "t2"]
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_COMPLETE


def test_input_required_pauses_without_terminalizing():
    poll = ScriptedPoll(task_payload(state=protocol.STATE_INPUT_REQUIRED, text="which branch?"))
    manager = _manager(poll)
    watch = _add(manager)

    manager.run_once(now=100.0)

    [injected] = manager.ctx.injected
    assert "which branch?" in injected["content"]
    [row] = _outbox(watch["watch_id"])
    assert _meta(row)["state"] == "input-required"
    stored = _store().get(watch["watch_id"])
    assert stored["items"][0]["state"] == watches.ITEM_PAUSED
    assert stored["state"] == watches.WATCH_PAUSED
    assert protocol.TaskStore().get("t1") is None or \
        protocol.TaskStore().get("t1")["state"] != protocol.STATE_FAILED

    # A paused task is not polled again.
    assert manager.run_once(now=200.0) == 0
    assert len(manager.ctx.injected) == 1


def test_repeated_poll_errors_back_off_then_give_up_without_failing_the_task():
    poll = ScriptedPoll(watches.TransientPollError("connection refused"))
    manager = _manager(poll)
    watch = _add(manager)

    now = 100.0
    for _ in range(watches.MAX_CONSECUTIVE_ERRORS - 1):
        manager.run_once(now=now)
        now += 1000.0
        assert manager.ctx.injected == [], "a transient poll error is not a remote outcome"

    assert poll.calls, "the watch must retry with backoff"
    manager.run_once(now=now)

    [injected] = manager.ctx.injected
    [row] = _outbox(watch["watch_id"])
    assert _meta(row)["state"] == "watch-error"
    assert "not cancelled or marked failed" in injected["content"]
    stored = _store().get(watch["watch_id"])
    assert stored["items"][0]["state"] == watches.ITEM_UNWATCHED
    assert stored["state"] == watches.WATCH_COMPLETE
    record = protocol.TaskStore().get("t1")
    assert record is None or record["state"] not in protocol.TERMINAL_STATES


def test_a_stale_duplicate_observation_injects_once():
    """A second process holding a stale item snapshot must not enqueue a second event."""
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")))
    watch = _add(manager)
    item = dict(_store().get(watch["watch_id"])["items"][0])

    manager._apply_payload(_store().get(watch["watch_id"]), item, task_payload(
        state=protocol.STATE_COMPLETED, text="done"), now=100.0)
    manager.flush_publications(now=100.0)
    # The same pre-update item snapshot, as a second process holding a stale row would see it.
    manager._apply_payload(_store().get(watch["watch_id"]), item, task_payload(
        state=protocol.STATE_COMPLETED, text="done"), now=101.0)
    manager.flush_publications(now=101.0)

    assert len(manager.ctx.injected) == 1, "the outbox must be idempotent per event id"
    assert _outbox(watch["watch_id"], pending_only=True) == [], (
        "a delivered event must leave the pending outbox"
    )


def test_an_expired_watch_reports_the_watcher_not_a_remote_failure():
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    watch = _add(manager)
    # Move the watch's deadline into the past without polling it.
    store = _store()
    with store._session() as con:
        con.execute("UPDATE a2a_watches SET expires_at=1 WHERE watch_id=?", (watch["watch_id"],))

    assert manager.run_once(now=10_000_000.0) == 0

    [injected] = manager.ctx.injected
    [row] = _outbox(watch["watch_id"])
    assert _meta(row)["state"] == "watch-expired"
    assert "remote task was not cancelled" in injected["content"].lower() or \
        "not cancelled" in injected["content"]
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_EXPIRED
    record = protocol.TaskStore().get("t1")
    assert record is None or record["state"] not in protocol.TERMINAL_STATES


def test_an_injection_failure_is_not_recorded_as_delivered():
    """A host that raises out of ``inject_message`` must leave the outbox row retryable."""

    class FailingCtx(FakeCtx):
        def inject_message(self, content, role="user", session_key=None, origin=None):
            raise RuntimeError("host unavailable")

    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=FailingCtx())
    watch = _add(manager)

    manager.run_once(now=100.0)

    item = _store().get(watch["watch_id"])["items"][0]
    assert item["state"] == watches.ITEM_DONE
    assert item["published_state"] == "", "a refused inject must stay retryable, not look delivered"
    [pending] = _outbox(watch["watch_id"], pending_only=True)
    assert pending["published_at"] is None
    assert pending["attempts"] == 1


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
    manager = next(iter(tools._WATCH_MANAGERS.values()))
    manager.poll = ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED))
    assert manager.run_once(now=100.0) == 0
    assert manager.poll.calls == []
    assert ctx.injected == []


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
    tools._WATCH_MANAGERS.clear()  # a new process starts with no manager

    resumed = tools._watch_manager(ctx)
    resumed.poll = ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="after restart"))
    assert resumed.rehydrate() == 1
    resumed.run_once(now=100.0)

    [injected] = ctx.injected
    assert "after restart" in injected["content"]
    [row] = _outbox(watch_id)
    assert row["event_id"].startswith("a2a:t1:")
    assert row["published_at"] is not None


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
    tools._WATCH_MANAGERS.clear()
    tools._resume_watches(ctx)
    [manager] = tools._WATCH_MANAGERS.values()
    assert manager.store.db_path.endswith("a2a_tasks.db")
    assert not (tmp_path / "a2a_tasks.db").exists()
    assert ctx.unload_callbacks, "the manager must stop on plugin unload"

    # A persisted watch resumes on the next load.
    _store().create(watch_id="watch_x", session_id=SESSION_ID,
                    target={"session_key": TARGET_SESSION_KEY},
                    tasks=[("peer", "t1")])
    tools._WATCH_MANAGERS.clear()
    tools._resume_watches(ctx)
    [manager] = tools._WATCH_MANAGERS.values()
    assert manager._thread is None  # start() is patched out in this suite
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
    tools._WATCH_MANAGERS.clear()
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
    tools._WATCH_MANAGERS.clear()
    tools._resume_watches(FakeCtx())
    assert not missing.exists(), "a plugin load created a task database with nothing to resume"


def _outbox(watch_id: str, *, pending_only: bool = False) -> list[dict]:
    con = sqlite3.connect(_store().db_path)
    con.row_factory = sqlite3.Row
    try:
        sql = "SELECT * FROM a2a_watch_publications WHERE watch_id=?"
        if pending_only:
            sql += " AND published_at IS NULL"
        rows = con.execute(sql + " ORDER BY created_at", (watch_id,)).fetchall()
    finally:
        con.close()
    return [dict(r) for r in rows]


def _meta(row: dict) -> dict:
    import json as _json
    return _json.loads(row.get("metadata_json") or "{}")


class FlakyCtx(FakeCtx):
    """Host that refuses injection until told otherwise (no live session, missing consent)."""

    def __init__(self):
        super().__init__()
        self.fail = True

    def inject_message(self, content, role="user", session_key=None, origin=None):
        if self.fail:
            return False
        return super().inject_message(content, role=role, session_key=session_key, origin=origin)


def test_publish_and_backlog_mark_commit_together():
    """One transaction: an adoption cannot land between the host accepting and the mark."""
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    protocol.TaskStore(_store().db_path).create(
        "t1", "t1", "peer", direction="outbound", remote_state=protocol.STATE_WORKING)
    assert manager.ownership.acquire("t1", SESSION_ID)
    manager.ownership.add_message("t1", "m1", "result", "the result")

    assert manager.publish_if_owned(SESSION_ID, "t1", lambda: True, mark_messages=["m1"])
    assert manager.ownership.queued("t1") == [], "the accepted message is marked in the same commit"

    manager.ownership.add_message("t1", "m2", "result", "refused")
    assert manager.publish_if_owned(SESSION_ID, "t1", lambda: False, mark_messages=["m2"]) is False
    assert [m["message_id"] for m in manager.ownership.queued("t1")] == ["m2"], (
        "a refused publish must leave the message queued")


def test_a_publication_naming_an_unknown_task_is_not_delivered():
    """A row whose task record is gone must fail closed, not publish into the watch's session."""
    store = _store()
    store.create(watch_id="watch_ghost", session_id=SESSION_ID,
                 target={"session_key": "agent:main:telegram:dm:42"},
                 tasks=[("peer", "t-ghost")], ttl_s=3600)
    with store._session() as con:
        con.execute("DELETE FROM a2a_tasks WHERE task_id='t-ghost'")
        con.execute(
            "INSERT INTO a2a_watch_publications (event_id, watch_id, content, metadata_json,"
            " created_at) VALUES ('evt-ghost', 'watch_ghost', 'orphan', ?, 1.0)",
            ('{"source": "a2a", "task_id": "t-ghost", "state": "completed"}',))

    with store.publication_guard("evt-ghost") as (_con, row):
        assert row is None, "an orphan publication must not be authorized"


def test_a_refused_injection_leaves_the_outbox_row_pending():
    """A host that returns False must leave the row pending with an increased attempt count and a
    future ``next_attempt_at`` (the exponential backoff used elsewhere in the manager)."""
    ctx = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=ctx)
    watch = _add(manager)

    manager.run_once(now=1_000.0)

    assert ctx.injected == [], "a refused host must not record a delivered message"
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_COMPLETE
    [pending] = _outbox(watch["watch_id"])
    assert pending["published_at"] is None, "the row must stay pending for the next flush"
    assert pending["attempts"] == 1, "the failed attempt must increment the counter"
    assert pending["next_attempt_at"] > 1_000.0, (
        "the next attempt must be scheduled in the future, not now"
    )
    item = _store().get(watch["watch_id"])["items"][0]
    assert item["published_state"] == "", "a refused inject must not look published"


def test_a_refused_injection_retries_after_the_host_recovers_and_delivers_once():
    """Once the host accepts, exactly one message reaches it: idempotency per event id is the
    outbox's job, not the host's."""
    ctx = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=ctx)
    watch = _add(manager)
    manager.run_once(now=1_000.0)
    [pending_before] = _outbox(watch["watch_id"])
    assert pending_before["published_at"] is None

    ctx.fail = False
    # Nothing left to poll: the outbox alone must drive the retry once its backoff elapses.
    manager.run_once(now=pending_before["next_attempt_at"] + 0.001)

    [injected] = ctx.injected
    assert "done" in injected["content"]
    assert injected["session_key"] == TARGET_SESSION_KEY
    [delivered] = _outbox(watch["watch_id"])
    assert delivered["event_id"] == pending_before["event_id"]
    assert delivered["published_at"] is not None
    assert _outbox(watch["watch_id"], pending_only=True) == []
    assert _store().get(watch["watch_id"])["items"][0]["published_state"] == "completed"

    # A second sweep with the host still healthy must not inject again.
    manager.run_once(now=pending_before["next_attempt_at"] + 100.0)
    assert len(ctx.injected) == 1


def test_a_pending_publication_survives_a_restart():
    first = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=first)
    watch = _add(manager)
    manager.run_once(now=1_000.0)
    [lost] = _outbox(watch["watch_id"])
    assert lost["published_at"] is None

    # A new process loads the plugin: no live watch, but the outbox row must still be resumed.
    second = FakeCtx()
    restarted = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED)),
                         ctx=second, store=_store())
    assert restarted.rehydrate() == 1, "a pending publication must count as work to resume"
    restarted.flush_publications(now=1_000.0 + 3600.0)

    [injected] = second.injected
    assert injected["session_key"] == TARGET_SESSION_KEY


def test_an_enqueue_failure_leaves_the_item_open_for_the_next_poll():
    """Completion is recorded only after its message is durable: a failed enqueue must retry."""
    ctx = FakeCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=ctx)
    watch = _add(manager)
    store = _store()
    real_enqueue = store.enqueue_publication
    calls = {"n": 0}

    def flaky_enqueue(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("ledger locked")
        return real_enqueue(**kwargs)

    store.enqueue_publication = flaky_enqueue  # type: ignore[method-assign]
    manager.store = store

    assert manager.run_once(now=1_000.0) == 1
    item = _store().get(watch["watch_id"])["items"][0]
    assert item["completed_at"] is None, "a completion that never reached the outbox must stay open"
    assert _outbox(watch["watch_id"]) == []

    assert manager.run_once(now=1_010.0) == 1
    assert ctx.injected
    assert _store().get(watch["watch_id"])["items"][0]["completed_at"] is not None


def test_a_late_response_from_a_stale_owner_injects_nothing():
    """The poll can outlive the lease; the fold must be fenced, not just the item update."""
    clock = [1_000.0]
    store = _store()
    box: dict[str, str] = {}

    def poll(peer, task_id):
        # While this poll is in flight, another process takes the watch after the lease lapses.
        clock[0] += watches.LEASE_S + 10
        assert store.claim(box["watch_id"], "other-owner", clock[0]) is True
        return task_payload(state=protocol.STATE_INPUT_REQUIRED, text="which key?")

    manager = watches.WatchManager(FakeCtx(), store=store, poll=poll, interval=5.0,
                                   ttl_s=3600, clock=lambda: clock[0])
    watch = _add(manager)
    box["watch_id"] = watch["watch_id"]

    manager.run_once(now=clock[0])

    assert manager.ctx.injected == [], "a stale owner must not inject a superseded observation"


def test_stop_mid_batch_does_not_poll_or_inject_the_next_task():
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")))
    watch = _add(manager, tasks=(("peer", "t1"), ("peer", "t2")))
    manager._stop.set()

    assert manager.run_once(now=1_000.0) == 0
    assert manager.ctx.injected == []
    assert _store().get(watch["watch_id"])["items"][0]["completed_at"] is None


def test_release_drops_undelivered_publications_and_keeps_the_messages_queued():
    """A task that leaves a session must not be injected into it; the next owner gets the message."""
    ctx = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=ctx)
    watch = _add(manager)
    manager.run_once(now=1_000.0)
    assert _outbox(watch["watch_id"], pending_only=True)
    assert [m["message_id"] for m in manager.ownership.queued("t1")]

    manager.cancel_session("sess-1")  # release / session end

    assert _outbox(watch["watch_id"], pending_only=True) == [], (
        "an undelivered publication must not survive the session that owned the task"
    )
    assert manager.ownership.queued("t1"), "the message stays queued for the next owner"


def test_releasing_every_task_also_removes_a_multi_task_expiry_notice():
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    watch = _add(manager, tasks=(("peer", "t1"), ("peer", "t2")))
    with _store()._session() as con:
        con.execute("UPDATE a2a_watches SET expires_at=1 WHERE watch_id=?", (watch["watch_id"],))
    manager.run_once(now=10_000.0)  # aggregate watch-expired notice with tasks=[t1, t2]
    assert [_meta(r)["state"] for r in _outbox(watch["watch_id"])] == ["watch-expired"]

    store = _store()
    store.release("t1", "sess-1")
    store.release("t2", "sess-1")

    assert _outbox(watch["watch_id"], pending_only=True) == [], (
        "an aggregate notice must die with the tasks it describes"
    )
    assert manager.flush_publications(now=20_000.0) == 0


def test_a_notice_written_after_release_is_never_injected():
    """The outbox insert and the ownership check are separate: the guard re-checks at inject."""
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    watch = _add(manager, tasks=(("peer", "t1"),))
    assert manager.ownership.release("t1", SESSION_ID)

    with _store()._session() as con:
        con.execute(
            "INSERT INTO a2a_watch_publications (event_id, watch_id, content, metadata_json,"
            " created_at) VALUES ('evt-late', ?, 'result', ?, ?)",
            (watch["watch_id"], '{"source": "a2a", "task_id": "t1", "state": "completed"}', 1.0))

    assert manager.flush_publications(now=10_000.0) == 0
    assert manager.ctx.injected == []
    assert _outbox(watch["watch_id"], pending_only=True) == []


def test_the_backlog_replay_refuses_a_released_task(monkeypatch):
    """Release sets owner to "": the former owner must not inject into itself either."""
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    monkeypatch.setattr(tools, "_watch_manager", lambda ctx=None: manager)
    _add(manager, tasks=(("peer", "t1"),))
    assert manager.ownership.release("t1", SESSION_ID)

    accepted = tools._publish_queued(
        ctx, "t1", "peer", SESSION_ID, {"session_key": TARGET_SESSION_KEY},
        [{"message_id": "m1", "kind": "result", "text": "result"}])

    assert accepted == [] and ctx.injected == []


def test_the_backlog_replay_refuses_a_task_another_session_owns(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    monkeypatch.setattr(tools, "_watch_manager", lambda ctx=None: manager)
    _add(manager, tasks=(("peer", "t1"),))
    assert manager.ownership.release("t1", SESSION_ID)
    assert manager.ownership.acquire("t1", "sess-other")

    accepted = tools._publish_queued(
        ctx, "t1", "peer", SESSION_ID, {"session_key": TARGET_SESSION_KEY},
        [{"message_id": "m1", "kind": "result", "text": "secret result"}])

    assert accepted == [] and ctx.injected == []


def test_history_messages_are_recorded_even_without_a_status_message(monkeypatch):
    ctx = FakeCtx()
    monkeypatch.setattr(tools, "_CTX", ctx)
    payload = {
        "status": {"state": protocol.STATE_COMPLETED},
        "artifacts": [],
        "history": [
            {"messageId": "r0", "role": "user", "parts": [{"kind": "text", "text": "do the work"}]},
            {"messageId": "h1", "role": "agent", "parts": [{"kind": "text", "text": "halfway"}]},
        ],
    }

    queued = tools._record_observation("t-hist", payload, "peer")

    ids = [m["message_id"] for m in queued]
    assert ids[0] == "h1", "peer history is recorded first, in order"
    assert "r0" not in ids, "our own request is not peer progress"


def test_release_drops_an_aggregate_notice_naming_peer_slash_task():
    """Production expiry notices list tasks as "peer/task", not bare ids."""
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    watch = _add(manager, tasks=(("peer", "t1"), ("peer", "t2")))
    with _store()._session() as con:
        con.execute(
            "INSERT INTO a2a_watch_publications (event_id, watch_id, content,"
            " metadata_json, created_at) VALUES ('evt1', ?, 'notice', ?, ?)",
            (watch["watch_id"], '{"source": "a2a", "state": "watch-expired",'
             ' "tasks": ["peer/t1", "peer/t2"]}', 1000.0))

    assert manager.ownership.release("t1", SESSION_ID)

    assert [_meta(r)["state"] for r in _outbox(watch["watch_id"], pending_only=True)] == []


def test_a_row_deleted_by_release_is_not_injected_from_a_stale_snapshot():
    """The flush must re-read under the write lock; a snapshot alone can outlive ownership."""
    ctx = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=ctx)
    watch = _add(manager)
    manager.run_once(now=1_000.0)  # inject refused -> durable outbox row
    assert _outbox(watch["watch_id"], pending_only=True)

    _store().due_publications(now=2_000.0)  # a flush pass that read the snapshot already
    manager.cancel_session("sess-1")        # release + adoption elsewhere
    ctx.fail = False
    assert manager.flush_publications(now=2_000.0) == 0
    assert ctx.injected == [], "a released task must not inject into the session that gave it up"


def test_an_oversized_result_is_truncated_to_the_host_limit():
    text = "x" * 33_000
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text=text)))
    watch = _add(manager)

    manager.run_once(now=1_000.0)

    [injected] = manager.ctx.injected
    assert len(injected["content"]) < 32_000
    assert "characters truncated" in injected["content"]
    assert _outbox(watch["watch_id"], pending_only=True) == []


def test_a_permanently_refused_publication_is_retired_not_retried():
    """A host that refuses every retry past the attempt cap is not coming back within the watch's
    life: the row must be retired so it cannot spin forever."""

    class RejectingCtx(FakeCtx):
        def inject_message(self, content, role="user", session_key=None, origin=None):
            return False  # permanent refusal; never throws

    ctx = RejectingCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="done")),
                       ctx=ctx)
    watch = _add(manager)

    # Drive enough flush passes to exceed _MAX_PUBLICATION_ATTEMPTS (12).
    now = 1_000.0
    for _ in range(watches._MAX_PUBLICATION_ATTEMPTS + 2):
        manager.run_once(now=now)
        now += 10_000.0

    # The final row is either retired (published_at non-NULL with a marker) or simply never
    # injected again; the contract is "no row spinning forever" and "host saw nothing".
    assert ctx.injected == [], "a permanent refusal must never reach the host's accepted list"
    for row in _outbox(watch["watch_id"]):
        assert row["attempts"] <= watches._MAX_PUBLICATION_ATTEMPTS, (
            "retired rows must not accumulate unbounded attempts"
        )


def test_an_expiring_watch_is_finished_after_a_restart():
    """Crash between 'expiring' and enqueue: a fresh manager must finish the notice."""
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    watch = _add(manager)
    with _store()._session() as con:
        con.execute("UPDATE a2a_watches SET expires_at=1 WHERE watch_id=?", (watch["watch_id"],))

    expired = _store().expire(now=10_000.0)  # commit happens, then the process dies
    assert expired and _store().get(watch["watch_id"])["state"] == "expiring"

    fresh = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)))
    fresh.run_once(now=10_000.0)

    [row] = _outbox(watch["watch_id"])
    assert _meta(row)["state"] == "watch-expired"
    assert _store().get(watch["watch_id"])["state"] == "expired"


def test_an_input_required_notice_that_could_not_be_queued_is_retried():
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_INPUT_REQUIRED,
                                                 text="which key?")))
    watch = _add(manager)
    store = _store()
    real_enqueue = store.enqueue_publication
    calls = {"n": 0}

    def flaky_enqueue(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("ledger locked")
        return real_enqueue(**kwargs)

    store.enqueue_publication = flaky_enqueue  # type: ignore[method-assign]
    manager.store = store

    manager.run_once(now=1_000.0)

    item = _store().get(watch["watch_id"])["items"][0]
    assert item["state"] != watches.ITEM_PAUSED, (
        "a task must not stop polling before its notice is durably queued"
    )
    assert manager.ctx.injected == []

    manager.run_once(now=1_010.0)
    [injected] = manager.ctx.injected
    assert "which key?" in injected["content"]
    assert _store().get(watch["watch_id"])["items"][0]["state"] == watches.ITEM_PAUSED


def test_a_refused_input_required_notice_is_retried():
    ctx = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_INPUT_REQUIRED,
                                                 text="which key?")), ctx=ctx)
    watch = _add(manager)
    manager.run_once(now=1_000.0)
    assert ctx.injected == []
    [pending] = _outbox(watch["watch_id"])
    assert pending["metadata_json"].find("input-required") >= 0

    ctx.fail = False
    manager.run_once(now=pending["next_attempt_at"] + 0.001)
    [injected] = ctx.injected
    assert "which key?" in injected["content"]


def test_a_refused_expiry_notice_is_retried():
    ctx = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_WORKING)), ctx=ctx)
    watch = _add(manager)
    with _store()._session() as con:
        con.execute("UPDATE a2a_watches SET expires_at=1 WHERE watch_id=?", (watch["watch_id"],))
    manager.run_once(now=10_000.0)
    assert ctx.injected == []
    [pending] = _outbox(watch["watch_id"])
    assert "watch-expired" in pending["metadata_json"]

    ctx.fail = False
    manager.run_once(now=pending["next_attempt_at"] + 0.001)
    [injected] = ctx.injected
    assert "watch-expired" in injected["content"] or "expired" in injected["content"].lower()


def test_the_lease_is_renewed_between_tasks_and_stale_writes_are_fenced():
    clock = [1_000.0]
    interlopers: list[bool] = []
    box: dict[str, str] = {}
    store = _store()
    other = watches.WatchManager(FakeCtx(), store=store, poll=lambda p, t: {}, interval=5.0,
                                 ttl_s=3600, clock=lambda: clock[0])

    def poll(peer, task_id):
        clock[0] += 60.0  # four polls would outlive the 90s lease if it were never renewed
        if box.get("watch_id"):
            # Mid-batch, with the clock 60s further on than the claim: still ours?
            interlopers.append(store.claim(box["watch_id"], other.owner, clock[0]))
        return task_payload(task_id=task_id, state=protocol.STATE_WORKING)

    manager = watches.WatchManager(FakeCtx(), store=store, poll=poll, interval=5.0,
                                   ttl_s=3600, clock=lambda: clock[0])
    watch = _add(manager, tasks=(("peer", "t1"), ("peer", "t2"), ("peer", "t3"), ("peer", "t4")))
    box["watch_id"] = watch["watch_id"]

    assert manager.run_once(now=clock[0]) == 4
    assert clock[0] > 1_000.0 + watches.LEASE_S, "the batch ran past the initial lease"
    assert interlopers and not any(interlopers), (
        "the lease expired mid-batch: a second process could take over and double-poll"
    )

    # Only after the lease really lapses may another owner take over ...
    assert store.claim(watch["watch_id"], other.owner, clock[0] + watches.LEASE_S) is True
    # ... and the old owner's writes are then rejected instead of overwriting the new owner's.
    old_item = dict(store.get(watch["watch_id"])["items"][0])
    manager.store.mark_item(watch["watch_id"], old_item["peer"], old_item["task_id"],
                            watches.ITEM_DONE, owner=manager.owner)
    assert store.get(watch["watch_id"])["items"][0]["completed_at"] is None, (
        "a write from a process that lost the lease must be fenced out"
    )


def test_background_work_uses_the_creating_profiles_scope(tmp_path, monkeypatch):
    """The same peer alias resolves per profile: the watcher must not use the launch profile."""
    pytest.importorskip("hermes_constants", reason="needs the Hermes source checkout")
    home_a, home_b = tmp_path / "profile-a", tmp_path / "profile-b"
    for home, url in ((home_a, "http://10.0.0.1:9901"), (home_b, "http://10.0.0.2:9901")):
        home.mkdir(parents=True)
        (home / "config.yaml").write_text(
            "a2a_agents:\n"
            "  peer:\n"
            f"    url: {url}\n"
            "    auth: {type: bearer, token: test}\n",
            encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home_a))  # ambient = launch profile
    resolved: list[str] = []
    persisted: list[str] = []
    from hermes_constants import get_hermes_home

    def poll(peer, task_id):
        resolved.append(tools._resolve_peer(peer)["url"])
        return task_payload(task_id=task_id, state=protocol.STATE_COMPLETED, text="done")

    monkeypatch.setattr(protocol, "persist_message",
                        lambda *a, **kw: persisted.append(str(get_hermes_home())))

    manager = watches.WatchManager(FakeCtx(), store=_store(), poll=poll, interval=5.0,
                                   ttl_s=3600, profile_home=str(home_b))
    _add(manager)
    manager.run_once(now=1_000.0)

    assert resolved == ["http://10.0.0.2:9901"]
    assert persisted == [str(home_b)], "the reply transcript must land in the creating profile"
    assert str(get_hermes_home()) == str(home_a), "the launch profile's scope must be restored"

# ── Amy PR #9 findings 2-4 ───────────────────────────────────────────────────

def _artifact_payload(state, text, artifact_id="a1", task_id="t1"):
    return {"id": task_id, "contextId": "ctx-1", "status": {"state": state},
            "artifacts": [{"artifactId": artifact_id, "parts": [{"text": text}]}]}


def test_a_completed_artifact_revision_is_not_dropped_as_a_duplicate():
    """A stable artifactId identifies an artifact, not one immutable revision.

    Amy's PR #9 finding 2: a working snapshot with "partial" and a completed snapshot with the
    same id carrying the final answer delivered only "partial", then closed the watch.
    """
    ctx = FakeCtx()
    manager = _manager(ScriptedPoll(
        _artifact_payload(protocol.STATE_WORKING, "partial"),
        _artifact_payload(protocol.STATE_COMPLETED, "complete final answer"),
    ), ctx=ctx)
    watch = _add(manager)

    manager.run_once(now=1_000.0)   # working snapshot: "partial" is delivered
    manager.run_once(now=1_005.0)   # completed: same artifact id, new content

    assert any("complete final answer" in m["content"] for m in ctx.injected), (
        "the final revision of an already-seen artifact must reach the session")
    assert _store().get(watch["watch_id"])["state"] == watches.WATCH_COMPLETE


def test_the_blocking_recorder_keeps_a_new_revision_of_the_same_artifact():
    """The blocking await shared the artifact-ID-only dedup; both recorders use the revision key."""
    tools._record_observation(
        "t1", _artifact_payload(protocol.STATE_WORKING, "partial"), "peer")
    queued = tools._record_observation(
        "t1", _artifact_payload(protocol.STATE_COMPLETED, "complete final answer"), "peer")

    assert any("complete final answer" in m["text"] for m in queued)
    # Re-observing the identical revision stays idempotent.
    again = tools._record_observation(
        "t1", _artifact_payload(protocol.STATE_COMPLETED, "complete final answer"), "peer")
    assert len([m for m in again if "complete final answer" in m["text"]]) == 1


def test_a_direct_replay_settles_the_pending_outbox_row():
    """Amy's PR #9 finding 3: refuse, replay the backlog directly, then the flush must not
    inject the same durable message a second time."""
    ctx = FlakyCtx()
    manager = _manager(ScriptedPoll(task_payload(state=protocol.STATE_COMPLETED, text="answer")),
                       ctx=ctx)
    watch = _add(manager)
    manager.run_once(now=1_000.0)  # terminal: the outbox row is written, the host refuses
    assert _outbox(watch["watch_id"], pending_only=True)
    queued = manager.ownership.queued("t1")
    assert queued

    ctx.fail = False
    accepted = tools._publish_queued(ctx, "t1", "peer", SESSION_ID,
                                     {"session_key": TARGET_SESSION_KEY}, queued)
    assert len(accepted) == 1
    assert len(ctx.injected) == 1

    manager.flush_publications(now=1_000.0 + 10_000.0)
    assert len(ctx.injected) == 1, (
        "the replay must settle the matching outbox row, not leave it publishable")
    assert _outbox(watch["watch_id"], pending_only=True) == []
    assert manager.ownership.queued("t1") == []


def test_stop_does_not_abandon_a_live_poller_for_a_second_one(monkeypatch):
    """Amy's PR #9 finding 4: a timed-out stop dropped the handle, start() cleared the shared
    stop flag, and two pollers ran under one lease-owner token."""
    monkeypatch.setattr(watches.WatchManager, "start", _REAL_START)
    in_poll, release = threading.Event(), threading.Event()

    def poll(*_args):
        in_poll.set()
        assert release.wait(10)
        return {"id": "t1", "status": {"state": protocol.STATE_WORKING}}

    manager = watches.WatchManager(FakeCtx(), store=_store(), poll=poll, interval=0.01,
                                   ttl_s=3600)
    _add(manager)
    old = manager._thread
    try:
        assert old is not None and in_poll.wait(5), "the poller must be inside a peer request"
        manager.stop(timeout=0.01)
        assert old.is_alive(), "the stop timed out; the old poller is still in flight"
        manager.start()
        assert manager._thread is old, "a second poller must not start beside the live one"
    finally:
        manager.stop(timeout=0.0)  # cancel the deferred hand-over
        release.set()
        manager.stop(timeout=5.0)
        old.join(timeout=5)
    assert not old.is_alive()


# ── Local review of the PR #9 fixes (D1-D4) ─────────────────────────────────

def test_the_answer_survives_a_crash_after_the_fence(monkeypatch):
    """D1: the fence must write the task row in the same transaction as the acknowledgement.

    A crash between 'consume' and 'TaskStore.complete' used to consume the only durable copy of
    the peer's text; the reply must already be in the task row when the fence commits.
    """
    protocol.TaskStore().create("t1", "ctx-1", "peer", direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    manager = _manager(ScriptedPoll(_artifact_payload(protocol.STATE_COMPLETED,
                                                      "THE FINAL ANSWER")))
    assert manager.ownership.acquire("t1", SESSION_ID)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    payload = _artifact_payload(protocol.STATE_COMPLETED, "THE FINAL ANSWER")
    monkeypatch.setattr(tools, "_rpc_request",
                        lambda peer, method, params, timeout=None: {"result": payload})
    monkeypatch.setattr(protocol.TaskStore, "complete",
                        lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("crash")))

    with pytest.raises(RuntimeError, match="crash"):
        tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 5},
                        SESSION_ID)

    fresh = protocol.TaskStore()
    record = fresh.get("t1")
    assert record["state"] == protocol.STATE_COMPLETED, "the fence must persist the terminal state"
    assert record["reply"] == "THE FINAL ANSWER", "the answer must be durable before the mirror"
    assert manager.ownership.queued("t1") == []


def test_a_streaming_artifact_delivers_once_with_its_final_content():
    """D2: one stable artifactId is one message, however many revisions the peer streams."""
    ctx = FakeCtx()
    revisions = [_artifact_payload(protocol.STATE_WORKING, f"chunk {i}") for i in range(8)]
    revisions.append(_artifact_payload(protocol.STATE_COMPLETED, "final"))
    manager = _manager(ScriptedPoll(*revisions), ctx=ctx)
    _add(manager)

    for step in range(len(revisions)):
        manager.run_once(now=1_000.0 + step * 5.0)

    assert len(ctx.injected) == 1, f"one artifact must not fan out per revision: {len(ctx.injected)}"
    assert "final" in ctx.injected[0]["content"]
    [row] = [m for m in manager.ownership.queued("t1")] or [{"text": "final"}]
    assert "final" in row["text"]


def test_a_changed_body_reopens_the_settled_outbox_row():
    """D2: an artifact keeps its event id across revisions; new content must be deliverable."""
    store = _store()
    store.create(watch_id="w", session_id=SESSION_ID,
                 target={"session_key": TARGET_SESSION_KEY}, tasks=[("peer", "t1")], ttl_s=3600)
    meta = {"task_id": "t1", "message_id": "a1", "session_id": SESSION_ID}

    store.enqueue_publication(watch_id="w", event_id="evt", content="partial", metadata=meta)
    store.mark_published("evt", now=1.0)
    assert _outbox("w", pending_only=True) == []

    store.enqueue_publication(watch_id="w", event_id="evt", content="final", metadata=meta)
    [reopened] = _outbox("w", pending_only=True)
    assert reopened["content"] == "final"

    store.mark_published("evt", now=2.0)
    store.enqueue_publication(watch_id="w", event_id="evt", content="final", metadata=meta)
    assert _outbox("w", pending_only=True) == [], "an identical body must stay settled"


def test_a_stop_that_returned_cannot_be_undone_by_a_queued_handover(monkeypatch):
    """D3: a deferred hand-over queued before a later stop() must not start a poller."""
    monkeypatch.setattr(watches.WatchManager, "start", _REAL_START)
    in_poll, release = threading.Event(), threading.Event()

    def poll(*_args):
        in_poll.set()
        assert release.wait(10)
        return {"id": "t1", "status": {"state": protocol.STATE_WORKING}}

    manager = watches.WatchManager(FakeCtx(), store=_store(), poll=poll, interval=0.01,
                                   ttl_s=3600)
    _add(manager)
    old = manager._thread
    started: list[int] = []
    real_start_locked = watches.WatchManager._start_locked
    try:
        assert in_poll.wait(5)
        manager.stop(timeout=0.01)          # the poller is still in flight
        manager.start()                     # queued hand-over for this stop
        monkeypatch.setattr(watches.WatchManager, "_start_locked",
                            lambda self: (started.append(1), real_start_locked(self)))
        manager.stop(timeout=0.0)           # a stop() that returns before the old thread exits
    finally:
        release.set()
        old.join(timeout=5)
    assert started == [], "a poller started after stop() returned"
    assert not old.is_alive()


def test_unreadable_publication_metadata_does_not_abort_a_publish():
    """D4: settlement parses metadata in Python; a malformed row must not roll back an accepted
    publish (the host already received the message)."""
    manager = _manager(ScriptedPoll(_artifact_payload(protocol.STATE_WORKING, "x")))
    protocol.TaskStore(_store().db_path).create("t1", "t1", "peer", direction="outbound",
                                                remote_state=protocol.STATE_WORKING)
    assert manager.ownership.acquire("t1", SESSION_ID)
    manager.ownership.add_message("t1", "m1", "result", "answer")
    watch = _add(manager)
    with _store()._session() as con:
        con.execute(
            "INSERT INTO a2a_watch_publications (event_id, watch_id, content, metadata_json,"
            " created_at) VALUES ('bad-row', ?, 'x', '{broken', 1.0)", (watch["watch_id"],))

    assert manager.publish_if_owned(SESSION_ID, "t1", lambda: True, mark_messages=["m1"]) is True
    assert manager.ownership.queued("t1") == [], "the accepted message must be marked delivered"
    with _store()._session() as con:
        row = con.execute(
            "SELECT published_at FROM a2a_watch_publications WHERE event_id='bad-row'").fetchone()
    assert row["published_at"] is None, "an unreadable row must be left alone, not settled"


# ── Local review round 2 (N1-N5) ────────────────────────────────────────────

def test_a_later_poll_cannot_overwrite_a_recorded_answer(monkeypatch):
    """N1: the fence mirrors TaskStore's refusal to transition out of a terminal state."""
    protocol.TaskStore().create("t1", "ctx-1", "peer", direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    manager = _manager(ScriptedPoll(_artifact_payload(protocol.STATE_COMPLETED, "THE ANSWER")))
    assert manager.ownership.acquire("t1", SESSION_ID)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    monkeypatch.setattr(
        tools, "_rpc_request",
        lambda *a, **k: {"result": _artifact_payload(protocol.STATE_COMPLETED, "THE ANSWER")})
    first = tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 5},
                            SESSION_ID)
    assert "THE ANSWER" in first

    # The peer re-reports a different terminal state for the same task.
    monkeypatch.setattr(
        tools, "_rpc_request",
        lambda *a, **k: {"result": {"id": "t1", "contextId": "ctx-1",
                                    "status": {"state": protocol.STATE_FAILED}}})
    tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 5}, SESSION_ID)

    record = protocol.TaskStore().get("t1")
    assert record["state"] == protocol.STATE_COMPLETED, "a finished answer must not be overwritten"
    assert record["reply"] == "THE ANSWER"

    # Nor may it be un-terminalized and swept as unfinished work.
    manager.ownership.release_all(SESSION_ID)
    cancel, _abandon = manager.ownership.unowned_due(
        now=time.time() + 10 ** 6, cancel_after_s=0, ttl_s=10 ** 9)
    assert "t1" not in [c["task_id"] for c in cancel]


def test_artifact_and_history_ids_do_not_collide():
    """N2: the artifact key is namespaced; a peer reusing an id must not lose history text."""
    payload = {"id": "t1", "contextId": "ctx-1",
               "status": {"state": protocol.STATE_COMPLETED},
               "history": [{"role": "agent", "messageId": "m-1",
                            "parts": [{"text": "HISTORY"}]}],
               "artifacts": [{"artifactId": "m-1", "parts": [{"text": "ARTIFACT"}]}]}

    queued = tools._record_observation("t1", payload, "peer")

    by_key = {(m["message_id"], m["kind"]): m["text"] for m in queued}
    assert by_key[("m-1", "progress")] == "HISTORY"
    assert by_key[("artifact-id:m-1", "result")] == "ARTIFACT"


def test_an_artifact_without_an_id_keeps_one_row():
    """N3: without an id the position is the only stable identity; revisions must not pile up."""
    for index in range(6):
        payload = {"id": "t1", "contextId": "ctx-1",
                   "status": {"state": protocol.STATE_WORKING},
                   "artifacts": [{"parts": [{"text": f"chunk {index}"}]}]}
        tools._record_observation("t1", payload, "peer")

    final = {"id": "t1", "contextId": "ctx-1", "status": {"state": protocol.STATE_WORKING},
             "artifacts": [{"parts": [{"text": "chunk 5"}]}]}
    queued = tools._record_observation("t1", final, "peer")
    assert len(queued) == 1
    assert queued[0]["message_id"] == "artifact-pos:0"
    assert queued[0]["text"] == "chunk 5"


def test_a_terminal_row_without_an_answer_accepts_the_later_answer(monkeypatch):
    """Round 3 N1: the guard must not refuse a record that has no answer to protect."""
    protocol.TaskStore().create("t1", "ctx-1", "peer", direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    manager = _manager(ScriptedPoll(_artifact_payload(protocol.STATE_COMPLETED, "THE ANSWER")))
    assert manager.ownership.acquire("t1", SESSION_ID)
    with manager.ownership._session() as con:
        # A terminal row with no reply, e.g. one terminalized by an explicit get_task failure.
        con.execute("UPDATE a2a_tasks SET state=?, reply='' WHERE task_id=?",
                    (protocol.STATE_FAILED, "t1"))
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    monkeypatch.setattr(
        tools, "_rpc_request",
        lambda *a, **k: {"result": _artifact_payload(protocol.STATE_COMPLETED, "THE ANSWER")})

    tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 5}, SESSION_ID)

    record = protocol.TaskStore().get("t1")
    assert record["state"] == protocol.STATE_COMPLETED
    assert record["reply"] == "THE ANSWER", "an empty row must still record the answer"


def test_a_placeholder_failure_does_not_block_the_real_answer(monkeypatch):
    """Round 3: a row whose only text was our own 'no text reply' placeholder must accept the
    answer that arrives later, and a later text-less poll must recover it from the row."""
    protocol.TaskStore().create("t1", "ctx-1", "peer", direction="outbound",
                                remote_state=protocol.STATE_WORKING)
    manager = _manager(ScriptedPoll(_artifact_payload(protocol.STATE_COMPLETED, "THE ANSWER")))
    assert manager.ownership.acquire("t1", SESSION_ID)
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})

    # 1. The peer fails with no text: our placeholder is not a durable reply.
    monkeypatch.setattr(tools, "_rpc_request",
                        lambda *a, **k: {"result": {"id": "t1", "contextId": "ctx-1",
                                                    "status": {"state": protocol.STATE_FAILED}}})
    first = tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 5},
                            SESSION_ID)
    assert "no text reply" in first
    assert (protocol.TaskStore().get("t1") or {}).get("reply") == ""

    # 2. The answer arrives later and replaces the empty terminal row.
    monkeypatch.setattr(
        tools, "_rpc_request",
        lambda *a, **k: {"result": _artifact_payload(protocol.STATE_COMPLETED, "THE ANSWER")})
    tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 5}, SESSION_ID)
    record = protocol.TaskStore().get("t1")
    assert record["state"] == protocol.STATE_COMPLETED
    assert record["reply"] == "THE ANSWER"

    # 3. A later poll with no text recovers the recorded answer.
    monkeypatch.setattr(
        tools, "_rpc_request",
        lambda *a, **k: {"result": {"id": "t1", "contextId": "ctx-1",
                                    "status": {"state": protocol.STATE_COMPLETED}}})
    out = tools.a2a_await({"agent": "peer", "task_id": "t1", "interval": 1, "timeout": 5},
                          SESSION_ID)
    assert "THE ANSWER" in out


def test_a_stale_report_cannot_flip_a_finished_task_with_no_text(monkeypatch):
    """Round 4: a completed task with an empty reply must not be rewritten by a stale report.

    The empty-reply escape hatch exists so a later REAL answer can be recorded; a text-less
    report must not un-terminalize the task (which would put it in the auto-cancel sweep) or
    rewrite its outcome.
    """
    manager = _manager(ScriptedPoll(_artifact_payload(protocol.STATE_COMPLETED, "unused")))
    monkeypatch.setattr(tools, "_resolve_peer", lambda agent: {"url": "http://peer.test"})
    for task_id, stale in (("t1", protocol.STATE_FAILED),
                           ("t2", protocol.STATE_INPUT_REQUIRED),
                           ("t3", protocol.STATE_COMPLETED)):
        protocol.TaskStore(_store().db_path).create(
            task_id, "ctx-1", "peer", direction="outbound",
            remote_state=protocol.STATE_COMPLETED)
        assert manager.ownership.acquire(task_id, SESSION_ID)
        with manager.ownership._session() as con:
            con.execute("UPDATE a2a_tasks SET state=?, reply='' WHERE task_id=?",
                        (protocol.STATE_COMPLETED, task_id))
        monkeypatch.setattr(
            tools, "_rpc_request",
            lambda *a, _stale=stale, _tid=task_id, **k: {
                "result": {"id": _tid, "contextId": "ctx-1", "status": {"state": _stale}}})
        tools.a2a_await({"agent": "peer", "task_id": task_id, "interval": 1, "timeout": 5},
                        SESSION_ID)
        record = protocol.TaskStore(_store().db_path).get(task_id)
        assert record["state"] == protocol.STATE_COMPLETED, (
            f"a stale {stale} report rewrote a finished task")
        assert record["reply"] == ""

    manager.ownership.release_all(SESSION_ID)
    cancel, abandon = manager.ownership.unowned_due(
        now=time.time() + 10 ** 6, cancel_after_s=0, ttl_s=10 ** 9)
    swept = {c["task_id"] for c in cancel} | {a["task_id"] for a in abandon}
    assert swept == set(), f"a finished task was swept as unfinished work: {swept}"


def test_named_and_positional_artifacts_do_not_alias():
    """Round 3 N2: artifactId "1" and an id-less artifact at index 1 are different artifacts."""
    payload = {"id": "t1", "contextId": "ctx-1",
               "status": {"state": protocol.STATE_WORKING},
               "artifacts": [{"artifactId": "1", "parts": [{"text": "NAMED"}]},
                             {"parts": [{"text": "IDLESS"}]}]}

    queued = tools._record_observation("t1", payload, "peer")

    by_key = {m["message_id"]: m["text"] for m in queued}
    assert by_key == {"artifact-id:1": "NAMED", "artifact-pos:1": "IDLESS"}
