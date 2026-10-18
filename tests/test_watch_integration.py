"""End-to-end: a real Hermes load of this plugin turns a background watch into a delivered turn.

Unlike ``test_watches.py`` (fake PluginContext, injected poll), this runs the real discovery and
the real publisher: the plugin is installed into a temporary ``HERMES_HOME``, a scripted A2A peer
answers ``GetTask``, ``a2a_await(background=true)`` is dispatched through the tool registry with a
bound session, and the completion must land in Hermes' durable ledger and on its completion queue
with the peer's reply in the formatted text.

Skipped unless a Hermes checkout with its dependencies is importable::

    PYTHONPATH=/path/to/hermes-agent /path/to/hermes-agent/.venv/bin/python \
        -m pytest tests/test_watch_integration.py -q
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

pc = pytest.importorskip("agent.plugin_completions")
pytest.importorskip("hermes_cli.plugins")


class Peer(BaseHTTPRequestHandler):
    """A2A peer: one WORKING then COMPLETED GetTask per task id."""

    polls: dict[str, int] = {}
    seen: list[dict] = []
    keep_working: set[str] = set()

    def log_message(self, *_args):
        pass

    def do_GET(self):
        body = b'{"name":"test-peer"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        type(self).seen.append(request)
        task_id = str((request.get("params") or {}).get("id") or "t1")
        count = type(self).polls.get(task_id, 0)
        type(self).polls[task_id] = count + 1
        state = ("TASK_STATE_WORKING" if task_id in type(self).keep_working or count == 0
                 else "TASK_STATE_COMPLETED")
        payload = {"id": task_id, "contextId": "ctx-1", "status": {"state": state}}
        if state.endswith("COMPLETED"):
            payload["artifacts"] = [{"parts": [{"text": f"peer finished {task_id}"}]}]
        body = json.dumps({"jsonrpc": "2.0", "id": request.get("id"), "result": payload}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def hermetic_home(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_WATCH_INTERVAL", "2")
    monkeypatch.setenv("A2A_TASKS_DB", str(home / "a2a_tasks.db"))
    return home


@pytest.fixture
def peer(monkeypatch):
    server = HTTPServer(("127.0.0.1", 0), Peer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    Peer.polls = {}
    Peer.seen = []
    Peer.keep_working = set()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def _wait(predicate, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return False


def test_background_await_delivers_through_real_hermes(hermetic_home, peer, monkeypatch):
    import hermes_yaml as yaml
    from gateway.session_context import clear_session_vars, set_session_vars
    from hermes_cli import plugins as plugins_mod
    from tools.process_registry import process_registry
    from tools.registry import registry

    plugin_dir = hermetic_home / "plugins" / "a2a-async"
    shutil.copytree(ROOT, plugin_dir, ignore=shutil.ignore_patterns(
        "__pycache__", ".git", ".pytest_cache", "tests", "docs", ".hermes"))
    (hermetic_home / "config.yaml").write_text(yaml.safe_dump({
        "plugins": {"enabled": ["a2a-async"]},
        "a2a_agents": {"peer": {"url": peer}},
    }), encoding="utf-8")

    previous = getattr(plugins_mod, "_plugin_manager", None)
    plugins_mod._plugin_manager = plugins_mod.PluginManager()
    tokens = set_session_vars(
        platform="telegram", source="telegram", chat_id="42", chat_type="dm",
        session_key="agent:main:telegram:dm:42", session_id="sess-int")
    try:
        plugins_mod.discover_plugins()
        result = registry.dispatch(
            "a2a_await",
            {"agent": "peer", "task_id": "t1", "background": True},
            session_id="sess-int",
        )
        assert result.startswith("[watch watch_"), result
        watch_id = result.split()[1]

        # The watch's own thread polls the peer and publishes through the real publisher. The
        # completion queue is process-global, so wait for THIS watch's event and leave any
        # unrelated events (another test's completions) where they were.
        queue = process_registry.completion_queue
        expected = f"a2a:{watch_id}:peer:t1:completed"
        deadline = time.time() + 20.0
        event, others = None, []
        while time.time() < deadline and event is None:
            while True:
                try:
                    candidate = queue.get_nowait()
                except Exception:
                    break
                if candidate.get("event_id") == expected:
                    event = candidate
                else:
                    others.append(candidate)
            if event is None:
                time.sleep(0.1)
        assert event is not None, f"the watch never published {expected}"
        for evt in others + [event]:  # restore the queue for the session drain below
            queue.put(evt)
        assert event["plugin_id"] == "a2a-async"
        assert event["event_id"] == f"a2a:{watch_id}:peer:t1:completed"
        assert "peer finished t1" in event["content"]
        assert event["metadata"]["state"] == "completed"
        assert event["session_key"] == "agent:main:telegram:dm:42"

        row = pc._existing_completion(
            "a2a-async", "agent:main:telegram:dm:42", event["event_id"])
        assert row is not None and row["delivery_state"] == "pending"

        # The session's drain claims and formats it, then acknowledges the ledger row.
        from tools.async_delegation import claim_event_delivery, complete_event_delivery
        drained = process_registry.drain_notifications(
            session_key="agent:main:telegram:dm:42",
            owns_event=lambda e: e.get("session_key") == "agent:main:telegram:dm:42")
        assert [(e.get("event_id"), text) for e, text in drained], "the completion was not drained"
        evt, text = next((e, t) for e, t in drained if e.get("event_id") == expected)
        assert "peer finished t1" in text and "plugin a2a-async" in text
        claim = claim_event_delivery(evt, "integration:cli")
        assert claim
        complete_event_delivery(evt, claim)
        assert pc._existing_completion(
            "a2a-async", "agent:main:telegram:dm:42", expected)["delivery_state"] == "delivered"

        # Cancelling a watch that is still polling stops it; the remote task is untouched and no
        # remote cancellation request is sent.
        out = registry.dispatch("a2a_watch_cancel", {"watch_id": watch_id}, session_id="sess-int")
        assert "already complete" in out, "a finished watch has nothing to cancel"

        Peer.keep_working = {"t2"}
        second = registry.dispatch(
            "a2a_await", {"agent": "peer", "task_id": "t2", "background": True},
            session_id="sess-int")
        second_id = second.split()[1]
        out = registry.dispatch("a2a_watch_cancel", {"watch_id": second_id}, session_id="sess-int")
        assert "canceled" in out, out
        assert not [r for r in Peer.seen if r.get("method") == "CancelTask"]
    finally:
        clear_session_vars(tokens)
        plugins_mod._plugin_manager = previous or plugins_mod.PluginManager()
