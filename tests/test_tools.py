from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import sys

ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
spec = __import__("importlib.util", fromlist=["spec_from_file_location"]).spec_from_file_location(
    "a2a_async_plugin", ROOT / "__init__.py", submodule_search_locations=[str(ROOT)]
)
assert spec and spec.loader
plugin = __import__("importlib.util", fromlist=["module_from_spec"]).module_from_spec(spec)
sys.modules[spec.name] = plugin
spec.loader.exec_module(plugin)
from a2a_async_plugin import tools


class Peer(BaseHTTPRequestHandler):
    response = {}
    seen = []

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
        length = int(self.headers.get("Content-Length", 0))
        request = json.loads(self.rfile.read(length))
        type(self).seen.append(request)
        result = type(self).response
        body = json.dumps({"jsonrpc": "2.0", "id": request.get("id"), "result": result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def peer_server(monkeypatch):
    server = HTTPServer(("127.0.0.1", 0), Peer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    monkeypatch.setattr(tools, "_load_config", lambda: {"a2a_agents": {"peer": {"url": url}}})
    return server, url


def test_submit_sends_nonblocking_request(monkeypatch):
    server, _url = peer_server(monkeypatch)
    try:
        Peer.response = {
            "id": "task-1",
            "contextId": "ctx-1",
            "status": {"state": tools.protocol.STATE_WORKING},
        }
        Peer.seen = []
        result = tools.a2a_submit({"agent": "peer", "message": "long job"})
        assert "task-1" in result
        assert Peer.seen[0]["method"] == "SendMessage"
        assert Peer.seen[0]["params"]["configuration"]["returnImmediately"] is True
    finally:
        server.shutdown()
        server.server_close()


def test_get_task_returns_remote_state(monkeypatch):
    server, _url = peer_server(monkeypatch)
    try:
        Peer.response = {
            "id": "task-1",
            "contextId": "ctx-1",
            "status": {"state": tools.protocol.STATE_COMPLETED},
            "artifacts": [{"parts": [{"text": "finished"}]}],
        }
        result = tools.a2a_get_task({"agent": "peer", "task_id": "task-1"})
        assert "completed" in result
        assert "finished" in result
    finally:
        server.shutdown()
        server.server_close()


def _rpc_error_response(request, code, message):
    body = json.dumps({"jsonrpc": "2.0", "id": request.get("id"), "error": {"code": code, "message": message}}).encode()
    return body


def test_get_task_rpc_error_does_not_terminalize_local_row(monkeypatch):
    """GitHub issue #4: a JSON-RPC error from GetTask (-32001 TaskNotFound,
    -32601 MethodNotFound, internal errors) is an RPC-level failure, not
    proof the peer's task failed. The local row must stay non-terminal so a
    later successful poll can reconcile."""
    import sqlite3
    import tempfile, os
    server, _url = peer_server(monkeypatch)
    # Point the store at an isolated temp DB so the live profile store is
    # not touched. TaskStore resolves A2A_TASKS_DB per-instance at init.
    tmpdir = tempfile.mkdtemp()
    monkeypatch.setenv("A2A_TASKS_DB", os.path.join(tmpdir, "tasks.db"))
    store = tools.protocol.TaskStore()
    store.create("task-out-1", "ctx-out-1", peer="peer", agent_slug="", tenant="",
                 direction="outbound", ownership="remote", remote_state="TASK_STATE_WORKING")
    store.set_state("task-out-1", tools.protocol.STATE_WORKING)
    try:
        for code in (-32001, -32601, -32603):
            def handler(self, code=code):
                request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                body = _rpc_error_response(request, code, "error")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            old_post = Peer.do_POST
            Peer.do_POST = handler
            try:
                result = tools.a2a_get_task({"agent": "peer", "task_id": "task-out-1"})
                assert f"Error" in result
                rec = store.get("task-out-1")
                assert rec is not None
                assert rec["state"] == tools.protocol.STATE_WORKING, (
                    f"RPC error {code} terminalized the local row: state={rec['state']!r}"
                )
            finally:
                Peer.do_POST = old_post
        # Reconciliation still works: a subsequent COMPLETED poll finalizes.
        # Read back through a FRESH store: TaskStore.get() serves from
        # per-instance memory, and the tool internally uses its own
        # instances — the DB is the shared source of truth this test can
        # actually observe across instances.
        Peer.response = {
            "id": "task-out-1",
            "contextId": "ctx-out-1",
            "status": {"state": tools.protocol.STATE_COMPLETED},
        }
        tools.a2a_get_task({"agent": "peer", "task_id": "task-out-1"})
        fresh = tools.protocol.TaskStore()
        assert fresh.get("task-out-1")["state"] == tools.protocol.STATE_COMPLETED
    finally:
        server.shutdown()
        server.server_close()


def test_a2a_list_kind_tasks_does_not_claim_no_peers(monkeypatch):
    """kind='tasks'/'conversations' skips the peer block without falsely
    reporting 'No peers configured' when peers are actually configured."""
    monkeypatch.setattr(tools, "_load_config", lambda: {"a2a_agents": {"sven": {"url": "http://127.0.0.1:9901"}}})
    out = tools.a2a_list({"kind": "tasks"})
    assert "No peers configured" not in out


def test_a2a_list_no_peers_reports_missing_config(monkeypatch):
    """When peers genuinely are unconfigured, the guidance still shows."""
    monkeypatch.setattr(tools, "_load_config", lambda: {"a2a_agents": {}})
    out = tools.a2a_list({"kind": "all"})
    assert "No peers configured" in out
