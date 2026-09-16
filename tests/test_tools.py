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
