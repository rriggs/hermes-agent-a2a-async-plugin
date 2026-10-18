#!/usr/bin/env python3
"""End-to-end driver: real Hermes CLI session + this plugin's background a2a_await.

Runs a real `hermes chat` on a pty against:
  * a scripted OpenAI-compatible LLM endpoint (the agent's model), and
  * a scripted A2A peer (GetTask: WORKING once, then COMPLETED),

then asserts that the background watch published through
PluginContext.publish_completion, that the CLI delivered it as a new turn in the
same session (the fake LLM sees a request carrying the plugin completion), and
that the durable ledger row was acknowledged.

Everything is local: two HTTP servers on loopback, one HERMES_HOME under /data.
Exit 0 = verified; non-zero = the phase named in the failure.
"""

from __future__ import annotations

import json
import os
import pty
import re
import select
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DATA = Path(os.environ.get("E2E_DATA", "/data"))
EVIDENCE = DATA / "evidence"
HERMES_HOME = DATA / "hermes"
PLUGIN_SRC = Path(os.environ.get("E2E_PLUGIN", "/plugin"))
HERMES_BIN = os.environ.get("E2E_HERMES", "hermes")

EVIDENCE.mkdir(parents=True, exist_ok=True)
log_lock = threading.Lock()


def log(message: str) -> None:
    with log_lock:
        print(f"[e2e] {message}", flush=True)


def dump(name: str, payload) -> None:
    (EVIDENCE / name).write_text(json.dumps(payload, indent=2, default=str))


# ── scripted completion endpoint ────────────────────────────────────────────

class CompletionServer(BaseHTTPRequestHandler):
    """OpenAI-compatible endpoint whose replies depend on the conversation."""

    requests: list[dict] = []
    states: dict[str, int] = {}

    def log_message(self, *_args):
        pass

    # -- helpers
    def _send_json(self, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/").endswith("/models"):
            self._send_json({"object": "list", "data": [{"id": "fake-model", "object": "model"}]})
            return
        self._send_json({"object": "list", "data": []})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        request = json.loads(self.rfile.read(length) or b"{}")
        index = len(type(self).requests)
        type(self).requests.append(request)
        dump(f"llm-request-{index}.json", request)
        reply = self._reply_for(request, index)
        log(f"LLM call {index}: returning {reply['kind']}")
        if request.get("stream"):
            self._stream(reply)
        else:
            self._send_json(self._completion(reply))

    def _conversation(self, request: dict) -> tuple[str, bool]:
        text = json.dumps(request.get("messages") or [], default=str)
        return text, "[PLUGIN COMPLETION" in text

    def _reply_for(self, request: dict, index: int) -> dict:
        if not request.get("messages"):
            # Metadata/probe call (no conversation): answer a harmless completion.
            return {"kind": "probe", "content": "ok"}
        text, saw_completion = self._conversation(request)
        if saw_completion:
            return {"kind": "completion-turn", "content": "COMPLETION_RECEIVED"}
        if "[watch watch_" in text:
            return {"kind": "watch-started", "content": "WATCH_STARTED"}
        if any(m.get("role") == "tool" for m in request.get("messages") or []):
            return {"kind": "after-tool", "content": "AFTER_TOOL"}
        return {
            "kind": "tool-call",
            "content": None,
            "tool_name": "a2a_await",
            "tool_args": json.dumps({
                "agent": "peer", "task_id": "t1", "background": True,
            }),
        }

    # -- wire shapes
    def _completion(self, reply: dict) -> dict:
        message: dict = {"role": "assistant", "content": reply.get("content")}
        finish = "stop"
        if reply.get("tool_name"):
            message["content"] = None
            message["tool_calls"] = [{
                "id": "call_e2e", "type": "function",
                "function": {"name": reply["tool_name"], "arguments": reply["tool_args"]},
            }]
            finish = "tool_calls"
        return {
            "id": "chatcmpl-e2e", "object": "chat.completion", "created": int(time.time()),
            "model": "fake-model",
            "choices": [{"index": 0, "message": message, "finish_reason": finish}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    def _stream(self, reply: dict) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        base = {"id": "chatcmpl-e2e", "object": "chat.completion.chunk",
                "created": int(time.time()), "model": "fake-model"}

        def emit(delta: dict, finish=None) -> None:
            chunk = dict(base, choices=[{"index": 0, "delta": delta, "finish_reason": finish}])
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.flush()

        if reply.get("tool_name"):
            emit({"role": "assistant", "content": None})
            emit({"tool_calls": [{
                "index": 0, "id": "call_e2e", "type": "function",
                "function": {"name": reply["tool_name"], "arguments": reply["tool_args"]},
            }]})
            emit({}, "tool_calls")
        else:
            emit({"role": "assistant", "content": reply.get("content") or ""})
            emit({}, "stop")
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


# ── scripted A2A peer ───────────────────────────────────────────────────────

class PeerServer(BaseHTTPRequestHandler):
    polls: dict[str, int] = {}
    seen: list[dict] = []

    def log_message(self, *_args):
        pass

    def do_GET(self):
        body = b'{"name":"e2e-peer","protocolVersion":"1.0"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        type(self).seen.append(request)
        task_id = str((request.get("params") or {}).get("id") or "t1")
        count = type(self).polls.get(task_id, 0)
        type(self).polls[task_id] = count + 1
        state = "TASK_STATE_WORKING" if count == 0 else "TASK_STATE_COMPLETED"
        result = {"id": task_id, "contextId": "ctx-e2e", "status": {"state": state}}
        if state.endswith("COMPLETED"):
            result["artifacts"] = [{"parts": [{"text": "peer finished t1: all checks passed"}]}]
        body = json.dumps({"jsonrpc": "2.0", "id": request.get("id"), "result": result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve(handler) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


# ── hermes under a pty ──────────────────────────────────────────────────────

def run_hermes(llm_port: int, peer_port: int, seed: str) -> tuple[subprocess.Popen, int, str]:
    config = (
        "plugins:\n"
        "  enabled:\n"
        "    - a2a-async\n"
        "providers:\n"
        "  e2e:\n"
        f"    api: \"http://127.0.0.1:{llm_port}/v1\"\n"
        "    api_key: e2e-test-key\n"
        "    transport: chat_completions\n"
        "    default_model: fake-model\n"
        "model:\n"
        "  provider: e2e\n"
        "  name: fake-model\n"
        "tools:\n"
        "  tool_search:\n"
        "    enabled: \"off\"\n"
        "platform_toolsets:\n"
        "  cli:\n"
        "    - hermes-cli\n"
        "    - a2a\n"
        "  local:\n"
        "    - hermes-cli\n"
        "    - a2a\n"
        "a2a_agents:\n"
        "  peer:\n"
        f"    url: \"http://127.0.0.1:{peer_port}\"\n"
        "    timeout: 30\n"
        "display:\n"
        "  background_process_notifications: all\n"
    )
    HERMES_HOME.mkdir(parents=True, exist_ok=True)
    (HERMES_HOME / "config.yaml").write_text(config)
    _install_plugin()

    cmd = [HERMES_BIN, "chat", "--yolo", "-q", seed]
    env = dict(os.environ)
    env.update({
        "HERMES_HOME": str(HERMES_HOME),
        "A2A_WATCH_INTERVAL": "2",
        "A2A_TASKS_DB": str(HERMES_HOME / "a2a_tasks.db"),
        "TERM": "xterm-256color",
        "COLUMNS": "120",
        "LINES": "40",
        "NO_COLOR": "1",
        "PYTHONUNBUFFERED": "1",
    })
    master, slave = pty.openpty()
    process = subprocess.Popen(
        cmd, stdin=slave, stdout=slave, stderr=slave, env=env, close_fds=True,
        start_new_session=True)
    os.close(slave)
    return process, master, cmd


def _install_plugin() -> None:
    """Copy the plugin under test into this home's plugin dir (the real discovery path)."""
    import shutil

    target = HERMES_HOME / "plugins" / "a2a-async"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(
        PLUGIN_SRC, target,
        ignore=shutil.ignore_patterns("__pycache__", ".git", ".pytest_cache", "tests", "docs", ".hermes"))


def pump(master: int, sink: list[str]) -> None:
    while True:
        try:
            ready, _, _ = select.select([master], [], [], 0.2)
        except (OSError, ValueError):
            return
        if master in ready:
            try:
                data = os.read(master, 4096)
            except OSError:
                return
            if not data:
                return
            sink.append(data.decode("utf-8", "replace"))


# ── assertions ──────────────────────────────────────────────────────────────

def wait_for(predicate, timeout: float, label: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise AssertionError(f"timeout waiting for {label}")


def main() -> int:
    # A previous run (or an interrupted one) may have left its gateway alive on this home; the
    # gateway stamps state.db with its home, so a reused path with a stale process refuses to boot.
    stop_stray_processes()
    time.sleep(0.5)
    import shutil
    shutil.rmtree(HERMES_HOME, ignore_errors=True)
    shutil.rmtree(EVIDENCE, ignore_errors=True)
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))

    llm = serve(CompletionServer)
    peer = serve(PeerServer)
    llm_port, peer_port = llm.server_address[1], peer.server_address[1]
    log(f"LLM on :{llm_port}, A2A peer on :{peer_port}, home {HERMES_HOME}")

    process, master, cmd = run_hermes(
        llm_port, peer_port, "Start a background A2A watch on peer task t1, then wait.")
    log("command: " + " ".join(cmd))
    output: list[str] = []
    threading.Thread(target=pump, args=(master, output), daemon=True).start()

    try:
        completion_request = wait_for(
            lambda: next((r for r in CompletionServer.requests
                          if "[PLUGIN COMPLETION" in json.dumps(r.get("messages") or [])), None),
            timeout=240, label="the completion turn at the model")

        transcript = "\n".join(output)
        (EVIDENCE / "hermes-output.txt").write_text(transcript)
        completion_text = json.dumps(completion_request.get("messages") or [])
        assert "peer finished t1" in completion_text, "the peer reply was not in the delivered turn"
        assert "plugin a2a-async" in completion_text, "provenance missing from the delivered turn"
        row = wait_for(lambda: _delivered_row(), timeout=30, label="the ledger ack")
        log(f"ledger row: {row['event_id']} state={row['delivery_state']}")
        assert row["delivery_state"] == "delivered"

        assert any(r.get("method") == "GetTask" for r in PeerServer.seen), "the peer was never polled"
        assert not any(r.get("method") == "CancelTask" for r in PeerServer.seen)
        # Watch id must have been returned by the tool: it is in the second LLM request's tool result.
        second = next((r for r in CompletionServer.requests
                       if any(m.get("role") == "tool" for m in r.get("messages") or [])), None)
        assert second is not None, "the tool result never reached the model"
        tool_text = json.dumps(second["messages"])
        assert re.search(r"\[watch watch_[0-9a-f]+", tool_text), "a2a_await did not return a watch_id"

        dump("summary.json", {
            "llm_calls": len(CompletionServer.requests),
            "peer_polls": sum(PeerServer.polls.values()),
            "ledger": row,
        })
        log("PASS: background watch published, delivered as a new turn, and acknowledged")
        return 0
    except AssertionError as exc:
        log(f"FAIL: {exc}")
        (EVIDENCE / "hermes-output.txt").write_text("\n".join(output))
        dump("llm-requests.json", CompletionServer.requests)
        dump("peer-seen.json", PeerServer.seen)
        return 1
    finally:
        try:
            os.write(master, b"/quit\n")
        except OSError:
            pass
        time.sleep(1.0)
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
        llm.shutdown()
        peer.shutdown()
        stop_stray_processes()


def stop_stray_processes() -> None:
    """Kill anything this run left behind that is bound to OUR HERMES_HOME (a chat autostarts a
    gateway with --idle-exit; a terminated client can leave it polling)."""
    home = str(HERMES_HOME).encode()
    mine = os.getpid()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == mine:
            continue
        try:
            environ = (entry / "environ").read_bytes()
        except OSError:
            continue
        if b"HERMES_HOME=" + home not in environ:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            log(f"stopped stray pid {pid}")
        except OSError:
            pass


def _delivered_row():
    db = HERMES_HOME / "state.db"
    if not db.exists():
        return None
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=2)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                "SELECT event_id, delivery_state, delivery_attempts, delivered_at "
                "FROM plugin_completions"
                " WHERE plugin_id='a2a-async' AND delivery_state='delivered'").fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return None
    return dict(row) if row else None


if __name__ == "__main__":
    sys.exit(main())
