#!/usr/bin/env python3
"""TUI/websocket end-to-end driver: the real gateway host, a real ws client, a real watch.

Unlike ``driver.py`` (classic CLI, which proves the refusal), this drives the production
TUI rail: a shared ``hermes gateway run`` owner, a WebSocket client that declares
``source=tui`` on ``session.create``, and the plugin's actual background delivery through
``PluginContext.inject_message``.

Flow:
  ws client -> session.create(source=tui)
            -> prompt.submit                (scripted model calls a2a_await(background=true))
            -> plugin asks the host async_delivery_visible()   (True: live GUI viewer)
            -> watch persisted, status line posted             (notification.show)
            -> scripted A2A peer: WORKING, then COMPLETED
            -> poller publishes through inject_message         (new user turn, same session)
            -> ws client sees message.complete #2, model saw "[A2A result · peer · task t1]"

Asserted:
  * the first tool result is a started watch, not a refusal;
  * the notification stream carries the plugin's live status line;
  * the injected turn reached the model with the label + peer artifact;
  * a2a_watches is settled and a2a_watch_publications has a delivered row.

Exit 0 = verified. Evidence lands in ``$E2E_DATA/evidence-tui``.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

DATA = Path(os.environ.get("E2E_DATA", "/data"))
EVIDENCE = DATA / "evidence-tui"
HOME = DATA / "home"
HERMES_HOME = HOME / ".hermes"
PLUGIN_SRC = Path(os.environ.get("E2E_PLUGIN", "/plugin"))
HERMES_SRC = Path(os.environ.get("E2E_HERMES_SRC", "/src"))

TURN_TIMEOUT_S = 120.0
INJECT_TIMEOUT_S = 90.0

llm_requests: list[dict] = []
llm_lock = threading.Lock()
peer_polls: dict[str, int] = {}
peer_seen: list[dict] = []
peer_lock = threading.Lock()
_EVENTS: "Events | None" = None


def log(message: str) -> None:
    print(f"[tui-e2e] {message}", flush=True)


def dump(name: str, payload: Any) -> None:
    (EVIDENCE / name).write_text(json.dumps(payload, indent=2, default=str))


# ── scripted model endpoint ─────────────────────────────────────────────────

class LLM(BaseHTTPRequestHandler):
    """OpenAI-compatible endpoint: one tool call, then plain replies."""

    def log_message(self, *_args):
        pass

    def _send_json(self, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._send_json({"object": "list", "data": [{"id": "fake-model", "object": "model"}]})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        request = json.loads(self.rfile.read(length) or b"{}")
        with llm_lock:
            index = len(llm_requests)
            llm_requests.append(request)
        dump(f"tui-llm-request-{index}.json", request)
        reply = self._reply_for(request)
        log(f"LLM call {index}: {reply['kind']}")
        if request.get("stream"):
            self._stream(reply)
        else:
            self._send_json(self._completion(reply))

    @staticmethod
    def _text_of(request: dict) -> str:
        return json.dumps(request.get("messages") or [], default=str, ensure_ascii=False)

    def _reply_for(self, request: dict) -> dict:
        if not request.get("messages"):
            return {"kind": "probe", "content": "ok"}
        # The gateway titles the session from the opening message (and again after a new turn);
        # answer that prompt as a title, never as a conversation turn.
        system = next((m for m in request["messages"] if m.get("role") == "system"), {})
        if "You name chat sessions" in str(system.get("content") or ""):
            return {"kind": "title", "content": "A2A background watch"}
        text = self._text_of(request)
        # The plugin's terminal content is what inject_message hands the host; the host runs it
        # as a user turn in the same session, so this marker proves the injected text arrived.
        if "[A2A result · peer · task t1]" in text:
            return {"kind": "completion-turn", "content": "COMPLETION_RECEIVED"}
        if "[watch watch_" in text:
            return {"kind": "watch-started", "content": "WATCH_STARTED"}
        if any(m.get("role") == "tool" for m in request.get("messages") or []):
            return {"kind": "after-tool", "content": "AFTER_TOOL"}
        return {
            "kind": "tool-call", "content": None,
            "tool_name": "a2a_await",
            "tool_args": json.dumps({"agent": "peer", "task_id": "t1", "background": True}),
        }

    def _completion(self, reply: dict) -> dict:
        message: dict = {"role": "assistant", "content": reply.get("content")}
        finish = "stop"
        if reply.get("tool_name"):
            message["content"] = None
            message["tool_calls"] = [{
                "id": "call_tui", "type": "function",
                "function": {"name": reply["tool_name"], "arguments": reply["tool_args"]},
            }]
            finish = "tool_calls"
        return {
            "id": "chatcmpl-tui", "object": "chat.completion", "created": int(time.time()),
            "model": "fake-model",
            "choices": [{"index": 0, "message": message, "finish_reason": finish}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    def _stream(self, reply: dict) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        base = {"id": "chatcmpl-tui", "object": "chat.completion.chunk",
                "created": int(time.time()), "model": "fake-model"}

        def emit(delta: dict, finish=None) -> None:
            chunk = dict(base, choices=[{"index": 0, "delta": delta, "finish_reason": finish}])
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.flush()

        if reply.get("tool_name"):
            emit({"role": "assistant", "content": None})
            emit({"tool_calls": [{
                "index": 0, "id": "call_tui", "type": "function",
                "function": {"name": reply["tool_name"], "arguments": reply["tool_args"]},
            }]})
            emit({}, "tool_calls")
        else:
            emit({"role": "assistant", "content": reply.get("content") or ""})
            emit({}, "stop")
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


class Peer(BaseHTTPRequestHandler):
    """A2A JSON-RPC peer: WORKING on the first GetTask, COMPLETED after that."""

    def log_message(self, *_args):
        pass

    def do_GET(self):
        body = b'{"name":"tui-e2e-peer","protocolVersion":"1.0"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        with peer_lock:
            peer_seen.append(request)
        task_id = str((request.get("params") or {}).get("id") or "t1")
        with peer_lock:
            count = peer_polls.get(task_id, 0)
            peer_polls[task_id] = count + 1
        state = "TASK_STATE_WORKING" if count == 0 else "TASK_STATE_COMPLETED"
        result = {"id": task_id, "contextId": "ctx-tui", "status": {"state": state}}
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


# ── home / plugin ───────────────────────────────────────────────────────────

def write_home(llm_port: int, peer_port: int) -> None:
    shutil.rmtree(HERMES_HOME, ignore_errors=True)
    (HERMES_HOME).mkdir(parents=True)
    config = f"""model:
  provider: custom
  base_url: "http://127.0.0.1:{llm_port}"
  default: fake-model
  context_length: 128000
agent:
  api_max_retries: 0
  auto_recovery_cycles: 0
  max_turns: 8
providers:
  custom:
    request_timeout_seconds: 60
    stale_timeout_seconds: 60
compression:
  enabled: false
memory:
  memory_enabled: false
  user_profile_enabled: false
updates:
  check: false
plugins:
  enabled:
    - a2a-async
  entries:
    a2a-async:
      # Plugin-triggered turns into a live gateway session are consent-gated per plugin.
      allow_gateway_injection: true
tools:
  tool_search:
    enabled: "off"
platform_toolsets:
  cli:
    - hermes-cli
    - a2a
  local:
    - hermes-cli
    - a2a
a2a_agents:
  peer:
    url: "http://127.0.0.1:{peer_port}"
    timeout: 10
display:
  background_process_notifications: all
"""
    (HERMES_HOME / "config.yaml").write_text(config)
    (HERMES_HOME / ".env").write_text("OPENAI_API_KEY=e2e-placeholder\n")
    target = HERMES_HOME / "plugins" / "a2a-async"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        PLUGIN_SRC, target,
        ignore=shutil.ignore_patterns("__pycache__", ".git", ".pytest_cache", "tests", "docs",
                                      ".hermes", "a2a_async"),
    )


# ── event pump ──────────────────────────────────────────────────────────────

class Events:
    """Collects every gateway event frame and lets callers wait for predicates."""

    def __init__(self) -> None:
        self.items: list[dict] = []
        self._task: asyncio.Task | None = None

    def start(self, client) -> None:
        self._task = asyncio.create_task(self._pump(client), name="tui-e2e-events")

    async def _pump(self, client) -> None:
        while True:
            frame = await client.events.get()
            self.items.append(frame)

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def wait(self, predicate, timeout: float, *, label: str) -> dict:
        deadline = time.monotonic() + timeout
        seen = 0
        while True:
            while seen < len(self.items):
                frame = self.items[seen]
                seen += 1
                if predicate(frame):
                    return frame
            left = deadline - time.monotonic()
            if left <= 0:
                types = sorted({Events.type_of(f) for f in self.items})
                raise AssertionError(f"timeout waiting for {label} (events: {types})")
            await asyncio.sleep(min(0.1, left))

    @staticmethod
    def _params(frame: dict) -> dict:
        return frame.get("params") or {}

    @classmethod
    def type_of(cls, frame: dict) -> str:
        return str(cls._params(frame).get("type") or "")

    @classmethod
    def sid_of(cls, frame: dict) -> str:
        return str(cls._params(frame).get("session_id") or "")

    @classmethod
    def payload_of(cls, frame: dict) -> dict:
        return cls._params(frame).get("payload") or {}


def _is_message_complete(frame: dict, sid: str) -> bool:
    return Events.type_of(frame) == "message.complete" and Events.sid_of(frame) == sid


def _db_rows() -> tuple[list[dict], list[dict]]:
    db = HERMES_HOME / "a2a_tasks.db"
    if not db.exists():
        return [], []
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
    try:
        con.row_factory = sqlite3.Row
        watches = [dict(r) for r in con.execute("SELECT * FROM a2a_watches")]
        pubs = [dict(r) for r in con.execute("SELECT * FROM a2a_watch_publications")]
    finally:
        con.close()
    return watches, pubs


# ── main ────────────────────────────────────────────────────────────────────

def stop_stray_processes() -> None:
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
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        # Only ever stop a Hermes/gateway process: the container's own entrypoint may carry the
        # launch env, and a bare env match once flagged pid 5 (no cmdline) as stray.
        if "hermes" not in cmdline and "gateway" not in cmdline:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            log(f"stopped stray pid {pid}: {cmdline[:120]}")
        except OSError:
            pass


async def run() -> int:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    stop_stray_processes()

    llm = serve(LLM)
    peer = serve(Peer)
    llm_port, peer_port = llm.server_address[1], peer.server_address[1]
    write_home(llm_port, peer_port)
    log(f"LLM :{llm_port} peer :{peer_port} home {HERMES_HOME}")

    # The gateway child inherits this environment; A2A_WATCH_INTERVAL keeps the poll fast.
    os.environ.update({
        "HERMES_HOME": str(HERMES_HOME),
        "HOME": str(HOME),
        "A2A_TASKS_DB": str(HERMES_HOME / "a2a_tasks.db"),
        "A2A_WATCH_INTERVAL": "2",
        "PYTHONPATH": str(HERMES_SRC),
        "PYTHONUNBUFFERED": "1",
        "NO_COLOR": "1",
    })

    from hermes_cli.gateway_client import connect_gateway

    events = Events()
    global _EVENTS
    _EVENTS = events
    try:
        async with connect_gateway() as client:
            events.start(client)
            await events.wait(lambda f: Events.type_of(f) == "gateway.ready",
                              timeout=60, label="gateway.ready")
            created = await client.rpc("session.create", source="tui", request_id="e2e-tui-1")
            sid = created["session_id"]
            log(f"session {sid} (stored {created.get('stored_session_id')})")

            submitted = await client.rpc(
                "prompt.submit", session_id=sid, submission_id="e2e-turn-1",
                text="Start a background A2A watch on peer task t1, then wait.")
            assert submitted.get("status") in {"started", "queued"}, submitted

            first = await events.wait(lambda f: _is_message_complete(f, sid),
                                      timeout=TURN_TIMEOUT_S, label="first message.complete")
            first_payload = Events.payload_of(first)
            assert first_payload.get("status") == "complete", first_payload

            tool_request = next((r for r in llm_requests
                                 if any(m.get("role") == "tool" for m in r.get("messages") or [])), None)
            assert tool_request is not None, "the model never received a tool result"
            tool_text = json.dumps(tool_request.get("messages") or [])
            assert "[watch watch_" in tool_text, (
                "a2a_await(background=true) did not start a watch on the TUI rail: "
                + tool_text[-1500:])

            # The watch settles through inject_message and starts a second turn in this session.
            # Wait on the model request (the delivery proof), not on the next event: the gateway
            # emits several lifecycle events per turn and the watch poll needs its interval.
            LABEL = "[A2A result · peer · task t1]"
            deadline = time.monotonic() + INJECT_TIMEOUT_S
            injected = None
            while time.monotonic() < deadline:
                injected = next((r for r in llm_requests
                                 if LABEL in json.dumps(r.get("messages") or [], ensure_ascii=False)), None)
                if injected is not None:
                    break
                await asyncio.sleep(0.1)
            assert injected is not None, (
                "the injected result never reached the model; watch/publications: "
                + json.dumps(_db_rows(), default=str)[:1200])
            injected_text = json.dumps(injected.get("messages") or [], ensure_ascii=False)
            assert "peer finished t1" in injected_text, "the peer artifact text was not delivered"
            assert "untrusted" in injected_text.lower(), (
                "the injected peer text lost its untrusted-input boundary")

            second = await events.wait(
                lambda f: _is_message_complete(f, sid)
                and "COMPLETION_RECEIVED" in str(Events.payload_of(f).get("text") or ""),
                timeout=TURN_TIMEOUT_S, label="injected turn's message.complete")
            assert Events.payload_of(second).get("status") == "complete", second

            notifications = [Events.payload_of(f) for f in events.items
                             if Events.type_of(f) == "notification.show"]
            dump("tui-notifications.json", notifications)
            status_lines = [str(n.get("text") or "") for n in notifications
                            if "a2a" in str(n.get("text") or "").lower()]
            assert status_lines, (
                "the live status line never reached the TUI notification stream: "
                + json.dumps(notifications)[:1200])

            watches, pubs = _db_rows()
            dump("tui-db.json", {"watches": watches, "publications": pubs})
            assert len(watches) == 1, watches
            assert pubs and pubs[0].get("published_at"), "the outbox row is not delivered"

            seen = sorted({str(e.get("method")) for e in peer_seen})
            assert sum(peer_polls.values()) >= 2, peer_polls
            assert not any(s == "CancelTask" for s in seen), f"unexpected peer calls: {seen}"

            summary = {
                "delivered": True,
                "session_id": sid,
                "llm_calls": len(llm_requests),
                "peer_polls": sum(peer_polls.values()),
                "watch_state": watches[0].get("state"),
                "publication_published_at": pubs[0].get("published_at"),
                "status_lines": status_lines,
                "notification_types": [Events.type_of(f) for f in events.items
                                       if Events.type_of(f) == "notification.show"][:10],
            }
            dump("summary.json", summary)
            log("PASS: background A2A result delivered into the TUI session")
            print(json.dumps(summary, indent=2))
            return 0
    finally:
        llm.shutdown()
        peer.shutdown()
        stop_stray_processes()


def main() -> int:
    try:
        return asyncio.run(run())
    except AssertionError as exc:
        log(f"FAIL: {exc}")
        if _EVENTS is not None:
            dump("tui-events.json", [_EVENTS._params(f) if isinstance(f, dict) else repr(f)
                                    for f in _EVENTS.items])
        dump("tui-llm-requests.json", llm_requests)
        dump("tui-peer-seen.json", peer_seen)
        return 1


if __name__ == "__main__":
    sys.exit(main())
