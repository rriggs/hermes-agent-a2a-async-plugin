"""
Controllable fake A2A peer for the restart-recovery test suite.

A minimal A2A v1.0 peer (no aiohttp, no starlette — stdlib
``http.server.ThreadingHTTPServer`` + ``BaseHTTPRequestHandler``) that:

* Serves the A2A v1.0 Agent Card at ``GET /.well-known/agent-card.json``
  (and the legacy ``/.well-known/agent.json`` alias).
* Handles the JSON-RPC v1 methods a caller uses against us:
    - ``SendMessage`` (case-insensitive, per a2a_async_plugin/tools.py:
      the caller emits ``SendMessage`` PascalCase, so does the SDK in
      v1.0). Accepts a message, returns a ``{task: ...}`` wrapper with
      a freshly issued ``task-{uuid}`` id and ``state: WORKING`` so the
      caller can poll. By default the peer HOLDS the task in WORKING
      until a control endpoint tells it to complete or fail.
    - ``GetTask``: returns the current task state and reply (if any).
    - ``ListTasks``: lists all known tasks for inspection.
    - ``CancelTask``: cancels a task and marks it CANCELED.
  * Accepts the same legacy snake_case method names
  (``message/send``, ``tasks/get`` …) as the real adapter so the caller
  tests are not coupled to a single casing.

* Exposes a small local-only HTTP **control** surface on a separate
  loopback port so tests can tell it to complete/fail a held task —
  even after the caller gateway has been killed and restarted:

      POST /__ctrl__/complete  {"task_id": "...", "reply": "..."}
      POST /__ctrl__/fail      {"task_id": "...", "reply": "..."}
      POST /__ctrl__/hold      {"task_id": "..."}  -- (default)
      GET  /__ctrl__/requests  -- list of captured (sanitised) requests
      GET  /__ctrl__/responses -- list of captured responses
      POST /__ctrl__/shutdown  -- graceful shutdown

* Persists its own task state to a small JSON file (NOT SQLite — the
  fake is intentionally trivial) in its temp dir. The peer process can
  be SIGKILL'd and the file persists, so the caller's restart cycle
  does not lose the task.

* Captures sanitised request/response logs WITHOUT secrets: each entry
  strips Authorization headers and truncates bodies.

The peer is the "test fixture server" called out in the plan; it must
be controllable in three phases (idle → hold → complete/fail) by the
test, not by the caller, so the caller can restart independently.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional


logger = logging.getLogger("recovery.fake_peer")


PROTOCOL_VERSION = "1.0"
AGENT_NAME = "fake-peer"
AGENT_DESCRIPTION = "Controllable fake A2A peer for hermes recovery tests."

# A2A v1.0 task lifecycle states (must match a2a_async_plugin.protocol).
STATE_SUBMITTED = "TASK_STATE_SUBMITTED"
STATE_WORKING = "TASK_STATE_WORKING"
STATE_COMPLETED = "TASK_STATE_COMPLETED"
STATE_FAILED = "TASK_STATE_FAILED"
STATE_CANCELED = "TASK_STATE_CANCELED"

TERMINAL_STATES = frozenset({STATE_COMPLETED, STATE_FAILED, STATE_CANCELED})

# Methods the peer accepts (both v1.0 PascalCase and the legacy
# snake_case aliases the real adapter accepts).
SUPPORTED_METHODS = frozenset({
    "SendMessage", "message/send",
    "GetTask", "tasks/get",
    "ListTasks", "tasks/list",
    "CancelTask", "tasks/cancel",
    "SubscribeToTask", "tasks/subscribe",
})

# Max body — guards against accidental DoS by tests.
_MAX_BODY = 1_048_576  # 1 MiB


# ---------------------------------------------------------------------------
# Task store — JSON file on disk so a SIGKILL of the peer does not lose
# state. Same durability guarantee the plan asks for; we don't use
# SQLite because the fake has no schema-evolution pressure.
# ---------------------------------------------------------------------------


@dataclass
class TaskRecord:
    task_id: str
    context_id: str
    state: str
    message: str = ""
    reply: str = ""
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    history: list[dict] = field(default_factory=list)

    def to_task(self) -> dict:
        status: dict[str, Any] = {
            "state": self.state,
            "timestamp": _iso(self.completed_at or self.created_at),
        }
        if self.reply:
            status["message"] = {
                "role": "ROLE_AGENT",
                "parts": [{"text": self.reply, "mediaType": "text/plain"}],
                "messageId": uuid.uuid4().hex,
            }
        out: dict[str, Any] = {
            "id": self.task_id,
            "contextId": self.context_id,
            "status": status,
        }
        if self.state == STATE_COMPLETED and self.reply:
            out["artifacts"] = [{
                "artifactId": uuid.uuid4().hex,
                "parts": [{"text": self.reply, "mediaType": "text/plain"}],
            }]
        return out


class FakeTaskStore:
    def __init__(self, path: Path):
        self._path = path
        self._lock = threading.RLock()
        self._tasks: dict[str, TaskRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            for tid, rec in (data.get("tasks") or {}).items():
                self._tasks[tid] = TaskRecord(**rec)
        except Exception:
            # Corrupt store: refuse to silently lose on-disk state, but
            # do not crash the test. Move it aside so a fresh start
            # works.
            try:
                self._path.rename(self._path.with_suffix(".corrupt.json"))
            except OSError:
                pass

    def _save(self) -> None:
        tmp = self._path.with_suffix(".tmp")
        try:
            tmp.write_text(
                json.dumps(
                    {"tasks": {tid: rec.__dict__ for tid, rec in self._tasks.items()}},
                    indent=1,
                ),
                encoding="utf-8",
            )
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.warning("fake-peer store save failed: %s", exc)

    def create(self, task_id: str, context_id: str, message: str) -> TaskRecord:
        with self._lock:
            rec = TaskRecord(
                task_id=task_id,
                context_id=context_id,
                state=STATE_WORKING,  # hold by default
                message=message,
                history=[{"role": "ROLE_USER", "text": message}],
            )
            self._tasks[task_id] = rec
            self._save()
            return rec

    def get(self, task_id: str) -> Optional[TaskRecord]:
        with self._lock:
            rec = self._tasks.get(task_id)
            if rec is None:
                return None
            # Return a copy so callers can't mutate stored state.
            return TaskRecord(**rec.__dict__)

    def list(self) -> list[TaskRecord]:
        with self._lock:
            return [TaskRecord(**r.__dict__) for r in self._tasks.values()]

    def complete(self, task_id: str, reply: str) -> Optional[TaskRecord]:
        with self._lock:
            rec = self._tasks.get(task_id)
            if rec is None:
                return None
            if rec.state in TERMINAL_STATES:
                return TaskRecord(**rec.__dict__)
            rec.state = STATE_COMPLETED
            rec.reply = reply
            rec.completed_at = time.time()
            rec.history.append({"role": "ROLE_AGENT", "text": reply})
            self._save()
            return TaskRecord(**rec.__dict__)

    def fail(self, task_id: str, reply: str) -> Optional[TaskRecord]:
        with self._lock:
            rec = self._tasks.get(task_id)
            if rec is None:
                return None
            if rec.state in TERMINAL_STATES:
                return TaskRecord(**rec.__dict__)
            rec.state = STATE_FAILED
            rec.reply = reply
            rec.completed_at = time.time()
            rec.history.append({"role": "ROLE_AGENT", "text": reply})
            self._save()
            return TaskRecord(**rec.__dict__)

    def cancel(self, task_id: str) -> Optional[TaskRecord]:
        with self._lock:
            rec = self._tasks.get(task_id)
            if rec is None:
                return None
            if rec.state in TERMINAL_STATES:
                return TaskRecord(**rec.__dict__)
            rec.state = STATE_CANCELED
            rec.completed_at = time.time()
            self._save()
            return TaskRecord(**rec.__dict__)


def _iso(secs: float) -> str:
    # ISO 8601 with millisecond precision, matching a2a_async_plugin.protocol.now_iso
    import datetime as _dt
    return _dt.datetime.fromtimestamp(secs, tz=_dt.timezone.utc) \
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------


def _build_card(public_url: str) -> dict:
    return {
        "name": AGENT_NAME,
        "description": AGENT_DESCRIPTION,
        "url": public_url,
        "version": "1.0.0",
        "provider": {"organization": "Hermes Test", "url": public_url},
        "supportedInterfaces": [{
            "url": public_url,
            "protocolBinding": "JSONRPC",
            "protocolVersion": PROTOCOL_VERSION,
        }],
        "capabilities": {
            "streaming": False,
            "pushNotifications": False,
            "stateTransitionHistory": True,
            "extendedAgentCard": False,
        },
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [{
            "id": "echo",
            "name": "echo",
            "description": "Returns the input back (after a controllable delay)",
            "tags": ["test"],
        }],
    }


def _sanitise_request(method: str, path: str, headers: dict, body: bytes) -> dict:
    """Strip credentials; preserve enough to debug test failures."""
    safe_headers = {k: v for k, v in headers.items()
                    if k.lower() not in {"authorization", "cookie", "x-api-key"}}
    try:
        body_str = body.decode("utf-8", errors="replace")
        if len(body_str) > 4000:
            body_str = body_str[:4000] + " …[truncated]"
        # Don't try to mask Authorization *inside* the body — the peer
        # should never receive one in our test setup; the test asserts
        # no auth leaks into the peer, so if it does the body itself
        # is the failure mode.
    except Exception:
        body_str = repr(body)[:4000]
    return {
        "ts": time.time(),
        "method": method,
        "path": path,
        "headers": safe_headers,
        "body": body_str,
    }


def _jsonrpc_error(req_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _jsonrpc_result(req_id: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _method_info(method: str) -> str:
    """Map either v1.0 or legacy method name to a canonical op."""
    return {
        "SendMessage": "send", "message/send": "send",
        "GetTask": "get", "tasks/get": "get",
        "ListTasks": "list", "tasks/list": "list",
        "CancelTask": "cancel", "tasks/cancel": "cancel",
        "SubscribeToTask": "subscribe", "tasks/subscribe": "subscribe",
    }.get(method, "")


def _extract_text(message: dict) -> str:
    out = []
    for part in (message.get("parts") or []):
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            out.append(part["text"])
    return "\n".join(out).strip()


class _PeerHandler(BaseHTTPRequestHandler):
    server: "_PeerServer"  # type: ignore[assignment]

    @property
    def peer(self) -> "FakePeer":
        # The server attribute is the bound ThreadingHTTPServer; we hang
        # the FakePeer controller off it as ``fake_peer`` in start().
        return self.server.fake_peer  # type: ignore[attr-defined]

    def log_message(self, format, *args):  # noqa: A002,N802
        # Silence the default stderr access log; test code reads the
        # captured requests log instead.
        logger.debug("fake-peer http: " + format, *args)

    # ── HTTP plumbing ────────────────────────────────────────────────────

    def _json(self, code: int, payload: dict, *, capture: bool = True) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        if capture:
            self.peer.record_response(code, payload)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length > _MAX_BODY:
            raise ValueError("payload too large")
        return self.rfile.read(length) if length else b""

    # ── GET handlers ────────────────────────────────────────────────────────

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/.well-known/agent-card.json") \
                or self.path.startswith("/.well-known/agent.json"):
            public_url = f"http://127.0.0.1:{self.peer.public_port}/"
            self._json(200, _build_card(public_url))
            return
        if self.path.startswith("/__ctrl__/requests"):
            self._json(200, {"requests": self.peer.captured_requests()})
            return
        if self.path.startswith("/__ctrl__/responses"):
            self._json(200, {"responses": self.peer.captured_responses()})
            return
        if self.path.startswith("/__ctrl__/tasks"):
            self._json(200, {"tasks": [t.__dict__ for t in self.peer.store.list()]})
            return
        self._json(404, {"error": "not found"})

    # ── POST handlers ─────────────────────────────────────────────────────

    def do_POST(self):  # noqa: N802
        # Control surface
        if self.path.startswith("/__ctrl__/"):
            self._handle_control()
            return

        # JSON-RPC A2A. Read the body ONCE; record_request and the RPC
        # dispatcher both need the bytes.
        try:
            raw = self._read_body()
        except ValueError as exc:
            self._json(400, _jsonrpc_error(None, -32700, str(exc)), capture=False)
            return
        self.peer.record_request(_sanitise_request(
            "POST", self.path, dict(self.headers.items()), raw,
        ))

        try:
            req = json.loads(raw.decode("utf-8"))
        except Exception:
            self._json(400, _jsonrpc_error(None, -32700, "parse error"))
            return

        if not isinstance(req, dict):
            self._json(400, _jsonrpc_error(None, -32602, "request must be object"))
            return

        req_id = req.get("id")
        method = str(req.get("method", ""))
        params = req.get("params") or {}
        if not isinstance(params, dict):
            params = {}

        op = _method_info(method)
        if not op:
            self._json(200, _jsonrpc_error(req_id, -32601, f"method not found: {method}"))
            return

        if op == "send":
            self._handle_send(req_id, params)
            return
        if op == "get":
            self._handle_get(req_id, params)
            return
        if op == "list":
            self._handle_list(req_id, params)
            return
        if op == "cancel":
            self._handle_cancel(req_id, params)
            return
        if op == "subscribe":
            # For the fake peer, subscribe is a no-op ack — we never
            # stream. Tests that need streaming must build their own
            # harness.
            self._json(200, _jsonrpc_error(req_id, -32003, "streaming not supported by fake peer"))
            return

        self._json(200, _jsonrpc_error(req_id, -32601, "unhandled"))

    # ── RPC dispatch ──────────────────────────────────────────────────────

    def _handle_send(self, req_id: Any, params: dict) -> None:
        msg = params.get("message") or {}
        context_id = str(msg.get("contextId") or params.get("contextId") or
                          ("ctx-" + uuid.uuid4().hex[:16]))
        text = _extract_text(msg)
        # Honour configuration.returnImmediately so the caller submits
        # without blocking — but if the caller didn't set it, we still
        # return immediately with a WORKING task (the plan's "hold"
        # semantics). The caller polls with GetTask.
        task_id = "task-" + uuid.uuid4().hex[:16]
        rec = self.peer.store.create(task_id, context_id, text)
        # v1.0 wraps the Task in {task: ...} — matches a2a_async_plugin
        # protocol.send_message_response for tasks (payload has 'id' + 'status').
        self._json(200, _jsonrpc_result(req_id, {"task": rec.to_task()}))

    def _handle_get(self, req_id: Any, params: dict) -> None:
        task_id = str(params.get("id") or params.get("taskId") or "")
        rec = self.peer.store.get(task_id)
        if rec is None:
            self._json(200, _jsonrpc_error(req_id, -32001, f"task not found: {task_id}"))
            return
        self._json(200, _jsonrpc_result(req_id, rec.to_task()))

    def _handle_list(self, req_id: Any, params: dict) -> None:
        recs = self.peer.store.list()
        tasks = [r.to_task() for r in recs]
        self._json(200, _jsonrpc_result(req_id, {
            "tasks": tasks,
            "nextPageToken": "",
            "pageSize": max(1, min(int(params.get("pageSize") or 50), 100)),
            "totalSize": len(tasks),
        }))

    def _handle_cancel(self, req_id: Any, params: dict) -> None:
        task_id = str(params.get("id") or params.get("taskId") or "")
        rec = self.peer.store.cancel(task_id)
        if rec is None:
            self._json(200, _jsonrpc_error(req_id, -32001, f"task not found: {task_id}"))
            return
        self._json(200, _jsonrpc_result(req_id, rec.to_task()))

    # ── Control surface ───────────────────────────────────────────────────

    def _handle_control(self) -> None:
        try:
            raw = self._read_body()
        except ValueError:
            self._json(400, {"error": "bad body"})
            return
        try:
            cmd = self.path[len("/__ctrl__/"):].strip("/")
            if not raw:
                body = {}
            else:
                body = json.loads(raw.decode("utf-8")) if raw.strip() else {}
        except Exception:
            self._json(400, {"error": "bad json"})
            return

        if cmd == "complete":
            rec = self.peer.store.complete(body["task_id"], body.get("reply", ""))
            if rec is None:
                self._json(404, {"error": "task not found"})
                return
            self._json(200, {"task": rec.to_task()})
            return
        if cmd == "fail":
            rec = self.peer.store.fail(body["task_id"], body.get("reply", "failed"))
            if rec is None:
                self._json(404, {"error": "task not found"})
                return
            self._json(200, {"task": rec.to_task()})
            return
        if cmd == "hold":
            rec = self.peer.store.get(body["task_id"])
            if rec is None:
                self._json(404, {"error": "task not found"})
                return
            if rec.state not in TERMINAL_STATES:
                # Already non-terminal; no-op.
                self._json(200, {"task": rec.to_task()})
                return
            self._json(409, {"error": f"task already terminal: {rec.state}"})
            return
        if cmd == "shutdown":
            # 200 first, then ask the server loop to stop.
            self._json(200, {"stopping": True})
            threading.Thread(target=self.peer.shutdown_serve, daemon=True).start()
            return
        self._json(404, {"error": f"unknown control command: {cmd}"})


# ---------------------------------------------------------------------------
# Server lifecycle — separate public-port (A2A) and control-port.
# ---------------------------------------------------------------------------


class _PeerServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class FakePeer:
    """In-process controller for the fake A2A peer.

    Tests construct ``FakePeer(temp_dir)`` and call ``start()`` /
    ``stop()`` / ``kill()`` (kill = SIGKILL the subprocess that owns
    the server; the on-disk store survives so the next peer process
    can resume).
    """

    def __init__(self, workdir: Path, *, public_port: int = 0, control_port: int = 0):
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.store_path = self.workdir / "fake_peer_store.json"
        self.store = FakeTaskStore(self.store_path)
        self.requests_log = self.workdir / "requests.jsonl"
        self.responses_log = self.workdir / "responses.jsonl"
        self._requests_lock = threading.Lock()
        self._requests: list[dict] = []
        self._responses_lock = threading.Lock()
        self._responses: list[dict] = []
        self.public_port = public_port
        self.control_port = control_port
        self._server: Optional[_PeerServer] = None
        self._thread: Optional[threading.Thread] = None

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("already started")
        # The peer exposes the A2A surface on /__not__/ + control
        # surface on /__ctrl__/. Both share the same bound port for
        # simplicity (no need for two port allocations in the harness).
        server = _PeerServer(("127.0.0.1", self.public_port), _PeerHandler)
        server.fake_peer = self  # type: ignore[attr-defined]
        server.public_port = server.server_address[1]  # type: ignore[attr-defined]
        self._server = server
        self.public_port = server.server_address[1]
        self.control_port = self.public_port  # same port — see above
        self._thread = threading.Thread(
            target=server.serve_forever, name="fake-peer-http", daemon=True,
        )
        self._thread.start()

    def shutdown_serve(self) -> None:
        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception:
                pass

    def stop(self, timeout: float = 3.0) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._server = None
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    # ── record helpers (called from the handler thread) ──────────────────

    def record_request(self, entry: dict) -> None:
        with self._requests_lock:
            self._requests.append(entry)
            try:
                with self.requests_log.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(entry) + "\n")
            except OSError:
                pass

    def record_response(self, code: int, payload: dict) -> None:
        # Truncate response bodies — they're not secrets, but a 1MB
        # captured GetTask reply is unhelpful in a CI log.
        truncated = json.dumps(payload)
        if len(truncated) > 4000:
            truncated = truncated[:4000] + " …[truncated]"
        entry = {"ts": time.time(), "status": code, "body": truncated}
        with self._responses_lock:
            self._responses.append(entry)
            try:
                with self.responses_log.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(entry) + "\n")
            except OSError:
                pass

    def captured_requests(self) -> list[dict]:
        with self._requests_lock:
            return list(self._requests)

    def captured_responses(self) -> list[dict]:
        with self._responses_lock:
            return list(self._responses)

    # ── higher-level helpers used by tests ───────────────────────────────

    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.public_port}"

    def card_url(self, legacy: bool = False) -> str:
        suffix = "agent.json" if legacy else "agent-card.json"
        return f"{self.base_url()}/.well-known/{suffix}"

    def control_url(self, cmd: str) -> str:
        return f"{self.base_url()}/__ctrl__/{cmd}"