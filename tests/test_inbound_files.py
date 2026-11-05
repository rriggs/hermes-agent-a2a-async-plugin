from __future__ import annotations

import base64
import asyncio
import io
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import stat
import threading
from types import SimpleNamespace
from typing import Any, cast
import urllib.request

import pytest

from a2a_async_plugin import protocol
from a2a_async_plugin import adapter as adapter_module
from a2a_async_plugin import tools


def test_raw_file_part_is_materialized_under_task_inbox(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"\x89PNG\r\n\x1a\nimage bytes"
    message = {
        "parts": [{
            "raw": base64.b64encode(payload).decode("ascii"),
            "filename": "page-003.png",
            "mediaType": "image/png",
        }],
    }

    rendered = protocol.extract_text(message, task_id="task-raw-1")

    file_line = next(line for line in rendered.splitlines() if "page-003.png" in line)
    path = Path(file_line.split("] ", 1)[1].split(" (", 1)[0])
    assert path.is_absolute()
    assert path.parent == inbox / "task-raw-1"
    assert path.read_bytes() == payload
    assert "bytes base64-encoded" not in rendered


def test_untrusted_filename_cannot_escape_task_inbox(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"attachment"
    message = {"parts": [{
        "raw": base64.b64encode(payload).decode("ascii"),
        "filename": "../../outside\x00.png",
        "mediaType": "image/png",
    }]}

    rendered = protocol.extract_text(message, task_id="../../escape")

    file_line = next(line for line in rendered.splitlines() if "image/png" in line)
    path = Path(file_line.split("] ", 1)[1].split(" (", 1)[0])
    assert path.resolve().is_relative_to(inbox.resolve())
    assert path.parent.name.startswith("______escape-")
    assert path.name == "outside_.png"
    assert path.read_bytes() == payload


def test_oversized_raw_file_is_rejected_in_agent_visible_text(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    monkeypatch.setenv("A2A_MAX_INBOUND_FILE_MB", "0.00001")
    message = {"parts": [{
        "raw": base64.b64encode(b"0123456789ABCDEF").decode("ascii"),
        "filename": "large.bin",
        "mediaType": "image/png",
    }]}

    rendered = protocol.extract_text(message, task_id="task-large")

    assert "[file: large.bin] rejected" in rendered
    assert "inbound file limit" in rendered
    assert not inbox.exists()


def test_configured_media_allowlist_can_enable_additional_types(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    monkeypatch.setenv("A2A_INBOUND_FILE_MEDIA_TYPES", "application/octet-stream")
    payload = b"opaque content"
    rendered = protocol.extract_text({"parts": [{
        "raw": base64.b64encode(payload).decode("ascii"),
        "filename": "payload.bin",
        "mediaType": "application/octet-stream",
    }]}, task_id="task-configured-type")

    assert "rejected" not in rendered
    assert (inbox / "task-configured-type" / "payload.bin").read_bytes() == payload


def test_existing_task_directory_is_restricted_before_saving(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    task_dir = inbox / "task-permissions"
    task_dir.mkdir(parents=True)
    task_dir.chmod(0o755)
    existing_file = task_dir / "private.png"
    existing_file.write_bytes(b"private")
    existing_file.chmod(0o644)
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"private"

    rendered = protocol.extract_text({"parts": [{
        "raw": base64.b64encode(payload).decode("ascii"),
        "filename": "private.png",
        "mediaType": "image/png",
    }]}, task_id="task-permissions")

    assert "rejected" not in rendered
    assert stat.S_IMODE(task_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE((task_dir / "private.png").stat().st_mode) == 0o600


def test_request_handler_accepts_body_over_one_mib_with_bounded_slots():
    class Security:
        def authenticate(self, *_args):
            return "peer"

        def is_trusted_peer(self, _identity):
            return True

    adapter = SimpleNamespace(
        _security_context=Security(),
        _body_read_slots=threading.BoundedSemaphore(2),
        _rate_limiter=SimpleNamespace(allow=lambda _identity: True),
        _route_for_request=lambda *_args: {"agent": {"name": "test"}},
    )
    body = json.dumps({
        "jsonrpc": "2.0",
        "id": "large-body",
        "method": "not/a/method",
        "params": {"message": {"parts": [{"raw": "A" * 1_500_000}]}},
    }).encode("utf-8")
    handler = object.__new__(adapter_module.A2ARequestHandler)
    handler.server = cast(Any, SimpleNamespace(adapter=adapter))
    handler.client_address = ("127.0.0.1", 1234)
    handler.headers = Message()
    handler.headers["Content-Length"] = str(len(body))
    handler.path = "/"
    handler.rfile = io.BytesIO(body)
    responses = []
    handler._json = lambda code, payload: responses.append((code, payload))

    handler.do_POST()

    assert responses[0][0] == 200
    assert responses[0][1]["error"]["code"] == protocol.ERR_METHOD_NOT_FOUND
    assert adapter._body_read_slots.acquire(blocking=False)
    adapter._body_read_slots.release()


def test_request_handler_rejects_negative_content_length_without_reading_body():
    class Security:
        def authenticate(self, *_args):
            return "peer"

    adapter = SimpleNamespace(
        _security_context=Security(),
        _body_read_slots=threading.BoundedSemaphore(2),
    )
    handler = object.__new__(adapter_module.A2ARequestHandler)
    handler.server = cast(Any, SimpleNamespace(adapter=adapter))
    handler.client_address = ("127.0.0.1", 1234)
    handler.headers = Message()
    handler.headers["Content-Length"] = "-1"
    handler.rfile = io.BytesIO(b"body must not be read")
    responses = []
    handler._json = lambda code, payload: responses.append((code, payload))

    handler.do_POST()

    assert responses[0][0] == 400
    assert handler.rfile.tell() == 0
    assert adapter._body_read_slots.acquire(blocking=False)
    adapter._body_read_slots.release()


def test_request_body_slot_is_released_before_task_subscription():
    slots = threading.BoundedSemaphore(1)
    slot_state = threading.local()
    slot_free_during_subscription = []
    responses = []

    class Security:
        def authenticate(self, *_args):
            return "peer"

        def is_trusted_peer(self, _identity):
            return True

    def release_body_slot():
        if getattr(slot_state, "held", False):
            slot_state.held = False
            slots.release()

    def subscribe(handler, request_id, _params, *, agent):
        acquired = slots.acquire(blocking=False)
        slot_free_during_subscription.append(acquired)
        if acquired:
            slots.release()
        handler._json(200, {"jsonrpc": "2.0", "id": request_id, "result": {"task": agent}})

    adapter = SimpleNamespace(
        _security_context=Security(),
        _body_read_slots=slots,
        _body_read_slot_state=slot_state,
        _release_body_read_slot=release_body_slot,
        _rate_limiter=SimpleNamespace(allow=lambda _identity: True),
        _route_for_request=lambda *_args: {"agent": {"name": "test"}},
        _rpc_tasks_subscribe=subscribe,
    )
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tasks/subscribe", "params": {}}).encode()
    handler = object.__new__(adapter_module.A2ARequestHandler)
    handler.server = cast(Any, SimpleNamespace(adapter=adapter))
    handler.client_address = ("127.0.0.1", 1234)
    handler.headers = cast(Any, {"Content-Length": str(len(body))})
    handler.rfile = io.BytesIO(body)
    handler.path = "/"
    handler._json = cast(Any, lambda code, payload: responses.append((code, payload)))

    handler.do_POST()

    assert slot_free_during_subscription == [True]
    assert responses[0][0] == 200
    assert slots.acquire(blocking=False)
    slots.release()


def test_peer_json_response_rejects_declared_oversized_body(monkeypatch):
    class Response:
        headers = {"Content-Length": str(32 * 1024 * 1024 + 1)}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, *_args):
            pytest.fail("oversized response body should not be read")

    monkeypatch.setattr(tools.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    with pytest.raises(ValueError, match="response body exceeds limit"):
        tools._http_post_json("http://peer.example.test/", {}, {}, 1)


def test_peer_json_response_limits_body_without_content_length(monkeypatch):
    monkeypatch.setattr(protocol, "MAX_JSON_BODY_BYTES", 3)

    class Response:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, limit):
            assert limit == 4
            return b"1234"

    monkeypatch.setattr(tools.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    with pytest.raises(ValueError, match="response body exceeds limit"):
        tools._http_get_json("http://peer.example.test/", {}, 1)


def test_text_and_data_parts_keep_their_existing_rendering(monkeypatch, tmp_path):
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "a2a_files"))
    message = {"parts": [
        {"text": "hello"},
        {"data": {"ok": True}, "mediaType": "application/json"},
    ]}

    assert protocol.extract_text(message, task_id="task-text") == (
        'hello\n[data (application/json)]\n{"ok": true}'
    )


def test_get_task_materializes_remote_file_artifact(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"remote image bytes"
    response = {"result": {
        "id": "remote-file-task",
        "status": {"state": protocol.STATE_COMPLETED},
        "artifacts": [{"parts": [{
            "raw": base64.b64encode(payload).decode("ascii"),
            "filename": "remote.png",
            "mediaType": "image/png",
        }]}],
    }}

    class Store:
        def complete(self, *_args):
            return True

    monkeypatch.setattr(tools, "_resolve_peer", lambda _agent: {"url": "http://peer.example.test"})
    monkeypatch.setattr(tools, "_rpc_request", lambda *_args, **_kwargs: response)
    monkeypatch.setattr(tools, "_task_store", lambda: Store())

    rendered = tools.a2a_get_task({"agent": "peer", "task_id": "remote-file-task"})

    path_text = next(line for line in rendered.splitlines() if "remote.png" in line)
    path = Path(path_text.rsplit("] ", 1)[-1].split(" (", 1)[0])
    assert path.parent == inbox / "remote-file-task"
    assert path.read_bytes() == payload


def test_watch_observation_materializes_remote_file_message(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"watch image bytes"
    observation = {
        "id": "watch-file-task",
        "history": [{"role": "ROLE_AGENT", "parts": [{
            "raw": base64.b64encode(payload).decode("ascii"),
            "filename": "watch.png",
            "mediaType": "image/png",
        }]}],
    }

    entries = tools._observation_messages(observation, protocol.STATE_COMPLETED)

    assert len(entries) == 1
    path_text = entries[0][2]
    path = Path(path_text.split("] ", 1)[1].split(" (", 1)[0])
    assert path.parent == inbox / "watch-file-task"
    assert path.read_bytes() == payload


def test_inbound_adapter_dispatches_file_path_to_agent(tmp_path, monkeypatch):
    from gateway.config import PlatformConfig

    home = tmp_path / "profile-home"
    home.mkdir()
    inbox = home / "a2a_files"
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_TASKS_DB", str(home / "a2a_tasks.db"))
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    monkeypatch.setenv("A2A_INBOUND_FILE_MEDIA_TYPES", "image/png, application/pdf")
    payload = b"image payload"
    params = {"message": {"role": "ROLE_USER", "contextId": "ctx-file-test", "parts": [{
        "raw": base64.b64encode(payload).decode("ascii"),
        "filename": "screen.png",
        "mediaType": "image/png",
    }]}}
    adapter = adapter_module.A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))
    loop = asyncio.new_event_loop()
    adapter._loop = loop
    setattr(adapter, "_message_handler", object())
    assert adapter._body_read_slots.acquire(blocking=False)
    adapter._body_read_slot_state.held = True
    captured = []

    async def capture(event):
        captured.append(event.text)

    def dispatch(coroutine, _loop):
        asyncio.run(coroutine)

    monkeypatch.setattr(adapter, "handle_message", capture)
    monkeypatch.setattr(adapter_module.asyncio, "run_coroutine_threadsafe", dispatch)

    assert adapter._build_card()["defaultInputModes"] == [
        "text/plain", "image/png", "application/pdf",
    ]

    terminal, pending = adapter._prepare_task(params, "peer")

    assert terminal is None
    assert pending is not None
    path_text = next(part for part in captured[0].splitlines() if "screen.png" in part)
    path = Path(path_text.split("] ", 1)[1].split(" (", 1)[0])
    assert path.parent == inbox / pending["task_id"]
    assert path.read_bytes() == payload
    assert "parts" not in params["message"]
    assert adapter._body_read_slots.acquire(blocking=False)
    adapter._body_read_slots.release()
    loop.close()


def test_safe_url_file_is_downloaded_and_url_is_preserved(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    url = "https://files.example.test/snapshot.png"
    payload = b"downloaded image"
    monkeypatch.setattr(protocol, "_resolve_public_file_address", lambda *_args: "93.184.216.34")

    class Response:
        headers = {"Content-Length": str(len(payload))}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _limit):
            return payload

    class Opener:
        def open(self, request, timeout):
            assert request.full_url == url
            assert timeout > 0
            return Response()

    monkeypatch.setattr(urllib.request, "build_opener", lambda *_handlers: Opener())
    message = {"parts": [{"url": url, "filename": "snapshot.png", "mediaType": "image/png"}]}

    rendered = protocol.extract_text(message, task_id="task-url")

    assert url in rendered
    assert "downloaded_to=" in rendered
    file_line = next(line for line in rendered.splitlines() if "downloaded_to=" in line)
    path = Path(file_line.split("downloaded_to=", 1)[1].split(" (", 1)[0])
    assert path.parent == inbox / "task-url"
    assert path.read_bytes() == payload


def test_unsafe_url_is_not_fetched_and_remains_visible(tmp_path, monkeypatch):
    url = "http://127.0.0.1/private.png"
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "a2a_files"))
    monkeypatch.setattr(urllib.request, "build_opener", lambda *_args: pytest.fail("unsafe URL fetched"))

    rendered = protocol.extract_text(
        {"parts": [{"url": url, "filename": "private.png", "mediaType": "image/png"}]},
        task_id="task-unsafe-url",
    )

    assert url in rendered
    assert "not downloaded" in rendered


def test_unknown_raw_media_type_is_rejected_without_writing_file(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    rendered = protocol.extract_text({"parts": [{
        "raw": base64.b64encode(b"opaque content").decode("ascii"),
        "filename": "payload.bin",
        "mediaType": "application/octet-stream",
    }]}, task_id="task-unknown-type")

    assert "application/octet-stream" in rendered
    assert "not allowed" in rendered
    assert not inbox.exists()


def test_profile_scoped_file_settings_do_not_use_default_profile_env(tmp_path, monkeypatch):
    from agent import secret_scope

    default_inbox = tmp_path / "default-profile-files"
    profile_inbox = tmp_path / "secondary-profile-files"
    monkeypatch.setenv("A2A_FILES_DIR", str(default_inbox))
    monkeypatch.setenv("A2A_MAX_INBOUND_FILE_MB", "20")
    monkeypatch.setenv("A2A_INBOUND_FILE_MEDIA_TYPES", "application/octet-stream")
    monkeypatch.setenv("A2A_INBOUND_FILE_TTL_DAYS", "1")
    monkeypatch.setattr(secret_scope, "is_multiplex_active", lambda: True)
    monkeypatch.setattr(secret_scope, "current_secret_scope", lambda: object())
    values = {
        "A2A_FILES_DIR": str(profile_inbox),
        "A2A_MAX_INBOUND_FILE_MB": "0.001",
        "A2A_INBOUND_FILE_MEDIA_TYPES": "application/pdf",
        "A2A_INBOUND_FILE_TTL_DAYS": "3",
    }
    monkeypatch.setattr(secret_scope, "get_secret", values.get)

    assert protocol._files_dir() == profile_inbox
    assert protocol._max_inbound_file_bytes() == int(float("0.001") * 1024 * 1024)
    policy = protocol.capture_inbound_file_policy()
    assert policy.media_types == ("application/pdf",)
    assert policy.ttl_seconds == 3 * 24 * 60 * 60


def test_explicit_empty_file_allowlist_rejects_all_media_types(monkeypatch):
    monkeypatch.setattr(protocol, "_allowed_inbound_file_media_types", lambda: ("image/png",))

    assert not protocol._inbound_media_type_allowed("image/png", ())


def test_dns_resolution_rejects_hosts_with_any_private_address(monkeypatch):
    def resolve(_host, _port, *, type):
        assert type == socket.SOCK_STREAM
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.8", 443)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)

    assert protocol._resolve_public_file_address("https://files.example.test/image.png") is None


def test_cleanup_removes_expired_terminal_files_but_keeps_active_tasks(tmp_path):
    inbox = tmp_path / "a2a_files"
    store = protocol.TaskStore()
    now = 10_000_000.0
    ttl = 3600
    for task_id in ("expired-task", "active-task"):
        task_dir = inbox / task_id
        task_dir.mkdir(parents=True)
        (task_dir / ".a2a-task-id").write_text(task_id, encoding="utf-8")
        (task_dir / "input.png").write_bytes(b"image")
        os.utime(task_dir, (now - ttl - 1, now - ttl - 1))
        store.create(task_id, f"ctx-{task_id}", "peer")
    store.complete("expired-task", protocol.STATE_COMPLETED)
    store._tasks["expired-task"]["completed_at"] = now - ttl - 1
    store.set_state("active-task", protocol.STATE_WORKING)

    removed = protocol.cleanup_inbound_files(inbox, store, now=now, ttl_seconds=ttl)

    assert removed == 1
    assert not (inbox / "expired-task").exists()
    assert (inbox / "active-task" / "input.png").exists()


def test_url_download_uses_pinned_resolution_and_limits_redirects(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"real local HTTP response"
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path == "/redirect.png":
                self.send_response(302)
                self.send_header("Location", "http://private.example.test/secret.png")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    public_host = "files.example.test"
    port = server.server_port
    monkeypatch.setattr(
        protocol,
        "_resolve_public_file_address",
        lambda url: "127.0.0.1" if public_host in url else None,
    )
    try:
        url = f"http://{public_host}:{port}/image.png"
        rendered = protocol.extract_text(
            {"parts": [{"url": url, "filename": "image.png", "mediaType": "image/png"}]},
            task_id="task-real-url",
        )
        assert "downloaded_to=" in rendered
        path = Path(rendered.split("downloaded_to=", 1)[1].split(" (", 1)[0])
        assert path.read_bytes() == payload

        redirect_url = f"http://{public_host}:{port}/redirect.png"
        redirected = protocol.extract_text(
            {"parts": [{"url": redirect_url, "filename": "redirect.png", "mediaType": "image/png"}]},
            task_id="task-redirect-url",
        )
        assert "not downloaded" in redirected
        assert requests == ["/image.png", "/redirect.png"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_peer_task_reply_uses_the_remote_task_inbox(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"task result image"
    task = {
        "id": "remote-task-13",
        "status": {"state": protocol.STATE_COMPLETED},
        "artifacts": [{"parts": [{
            "raw": base64.b64encode(payload).decode("ascii"),
            "filename": "result.png",
            "mediaType": "image/png",
        }]}],
    }

    rendered = tools._reply_text_from_result(task)

    file_line = next(line for line in rendered.splitlines() if "result.png" in line)
    path = Path(file_line.split("] ", 1)[1].split(" (", 1)[0])
    assert path.parent == inbox / "remote-task-13"
    assert path.read_bytes() == payload


def test_watch_observation_keeps_nontext_file_artifact(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    payload = b"backlog image"
    task = {
        "id": "remote-task-backlog",
        "artifacts": [{"artifactId": "artifact-1", "parts": [{
            "raw": base64.b64encode(payload).decode("ascii"),
            "filename": "backlog.png",
            "mediaType": "image/png",
        }]}],
    }

    entries = tools._observation_messages(task, protocol.STATE_COMPLETED)

    assert len(entries) == 1
    assert "backlog.png" in entries[0][2]
    file_line = next(line for line in entries[0][2].splitlines() if "backlog.png" in line)
    path = Path(file_line.split("] ", 1)[1].split(" (", 1)[0])
    assert path.parent == inbox / "remote-task-backlog"
    assert path.read_bytes() == payload


def test_get_task_returns_materialized_artifact_path(tmp_path, monkeypatch):
    inbox = tmp_path / "a2a_files"
    home = tmp_path / "profile-home"
    home.mkdir()
    monkeypatch.setenv("A2A_FILES_DIR", str(inbox))
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_TASKS_DB", str(home / "a2a_tasks.db"))
    payload = b"get task image"
    task = {
        "id": "remote-task-get",
        "contextId": "ctx-get",
        "status": {"state": protocol.STATE_COMPLETED},
        "artifacts": [{"parts": [{
            "raw": base64.b64encode(payload).decode("ascii"),
            "filename": "get.png",
            "mediaType": "image/png",
        }]}],
    }
    monkeypatch.setattr(tools, "_resolve_peer", lambda _agent: {"url": "http://peer"})
    monkeypatch.setattr(tools, "_rpc_request", lambda *_args, **_kwargs: {"result": task})

    rendered = tools.a2a_get_task({"agent": "peer", "task_id": "remote-task-get"})

    assert "get.png" in rendered
    assert str(inbox / "remote-task-get") in rendered
    assert (inbox / "remote-task-get" / "get.png").read_bytes() == payload
