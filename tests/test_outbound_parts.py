from __future__ import annotations

import base64
import asyncio
from concurrent.futures import Future
import hashlib
import io
import json
import threading
import time

import pytest

from a2a_async_plugin import protocol


def test_completed_task_exposes_same_data_parts_in_message_and_artifact():
    parts = [protocol.data_part({"answer": 42, "ok": True})]

    task = protocol.build_task(
        "task-json", "ctx-json", protocol.STATE_COMPLETED, "",
        parts=parts,
    )

    assert task["status"]["message"]["parts"] == parts
    assert task["artifacts"][0]["parts"] == parts
    assert '"answer": 42' in protocol.extract_text(task["status"]["message"])


def test_extract_text_renders_null_data_part():
    assert protocol.extract_text({"parts": [protocol.data_part(None)]}) == "[data (application/json)]\nnull"


def test_text_only_task_shape_is_unchanged():
    task = protocol.build_task(
        "task-text", "ctx-text", protocol.STATE_COMPLETED, "plain reply",
    )

    assert task["status"]["message"]["parts"] == [protocol.text_part("plain reply")]
    assert task["artifacts"][0]["parts"] == [protocol.text_part("plain reply")]


def test_artifact_update_accepts_multiple_parts():
    parts = [protocol.text_part("image result"), protocol.file_part(raw="aGVsbG8=", filename="x.txt", media_type="text/plain")]

    update = protocol.artifact_update("task-file", "ctx-file", parts=parts)

    assert update["artifactUpdate"]["artifact"]["parts"] == parts


def test_agent_card_advertises_json_by_default_and_keeps_text_first():
    card = protocol.build_agent_card(
        name="agent", url="http://127.0.0.1:1/", description="test",
    )

    assert card["defaultOutputModes"] == ["text/plain", "application/json"]


def test_agent_card_normalizes_configured_output_modes():
    card = protocol.build_agent_card(
        name="agent", url="http://127.0.0.1:1/", description="test",
        output_modes=["image/png", "text/plain", "image/png"],
    )

    assert card["defaultOutputModes"] == ["text/plain", "image/png"]


def test_toolset_skill_can_advertise_an_output_mode_override():
    skills = protocol.skills_from_toolsets(
        {"image": ["image_generate"], "web": ["web_search"]},
        output_modes={"image": ["image/png"]},
    )

    image_skill = next(skill for skill in skills if skill["name"] == "image")
    web_skill = next(skill for skill in skills if skill["name"] == "web")
    assert image_skill["outputModes"] == ["text/plain", "image/png"]
    assert "outputModes" not in web_skill


def test_skill_output_modes_are_usable_by_reply_dispatch(monkeypatch, tmp_path):
    from gateway.config import PlatformConfig
    from a2a_async_plugin.adapter import A2AAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "tasks.db"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))

    modes = adapter._effective_output_modes({
        "output_modes": ["application/json"],
        "skill_output_modes": {"image": ["image/png"]},
    })

    assert modes == ["text/plain", "application/json", "image/png"]


def test_adapter_reads_output_modes_from_profile_a2a_config(monkeypatch, tmp_path):
    from gateway.config import PlatformConfig
    from a2a_async_plugin import adapter as adapter_module

    config = {
        "a2a": {
            "output_modes": ["image/png", "application/json"],
            "skill_output_modes": {"image": ["image/png"]},
        },
    }
    monkeypatch.setattr(
        adapter_module.A2AAdapter, "_load_global_a2a_config",
        lambda self: config,
    )
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "tasks.db"))
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    adapter = adapter_module.A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))

    card = adapter._build_card()

    assert card["defaultOutputModes"] == ["text/plain", "image/png", "application/json"]


def test_json_reply_becomes_data_part_only_when_both_sides_accept_json():
    reply = '{"answer":42}'

    structured = protocol.build_output_parts(
        reply,
        output_modes=["text/plain", "application/json"],
        accepted_output_modes=["application/json"],
    )
    legacy = protocol.build_output_parts(
        reply,
        output_modes=["text/plain", "application/json"],
        accepted_output_modes=None,
    )

    assert structured == [protocol.data_part({"answer": 42})]
    assert legacy == [protocol.text_part(reply)]


def test_file_output_requires_configured_and_peer_accepted_media_mode():
    file_part = protocol.file_part(raw="aGVsbG8=", filename="image.png", media_type="image/png")

    accepted = protocol.build_output_parts(
        "created image",
        [file_part],
        output_modes=["image/*"],
        accepted_output_modes=["image/png"],
    )
    rejected = protocol.build_output_parts(
        "created image",
        [file_part],
        output_modes=["text/plain", "image/png"],
        accepted_output_modes=["application/json"],
    )

    assert accepted == [protocol.text_part("created image"), file_part]
    assert rejected == [protocol.text_part("created image")]


def test_local_file_part_preserves_bytes_and_infers_media_type(tmp_path):
    payload = b"\x00\x01 generated image bytes \xff"
    path = tmp_path / "generated.png"
    path.write_bytes(payload)

    part, error = protocol.outbound_file_part(path, max_bytes=1024)

    assert error == ""
    assert part == {
        "raw": base64.b64encode(payload).decode("ascii"),
        "filename": "generated.png",
        "mediaType": "image/png",
    }


def test_receiving_peer_materializes_outbound_filepart_with_matching_hash(tmp_path):
    payload = b"\x00generated image bytes\xff"
    source = tmp_path / "generated.png"
    source.write_bytes(payload)
    part, error = protocol.outbound_file_part(source)

    assert error == ""
    rendered = protocol.extract_text(
        {"parts": [part]}, task_id="receiver-task", files_dir=tmp_path / "receiver-files",
    )

    destination = tmp_path / "receiver-files" / "receiver-task" / "generated.png"
    assert str(destination) in rendered
    assert hashlib.sha256(destination.read_bytes()).digest() == hashlib.sha256(payload).digest()


def test_local_file_part_rejects_symlinks_and_oversize_files(tmp_path):
    target = tmp_path / "target.bin"
    target.write_bytes(b"bytes")
    link = tmp_path / "link.bin"
    link.symlink_to(target)
    large = tmp_path / "large.bin"
    large.write_bytes(b"x" * 11)

    symlink_part, symlink_error = protocol.outbound_file_part(link, max_bytes=10)
    large_part, large_error = protocol.outbound_file_part(large, max_bytes=10)

    assert symlink_part is None
    assert "regular file" in symlink_error
    assert large_part is None
    assert "10-byte" in large_error


def test_public_file_url_is_emitted_as_a_url_part(monkeypatch):
    monkeypatch.setattr(protocol, "_safe_inbound_url", lambda url: url.startswith("https://"))

    part, error = protocol.outbound_file_part(
        "https://cdn.example.test/image.png", filename="image.png", media_type="image/png",
    )

    assert error == ""
    assert part == {
        "url": "https://cdn.example.test/image.png",
        "filename": "image.png",
        "mediaType": "image/png",
    }


def test_completed_reply_parts_survive_taskstore_restart(tmp_path):
    db_path = tmp_path / "tasks.db"
    parts = [protocol.data_part({"answer": 42})]
    store = protocol.TaskStore(str(db_path))
    store.create("task-json", "ctx-json", "peer")
    store.complete(
        "task-json", protocol.STATE_COMPLETED, '{"answer":42}', reply_parts=parts,
    )

    reopened = protocol.TaskStore(str(db_path))
    record = reopened.get("task-json")
    assert record is not None
    rendered = protocol.TaskStore.to_task(record)

    assert record["reply_parts"] == parts
    assert rendered["status"]["message"]["parts"] == parts
    assert rendered["artifacts"][0]["parts"] == parts


def test_taskstore_migrates_schema_v7_without_losing_text_reply(tmp_path):
    import sqlite3

    db_path = tmp_path / "tasks-v7.db"
    store = protocol.TaskStore(str(db_path))
    store.create("task-v7", "ctx-v7", "peer")
    store.complete("task-v7", protocol.STATE_COMPLETED, "legacy reply")
    with sqlite3.connect(db_path) as connection:
        connection.execute("ALTER TABLE a2a_tasks DROP COLUMN reply_parts")
        connection.execute("PRAGMA user_version = 7")

    migrated = protocol.TaskStore(str(db_path)).get("task-v7")

    assert migrated is not None
    assert migrated["reply"] == "legacy reply"
    assert migrated["reply_parts"] == []


def test_final_adapter_reply_persists_negotiated_json_data_part(monkeypatch, tmp_path):
    from gateway.config import PlatformConfig
    from a2a_async_plugin.adapter import A2AAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "tasks.db"))
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))
    adapter.tasks = protocol.TaskStore(str(tmp_path / "tasks.db"))
    adapter.tasks.create("task-json", "ctx-json", "peer")
    future = adapter._add_pending(
        "task-json", "ctx-json", output_modes=["application/json"],
        accepted_output_modes=["application/json"],
    )
    adapter._note_turn_start("ctx-json", "task-json")
    pending = {
        "task_id": "task-json", "context_id": "ctx-json", "peer": "peer",
        "future": future, "created_iso": "", "started": time.time(),
    }

    asyncio.run(adapter.send(
        "ctx-json", '{"answer":42}', metadata={"notify": True},
    ))
    state, reply = adapter._await_reply(pending)
    adapter._finalize_task(pending, state, reply)

    record = adapter.tasks.get("task-json")
    assert record is not None
    assert record["reply_parts"] == [protocol.data_part({"answer": 42})]
    rendered = protocol.TaskStore.to_task(record)
    assert rendered["status"]["message"]["parts"] == record["reply_parts"]
    assert rendered["artifacts"][0]["parts"] == record["reply_parts"]


def test_final_adapter_reply_attaches_explicit_media_file(monkeypatch, tmp_path):
    from gateway.config import PlatformConfig
    from a2a_async_plugin.adapter import A2AAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "tasks.db"))
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))
    adapter.tasks = protocol.TaskStore(str(tmp_path / "tasks.db"))
    adapter.tasks.create("task-file", "ctx-file", "peer")
    future = adapter._add_pending(
        "task-file", "ctx-file", output_modes=["image/png"],
        accepted_output_modes=["image/png"],
    )
    adapter._note_turn_start("ctx-file", "task-file")
    pending = {
        "task_id": "task-file", "context_id": "ctx-file", "peer": "peer",
        "future": future, "created_iso": "", "started": time.time(),
    }
    payload = b"generated image bytes"
    file_path = tmp_path / "generated.png"
    file_path.write_bytes(payload)

    asyncio.run(adapter.send(
        "ctx-file", f"Generated image\nMEDIA:{file_path}", metadata={"notify": True},
    ))
    state, reply = adapter._await_reply(pending)
    adapter._finalize_task(pending, state, reply)

    record = adapter.tasks.get("task-file")
    assert record is not None
    file_part = next(part for part in record["reply_parts"] if "raw" in part)
    assert base64.b64decode(file_part["raw"]) == payload
    assert file_part["filename"] == "generated.png"
    assert file_part["mediaType"] == "image/png"


def test_blocking_send_returns_the_persisted_data_part(monkeypatch, tmp_path):
    from gateway.config import PlatformConfig
    from a2a_async_plugin.adapter import A2AAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "tasks.db"))
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))
    adapter.tasks = protocol.TaskStore(str(tmp_path / "tasks.db"))
    adapter.tasks.create("task-json-rpc", "ctx-json-rpc", "peer")
    future: Future = adapter._add_pending(
        "task-json-rpc", "ctx-json-rpc", output_modes=["application/json"],
        accepted_output_modes=["application/json"],
    )
    future.set_result((protocol.STATE_COMPLETED, '{"answer":42}'))
    pending = {
        "task_id": "task-json-rpc", "context_id": "ctx-json-rpc", "peer": "peer",
        "future": future, "created_iso": "", "started": time.time(),
    }
    monkeypatch.setattr(adapter, "_prepare_task", lambda *args, **kwargs: (None, pending))

    response = adapter._rpc_message_send("request-1", {}, "peer")

    task = response["result"]
    assert task["status"]["message"]["parts"] == [protocol.data_part({"answer": 42})]
    assert task["artifacts"][0]["parts"] == [protocol.data_part({"answer": 42})]


def test_prepare_task_captures_peer_accepted_output_modes(monkeypatch, tmp_path):
    from gateway.config import PlatformConfig
    from a2a_async_plugin.adapter import A2AAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "tasks.db"))
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))
    adapter._loop = asyncio.new_event_loop()
    setattr(adapter, "_message_handler", lambda event: None)
    monkeypatch.setattr(adapter, "_session_is_busy", lambda event: False)

    def discard_coroutine(coroutine, loop):
        coroutine.close()
        return None

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", discard_coroutine)
    terminal, pending = adapter._prepare_task(
        {
            "message": {"parts": [protocol.text_part("return JSON")]},
            "configuration": {"acceptedOutputModes": ["application/json"]},
        },
        "peer",
    )

    assert terminal is None
    assert pending is not None
    settings = adapter._pending_output_settings[pending["task_id"]]
    assert settings["accepted_output_modes"] == ["application/json"]
    assert settings["output_modes"] == ["text/plain", "application/json"]
    adapter._loop.close()


class _SseSink:
    """Minimal request handler for the adapter's SSE writer."""

    def __init__(self):
        self.wfile = io.BytesIO()

    def frames(self):
        payloads = []
        for chunk in self.wfile.getvalue().decode("utf-8").split("\n\n"):
            line = chunk.strip()
            if line.startswith("data: "):
                envelope = json.loads(line[len("data: "):])
                payloads.append(envelope.get("result", envelope))
        return payloads


def _adapter_at(monkeypatch, tmp_path):
    from gateway.config import PlatformConfig
    from a2a_async_plugin.adapter import A2AAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_TASKS_DB", str(tmp_path / "tasks.db"))
    monkeypatch.setenv("A2A_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.delenv("A2A_PORT", raising=False)
    return A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))


def _adapter_with_pending(monkeypatch, tmp_path, task_id, *, output_modes, accepted,
                          context_id="ctx-stream"):
    adapter = _adapter_at(monkeypatch, tmp_path)
    adapter.tasks = protocol.TaskStore(str(tmp_path / "tasks.db"))
    adapter.tasks.create(task_id, context_id, "peer")
    future = adapter._add_pending(
        task_id, context_id, output_modes=output_modes,
        accepted_output_modes=accepted,
    )
    adapter._note_turn_start(context_id, task_id)
    pending = {
        "task_id": task_id, "context_id": context_id, "peer": "peer",
        "future": future, "created_iso": "", "started": time.time(),
    }
    return adapter, pending


def test_message_stream_emits_the_persisted_data_part_in_the_artifact(monkeypatch, tmp_path):
    """The terminal SSE artifact must carry the stored parts, not re-render text.

    This is the stream wiring the builder tests cannot see: ``_finalize_task``
    persists ``reply_parts`` and ``_rpc_message_stream`` has to read them back
    after finalization.
    """
    adapter, pending = _adapter_with_pending(
        monkeypatch, tmp_path, "task-stream-json",
        output_modes=["text/plain", "application/json"],
        accepted=["application/json"],
    )
    monkeypatch.setattr(adapter, "_prepare_task", lambda *args, **kwargs: (None, pending))
    monkeypatch.setattr(
        adapter, "_await_reply", lambda *args, **kwargs: (protocol.STATE_COMPLETED, '{"answer":42}'),
    )
    monkeypatch.setattr(adapter, "_sse_headers", lambda handler: None)
    sink = _SseSink()

    adapter._rpc_message_stream(sink, "request-stream", {}, "peer")

    artifact = next(
        frame["artifactUpdate"] for frame in sink.frames() if "artifactUpdate" in frame
    )
    status = [
        frame["statusUpdate"] for frame in sink.frames() if "statusUpdate" in frame
    ][-1]
    assert artifact["artifact"]["parts"] == [protocol.data_part({"answer": 42})]
    assert status["status"]["state"] == protocol.STATE_COMPLETED


def test_terminal_event_with_only_file_parts_still_emits_the_artifact():
    """A reply that is only a file has no text; the artifact must still go out."""
    from a2a_async_plugin.adapter import A2AAdapter

    adapter = object.__new__(A2AAdapter)
    sink = _SseSink()
    file_part = protocol.file_part(raw="aGVsbG8=", filename="x.txt", media_type="text/plain")

    adapter._emit_terminal(
        sink, "task-file-only", "ctx-file-only", protocol.STATE_COMPLETED, "",
        req_id="request-file", parts=[file_part],
    )

    artifact = next(
        frame["artifactUpdate"] for frame in sink.frames() if "artifactUpdate" in frame
    )
    assert artifact["artifact"]["parts"] == [file_part]


def _host_event(adapter, context_id, task_id):
    from gateway.platforms.base import MessageEvent, MessageType

    return MessageEvent(
        text="generate", message_type=MessageType.TEXT,
        source=adapter.build_source(chat_id=context_id, chat_type="dm", user_id="peer"),
        message_id=task_id,
    )


def test_host_response_delivery_attaches_the_media_file(monkeypatch, tmp_path):
    """The real host path strips MEDIA before send(): the file must still ship.

    This drives the inherited response pipeline rather than calling send()
    directly, so it fails if attachments are only extracted inside send().
    """
    adapter, pending = _adapter_with_pending(
        monkeypatch, tmp_path, "task-host", output_modes=["image/png"],
        accepted=["image/png"], context_id="ctx-host",
    )
    source = tmp_path / "image.png"
    source.write_bytes(b"generated image")

    async def handler(event):
        return f"Generated image\nMEDIA:{source}"

    adapter._message_handler = handler
    event = _host_event(adapter, "ctx-host", "task-host")
    asyncio.run(adapter._process_message_background(event, adapter._event_session_key(event)))
    state, reply = adapter._await_reply(pending)
    adapter._finalize_task(pending, state, reply)

    record = adapter.tasks.get("task-host")
    assert record is not None
    file_parts = [part for part in record["reply_parts"] if "raw" in part]
    assert file_parts, record["reply_parts"]
    assert base64.b64decode(file_parts[0]["raw"]) == b"generated image"
    assert file_parts[0]["mediaType"] == "image/png"


def test_host_response_delivery_of_a_file_only_reply_settles_the_task(monkeypatch, tmp_path):
    """No text means no send(); the attachment hook is the only settlement point."""
    adapter, pending = _adapter_with_pending(
        monkeypatch, tmp_path, "task-file-only", output_modes=["image/png"],
        accepted=["image/png"], context_id="ctx-file-only",
    )
    source = tmp_path / "only.png"
    source.write_bytes(b"file only")

    async def handler(event):
        return f"MEDIA:{source}"

    adapter._message_handler = handler
    event = _host_event(adapter, "ctx-file-only", "task-file-only")
    asyncio.run(adapter._process_message_background(event, adapter._event_session_key(event)))
    state, reply = adapter._await_reply(pending)
    adapter._finalize_task(pending, state, reply)

    record = adapter.tasks.get("task-file-only")
    assert record is not None
    assert any("raw" in part for part in record["reply_parts"]), record["reply_parts"]


def test_missing_output_preferences_keep_the_legacy_text_carrier():
    """Omitted acceptedOutputModes means text-only, even for a text/plain file."""
    report = protocol.file_part(raw="aGVsbG8=", filename="report.txt", media_type="text/plain")

    legacy = protocol.build_output_parts("report", [report], accepted_output_modes=None)
    explicit = protocol.build_output_parts("report", [report], accepted_output_modes=["text/plain"])

    assert legacy == [protocol.text_part("report")]
    assert explicit == [protocol.text_part("report"), report]


def test_forwarded_agent_honors_advertised_json(monkeypatch, tmp_path):
    """A routed (non-local) profile negotiates output modes like a local one."""
    adapter = _adapter_at(monkeypatch, tmp_path)
    monkeypatch.setattr(
        adapter, "_forward_to_profile",
        lambda *args: ('{"answer":42}', protocol.STATE_COMPLETED),
    )
    agent = {
        "slug": "other", "tenant": "other", "profile": "other", "local": False,
        "output_modes": ["application/json"],
    }

    response = adapter._rpc_message_send(
        "request-forwarded",
        {
            "message": {"parts": [protocol.text_part("return json")]},
            "configuration": {"acceptedOutputModes": ["application/json"]},
        },
        "peer", agent=agent,
    )

    assert response["result"]["artifacts"][0]["parts"] == [protocol.data_part({"answer": 42})]


_SUBSCRIBE_PARTS = [
    protocol.data_part({"answer": 42}),
    protocol.file_part(raw="aGVsbG8=", filename="x.txt", media_type="text/plain"),
]


@pytest.mark.parametrize("expected", _SUBSCRIBE_PARTS, ids=["data", "file"])
def test_subscribe_already_terminal_emits_the_persisted_parts(monkeypatch, tmp_path, expected):
    """SubscribeToTask must render stored parts for a task that already finished."""
    adapter = _adapter_at(monkeypatch, tmp_path)
    adapter.tasks = protocol.TaskStore(str(tmp_path / "tasks.db"))
    adapter.tasks.create("task-sub", "ctx-sub", "peer")
    adapter.tasks.complete(
        "task-sub", protocol.STATE_COMPLETED, "reply", reply_parts=[expected],
    )
    monkeypatch.setattr(adapter, "_sse_headers", lambda handler: None)
    sink = _SseSink()

    adapter._rpc_tasks_subscribe(sink, "req-sub", {"taskId": "task-sub"})

    artifact = next(
        frame["artifactUpdate"] for frame in sink.frames() if "artifactUpdate" in frame
    )
    assert artifact["artifact"]["parts"] == [expected]


@pytest.mark.parametrize("expected", _SUBSCRIBE_PARTS, ids=["data", "file"])
def test_subscribe_live_completion_emits_the_persisted_parts(monkeypatch, tmp_path, expected):
    """A live subscription picks up parts persisted by the completing writer."""
    adapter = _adapter_at(monkeypatch, tmp_path)
    adapter.tasks = protocol.TaskStore(str(tmp_path / "tasks.db"))
    adapter.tasks.create("task-live", "ctx-live", "peer")
    adapter.tasks.set_state("task-live", protocol.STATE_WORKING)
    monkeypatch.setattr(adapter, "_sse_headers", lambda handler: None)
    sink = _SseSink()

    worker = threading.Thread(
        target=adapter._rpc_tasks_subscribe,
        args=(sink, "req-live", {"taskId": "task-live"}),
    )
    worker.start()
    deadline = time.time() + 3.0
    while time.time() < deadline and not adapter.tasks._watchers.get("task-live"):
        time.sleep(0.01)
    adapter.tasks.complete(
        "task-live", protocol.STATE_COMPLETED, "reply", reply_parts=[expected],
    )
    worker.join(timeout=5.0)

    assert not worker.is_alive()
    artifact = next(
        frame["artifactUpdate"] for frame in sink.frames() if "artifactUpdate" in frame
    )
    assert artifact["artifact"]["parts"] == [expected]
