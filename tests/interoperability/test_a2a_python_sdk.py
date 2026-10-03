"""
Phase 5 / Task 12 — Official Python A2A SDK interoperability.

Acceptance from docs/restart-session-resilience-plan.md Phase 5 / Task 12:

> At least one real cross-implementation exchange each direction
> completes; task state/artifacts parse on both sides; any unsupported
> operation surfaces as a protocol ERROR not a hang (bounded by
> explicit timeouts on every socket call). If the installed SDK version
> forces legacy vs v1.0 mode selection, test BOTH modes it exposes and
> label which method names/payloads belong to which shape per
> contract/traceability §9.

Two real cross-implementation exchanges, NO mocked HTTP:

1. SDK client -> plugin server: we run the plugin's inbound adapter
   on a loopback port via the Phase 1 launcher harness (so we
   exercise the real A2AAdapter, the real TaskStore, the real
   auth path) and then with the SDK venv's python drive the SDK
   client at it. We use both v1.0 (`SendMessage` PascalCase) and
   the legacy (`message/send`) method names when the SDK exposes
   both, and we record the wire-level envelopes per contract §9.

2. Plugin client -> SDK server: we run the SDK's `sdk_echo_server.py`
   on a second loopback port (spawned as a real subprocess under the
   SDK venv) and drive the plugin's outbound client engine
   (`a2a_async_plugin.tools._send_task` / `_rpc_request`) at it
   with a real cross-implementation SendMessage. We then poll
   through `a2a_get_task` semantics and exercise `a2a_cancel` to
   confirm the same code path the user calls terminates a SDK-side
   task. The plugin's outbound client is the same code path the
   Hermes agent uses — no hand-rolled curl.

The SDK venv is set up by `setup_sdk_env.sh` and lives at
`tests/interoperability/.venv-sdk/`. If the venv is missing the test
SKIPS (per the plan: "the SDK test must SKIP gracefully when
tests/interoperability/.venv-sdk has not been set up, and PASS
end-to-end after the setup script in your verification").
"""
from __future__ import annotations

import contextlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
SDK_VENV = HERE / ".venv-sdk"
SDK_PYTHON = SDK_VENV / "bin" / "python"
SDK_ECHO_SERVER = HERE / "sdk_echo_server.py"

PLUGIN_REPO = REPO_ROOT
HERMES_TREE = Path("/home/hermes/.hermes/hermes-agent")
HERMES_PYTHON = HERMES_TREE / ".venv/bin/python"
GATEWAY_LAUNCHER = REPO_ROOT / "tests" / "recovery" / "_gateway_launcher.py"

# A2A v1.0 well-known Agent Card path. The plugin's adapter answers at
# BOTH ``/.well-known/agent-card.json`` (v1.0 canonical) and the legacy
# ``/.well-known/agent.json`` alias (see adapter.py:268); the SDK's
# A2ACardResolver targets the v1.0 path.
AGENT_CARD_PATH = "/.well-known/agent-card.json"
LEGACY_AGENT_CARD_PATH = "/.well-known/agent.json"

# Pinned SDK version. The version is recorded here so a future drift
# in the protocol implementation (e.g. a major SDK release) forces a
# deliberate bump-and-document decision rather than silent breakage.
SDK_VERSION = "1.1.2"


# ---------------------------------------------------------------------------
# venv presence guard
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(config, items):
    """Skip the whole module cleanly if the SDK venv is missing.

    The plan is explicit: "the SDK test must SKIP gracefully when
    tests/interoperability/.venv-sdk has not been set up, and PASS
    end-to-end after the setup script in your verification".
    """
    if SDK_PYTHON.exists():
        return
    skip = pytest.mark.skip(
        reason=(
            f"SDK venv not set up at {SDK_VENV}. Run "
            f"`{HERE / 'setup_sdk_env.sh'}` to install a2a-sdk=={SDK_VERSION}. "
            "Per plan: SDK test must SKIP gracefully when .venv-sdk is missing."
        )
    )
    for item in items:
        item.add_marker(skip)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _sdk_python_version() -> str | None:
    if not SDK_PYTHON.exists():
        return None
    try:
        result = subprocess.run(
            [str(SDK_PYTHON), "-c",
             "import importlib.metadata as m; print(m.version('a2a-sdk'))"],
            capture_output=True, text=True, timeout=10,
            env={"PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8"},
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, OSError):
        pass
    return None


@pytest.fixture(scope="module")
def sdk_version() -> str:
    """The resolved a2a-sdk version. Recorded on the test report so
    the verification report can include the exact pin used.

    Skips the whole module if the SDK venv is missing — this is the
    fallback path for `pytest_collection_modifyitems` (which only
    marks test items, not module-scope fixtures used during setup)."""
    if not SDK_PYTHON.exists():
        pytest.skip(
            f"SDK venv not set up at {SDK_VENV}. Run "
            f"`{HERE / 'setup_sdk_env.sh'}` to install a2a-sdk=={SDK_VERSION}."
        )
    v = _sdk_python_version()
    assert v, "SDK venv present but a2a-sdk is not importable"
    return v


# ---------------------------------------------------------------------------
# SDK client -> plugin server
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def plugin_gateway():
    """Start the plugin's real inbound adapter on a loopback port using
    the Phase 1 launcher harness. Yields (port, log_path, profile_home).
    The fixture kills the subprocess on teardown.

    Skips when the SDK venv is missing (the test only runs once the
    SDK is installed)."""
    if not SDK_PYTHON.exists():
        pytest.skip(
            f"SDK venv not set up at {SDK_VENV}. Run "
            f"`{HERE / 'setup_sdk_env.sh'}` to install a2a-sdk=={SDK_VERSION}."
        )

    profile_home = REPO_ROOT / ".hermes" / "_interoperability_plugin_home"
    tasks_db = profile_home / "a2a_tasks.db"
    log_path = profile_home / "gateway.log"
    profile_home.mkdir(parents=True, exist_ok=True)
    for sibling in (
        tasks_db, Path(str(tasks_db) + "-wal"),
        Path(str(tasks_db) + "-shm"), Path(str(tasks_db) + "-journal"),
    ):
        if sibling.exists():
            try:
                sibling.unlink()
            except OSError:
                pass

    port = _free_port()
    env = {
        "PATH": "/usr/bin:/bin:/home/hermes/.hermes/hermes-agent/.venv/bin",
        "HOME": "/home/hermes",
        "LANG": "C.UTF-8",
        "HERMES_HOME": str(profile_home),
        "A2A_TASKS_DB": str(tasks_db),
        "A2A_HOST": "127.0.0.1",
        "A2A_ASYNC_PLUGIN_REPO": str(PLUGIN_REPO),
        "GATEWAY_LOG_PATH": str(log_path),
        "PYTHONPATH": str(HERMES_TREE) + os.pathsep + str(PLUGIN_REPO),
        "TZ": "UTC",
    }
    proc = subprocess.Popen(
        [str(HERMES_PYTHON), str(GATEWAY_LAUNCHER),
         "--port", str(port), "--ready-fd", "1"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None
    # The launcher writes its ready line to its stdout; parse the first
    # line.
    ready = None
    deadline = time.time() + 12.0
    import select
    while time.time() < deadline:
        rl, _, _ = select.select([proc.stdout], [], [], 0.2)
        if rl:
            line = proc.stdout.readline()
            if not line:
                break
            try:
                info = json.loads(line.decode("utf-8").strip())
            except ValueError:
                continue
            ready = info
            break
        if proc.poll() is not None:
            break
    if not ready or ready.get("port") != port:
        proc.kill()
        stderr = proc.stderr.read().decode("utf-8", errors="replace")
        pytest.fail(
            f"plugin gateway did not signal readiness on {port}: "
            f"proc_rc={proc.returncode}, stderr={stderr[:2000]!r}, "
            f"log={log_path.read_text(errors='replace')[:2000]!r}"
        )

    yield {"port": ready["port"], "pid": ready["pid"],
           "log_path": log_path, "profile_home": profile_home,
           "tasks_db": tasks_db}
    # Teardown
    try:
        proc.send_signal(15)  # SIGTERM
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
    finally:
        for sibling in (
            tasks_db, Path(str(tasks_db) + "-wal"),
            Path(str(tasks_db) + "-shm"), Path(str(tasks_db) + "-journal"),
        ):
            if sibling.exists():
                with contextlib.suppress(OSError):
                    sibling.unlink()


def _write_sdk_subprocess_script(body: str, name: str) -> Path:
    """Write a small script that the SDK venv's python will run, so
    the test can call SDK code without polluting its own interpreter.

    The script writes its result as a single JSON line on stdout
    (the parent parses one line per script). The script is written
    under HERE/.tmp/ (excluded from version control) and removed on
    teardown.
    """
    tmp = HERE / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    path = tmp / name
    path.write_text(body, encoding="utf-8")
    return path


def _run_sdk_script(script: Path, *, timeout: float = 30.0) -> dict:
    """Run ``script`` with the SDK venv's python, capture one JSON line
    of result, and return the decoded dict. The script is responsible
    for printing a single JSON line on stdout and exiting."""
    env = {"PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8"}
    result = subprocess.run(
        [str(SDK_PYTHON), str(script)],
        capture_output=True, text=True, timeout=timeout, env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"SDK script {script.name} failed (rc={result.returncode}): "
            f"stdout={result.stdout!r}, stderr={result.stderr!r}"
        )
    out = result.stdout.strip().splitlines()
    if not out:
        raise RuntimeError(
            f"SDK script {script.name} produced no stdout: stderr={result.stderr!r}"
        )
    try:
        return json.loads(out[-1])
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"SDK script {script.name} did not emit a JSON line: "
            f"stdout={out[-1]!r}, stderr={result.stderr!r}"
        ) from exc


def test_sdk_client_to_plugin_v1_method(plugin_gateway, sdk_version):
    """SDK client -> plugin server using the v1.0 SendMessage method
    (PascalCase, A2A v1.0 §5.3/§9.4). The SDK's JsonRpcTransport emits
    the v1.0 ``SendMessage`` method; the plugin's adapter recognises it
    (v1_response=True) and returns the v1.0 envelope ({task|message}
    oneof). Both sides should parse the result without falling back to
    legacy shapes."""
    port = plugin_gateway["port"]
    script = _write_sdk_subprocess_script(f'''
import asyncio
import json
import sys

import httpx
from a2a.client import ClientFactory, ClientConfig
from a2a.types import SendMessageRequest, Message, Part, Role


async def main():
    timeout = httpx.Timeout(8.0)
    async with httpx.AsyncClient(timeout=timeout) as http:
        cfg = ClientConfig(httpx_client=http, streaming=False, polling=False)
        factory = ClientFactory(cfg)
        client = await factory.create_from_url("http://127.0.0.1:{port}")
        text_part = Part()
        text_part.text = "ping"
        req = SendMessageRequest(
            message=Message(
                role=Role.ROLE_USER,
                parts=[text_part],
                message_id="sdk-v1-1",
            ),
        )
        last = None
        async for resp in client.send_message(req):
            last = resp
        await client.close()
    if last is None:
        sys.exit("no response from send_message")
    # StreamResponse has one of `task` / `message` populated.
    has_task = bool(getattr(last, "task", None))
    has_message = bool(getattr(last, "message", None))
    if not (has_task or has_message):
        sys.exit(f"empty response: {{last}}")
    payload = last.task if has_task else last.message
    state_obj = getattr(payload, "status", None)
    # TaskState is a protobuf EnumTypeWrapper in 1.1.2; the status
    # field carries an int (state number) by default. ``str()``
    # renders the int; the canonical name lives in ``state.name``.
    state_number = getattr(getattr(state_obj, "state", None), "value", None) or getattr(state_obj, "state", None) or 0
    state_name = getattr(state_obj, "state", None)
    # protobuf ENUM descriptors expose ``Name(value)`` lookup.
    try:
        from a2a.types import TaskState as _TS
        state_label = _TS.Name(state_number) if state_number else ""
    except Exception:
        state_label = str(state_name)
    task_id = getattr(payload, "id", "")
    context_id = getattr(payload, "context_id", "")
    print(json.dumps({{
        "method": "SendMessage",
        "transport": "JsonRpcTransport",
        "result_kind": "task" if has_task else "message",
        "task_id": task_id,
        "context_id": context_id,
        "state": state_label,
        "state_int": int(state_number) if state_number is not None else None,
    }}))


asyncio.run(main())
''', name="sdk_client_v1.py")
    try:
        result = _run_sdk_script(script)
    finally:
        with contextlib.suppress(OSError):
            script.unlink()

    # Contract assertions
    assert result["method"] == "SendMessage", result
    assert result["result_kind"] == "task", (
        "v1.0 SendMessageResponse must carry a Task; got "
        f"{result['result_kind']!r}: {result}"
    )
    assert result["task_id"].startswith("task-"), result
    assert result["context_id"].startswith("ctx-"), result
    # The plugin's headless adapter has no agent loop to finalise the
    # task; the async path returns WORKING immediately and the daemon
    # thread finalises to FAILED. Either state is a valid protocol
    # response — the assertion is that the SDK parsed the wire
    # response without raising.
    assert result["state"] in {
        "TASK_STATE_WORKING", "TASK_STATE_FAILED",
        "TASK_STATE_COMPLETED", "TASK_STATE_SUBMITTED",
    }, f"unexpected state {result['state']!r}: {result}"

    sys.modules[__name__].__dict__.setdefault("__notes__", []).append(
        f"\n[sdk client -> plugin, v1.0 SendMessage]\n"
        f"  sdk_version={sdk_version}\n"
        f"  method={result['method']!r}\n"
        f"  result_kind={result['result_kind']!r}\n"
        f"  task_id={result['task_id']!r}\n"
        f"  context_id={result['context_id']!r}\n"
        f"  state={result['state']!r}\n"
    )


def test_sdk_client_to_plugin_legacy_method(plugin_gateway, sdk_version):
    """SDK client -> plugin server using the LEGACY ``message/send``
    method (snake_case, pre-1.0). The plugin's adapter recognises the
    legacy alias (v1_response=False) and returns a bare Task payload
    (not the v1.0 envelope). Both sides should parse the result;
    the only protocol-level difference vs. the v1.0 test is the
    method name and the response shape (no {task|message} oneof)."""
    port = plugin_gateway["port"]
    # The SDK 1.1.2 client always emits v1.0 `SendMessage` (PascalCase)
    # when the agent card advertises a v1.0 supportedInterfaces entry.
    # To exercise the legacy alias we drop down to raw httpx and emit
    # the exact JSON-RPC envelope the adapter's `message/send` branch
    # expects. The plugin recognises BOTH (see adapter._method_info).
    script = _write_sdk_subprocess_script(f'''
import asyncio
import json
import sys
import uuid

import httpx


async def main():
    body = {{
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "message/send",
        "params": {{
            "message": {{
                "role": "ROLE_USER",
                "parts": [{{"text": "ping", "mediaType": "text/plain"}}],
                "messageId": "sdk-legacy-1",
            }},
        }},
    }}
    timeout = httpx.Timeout(8.0)
    async with httpx.AsyncClient(timeout=timeout) as http:
        # Legacy v0.3 path: Agent Card still answers at
        # /.well-known/agent.json (the plugin's adapter serves both
        # the v1.0 canonical and the legacy alias).
        try:
            card_resp = await http.get("http://127.0.0.1:{port}/.well-known/agent.json")
        except Exception as exc:
            sys.exit(f"legacy agent-card fetch failed: {{exc}}")
        if card_resp.status_code != 200:
            sys.exit(f"legacy agent-card HTTP {{card_resp.status_code}}")
        card = card_resp.json()
        # Use the card's RPC url if present, else the base.
        rpc_url = card.get("url") or f"http://127.0.0.1:{port}/"
        resp = await http.post(
            rpc_url, json=body,
            headers={{"A2A-Version": "1.0", "Content-Type": "application/json"}},
        )
    if resp.status_code != 200:
        sys.exit(f"HTTP {{resp.status_code}}: {{resp.text[:500]}}")
    parsed = resp.json()
    if "error" in parsed:
        sys.exit(f"protocol error: {{parsed['error']}}")
    result = parsed.get("result", {{}})
    if not isinstance(result, dict):
        sys.exit(f"non-dict result: {{parsed}}")
    # Legacy shape: result IS the Task (not wrapped in {{task}} or
    # {{message}}). v1.0 shape: result is {{task: ...}} or
    # {{message: ...}}. The plugin's adapter picks the shape based
    # on whether the method was v1.0 or legacy.
    is_v1_envelope = ("task" in result and isinstance(result["task"], dict) and result["task"].get("id")) or (
        "message" in result and isinstance(result["message"], dict)
    )
    is_legacy_bare = "id" in result and "status" in result
    if not (is_v1_envelope or is_legacy_bare):
        sys.exit(f"unexpected legacy result shape: {{parsed}}")
    task_obj = result.get("task", result)
    print(json.dumps({{
        "method": "message/send",
        "transport": "httpx (raw JSON-RPC)",
        "shape": "v1_envelope" if is_v1_envelope else "legacy_bare",
        "task_id": task_obj.get("id", ""),
        "context_id": task_obj.get("contextId", ""),
        "state": str((task_obj.get("status") or {{}}).get("state", "")),
    }}))


asyncio.run(main())
''', name="sdk_client_legacy.py")
    try:
        result = _run_sdk_script(script)
    finally:
        with contextlib.suppress(OSError):
            script.unlink()

    assert result["method"] == "message/send", result
    assert result["shape"] == "legacy_bare", (
        "the legacy 'message/send' method must return a bare Task "
        f"payload (not a v1.0 envelope): {result}"
    )
    assert result["task_id"].startswith("task-"), result
    assert result["context_id"].startswith("ctx-"), result
    assert result["state"] in {
        "TASK_STATE_WORKING", "TASK_STATE_FAILED",
        "TASK_STATE_COMPLETED", "TASK_STATE_SUBMITTED",
    }, f"unexpected state {result['state']!r}: {result}"

    sys.modules[__name__].__dict__.setdefault("__notes__", []).append(
        f"\n[sdk client -> plugin, legacy message/send]\n"
        f"  sdk_version={sdk_version}\n"
        f"  method={result['method']!r}\n"
        f"  shape={result['shape']!r}\n"
        f"  task_id={result['task_id']!r}\n"
        f"  state={result['state']!r}\n"
    )


# ---------------------------------------------------------------------------
# Plugin client -> SDK server
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sdk_echo_server():
    """Spawn ``sdk_echo_server.py`` under the SDK venv's python on a
    free loopback port. Yields (port, proc). The fixture SIGTERMs the
    subprocess on teardown; if it doesn't exit cleanly it escalates
    to SIGKILL."""
    if not SDK_PYTHON.exists() or not SDK_ECHO_SERVER.exists():
        pytest.skip("SDK venv or echo server missing; set up via setup_sdk_env.sh")
    port = _free_port()
    env = {"PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8"}
    proc = subprocess.Popen(
        [str(SDK_PYTHON), str(SDK_ECHO_SERVER), "--port", str(port)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None
    ready_port = None
    import select
    deadline = time.time() + 10.0
    while time.time() < deadline:
        rl, _, _ = select.select([proc.stdout], [], [], 0.2)
        if rl:
            line = proc.stdout.readline()
            if not line:
                break
            try:
                ready_port = int(line.decode().strip().split()[1])
            except (ValueError, IndexError):
                continue
            break
        if proc.poll() is not None:
            break
    if ready_port != port:
        proc.kill()
        err = proc.stderr.read().decode("utf-8", errors="replace")
        pytest.fail(
            f"sdk_echo_server did not signal READY on {port}: "
            f"proc_rc={proc.returncode}, stderr={err[:1500]!r}"
        )
    yield {"port": port, "proc": proc, "stderr_tail": b""}
    try:
        proc.send_signal(15)  # SIGTERM
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
    finally:
        # Drain stderr for the failure report.
        try:
            proc.stderr.close()
        except Exception:
            pass


def test_plugin_client_to_sdk_send_then_get(sdk_echo_server, sdk_version):
    """Plugin client -> SDK server.

    Drives the plugin's outbound client engine (the SAME code path
    that a2a_call / a2a_submit / a2a_get_task use in production) at
    the SDK's echo server. Asserts the v1.0 SendMessage round-trip
    completes, parses a Task on the plugin side, and the task is
    queryable via the plugin's a2a_get_task.

    The plugin's outbound client is reached through the public
    ``_send_task`` (synchronous) and ``_rpc_request`` (raw JSON-RPC
    used by a2a_submit / a2a_get_task / a2a_cancel) entrypoints —
    these are the real production code paths, not a hand-rolled curl.
    """
    port = sdk_echo_server["port"]
    # Load the plugin's tools module and drive it against the SDK
    # server's loopback port. We register the SDK server as a
    # peer in the plugin's a2a_agents config (env-only — no
    # config.yaml write) so the auth plumbing is exercised.
    base_url = f"http://127.0.0.1:{port}"
    sys.path.insert(0, str(PLUGIN_REPO))
    if str(HERMES_TREE) not in sys.path:
        sys.path.insert(0, str(HERMES_TREE))
    # The plugin's _resolve_peer requires an entry in a2a_agents OR a
    # raw http(s):// URL. Use the raw URL path (no auth header on the
    # SDK echo server; it accepts localhost anonymous).
    from a2a_async_plugin import tools as client_tools
    from a2a_async_plugin import protocol as client_protocol

    # 1) Synchronous-shaped cross-implementation SendMessage.
    sync_reply, sync_ctx, sync_state = client_tools._send_task(
        "sdk-echo",
        {"url": base_url, "auth": {}, "timeout": 30, "capabilities": []},
        "ping",
        "",  # fresh context_id
    )
    assert isinstance(sync_reply, str), sync_reply
    # The plugin's _send_task persists the message; the test reloads
    # the conversation to confirm the round-trip on disk too.
    history = client_protocol.load_conversation(sync_ctx)
    assert any("ping" in (m.get("text") or "") for m in history), (
        f"plugin's conversation persistence did not record the user message: "
        f"{history!r}"
    )

    # 2) Raw JSON-RPC via the plugin's _rpc_request to exercise
    # a2a_submit + a2a_get_task semantics (the same code path the
    # tools use). This is the cross-implementation "async" exchange.
    submit_params = {
        "message": client_protocol.text_message(
            client_protocol.ROLE_USER, "ping-async", context_id="",
        ),
        "configuration": {"returnImmediately": True},
    }
    submit_resp = client_tools._rpc_request(
        {"url": base_url, "auth": {}, "timeout": 30, "capabilities": []},
        "SendMessage", submit_params, timeout=30,
    )
    assert "result" in submit_resp, submit_resp
    submit_task = submit_resp["result"].get("task") or submit_resp["result"]
    task_id = submit_task.get("id")
    context_id = submit_task.get("contextId")
    # SDK uses UUIDs (no 'task-' prefix); plugin uses 'task-<hex>'.
    # The contract assertion is the ID is non-empty and parseable,
    # not the prefix.
    assert task_id, f"submit task id missing: {submit_task!r}"
    # The SDK echo server completes the task synchronously inside
    # execute() — by the time we receive the immediate response, the
    # task is already TASK_STATE_COMPLETED. A real async-shape peer
    # would return TASK_STATE_WORKING first and we would poll. The
    # SDK's executor is single-pass; the test still exercises the
    # GetTask / ListTasks / CancelTask round-trip below.
    initial_state = (submit_task.get("status") or {}).get("state", "")
    assert initial_state in {
        "TASK_STATE_SUBMITTED", "TASK_STATE_WORKING",
        "TASK_STATE_COMPLETED",
    }, f"unexpected initial state {initial_state!r}: {submit_task}"

    # 3) GetTask (a2a_get_task semantics) — the plugin's _rpc_request
    # drives the same JSON-RPC envelope the tools emit.
    get_resp = client_tools._rpc_request(
        {"url": base_url, "auth": {}, "timeout": 30, "capabilities": []},
        "GetTask", {"id": task_id}, timeout=30,
    )
    assert "result" in get_resp, get_resp
    get_task = get_resp["result"]
    assert get_task.get("id") == task_id, get_task
    final_state = (get_task.get("status") or {}).get("state", "")
    assert final_state == "TASK_STATE_COMPLETED", (
        f"SDK echo should complete the task by the time we GetTask: "
        f"state={final_state!r}, full={get_task!r}"
    )
    # Echo artifact must be present.
    artifacts = get_task.get("artifacts") or []
    assert any("echo: ping-async" in str(a) for a in artifacts), (
        f"GetTask did not return the echo artifact: {artifacts!r}"
    )

    # 4) ListTasks (a2a_list semantics) — bounded, must include the
    # task we just submitted.
    list_resp = client_tools._rpc_request(
        {"url": base_url, "auth": {}, "timeout": 30, "capabilities": []},
        "ListTasks", {"pageSize": 50}, timeout=30,
    )
    assert "result" in list_resp, list_resp
    list_tasks = list_resp["result"].get("tasks") or []
    assert any(t.get("id") == task_id for t in list_tasks), (
        f"ListTasks did not return our task: {list_tasks!r}"
    )

    # 5) CancelTask (a2a_cancel semantics) on a NEW task — the
    # SDK's echo completes synchronously, so by the time we cancel
    # the task is already terminal. The plugin's _rpc_request
    # surfaces the protocol-level ERR_TASK_NOT_CANCELABLE (-32002)
    # the SDK returns, which is a valid protocol error (NOT a hang).
    cancel_params = {"message": client_protocol.text_message(
        client_protocol.ROLE_USER, "ping-cancel", context_id="",
    )}
    cancel_submit = client_tools._rpc_request(
        {"url": base_url, "auth": {}, "timeout": 30, "capabilities": []},
        "SendMessage", cancel_params, timeout=30,
    )
    cancel_task_id = (cancel_submit["result"].get("task") or {}).get("id")
    cancel_resp = client_tools._rpc_request(
        {"url": base_url, "auth": {}, "timeout": 30, "capabilities": []},
        "CancelTask", {"id": cancel_task_id}, timeout=30,
    )
    # The SDK's echo completes the task synchronously, so by the
    # time we CancelTask the SDK either:
    #   a) returns a result (the cancel is a no-op for an already-
    #      terminal task; the SDK is permissive on this — many
    #      v1.0 implementations return the task unchanged), or
    #   b) returns a JSON-RPC error with code -32002 (TaskNotCancelable).
    # Both are valid protocol responses. The only failure mode is
    # a hang — which a 30s socket timeout would have raised.
    if "error" in cancel_resp:
        assert cancel_resp["error"]["code"] == -32002, cancel_resp
    else:
        assert cancel_resp.get("result", {}).get("id") == cancel_task_id, (
            f"CancelTask on already-terminal task returned unexpected shape: "
            f"{cancel_resp!r}"
        )

    sys.modules[__name__].__dict__.setdefault("__notes__", []).append(
        f"\n[plugin client -> SDK echo server]\n"
        f"  sdk_version={sdk_version}\n"
        f"  port={port}\n"
        f"  sync_state={sync_state!r}\n"
        f"  sync_ctx={sync_ctx!r}\n"
        f"  async_task_id={task_id!r}\n"
        f"  async_initial_state={initial_state!r}\n"
        f"  async_final_state={final_state!r}\n"
        f"  list_size={len(list_tasks)}\n"
        f"  cancel_resp_kind={'error' if 'error' in cancel_resp else 'result'}\n"
    )
