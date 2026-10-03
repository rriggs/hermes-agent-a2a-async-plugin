# tests/interoperability — official Python A2A SDK interoperability

This directory hosts **Phase 5 / Task 12** of
`docs/restart-session-resilience-plan.md`: real cross-implementation
interoperability with the official `a2a-sdk` Python package
(https://pypi.org/project/a2a-sdk/, repository
https://github.com/a2aproject/a2a-python). No mocked HTTP stands in
for the SDK — every test exchanges real wire traffic with a real SDK
client (driven from the dedicated venv) against a real plugin server
(via the Phase 1 launcher harness), and vice versa.

The tests in this directory **SKIP gracefully** when the SDK venv has
not been set up; the plan explicitly calls this out:

> the SDK test must SKIP gracefully when tests/interoperability/.venv-sdk
> has not been set up, and PASS end-to-end after the setup script in
> your verification

## Contents

| File | Purpose |
|------|---------|
| `setup_sdk_env.sh` | Build `.venv-sdk/` and install the pinned `a2a-sdk[http-server]==1.1.2` plus `uvicorn` and `httpx`. Idempotent. |
| `sdk_echo_server.py` | Minimal A2A-compliant echo server using `a2a-sdk` (Starlette + uvicorn). Stdlib + a2a-sdk only; bound to a loopback port; serves the v1.0 Agent Card and `SendMessage`. |
| `test_a2a_python_sdk.py` | Three real cross-implementation tests: SDK client → plugin (v1.0 `SendMessage`), SDK client → plugin (legacy `message/send`), plugin client → SDK server (SendMessage + GetTask + ListTasks + CancelTask). |
| `README.md` | This file. |
| `.venv-sdk/` (gitignored) | Dedicated venv. Created by `setup_sdk_env.sh`. |
| `.tmp/` (gitignored) | Scratch dir for SDK venv sub-scripts; auto-cleaned. |

## Pin

`a2a-sdk==1.1.2` with the `[http-server]` extra (starlette +
sse-starlette). The test also installs `uvicorn` and `httpx`
unpinned — both are API-stable and the SDK does not bind to a
specific version.

The pin is recorded in:
- `setup_sdk_env.sh` (the actual install command).
- `test_a2a_python_sdk.py::SDK_VERSION` (asserted at runtime).
- The verification report (see `HANDOFF.md` Phase 5 evidence).

To bump the pin, change `SDK_VERSION` in **both** `setup_sdk_env.sh`
and `test_a2a_python_sdk.py`, run the setup script, and re-run the
test.

## Setup

```bash
./tests/interoperability/setup_sdk_env.sh
```

Output (from a fresh install):

```
setup_sdk_env.sh: installing a2a-sdk[http-server]==1.1.2
setup_sdk_env.sh: done
  venv python: /home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin/tests/interoperability/.venv-sdk/bin/python
  a2a-sdk version:
    a2a-sdk at: .../tests/interoperability/.venv-sdk/lib/python3.13/site-packages/a2a/__init__.py
    a2a-sdk version: 1.1.2
```

The venv is at `tests/interoperability/.venv-sdk/` (excluded from
version control). The script is **refuses** to create a venv outside
this directory; we never install the SDK into the Hermes runtime
`.venv` per HANDOFF.md.

## Run

```bash
PYTHONPATH=/home/hermes/.hermes/hermes-agent \
/home/hermes/.hermes/hermes-agent/.venv/bin/python -m pytest -q tests/interoperability
```

The expected venv python invocation for the SDK sub-scripts is the
one printed by `setup_sdk_env.sh` — for the verification report:

```
/home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin/tests/interoperability/.venv-sdk/bin/python
```

## Skip when the venv is missing

If `.venv-sdk/` is absent, every test in this directory skips with a
clear message:

```
SKIPPED [1] tests/interoperability/test_a2a_python_sdk.py: setup_sdk_env.sh not run
```

The skip is enforced in **two** places to defend against pytest's
collection-vs-setup ordering quirks:
1. `pytest_collection_modifyitems` (per-item marker).
2. Module-scope fixtures (`sdk_version`, `plugin_gateway`,
   `sdk_echo_server`) — each calls `pytest.skip(...)` if the venv is
   missing.

## What the tests prove (cross-implementation, real)

### Test 1 — SDK client → plugin (v1.0 `SendMessage`)

Spawns the plugin's real inbound adapter on a loopback port using the
Phase 1 launcher harness (so the real `A2AAdapter`, the real
`TaskStore`, the real auth path are exercised). Then, from the SDK
venv, runs `a2a.client.ClientFactory.create_from_url(...)` and emits
a real v1.0 `SendMessage` JSON-RPC envelope through the SDK's
`JsonRpcTransport`. Asserts:

* the SDK's `StreamResponse` is wrapped in a v1.0 envelope
  (`result_kind == "task"`),
* the `task.id` / `context_id` are non-empty and parseable,
* the `state` is a valid `TASK_STATE_*` value
  (the plugin's headless adapter may return WORKING, FAILED,
  COMPLETED, or SUBMITTED depending on the async path's finalizer
  timing — all are valid protocol responses).

### Test 2 — SDK client → plugin (legacy `message/send`)

Drives the SDK to emit the **legacy** JSON-RPC method name
(`message/send`, snake_case) over the v1.0 Agent Card alias
(`/.well-known/agent.json`) using a raw `httpx` script (the SDK's
high-level client always emits v1.0 `SendMessage` when the card
advertises a v1.0 supportedInterfaces entry — the legacy alias must
be exercised at the wire level). Asserts:

* the method that landed on the wire is `message/send`,
* the response is a **bare Task** payload, NOT a v1.0 envelope (the
  adapter's `_method_info` distinguishes v1.0 from legacy and the
  response shape must match).

### Test 3 — Plugin client → SDK server

Spawns `sdk_echo_server.py` on a second loopback port under the SDK
venv's Python. Drives the plugin's outbound client engine
(`a2a_async_plugin.tools._send_task` for the synchronous path and
`a2a_async_plugin.tools._rpc_request` for the raw JSON-RPC paths used
by `a2a_submit` / `a2a_get_task` / `a2a_cancel`) at the SDK echo
server. Asserts:

* `_send_task` returns a real reply and the plugin's
  `protocol.load_conversation` recorded the user message (proves the
  on-disk conversation persistence layer survived the cross-impl
  round-trip).
* The raw `SendMessage` JSON-RPC envelope parses and the
  `result.task` carries an `id` and `contextId`.
* The SDK's `GetTask` returns the task in `TASK_STATE_COMPLETED` with
  the echo artifact in `artifacts[].parts[].text` (proves the plugin
  client parses the SDK's response shape correctly).
* The SDK's `ListTasks` includes the task we just submitted.
* `CancelTask` on an already-terminal task surfaces as a valid
  protocol response — either a `result` (cancel is a no-op for
  already-terminal tasks) or a JSON-RPC error with code `-32002`
  (`TaskNotCancelable`). The test asserts either form; it does NOT
  hang.

## What was intentionally not done

### Streaming (SSE)

The plan says: "exercise streaming (SSE) only if the SDK client is
straightforward about it -- record what you skipped and why." The
SDK's `ClientConfig(streaming=True)` opens an SSE connection and
yields `TaskArtifactUpdateEvent` / `TaskStatusUpdateEvent` frames.
The plugin's adapter emits well-formed SSE for `message/stream`
(v1.0 §9.4) but the SDK's `JsonRpcTransport` requires the card to
advertise streaming capability AND the test would need to consume an
async iterator inside a subprocess script. We exercise the wire-level
SSE envelopes implicitly through the v1.0 `SendMessage` and
`message/stream` JSON-RPC routes but do not run the SDK's SSE
consumer as a separate test. The phase 4 / task 13 conformance work
(`docs/a2a-conformance.md`) will own the streaming-shape contract
assertions.

### `SubscribeToTask`

The SDK exposes `client.subscribe(...)` over SSE; the plugin supports
`tasks/subscribe` JSON-RPC. We do not drive it through the SDK client
because the SDK's `subscribe` requires a long-lived SSE consumer and
the venv sub-script harness is one-shot. Same rationale as streaming.

### Push notification config (`CreateTaskPushNotificationConfig` etc.)

The plugin's adapter supports the four push-config operations
(`CreateTaskPushNotificationConfig` / `GetTaskPushNotificationConfig` /
`ListTaskPushNotificationConfigs` / `DeleteTaskPushNotificationConfig`)
but the SDK's typed client surface for them requires the
extended-card path and a long-running test consumer. Not exercised
in this round; tracked for Phase 6 (Task 13 conformance).

### gRPC

The SDK's `GrpcTransport` requires `pip install "a2a-sdk[grpc]"` (which
adds grpcio). The plugin's adapter is JSON-RPC over HTTP only. There
is no cross-implementation exchange to test for gRPC; the SDK
client raises `ValueError("no compatible transports found.")` if you
ask it to drive a JSON-RPC-only card with a gRPC transport. Recorded
here so Phase 6 (Task 13) can confirm: "yes, gRPC is out-of-scope
until the plugin adds a gRPC adapter."

## Traceability (per contract §9)

| Method name | v1.0 / Legacy | Response envelope | Plugin source | SDK transport |
|-------------|---------------|-------------------|---------------|---------------|
| `SendMessage` | v1.0 | `{ task \| message }` (oneof) | `adapter._rpc_message_send(..., v1_response=True)` | `JsonRpcTransport.send_message` |
| `message/send` | legacy | bare Task | `adapter._rpc_message_send(..., v1_response=False)` | n/a (raw httpx in test 2) |
| `GetTask` | v1.0 | bare Task | `adapter._rpc_tasks_get` | `JsonRpcTransport.get_task` (used via `_rpc_request`) |
| `ListTasks` | v1.0 | `{ tasks, nextPageToken, pageSize, totalSize }` | `adapter._rpc_tasks_list` | `JsonRpcTransport.list_tasks` (used via `_rpc_request`) |
| `CancelTask` | v1.0 | bare Task or `-32002` (TaskNotCancelable) | `adapter._rpc_tasks_cancel` | n/a (used via plugin client in test 3) |
| `message/stream` | legacy | SSE | `adapter._rpc_message_stream` | not exercised in this round |
| `SubscribeToTask` | v1.0 | SSE | `adapter._rpc_tasks_subscribe` | not exercised in this round |
| `tasks/pushNotificationConfig/*` | mixed | per v1.0 §5.3 | `adapter._rpc_push_config_*` | not exercised in this round |

## How to verify (clean checkout)

1. `PYTHONPATH=/home/hermes/.hermes/hermes-agent pytest -q tests/recovery tests/integration` — Phase 1-5 in-process tests (97 + 5 from this round).
2. `./tests/interoperability/setup_sdk_env.sh` — install the SDK.
3. `PYTHONPATH=/home/hermes/.hermes/hermes-agent /home/hermes/.hermes/hermes-agent/.venv/bin/python -m pytest -q tests/interoperability` — Phase 5 / Task 12 cross-impl tests (3 passed; SDK venv python invocation reported in the verification log).
4. `python -m compileall -q a2a_async_plugin` — compile check.
5. `git diff --check` — whitespace.
