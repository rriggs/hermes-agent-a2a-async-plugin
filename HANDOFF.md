# A2A Async Standalone Plugin Handoff

Date: 2026-09-25
Owner: Sven
Repository: https://github.com/rriggs/hermes-agent-a2a-async-plugin.git
Workspace: /home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin

## Current status

The standalone plugin provides the inbound asynchronous A2A gateway adapter, durable protocol/task state, security helpers, and outbound synchronous/asynchronous client tools.

Current pushed HEAD:

- `a4d68e8 fix(a2a): log scoped agent name resolution errors`
- Branch: `main`
- Remote: `origin/main`
- The five commits after `61a95e3` were pushed successfully.

The plugin manifest declares:

- name: `a2a-async`
- version: `0.1.6`
- kind: `platform`
- requires Hermes: `>=0.21.3`
- ten tools in the shared `a2a` toolset

## Recent Agent Card identity fix

In a multiplexed gateway, `os.environ` can contain the default profile's bridged values. The adapter now resolves Agent Card identity in this order:

1. Scoped `A2A_AGENT_NAME` when running in a multiplexed profile scope
2. Unscoped `A2A_AGENT_NAME` for older/single-profile deployments
3. `platforms.a2a.extra.agent_name` from profile configuration
4. `hermes-<hostname>` fallback

The scoped lookup logs a warning if profile-scope resolution fails instead of failing silently.

Recommended per-profile configuration:

```yaml
platforms:
  a2a:
    extra:
      agent_name: kevin
      port: 9902
```

The code and tests cover scoped precedence, unscoped environment fallback, config fallback, and hostname fallback. The README documents the configuration surface.

## Architecture

The root `__init__.py` is the directory-plugin entry point. It registers:

- all ten outbound/client tools under `toolset="a2a"`
- the inbound platform named `a2a`
- the `A2AAdapter` HTTP server through `ctx.register_platform`

Implementation package:

- `a2a_async_plugin/adapter.py` -- inbound HTTP server, JSON-RPC dispatch, SSE, task lifecycle, push notification handling, watchdog/recovery behavior
- `a2a_async_plugin/protocol.py` -- A2A protocol helpers, SQLite `TaskStore`, task state, persistence, listing, recovery, metrics
- `a2a_async_plugin/security.py` -- authentication, bind safety, redaction, injection filtering, audit, push signing
- `a2a_async_plugin/tools.py` -- all ten client tool schemas and handlers

The plugin must be the sole inbound A2A platform owner. Do not run a duplicate bundled inbound A2A adapter alongside it. Synchronous and asynchronous tools remain in the single shared `a2a` toolset.

## Verification

Local verification using the Hermes source tree and virtualenv:

```text
PYTHONPATH=/home/hermes/.hermes/hermes-agent \
/home/hermes/.hermes/hermes-agent/.venv/bin/python -m pytest -q

7 passed
```

Also verified:

- Python compilation of plugin modules
- `git diff --check`
- direct external-loader registration
- ten tools register under `a2a`
- inbound platform registers as `a2a`
- Amy review of commits `7a354d8` and `61a95e3`: ship with changes, no blockers

Amy's findings were addressed in separate commits:

- `e5b58ad` -- avoid replacing process environment in tests
- `4c336cf` -- preserve scoped Agent Card name precedence and test resolution branches
- `b9ba581` -- cover hostname fallback
- `9be1215` -- document configured Agent Card names
- `a4d68e8` -- log scoped Agent Card name resolution errors

## Emerald deployment

The updated plugin was deployed to Emerald (`openclaw@10.100.0.3`) in all five profile plugin directories:

- `~/.hermes/plugins/a2a-async`
- `~/.hermes/profiles/kevin/plugins/a2a-async`
- `~/.hermes/profiles/stuart/plugins/a2a-async`
- `~/.hermes/profiles/dave/plugins/a2a-async`
- `~/.hermes/profiles/phil/plugins/a2a-async`

Emerald plugin validation passed. The multiplexed user gateway was restarted and verified active. Profile Agent Card names were configured as:

- default: `hermes-emerald`
- Kevin: `kevin`
- Stuart: `stuart`
- Dave: `dave`
- Phil: `phil`

Configured ports remain 9901 through 9905 respectively.

## Previously verified Alex results

Doctor against the deployed standalone plugin passed:

```text
Plugin Doctor: /Users/rob/.hermes/plugins/a2a-async
  manifest: a2a-async 0.1.6 (platform)
  OK: runtime discovery, manifest parsing, import, and registration passed
  registrations: 10 tool(s), 0 hook(s)
```

The default Alex gateway verified through the native A2A path:

- asynchronous submission
- immediate working task acceptance
- background completion
- `a2a_get_task`
- durable `a2a_list`
- cancellation
- context continuity and steering with distinct task IDs
- invalid bearer authentication rejection with HTTP 401
- Agent Card and shared ten-tool capability advertisement

An isolated temporary Alex profile also verified direct authenticated SSE streaming, including submitted, working, artifact, completed, and terminal events.

## Acceptance priorities

Rob's acceptance priorities, in approximate order, are:

1. Reliability, especially restart resilience, durable recovery, and reconciliation.
2. Compatibility with Hermes' existing A2A implementation.
3. Interoperability with at least one external implementation, preferably the official Python SDK/reference implementation.
4. Conformance with the A2A specification, with pragmatic flexibility where the protocol version or SDK transition requires it.

HMAC push signing is intentionally out of scope. The deployment does not cross trust boundaries, and A2A does not require HMAC for push callbacks.

## Open items

These are still open and should be addressed against the acceptance priorities above before calling the plugin production-complete:

1. Run restart/recovery and remote-state reconciliation in a temporary secondary profile. Preserve a durable nonterminal task, restart only the temporary gateway, then verify `tasks/get`, `tasks/list`, orphan handling, and reconciliation.
2. Verify compatibility with Hermes' existing synchronous A2A implementation and shared `a2a` toolset, including mixed synchronous/asynchronous operation in one gateway.
3. Run an interoperability test against the official Python A2A SDK/reference implementation, covering Agent Card discovery, message send, task polling, streaming where supported, and push notification delivery where practical.
4. Perform a focused A2A specification conformance review for the supported v1.0-shaped and legacy-compatible methods and payloads.
5. Expand the repository test suite beyond the current seven tests. Add focused adapter, security, TaskStore, SSE, push, restart/recovery, and loader-boundary coverage.
6. Investigate or document the `hermes plugins validate` / `hermes_state_ids` discrepancy observed on Alex. Doctor passed, but validation previously failed in an environment whose capability subprocess could not import `hermes_state_ids`.
7. Re-run validation against a clean tagged checkout and create a release tag only after the acceptance tests pass.
8. Re-run the full Hermes-side plugin Doctor and deployment checks after any release/tag operation.

The plugin is usable and deployed, but it is not yet production-complete because restart/recovery, Hermes compatibility, external interoperability, expanded acceptance coverage, and clean tagged-checkout verification remain incomplete.

## Phase 5: Hermes compatibility and external interoperability

Status: complete (Tasks 11 and 12 from
`docs/restart-session-resilience-plan.md`).

Pytest: `103 passed, 2 skipped` from a clean checkout run
(`PYTHONPATH=/home/hermes/.hermes/hermes-agent
/home/hermes/.hermes/hermes-agent/.venv/bin/python -m pytest -q`).
The 2 skips are the live-peer (c) tests — the conftest looks for an
auth token at `tests/integration/.a2a_live_token`; on hosts without
the sven profile's dev-a peer, those tests skip cleanly with the
plan's "skip if the peer probe is unreachable or not authed" guard.

### Task 11 — Mixed Hermes A2A compatibility (`tests/integration/test_hermes_compatibility.py`)

* **(a) Registration / mix test, in-process, hermetic env (NOTES #12).**
  Three tests:
  - `test_registration_records_exactly_ten_a2a_tools_and_one_platform`:
    loads the repo-root plugin the way `tests/test_plugin.py` does
    (importlib against `__init__.py`), calls `register(ctx)` against
    a hermetic `HERMES_HOME`, and asserts the ten tools
    `a2a_discover / a2a_call / a2a_list / a2a_history / a2a_orchestrate
    / a2a_submit / a2a_get_task / a2a_await / a2a_cancel / a2a_steer`
    are all registered under `toolset='a2a'` with zero duplicate
    names, and the inbound platform named `'a2a'` is registered
    exactly once via `ctx.register_platform`.
  - `test_both_send_message_shapes_through_single_owner`: builds the
    adapter in-process (the same `_gateway_launcher.py` construction
    the Phase 1 harness uses), binds it on a loopback port, and
    asserts BOTH `SendMessage` shapes — synchronous (no
    `returnImmediately`, blocking) and asynchronous
    (`configuration.returnImmediately=true`, immediate
    `TASK_STATE_WORKING`) — produce valid JSON-RPC responses through
    the SAME adapter. This is the "both call shapes through one
    owner" mixed-mode proof.
* **(b) Real installation check, `hermes plugins list --json`.**
  `test_real_runtime_registration_via_hermes_plugins_list` runs
  `/home/hermes/.hermes/hermes-agent/.venv/bin/hermes plugins list
  --json` and asserts the a2a-async row has `status="enabled"` and
  `source="user"`, and that the description references the shared
  `a2a` toolset. Recorded row (current pinned HEAD):
  - name: `a2a-async`
  - status: `enabled`
  - version: `0.1.6`
  - source: `user`
  - description: `"Complete asynchronous A2A client and inbound
    gateway contributed to the shared a2a toolset."`
  The full ten-tool list assertion lives in (a); (b) only confirms
  the runtime is wired up. The skip-if-runtime-unavailable guard
  honours the plan's "never fail because the runtime is absent, but
  DO fail on a present runtime missing the registration" rule.
* **(c) Real peer loop, live evidence, plugin client → dev-a.**
  `test_real_peer_sync_via_a2a_call` and
  `test_real_peer_async_via_a2a_submit_then_get_task` drive the
  plugin's own outbound client (`a2a_call`, `a2a_submit`,
  `a2a_get_task`) at dev-a (a real Hermes agent on this host that
  runs the SAME async plugin as its inbound adapter). Skip guard
  (TCP probe + auth check) prevents 401s/connect-refused from
  failing the run. When the sven-profile `A2A_BEARER_TOKEN` is in
  the test env (drop a token into `tests/integration/.a2a_live_token`
  — excluded from version control), the tests pass and record the
  exact task id, state, and timing in the test log.

#### Scope correction rationale

Task 11's plan wording ("standalone async plugin alongside Hermes'
existing synchronous A2A implementation in one gateway") predates
the fleet cutover — the standalone plugin is now the SOLE inbound
A2A platform owner (HANDOFF.md constraint: never two inbound
adapters). Therefore "mixed operation" means: ONE adapter owning
inbound, with BOTH synchronous-style calls AND async task-based
calls working through it, and the shared `a2a` toolset registering
all tools without duplicates. The test in (a) exercises exactly
this shape — the adapter handles both `SendMessage` call shapes
through one owner; the toolset registration is a single, deduped
list of ten tools. The plan's "Do not attempt to install or run a
second bundled inbound adapter" instruction was honoured — there
is no second adapter under test, just the one inbound owner with
both call shapes.

### Task 12 — Official Python A2A SDK interoperability (`tests/interoperability/`)

* **SDK version pinned: `a2a-sdk==1.1.2`** (the latest 1.1.x at
  acceptance time; install command in `setup_sdk_env.sh` is
  `a2a-sdk[http-server]==1.1.2`). Pinned in two places —
  `setup_sdk_env.sh::SDK_VERSION` and
  `test_a2a_python_sdk.py::SDK_VERSION` — and asserted at runtime
  via the `sdk_version` fixture. The venv is gitignored at
  `tests/interoperability/.venv-sdk/`; the setup script refuses to
  install outside this directory. We never install the SDK into
  the Hermes runtime `.venv` per the HANDOFF.md constraint.
* **Setup script** (`setup_sdk_env.sh`): builds
  `tests/interoperability/.venv-sdk/` with `a2a-sdk[http-server]==1.1.2`
  + `uvicorn` + `httpx`. Strips the leaky
  `/home/hermes/.hermes/installs/` Python from `PYTHONPATH` before
  each `pip` invocation (a quirk of subagent execution: the
  subagent's `PYTHONPATH` can carry a Python 3.14 venv that breaks
  the SDK's `pydantic_core` ABI on a Python 3.13 venv). Idempotent
  on re-run; a venv whose pin matches the README is a no-op.
* **SDK server helper** (`sdk_echo_server.py`): a minimal
  a2a-sdk v1.0-compliant echo server (Starlette + uvicorn, stdlib
  + a2a-sdk only) that serves the v1.0 Agent Card at
  `/.well-known/agent-card.json` and `SendMessage` /
  `message/stream` over JSON-RPC. Spawned as a real subprocess by
  the test fixture.
* **Tests** (`test_a2a_python_sdk.py`):
  - `test_sdk_client_to_plugin_v1_method`: SDK client →
    plugin server using the v1.0 `SendMessage` method
    (PascalCase, A2A v1.0 §5.3/§9.4). The SDK's
    `JsonRpcTransport` emits the v1.0 method; the plugin's
    `A2ARequestHandler.do_POST` recognises it (v1_response=True)
    and returns the v1.0 envelope (`{ task | message }` oneof).
    Both sides parse without falling back to legacy shapes.
  - `test_sdk_client_to_plugin_legacy_method`: SDK client →
    plugin server using the legacy `message/send` method
    (snake_case, pre-1.0). Driven at the wire level with raw
    `httpx` because the SDK's high-level client always emits
    v1.0 `SendMessage` when the card advertises a v1.0
    supportedInterfaces entry; the legacy alias is reachable
    only at the JSON-RPC envelope level. Asserts the response is
    a bare Task payload (NOT a v1.0 envelope) per
    `adapter._method_info`'s v1.0/legacy discrimination.
  - `test_plugin_client_to_sdk_send_then_get`: plugin client →
    SDK server. Drives the plugin's outbound client engine
    (`a2a_async_plugin.tools._send_task` for sync,
    `a2a_async_plugin.tools._rpc_request` for the raw JSON-RPC
    paths used by `a2a_submit` / `a2a_get_task` / `a2a_cancel`)
    at the SDK echo server. Asserts: synchronous `_send_task`
    round-trip with on-disk conversation persistence intact;
    `SendMessage` returns a Task with non-empty id and
    contextId; `GetTask` returns the task in
    `TASK_STATE_COMPLETED` with the echo artifact in
    `artifacts[].parts[].text` (proves the plugin client parses
    the SDK's response shape correctly); `ListTasks` includes
    the task we just submitted; `CancelTask` on an
    already-terminal task surfaces as a valid protocol response
    (either a `result` or a JSON-RPC error with code `-32002`
    `TaskNotCancelable` — the test asserts either form; it does
    NOT hang).
* **Skip path:** when `tests/interoperability/.venv-sdk/` is
  absent, the whole module SKIPs with a clear message pointing at
  `setup_sdk_env.sh`. Enforced in two places: `pytest_collection_modifyitems`
  (per-item marker) and module-scope fixtures
  (`sdk_version`, `plugin_gateway`, `sdk_echo_server`) call
  `pytest.skip(...)` themselves as a belt-and-braces.
* **Per-test venv python invocation:** the SDK sub-scripts
  (test 1 and test 2) run under
  `/home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin/tests/interoperability/.venv-sdk/bin/python`
  — the path printed by `setup_sdk_env.sh`.
* **What was intentionally not done (recorded in
  `tests/interoperability/README.md`):** streaming (SSE) is
  exercised implicitly through the v1.0 `SendMessage` and
  `message/stream` JSON-RPC routes but the SDK's async SSE
  consumer is not run as a separate test; `SubscribeToTask` and
  the four push-notification config operations are not exercised
  in this round (long-lived SSE consumer + extended-card path);
  gRPC is out of scope (the plugin's adapter is JSON-RPC over
  HTTP only). All three are tracked for Phase 6 (Task 13
  conformance).

### Traceability (per `docs/restart-recovery-contract.md` §9)

| Method name | v1.0 / Legacy | Response envelope | Plugin source | SDK transport |
|-------------|---------------|-------------------|---------------|---------------|
| `SendMessage` | v1.0 | `{ task \| message }` (oneof) | `adapter._rpc_message_send(..., v1_response=True)` | `JsonRpcTransport.send_message` (test 1) |
| `message/send` | legacy | bare Task | `adapter._rpc_message_send(..., v1_response=False)` | raw `httpx` (test 2) |
| `GetTask` | v1.0 | bare Task | `adapter._rpc_tasks_get` | plugin client (test 3) |
| `ListTasks` | v1.0 | `{ tasks, nextPageToken, pageSize, totalSize }` | `adapter._rpc_tasks_list` | plugin client (test 3) |
| `CancelTask` | v1.0 | bare Task or `-32002` | `adapter._rpc_tasks_cancel` | plugin client (test 3) |
| `message/stream` | legacy | SSE | `adapter._rpc_message_stream` | not exercised in this round |
| `SubscribeToTask` | v1.0 | SSE | `adapter._rpc_tasks_subscribe` | not exercised in this round |
| `tasks/pushNotificationConfig/*` | mixed | per v1.0 §5.3 | `adapter._rpc_push_config_*` | not exercised in this round |

## Known gap: cancellation does not stop execution

Cancellation is spec-compliant but best-effort in the weakest sense. Per A2A
v1.0 §3.1.5, the server "will attempt to cancel the task, but success is not
guaranteed"; the task lifecycle (`TASK_STATE_*`) is the entire contract and the
protocol never binds what the server does behind it (opaque execution, §1.1).
Here, `tasks/cancel` (adapter.py `_rpc_tasks_cancel`) transitions the task
record to `CANCELED` synchronously and resolves the pending reply future, so
the caller sees terminal state immediately. However, the inbound task was
routed into the agent's live gateway session, and cancel does not abort that
in-flight turn: the session keeps processing to completion, its eventual reply
is discarded silently, and tokens/compute burn until then.

Quality-of-implementation gap, not a compliance gap. A stronger
implementation would abort the gateway turn (gateway-side interrupt by
message/task id). Not required for conformance; flagged here so nobody
mistakes `CANCELED` for "the work stopped."

Cancellation also carries no message. `CancelTaskRequest` is id-only in the
normative proto (`a2a_cancel` sends `{"id": task_id}`); a terminal task cannot
receive further messages (`UnsupportedOperationError`), so the protocol offers
no "last words" channel inside a cancel. The caller-side alternative is
steer-before-cancel (`a2a_steer` then `a2a_cancel`): it works but is a blunt
instrument -- the steer queues a NEW turn rather than interrupting the
in-flight one, so the peer briefly runs two tasks, and the steer's own task id
is typically orphaned.

Investigation direction (chosen): synthetic steer on cancel. Since the
in-flight session is not aborted anyway, `_rpc_tasks_cancel` could inject a
synthetic steering message into the live gateway session (context_id of the
canceled task) BEFORE completing the record, giving the agent a chance to
checkpoint/wrap up. Open questions before implementing:
- Gateway behavior when a turn is enqueued for a context whose A2A task
  record is already `CANCELED` (queue injection path, turn accounting).
- Race between the synthetic steer and the original in-flight turn --
  ordering guarantees in the gateway queue.
- Whether the steer task should be recorded (ownership/direction metadata)
  and who polls its response.
- Cost tradeoff: this deliberately spends tokens to elicit a clean
  checkpoint; make it conditional (config flag) if a caller wants hard-stop
  semantics.
- Rejection semantics: if injection fails, cancel proceeds anyway (best
  effort per spec); log the failure to the audit trail.

## Known gap: historyLength > 0 does not truncate history

A2A v1.0 §3.2.4: `historyLength: 0` MUST omit the history field
(honored), and `historyLength > 0` MUST return at most N most recent
messages. The plugin honors only `0`; `> 0` is parsed but the history
field is left absent (`protocol.py` `TaskStore.to_task`). The plugin's
conversation persistence is file-based (JSONL per context), not a
`Task.history` array; `load_conversation(context_id)` exposes it to the
outbound `a2a_history` client tool, but `GetTask`/`ListTasks` JSON-RPC
responses carry no `history`. Conformance checklist row: G6 (Medium) in
`docs/a2a-conformance.md` §11. A v1.0 client depending on
`task.history` will not get one; callers in our fleet use
`a2a_history`/`a2a_get_task` instead, so this is latent for our trust
model but visible to strict external peers.

## Operational constraints

- All normal code work stays under `/home/hermes/.hermes/profiles/sven/workspace`.
- Never use `/tmp` for code repositories, edits, builds, tests, or artifacts.
- Do not overwrite Alex's default profile.
- Do not print or transfer credentials.
- Do not run a duplicate inbound A2A gateway alongside the standalone plugin.
- Keep plugin version, release tag, and clean-checkout Doctor output consistent before catalog/review handoff.
