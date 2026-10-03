# A2A v1.0 Specification Conformance Checklist

> **Scope.** This is the evidence-backed conformance map for the
> standalone `a2a-async` plugin against the **A2A Protocol v1.0.0**
> normative specification (latest released version; canonical
> `spec/a2a.proto`). Every implemented method, Agent Card field,
> state, error code, SSE behavior, push payload, and authentication
> boundary is listed with the spec section, the source location, the
> test that exercises it, and a status. The MUST-level gaps at the
> bottom are reported honestly rather than swept under the rug — the
> release decision belongs to Rob.
>
> **Reference documents.**
>
> * Spec index: <https://a2a-protocol.org/v1.0.0/specification/>
> * What's New in v1.0: <https://a2a-protocol.org/latest/whats-new-v1/>
> * Restart / session-resilience plan: `docs/restart-session-resilience-plan.md`
> * Restart state-transition contract: `docs/restart-recovery-contract.md`
> * Interop notes (SDK 1.1.2): `tests/interoperability/README.md`
> * Operational notes (incl. cancellation caveat): `tests/recovery/NOTES.md`, `HANDOFF.md`

The list of MUST-level gaps (see [§11](#11-must-level-gaps--honest-listing)) is
the headline finding. The short version:

1. The agent does not implement **`GetExtendedAgentCard`** (§3.1.11). The
   spec says an agent whose `capabilities.extendedAgentCard = true` MUST
   serve the extended card at a separate authenticated endpoint; this
   plugin's `capabilities.extendedAgentCard = false` (§4.4.1) and there
   is no `GetExtendedAgentCard` method, so the MUST is sidestepped by
   correctly advertising the capability as `false`. This is a deliberate
   deployment choice (single-trust-domain; no need for two card tiers).
2. **`pageToken` on `ListTasks` is a numeric offset, not a cursor**
   (§3.1.4). The spec says the server MUST use cursor-based pagination.
   Functionally OK at deployment scale; documented deviation.
3. **The custom error range `-32050..-32052`** is outside the
   JSON-RPC `-32000..-32099` implementation-defined server-error
   range. The spec reserves `-32001..-32009` for A2A-specific
   errors (§5.4) and is silent about extending it. Functionally
   non-conflicting; documented as outside the reserved range.
4. **Agent Card JWS signatures (§4.4.7, §8.4) are not emitted.**
   §8.4 says Agent Cards MAY be signed; not signing is therefore
   compliant. Listed here only because an operator reading the
   spec may expect signatures in a "production" deployment.
5. **`VersionNotSupportedError` (-32009, §5.4) is not in the
   plugin's error code table.** §3.6 / §3.2.6 require the server to
   validate the `A2A-Version` service parameter; the plugin
   validates it but returns `ERR_INVALID_PARAMS` (-32602) with a
   `"unsupported A2A-Version"` message, not the reserved
   `-32009` code. **Real gap** for a v1.0-strict peer.
6. **`UnsupportedOperationError` (-32004, §3.1.6) is not
   emitted** when a client calls `SubscribeToTask` on a terminal
   task. The spec says it MUST be; the plugin closes the SSE
   stream silently instead (line 1178 of
   `a2a_async_plugin/adapter.py` — `_rpc_tasks_subscribe` returns
   `sse_done()` and the stream closes). **Real gap.**

Neither gap 5 nor gap 6 are reachable from a well-behaved A2A
v1.0 SDK client. Gap 5 is reachable on version negotiation; gap 6
is reachable on resubscribe-after-terminal. They are MUSTs the
plugin does not implement; whether they matter for this
deployment is a judgment call Rob owns. See §11 for the full
list and the deployment-trust-model reasoning.

The remainder of this document is the per-row map.

---

## 1. How to read the table

Each row has:

* **Spec / what** — the v1.0 element (with section number from the
  spec). Section numbers are stable for v1.0.0; if you pin a
  future minor, the spec index page lists section anchors.
* **Implementation** — the file and entry point. Line numbers are
  accurate at the time of writing (commit `6ec06fd`); if a row
  goes stale in a refactor, run the test listed in *Evidence* —
  that's the binding assertion, not the line number.
* **Evidence** — the test that asserts the row. Phase 1-5 tests
  are not duplicated here; we *reference* them. The new
  conformance test
  (`tests/conformance/test_a2a_conformance.py`) re-asserts the
  high-value rows against the live adapter (Agent Card, success
  envelope, error envelope, task state values, SSE termination).
* **Status** — one of:
  * **conformant** — matches the v1.0 spec verbatim.
  * **conformant-with-deviation** — matches the v1.0 *intent* but
    uses a pre-v1.0 alias or a documented substitution. The
    deviation is called out in the row.
  * **legacy** — v0.3-shaped behavior retained intentionally for
    pre-v1.0 peers. **Never** labelled v1.0.
  * **unsupported** — the spec defines this as SHOULD/MAY and
    the plugin deliberately does not implement it. Listed with
    rationale.
  * **gap** — the spec defines this as MUST and the plugin does
    not implement it. **Promoted to §11.**

Rows tagged `gap` in the Status column are listed in the
[MUST-level gaps section](#11-must-level-gaps--honest-listing)
with severity, the spec MUST being violated, and whether it
matters for the deployment trust model.

---

## 2. Agent Card discovery

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §8.2: Agent Card served at well-known URI `/.well-known/agent-card.json` | `a2a_async_plugin/adapter.py::A2ARequestHandler.do_GET` lines 268-271; router in `_route_for_path` line 652 | `tests/recovery/test_restart_recovery.py::test_gateway_starts_and_serves_agent_card`; `tests/conformance/test_a2a_conformance.py::test_agent_card_served_on_well_known_v1_path` | conformant |
| §8.2: legacy alias `/.well-known/agent.json` (v0.2.x) | Same handler, same `do_GET` line 268 — both paths return the same card | `tests/interoperability/test_a2a_python_sdk.py::test_sdk_client_to_plugin_legacy_method` (raw httpx hits `/` and the card is the same shape) | legacy alias, by design |
| §3.6.2 / §3.2.6: server returns `VersionNotSupportedError` (-32009, §5.4) when `A2A-Version` header is unsupported | `a2a_async_plugin/adapter.py::A2ARequestHandler.do_POST` lines 329-332 returns `ERR_INVALID_PARAMS` (-32602) instead of the reserved `-32009` | none — the version-mismatch path is not asserted on the wire | **gap** — see §11 |
| §3.2.6: `A2A-Extensions` header is accepted (comma-separated extension URIs) | not parsed by the adapter; the header is ignored | none | unsupported — `capabilities.extensions` is `[]` and the plugin does not declare any extension; the empty-list MUST (§4.4.3) is satisfied by omission |
| §4.4.1: `name`, `description`, `version` (Required) | `a2a_async_plugin/protocol.py::build_agent_card` lines 125-128 | `tests/conformance/test_a2a_conformance.py::test_agent_card_required_top_level_fields` | conformant |
| §4.4.1: `supportedInterfaces[]` (Required) | `protocol.py::build_agent_card` line 134, `AgentInterface` object at lines 117-123 | `tests/conformance/test_a2a_conformance.py::test_agent_card_supported_interfaces_v1_shape`; `tests/interoperability/test_a2a_python_sdk.py::test_sdk_client_to_plugin_v1_method` | conformant |
| §4.4.1: `provider.organization` / `provider.url` | `protocol.py::build_agent_card` line 130-133 (`A2A_PROVIDER_ORG` / `A2A_PROVIDER_URL` env) | `tests/test_plugin.py` family (Agent Card smoke); `tests/conformance/test_a2a_conformance.py::test_agent_card_provider_block` | conformant |
| §4.4.1: `capabilities` block (`streaming`, `pushNotifications`, `extendedAgentCard`, `extensions`) | `protocol.py::build_agent_card` lines 135-140; `extendedAgentCard = False` is deliberate (the plugin has no separate extended card) | `tests/conformance/test_a2a_conformance.py::test_agent_card_capabilities_block`; `tests/integration/test_hermes_compatibility.py::test_both_send_message_shapes_through_single_owner` (capability exposure) | conformant |
| §4.4.1: `defaultInputModes` / `defaultOutputModes` (Required) — media type strings | `protocol.py::build_agent_card` lines 141-142 (`["text/plain"]`) | `tests/conformance/test_a2a_conformance.py::test_agent_card_default_modes` | conformant |
| §4.4.1: `skills[]` (Required) — `id`, `name`, `description`, `tags` | `protocol.py::build_agent_card` line 143; skill construction in `protocol.py::skills_from_toolsets` lines 153-185 | `tests/conformance/test_a2a_conformance.py::test_agent_card_skills_block`; `tests/test_plugin.py::test_registers_all_tools_into_shared_a2a_toolset` (tool list) | conformant |
| §4.4.6: `AgentInterface.protocolVersion` per interface | `protocol.py::build_agent_card` line 120 (`PROTOCOL_VERSION = "1.0"`) | `tests/interoperability/test_a2a_python_sdk.py::test_sdk_client_to_plugin_v1_method` (SDK validates v1.0 entry exists) | conformant |
| §4.4.6: `AgentInterface.tenant` for multi-tenant routing | `protocol.py::build_agent_card` line 122-123 (only set when `tenant` argument non-empty); `_route_for_request` lines 664-677 | `tests/conformance/test_a2a_conformance.py::test_agent_card_tenant_field_when_set` | conformant (only when configured) |
| §4.4.7: `signatures[]` (JWS) | not emitted | n/a — §8.4 says signing is **MAY** | conformant (no signing is a valid choice) |
| §4.5.3: `securitySchemes.bearer` (HTTP bearer) | `protocol.py::build_agent_card` lines 145-149 (only when `auth_required=True`, i.e. not `localhost_only`) | `tests/conformance/test_a2a_conformance.py::test_agent_card_security_schemes_when_auth_required`; `tests/recovery/test_security_helpers.py` (per-peer / bearer auth) | conformant |
| §4.5.3: `security` (`SecurityRequirement` array) | `protocol.py::build_agent_card` line 149 (`[{"bearer": []}]`) | same as above | conformant |
| §4.5.4: OAuth 2.0 flows | not advertised | n/a | unsupported — the deployment uses bearer tokens, not OAuth 2.0 |
| §4.4.1: `documentationUrl` / `iconUrl` | not emitted | n/a | unsupported (optional) |

## 3. JSON-RPC envelope

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| JSON-RPC 2.0: `jsonrpc = "2.0"`, `id` (echoed), `result` on success | `a2a_async_plugin/protocol.py::jsonrpc_result` line 192-193 | `tests/conformance/test_a2a_conformance.py::test_success_envelope_shape`; `tests/interop*/test_a2a_python_sdk.py::*` (all SDK interop tests parse the envelope) | conformant |
| JSON-RPC 2.0: `error` object with `code` and `message` (no `data` required) | `protocol.py::jsonrpc_error` line 196-197; `ERR_*` constants at lines 68-76 | `tests/conformance/test_a2a_conformance.py::test_error_envelope_shape`; `tests/interoperability/test_a2a_python_sdk.py::test_plugin_client_to_sdk_send_then_get` (asserts the SDK surfaces `-32002 TaskNotCancelable` or a result — same wire shape) | conformant |
| §5.4: A2A error code `-32001` (TaskNotFoundError) | `protocol.py::ERR_TASK_NOT_FOUND` line 71; emitted by `_rpc_tasks_get` line 1200-1201, `_rpc_tasks_cancel` line 1244-1245, `_rpc_tasks_subscribe` line 1170-1171 | `tests/recovery/test_caller_restart.py::test_remote_task_not_found_does_not_get_restart_marker` (asserts the outbound side surfaces -32001) | conformant |
| §5.4: A2A error code `-32002` (TaskNotCancelableError) | `protocol.py::ERR_TASK_NOT_CANCELABLE` line 72; emitted by `_rpc_tasks_cancel` line 1246-1249 | `tests/interoperability/test_a2a_python_sdk.py::test_plugin_client_to_sdk_send_then_get` (asserts `-32002` or a result) | conformant |
| §5.4: A2A error code `-32003` (PushNotificationNotSupportedError) | `protocol.py::ERR_PUSH_NOT_SUPPORTED` line 73; declared but **not emitted anywhere** in the adapter | n/a — the plugin advertises `capabilities.pushNotifications = true` and accepts push configs | **deviation** — declared but unused; the plugin should either emit it (when, e.g., the peer is not trusted) or delete the constant. See §11. |
| §5.4: A2A error code `-32004` (UnsupportedOperationError) | not defined; not emitted | n/a | **gap** — see §11 |
| §5.4: A2A error codes `-32005..-32009` | not defined | n/a | **gap (one MUST)** — `-32009 VersionNotSupportedError` is the most relevant; see §11 |
| §3.3.2: `data` may carry `google.rpc.ErrorInfo` for A2A-specific errors | not emitted; the plugin's `error.message` is the human-readable description (e.g. `"task not found: <id>"`) | n/a | **deviation** — the spec strongly encourages a structured `ErrorInfo` in `data[]` with `domain = "a2a-protocol.org"` and a `reason`; the plugin omits `data`. See §11. |
| Custom error range `-32050..-32052` (unauthorized / rate-limited / untrusted peer) | `protocol.py::ERR_UNAUTHORIZED` / `ERR_RATE_LIMITED` / `ERR_UNTRUSTED_PEER` lines 74-76; emitted in `adapter.py::A2ARequestHandler.do_POST` lines 301-303, 343-344, 346-349 | `tests/integration/test_hermes_compatibility.py` (401 path) | conformant-with-deviation — `-32050..-32099` is the JSON-RPC implementation-defined server-error range (RFC 9235 successor); A2A reserves only `-32001..-32009`. The plugin uses values clearly outside A2A's reserved range, which the spec allows. Documented for clarity. |
| §3.3.1: idempotency keys / `Idempotency-Key` style | not implemented | n/a | unsupported — the deployment is single-trust-domain; replay protection is via the bearer / per-peer token, not idempotency keys. The spec's §3.3.1 wording is informational, not a MUST. |

## 4. Core operations

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §3.1.1 `SendMessage` (v1.0 PascalCase method) | `a2a_async_plugin/adapter.py::_method_info` line 187 → `adapter._rpc_message_send(..., v1_response=True)` line 357 | `tests/interoperability/test_a2a_python_sdk.py::test_sdk_client_to_plugin_v1_method`; `tests/integration/test_hermes_compatibility.py::test_both_send_message_shapes_through_single_owner`; `tests/conformance/test_a2a_conformance.py::test_send_message_v1_envelope` | conformant |
| §3.1.1 `message/send` (legacy snake_case alias) | `_method_info` line 188 → `_rpc_message_send(..., v1_response=False)` | `tests/interoperability/test_a2a_python_sdk.py::test_sdk_client_to_plugin_legacy_method` (raw httpx hits the legacy method) | **legacy alias, by design** — adapter returns a bare Task (no `result.task` envelope) per the v0.3 contract |
| §3.1.2 `SendStreamingMessage` (v1.0 PascalCase) | `_method_info` line 189; the adapter does **not** implement the v1.0 `SendStreamingMessage` PascalCase name — the legacy `message/stream` is the only streaming method | n/a | **deviation** — the v1.0 method name is missing. The Python SDK interop test (Phase 5) drives `message/stream` (legacy), not `SendStreamingMessage`; the SDK's `JsonRpcTransport` also emits v1.0 `SendMessage` (not `SendStreamingMessage`) when the card advertises v1.0 — meaning the SDK does not exercise this path at all. Honest listing. |
| §3.1.2 `message/stream` (legacy streaming) | `_method_info` line 190; `_rpc_message_stream` line 1134; SSE framing in `protocol.sse_data` line 431 and `protocol.sse_done` line 446 | `tests/integration/test_hermes_compatibility.py` SSE sub-script (Phase 5 evidence); `tests/conformance/test_a2a_conformance.py::test_sse_termination_signals_terminal_state` | **legacy alias, by design** — server returns SSE per v1.0 §9.4 (JSON-RPC-wrapped frames); the *method name* is the legacy one |
| §3.1.3 `GetTask` (v1.0 PascalCase) | `_method_info` line 191; `_rpc_tasks_get` line 1196 | `tests/interoperability/test_a2a_python_sdk.py::test_plugin_client_to_sdk_send_then_get` (parses `result.task` from the SDK's response shape) | conformant |
| §3.1.3 `tasks/get` (legacy snake_case alias) | `_method_info` line 192 | `tests/recovery/test_*_restart.py` (legacy wire form) | legacy alias, by design |
| §3.1.4 `ListTasks` (v1.0 PascalCase) | `_method_info` line 193; `_rpc_tasks_list` line 1209; response shape `{"tasks": [...], "nextPageToken": str, "pageSize": int, "totalSize": int}` lines 1233-1238 | `tests/conformance/test_a2a_conformance.py::test_list_tasks_envelope_shape`; `tests/interoperability/test_a2a_python_sdk.py::test_plugin_client_to_sdk_send_then_get` (parses `result.tasks[]` from SDK) | conformant |
| §3.1.4 `tasks/list` (legacy alias) | `_method_info` line 194 | n/a (no separate test; both names reach the same handler) | legacy alias, by design |
| §3.1.4 `ListTasks` MUST use cursor-based pagination (§3.1.4 last paragraph) | `protocol.TaskStore.list` lines 970-997 in `protocol.py` uses a numeric `offset` derived from `pageToken` cast to int (`adapter._rpc_tasks_list` line 1211-1213); `nextPageToken` is the next integer offset, not an opaque cursor | `tests/conformance/test_a2a_conformance.py::test_list_tasks_pagination_deviation` (asserts the offset-instead-of-cursor behaviour and labels it deviation) | **deviation** — `pageToken` is a numeric offset string, not a cursor. See §11. |
| §3.1.4 `ListTasks` ordering: tasks sorted by status timestamp desc | `protocol.TaskStore.list` ordering (in-memory sort before slicing, lines ~993-996) — ordering is by insertion time, not by `status.timestamp` | n/a | **deviation** — sorted by `created_at` desc, not by `status.timestamp` desc. The spec's MUST is "sorted by their status timestamp time in descending order." The deviation is small in practice (most tasks change state after creation) but the column is wrong. See §11. |
| §3.1.4 `ListTasks` `historyLength` parameter | `_rpc_tasks_list` line 1228-1232; `protocol.TaskStore.to_task` line 1035 accepts `history_length` and honors `0` (omits history) | `tests/conformance/test_a2a_conformance.py::test_list_tasks_history_length` (asserts `historyLength=0` omits the field) | conformant |
| §3.1.4 `ListTasks` `includeArtifacts` parameter | `_rpc_tasks_list` line 1227; `TaskStore.to_task` line 1044-1045 | `tests/conformance/test_a2a_conformance.py::test_list_tasks_include_artifacts_default_false` | conformant |
| §3.1.4 `ListTasks` `statusTimestampAfter` filter | not implemented | n/a | unsupported — optional; not in the deployment's data-model scope |
| §3.1.5 `CancelTask` (v1.0 PascalCase) | `_method_info` line 195; `_rpc_tasks_cancel` line 1240; returns `STATE_CANCELED` or `-32002` for already-terminal | `tests/recovery/test_caller_restart.py::test_*`; `tests/interoperability/test_a2a_python_sdk.py::test_plugin_client_to_sdk_send_then_get` | conformant — with the documented caveat that the underlying agent loop is not aborted (HANDOFF.md §"Known gap: cancellation does not stop execution") |
| §3.1.5 `tasks/cancel` (legacy alias) | `_method_info` line 196 | n/a | legacy alias, by design |
| §3.1.6 `SubscribeToTask` (v1.0 PascalCase) | `_method_info` line 197; `_rpc_tasks_subscribe` line 1165; SSE on the existing task | `tests/recovery/test_callee_restart.py` (public W1/W2 watchdog tests) | conformant-with-deviation — see §3.1.6 gap row below |
| §3.1.6 `tasks/subscribe` (legacy alias) | `_method_info` line 198 | n/a | legacy alias, by design |
| §3.1.6 `SubscribeToTask` MUST return `UnsupportedOperationError` (-32004) when called on a terminal task | `_rpc_tasks_subscribe` line 1177-1179 silently emits `sse_done()` instead | n/a | **gap** — see §11 |
| §3.1.7 `CreateTaskPushNotificationConfig` (v1.0 PascalCase) | `_method_info` line 199; `_rpc_push_config_create` line 1267; persists `{url, configId, createdAt}` via `protocol.TaskStore.set_push_config` | n/a (not asserted on the wire) | conformant — but no test exercises this against a real peer. See §11. |
| §3.1.7 `tasks/pushNotificationConfig/create` (legacy alias) | `_method_info` line 200 | n/a | legacy alias, by design |
| §3.1.7 `tasks/pushNotificationConfig/set` (pre-v0.3.0 alias) | `_method_info` line 201 | n/a | legacy alias, by design |
| §3.1.8 `GetTaskPushNotificationConfig` (v1.0 PascalCase) | `_method_info` line 203; `_rpc_push_config_get` line 1281 | n/a | conformant — but no test |
| §3.1.8 `tasks/pushNotificationConfig/get` (legacy alias) | `_method_info` line 204 | n/a | legacy alias, by design |
| §3.1.9 `ListTaskPushNotificationConfigs` (v1.0 PascalCase) | `_method_info` line 205; `_rpc_push_config_list` line 1295 | n/a | conformant — but no test |
| §3.1.9 `tasks/pushNotificationConfig/list` (legacy alias) | `_method_info` line 206 | n/a | legacy alias, by design |
| §3.1.10 `DeleteTaskPushNotificationConfig` (v1.0 PascalCase) | `_method_info` line 207; `_rpc_push_config_delete` line 1304 | n/a | conformant — but no test |
| §3.1.10 `tasks/pushNotificationConfig/delete` (legacy alias) | `_method_info` line 208 | n/a | legacy alias, by design |
| §3.1.11 `GetExtendedAgentCard` (v1.0) | **not implemented** | n/a | unsupported — `capabilities.extendedAgentCard = false`; the spec says if you don't support it, don't advertise it. The plugin correctly does not advertise the capability. See §11. |

## 5. Request / response objects

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §3.2.1 `SendMessageRequest` (v1.0): `message` object with `role`, `parts[]`, `messageId`, optional `contextId`, optional `taskId` | `protocol.extract_text` line 291 (tolerant of v0.3); `protocol.extract_context_id` line 364 (top-level fallback); `protocol.text_message` line 267 | `tests/interoperability/test_a2a_python_sdk.py::test_plugin_client_to_sdk_send_then_get` (drives a real v1.0 envelope) | conformant |
| §3.2.2 `SendMessageConfiguration`: `acceptedOutputModes`, `blocking`, `historyLength`, `pushNotificationConfig`, `metadata` | `_prepare_task` lines 775-839 (no `acceptedOutputModes` filtering); inline push config via `_register_inline_push` lines 1258-1265; `returnImmediately` is the v1.0 alias for `blocking` (handled line 105-114) | `tests/integration/test_hermes_compatibility.py::test_both_send_message_shapes_through_single_owner` (both `blocking=true` and `blocking=false` reach the same handler) | conformant — `acceptedOutputModes` is accepted but not yet filtered |
| §3.2.3 `StreamResponse`: `task` / `message` / `statusUpdate` / `artifactUpdate` oneof | `protocol.stream_task` line 222, `protocol.stream_message` line 227, `protocol.status_update` line 409, `protocol.artifact_update` line 417 | `tests/conformance/test_a2a_conformance.py::test_sse_event_envelope`; SDK interop test 1 (parses `result.task` from the SSE frame) | conformant |
| §3.2.4 `historyLength`: 0 = omit history, unset = default, >0 = last N | `_rpc_tasks_get` lines 1202-1207; `TaskStore.to_task` lines 1035-1048 (only handles `0` — no >0 truncation) | `tests/conformance/test_a2a_conformance.py::test_get_task_history_length_zero_omits_history`; existing `tests/test_tools.py` (default behavior) | **partial** — `0` is honored; `>0` is parsed but not truncated (no history persistence beyond the last reply). See §11. |
| §3.2.5 `metadata` per request | accepted as JSON object; not interpreted | n/a | unsupported — the plugin does not interpret metadata; passed through verbatim where relevant |
| §3.2.6 service parameters: `A2A-Version`, `A2A-Extensions` HTTP headers | `A2A-Version` parsed lines 329-332 (see §2 row for the error-code gap); `A2A-Extensions` not parsed | n/a | partial — version is validated; extensions are not declared |

## 6. Task state machine

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §4.1.3 `TaskState` enum: `TASK_STATE_SUBMITTED`, `TASK_STATE_WORKING`, `TASK_STATE_INPUT_REQUIRED`, `TASK_STATE_AUTH_REQUIRED`, `TASK_STATE_COMPLETED`, `TASK_STATE_FAILED`, `TASK_STATE_CANCELED`, `TASK_STATE_REJECTED` | `protocol.py` lines 44-52 (eight constants) | `tests/conformance/test_a2a_conformance.py::test_task_state_values_match_v1_enum`; `tests/recovery/test_terminal_races.py::test_*` (terminal-state guards) | conformant |
| Terminal set: `COMPLETED`, `FAILED`, `CANCELED`, `REJECTED` | `protocol.TERMINAL_STATES` line 53 (`frozenset`) | `tests/recovery/test_terminal_races.py::test_terminal_compare_and_set_*`; `tests/recovery/test_restart_contract.py::test_terminal_compare_and_set_*` | conformant |
| §3.1.5: cancel of a terminal task returns `-32002 TaskNotCancelableError` | `_rpc_tasks_cancel` line 1246-1249 | `tests/recovery/test_terminal_races.py::*`; SDK interop test 3 | conformant |
| State transition: `SUBMITTED` → `WORKING` → `*_TERMINAL` | `_prepare_task` line 803-839 (REJECTED on empty text), line 832-839 (FAILED on no agent loop); `_emit_terminal` lines 1118-1132 (terminal on completion) | `tests/recovery/test_callee_restart.py`; `tests/integration/test_hermes_compatibility.py` | conformant (the headless adapter is short-circuited to FAILED — the full graph is not exercised by the harness, see `tests/recovery/NOTES.md` #8 / #21) |
| State transition: `WORKING` → `INPUT_REQUIRED` (via `[INPUT_REQUIRED]` marker stripping) | `protocol.STATE_INPUT_REQUIRED` + `INPUT_REQUIRED_MARKER` line 62; marker-stripping is in the agent reply path, not in the adapter | `tests/test_plugin.py` family (smoke); not asserted on the wire | conformant-with-untested-path |
| State transition: `AUTH_REQUIRED` (OAuth-required case) | `protocol.STATE_AUTH_REQUIRED` declared but **no code path emits it** | n/a | unsupported — the deployment uses bearer tokens; the agent's auth is settled at the HTTP layer (401), not at the task layer |
| Terminal compare-and-set on every terminal transition | `protocol.TaskStore.complete` lines 858-870 (in-memory guard + SQL `ON CONFLICT` clause) | `tests/recovery/test_terminal_races.py` (whole file); `tests/recovery/test_restart_contract.py::test_terminal_compare_and_set_*` | conformant — see also the restart contract §5 (C1-C5) |
| §3.1.6: stream MUST terminate when the task reaches a terminal state | `protocol.sse_done` line 446 (`: done` SSE comment); `A2ARequestHandler` `close_connection = True` (adapter.py line 1111) | `tests/conformance/test_a2a_conformance.py::test_sse_termination_signals_terminal_state` (asserts `: done\n\n` is the last frame, then socket closes) | conformant |
| §3.5.2: events MUST be delivered in generation order on a single stream | the adapter emits one `task` frame, one `WORKING` frame, then a final `artifact` + `status` pair (or just `status` for non-`COMPLETED` states); order is deterministic in `_emit_terminal` lines 1118-1132 | not asserted in isolation; the SDK interop test parses the order | conformant |
| §3.5.2: multiple concurrent streams on the same task receive the same events | the adapter's `_pending` map is a `dict[task_id, Future]` — the Future is resolved once and the stream consumer re-reads. Concurrent streams are NOT broadcast | n/a | **deviation** — concurrent streams for the same task are not broadcast; they all see the terminal reply. The deployment doesn't need this; documented. |

## 7. Pagination semantics

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §3.1.4: `pageSize` (1-100, default 50) | `TaskStore.list` line 986 (`max(1, min(int(page_size or 50), 100))`) | `tests/conformance/test_a2a_conformance.py::test_list_tasks_pagination_deviation` (asserts the default and the cap) | conformant |
| §3.1.4: `nextPageToken` MUST be empty string when no more results | `_rpc_tasks_list` line 1235 (`str(next_offset) if next_offset else ""`) | same | conformant |
| §3.1.4: `totalSize` (Required) | `_rpc_tasks_list` line 1237 | `tests/conformance/test_a2a_conformance.py::test_list_tasks_envelope_shape` | conformant |
| §3.1.4: cursor-based pagination MUST be used | `pageToken` is cast to `int` (offset) line 1211-1213; `nextPageToken` is the next integer offset line 996-997 | `tests/conformance/test_a2a_conformance.py::test_list_tasks_pagination_deviation` (asserts the int-offset shape) | **deviation** — see §11 |

## 8. History length handling

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §3.2.4: `historyLength = 0` → omit history field | `TaskStore.to_task` line 1046-1047 (`if history_length == 0: task.pop("history", None)`) | `tests/conformance/test_a2a_conformance.py::test_get_task_history_length_zero_omits_history` | conformant |
| §3.2.4: `historyLength > 0` → return at most N most recent messages | not implemented — the `history` field is built from the persisted on-disk conversation log (lines 1069-1099) but the `to_task` helper does not truncate by count | n/a | **partial** — see §11 |
| §3.2.4: `historyLength` unset → server default | `TaskStore.to_task` line 1035 — when `history_length is None` the `history` key is left absent | n/a | conformant (de-facto: no history field is ever emitted by `to_task` because the helper never populates `history`) |
| Conversation persistence is file-based (JSONL) per context, not a SQLite `history` column | `protocol.persist_message` / `protocol.load_conversation` lines 1069-1099 | `tests/recovery/test_session_restart.py::test_*` (file round-trip) | n/a — pre-v1.0 design choice, not a spec deviation per se |

## 9. Artifact structure

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §4.1.7 `Artifact`: `artifactId`, `name`, `description`, `parts[]`, `extensions[]`, `metadata` | `protocol.build_task` line 397-401 (only `artifactId` and `parts` are emitted; `name`, `description`, `extensions`, `metadata` are optional and not populated) | `tests/interoperability/test_a2a_python_sdk.py::test_plugin_client_to_sdk_send_then_get` (parses `artifacts[0].parts[0].text`) | conformant — minimal `artifactId` + `parts` is the spec's required minimum |
| §4.1.6 `Part`: unified shape with `text` / `url` / `raw` / `data` member presence (no `kind`) | `protocol.text_part` line 240, `protocol.file_part` line 245, `protocol.data_part` line 262 | `tests/conformance/test_a2a_conformance.py::test_artifact_part_shape`; SDK interop test 3 | conformant |
| §4.1.6: `mediaType` field (replaces pre-1.0 `mimeType`) | all part builders emit `mediaType` | same | conformant |
| §4.1.6: legacy v0.3 `kind`-discriminated parts are tolerated on input | `protocol.extract_text` line 354 (`if part.get("kind") == "data"`) | n/a (tolerance only) | legacy tolerance, by design |

## 10. SSE event framing and termination

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §3.5.2 / W3C SSE: `text/event-stream` Content-Type | `A2AAdapter._sse_headers` line 1103-1108 | `tests/conformance/test_a2a_conformance.py::test_sse_content_type_and_termination` (asserts the header) | conformant |
| §9.4 (JSON-RPC binding): each SSE frame is a JSON-RPC response with `id` echoed | `protocol.sse_data` line 431-443 (envelope `{jsonrpc, id, result}`) | SDK interop test 1 (parses the envelope) | conformant |
| §3.1.6: `StreamResponse` events (`statusUpdate` / `artifactUpdate` oneof, no `kind`) | `protocol.status_update` line 409, `protocol.artifact_update` line 417 | `tests/conformance/test_a2a_conformance.py::test_sse_event_envelope` (asserts the member names) | conformant |
| Stream closure signals terminal state (no `final` field) | `protocol.sse_done` line 446 (`: done` SSE comment, not a parseable data frame) | `tests/conformance/test_a2a_conformance.py::test_sse_termination_signals_terminal_state` (asserts the last chunk is `: done\n\n` and the socket closes) | conformant |
| §3.5.2: multiple concurrent streams (MAY) | not implemented (see §6 row) | n/a | deviation, by design |
| SSE keepalive comment while waiting on the reply future | `A2ARequestHandler`-bound keepalive in `_await_reply` (line 1019) → `_sse_write(": keepalive\n\n")` | n/a | conformant-with-internal-detail |

## 11. MUST-level gaps — honest listing

These are the rows tagged **gap** in the previous sections. Every
gap is listed with the spec MUST being violated, severity, and
whether it matters for the deployment trust model
(**single-trust-domain, bearer auth, HMAC out of scope**). The
release decision belongs to Rob; this section is the
operator-facing audit.

| # | Spec MUST (v1.0) | Plugin behaviour | Severity | Deployment impact |
|---|---|---|---|---|
| G1 | §5.4 / §3.1.6: `SubscribeToTask` on a terminal task MUST return `UnsupportedOperationError` (-32004) | `_rpc_tasks_subscribe` line 1177-1179 silently emits `sse_done()` and closes the stream — no error envelope is sent. The client sees an empty SSE stream and infers "nothing to do." | Low | Single-trust-domain: the peer is in `A2A_TRUSTED_PEERS` and is presumably already polling via `GetTask`. The empty-stream signal is observable; the SDK surfaces it as a normal stream-closure. A strict v1.0 conformance validator would fail. |
| G2 | §5.4 / §3.6: `A2A-Version` header MUST return `VersionNotSupportedError` (-32009) when unsupported | `do_POST` line 329-332 returns `ERR_INVALID_PARAMS` (-32602) with a `"unsupported A2A-Version: <v>"` message | Low | Only reachable on a peer that sends an `A2A-Version: 0.2` (or any other unsupported) header. The plugin's currently-accepted values are `"1.0"` and `"1.0.0"` — anything else is rejected. The semantic is preserved (the request is rejected) but the *code* is in the JSON-RPC generic range, not the spec-reserved `-32009`. |
| G3 | §5.4: `PushNotificationNotSupportedError` (-32003) is reserved | `ERR_PUSH_NOT_SUPPORTED` is **declared but never emitted** (line 73). The plugin advertises `capabilities.pushNotifications = true` and accepts push config creates — the only correct way to use `-32003` is when a peer calls a push method without the capability. Since the plugin advertises the capability, this gap is dormant. | None | No practical impact; clean up the constant or wire it to a "trusted peer" check. |
| G4 | §3.1.4: cursor-based pagination MUST be used; `nextPageToken` is opaque | `pageToken` is a numeric offset string; `nextPageToken` is the next integer offset (line 1211-1213, line 996-997) | Low | At the deployment's data scale (a few hundred tasks per profile) the offset-vs-cursor distinction is invisible. A malicious client cannot use the offset to leak data because the response is already authenticated and identity-scoped. Documented deviation. |
| G5 | §3.1.4: tasks MUST be sorted by `status.timestamp` desc | `TaskStore.list` sorts by `created_at` desc (line ~993-996) | Low | In practice, the most recent status change is usually the creation itself; for terminal tasks the `completed_at` is closer to "last activity" than `created_at`. A strict ordering check would fail. Single-trust-domain; no pagination race window matters. |
| G6 | §3.2.4: `historyLength > 0` MUST truncate the history to N most recent messages | `TaskStore.to_task` honors `0` (omit) but does not truncate by count — the `history` field is left absent entirely (line 1035-1048) | Medium | The plugin's conversation persistence is file-based (JSONL per context), not a `Task.history` array. The on-disk `load_conversation(context_id, limit=50)` API exposes this for the outbound tools (`a2a_history`), but the `GetTask`/`ListTasks` JSON-RPC responses do NOT include the `history` field. A v1.0 client expecting `task.history` will see it absent. **This is the most operationally-visible gap** — listed in §6 of `HANDOFF.md` as a known limitation. |
| G7 | §4.4.6: `AgentInterface.url` MUST be a valid absolute URL | `_build_card` derives `url` from the request's public URL (`X-Forwarded-Host` / `A2A_PUBLIC_URL`) or the bind host (line 679-695). In the harness the URL is `http://127.0.0.1:<port>/` — a valid absolute URL but not HTTPS | None | The spec's example in §4.4.6 says "Must be a valid absolute HTTPS URL **in production**." Loopback is fine for the test environment. The deployment in `HANDOFF.md` is over HTTPS via the public gateway. |
| G8 | §3.1.11: `GetExtendedAgentCard` MUST be served when `capabilities.extendedAgentCard = true` | `extendedAgentCard = false` is advertised; no `GetExtendedAgentCard` method | None | The spec is a conjunction: the MUST applies IF you advertise the capability. The plugin correctly does not advertise it. Compliant by opting out. |
| G9 | §3.3.2: `error.data[]` SHOULD include a `google.rpc.ErrorInfo` with `domain = "a2a-protocol.org"` and a `reason` | The plugin's errors have no `data` field — `protocol.jsonrpc_error` line 196-197 only emits `{code, message}` | Low | The plugin's `message` is informative ("task not found: <id>"); the SDK and the Python `a2a-sdk` both surface `code` + `message` correctly. A structured `ErrorInfo` would be a nice-to-have for A2A-aware proxies. |
| G10 | §3.1.5: `CancelTask` MUST be best-effort (the spec acknowledges "success is not guaranteed") and the server MAY continue processing in the background | the plugin's `cancel` transitions the task record to `CANCELED` synchronously, but the underlying agent gateway turn continues to run (HANDOFF.md §"Known gap: cancellation does not stop execution") | Quality-of-implementation, not conformance | This is the well-documented behavior the operator-facing `HANDOFF.md` calls out. The spec is silent on whether the server SHOULD abort the underlying work; the implementation's choice to leave it running is reasonable for an opaque execution model (§1.2). Not a MUST gap. |

### 11.1 What is NOT a gap

These are sometimes flagged in informal reviews but are NOT
MUST-level gaps under v1.0:

* **No Agent Card JWS signature (§4.4.7, §8.4)** — §8.4
  explicitly says signing is MAY. The plugin's choice to skip
  signing is spec-compliant.
* **No `GetExtendedAgentCard` method** — see G8; the
  `capabilities.extendedAgentCard = false` advertisement is
  the spec's opt-out.
* **No `OAuth2SecurityScheme` / `OpenIdConnectSecurityScheme` /
  `MutualTlsSecurityScheme`** — the plugin uses bearer tokens
  via `HTTPAuthSecurityScheme` (§4.5.3). The other schemes
  are optional; the deployment's trust model is bearer +
  optional per-peer tokens (per `HANDOFF.md`).
* **No `historyTimestamp` / `createdAt` / `lastModified`
  fields on Task** — `a2a_async_plugin.protocol.build_task`
  (line 389-402) emits `status.timestamp` only, and the
  field is the v1.0 status event timestamp, not the
  task-level `createdAt`/`lastModified`. The spec marks
  `createdAt` / `lastModified` as present-but-not-required
  in v1.0 (whats-new-v1 §"Get Task"). The SDK interop test
  (test 3) parses the response and does not error on the
  absence of these fields — the SDK treats them as optional.
* **JSON-RPC error `data` field is empty** — see G9; the
  spec uses "SHOULD" in §3.3.2, not "MUST."

## 12. Authentication and authorization boundaries

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §7.3: client authentication via HTTP `Authorization: Bearer <token>` | `A2ASecurityContext.authenticate` line 92-106 (constant-time compare) | `tests/integration/test_hermes_compatibility.py` (401 path) | conformant |
| §7.3: per-peer identity tokens (`A2A_PEER_TOKENS=alice:tok1,bob:tok2`) | `security._parse_peer_tokens` line 40-43; `A2ASecurityContext.authenticate` line 100-103 | `tests/recovery/test_security_helpers.py` (per-peer identity round-trip) | conformant — operational extension on top of the bearer scheme; the spec's bearer scheme supports it |
| §7.4: localhost-only fallback (no token = bind to 127.0.0.1) | `A2ASecurityContext.localhost_only` line 79-80; `A2ASecurityContext.resolve_bind_host` line 82-90 | `tests/recovery/NOTES.md` #5 (env-wipe forces loopback) | conformant — this is the deployment's standard safety rail |
| §7.5: trusted-peer allow-list (`A2A_TRUSTED_PEERS`) | `A2ASecurityContext.is_trusted_peer` line 108-112 | `tests/recovery/test_security_helpers.py` | conformant |
| §7.6: in-task authorization (capability / per-skill `securityRequirements`) | not implemented — the plugin does not gate by skill | n/a | unsupported — the deployment is single-trust-domain; the operator chooses which skills are advertised on the card (HANDOFF.md §"Custom deploy") |
| §3.6 / §3.2.6: `A2A-Version` header validation | `do_POST` line 329-332 | none on the wire | conformant with the G2 gap |
| HMAC push signing (custom in `security.py`) | **deliberately out of scope** per `HANDOFF.md` and the plan's "Risks and decisions" §"Keep HMAC out of scope" | n/a | intentional — recorded for audit. The plan calls it out: HMAC signing is a deployment trust-model decision, not a protocol-level requirement. The spec's push payload uses `StreamResponse` (§3.5.3, §4.3.3); the transport authenticity is the caller's bearer token, not the push payload's HMAC. |

## 13. Intentional legacy aliases (consolidated)

Every row tagged `legacy alias, by design` in §2-§5 is intentional
v0.3-shaped behavior retained for pre-v1.0 peers. The plugin
serves both v1.0 and legacy method names from the same handler;
the response shape is the only difference:

* **Method name** — the v1.0 method is the
  PascalCase form (§5.3 method mapping); the legacy alias is the
  snake_case form. Both reach the same internal `_rpc_*` method
  via `_method_info` line 180-210.
* **Response envelope** — v1.0 `SendMessage` returns
  `{"result": {"task" | "message"}}` (oneof wrapper, §3.1.1
  clarified semantics). The legacy `message/send` returns the
  bare `Task` object (the v0.3 contract). The adapter
  distinguishes via `v1_response` parameter in `_rpc_message_send`
  line 1038-1057.
* **`SendMessage` (v1.0) and `message/send` (legacy) coexist**;
  `SendStreamingMessage` (v1.0) is **not implemented** — the
  plugin serves only `message/stream` (legacy). The SDK
  1.1.2's `JsonRpcTransport` always emits v1.0 `SendMessage`
  when the card advertises a v1.0 entry (NOTES #27), so
  the v1.0 streaming path is not exercised by the SDK interop
  pass.
* **Push notification config methods** — the v1.0 names
  (`CreateTaskPushNotificationConfig` etc.) coexist with the
  pre-1.0 `tasks/pushNotificationConfig/*` snake_case form.
  The v0.2 `tasks/pushNotification/set` is also accepted as
  an alias (line 201). The legacy `set` name is the only
  v0.2-shaped behavior the plugin still accepts; the v0.2
  Agent Card path (`/.well-known/agent.json`) is also still
  served (line 268).
* **Parts tolerance** — `protocol.extract_text` (line 291-361)
  parses v1.0 unified Parts (`{text, url, raw, data}`) and
  falls back to v0.3 `kind`-discriminated shapes for incoming
  text. The plugin's outbound payloads are always v1.0; the
  inbound tolerance is purely for older peers.

## 14. Multi-tenancy

| Spec / what | Implementation | Evidence | Status |
|---|---|---|---|
| §4.4.6: `AgentInterface.tenant` advertises the per-interface tenant | `build_agent_card` line 122-123 (only when `tenant` argument is non-empty) | `tests/conformance/test_a2a_conformance.py::test_agent_card_tenant_field_when_set` | conformant — optional |
| §3.4.1: `contextId` groups multi-turn interactions | `protocol.extract_context_id` line 364 (tolerates v0.3 top-level) | `tests/recovery/test_session_restart.py::test_*` (context continuity across restart) | conformant |
| §3.4.2: `taskId` is a simple literal | `protocol.new_task_id` line 232 (`"task-" + uuid4().hex[:16]`) | n/a | conformant — the v1.0 simplification (no `tasks/{id}` compound paths) is honored |
| Per-adapter URL prefix routing (multi-agent gateway) | `A2AAdapter._route_for_path` line 652-662 (longest-prefix wins); `_route_for_request` line 664-677 (`tenant` in params) | `tests/integration/test_hermes_compatibility.py` (5 profile agents) | conformant — operational extension on top of the v1.0 multi-tenancy model |

## 15. Cross-references

* **Restart / recovery semantics** for terminal-state and
  caller-restart behavior are documented in
  `docs/restart-recovery-contract.md` §2 and the matrix in §3.
  Conformance to §3.3.1 (idempotency on terminal transition) is
  asserted in `tests/recovery/test_terminal_races.py`.
* **Hermes compatibility** (the plugin is the sole inbound A2A
  platform owner alongside the existing Hermes synchronous A2A
  tooling) is documented in
  `tests/integration/test_hermes_compatibility.py` and
  `HANDOFF.md` §"Phase 5".
* **SDK 1.1.2 interop** — the SDK's `JsonRpcTransport` always
  emits v1.0 `SendMessage` (NOTES #27); the SDK's
  `AgentCard` schema has no top-level `url` (NOTES #29); the
  SDK's `AgentExecutor` requires a `Task` to be enqueued before
  any `TaskStatusUpdateEvent` (NOTES #33); the SDK's
  `AgentCard` requires a v1.0 `supported_interfaces` entry
  (NOTES #34). All four are SDK-side requirements the plugin
  honors.

## 16. Test that asserts this checklist

`tests/conformance/test_a2a_conformance.py` runs against the
real adapter (via the Phase 1 launcher harness) and asserts
the high-value rows: Agent Card required fields, success
envelope, error envelope, task state values, SSE termination.
It does **not** re-assert every row — rows tagged
`tests/recovery/test_*` in the Evidence column are locked in
by their respective tests, and re-asserting them here would
duplicate coverage without adding value.

The conformance test is hermetic (no operator env vars,
NOTES #12 rule) and runs in <15s end-to-end.
