# A2A Restart and Session Resilience Testing Plan

> For Hermes: execute this plan with subagent-driven development, using TDD for code changes and independent verification of every external test result.

Goal: Prove and, where necessary, fix durable A2A task/session behavior across gateway restart, caller restart, callee restart, process interruption, and remote-state reconciliation without confusing a caller restart with callee task failure.

Architecture: Use a temporary Hermes profile and temporary gateway instance only. The SQLite TaskStore is the durable source of truth for task metadata and state. Conversation/session persistence is tested separately from task persistence. External A2A calls use a controllable fixture peer, then one interoperability pass uses the official Python A2A SDK. No production profile, Emerald gateway, bearer token, or real remote task may be modified.

Acceptance priorities:

1. Reliability and restart recovery.
2. Compatibility with Hermes' existing A2A implementation and shared `a2a` toolset.
3. Interoperability with the official Python A2A SDK/reference implementation.
4. A2A specification conformance.

HMAC signing is out of scope. Push tests verify callback payload, task identity/state, HTTP behavior, and metrics only.

---

## Current code risks and hypotheses

The current `TaskStore` documentation says completed tasks survive restart, while persisted nonterminal records are marked failed with `[gateway restarted]`. That behavior may be correct for an inbound callee task whose worker died, but it is not automatically correct for an outbound caller record or a task owned by a remote callee. Tests must distinguish:

- caller gateway restart: local outbound task remains an unresolved remote task and must not be marked failed solely because the caller restarted;
- callee gateway restart before completion: the callee may mark its local execution failed, or recover it if the execution engine supports recovery;
- clean callee shutdown with a durable nonterminal task: reconciliation policy must be explicit and observable;
- process crash: recovery must not produce duplicate completion, overwrite a prior failure, or lose terminal artifacts;
- gateway restart after terminal completion: task and conversation records must remain queryable.

Relevant implementation areas:

- `a2a_async_plugin/protocol.py:582+` -- `TaskStore`, SQLite setup, recovery, persistence, listing, state transitions.
- `a2a_async_plugin/adapter.py:422+` -- per-adapter stores, pending waiters, watchdog, task RPCs, task finalization.
- `a2a_async_plugin/tools.py:41+`, `240+`, `573+` -- outbound task persistence, polling, await, cancellation, and local listing.
- `tests/test_plugin.py` and `tests/test_tools.py` -- current loader/registration and client coverage.
- `HANDOFF.md` -- acceptance record and final evidence.

## Test matrix and required evidence

Every scenario records: profile home, database path, gateway PID, task ID, context ID, direction, owner, peer, state before restart, state after restart, and exact API responses. Do not rely on logs or agent summaries alone. Read the SQLite database and query the live A2A API after each restart.

| Scenario | Expected result | Required evidence |
|---|---|---|
| Terminal inbound task, clean restart | Task remains terminal with identical reply/artifacts | SQLite row and `GetTask` match before/after |
| Terminal outbound task, caller restart | Local record remains terminal and metadata survives | local `a2a_list`/DB plus peer task state |
| Nonterminal outbound task, caller restart | Record remains remote/nonterminal, not failed solely due to caller restart | DB direction/ownership/remote state and post-restart `GetTask` |
| Nonterminal inbound task, callee clean restart | Explicit documented policy: recover or fail with restart reason, never silently disappear | DB transition, API state, logs |
| Nonterminal inbound task, process kill | Same explicit policy; no duplicate finalization | kill/restart evidence, DB history, task API |
| Remote task completes while caller is down | Caller reconciles to terminal state on `GetTask`/await/list recovery | remote fixture state and local state transition |
| Remote task disappears/fails | Caller records remote failure distinctly from caller restart | remote response and local metadata |
| Repeated polling/finalization | Idempotent terminal result, no overwrite or duplicate artifacts | repeated `GetTask`, DB row, metrics |
| Session/context restart | `context_id` history remains recoverable and continuation behavior is explicit | persisted conversation file and `a2a_history`/new exchange |
| Concurrent restart/query | No SQLite corruption, lock failure, or partial JSON response | integrity check, API responses, logs |

## Phase 1: Establish isolated harness

### Task 1: Add a temporary-profile test harness

Files:
- Create: `tests/recovery/conftest.py`
- Create: `tests/recovery/test_restart_recovery.py`
- Modify only if needed: `tests/conftest.py`

Build fixtures that create a temporary `HERMES_HOME`, isolated `A2A_TASKS_DB`, deterministic environment, and a gateway process with explicit startup/shutdown/kill controls. Use a local loopback port selected by the fixture. Never use `/tmp` for repository artifacts; pytest's temporary directory is acceptable for ephemeral test data only if the repository's existing test policy permits it.

Acceptance:
- Fixture can start a gateway, query its Agent Card, stop cleanly, and start again against the same profile/database.
- Fixture can terminate the process without running cleanup handlers.
- Each test reports PID, port, DB path, and profile home on failure.

### Task 2: Add a controllable fake A2A peer

Files:
- Create or modify: `tests/recovery/fake_peer.py`
- Test: `tests/recovery/test_restart_recovery.py`

Implement a loopback peer that supports Agent Card, SendMessage, GetTask, ListTasks, CancelTask, and controllable delayed completion. It must persist its own task state in a temporary SQLite/file store so the caller can restart independently.

Acceptance:
- The peer can hold a task in WORKING while the caller restarts.
- The peer can complete or fail the task after the caller is down.
- Requests and responses are captured without credentials or secrets.

Commit checkpoint: `test(a2a): add isolated restart recovery harness`.

## Phase 2: Establish baseline persistence behavior

### Task 3: Test terminal TaskStore persistence

Files:
- Test: `tests/recovery/test_taskstore_restart.py`
- Modify only if behavior is incorrect: `a2a_async_plugin/protocol.py`

Create submitted, working, completed, failed, and canceled records, close the store, reopen it, and verify IDs, context IDs, direction/ownership metadata, peer, reply, timestamps, state, and artifacts. Run SQLite integrity checking.

Acceptance:
- All terminal rows survive.
- No duplicate rows appear.
- `ListTasks` pagination and filtering agree before and after restart.
- Future-schema or malformed rows fail safely rather than being silently discarded.

### Task 4: Test durable conversation/session persistence

Files:
- Test: `tests/recovery/test_session_restart.py`
- Modify only if behavior is incorrect: `a2a_async_plugin/protocol.py`, `a2a_async_plugin/tools.py`

Persist a multi-turn context, restart the caller gateway/process, then retrieve history and continue the context. Verify message order, context ID, task ID association, and redaction behavior.

Acceptance:
- History survives process restart and compaction-like reload.
- A missing/corrupt history file produces a bounded error, not a gateway crash.
- A new context is not accidentally merged with an old one.

Commit checkpoint: `test(a2a): prove terminal task and session persistence`.

## Phase 3: Define and test restart ownership semantics

### Task 5: Write the restart state-transition contract

Files:
- Create: `docs/restart-recovery-contract.md`
- Test scaffolding: `tests/recovery/test_restart_contract.py`

Document, in exact terms, state transitions for inbound/callee and outbound/caller records. At minimum distinguish `direction`, local owner, peer, and remote state. Define whether a callee task interrupted during execution becomes FAILED, remains WORKING for reconciliation, or becomes a distinct recoverable/orphan state. Define caller restart behavior as preservation plus later reconciliation, not automatic failure.

Acceptance:
- Every matrix row has an expected state and reason.
- Terminal compare-and-set behavior is specified.
- A caller restart cannot overwrite a prior remote completion/failure.

### Task 6: Test caller restart with remote WORKING task

Files:
- Test: `tests/recovery/test_caller_restart.py`
- Modify as needed: `a2a_async_plugin/tools.py`, `a2a_async_plugin/protocol.py`

Submit an outbound task to the fake peer, force WORKING, terminate and restart only the caller, then call local list/get/await and remote GetTask. Complete the remote task and verify local reconciliation.

Acceptance:
- Local task remains queryable and nonterminal after caller restart.
- Local metadata identifies outbound direction and remote peer.
- `GetTask` and await update local state from the peer.
- A caller restart alone never creates `[gateway restarted]` failure.

### Task 7: Test callee restart and crash recovery

Files:
- Test: `tests/recovery/test_callee_restart.py`
- Modify as needed: `a2a_async_plugin/adapter.py`, `a2a_async_plugin/protocol.py`

Run inbound delayed tasks, then exercise clean stop and hard process termination. Verify the documented contract, watchdog behavior, orphan handling, and task queries after restart. Test restart while an HTTP waiter is present and after the waiter disconnects.

Acceptance:
- No task disappears.
- No duplicate terminal completion or artifact.
- Restart reason is explicit when failure is the chosen policy.
- Protected live waiters are not incorrectly orphaned by the watchdog.

Commit checkpoint: `test(a2a): cover caller and callee restart semantics`.

## Phase 4: Reconciliation, races, and failure injection

### Task 8: Test remote completion and remote failure reconciliation

Files:
- Test: `tests/recovery/test_reconciliation.py`
- Modify as needed: `a2a_async_plugin/tools.py`, `a2a_async_plugin/protocol.py`

Cover remote completion while caller is offline, remote failure, remote cancellation, remote task not found, peer timeout, malformed peer response, and peer becoming unavailable permanently.

Acceptance:
- Remote terminal state is authoritative for outbound tasks.
- A missing remote task is represented distinctly from local caller failure.
- Malformed responses do not corrupt local records.
- Bounded retries never create unbounded duplicate requests.

### Task 9: Test terminal races and idempotence

Files:
- Test: `tests/recovery/test_terminal_races.py`
- Modify as needed: `a2a_async_plugin/protocol.py`, `a2a_async_plugin/adapter.py`

Race completion, cancellation, watchdog failure, restart recovery, and remote reconciliation against the same task. Verify compare-and-set/attempt scoping so a late finalizer cannot overwrite a previous terminal failure or success.

Acceptance:
- Exactly one terminal state wins according to the contract.
- Repeated finalization is harmless.
- No reply/artifact from a losing attempt replaces the winning result.

### Task 10: Inject SQLite and process failures

Files:
- Test: `tests/recovery/test_storage_failures.py`
- Modify as needed: `a2a_async_plugin/protocol.py`

Exercise locked DB, unavailable DB, truncated DB, interrupted write, and WAL recovery using copied temporary databases. Verify errors are visible and bounded, and no state is falsely reported as completed.

Commit checkpoint: `test(a2a): verify reconciliation and terminal race safety`.

## Phase 5: Hermes compatibility and external interoperability

### Task 11: Mixed Hermes A2A compatibility test

Files:
- Test: `tests/integration/test_hermes_compatibility.py`
- Documentation: `HANDOFF.md`

Run the standalone async plugin alongside Hermes' existing synchronous A2A implementation in one isolated gateway. Verify shared `a2a` toolset registration, no duplicate tool names, Agent Card capabilities, synchronous call, asynchronous submit/get/await/cancel, and conversation continuity.

Acceptance:
- Existing synchronous Hermes A2A calls still work.
- Async tools are available in the same configured toolset.
- Inbound A2A is owned by exactly one adapter.
- Restart tests pass with both paths active.

### Task 12: Official Python SDK interoperability

Files:
- Create: `tests/interoperability/test_a2a_python_sdk.py`
- Documentation: `docs/interoperability.md`

Use the official `a2a-sdk` 1.0 client/server or a pinned reference sample. Run both directions where practical:

1. Python SDK client -> plugin server: Agent Card, SendMessage, GetTask, ListTasks, CancelTask, streaming, and push callback.
2. Plugin client -> Python SDK server: Agent Card, SendMessage, task polling, streaming, and cancellation.

Pin the SDK version and record the exact command/version. Do not add the SDK as a runtime dependency of the plugin unless required; use a test extra or isolated environment.

Acceptance:
- At least one real cross-implementation message completes.
- Task state and artifacts parse on both sides.
- Any unsupported operation is reported as a protocol error, not a hang.
- Legacy compatibility is tested separately from v1.0 behavior.

Commit checkpoint: `test(a2a): verify Hermes and Python SDK interoperability`.

## Phase 6: Spec conformance and release evidence

### Task 13: Build a conformance checklist

Files:
- Create: `docs/a2a-conformance.md`
- Test: `tests/conformance/test_a2a_conformance.py`

Map each implemented operation and Agent Card field to the A2A v1.0 normative specification and identify intentional legacy aliases. Check method names, request/response envelopes, task states, pagination, history length, artifacts, SSE termination, push payloads, authentication boundaries, and error codes.

Acceptance:
- Every supported field has a spec reference and test evidence.
- Unsupported fields/operations are explicitly documented.
- v0.3 compatibility behavior is not mislabeled as v1.0 behavior.

### Task 14: Clean checkout and acceptance run

Files:
- Documentation: `HANDOFF.md`
- Release metadata only if all prior tasks pass.

Run, in order:

```bash
PYTHONPATH=/home/hermes/.hermes/hermes-agent pytest -q
PYTHONPATH=/home/hermes/.hermes/hermes-agent pytest -q tests/recovery tests/integration tests/interoperability tests/conformance
PYTHONPATH=/home/hermes/.hermes/hermes-agent python -m compileall -q a2a_async_plugin
 git diff --check
```

Then validate a clean tagged checkout with Hermes Plugin Doctor and record exact outputs. Do not create a release tag merely because unit tests pass.

Acceptance:
- Reliability scenarios pass.
- Hermes compatibility passes.
- At least one external interoperability direction passes.
- Conformance checklist has no unexplained MUST-level gaps.
- Handoff states remaining limitations honestly.

## Risks and decisions

- Do not automatically mark every persisted nonterminal task failed on restart until direction/ownership semantics are tested. This is the central reliability risk.
- Do not use a live Emerald or Alex profile for destructive restart testing.
- Do not treat a successful process exit as a successful model/task completion. Inspect task state and actual response.
- Do not claim external interoperability from mocked HTTP tests. Use the official SDK or a real reference sample.
- Keep HMAC out of the scope unless the deployment trust model changes.
- Prefer a small deterministic fake peer for failure injection, then one real Python SDK pass for interoperability.
- Preserve all existing commits. Each checkpoint is a separate commit; do not amend or force-push.

## Definition of done

The work is complete when the isolated test suite demonstrates durable task/session behavior across caller and callee restart, remote reconciliation is explicit and idempotent, Hermes' existing A2A path remains compatible, at least one Python SDK cross-implementation exchange passes, the conformance checklist is evidence-backed, and the clean tagged checkout passes Plugin Doctor. Update `HANDOFF.md` with exact results before release handoff.
