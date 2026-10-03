# A2A Restart and Session Resilience — State-Transition Contract

> Authoritative source of truth for the restart / recovery semantics the
> A2A plugin MUST honour. The tests in
> `tests/recovery/test_restart_contract.py`,
> `tests/recovery/test_caller_restart.py`, and
> `tests/recovery/test_callee_restart.py` assert the clauses below
> against the real `protocol.TaskStore`, the real
> `a2a_async_plugin.adapter.A2AAdapter`, and the real outbound client
> tools in `a2a_async_plugin.tools`.

## 0. Vocabulary

* **direction** (SQLite column `a2a_tasks.direction`): `inbound` — this
  gateway received the task from a peer; `outbound` — this gateway
  initiated the task to a peer.
* **ownership** (`a2a_tasks.ownership`): `local` — the durable
  execution state lives in this gateway's process; `remote` — the
  durable state lives at a peer (the gateway only mirrors it for
  queryability).
* **remote_state** (`a2a_tasks.remote_state`): for `direction=outbound`
  rows, the last state the peer reported (e.g. `TASK_STATE_WORKING`,
  `TASK_STATE_COMPLETED`). For `direction=inbound` rows this column is
  unused and remains empty.
* **local owner**: the process whose crash/restart we are talking
  about. There is exactly one such process per restart scenario —
  either the *caller* gateway (outbound) or the *callee* gateway
  (inbound).
* **peer**: the remote A2A agent on the other side of the wire. The
  peer's state is authoritative for outbound records; the local owner's
  state is authoritative for inbound records.
* **terminal state**: one of `TASK_STATE_COMPLETED`,
  `TASK_STATE_FAILED`, `TASK_STATE_CANCELED`, `TASK_STATE_REJECTED`
  (see `protocol.TERMINAL_STATES`).

## 1. Why a contract

The Phase 2 baseline tests (`tests/recovery/test_taskstore_restart.py`,
`test_session_restart.py`) lock in durability of terminal rows and
sessions. They explicitly do NOT touch the policy that converts
non-terminal inbound rows to `STATE_FAILED` on reopen.

That policy was correct for the original use case (a callee whose
worker died mid-task: the gateway did the safe thing and gave up) but
it is NOT correct for outbound/caller-owned tasks. The
`docs/restart-session-resilience-plan.md` plan calls this out as the
**central reliability risk** (lines 22–28, 277–280).

This contract separates the two directions and states, in exact terms,
what the plugin MUST do on each restart scenario.

## 2. The matrix

| # | Scenario | Local direction / ownership | Expected DB / API state after restart |
|---|--------------|----------------------------|-------------------------------------|
| 1 | Terminal inbound task, clean gateway restart | `inbound` / `local` | Identical terminal state, reply, artifacts, `completed_at`. |
| 2 | Terminal outbound task, caller restart | `outbound` / `remote` | Identical terminal state, reply, artifacts, `completed_at`. |
| 3 | Non-terminal outbound task, caller clean restart | `outbound` / `remote` | **`state` and `remote_state` unchanged.** `reply` empty. `completed_at` not backfilled. Caller can re-query the peer and reconcile. |
| 4 | Non-terminal outbound task, caller hard-kill restart | `outbound` / `remote` | Same as row 3. A hard kill must not be distinguishable in DB rows. |
| 5 | Non-terminal outbound task, caller restart, peer completes while caller is down | `outbound` / `remote` | DB row unchanged at restart moment; subsequent `GetTask`/`a2a_get_task`/`a2a_await` reconciles to terminal.** |
| 7 | Non-terminal inbound task, callee clean restart | `inbound` / `local` | API state (`TaskStore.get` in the restarted process) returns `state = TASK_STATE_FAILED`, `reply = "[gateway restarted before task completed]"`, `completed_at` set. **The on-disk SQLite row keeps its persisted state/reply** (the conversion is in-memory only — `tests/recovery/NOTES.md` #15). **No silent recovery; the restart reason is explicit in the in-memory API.** |
| 8 | Non-terminal inbound task, callee hard-kill restart | `inbound` / `local` | Same as row 7 (hard-kill is indistinguishable from clean restart for non-terminal inbound rows). |
| 9 | Remote task disappears (peer says "not found") | `outbound` / `remote` | Local row marked `TASK_STATE_FAILED` with a peer-specific reply; NOT the `[gateway restarted]` marker. |
| 10 | Repeated `finalize` attempts on an already-terminal record | any | Compare-and-set: the second attempt is a no-op; no DB drift, no metric drift, no watcher leak. |
| 11 | Caller restart AFTER peer remote completion | `outbound` / `remote` | The prior terminal state survives reopen; a caller restart CANNOT overwrite a prior terminal. |

\*\** "Reconciles to terminal" means the next call to `a2a_get_task`,
`a2a_await`, or `GetTask` against the peer drives `complete()` (terminal
compare-and-set on the local row) and reflects the peer's terminal
state. The protocol behaviour for this is in §3.

## 3. Caller-side reconciliation rule

For an outbound task whose `state` is non-terminal at restart time
(row 3/4), the local row is preserved verbatim. The next local
operation against the task that hits the peer (`a2a_get_task`,
`a2a_await`, `a2a_cancel`) issues a fresh `GetTask` / `CancelTask`
RPC and:

* if the peer reports a non-terminal state, the local `remote_state`
  is updated via `TaskStore.set_state` (which mirrors to SQLite via
  `_persist`);
* if the peer reports a terminal state, the local row is terminalised
  via `TaskStore.complete` (terminal compare-and-set: once terminal,
  the row cannot be overwritten by a later attempt).

If the peer says "task not found" (`-32001`), the local row is
transitioned to `TASK_STATE_FAILED` with a peer-specific reply
distinguishable from `[gateway restarted]`.

## 4. Callee-side restart policy (chosen)

For a non-terminal inbound task at restart (rows 7, 8), the plugin's
chosen policy is **fail with explicit restart reason**, not silent
recovery. Concretely:

* `state` becomes `TASK_STATE_FAILED`
* `reply` becomes `[gateway restarted before task completed]` (in-memory only; the on-disk row keeps its persisted `reply` — see §C5)
* `completed_at` is set in-memory to the wall-clock time at reopen (NOT persisted)
* All other durable fields (`task_id`, `context_id`, `peer`,
  `agent_slug`, `tenant`, `direction`, `ownership`, `remote_state`,
  `created_at`, `created_iso`) are preserved

This is the implementation today
(`a2a_async_plugin/protocol.py::_recover_from_db`, lines 729–734). The
test `tests/recovery/test_taskstore_restart.py::test_nonterminal_inbound_marked_failed_on_reopen`
already locks it in at the TaskStore level.

The contract test (`test_restart_contract.py`) re-asserts this
clause against the real subprocess restart path (the Phase 1 launcher
harness), so the policy cannot silently regress in a refactor.

### 4.1. Why "fail with marker" not "recover"

Recovery would require the plugin's execution engine to be
deterministic and idempotent across process boundaries (the engine
would have to re-run the tool calls the prior process was mid-way
through without double-executing irreversible side effects). The
plugin's headless launcher has no execution engine at all (see
`tests/recovery/NOTES.md` #8), and the engine the real gateway uses is
not designed for that property. Marking the task FAILED with an
explicit, greppable reason is the bounded, observable policy that
matches the rest of the contract.

### 4.2. Hard-kill vs. clean restart

Hard-kill (SIGKILL) and clean restart (SIGTERM + `disconnect()`) are
NOT distinguishable in DB rows for non-terminal inbound tasks. Both
produce the same FAILED + marker state. The audit trail of WHICH
process exited HOW lives in the gateway log, not the DB — and that is
acceptable: the marker in `reply` is the operator-facing signal; the
log is the on-call-facing signal.

## 5. Terminal compare-and-set rules

A "terminal transition" is any write that takes a record from a
non-terminal state (`SUBMITTED`, `WORKING`, `INPUT_REQUIRED`,
`AUTH_REQUIRED`) to a terminal state (`COMPLETED`, `FAILED`,
`CANCELED`, `REJECTED`).

The following rules apply to ALL transitions, inbound or outbound:

* **C1.** A terminal transition only succeeds when the row's current
  state is non-terminal. `TaskStore.complete()` enforces this — the
  second terminal attempt returns `None` and does not modify the row.
* **C2.** A late finalizer CANNOT overwrite a prior terminal state.
  After row R has reached state `COMPLETED`, attempts to mark it
  `FAILED` are no-ops. This is the rule that protects a peer-reported
  `COMPLETED` from being clobbered by a later `CANCELED` (e.g. the
  caller's local `a2a_cancel` after the peer already replied).
* **C3.** Exactly one terminal state wins per record. The winning
  state is whichever terminal transition completes first (compare-and-
  set at the row level, with `state` as the column that arbitrates).
* **C4.** A caller restart CANNOT overwrite a prior terminal. The
  persistence layer (`TaskStore._persist`) is `ON CONFLICT(task_id) DO
  UPDATE` on `(state, reply, completed_at, direction, ownership,
  remote_state)` — but `set_state` and `complete` both gate the write
  on the in-memory check "is current state terminal?" and refuse to
  overwrite. The `_recover_from_db` path treats persisted terminal
  rows as ground truth.
* **C5.** A non-terminal inbound reopen converts the row to FAILED
  with the restart marker ONCE per reopen (per-process). The
  conversion updates the in-memory record but does not re-persist;
  the on-disk `reply` field is NOT rewritten by subsequent reopens.
  (This is a known gap — `tests/recovery/NOTES.md` #15 — and is
  acceptable because the marker is identifiable regardless of which
  reopen wrote the in-memory copy.)

## 6. Watchdog / orphan-protection rules

The adapter runs a background watchdog that fails tasks older than
`a2a_async_plugin.adapter._ORPHAN_TIMEOUT` (300 s) that have no live
HTTP waiter / live `_pending` future
(`adapter._watchdog_loop` calling `TaskStore.fail_orphans`).

* **W1.** A task with a live HTTP waiter (the caller's blocking HTTP
  thread is still in `_pending[task_id]`) is PROTECTED. The watchdog
  reads `set(self._pending.keys())` and passes it as `protected` to
  `fail_orphans`.
* **W2.** After the HTTP waiter disconnects, the future is resolved
  (with whatever state — see `adapter.disconnect()` line 528-532)
  and the entry is popped from `_pending`. The next watchdog cycle
  treats the task as orphaned and fails it on the next tick past
  `_ORPHAN_TIMEOUT`.
* **W3.** A clean gateway disconnect (`adapter.disconnect()`) fails
  every pending future with `[agent shutting down]` and clears
  `_pending`. Restarted gateway: no stranded waiters across restarts.
* **W4.** Hard-kill bypasses disconnect cleanup. There is no in-memory
  `_pending` across a hard kill (it lives only in process memory);
  the watchdog is gone too. The DB is the source of truth on reopen.
  Non-terminal inbound tasks follow §4. Outbound tasks follow §3.

## 7. SQLite / persistence invariants

* **P1.** `PRAGMA integrity_check` returns `ok` after any sequence of
  reopen + new writes. Tested in
  `test_taskstore_restart.py::test_sqlite_integrity_check_passes_after_round_trip`.
* **P2.** `task_id` is the primary key — no two records share a
  `task_id`. `TaskStore.create` writes under the lock; concurrent
  writers are serialised by `self._lock` and the SQLite WAL.
* **P3.** A future schema version (`user_version > 3`) raises
  `RuntimeError` from `TaskStore.__init__`. Tested in
  `test_taskstore_restart.py::test_newer_schema_version_raises_runtime_error`.
* **P4.** A malformed row in SQLite (column-rename, etc.) does not
  silently come up empty. Currently the SELECT `OperationalError` is
  caught by the blanket `except Exception` in `_recover_from_db` —
  this is the known gap `tests/recovery/NOTES.md` #11 documents.
  Phase 3 may tighten this; the contract reserves the right to
  require "loud failure" rather than "silent empty store" if a future
  tightening test asserts it.

## 8. What this contract does NOT cover

* The official `a2a-sdk` Python interoperability pass (Phase 5).
* Concurrent restart/query races (Phase 4 / Task 9). The compare-and-set
  rules in §5 cover the wire-level races; the in-process races between
  the watchdog tick and a finalizer are part of the Phase 4 acceptance.
* The bounded-retry policy on the outbound side (Phase 4 / Task 8).
* Storage-failure injection (Phase 4 / Task 10) — locked DB, truncated
  DB, interrupted write, WAL recovery.

## 9. Traceability

| Contract clause | Asserted by test |
|-----------------|------------------|
| Matrix row 1    | `test_taskstore_restart.py::test_terminal_rows_survive_close_and_reopen` (terminal rows; the inbound subset) |
| Matrix row 2    | `test_taskstore_restart.py::test_terminal_rows_survive_close_and_reopen` (terminal rows; the outbound subset) |
| Matrix row 3    | `test_taskstore_restart.py::test_nonterminal_outbound_survives_reopen_untouched` |
| Matrix row 4    | `test_caller_restart.py::test_caller_hardkill_outbound_task_remains_queryable_and_nonterminal` |
| Matrix row 5    | `test_caller_restart.py::test_peer_completes_while_caller_is_down_then_caller_reconciles` |
| Matrix row 7    | `test_taskstore_restart.py::test_nonterminal_inbound_marked_failed_on_reopen` and `test_restart_contract.py::test_inbound_nonterminal_policy_in_subprocess_restart` |
| Matrix row 8    | `test_callee_restart.py::test_inbound_nonterminal_after_hardkill_becomes_failed_with_marker` |
| Matrix row 9    | `test_caller_restart.py::test_remote_task_not_found_does_not_get_restart_marker` |
| Matrix row 10   | `test_restart_contract.py::test_terminal_compare_and_set_*` |
| Matrix row 11   | `test_caller_restart.py::test_caller_restart_does_not_overwrite_prior_remote_terminal` |
| C1/C2/C3         | `test_restart_contract.py::test_terminal_compare_and_set_*` |
| W1               | `test_callee_restart.py::test_watchdog_protects_live_waiter_via_fail_orphans` |
| W2               | `test_callee_restart.py::test_watchdog_after_waiter_disconnects_fails_orphan` |
| P1               | `test_taskstore_restart.py::test_sqlite_integrity_check_passes_after_round_trip` |

## 10. Change policy

This contract is the single source of truth for restart semantics.
Changing it (e.g. switching the row-7 policy from "fail with marker"
to "attempt recovery") requires:

1. A new task in the plan that names the change and the evidence
   required to support it.
2. Updates to the matrix and the rules above.
3. Updates to the affected `tests/recovery/test_*.py` cases.
4. A `NOTES.md` entry recording the prior behavior, the new behavior,
   and the failure mode the change addresses.

The compare-and-set invariants C1–C5 and the persistence invariants
P1–P4 are load-bearing — they MUST NOT change without a Phase 4+
deliberate change.