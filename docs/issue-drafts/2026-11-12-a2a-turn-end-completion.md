Title: [Bug]: A2A completes the task on the turn's first reply and drops the later report
Labels: bug
Template: Generic bug report (no repository issue form found)
Affected Component: A2A async plugin (inbound task lifecycle, completion, watch publication)
Messaging Platform: A2A
Status: filed as #25 (2026-11-12)
Verified against: plugin 0.5.1 at 3d85e4a; live incident on Stuart -> Nic 2026-11-12 with the ledger row read back read-only; adapter and TaskStore code paths traced; orphan-watchdog behavior reproduced on a temp TaskStore.

---

## Bug Description

An inbound A2A task is completed by the first final assistant reply of the Hermes
turn that handled it. When the agent starts background work and answers with a
progress note ("still running, I will report later"), the durable task is already
`TASK_STATE_COMPLETED`. The caller's watch publishes that progress note as the
task result and closes. The real report, produced when the background process
completion wakes the session, has no open task row to land in and is dropped
silently.

The adapter equates the Hermes turn boundary with task completion because it has
no non-terminal "turn yielded, work continues" path. `TASK_STATE_WORKING` exists
in A2A v1.0 for exactly this case, but nothing emits it after a reply.

## Observed Evidence

Incident 2026-11-12, plugin 0.5.1, peer `stuart` -> caller `nic`,
`task-f4b3fac14e684006`, `ctx-33b0ad1dde2f4db1`:

- Task created 11:06:49.
- Stuart's turn ended 11:40:57.827 with a 193-char progress note: build at 15%,
  sims green, will report the resource table when the run finishes.
- Stuart's task row completed 11:40:58. Nic's outbound row and watch published
  at 11:41:03 (poll lag), so `completed` was visible before the work finished.
- The only `a2a_task_messages` row is `artifact-id:43255a22931d44058868f7b5a2e3e077`,
  `kind='result'`, carrying that progress note. The progress note became the
  task's deliverable artifact.
- Background completion fired 11:48:37.402. Stuart's gateway sent the real
  3,487-char report to the same context at 11:52:32. No second message row, no
  publication, no inbound entry on Nic's side. The report exists only in
  Stuart's session `20261112_110649_9b6a0efe`.

Code path:

- `Adapter.send()` accepts only `metadata['notify']` sends (`adapter.py:2032`)
  and settles the turn's task(s) as `TASK_STATE_COMPLETED`
  (`adapter.py:2050-2053` -> `_finalize_task` -> `tasks.complete()` at
  `adapter.py:1490`). `on_processing_complete` does the same for a reply-less
  success (`adapter.py:2280-2281`).
- This mapping predates the #21 fix. `send()` has done it since the initial
  commit `a1aec0f` (2026-09-16). `d47afe4` (#21) changed only how a completed
  task's output is emitted (artifact instead of `status.message`) and
  receiver-side duplicate coalescing; it did not touch `send()` or when a task
  completes.
- The later background-wake turn is a synthetic `MessageEvent` with
  `message_id=None` (`gateway/run_notifications.py:884` in core).
  `on_processing_start` needs a task id (`adapter.py:1050-1055`), so no turn
  record is created. `send()` then calls `_resolve_oldest_for_context`, which
  scans only in-memory pending futures (`adapter.py:1030-1044`). The original
  future was popped when the task completed, so `send()` logs at debug and
  returns `SendResult(success=True)` (`adapter.py:2054-2061`). The host believes
  the report was delivered.

Related defect confirmed while diagnosing: `fail_orphans()` flips
`TASK_STATE_INPUT_REQUIRED` rows to `TASK_STATE_FAILED` 300 seconds after
creation (`protocol.py:1872`). Probe on a temp `TaskStore`: state
`INPUT_REQUIRED` -> `failed_by_watchdog ['task-probe-input']` ->
`TASK_STATE_FAILED`. A caller is asked a question and the peer's own timer
answers "failed".

## Steps to Reproduce

1. Send an inbound A2A task to an agent profile whose agent starts a background
   process with `notify_on_complete=true` and ends the turn with a progress
   note instead of the final result.
2. Watch the task from the caller (`a2a_submit` + `a2a_await(background=true)`,
   or poll `a2a_get_task`).
3. Observe the task reach `completed` with the progress note as its artifact
   while the background process is still running.
4. Let the background process finish and wake the peer's session. Observe the
   real report sent into the same A2A context produce no task row and no
   caller-visible message.

## Expected Behavior

A task whose agent is still working stays non-terminal. The first reply is a
`WORKING` status message, not a completed artifact. The task completes when the
agent reports the result, and that result becomes the artifact. A caller's watch
publishes the progress note as progress and the result as the result.

## Actual Behavior

The first turn-final reply completes the task. The progress note is published as
the result. The later report is discarded with a debug log and a success return.

## Proposed Fix

Add a non-terminal deferral path:

1. `[WORKING]` marker at the start of a turn-final reply, same convention as the
   existing `[INPUT_REQUIRED]` marker. The adapter strips it, records
   `TASK_STATE_WORKING` with the progress text in `status.message`, and leaves
   the task non-terminal. `build_task()` already renders non-completed states as
   a status message (`protocol.py:1059-1063`), and the watch path already
   records non-terminal payload text as `progress` and keeps polling
   (`watches.py:1597-1644`).
2. Do not resolve the reply waiter on a deferral. The async finalizer keeps
   waiting (`A2A_ASYNC_TIMEOUT`, 8h default). The background-wake turn has no
   turn record, so its final send settles the deferred task and the finalizer
   writes the real artifact.
3. Add a durable fallback: a notify send in the context with no in-memory
   waiter completes the oldest `WORKING` row marked deferred. Add
   `a2a_tasks.deferred_at` (schema 11), exempt deferred rows from the 300s
   orphan cutoff (fail after the async timeout instead), and clear
   `deferred_at` on every terminal transition.
4. Do not treat a blocking caller's disconnect as cancellation of a deferred
   task.
5. Document the marker with `ctx.register_system_prompt_section()`, README, and
   the conformance doc.
6. Exempt `INPUT_REQUIRED` from the 300s orphan watchdog in the same change.

## Alternatives Considered

- A core capability that tells the plugin the session has pending background
  work. Correct long term, but needs a new public `PluginContext` API and still
  races the arming of the process. Rejected for this fix.
- Require an explicit `[FINAL]` marker on every reply. Breaks every existing
  peer and agent.
- Tell agents not to use background notify in A2A sessions. Costs the worker
  for the whole run and does not survive turn timeouts or restarts.

## Scope

- Inbound task lifecycle in `adapter.py` and `TaskStore`.
- Additive schema 11 (`deferred_at`).
- Docs: `README.md`, `docs/a2a-conformance.md`, and a note in
  `docs/restart-recovery-contract.md` if the restart policy is unchanged.

## Acceptance Criteria

- A turn-final reply starting with `[WORKING]` leaves the task `WORKING`,
  records the progress text in `status.message`, and does not resolve the
  caller's waiter.
- A later notify send in the same context completes the deferred task with the
  report as the artifact. A watch publishes the progress note, then the result.
- The same completion works when the blocking waiter is gone (durable deferred
  row, no in-memory pending entry).
- A normal reply without the marker still completes exactly as today.
- A deferred task is not failed by the 300s orphan watchdog; it is failed after
  the async timeout if no report arrives.
- `INPUT_REQUIRED` is not failed by the 300s orphan watchdog.
- New regression tests fail on `3d85e4a`.

## Explicit Non-Goals

- Recovering a deferred task across a gateway restart. The restart contract
  still fails non-terminal inbound rows.
- Task continuation for a caller-supplied `taskId`.
- Changing the artifact/status split for completed tasks (#21).
- Changing watch polling cadence or publication retry policy.

## Open Questions

None. The marker is opt-in, so existing agents keep the current completion
behavior until they start using `[WORKING]`.
