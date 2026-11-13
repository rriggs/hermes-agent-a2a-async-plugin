# [Bug]: a deferred task re-delivers its progress note on every watch poll

Status: draft, not filed (needs approval to post).

## Summary

While a task is deferred (`TASK_STATE_WORKING` with `deferred_at` set), every `GetTask` render mints
a fresh `messageId` for the status message. A watcher keys its backlog and outbox on that id, so it
treats the same unchanged progress note as a new message on every poll and injects it again.

## Evidence

Nic's end-to-end probe on 0.6.0, task `task-98f441af9ace4160`:

- The turn ended on `[WORKING]`; the row stayed `TASK_STATE_WORKING` with `deferred_at` set.
- The wake turn delivered the real report and the row completed with `PROBE-DONE 2026-11-13T06:08:46Z`.
- The caller received **11 progress rows in 45 s, with only 2 distinct texts**. The store confirms
  11 `a2a_task_messages` rows and 10 duplicates of `Probe job running.` with different message ids,
  all delivered.

At a 5 s poll interval, an 8 h `A2A_ASYNC_TIMEOUT` deferral is 5,760 polls, so an unchanged note
can push ~5,760 duplicate injections into the dispatcher session.

## Root cause

- `protocol.py` `text_message()` mints `uuid.uuid4().hex` per call.
- `protocol.build_task()` builds a `status.message` for every non-completed state, and
  `TaskStore.to_task()` calls it on every `GetTask`/`ListTasks`.
- `tools._observation_messages()` uses `status.message.messageId` when present; the
  `status:{digest(text)}` fallback is unreachable for our own payloads.
- `ownership.record_messages()` inserts by `(task_id, message_id)`, and `watches.py` keys its
  outbox event id on `a2a:{task_id}:{message_id}`, so each new id becomes a new message row and a
  new publication.

This was latent before 0.6.0 because a `WORKING` status could not persist; the first reply
completed the task. `INPUT_REQUIRED` had the same shape and was already exposed.

## Fix

Derive the `messageId` of a non-terminal status message from the task, context, state, and Parts
(a UUIDv5 over a canonical content hash) instead of a fresh UUID. Identical snapshots keep their
id; a changed progress note gets a new id. Apply the same rule in `build_task()` and
`status_update()`. This is a patch release with no schema change.

## Acceptance criteria

- Two renders of the same non-terminal task produce the same `status.message.messageId`.
- A changed progress text (or Parts) produces a different id.
- A watch polling the same deferred snapshot twice records one message row and one publication.
- A later changed note is still delivered once, and the terminal result is delivered once.
- `INPUT_REQUIRED` gets the same stable identity.
- No schema change; schema stays 11.

## Non-goals

- Artifact identity for completed tasks is unchanged in this fix.
- No consumer-side compatibility hedge for peers that mint unstable ids; the producer fix covers
  our own emitter.

## References

- Nic's probe: task `task-98f441af9ace4160`, emerald, 2026-11-13T06:08:46Z.
- `protocol.py` `text_message()`, `build_task()`, `status_update()`, `TaskStore.to_task()`.
- `tools.py::_observation_messages`, `ownership.py::record_messages`, `watches.py` outbox event id.
- Regression tests: `tests/test_outbound_parts.py::test_non_terminal_status_message_id_is_stable_across_renders`;
  `tests/test_watches.py::test_repeated_polls_of_one_working_note_deliver_once`.
