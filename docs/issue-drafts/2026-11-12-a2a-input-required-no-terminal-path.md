# [Bug]: INPUT_REQUIRED has no terminal path and no caller-answer window

Status: draft, not filed (needs approval to post).

## Summary

`TASK_STATE_INPUT_REQUIRED` is now correctly exempt from the 300-second orphan watchdog, but it has
no terminal path at all. A row in that state stays non-terminal forever, and the caller cannot
answer the task it was asked about because every new inbound message mints a new task id.

## What happens

- `fail_orphans()` exempts `INPUT_REQUIRED` unconditionally (`a2a_async_plugin/protocol.py`), so
  the watchdog never reaps it.
- `_prepare_task()` always calls `protocol.new_task_id()` (`a2a_async_plugin/adapter.py:1368`), so
  a caller's follow-up answer becomes a new task row rather than continuing the task that asked
  the question.
- Result: one permanent non-terminal row per clarification request. `fail_orphans()` scans every
  row on each watchdog tick, so the cost grows with the number of abandoned questions.

Probe from the PR #26 review: after 30 simulated days the row is still `TASK_STATE_INPUT_REQUIRED`;
no reaper touches it.

## Why it matters

Before issue #25 this row was falsely marked `FAILED` at 300 seconds. It is now truthful, but
immortal. A blocked caller has no way to answer the exact task, and the store never releases the
row.

## Proposed fix

Either:

1. Add a caller-answer window, same shape as `deferred_timeout_seconds`: after
   `input_timeout_seconds` (default e.g. 24 hours), transition to `FAILED` with a truthful status
   such as `[caller did not answer]`; or
2. Support task continuation by `taskId`, so a caller's answer updates the existing row instead of
   creating a new one. This is the more complete A2A behavior but a larger change.

Option 1 bounds the store and is enough to stop the leak. Option 2 should be considered separately.

## Acceptance criteria

- An `INPUT_REQUIRED` row older than the configured window is terminal and carries a truthful
  reason.
- A row answered inside the window is not reaped.
- `fail_orphans()` remains linear in the number of non-terminal rows and does not scan terminal
  rows.
- A regression test covers the window and the answer path.

## References

- PR #26 review round 3 (Amy), P2 follow-up.
- `a2a_async_plugin/protocol.py` `fail_orphans()` INPUT_REQUIRED exemption.
- `a2a_async_plugin/adapter.py:1368` new task id per inbound message.
