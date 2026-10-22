# Open review items

Known, deliberately unfixed items. Recorded so a later session treats them as known, not new.

Does not block a canary deployment on a friendly account.

## 1. Plugin, MAJOR -- blocking `a2a_await` has a check-then-consume race -- RESOLVED

`a2a_async_plugin/tools.py::a2a_await` (ownership re-check after `GetTask`), `ownership.py`.

Was: ownership was re-checked after the peer call returned, but result recording, backlog
acknowledgement and the return value were not serialized with a release/adoption, and the check was
skipped entirely when `session_id` is empty. A release followed by adoption in that gap let the
former owner receive the result and empty the new owner's backlog.

Resolved in two places:

* `consume_if_owned(task_id, session_id, message_ids)` now takes the SQLite write lock
  (`BEGIN IMMEDIATE`) *before* reading ownership, and repeats the ownership condition on each
  backlog mark. Release/acquire are UPDATEs and need the same lock, so they cannot interleave.
* The blocking await calls it before writing the shared task record or the transcript, so a lost
  task leaves both untouched instead of writing the result and then reporting the loss.

Regression: `tests/test_ownership.py::test_consume_if_owned_holds_the_write_lock_before_reading_ownership`
and the adoption-during-poll test in the same file.

## Resolved: the core-publisher branch is superseded

`feat/session-completion-publishing` (core) added a completion-target table, a dedup ledger and two
new drains inside Hermes' `state.db` so a plugin could publish an async result. That is too much
surface for upstream and the wrong owner for the state.

Upstream already exposes the public surface we actually need: `PluginContext.inject_message`
routes text to the live CLI, the owning TUI/desktop session, or a messaging chat, and returns
whether a host accepted it. `feat/inject-message-delivery` uses it and keeps durability,
de-duplication, retries and the outbox in the plugin's own `a2a_tasks.db`. No core change is
required, and the installable artifact is plugin-only.
