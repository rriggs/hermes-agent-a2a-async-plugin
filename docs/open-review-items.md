# Open review items

Known, deliberately unfixed items. Recorded so a later session treats them as known, not new.

Does not block a canary deployment on a friendly account.

## 1. Plugin, MAJOR -- blocking `a2a_await` has a check-then-consume race

`a2a_async_plugin/tools.py::a2a_await` (ownership re-check after `GetTask`), `ownership.py`.

Ownership is re-checked after the peer call returns, but result recording, backlog acknowledgement
and the return value are not serialized with a release/adoption, and the check is skipped entirely
when `session_id` is empty. A release followed by adoption in that gap lets the former owner
receive the result and empty the new owner's backlog.

Fix: a store-level `consume_if_owned(task_id, session_id, message_ids)` that under the store write
lock verifies the live owner equals the bound session (or that the task is unowned for an unbound
caller) and marks the backlog delivered in the same transaction; the await path then calls it
instead of check-then-mark. Needs a release plus an adoption inside one GetTask round trip.

## Resolved: the core-publisher branch is superseded

`feat/session-completion-publishing` (core) added a completion-target table, a dedup ledger and two
new drains inside Hermes' `state.db` so a plugin could publish an async result. That is too much
surface for upstream and the wrong owner for the state.

Upstream already exposes the public surface we actually need: `PluginContext.inject_message`
routes text to the live CLI, the owning TUI/desktop session, or a messaging chat, and returns
whether a host accepted it. `feat/inject-message-delivery` uses it and keeps durability,
de-duplication, retries and the outbox in the plugin's own `a2a_tasks.db`. No core change is
required, and the installable artifact is plugin-only.
