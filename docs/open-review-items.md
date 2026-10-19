# Open review items

Findings from the last external review round (core `8a2f299a69`, plugin `784c60c`) that are
**known and deliberately unfixed**. They are recorded here so a later session does not rediscover
them as new, and so the decision to fix or ship them is explicit.

Neither item blocks a canary deployment on a friendly account.

## 1. Core, MAJOR -- stateful API-server sessions are refused by capture

`agent/plugin_completions.py::_require_async_delivery` (via `capture_completion_target`).

`gateway/platforms/api_server.py:4082` binds the session with `async_delivery=False` even when the
request declares `session_history_delivery=1`, so capture raises `CompletionDeliveryUnsupported`
for stateful API sessions. The gateway already has an API-session self-post path for plugin
completions (`gateway/run_notifications.py:869-899`), and the documentation lists only **stateless**
API sessions as unsupported.

Decision required (product, not code): either

- honor the explicit `session_history_delivery` capability in `api_server` / `_require_async_delivery`, or
- document and test that **all** API-server sessions are unsupported.

## 2. Plugin, MAJOR -- blocking `a2a_await` has a TOCTOU before consumption

`a2a_async_plugin/tools.py::a2a_await` (ownership re-check after `GetTask`), `ownership.py`.

Ownership is re-checked after the peer call returns, but result recording, backlog acknowledgement
and the return value are not serialized with release/adoption, and the check is skipped entirely
when `session_id` is empty. A release followed by adoption in that gap lets the former owner
receive the result and empty the new owner's backlog.

Required fix: a store-level `consume_if_owned(task_id, session_id, message_ids)` that under the
store write lock verifies the live owner equals the bound session (or that the task is unowned for
an unbound caller) and marks the backlog delivered in the same transaction; the await path uses it
instead of check-then-mark.
