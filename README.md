# hermes-agent-a2a-async-plugin

Standalone asynchronous A2A client tools for Hermes Agent.

This plugin extends the built-in A2A integration without replacing it. It contributes the async tools to Hermes' existing `a2a` toolset, so operators do not need to manage a second toolset:

- `a2a_submit` -- submit a task without waiting
- `a2a_get_task` -- retrieve a task snapshot
- `a2a_await` -- poll until a task reaches a terminal state, or watch it in the background
- `a2a_watch_cancel` -- stop a background watch (local polling only)
- `a2a_cancel` -- request cancellation
- `a2a_steer` -- queue a non-blocking continuation on an existing context

## Install

From a local checkout:

```bash
hermes plugins install /path/to/hermes-agent-a2a-async-plugin
hermes plugins enable a2a-async
```

Or install from GitHub using the Hermes plugin manager. Keep the built-in A2A plugin enabled. This plugin adds tools to the shared `a2a` toolset; it does not register a second toolset or replace inbound A2A serving.

After installation, enable `a2a` for the relevant platform/profile if it is not already enabled. The plugin contributes no tools when disabled.

## Typical workflow

```text
a2a_submit(agent="researcher", message="Investigate ...")
# Continue useful work
a2a_get_task(agent="researcher", task_id="...")
a2a_await(agent="researcher", task_id="...")            # block until done
a2a_await(agent="researcher", task_id="...", background=true)   # or get the result later
```

Use `a2a_steer` with the `context_id` returned by an earlier A2A exchange when a running conversation needs a correction or follow-up. It returns a distinct task ID immediately.

## Task ownership

A task is either **unowned** or **owned by exactly one session**. Ownership belongs to the session,
not to a watch:

- `a2a_await` (blocking or background) implicitly acquires an unowned task and refuses one owned by
  another session.
- `a2a_watch_cancel` stops polling but keeps ownership. `a2a_release_task_ownership` gives it up
  (tasks, or `all=true` for every task the session owns); the tasks become adoptable elsewhere.
- Ending a session (exit, close, `/new`, `/reset`) releases everything it owns. Nothing keeps
  polling for a session that is gone.
- Resuming a session attempts to reclaim what it released. It is told, through a model-visible
  note, which tasks it reclaimed and which it lost to another session. Reclaim is not automatic
  delivery: `a2a_await` the task to receive its messages.
- Adoption is final. Only unowned tasks can be adopted; `a2a_list` shows ownership per task.
- Cross-profile adoption is not supported: the task store is per profile.

While a task is unowned, every message it produces is queued on the task: results, progress notes,
steering acknowledgements, input requests and watcher problems. Results arrived while unowned are
delivered when the adopting session awaits the task, once per message, oldest first.

An unowned task is cancelled remotely after `a2a.unowned_cancel_after_s` (default 86400s, one day)
and abandoned after `a2a.unowned_task_ttl_s` (default 604800s, seven days). Both can also be set
with `A2A_UNOWNED_CANCEL_AFTER` and `A2A_UNOWNED_TASK_TTL`.

## Background awaits

`a2a_await(background=true)` returns immediately with a `watch_id`. The watch persists in the
profile's `a2a_tasks.db`, polls the peer off-turn, and publishes the result through Hermes'
public plugin injection surface (`PluginContext.inject_message`), so the result arrives as a new
turn in the session that started the work -- after the current turn ends, never in the middle of
one. Delivery is durable on OUR side: every message is written to the plugin's outbox first
(`a2a_tasks.db`), retried with backoff while the host refuses it, and marked published only when a
live host accepts it. Nothing is written to the host's `state.db`.

Delivery is **at least once**. The host is handed the message and only then is the row committed;
if the process dies in that window the message is delivered again on the next attempt. A duplicate
is preferable to a silently dropped result, and the host treats a repeated result as another
message, not as a protocol duplicate.

```text
a2a_await(agent="researcher", task_id="task-1", background=true)
a2a_await(background=true, tasks=[{agent: "researcher", task_id: "task-1"},
                                  {agent: "builder", task_id: "task-9"}])
```

Semantics:

- One completion per watched task, published the moment the peer reports a terminal state
  (`completed`, `failed`, `canceled`, `rejected`). Sibling tasks keep being watched.
- Duplicate observations and watcher restarts do not notify twice: the event id is
  `a2a:<watch>:<peer>:<task>:<state>` and Hermes deduplicates it per session.
- `input-required` is not terminal. It publishes one attention-needed event and pauses that task
  until the caller sends a follow-up (`a2a_submit` with the task's `context_id`) and starts a new
  watch.
- A watch is leased in SQLite before it is polled and the lease is renewed before every task, so
  several processes on one profile (CLI, gateway, TUI) never poll the same watch concurrently --
  not even a long multi-task batch -- and writes from a process that lost its lease are rejected.
  A crashed process's watches are picked up when its lease expires.
- Every event goes through a durable outbox before it is published: a failed publish (host
  admission refused, ledger briefly locked) is retried on the next pass and after a restart,
  never dropped because the watch closed. The outbox row is also the idempotency key.
- Watches and undelivered publications survive a restart: both are rehydrated when the plugin
  loads, and every background operation runs in the profile scope that created the watch, so
  peers, credentials and transcripts resolve against that profile even in a multiplexed gateway.
- `a2a_watch_cancel(watch_id)` stops local polling only. The remote task keeps running;
  `a2a_cancel(agent, task_id)` remains the separate, best-effort remote cancellation request.
- A local watch expiry (default 7 days), cancellation, or repeated polling failure describes the
  watcher, not the peer: it publishes a `watch-error` / `watch-expired` notice and never marks the
  remote task failed. After 12 consecutive polling errors with exponential backoff the task is
  given up on (still not failed).

Tuning: `A2A_WATCH_INTERVAL` (seconds between polls, default 5, minimum 2) and `A2A_WATCH_TTL`
(watch lifetime in seconds, default 604800). Publication retries back off from 5s to 5 minutes;
finished watches and delivered events are pruned after 7 days.

Requirement: background mode needs a session that can take a later turn and a runtime whose
`PluginContext` exposes `inject_message` (any current Hermes). One-shot, worker, cron and stateless
API sessions are refused up front instead of starting a watch that could never deliver.

On Hermes 0.21.x an interactive CLI session binds no route key. The plugin still supports
background mode there by injecting through the attached CLI reference: the target is marked
`keyless_cli` and is process-bound, so a watch is never delivered into a different CLI process
after a restart. A normal session end cancels its watches; a hard kill leaves them to expire.

For a messaging or gateway-hosted session, injection is consent-gated per plugin:

```yaml
plugins:
  entries:
    a2a-async:
      allow_gateway_injection: true
```

Background publications are framed with the same untrusted boundary as inbound A2A dispatch
(`security.wrap_inbound`): the peer's text is prefixed as remote-agent content and injection
markers are filtered before it reaches the session as a user turn. Blocking `a2a_await` and
`a2a_get_task` do not use the injection rail.

Peers are configured in `config.yaml` under `a2a_agents`. Authentication and endpoint configuration remain owned by the built-in A2A integration.

The inbound Agent Card identity can be set per profile under the A2A platform configuration. In a multiplexed gateway, put the name in each profile's config rather than relying on the shared process environment:

```yaml
platforms:
  a2a:
    extra:
      agent_name: kevin
      port: 9902
```

The plugin falls back to `A2A_AGENT_NAME` for older deployments, then to `hermes-<hostname>`.

## Architecture

```text
Hermes built-in A2A plugin
  synchronous client tools + inbound A2A platform
                 \
                  shared `a2a` toolset
                 /
hermes-agent-a2a-async-plugin
  async client tools + durable task state
```

The plugin uses the stdlib HTTP transport and the A2A JSON-RPC protocol. It has no third-party runtime dependency.

## Files

| Path | Purpose |
|---|---|
| `plugin.yaml` | Hermes plugin manifest |
| `__init__.py` | Hermes plugin entry point; delegates to the implementation package |
| `a2a_async_plugin/tools.py` | Async tool schemas, handlers, and registration |
| `a2a_async_plugin/protocol.py` | A2A protocol helpers and durable task/watch store |
| `a2a_async_plugin/watches.py` | Background watch manager: polling, leases, completion publishing |
| `a2a_async_plugin/security.py` | Outbound redaction and audit helpers |

## Development

Run syntax checks with:

```bash
python -m py_compile a2a_async_plugin/*.py
python -m pytest tests -q
```

The cross-repo test (`test_watch_publishes_through_the_real_hermes_publisher`) skips unless a
Hermes checkout with its dependencies is importable; run it against one with:

```bash
PYTHONPATH=/path/to/hermes-agent /path/to/hermes-agent/.venv/bin/python -m pytest tests -q
```

Without a Hermes checkout the publisher, profile-scope and registration tests skip; the rest of
the suite (watches, ownership, protocol) runs standalone.

The plugin is intended to be tested through Hermes' real plugin discovery path with a temporary `HERMES_HOME`. Do not run it alongside another plugin that registers the same async tool names.

## Conversation transcripts

Inbound and outbound turns are persisted as JSONL under `a2a_conversations/` inside the active profile's Hermes home, next to the profile-scoped task DB (`a2a_tasks.db`). The directory is resolved once at adapter initialization and pinned via the `A2A_CONVERSATIONS_DIR` environment override, so HTTP worker threads never re-resolve the Hermes home themselves. Set `A2A_CONVERSATIONS_DIR` explicitly to relocate the transcripts.

`a2a_history` replays transcript turns for an agent's own context. Peer-generated turns (inbound tasks on the callee side, peer replies on the caller side) are filtered before persistence and framed as untrusted remote content on replay; locally-generated turns (our own sends and replies) replay unframed. Peer records carry explicit attribution in the JSONL (`peer` field, or `origin: local` for our own turns); legacy records written before v0.1.9 have no attribution and are framed fail-safe (callee-style `[user]` records as untrusted with `peer:unknown`). Transcripts written before v0.1.9 lack the peer attribution frame and are not migrated — their replay is framed as untrusted with `peer:unknown`, but content persisted under an older version in the wrong root (for example, under the default home on multiplexed gateways) is not relocated. Those older artifacts are accepted as lost; re-point a2a_history at the profile-scoped directory only for transcripts captured after this version.

Reads (`a2a_history`, `a2a_list kind=conversations`) resolve the same directory as writes via `A2A_CONVERSATIONS_DIR` when set. On multiplexed gateways where tool handlers may execute outside the profile's runtime scope, set `A2A_CONVERSATIONS_DIR` explicitly so both sides of the artifact agree regardless of scope propagation.

## Safety

- Remote endpoints must be explicitly configured.
- Outbound messages pass through the plugin's redaction and audit path.
- Cancellation is best effort because a peer may already be processing the task.
- The orphan watchdog fails only non-terminal LOCAL INBOUND tasks (no live waiter, past a 300s age threshold); outbound/remote rows are never failed by a local timer — the peer's reported state settles them.
- Do not put API tokens in this repository or in `config.yaml`; use Hermes secret handling.
