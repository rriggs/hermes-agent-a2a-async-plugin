# hermes-agent-a2a-async-plugin

The complete A2A platform plugin for Hermes Agent: it serves inbound A2A requests and provides synchronous and asynchronous outbound client tools with durable task state.

The plugin registers the inbound `a2a` platform and contributes its client tools to Hermes' shared `a2a` toolset. It is not client-only and does not register a second toolset. Its asynchronous client tools include:

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

Or install from GitHub using the Hermes plugin manager. This plugin must be the sole inbound A2A platform owner: disable the bundled inbound A2A adapter when enabling this plugin, and do not run both. The plugin contributes client tools to the shared `a2a` toolset; it does not register a second toolset.

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

## Blocking and asynchronous sends

Inbound `message/send` has two shapes, and the difference matters for long work:

- **Blocking** (no `configuration.returnImmediately`, or `blocking: true`): the plugin waits until
  the task reaches a terminal or interrupted state, then returns that state and reply. There is no
  artificial reply cap by default, per A2A v1.0 §3.2.2. A healthy agent that takes an hour gets an
  hour; the wait is bounded only by the client connection (a disconnected caller is detected and the
  wait stops) and by gateway shutdown.
- **Asynchronous** (`configuration.returnImmediately: true`): the plugin returns `WORKING` with the
  task id immediately and finalizes the task off the request thread, bounded by
  `A2A_ASYNC_TIMEOUT` (default 28800s). Use this for multi-turn conversations and any caller that
  cannot hold a connection open.

`A2A_REPLY_TIMEOUT` opts into a cap for blocking sends. On expiry the task is recorded `CANCELED`
with a caller-side marker, never `FAILED`: the agent may still be working, and `FAILED` would assert
an outcome that never happened. The cap does not abort the agent's turn; a reply that arrives after
expiry is dropped rather than written into the terminal record. Leave it unset unless a client-side
deadline is required.

Profile-forwarded tasks keep a bounded dispatch timeout (default 300s): expiry kills the child
process, so the `FAILED` outcome there is truthful.

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
with `A2A_UNOWNED_CANCEL_AFTER` and `A2A_UNOWNED_TASK_TTL`. The clock starts at the last release,
or at creation for a task that was submitted and never awaited.

Before the cancel timer fires, an unowned outbound task is reconciled against its peer. `GetTask`
is issued once the task is older than `a2a.reconcile_after_s` (default 3600s), at most once per
`a2a.reconcile_interval_s` (default 300s) per row, in batches of `a2a.reconcile_batch` (default
10). The peer's terminal state and reply are folded into the local record; `TaskNotFound` marks the
task canceled with a "peer no longer has this task" reply rather than FAILED. Observed messages are
queued in the same ownership-fenced transaction, so an adopter can receive a recovered answer even
if the peer is offline. Auto-cancel responses use the same release-generation fence: if a task is
adopted while `CancelTask` or its `-32002` `GetTask` follow-up is in flight, the new owner's row is
not changed. An unreachable peer leaves task state untouched and is backed off for the rest of the
current batch as well as later passes. These can also be set with `A2A_RECONCILE_AFTER`,
`A2A_RECONCILE_INTERVAL` and `A2A_RECONCILE_BATCH`. `a2a_list` shows each task's age, so an
abandoned row is visible without opening the database.

## Operator commands

Two user surfaces over the same task store. Neither is an agent tool.

```bash
hermes a2a-tasks list   [--session <id>] [--unowned] [--task <id> ...]
hermes a2a-tasks release --session <id> (--all | --task <id> ...) [--yes]
hermes a2a-tasks transfer --from <session> --to <session> [--task <id> ...] [--yes]
```

In a session, the same grammar is available as `/a2a tasks ...`, with `--session` defaulting to
the session the command is typed in:

```text
/a2a tasks list
/a2a tasks release --all --yes
/a2a tasks transfer --from <old-session> --to <new-session> --yes
```

`release` clears ownership, stops that session's watches for the tasks, and drops its pending
deliveries; queued peer messages stay for the next owner. `transfer` moves ownership in one
transaction with the same cleanup for the previous owner. Both require `--yes`; without it, the
shell command prompts on a TTY and refuses when non-interactive. Neither command needs a running
gateway. This is how an operator clears a stuck owner without taking over the session that holds it.

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

Every injected turn starts with a one-line locator, for example
`[A2A result · researcher · task t1]`, followed by the untrusted-input frame and then the peer's
text. The locator exists because the TUI collapses a queued prompt to a single truncated line,
which used to be the identical frame for every result. It carries plugin-authored metadata only:
peer, task and kind are charset-checked, and a task id that fails the check or is over-long shows
as `?` rather than being partially sanitized into text above the boundary.

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
  A `CancelTask` reply carries the task the peer now holds, so that state is folded into the local
  snapshot: `a2a_list` shows a successful cancel immediately instead of the pre-cancel state. A
  bare success with no task body changes nothing, because it is not evidence of any state.
- A local watch expiry (default 7 days), cancellation, or repeated polling failure describes the
  watcher, not the peer: it publishes a `watch-error` / `watch-expired` notice and never marks the
  remote task failed. After 12 consecutive polling errors with exponential backoff the task is
  given up on (still not failed).

Tuning: `A2A_WATCH_INTERVAL` (seconds between polls, default 5, minimum 2) and `A2A_WATCH_TTL`
(watch lifetime in seconds, default 604800). Publication retries back off from 5s to 5 minutes;
finished watches and delivered events are pruned after 7 days.

### Live status line

While a watch runs, the session that started it gets one editable status notice through
`PluginContext.post_message` / `PluginMessage.edit`. On a TUI that is a status-bar line; a
messaging chat gets an editable message when the adapter supports editing. The line is rebuilt
from the watch store and updated in place:

```text
⧗ a2a 2 tasks · working · dev-a 1m02s · dev-b 0m31s
✓ a2a dev-a · completed
```

State changes (a terminal result, `input-required`, cancellation, expiry) repaint immediately;
elapsed-time refreshes are throttled to every 15s, so a 5s poll does not repaint the status bar.
It is one notice per session: a second watch edits the same line instead of stacking another.

The line is presentation only. A host without `post_message`, a refused post, or a surface that
cannot show notices still gets the durable completion turn. A gateway restart drops the notice
handle: the watch keeps polling and still delivers, but the line does not come back until a new
watch starts in that session. `a2a_list(kind="watches")` reports the same state on demand,
restart included.

Requirement: background mode needs a session that can take a later turn, a runtime whose
`PluginContext` exposes `inject_message`, a durable route key for that session, and a host path
that actually displays the result. The plugin asks core's
`PluginContext.async_delivery_visible()` and refuses before creating a watch when that says no.
A classic CLI session served by the gateway currently reports no visible path: the turn runs in
the transcript but the attached terminal does not display it. TUI/Desktop and messaging sessions
report visible, as does an in-process classic CLI. If core later adopts plugin admissions in the
classic CLI, the capability flips and background mode works there with no plugin change. Hermes
0.22.2 or newer binds the durable route key; older cores are refused rather than using keyless
injection (the host would pick whichever session is calling at flush time when several CLI
sessions share the gateway process). One-shot, worker, cron and stateless API sessions are
refused up front instead of starting a watch that could never deliver.

Injection is consent-gated per plugin by Hermes core for every gateway-routed session, including a
classic CLI session (0.22.2+ runs those turns in the gateway):

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

Outbound peers are configured in the profile's `config.yaml` under `a2a_agents`. Inbound endpoint binding, the Agent Card, and peer authentication are owned by this plugin's `a2a_async_plugin/adapter.py` and `security.py`, not by the bundled integration. The listener is localhost-only by default. To expose it remotely, configure `A2A_BEARER_TOKEN` or `A2A_PEER_TOKENS` and set `A2A_HOST`; never put tokens in `config.yaml`.

The inbound Agent Card identity can be set per profile under the A2A platform configuration. In a multiplexed gateway, put the name in each profile's config rather than relying on the shared process environment:

```yaml
platforms:
  a2a:
    extra:
      agent_name: kevin
      port: 9902
```

The plugin falls back to `A2A_AGENT_NAME` for older deployments, then to `hermes-<hostname>`.

## Inbound attachments

Inbound A2A `FilePart` bytes are validated and materialized as mode-`0600` files under
`<profile-home>/a2a_files/<task-id>/` (task directories are mode `0700`). `A2A_FILES_DIR` overrides
the inbox location. The Agent Card advertises `text/plain` and the configured inbound file media
types; the default allowlist covers common image, audio, video, text, PDF, Office/OpenDocument, and
ZIP formats. Unsupported media types remain visible to the agent as an explicit rejection note.

- `A2A_MAX_INBOUND_FILE_MB`: per-file size cap, default `20` MiB. Oversized raw parts are rejected
  with an agent-visible note. URL parts are fetched only over HTTP(S) to globally routable addresses;
  DNS results are pinned for the connection, redirects are checked again, and downloads are bounded
  by the same size cap. The original URL stays visible. No peer authorization headers or environment
  proxy credentials are forwarded.
- `A2A_INBOUND_FILE_MEDIA_TYPES`: optional comma-separated media-type allowlist. Defaults are the
  built-in supported types above; exact media types and patterns such as `image/*` are accepted.
- `A2A_INBOUND_FILE_TTL_DAYS`: terminal/expired task inbox retention, default `7` days. Active task
  inboxes are retained; set `0` to disable cleanup. Cleanup runs at adapter startup and on the
  watchdog interval.

Inbound JSON-RPC request bodies and peer JSON responses are capped at `32` MiB. The inbound adapter
actively ingests at most two large request bodies concurrently, where "large" means over `256` KiB;
that accommodates the default 20 MiB raw-file cap plus base64 and JSON overhead while limiting
concurrent inbound request-body buffers to `64` MiB per adapter. Smaller calls are read without an
admission slot so a JSON-RPC operation never queues behind a stalled attachment upload, and every
body must arrive within 30 seconds of the request line or the adapter answers `408` and closes the
connection.

## Architecture

```text
hermes-agent-a2a-async-plugin
  inbound A2A platform (sole owner)
  synchronous + asynchronous client tools
  durable task state
                 |
         shared `a2a` toolset
```

The plugin uses the stdlib HTTP transport and the A2A JSON-RPC protocol. It has no third-party runtime dependency.

## Files

| Path | Purpose |
|---|---|
| `plugin.yaml` | Hermes plugin manifest |
| `__init__.py` | Hermes plugin entry point; delegates to the implementation package |
| `a2a_async_plugin/adapter.py` | Inbound A2A HTTP server, Agent Card, attachment ingestion, JSON-RPC/SSE dispatch, push notifications, and metrics |
| `a2a_async_plugin/tools.py` | Synchronous and asynchronous A2A client tools, task-result rendering, and registration |
| `a2a_async_plugin/protocol.py` | A2A protocol helpers, inbound attachment materialization, Agent Card construction, and durable task store |
| `a2a_async_plugin/ownership.py` | Session-scoped task ownership and queued-message backlog |
| `a2a_async_plugin/task_admin.py` | Operator CLI and slash commands for task ownership |
| `a2a_async_plugin/watches.py` | Background watch manager: polling, leases, completion publishing |
| `a2a_async_plugin/security.py` | Inbound authentication, bind safety, outbound redaction, and audit helpers |
| `tests/test_inbound_files.py` | Regression tests for inbound file parts, safe URL downloads, and request limits |

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
