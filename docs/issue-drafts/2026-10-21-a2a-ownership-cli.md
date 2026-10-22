Title: [Feature]: User commands to inspect, release, or transfer A2A task ownership across sessions
Labels: enhancement
Issue: [#10](https://github.com/rriggs/hermes-agent-a2a-async-plugin/issues/10)

## Problem or Use Case

A2A task ownership is session-scoped. Only the owning session can release a task, and only an
unowned task can be adopted. When the owner is gone, stuck, or simply busy in another session, the
task is stranded: another session cannot take it over through any supported path.

Live case (Dean, 2026-10-21):

```text
task-559cf9cab1b849ab  peer=dev-c  state=working  owner=local-83bdf06b...  released_at=0
task-be576ae3730f4eeb  peer=dev-a  state=working  owner=local-83bdf06b...  released_at=0
```

The owner is the local "Review handoff doc and wave 41 plan" session. No release has landed, and a
second Dean session cannot adopt either task. The current options are to drive the owner session
into calling `a2a_release_task_ownership(all=true)`, or to edit `a2a_tasks.db` by hand. The first
requires control of a session the operator may not be talking to; the second bypasses the
transactional rules that protect pending deliveries.

The agent tool surface is the wrong shape for this. Releasing another session's ownership is an
operator action, not a model decision. It also cannot run in the state that matters here: in this
core every `hermes chat` turn executes in the gateway (`_chat_runs_at_gateway` covers `None`,
`chat`, and `rl`), so no session means no tool call. The outbound A2A transport itself is direct
stdlib HTTP, but invoking the tool still requires the turn that the gateway runs. The operator
command must work with neither a session nor a gateway process.

Operators also need this from inside a running session. Dropping to a shell to run
`hermes a2a-tasks` is friction, and the session is where the stuck owner and the task ids are
visible. That surface must be a user-typed slash command (`/a2a tasks ...`), not an agent tool: it
is not offered to the model, cannot be invoked during reasoning, and its output is local to the
session.

## Proposed Solution

Two user surfaces over one implementation:

- `hermes a2a-tasks ...` for the shell, including when no gateway or session exists.
- `/a2a tasks ...` inside a session, against the active profile and the session the command is
  typed in.

Both are thin adapters over one `task_admin` module (list, release, transfer, shared formatting), so
behavior and output cannot drift. Neither registers an agent tool.

### Shell command

Registered with `ctx.register_cli_command()` and resolved against the active profile's
`a2a_tasks.db`. It works without a live gateway because plugin CLI commands dispatch before any
agent turn, so no gateway is started, and it opens the database directly, the same file the tools
use.

```text
hermes a2a-tasks list   [--session <id>] [--unowned] [--task <id> ...]
hermes a2a-tasks release --session <id> (--all | --task <id> ...) [--yes]
hermes a2a-tasks transfer --from <session> --to <session> [--task <id> ...] [--yes]
```

Behavior:

- `list` prints task id, peer, state, owner, age, undelivered message count and pending delivery
  count. `--session` shows only that session's tasks; `--unowned` shows adoptable tasks.
- `release --session X` clears ownership of X's tasks in one transaction, stops X's watches for
  those tasks, and drops X's pending deliveries for them. Undelivered peer messages stay queued.
  `--all` is explicit; a release without `--task` or `--all` is a usage error.
- `transfer` moves ownership in one transaction and applies the same watch and delivery cleanup to
  the previous owner. The new owner's backlog is untouched.
- A non-interactive run requires `--yes` when the target session is not the active session.
- Every task changed is printed as one line (task id, previous owner, new owner/`unowned`, counts
  dropped) and logged through the plugin logger.
- Exit codes: 0 success, 1 nothing matched, 2 usage error, 3 ownership changed under the command
  (report and change nothing).
- `hermes --profile <name> a2a-tasks ...` selects the profile; the command never reaches across
  profiles.

### In-session command set

Registered with `ctx.register_command("a2a", handler, args_hint="tasks [list|release|transfer] ...")`.
The command mirrors the shell grammar exactly:

```text
/a2a tasks list    [--session <id>] [--unowned] [--task <id> ...]
/a2a tasks release [--session <id>] (--all | --task <id> ...) --yes
/a2a tasks transfer --from <session> --to <session> [--task <id> ...] --yes
```

Behavior:

- `--session` defaults to the session the command is typed in (the command dispatcher passes
  `session_id`), so `/a2a tasks list` shows what this session owns and
  `/a2a tasks release --all --yes` releases it. `--session <other>` targets another session.
- `release` and `transfer` require `--yes`, including for the current session. A slash handler
  returns text synchronously and cannot block on a prompt, so there is no interactive confirmation
  in v1.
- Output is a local command result, never a model turn and never an injected message. It uses the
  same lines as the shell command: task id, peer, state, previous owner, new owner/`unowned`,
  undelivered messages kept, pending deliveries dropped, watches stopped.
- The command resolves the store from the active profile. It never crosses profiles; a session id
  from another profile simply does not match and is reported as unknown.
- Failures print the same diagnostic and change nothing: unknown subcommand, missing `--yes`,
  unknown session, nothing matched, concurrent ownership change.
- `/a2a` with no arguments prints the usage block. `/a2a tasks` with no subcommand prints the
  `tasks` usage block.
- `/a2a` is free in core 0.22.2 (`resolve_command("a2a")` is None). `/tasks` is a built-in alias for
  the `agents` command, so `tasks` must be a subcommand of `/a2a`, never a top-level command.

## Alternatives Considered

- **Agent tool with `from_session` on `a2a_release_task_ownership`.** Implemented locally, then
  rejected: it puts an operator-only action in the model's tool surface, and it cannot help when the
  gateway is down or no session is bound.
- **`a2a_acquire_task_ownership(force=true)`.** Same objection, plus it silently takes work away
  from a session that may still be using it. Transfer through an explicit command is auditable.
- **Shell command only.** Covers the broken-gateway case but forces an operator who is already in a
  session to leave it, and makes the current session id awkward to obtain.
- **Slash command only.** Cannot help when no session or gateway exists, which is the case that
  motivates the release path.
- **Hand-edited SQL.** Fast, but it skips the pending-delivery cleanup and can leave a publication
  addressed to the previous session.
- **Wait for the owner to release.** Unacceptable when the owner is idle or unreachable.

## Scope

Medium. One new `task_admin` module for the list/release/transfer core and shared formatting, a CLI
adapter registered with `ctx.register_cli_command()`, and a slash adapter registered with
`ctx.register_command()`. Reuse `OwnershipStore.release_all` / `release` and
`WatchStore.cancel_session`. No schema change, no new agent tool.

Tests: core effects on ownership, watches and pending deliveries; CLI parsing and exit codes; slash
parsing with the session default; parity between both surfaces against the same database; and a
no-gateway invocation against a temp profile database.

Out of scope for v1: remote task cancellation, cross-profile transfers, interactive confirmation,
and notifying the previous session that it lost the task.

## Acceptance Criteria

1. `hermes a2a-tasks list --session <id>` shows exactly the tasks that session owns.
2. `release --session X --all` leaves every task X owned unowned, with `released_by=X` and
   `unowned_since` set, and the command exits 0.
3. The same command removes X's pending deliveries for those tasks and stops X's watch items; a
   queued peer message is still delivered to a later adopter.
4. `transfer --from X --to Y` moves ownership atomically; a concurrent release or adoption yields
   exit code 3 and no partial change.
5. Neither shell command requires a running gateway process or a bound session.
6. `hermes --profile dean a2a-tasks list --session local-83bdf06b...` reports the two stranded
   tasks on the Dean profile.
7. `/a2a tasks list` shows only the current session's tasks; `--unowned` shows adoptable tasks.
8. `/a2a tasks release --all --yes` releases the current session's tasks, and
   `/a2a tasks release --session X --all --yes` performs the same release for another session.
9. `/a2a tasks transfer --from X --to Y --yes` moves ownership in-session and leaves the new
   owner's backlog queued.
10. Both surfaces call the same core function: for the same database state they produce identical
    result lines and identical durable effects (asserted by test).
11. Slash command output never becomes a model turn or an injected message; an unknown subcommand
    or a missing `--yes` changes nothing.
12. `/a2a tasks` is usable in TUI and classic CLI sessions without a gateway reload.

## Open Questions

- Registered slash name: `a2a` recommended (`/a2a tasks ...`; free in core 0.22.2). Fallback
  `a2a-tasks` if a future built-in claims `a2a`. `/tasks` is already the `agents` alias.
- Confirmation in-session: mandatory `--yes` (recommended) or a two-step
  `/a2a tasks confirm <token>` for interactive TUI. The two-step adds state to the command; defer.
- Default subcommand: `/a2a` prints usage (recommended) or defaults to `list`.
- Should `/a2a tasks list` accept `--all-sessions` as a shorthand for the operator view, in addition
  to `--session <id>`? Recommended: not in v1.
- Is `transfer` needed in v1, or is `release` plus the existing acquire tool enough? Recommended:
  ship both in v1; they share the same transaction helper.
- Shell confirmation when `--yes` is omitted: prompt on a TTY, refuse non-interactively
  (recommended).

## Evidence

- Dean `a2a_tasks.db`: both task rows above, `owner_session_id=local-83bdf06b...`,
  `released_by_session_id=''`, `released_at=0`, `unowned_since=0`, no queued messages, no pending
  publications.
- Dean `a2a_watches`: `watch_97b41d635f704823` already `canceled`; cancelling a watch did not
  release ownership, which is the documented split between the two operations.
- The deployed 0.3.5 tool surface has no cross-session release: `a2a_release_task_ownership(all=true)`
  is scoped to the calling session.
- Core 0.22.2 command registry: `resolve_command("a2a")` returns None; `resolve_command("tasks")`
  returns the `agents` command alias.
- `hermes_cli/plugin_command_dispatch.invoke_plugin_command` passes `source`, `session_id`,
  `session_key`, and `platform` to slash handlers that declare them, which is how `/a2a tasks`
  defaults `--session` to the calling session.
- In-session command discovery is `ctx.register_command()`; the handler returns text and cannot
  block, so confirmation must be an argument.
- Core 0.22.2 `hermes_cli/main.py:_chat_runs_at_gateway` returns true for `None`, `chat`, and `rl`:
  every chat turn executes in the gateway, so an agent tool call requires one. The outbound A2A
  RPC itself uses `urllib.request` in `a2a_async_plugin/tools.py`; the transport is direct, the
  tool invocation is not.
