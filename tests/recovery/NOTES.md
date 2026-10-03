# Phase 1 recovery-harness uncertainties

This file records Hermes-API ambiguities or interpretations made while
implementing the Phase 1 harness. Each entry names the uncertainty,
the choice the harness makes, and the impact on future phases.

## 1. Which gateway entrypoint to spawn

**Uncertainty.** The plan says "use the installed Hermes source tree
at /home/hermes/.hermes/hermes-agent with its .venv python to spawn
`hermes` gateway or the equivalent module entry." Three plausible
candidates:

1. `hermes gateway run` (the full Hermes gateway CLI) — wires up the
   Hermes plugin loader, Telegram, profile bootstrap, and the whole
   model-launching surface.
2. `python -m gateway.run` — the gateway runner module, slightly lower
   level but still pulls the full startup chain.
3. A minimal subprocess that runs the plugin's `register(ctx)` against
   a stub `Context`, then instantiates the registered `A2AAdapter`
   against a minimal `PlatformConfig(extra={"port": PORT})` and runs
   `adapter.connect()` / `disconnect()` directly.

**Choice.** Option 3 (the launcher in `_gateway_launcher.py`).

**Why.** Option 1 needs a configured provider and an active Hermes
profile to even import cleanly — both are environment-coupled and
flake in CI. Option 3 gives us a real subprocess with a real PID
we can SIGKILL, a real HTTP server we can hit over loopback, and the
plugin's real `TaskStore` SQLite lifecycle — exactly the surfaces the
restart tests need to exercise — without the unrelated messaging
gateways. This matches the plan's call-out for "the most headless-
reliable entry point."

**Impact on later phases.** Any Phase 2+ test that wants to exercise
the *full* Hermes gateway loop (Telegram / model launcher / session
compaction etc.) needs a different harness. The current launcher is
sufficient for restart / recovery tests because the plugin's surfaces
are self-contained behind the A2A HTTP server.

## 2. How the launcher loads the plugin

**Uncertainty.** The plugin repo has two `__init__.py` files:

* `/.../a2a_async_plugin/__init__.py` — defines `register_tools`
  (only).
* `/.../__init__.py` (repo root) — defines `register(ctx)`.

`import a2a_async_plugin` resolves the *inner* package and exposes
`register_tools`, not `register`. The Hermes plugin loader
(`hermes_cli/plugins.py`) reads the `plugin.yaml` manifest and loads
the repo-root `__init__.py` as the plugin module — same pattern the
existing `tests/test_plugin.py` uses (`importlib.util.spec_from_file_
location`).

**Choice.** The launcher mirrors the existing test pattern:

```python
spec = importlib.util.spec_from_file_location(
    "a2a_async_plugin",
    "<repo>/__init__.py",
    submodule_search_locations=["<repo>"],
)
```

so `from a2a_async_plugin.adapter import A2AAdapter` and friends
resolve correctly in the subprocess.

**Impact.** If the repo root ever stops being the plugin's loadable
module, the launcher needs to change in parallel with the existing
`tests/test_plugin.py`.

## 3. Adapter instantiation: what `PlatformConfig` looks like

**Uncertainty.** `PlatformConfig` is a `@dataclass` with a fixed
field set (`enabled`, `token`, `api_key`, `home_channel`, `reply_to_
mode`, `gateway_restart_notification`, `typing_indicator`,
`typing_status_text`, `channel_overrides`, `extra`). It has no
`name` field — that's carried by `Platform` (an `Enum`). The adapter
only reads `config.extra.get("port")` and `config.extra.get("agent_
name")`.

**Choice.** The launcher instantiates
`PlatformConfig(enabled=True, extra={"port": port, "agent_name":
"recovery-gateway"})`. No `name=`, no `connection_mode=`.

**Impact.** None — but if a future adapter version reads additional
typed attributes on `PlatformConfig`, this construction site is the
only place to update.

## 4. Adapter lifecycle: signal handlers in the launcher

The plugin's `A2AAdapter` is registered as a `BasePlatformAdapter`
subclass whose lifecycle is `await adapter.connect()` /
`await adapter.disconnect()`. The adapter itself does not install
signal handlers — it's the launcher's responsibility.

**Choice.** The launcher's asyncio loop installs `SIGTERM` and
`SIGINT` handlers that flip a stop `asyncio.Event`; the main coroutine
awaits the event and then calls `await adapter.disconnect()` before
returning. `SIGKILL` is intentionally NOT handled — the plan calls
for a hard kill that bypasses cleanup. The harness's `kill()` method
uses `subprocess.Popen.send_signal(SIGKILL)` directly.

**Impact.** The clean stop runs `adapter.disconnect()`, which shuts
the HTTP server (`ThreadingHTTPServer.shutdown()`) and joins the
watchdog thread. After SIGTERM, the SQLite WAL file may still be open
but WAL guarantees a clean shutdown regardless. After SIGKILL, WAL
sidecars (`<db>-wal`, `<db>-shm`) can be left behind; the harness
unlinks them on teardown.

## 5. Bind host in hermetic mode

The plugin's `A2ASecurityContext.resolve_bind_host()` returns
`127.0.0.1` unless both a token AND a wider host are configured.
The harness unsets `A2A_BEARER_TOKEN` / `A2A_PEER_TOKENS` /
`A2A_HOST` in the subprocess env to force loopback.

**Note.** If the operator's parent shell has `A2A_HOST=0.0.0.0` set,
the hermetic env-wipe in `_sanitize_env()` strips it. Tests can rely
on `127.0.0.1` always.

## 6. PID / port reporting on test failure

**Uncertainty.** The plan says "each test failure reports PID, port,
DB path, profile home."

**Choice.** The `gateway` fixture wraps the test body in a
`report_failure_on_error` context manager that appends a formatted
block to the exception's `__notes__`. Pytest's `--tb` shows `__notes__`
attached to the failing exception.

Additionally a `pytest_exception_interact` hook appends a one-liner
to every failed recovery-test report so the harness context is visible
even when the test didn't go through the `gateway` fixture.

**Impact.** `capsys` is no longer needed for failure reporting — the
`__notes__` chain handles it. If pytest's `__notes__` rendering
changes, the diagnostic block could go silent; we keep the hook as a
defensive fallback.

## 7. Free port re-use across `restart()`

**Observation.** `restart()` re-uses the port the first subprocess
bound. The `free_port` fixture closes the probe socket so SO_REUSEADDR
lets the launcher rebind. Linux's default `TIME_WAIT` for the port
would normally block the rebind, but the `free_port` fixture uses a
slight grace period (typically < 0.5s) before the harness issues the
restart. This is implicit — if a CI environment is aggressively slow,
the `start_new_session=True` plus the clean `disconnect()` makes the
gap small.

## 8. TaskStore durability across restart — by design or by accident?

**Observation.** `_recover_from_db` (in `a2a_async_plugin/protocol.py`)
marks persisted non-terminal *inbound* tasks as `STATE_FAILED` with
reply `[gateway restarted before task completed]`. The plan calls
this out as the central risk to test in Phase 3.

**Current harness behaviour.** Phase 1 does NOT exercise this path —
the headless gateway has no agent to keep tasks in `WORKING`. Tasks
submitted to the gateway immediately return `STATE_FAILED` because the
async reply path has no agent to answer. So restart tests cannot
produce a held task on the gateway side; they exercise the peer's
HOLD semantics, not the gateway's.

**Action for Phase 3.** Either wire a stub agent that keeps tasks
in `WORKING`, or use the plugin's outbound-calling path with a fake
peer that holds the task — the latter is already how Phase 1 is
shaped. The latter requires a real outbound wire (e.g. the plugin's
`a2a_submit` tool) and a populated config.yaml; the harness does not
have this today. Either approach is acceptable; documenting it here
so Phase 3 picks the right one.

## 9. SQLite schema version

`TaskStore._ensure_db()` raises on schema versions newer than
`A2A_TASK_SCHEMA_VERSION = 3`. The harness never bumps this; if a
newer version is deployed, all Phase 1+ tests will start failing at
the `TaskStore` constructor. NOTES entry is intentional — a version
mismatch should be a visible test failure, not a silent ignore.

## 10. What the launcher writes to stderr

The launcher writes its log to `GATEWAY_LOG_PATH` (set by the conftest
to `<tmp_path>/gateway.log`). It does NOT write to stderr unless
something explodes; we keep stderr in `subprocess.PIPE` and surface it
in `RuntimeError` messages when the subprocess dies unexpectedly. The
8-KB cap on `subprocess.PIPE` is fine because the launcher only logs
to the file in normal operation.