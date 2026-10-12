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

---

# Phase 2 / Task 3 + Task 4 follow-up notes

These entries record decisions and observations made while writing
``tests/recovery/test_taskstore_restart.py`` and
``tests/recovery/test_session_restart.py`` (Phase 2). Phase 2's
rules are stricter: we may only modify
``a2a_async_plugin/protocol.py`` or ``tools.py`` when a test
proves the CURRENT behaviour is actually incorrect for
terminal-row or session durability; the non-terminal inbound
restart-to-FAILED semantics must remain untouched.

## 11. Schema-drift SELECT (OperationalError) is silently swallowed

**Uncertainty.** ``TaskStore._recover_from_db`` previously caught
the transition log-level ``Exception`` blanket. That swallowed two
distinct failure modes:

* ``RuntimeError`` from ``_ensure_db`` when the DB's
  ``user_version`` is newer than ``A2A_TASK_SCHEMA_VERSION`` —
  this was the documented "fail loudly" path but in practice
  the recovery loop's catch hid the failure.
* ``sqlite3.OperationalError`` from the SELECT statement when a
  column has been renamed or removed — silently coming up
  empty is data loss from the operator's perspective.

**Choice.** The Phase 2 / Task 3 test
``test_newer_schema_version_raises_runtime_error`` proves the
RuntimeError path was incorrect — fixing it is in-scope per the
plan's "modify only if behaviour is incorrect" rule.

**Fix applied (minimal).** In ``protocol._recover_from_db``:

* Re-raise ``RuntimeError`` explicitly (the future-schema case).
* Wrap the per-row unpacking in ``try / except
  (IndexError, TypeError, ValueError)`` so a single malformed row
  logs and continues — "bounded error, no silent discard of the
  rest".

The blanket ``except Exception`` is preserved for transient I/O
errors (locked DB, permission) so first boot does not block on a
wedged file.

**Remaining gap.** An ``OperationalError`` from the SELECT
(rename/drop of a column) still falls into the
``except Exception`` branch — the store comes up empty. That is
silent data loss for the operator, and the Phase 3 follow-up
should either re-raise ``OperationalError`` or alert. The Task 3
test ``test_renamed_schema_column_fails_safely`` documents this
gap and skips until Phase 3 tightens it; the Phase 2 contract is
"terminal rows survive, future schema fails loudly" and that is
locked in.

## 12. in-process ``protocol._conv_dir()`` ignores the harness env

**Observation.** The ``isolated_profile`` fixture in
``tests/recovery/conftest.py`` sets ``HERMES_HOME`` in the env
dict that is passed to the gateway subprocess — but it does NOT
update ``os.environ["HERMES_HOME"]`` in the test process. So
when the test process calls ``protocol.persist_message`` /
``protocol.load_conversation`` directly (as Phase 2 / Task 4
does), those calls resolve ``HERMES_HOME`` to the platform
default (``~/.hermes``).

**Impact.** Without explicit redirection in the test, in-process
persistence writes land in the operator's real
``~/.hermes/a2a_conversations/`` — observable as accumulated test
records in production files after a failure run.

**Choice.** Phase 2 / Task 4 subprocess tests use
``monkeypatch.setenv("HERMES_HOME", gateway.profile_home)`` to
mirror the subprocess's hermetic home into the test process.
Without this the test silently pollutes the operator's
directory.

**Future harness improvement.** The ``isolated_profile``
fixture should also ``monkeypatch.setenv("HERMES_HOME", ...)``
in the test process by default; that is a one-line change but
out of Phase 2 scope (it would touch ``conftest.py``, which
the plan only allows Phase 1 to do).

##  14. ``_safe_name`` collapses distinct unsafe-char contexts

**Observation.** ``protocol._safe_name`` rewrites
non-alphanumeric characters to ``_``. Two context_ids that
differ only in unsafe characters (e.g. ``ctx/alpha`` vs.
``ctx_alpha``) end up in the same ``.jsonl`` file. This is
documented behaviour, not a bug; callers are expected to use
safe context IDs.

**Test impact.** ``test_unsafe_chars_in_context_id_are_sanitized``
locks in the distinct-safe-name case and asserts the loader
returns per-context messages, not merged ones.

##  15. ``_recover_from_db`` does not re-persist inbound non-terminal rows

**Observation.** When a non-terminal inbound row is converted
to ``STATE_FAILED`` with the marker ``[gateway restarted before
task completed]`` on reopen, the change is only made in the
in-memory ``self._tasks`` dict — ``_persist`` is not called.
A subsequent restart will re-read the same row from SQLite,
re-mark it FAILED, and overwrite the in-memory ``completed_at``
again. The on-disk ``reply`` is unchanged (still empty), so the
behaviour is bounded — the row stays identifiable as a restart
failure — but ``completed_at`` is not stable across multiple
restarts.

**Impact.** Not a bug per the plan: "Phase 2 locks in
terminal-row behaviour and documents the rest." Phase 3 may
choose to write the FAILED state back to disk on first
recovery so subsequent restarts see the terminal state.

## 16. Conversation persistence is file-based, not SQLite

**Observation.** ``protocol.persist_message`` /
``protocol.load_conversation`` write to
``$HERMES_HOME/a2a_conversations/<safe_name>.jsonl``, not to
SQLite. The plan acknowledges this explicitly in Task 4: "If
conversation persistence lives in files rather than SQLite,
test the file round-trip plus restart behavior explicitly."

Phase 2 / Task 4 exercises that path: append-only JSONL,
no concurrent writers (the plugin's HTTP request handlers are
serialized through Python's threading.Lock in ``_send_task``
and ``A2ARequestHandler``), and the on-disk bytes are the
authoritative record (not the in-memory cache — there isn't
one).

**Concurrency note.** ``protocol.persist_message`` opens the
file in append mode without an explicit ``flock``. Concurrent
writes from many HTTP request threads could interleave at the
POSIX level; in practice the line-buffered JSON+newline output
is short enough that the kernel's pipe-buffer flush never
tears a line on Linux, but a paranoid operator should wrap the
write in ``fcntl.flock`` if they observe torn lines. Not in
Phase 2 scope.

---

# Phase 3 / Task 5 + 6 + 7 follow-up notes

These entries record the decisions and observations made while
implementing ``tests/recovery/test_restart_contract.py``,
``test_caller_restart.py``, and ``test_callee_restart.py``
(Phase 3 / Tasks 5, 6, 7). Phase 3 is allowed to modify
``a2a_async_plugin/*.py`` when a test proves current behaviour
is incorrect for restart semantics; in this round no source
changes were required — the existing ``_recover_from_db`` policy
matches the contract documented in
``docs/restart-recovery-contract.md``.

## 17. The contract doc distinguishes on-disk vs API state

The matrix row 7 originally asserted the FAILED-with-marker
transition happens in the database. NOTES #15 already documents
that ``_recover_from_db`` updates the in-memory ``self._tasks``
dict but does NOT re-persist the FAILED transition. Phase 3
makes this distinction explicit in
``docs/restart-recovery-contract.md``:

* **API state** (what ``TaskStore.get`` returns in the restarted
  process): FAILED + marker + completed_at — observable.
* **On-disk state** (what the next ``sqlite3.connect`` reads
  from the same DB): the persisted WORKING + empty reply —
  unchanged.

The in-memory API is the operator-facing signal (it drives
``/metrics`` and the A2A ``GetTask`` JSON-RPC). The on-disk row
is the durability-of-truth (it survives a hard kill without
the in-memory conversion). Both are observable surfaces, and
both are tested in ``test_restart_contract.py::test_contract_matrix_row_7_inbound_nonterminal_policy_in_subprocess_restart``.

## 18. ``fail_orphans`` reads the in-memory ``created_at``

The watchdog's ``TaskStore.fail_orphans`` (protocol.py:983) reads
``rec["created_at"]`` from the in-memory ``_tasks`` dict, NOT
from the SQLite column. Tests that need to exercise the age
filter must backdate the in-memory dict (under
``store._lock``); the SQL ``created_at`` doesn't affect the
watchdog's decision but we keep both surfaces consistent for
clarity.

## 19. Caller subprocess path uses inline worker script

``test_caller_hardkill_outbound_task_remains_queryable_and_nonterminal``
drives the outbound caller from a real subprocess (Python
script written to ``tmp_path`` and SIGKILL'd by the test). The
worker inlines the SendMessage wire call and the
``TaskStore.create`` invocation so it doesn't depend on the
``tools._task_store`` factory's config-lookup side effects.
The worker adds the plugin repo and Hermes source tree to
``sys.path`` explicitly because the parent pytest's
``PYTHONPATH`` doesn't always include them.

## 20. ``A2AAdapter`` requires ``gateway.config.PlatformConfig``

The Phase 1 launcher constructs the adapter against a
``PlatformConfig(enabled=True, extra={"port": port, ...})``.
``test_callee_restart.py::test_clean_disconnect_resolves_all_pending_futures_and_clears_pending``
uses the same construction shape in-process so the watchdog
thread + ``_pending`` plumbing are real. The plan allows driving
public methods (even underscore-prefixed ones) on a real adapter
instance.

## 21. ``test_callee_restart.py`` does not exercise a live SSE
HTTP waiter

The plan calls for "restart while an HTTP waiter (SSE/long-poll
GetTask) is present." The Phase 1 launcher has no agent loop to
hold an inbound task in WORKING (NOTES #8), and the adapter's
``_prepare_task`` returns the FAILED terminal immediately when
``self._loop is None or self._message_handler is None`` — no
``_pending`` entry is created. We cannot exercise a live SSE
HTTP waiter through the harness.

The watchdog-protected-live-waiter behaviour (W1, W2) is
exercised via the ``fail_orphans(protected=...)`` public entry
point instead, with the test directly controlling whether the
task_id is in the protected set. This is the same code path the
real adapter's watchdog thread takes
(``adapter._watchdog_loop`` line 545-549); the only difference
is the source of the protected set (we drive it directly
instead of going through ``_pending``). A real production
gateway with an agent loop would exercise the full path; the
test does not regress the contract.

## 22. No source code changes were needed in Phase 3

Phase 3's hard requirement ("a caller restart alone NEVER
creates a '[gateway restarted]' failure on an OUTBOUND record")
was already satisfied by the existing ``_recover_from_db``
policy in ``protocol.py:729-734`` (gated on
``rec["direction"] == "inbound"``). The contract doc captures
this as the chosen policy; the tests pin it down. If a future
change to ``_recover_from_db`` regresses this invariant, the
Phase 3 tests will fail with a clear contract-violation
message.

---

# Phase 4 / Task 8, 9, 10 follow-up notes

These entries record the decisions and observations made while
implementing ``tests/recovery/test_reconciliation.py``,
``test_terminal_races.py``, and ``test_storage_failures.py``
(Phase 4). Phase 4 is allowed to modify
``a2a_async_plugin/*.py`` when a test proves current behaviour
is actually incorrect for the restart / race / storage-failure
semantics; in this round one source change was applied
(tightening ``_recover_from_db`` to re-raise the schema-drift
SELECT ``OperationalError``), and one source-facing gap was
documented honestly (no outbound retry / backoff — bounded
single-attempt per call, by design).

## 23. The outbound engine has no retry / backoff loop

**Observation.** ``a2a_async_plugin/tools.py`` has no retry or
backoff on any of the outbound client paths
(``a2a_call``, ``a2a_submit``, ``a2a_get_task``,
``a2a_await``, ``a2a_cancel``, ``a2a_steer``). Each call is
one synchronous HTTP request. Transient failures
(``HTTPError``, connection refused, timeout) surface as a
bounded ``"Error: ..."`` string returned to the caller; the
local TaskStore is not mutated by the failure.

**Phase 4 plan guidance.** "if none exists, document that in
NOTES rather than inventing one."

**Decision.** Document honestly. The
``test_reconcile_peer_permanently_unavailable_no_unbounded_retry``
test asserts that N successive calls against an unreachable
peer each return a bounded error within the per-call HTTP
timeout window, and that the local row is never advanced as
a side effect. There is no unbounded retry loop to regress.

**Impact.** Callers that need retry semantics (e.g. for a
flaky network) must implement them at the application layer
above the A2A tools. The plan reserves the right to add
backoff in a future phase if production telemetry shows the
absence is a real reliability problem.

## 24. Schema-drift SELECT now re-raises (closes Phase 2 skip #11)

**Observation.** ``_recover_from_db`` previously caught
``RuntimeError`` and ``OperationalError`` from the recovery
SELECT in a blanket ``except Exception`` block, swallowing
the schema-drift case as a silent-empty store. The Phase 2
``test_renamed_schema_column_fails_safely`` documented the
gap with a ``pytest.skip``; the Phase 3 contract
(``docs/restart-recovery-contract.md`` §7 P4) reserved the
right to tighten this to a loud failure.

**Decision.** Tighten. Phase 4 / Task 10 closes the skip.
The new handling in ``protocol._recover_from_db`` (around
line 716) re-raises ``OperationalError`` AND ``RuntimeError``
when the error message contains "no such column", "no such
table", or "schema" — those are the schema-drift signatures.
Connection-level errors (database is locked, unable to open)
carry different messages and stay in the transient-I/O
branch.

**Source change (one-line scope).**
``a2a_async_plugin/protocol.py`` only:

* Added ``import sys`` (needed by ``sys.exc_info()``).
* Added the (RuntimeError, OperationalError) branch in
  ``_recover_from_db`` that re-raises for schema-shaped
  messages.

**Test impact.** The Phase 2
``test_renamed_schema_column_fails_safely`` still passes (it
accepts either a raise or a skip; the new code takes the
raise path). The new
``test_storage_schema_drift_select_raises_operational_error``
asserts the loud path explicitly. No Phase 1-3 test
regressed.

## 25. Fake peer extended with fault-injection knobs

**Observation.** The Phase 1 ``fake_peer.py`` had a
delayed-completion control surface (``complete`` / ``fail``
/ ``cancel`` / ``hold``) but no fault-injection surface for
malformed responses, 5xx errors, or timed-out replies. The
plan required the Phase 4 / Task 8 tests to "extend the
fake peer ONLY if a needed fault mode is missing (note the
extension in NOTES)."

**Extensions applied (minimal).** Three additions to
``tests/recovery/fake_peer.py``:

1. ``FakePeer.fault_mode: dict`` attribute (default ``{}``)
   + ``set_fault_mode(mode)`` method to replace it
   wholesale. Replaced atomically so reads from the HTTP
   handler thread are race-free without a separate lock.
2. ``_handle_get`` checks ``fault_mode`` first and short-
   circuits to: ``delay_get`` (sleep N seconds),
   ``malformed_get_garbage`` (non-JSON body),
   ``malformed_get_wrong_shape`` (valid JSON, result is a
   list), ``malformed_get_missing_fields`` (task dict
   without ``status`` / ``state``), ``http_500`` (HTTP 500).
3. ``_handle_send`` checks for the matching
   ``malformed_send_*`` / ``http_500_send`` kinds. The
   ``__ctrl__/cancel`` control command was also added so
   tests can drive a peer-side cancellation without going
   through the JSON-RPC ``CancelTask`` op.

**No production-impact change.** ``fake_peer.py`` lives in
``tests/recovery/`` and is not part of the plugin's runtime
surface. The extensions are opt-in via ``set_fault_mode``;
the default ``fault_mode={}`` preserves every Phase 1-3
behaviour (verified by re-running the full pytest suite —
97 passed, 0 skipped).

---

# Phase 5 / Task 11 + 12 follow-up notes

These entries record the decisions and observations made while
implementing ``tests/integration/test_hermes_compatibility.py``
(Phase 5 / Task 11) and ``tests/interoperability/test_a2a_python_sdk.py``
(Phase 5 / Task 12). Phase 5 is allowed to modify
``a2a_async_plugin/*.py`` when a test proves current behaviour is
incorrect for compatibility / interop; in this round no source
changes were required.

## 26. ``a2a_send_task`` / ``_rpc_request`` are the right entrypoints
for cross-implementation interop tests

The plan's instruction "plugin's outbound client engine (tools.py
entrypoint, NOT a hand-rolled curl)" is satisfied by driving
``a2a_async_plugin.tools._send_task`` (synchronous path used by
``a2a_call``) and ``a2a_async_plugin.tools._rpc_request`` (raw
JSON-RPC used by ``a2a_submit`` / ``a2a_get_task`` / ``a2a_cancel``).
Both are the same code paths the public tools emit, so the
interoperability test exercises the production wire surface
without re-implementing it. The test is the cross-impl proof, not
a re-derivation of the wire format.

## 27. SDK 1.1.2's ``JsonRpcTransport`` always emits v1.0 ``SendMessage``

The SDK's high-level client (``ClientFactory.create_from_url``)
reads the card's ``supported_interfaces`` and emits the v1.0
``SendMessage`` PascalCase method whenever the card advertises a
v1.0 entry. To exercise the legacy ``message/send`` alias the
interoperability test drops down to raw ``httpx`` and emits the
exact JSON-RPC envelope the adapter's legacy branch expects.
This is the only way to drive both method names against a single
card that advertises v1.0 (the SDK's high-level surface
deliberately prefers the v1.0 method per the protocol version
negotiation contract).

## 28. SDK 1.1.2 ``TaskState`` is a protobuf enum, ``str()`` is the int

The SDK's ``TaskState`` field is a protobuf ``EnumTypeWrapper``,
not a plain Python ``enum.IntEnum``. Calling ``str(task.status.state)``
on a parsed task returns the integer value (``"4"`` for
``TASK_STATE_WORKING``), not the enum name. The interoperability
test's SDK sub-script looks up the canonical name via
``TaskState.Name(value)`` before serialising the result on stdout
so the parent pytest assertion can compare against the string
labels the plugin emits (``"TASK_STATE_WORKING"`` etc.).

## 29. SDK 1.1.2 server's ``AgentCard`` schema: no top-level ``url`` or ``protocol_version``

The SDK 1.1.2 ``AgentCard`` schema dropped both the legacy
top-level ``url`` field and the ``protocol_version`` field. The
RPC URL is now in ``supported_interfaces[].url`` and the protocol
version is per-interface (``AgentInterface.protocol_version``).
The minimal SDK echo server builds the card accordingly. The
plugin's adapter is the consumer that *serves* the card, not the
producer of the v1.1.2 SDK card — its own card-building code
(``protocol.build_agent_card``) is correct for the plugin's
A2A v1.0 surface.

## 30. The hermes runtime venv leaks into subagent PYTHONPATH and
breaks the SDK's pydantic_core ABI

When the SDK venv's ``pip`` runs in a subagent shell, the
parent's ``PYTHONPATH`` may carry ``/home/hermes/.hermes/installs/``
(a Python 3.14 venv). The SDK venv is Python 3.13, and Python 3.13's
``pydantic_core`` C extension is incompatible with the 3.14 ABI;
the resulting import failure surfaces as
``ModuleNotFoundError: No module named 'pydantic_core._pydantic_core'``
at the SDK module's first protobuf import.

``setup_sdk_env.sh`` strips every PYTHONPATH entry matching
``/home/hermes/.hermes/installs/`` or ``/home/hermes/.local/``
before every ``pip`` invocation. The same sanitisation is
necessary for the SDK sub-scripts (``sdk_client_v1.py``,
``sdk_client_legacy.py``) — the test harness runs them with a
cleaned env (``{"PATH", "HOME", "LANG"}`` only). The pytest parent
process is unaffected because it does not import the SDK; only
the SDK venv's python sees the leak.

The reproduction recipe (kept here so future operators do not have
to rediscover it): ``PYTHONPATH=/home/hermes/.hermes/installs/.../venv/lib/python3.14/site-packages
./tests/interoperability/.venv-sdk/bin/python -c 'import a2a'`` fails
with the pydantic_core ABI error. Run with a clean env and it
imports cleanly.

## 31. Hermes ``plugins list --json`` is the right surface for a
machine-readable installation check

The plan's (b) "verify a2a-async installed/enabled" check has two
plausible surfaces: the rich-table text (default ``hermes
plugins list``) and the JSON variant (``--json``). The JSON
surface is stable across recent Hermes versions and parses
without a table-rendering regex. The test prefers ``--json`` and
falls back to a regex search on the rich-table text only if the
JSON parse fails. The recorded a2a-async row (name, status,
version, source, description) is captured into the test's
``__notes__`` for the post-run report.

## 32. Live peer (c) auth lives in a narrowly-scoped test-local
file, not the test code

The plan's (c) "use the deferred a2a tools" reads naturally for a
subagent that has those tools in its MCP context. Pytest does
not have that context. The conftest at
``tests/integration/conftest.py`` reads a bearer token from a
narrowly-scoped file ``tests/integration/.a2a_live_token`` (added
to ``.gitignore``) and exports it under the standard
``A2A_BEARER_TOKEN`` name. The conftest never invents a token —
the file is operator-supplied; without it, (c) skips with the
plan's "skip if the peer probe is unreachable or not authed"
guard. This keeps secrets out of test code, out of HANDOFF.md,
and out of the test report.

## 33. The SDK's ``AgentExecutor.execute`` must enqueue a Task
before any ``TaskStatusUpdateEvent``

The SDK 1.1.2 ``active_task`` consumer validates the event stream
and raises ``InvalidAgentResponseError("Agent should enqueue
Task before TaskStatusUpdateEvent event")`` if the executor
emits a status update before the initial Task object. The
``sdk_echo_server.py`` follows the SDK's own helloworld sample
exactly: check ``context.current_task``; if absent, call
``new_task_from_user_message(context.message)`` and
``await event_queue.enqueue_event(task)`` BEFORE any
``TaskUpdater.update_status(...)`` call. This is documented in
``a2a.server.agent_execution.AgentExecutor.execute``'s docstring
but easy to miss; the failure mode is a hard protocol error from
the SDK consumer, not a hang.

## 34. The SDK 1.1.2 server requires a v1.0 ``supported_interfaces``
entry; the legacy ``url`` field is rejected

``a2a.types.AgentCard`` in 1.1.2 does NOT expose a top-level
``url`` field (see #29). Constructing an AgentCard with
``url=...`` raises ``ValueError: Protocol message AgentCard has
no "url" field.`` The ``sdk_echo_server.py`` builds the card
with ``supported_interfaces=[AgentInterface(url=..., protocol_binding="JSONRPC", protocol_version="1.0")]``
and omits the legacy top-level ``url``. The plugin's adapter
(which is the *consumer* in test 1 and test 2, the *producer* in
test 3) reads its own card-building code at
``adapter._build_card`` → ``protocol.build_agent_card``; that
code path correctly produces the v1.0 ``supported_interfaces``
shape. The two surfaces line up.

---

# Phase 6 / Task 13 follow-up notes

These entries record the decisions and observations made while
building ``docs/a2a-conformance.md`` and
``tests/conformance/test_a2a_conformance.py`` (Phase 6 / Task 13).
Phase 6 is documentation-and-evidence only — no source changes
were required, but the conformance audit surfaced two real
MUST-level gaps (see G1 and G2 in the doc §11) and four
deliberate deviations. The audit's row count is 87; the gap
list is 10 items, two of which (G7, G8) are opt-outs
correctly handled by the plugin's own capability advertisement.

## 35. The conformance test must reuse the recovery harness, not duplicate it

The Phase 1 launcher harness (``gateway`` / ``isolated_profile`` /
``free_port``) is the single source of truth for the
A2A-gateway-subprocess lifecycle. Phase 6 needs the same
fixtures from a sibling directory (``tests/conformance/``);
pytest's auto-discovery walks the tree DOWNWARD, so a
conftest in ``tests/recovery/`` is not visible to tests in
``tests/conformance/``.

**Decision.** Add a tiny ``tests/conftest.py`` that imports
``tests/recovery/conftest.py`` via ``importlib.util`` and
re-exports the three fixture objects by reference. Pytest's
auto-discovery then picks them up for every test below
``tests/`` (including the conformance sibling). A
``pytest_plugins = ["tests.recovery.conftest"]`` approach was
rejected because pytest auto-discovers the recovery
conftest when it walks the recovery directory, causing
"Plugin already registered under a different name" on
collection.

**Side effect.** ``tests/__init__.py`` was added so that
``tests`` is a regular Python package (the import path
``tests.recovery.conftest`` requires it). The existing
``pytest.ini --ignore=__init__.py`` flag still prevents the
file from being collected as a test module.

## 36. The recovery harness's env teardown leaks across test directories

**Observation.** ``isolated_profile`` (in
``tests/recovery/conftest.py`` line 196-202) does
``os.environ.pop("HERMES_HOME", None)`` in its teardown. The
recovery suite doesn't notice because every test in that
suite also goes through ``isolated_profile`` (which re-sets
``HERMES_HOME`` for the subprocess env). The integration
suite (``tests/integration/test_hermes_compatibility.py``)
does NOT touch ``HERMES_HOME`` directly — it relies on the
integration conftest's ``pytest_configure`` to set it once
at session start.

**Failure mode.** Once the conformance suite (which uses the
recovery harness) runs before the integration suite (alphabetical
collection order: ``conformance`` < ``integration``), the
recovery teardown pops ``HERMES_HOME``, and the next
integration test sees a missing HERMES_HOME. The operator's
sven-profile ``a2a_agents`` config is keyed off ``HERMES_HOME``;
without it, ``_resolve_peer("dev-a")`` returns ``None`` and
the test fails with "unknown agent 'dev-a'."

**Fix.** A function-scoped autouse fixture in
``tests/conformance/conftest.py`` saves ``HERMES_HOME`` at
the start of every conformance test and re-installs it
after. The integration tests see the restored value.

**General principle.** The recovery harness's env mutation
is a known leak in the cross-directory test ordering. A
cleaner long-term fix would be to make the recovery
``isolated_profile`` fixture save-and-restore the entire
env, not just pop the four keys. Out of scope for Phase 6
(Task 13 only); flagged here so a future tightening pass
can address it without rediscovering the issue.

## 37. Two real MUST-level gaps surfaced, both unreachable from a well-behaved v1.0 SDK

The conformance audit found two MUST-level gaps that an
official `a2a-sdk` 1.1.2 client would never hit in
practice but that a strict v1.0 conformance validator
would fail on:

* **G1 (Low)**: ``SubscribeToTask`` on a terminal task MUST
  return ``UnsupportedOperationError`` (-32004). The
  plugin's ``_rpc_tasks_subscribe`` line 1177-1179 silently
  emits ``: done`` and closes the SSE stream instead. The
  SDK's subscribe client surfaces this as a normal
  stream-closure; a hand-rolled v1.0 conformance test
  would fail.
* **G2 (Low)**: ``A2A-Version: 0.x`` (anything not "1.0"
  or "1.0.0") MUST return ``VersionNotSupportedError``
  (-32009). The plugin returns ``ERR_INVALID_PARAMS``
  (-32602) with a descriptive message. The SDK's
  ``JsonRpcTransport`` always emits "1.0" so the SDK
  client never hits the gap.

Both gaps are documented honestly in
``docs/a2a-conformance.md`` §11. The release decision
belongs to Rob; if the deployment ever expects
non-SDK v1.0 strict peers, both gaps need a one-line
fix in the adapter.

## 39. Review fixes land in tests/, not the operator's config (2026-10-03)

Post-review cleanup (Amy verdict SHIP WITH CHANGES, areas B and E):

- **B (portability gate).** `tests/integration/conftest.py` no longer
  redirects HERMES_HOME to an authoring-host profile and no longer
  forwards its bearer token by default. The live-peer (c) tests are
  opt-in through `A2A_LIVE_PEER_PROFILE` (absolute profile-home
  path). Credential channels when the gate is open:
  operator-exported `A2A_BEARER_TOKEN`/`A2A_PEER_TOKENS` win;
  otherwise the operator-created `tests/integration/.a2a_live_token`
  (still the only token file, still git-excluded). The
  `hermes plugins list` subprocess env build in
  `test_hermes_compatibility.py` adopts the gated profile the same
  way, and its PATH override is derived from `HERMES_TREE` (itself
  overridable via `A2A_HERMES_TREE`) instead of hardcoding machine
  paths. `tests/conftest.py` autouse restore now covers
  `A2A_BEARER_TOKEN`/`A2A_PEER_TOKENS` symmetrically with
  HERMES_HOME, including restoring "absent" state.
- **E (contract P4 contradiction).** `restart-recovery-contract.md`
  §7 P4 rewritten to the shipped semantics (schema-shaped SELECT
  OperationalError re-raised; transient connection-level errors
  still caught; message-string matching documented as a known
  deviation pending errorcode-based tightening); traceability rows
  added for P3 and P4. The doc and the code now agree.
- The message-sniff -> `sqlite_errorcode` conversion in protocol.py
  is a real improvement (review area A) but is behavior-affecting
  source work, so it is queued with the other pre-tag fixes rather
  than slipped into this docs/tests-only pass.