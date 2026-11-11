# A2A Async Standalone Plugin Handoff

Date: 2026-11-10
Owner: Sven
Repository: https://github.com/rriggs/hermes-agent-a2a-async-plugin.git
Workspace: /home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin

## Current handoff status (2026-11-10)

The standalone plugin provides the inbound asynchronous A2A gateway adapter, durable protocol/task
state, security helpers, and outbound synchronous/asynchronous client tools.

Current state:

- `main` is `45e6b64`, the merge of PR [#23](https://github.com/rriggs/hermes-agent-a2a-async-plugin/pull/23)
  (outbound `DataPart`/`FilePart` output, subscription parts, schema 8). That merge closed Issues
  #14 and #20 and is tagged `v0.5.0`. Issue #21 (duplicate completion text) is the only open issue.
- The merge sits on `1f715d3`, the merge of PR [#22](https://github.com/rriggs/hermes-agent-a2a-async-plugin/pull/22)
  (container gate builds the `a2a-sdk` venv and runs the compatibility tests), which is on top of
  `7663468`, the merge of PR [#19](https://github.com/rriggs/hermes-agent-a2a-async-plugin/pull/19)
  (`fix/issue-13-inbound-file-parts`, Amy approved at `d171435`). PR #19 closed Issue #13 and
  shipped inbound `FilePart` materialization plus the body-deadline and locator fixes below.
- The container gate is `e2e/container/run-suite.sh` with `SDK_VENV_DIR` / `A2A_SDK_VENV` overrides
  and `A2A_INTEROP_WORK_DIR` for a read-only export; its three interoperability tests now run
  instead of skipping.

The plugin manifest declares:

- name: `a2a-async`
- version: `0.5.1` (0.5.0 plus the #21 duplicate-completion fix and the schema-8 to 9 message-table migration)
- kind: `platform`
- requires Hermes: `>=0.22.2`
- thirteen tools in the shared `a2a` toolset

## PR #19 review round 2 (2026-11-10)

Amy's request-changes review at `53df155` carried two reproduced findings. Both are fixed here:

- **P1 body-slot starvation.** Every POST charged one of two global body-read admission slots
  before reading. Two clients that sent `Content-Length` and stalled held both slots forever, and
  a complete `GetTask` waited five seconds for a slot and then got HTTP 429. Fix: a slot is charged
  only for declared bodies over `256` KiB, and `_read_body()` enforces a 30-second wall-clock
  deadline (not a per-read timeout, so a drip feed cannot extend it). A timeout answers `408` and
  closes the connection. See `a2a_async_plugin/adapter.py` (`_BODY_SLOT_MIN_BYTES`,
  `_BODY_READ_TIMEOUT`, `_read_body`).
- **P2 attachment path truncated out of `a2a_get_task`.** The tool sliced the concatenated
  extraction at 2,000/4,000 characters, so a long TextPart ahead of a FilePart dropped the path of
  a file that had already been written to disk. Fix: `protocol.extract_text(..., max_chars=)`
  budgets prose separately from file locator lines, which are always rendered (bounded at 20,000
  characters with an explicit omission marker). The tool passes its existing 2,000/4,000 budgets
  through instead of slicing the result.

Also fixed in this round: `tests/recovery/test_caller_restart.py` hardcoded the operator workspace
path for the worker's `sys.path`, so in any linked worktree the recovery worker imported a
different tree (and wrote a schema-8 DB that this branch then refused to open). It now uses
`PLUGIN_REPO` / `HERMES_TREE` from `tests/recovery/conftest.py`.

### Round 3 (same day): the deadline did not survive a drip feed

Amy's re-review at `66bf09b` retired both original findings and found one new P2: the absolute body
deadline was enforced only between buffered reads. `rfile.read(64 KiB)` issues as many underlying
socket reads as it needs, each restarting the idle timeout, so a peer sending one byte every 50 ms
kept a large-body permit past the budget (reproduced here: no response after 0.9 s on a 0.25 s
budget, one permit still held). `_read_body()` now uses `read1`, which performs at most one raw
read, and recomputes the remaining absolute deadline before every read. Regression:
`test_drip_fed_body_still_hits_the_deadline_while_the_drip_continues` asserts the 408 and the
returned permit while the drip is still running; it fails at `66bf09b` with
`no 408 after 0.800s with a 0.25s body deadline; drips=15, free_slots=1`.

Verification of this round (worktree `fix/issue-13-inbound-file-parts`; round 2 ran uncommitted at
`66bf09b`, round 3 at `cc6a408`):

- Host suite: 389 passed, 3 skipped with the `tests/interoperability/.venv-sdk` venv present; a
  clean checkout without that venv is 386 passed, 6 skipped. The three remaining skips are the
  live-peer gates (`A2A_LIVE_PEER_PROFILE` unset by design) and the runtime-registration test,
  whose `HERMES_TREE/.venv/bin/hermes` path does not exist in this core checkout.
- Clean-export container suite at `cc6a408` (Podman, disposable home, loopback peer): 387 passed,
  5 skipped; `plugins validate` passed, `plugins doctor` reported 13 tools / 3 hooks with only the
  expected warnings.
- Amy's nine independent controls, run unchanged against the clean export at `/review/plugin`:
  9 passed (her drip control was the one failure she reported at `66bf09b`).
- TUI delivery E2E: PASS (`delivered: true`, watch `complete`, publication published, two status
  notifications).
- Classic CLI refusal E2E: PASS (`refused: true`, `peer_polls: 0`, `watch_rows: 0`).
- New regression tests fail on the tree they were written against and pass on this head:
  `test_stalled_attachment_bodies_do_not_starve_a_small_jsonrpc_request` reproduces the 429 on
  `53df155`, `test_incomplete_request_body_times_out_and_frees_the_slot`,
  `test_long_text_part_does_not_truncate_the_file_locator`, and
  `test_extract_text_budget_keeps_every_file_locator` fail on `53df155`;
  `test_drip_fed_body_still_hits_the_deadline_while_the_drip_continues` fails on `66bf09b`.

- Issue #14: `fix/issue-14-outbound-parts` is rebased onto `1f715d3` and is being gated now. The
  branch is local only, not pushed. It adds outbound `DataPart`/`FilePart` emission, the
  `reply_parts` store column (schema 8), and the configurable output modes documented below.
- Issue #21: open bug for duplicate completion text in history and artifact.

## Recent Agent Card identity fix

In a multiplexed gateway, `os.environ` can contain the default profile's bridged values. The adapter now resolves Agent Card identity in this order:

1. Scoped `A2A_AGENT_NAME` when running in a multiplexed profile scope
2. Unscoped `A2A_AGENT_NAME` for older/single-profile deployments
3. `platforms.a2a.extra.agent_name` from profile configuration
4. `hermes-<hostname>` fallback

The scoped lookup logs a warning if profile-scope resolution fails instead of failing silently.

Recommended per-profile configuration:

```yaml
platforms:
  a2a:
    extra:
      agent_name: kevin
      port: 9902
```

The code and tests cover scoped precedence, unscoped environment fallback, config fallback, and hostname fallback. The README documents the configuration surface.

## Inbound attachments (Issue #13)

Inbound A2A `FilePart` payloads are materialized beneath the active profile's `a2a_files` inbox and
rendered to the agent as task-scoped paths. Files are mode `0600`, task directories are mode `0700`,
raw payloads default to a 20 MiB per-file limit, and terminal inboxes default to a seven-day TTL.
URL parts are fetched only from globally routable HTTP(S) addresses with DNS-pinned connections,
redirect validation, no environment proxy, and the same byte limit. The Agent Card advertises
`text/plain` plus the active inbound media allowlist. JSON-RPC requests and peer responses have a
32 MiB body cap; at most two large (> `256` KiB) bodies are ingested concurrently, and every body
must complete within 30 seconds or the adapter answers `408` (see review round 2 above).

Configuration: `A2A_FILES_DIR`, `A2A_MAX_INBOUND_FILE_MB`,
`A2A_INBOUND_FILE_MEDIA_TYPES`, and `A2A_INBOUND_FILE_TTL_DAYS`. This shipped on `main` in PR #19
(merged 2026-11-10 as `7663468`) after Amy approved `d171435`; the two round-2 findings (stalled
bodies starving small requests, attachment paths truncated from listings) are fixed there and
covered by regressions.

## Outbound content parts (Issue #14)

The outbound path can return structured JSON as a `DataPart` and generated files as `FilePart`s.
Agent Cards default to `text/plain` and `application/json`, with text first; `a2a.output_modes`
configures global output modes and `a2a.skill_output_modes` configures per-toolset modes. Because
SendMessage does not name a selected skill, the dispatch path uses the union of the global and
per-toolset modes. A peer's `acceptedOutputModes` still gates each non-text part; if omitted, the
reply remains text-only for compatibility.

Local file parts are restricted to regular non-symlink files and a 20 MiB aggregate raw-byte cap per
reply. Safe public HTTP(S) URLs are carried as URL file parts. Terminal reply parts are stored in
`TaskStore.reply_parts` (schema version 8), returned only as the completed task's artifact, and
preserved across restarts. A completed task's result is never a status Message (A2A 1.0 §3.7);
status messages carry progress, clarification, and failure reasons only.

Attachments are assembled at the host delivery boundary, not inside `send()`. The base adapter's
`_extract_response_content()` strips `MEDIA:` directives and image links before the text send, so
the adapter overrides it to stash the extracted files, settles the task with text plus parts on the
send (or in `_deliver_attachments` for a file-only reply, where no text send happens at all), and
makes the native upload hooks return success because A2A carries files inside the task. A send
removes the entry from the stash as it takes it, so the encoded payload does not outlive delivery;
a turn that ends without delivering drops its entry, and the watchdog sweeps anything older than
120 seconds independently of the originating context.

Configuration example:

```yaml
a2a:
  output_modes: [text/plain, application/json, image/png]
  skill_output_modes:
    image: [text/plain, image/png]
```

Issues #14 and #20 shipped together in PR [#23](https://github.com/rriggs/hermes-agent-a2a-async-plugin/pull/23),
merged 2026-11-10 as `45e6b64` and tagged `v0.5.0`. Amy approved `7e412d0` after two repair rounds
(one P1 and four P2s, all reproduced and fixed on the branch). The tag is the release marker for
the schema-7 to 8 migration: an older plugin cannot open a v8 database, so deployments must move
forward, not back.

### PR #23 review round (2026-11-10)

- **P1 file emission was disconnected from host delivery.** Attachments were extracted only inside
  `send()`, but the host strips `MEDIA:` first and then calls the native upload hooks, so a real
  turn produced a text-only task and logged `send_image_file fallback`. Fixed at the boundary:
  `_extract_response_content` override (+ stash), settlement on the text send, `_deliver_attachments`
  for file-only replies, native hooks as success no-ops.
- **P2 omitted `acceptedOutputModes` still allowed a raw FilePart.** A `text/plain` attachment
  matched the legacy MIME list. Missing preferences now mean the text-carrier contract: no file
  parts at all.
- **P2 routed profiles bypassed negotiation.** `_prepare_task` returned the forwarded reply before
  capturing modes or building parts. The non-local path now negotiates, persists `reply_parts`, and
  returns the stored task.

All four of Amy's probes (three findings plus her non-blocking MEDIA-retention diagnostic) pass on
the fixed tree. The new regressions fail on `434f178` with the exact defects, verified in a detached
worktree.

### PR #23 review round 2 (`88120f9`)

Two P2s, no P1:

- **Routed profiles never emitted a FilePart.** The forwarded reply went straight into
  `build_output_parts()` without splitting its `MEDIA` carriers, so an advertised image mode still
  produced text with a path. The non-local path now extracts and encodes attachments with the same
  limits and validation as local replies, and the legacy fallback keeps the raw directive when the
  peer does not accept the carrier. Regressions cover accepted and rejected file modes on that
  route.
- **The attachment stash retained encoded payloads after delivery.** A claim marked the entry
  instead of removing it, the early return skipped the TTL, and only another extraction for the
  same context could clear it, so each completed turn kept a full base64 copy. A claim now pops the
  entry, turn completion drops it, and the watchdog sweeps expired entries regardless of context.
  Regressions cover delivered, file-only, failed, and abandoned-expired turns.

## SubscribeToTask output parts (Issue #20)

Issue [#20](https://github.com/rriggs/hermes-agent-a2a-async-plugin/issues/20) tracks a gap exposed
by #14: `_rpc_tasks_subscribe()` waited for the task state and reply, then called
`_emit_terminal()` without the persisted `reply_parts`, so a completed task subscribed through SSE
emitted a text-only status message instead of its stored `DataPart` or `FilePart` artifact.
`tasks/get` already rendered those parts.

Fixed in PR #23, since it is the same surface: the subscribe path re-reads the row after the
watcher resolves and passes its parts to `_emit_terminal`, and the already-terminal branch of
`message/stream` now renders the parts carried by the terminal task instead of re-extracting text.
Four regressions cover already-terminal and live subscriptions with both a `DataPart` and a
`FilePart`; all four fail on `87b9ae8`.

## Duplicate completion text (Issue #21)

Issue [#21](https://github.com/rriggs/hermes-agent-a2a-async-plugin/issues/21) reported the final
answer arriving twice: once as a progress note and once as a result. Two distinct mechanisms
produced it, and both are fixed.

**Emitter (our side).** `protocol.build_task()` put the reply parts in `status.message` AND cloned
them into `artifacts[0]`, so every completed task exposed its output twice on the wire. A2A 1.0
§3.7 says messages are not the output channel: "Messages SHOULD NOT be used to deliver task
outputs. Results SHOULD BE returned using Artifacts associated with a Task." A completed task now
carries its parts only in the artifact; `status_update()` refuses text for `COMPLETED`;
`_emit_terminal()` always emits the output as an artifact (falling back to a text part) with a
state-only status event; and a completed push notification carries `{"task": ...}` instead of a
status message with the answer. Non-completed states keep their status message: a clarification
request or a failure reason is communication, not output.

**Receiver (peer side).** A peer may still repeat its final text in history or `status.message`
and in the result artifact, which is a SHOULD violation a client must tolerate.
`OwnershipStore.record_messages()` now coalesces exact-text duplicates within one task, in the same
transaction that records them. The duplicate half is marked `suppressed_at` (schema 9), which is
deliberately NOT `delivered_at`: suppression retires a row from delivery without claiming the host
accepted it. When both are pending the Message is suppressed and the artifact stays queued, so a
refused artifact survives re-observation and is delivered when the host recovers (review round 3,
P1). When a Message was actually delivered, the artifact is suppressed instead. Suppressed rows are
excluded from `queued()`, from `publish_if_owned`, and from the outbox flush, which retires any
pre-enqueued copy. Distinct text, other tasks, and other sessions' tasks are untouched.

The receiver half also covers our own older emitters and any peer that violates §3.7. Tests assert
the host-accepted injection count through the real watch path, the blocking `a2a_await` return
value, and the store's undelivered rows; the pre-fix tree fails twelve of them, including the
blocking duplicate rendered as `[queued progress] ...` followed by the same text as the result.
Review round 3 added the refusal case: the host refuses the artifact, the payload is observed
again, and the result must still be queued and delivered on recovery. That regression fails on
`d47afe4`, where suppression had consumed the undelivered answer.

Gates on the fixed tree: host **444 passed, 3 skipped**; container **445 passed, 2 skipped** with
all three `a2a-sdk` interoperability tests; TUI delivery and classic CLI refusal E2Es PASS;
validate/doctor PASS (13 tools, 3 hooks). Plugin version is `0.5.1`, task schema is 9.

## Verification at merge (2026-11-10)

Tested on the branch head `7e412d0`, merged as `45e6b64`:

- Host `pytest tests -q -p no:cacheprovider`: **426 passed, 3 skipped** (the three live-peer gates;
  the SDK venv exists in this checkout, so the interoperability module ran).
- Clean-export container suite (`e2e/container/run-suite.sh`): **427 passed, 2 skipped**; the three
  `a2a-sdk` interoperability tests ran and passed.
- TUI delivery E2E: PASS; watch complete, result published.
- Classic CLI refusal E2E: PASS; `refused=true`, `peer_polls=0`, `watch_rows=0`.
- `plugins validate` / `plugins doctor`: passed inside the container gate (13 tools, 3 hooks).
- `git diff --check`: clean.

Amy's independent review of `7e412d0` reproduced the same numbers (427 passed, 2 live-peer skips;
3 interoperability tests; 39 probe and outbound-parts tests) and confirmed the pre-fix control at
`88120f9` fails exactly the five intended cases.

Regressions added in this round, each proven to fail on the pre-fix tree:
`test_host_response_delivery_attaches_the_media_file` and
`test_host_response_delivery_of_a_file_only_reply_settles_the_task` drive the inherited
`_process_message_background` pipeline (not a direct `send()`), `test_missing_output_preferences_keep_the_legacy_text_carrier`
uses a `text/plain` file part, and `test_forwarded_agent_honors_advertised_json` goes through
`_rpc_message_send` with a non-local agent. The stream regression still fails when
`_rpc_message_stream` drops `parts=parts`. For #20,
`test_subscribe_already_terminal_emits_the_persisted_parts` and
`test_subscribe_live_completion_emits_the_persisted_parts` are parametrized over `DataPart` and
`FilePart` and all four cases fail on `87b9ae8`. For the second delta,
`test_routed_profile_file_reply_returns_a_file_part` and the four stash-lifetime regressions
(delivered, file-only, failed turn, abandoned-expired) fail on `88120f9`.

Push payloads remain text-only; that is out of scope for #14/#20.

## Architecture

The root `__init__.py` is the directory-plugin entry point. It registers:

- all thirteen outbound/client tools under `toolset="a2a"`
- the inbound platform named `a2a`
- the `A2AAdapter` HTTP server through `ctx.register_platform`

Implementation package:

- `a2a_async_plugin/adapter.py` -- inbound HTTP server, Agent Card, inbound/outbound part handling, JSON-RPC dispatch, SSE, task lifecycle, push notification handling, watchdog/recovery behavior
- `a2a_async_plugin/protocol.py` -- A2A protocol and MIME negotiation helpers, SQLite `TaskStore`, task state, persistence, listing, recovery, metrics
- `a2a_async_plugin/security.py` -- authentication, bind safety, redaction, injection filtering, audit, push signing
- `a2a_async_plugin/tools.py` -- all thirteen client tool schemas and handlers

The plugin must be the sole inbound A2A platform owner. Do not run a duplicate bundled inbound A2A adapter alongside it. Synchronous and asynchronous tools remain in the single shared `a2a` toolset.

## Historical verification (2026-09)

Local verification using the Hermes source tree and virtualenv:

```text
PYTHONPATH=/home/hermes/.hermes/hermes-agent \
/home/hermes/.hermes/hermes-agent/.venv/bin/python -m pytest -q

7 passed
```

Also verified:

- Python compilation of plugin modules
- `git diff --check`
- direct external-loader registration
- ten tools register under `a2a`
- inbound platform registers as `a2a`
- Amy review of commits `7a354d8` and `61a95e3`: ship with changes, no blockers

Amy's findings were addressed in separate commits:

- `e5b58ad` -- avoid replacing process environment in tests
- `4c336cf` -- preserve scoped Agent Card name precedence and test resolution branches
- `b9ba581` -- cover hostname fallback
- `9be1215` -- document configured Agent Card names
- `a4d68e8` -- log scoped Agent Card name resolution errors

## Emerald deployment

The updated plugin was deployed to Emerald (`openclaw@10.100.0.3`) in all five profile plugin directories:

- `~/.hermes/plugins/a2a-async`
- `~/.hermes/profiles/kevin/plugins/a2a-async`
- `~/.hermes/profiles/stuart/plugins/a2a-async`
- `~/.hermes/profiles/dave/plugins/a2a-async`
- `~/.hermes/profiles/phil/plugins/a2a-async`

Emerald plugin validation passed. The multiplexed user gateway was restarted and verified active. Profile Agent Card names were configured as:

- default: `hermes-emerald`
- Kevin: `kevin`
- Stuart: `stuart`
- Dave: `dave`
- Phil: `phil`

Configured ports remain 9901 through 9905 respectively.

## Previously verified Alex results

Doctor against the deployed standalone plugin passed:

```text
Plugin Doctor: /Users/rob/.hermes/plugins/a2a-async
  manifest: a2a-async 0.1.6 (platform)
  OK: runtime discovery, manifest parsing, import, and registration passed
  registrations: 10 tool(s), 0 hook(s)
```

The default Alex gateway verified through the native A2A path:

- asynchronous submission
- immediate working task acceptance
- background completion
- `a2a_get_task`
- durable `a2a_list`
- cancellation
- context continuity and steering with distinct task IDs
- invalid bearer authentication rejection with HTTP 401
- Agent Card and shared ten-tool capability advertisement

An isolated temporary Alex profile also verified direct authenticated SSE streaming, including submitted, working, artifact, completed, and terminal events.

## Acceptance priorities

Rob's acceptance priorities, in approximate order, are:

1. Reliability, especially restart resilience, durable recovery, and reconciliation.
2. Compatibility with Hermes' existing A2A implementation.
3. Interoperability with at least one external implementation, preferably the official Python SDK/reference implementation.
4. Conformance with the A2A specification, with pragmatic flexibility where the protocol version or SDK transition requires it.

HMAC push signing is intentionally out of scope. The deployment does not cross trust boundaries, and A2A does not require HMAC for push callbacks.

## Open items

These are still open and should be addressed against the acceptance priorities above before calling the plugin production-complete:

1. Run restart/recovery and remote-state reconciliation in a temporary secondary profile. Preserve a durable nonterminal task, restart only the temporary gateway, then verify `tasks/get`, `tasks/list`, orphan handling, and reconciliation.
2. Verify compatibility with Hermes' existing synchronous A2A implementation and shared `a2a` toolset, including mixed synchronous/asynchronous operation in one gateway.
3. Run an interoperability test against the official Python A2A SDK/reference implementation, covering Agent Card discovery, message send, task polling, streaming where supported, and push notification delivery where practical.
4. Perform a focused A2A specification conformance review for the supported v1.0-shaped and legacy-compatible methods and payloads.
5. Expand the repository test suite beyond the current seven tests. Add focused adapter, security, TaskStore, SSE, push, restart/recovery, and loader-boundary coverage.
6. Investigate or document the `hermes plugins validate` / `hermes_state_ids` discrepancy observed on Alex. Doctor passed, but validation previously failed in an environment whose capability subprocess could not import `hermes_state_ids`.
7. Re-run validation against a clean tagged checkout and create a release tag only after the acceptance tests pass.
8. Re-run the full Hermes-side plugin Doctor and deployment checks after any release/tag operation.

The plugin is usable and deployed, but it is not yet production-complete because restart/recovery, Hermes compatibility, external interoperability, expanded acceptance coverage, and clean tagged-checkout verification remain incomplete.

## Phase 5: Hermes compatibility and external interoperability

Status: complete (Tasks 11 and 12 from
`docs/restart-session-resilience-plan.md`).

Pytest: `103 passed, 2 skipped` from a clean checkout run
(`PYTHONPATH=/home/hermes/.hermes/hermes-agent
/home/hermes/.hermes/hermes-agent/.venv/bin/python -m pytest -q`).
The 2 skips are the live-peer (c) tests — those tests are **opt-in**
through an explicit gate: set `A2A_LIVE_PEER_PROFILE` to a usable
profile home and the conftest forwards that profile's identity (and,
optionally, a token from `tests/integration/.a2a_live_token` or the
operator's own `A2A_BEARER_TOKEN`). On hosts without the gate the
tests skip cleanly with the plan's "skip if the peer probe is
unreachable or not authed" guard. The gate exists so the suite can
never silently adopt an ambient profile's peer configuration
(review finding, Amy, 2026-10-03, area B).

### Task 11 — Mixed Hermes A2A compatibility (`tests/integration/test_hermes_compatibility.py`)

* **(a) Registration / mix test, in-process, hermetic env (NOTES #12).**
  Three tests:
  - `test_registration_records_exactly_ten_a2a_tools_and_one_platform`:
    loads the repo-root plugin the way `tests/test_plugin.py` does
    (importlib against `__init__.py`), calls `register(ctx)` against
    a hermetic `HERMES_HOME`, and asserts the ten tools
    `a2a_discover / a2a_call / a2a_list / a2a_history / a2a_orchestrate
    / a2a_submit / a2a_get_task / a2a_await / a2a_cancel / a2a_steer`
    are all registered under `toolset='a2a'` with zero duplicate
    names, and the inbound platform named `'a2a'` is registered
    exactly once via `ctx.register_platform`.
  - `test_both_send_message_shapes_through_single_owner`: builds the
    adapter in-process (the same `_gateway_launcher.py` construction
    the Phase 1 harness uses), binds it on a loopback port, and
    asserts BOTH `SendMessage` shapes — synchronous (no
    `returnImmediately`, blocking) and asynchronous
    (`configuration.returnImmediately=true`, immediate
    `TASK_STATE_WORKING`) — produce valid JSON-RPC responses through
    the SAME adapter. This is the "both call shapes through one
    owner" mixed-mode proof.
* **(b) Real installation check, `hermes plugins list --json`.**
  `test_real_runtime_registration_via_hermes_plugins_list` runs
  `/home/hermes/.hermes/hermes-agent/.venv/bin/hermes plugins list
  --json` and asserts the a2a-async row has `status="enabled"` and
  `source="user"`, and that the description references the shared
  `a2a` toolset. Recorded row (current pinned HEAD):
  - name: `a2a-async`
  - status: `enabled`
  - version: `0.1.6`
  - source: `user`
  - description: `"Complete asynchronous A2A client and inbound
    gateway contributed to the shared a2a toolset."`
  The full ten-tool list assertion lives in (a); (b) only confirms
  the runtime is wired up. The skip-if-runtime-unavailable guard
  honours the plan's "never fail because the runtime is absent, but
  DO fail on a present runtime missing the registration" rule.
* **(c) Real peer loop, live evidence, plugin client → dev-a.**
  `test_real_peer_sync_via_a2a_call` and
  `test_real_peer_async_via_a2a_submit_then_get_task` drive the
  plugin's own outbound client (`a2a_call`, `a2a_submit`,
  `a2a_get_task`) at dev-a (a real Hermes agent on this host that
  runs the SAME async plugin as its inbound adapter). Skip guard
  (profile gate + TCP probe + auth check) prevents 401s and
  connect-refused from failing the run, and prevents silent
  adoption of an ambient profile (see the Phase 5 section above
  for the `A2A_LIVE_PEER_PROFILE` gate). When the gate is open and
  a token is present (drop it into `tests/integration/.a2a_live_token`
  — excluded from version control — or export `A2A_BEARER_TOKEN`
  yourself), the tests pass and record the exact task id, state, and
  timing in the test log.

#### Scope correction rationale

Task 11's plan wording ("standalone async plugin alongside Hermes'
existing synchronous A2A implementation in one gateway") predates
the fleet cutover — the standalone plugin is now the SOLE inbound
A2A platform owner (HANDOFF.md constraint: never two inbound
adapters). Therefore "mixed operation" means: ONE adapter owning
inbound, with BOTH synchronous-style calls AND async task-based
calls working through it, and the shared `a2a` toolset registering
all tools without duplicates. The test in (a) exercises exactly
this shape — the adapter handles both `SendMessage` call shapes
through one owner; the toolset registration is a single, deduped
list of ten tools. The plan's "Do not attempt to install or run a
second bundled inbound adapter" instruction was honoured — there
is no second adapter under test, just the one inbound owner with
both call shapes.

### Task 12 — Official Python A2A SDK interoperability (`tests/interoperability/`)

* **SDK version pinned: `a2a-sdk==1.1.2`** (the latest 1.1.x at
  acceptance time; install command in `setup_sdk_env.sh` is
  `a2a-sdk[http-server]==1.1.2`). Pinned in two places —
  `setup_sdk_env.sh::SDK_VERSION` and
  `test_a2a_python_sdk.py::SDK_VERSION` — and asserted at runtime
  via the `sdk_version` fixture. The venv is gitignored at
  `tests/interoperability/.venv-sdk/`; the setup script refuses to
  install outside that directory unless `SDK_VENV_DIR` names an
  absolute target, which is how a read-only checkout (the container
  suite) builds it; the test follows `A2A_SDK_VENV`. We never install
  the SDK into the Hermes runtime `.venv` per the HANDOFF.md constraint.
* **Setup script** (`setup_sdk_env.sh`): builds
  `tests/interoperability/.venv-sdk/` with `a2a-sdk[http-server]==1.1.2`
  + `uvicorn` + `httpx`. Strips the leaky
  `/home/hermes/.hermes/installs/` Python from `PYTHONPATH` before
  each `pip` invocation (a quirk of subagent execution: the
  subagent's `PYTHONPATH` can carry a Python 3.14 venv that breaks
  the SDK's `pydantic_core` ABI on a Python 3.13 venv). Idempotent
  on re-run; a venv whose pin matches the README is a no-op.
* **SDK server helper** (`sdk_echo_server.py`): a minimal
  a2a-sdk v1.0-compliant echo server (Starlette + uvicorn, stdlib
  + a2a-sdk only) that serves the v1.0 Agent Card at
  `/.well-known/agent-card.json` and `SendMessage` /
  `message/stream` over JSON-RPC. Spawned as a real subprocess by
  the test fixture.
* **Tests** (`test_a2a_python_sdk.py`):
  - `test_sdk_client_to_plugin_v1_method`: SDK client →
    plugin server using the v1.0 `SendMessage` method
    (PascalCase, A2A v1.0 §5.3/§9.4). The SDK's
    `JsonRpcTransport` emits the v1.0 method; the plugin's
    `A2ARequestHandler.do_POST` recognises it (v1_response=True)
    and returns the v1.0 envelope (`{ task | message }` oneof).
    Both sides parse without falling back to legacy shapes.
  - `test_sdk_client_to_plugin_legacy_method`: SDK client →
    plugin server using the legacy `message/send` method
    (snake_case, pre-1.0). Driven at the wire level with raw
    `httpx` because the SDK's high-level client always emits
    v1.0 `SendMessage` when the card advertises a v1.0
    supportedInterfaces entry; the legacy alias is reachable
    only at the JSON-RPC envelope level. Asserts the response is
    a bare Task payload (NOT a v1.0 envelope) per
    `adapter._method_info`'s v1.0/legacy discrimination.
  - `test_plugin_client_to_sdk_send_then_get`: plugin client →
    SDK server. Drives the plugin's outbound client engine
    (`a2a_async_plugin.tools._send_task` for sync,
    `a2a_async_plugin.tools._rpc_request` for the raw JSON-RPC
    paths used by `a2a_submit` / `a2a_get_task` / `a2a_cancel`)
    at the SDK echo server. Asserts: synchronous `_send_task`
    round-trip with on-disk conversation persistence intact;
    `SendMessage` returns a Task with non-empty id and
    contextId; `GetTask` returns the task in
    `TASK_STATE_COMPLETED` with the echo artifact in
    `artifacts[].parts[].text` (proves the plugin client parses
    the SDK's response shape correctly); `ListTasks` includes
    the task we just submitted; `CancelTask` on an
    already-terminal task surfaces as a valid protocol response
    (either a `result` or a JSON-RPC error with code `-32002`
    `TaskNotCancelable` — the test asserts either form; it does
    NOT hang).
* **Skip path:** when `tests/interoperability/.venv-sdk/` is
  absent, the whole module SKIPs with a clear message pointing at
  `setup_sdk_env.sh` (or `A2A_SDK_VENV`). Enforced in two places: `pytest_collection_modifyitems`
  (per-item marker) and module-scope fixtures
  (`sdk_version`, `plugin_gateway`, `sdk_echo_server`) call
  `pytest.skip(...)` themselves as a belt-and-braces.
* **Container gate:** `e2e/container/run-suite.sh` builds the venv at
  `/data/sdk-venv`, exports `A2A_SDK_VENV`, and fails the run unless
  `pytest tests/interoperability` reports `3 passed`, so the three
  compatibility tests cannot silently skip in CI-style runs. Writable
  scratch for the gateway profile and generated SDK sub-scripts is
  controlled by `A2A_INTEROP_WORK_DIR` (default: the repository, which
  is what the host runs use).
* **Per-test venv python invocation:** the SDK sub-scripts
  (test 1 and test 2) run under
  `/home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin/tests/interoperability/.venv-sdk/bin/python`
  — the path printed by `setup_sdk_env.sh`.
* **What was intentionally not done (recorded in
  `tests/interoperability/README.md`):** streaming (SSE) is
  exercised implicitly through the v1.0 `SendMessage` and
  `message/stream` JSON-RPC routes but the SDK's async SSE
  consumer is not run as a separate test; `SubscribeToTask` and
  the four push-notification config operations are not exercised
  in this round (long-lived SSE consumer + extended-card path);
  gRPC is out of scope (the plugin's adapter is JSON-RPC over
  HTTP only). All three are tracked for Phase 6 (Task 13
  conformance).

### Traceability (per `docs/restart-recovery-contract.md` §9)

| Method name | v1.0 / Legacy | Response envelope | Plugin source | SDK transport |
|-------------|---------------|-------------------|---------------|---------------|
| `SendMessage` | v1.0 | `{ task \| message }` (oneof) | `adapter._rpc_message_send(..., v1_response=True)` | `JsonRpcTransport.send_message` (test 1) |
| `message/send` | legacy | bare Task | `adapter._rpc_message_send(..., v1_response=False)` | raw `httpx` (test 2) |
| `GetTask` | v1.0 | bare Task | `adapter._rpc_tasks_get` | plugin client (test 3) |
| `ListTasks` | v1.0 | `{ tasks, nextPageToken, pageSize, totalSize }` | `adapter._rpc_tasks_list` | plugin client (test 3) |
| `CancelTask` | v1.0 | bare Task or `-32002` | `adapter._rpc_tasks_cancel` | plugin client (test 3) |
| `message/stream` | legacy | SSE | `adapter._rpc_message_stream` | not exercised in this round |
| `SubscribeToTask` | v1.0 | SSE | `adapter._rpc_tasks_subscribe` | not exercised in this round |
| `tasks/pushNotificationConfig/*` | mixed | per v1.0 §5.3 | `adapter._rpc_push_config_*` | not exercised in this round |

## Known gap: cancellation does not stop execution

Cancellation is spec-compliant but best-effort in the weakest sense. Per A2A
v1.0 §3.1.5, the server "will attempt to cancel the task, but success is not
guaranteed"; the task lifecycle (`TASK_STATE_*`) is the entire contract and the
protocol never binds what the server does behind it (opaque execution, §1.1).
Here, `tasks/cancel` (adapter.py `_rpc_tasks_cancel`) transitions the task
record to `CANCELED` synchronously and resolves the pending reply future, so
the caller sees terminal state immediately. However, the inbound task was
routed into the agent's live gateway session, and cancel does not abort that
in-flight turn: the session keeps processing to completion, its eventual reply
is discarded silently, and tokens/compute burn until then.

Quality-of-implementation gap, not a compliance gap. A stronger
implementation would abort the gateway turn (gateway-side interrupt by
message/task id). Not required for conformance; flagged here so nobody
mistakes `CANCELED` for "the work stopped."

Cancellation also carries no message. `CancelTaskRequest` is id-only in the
normative proto (`a2a_cancel` sends `{"id": task_id}`); a terminal task cannot
receive further messages (`UnsupportedOperationError`), so the protocol offers
no "last words" channel inside a cancel. The caller-side alternative is
steer-before-cancel (`a2a_steer` then `a2a_cancel`): it works but is a blunt
instrument -- the steer queues a NEW turn rather than interrupting the
in-flight one, so the peer briefly runs two tasks, and the steer's own task id
is typically orphaned.

Investigation direction (chosen): synthetic steer on cancel. Since the
in-flight session is not aborted anyway, `_rpc_tasks_cancel` could inject a
synthetic steering message into the live gateway session (context_id of the
canceled task) BEFORE completing the record, giving the agent a chance to
checkpoint/wrap up. Open questions before implementing:
- Gateway behavior when a turn is enqueued for a context whose A2A task
  record is already `CANCELED` (queue injection path, turn accounting).
- Race between the synthetic steer and the original in-flight turn --
  ordering guarantees in the gateway queue.
- Whether the steer task should be recorded (ownership/direction metadata)
  and who polls its response.
- Cost tradeoff: this deliberately spends tokens to elicit a clean
  checkpoint; make it conditional (config flag) if a caller wants hard-stop
  semantics.
- Rejection semantics: if injection fails, cancel proceeds anyway (best
  effort per spec); log the failure to the audit trail.

## Known gap: historyLength > 0 does not truncate history

A2A v1.0 §3.2.4: `historyLength: 0` MUST omit the history field
(honored), and `historyLength > 0` MUST return at most N most recent
messages. The plugin honors only `0`; `> 0` is parsed but the history
field is left absent (`protocol.py` `TaskStore.to_task`). The plugin's
conversation persistence is file-based (JSONL per context), not a
`Task.history` array; `load_conversation(context_id)` exposes it to the
outbound `a2a_history` client tool, but `GetTask`/`ListTasks` JSON-RPC
responses carry no `history`. Conformance checklist row: G6 (Medium) in
`docs/a2a-conformance.md` §11. A v1.0 client depending on
`task.history` will not get one; callers in our fleet use
`a2a_history`/`a2a_get_task` instead, so this is latent for our trust
model but visible to strict external peers.

## Phase 6: Conformance checklist and clean-checkout acceptance run (Task 13, Task 14)

### Task 13 -- conformance checklist

`docs/a2a-conformance.md`: 87 evidence-backed rows mapping every
implemented operation and Agent Card field to A2A v1.0 normative
sections, with implementation locations, test evidence, and status
(conformant / conformant-with-deviation / legacy-labeled / unsupported).
Asserted against the real adapter by `tests/conformance/
test_a2a_conformance.py` (21 tests).

MUST-level gaps, honestly listed (full detail in §11 of the checklist):

- G1 [Low]: `SubscribeToTask` on a terminal task must return
  UnsupportedOperationError (-32004); plugin instead emits the SSE
  `: done` termination (unreachable from the SDK client).
- G2 [Low]: non-1.0 `A2A-Version` header must return
  VersionNotSupportedError (-32009); plugin returns ERR_INVALID_PARAMS
  with a descriptive message.
- G4 [Low]: ListTasks pagination uses a numeric offset pageToken
  instead of a spec-style cursor.
- G5 [Low]: ListTasks sorts by `created_at` instead of
  `status.timestamp` desc.
- G6 [Medium]: `historyLength > 0` does not truncate/present
  `task.history` (only `0` is honored). Recorded as its own known-gap
  section above.
- G9 [Low]: error responses lack `data` (google.rpc ErrorInfo shape).
- G3 [Dormant]: ERR_PUSH_NOT_SUPPORTED declared but never emitted
  (push is advertised and supported).

All are explained against the single-trust-domain, bearer-auth
deployment model; none block fleet-scale use.

### Task 14 -- acceptance battery (exact outputs, 2026-10-03)

Development tree, full suite:

```text
PYTHONPATH=/home/hermes/.hermes/hermes-agent \
/home/hermes/.hermes/hermes-agent/.venv/bin/python -m pytest -q
126 passed in 62.80s
```

Acceptance directories only (recovery, integration, interoperability,
conformance):

```text
119 passed in 62.19s
python -m compileall -q a2a_async_plugin  # OK
git diff --check                          # clean
```

Clean checkout of `main` (fresh clone at HEAD `5e20030`):

```text
123 passed, 3 skipped in 62.35s
```

The 3 skips are the designed environment guards: the two live-peer
tests (no auth-token file in the clean clone) and the SDK interoperability
module (`.venv-sdk/` not set up there). Both skip paths are documented
above.

Plugin Doctor on the clean checkout:

```text
Plugin Doctor: <clean-checkout-path>
  manifest: a2a-async 0.1.6 (platform)
  OK: runtime discovery, manifest parsing, import, and registration passed
  registrations: 10 tool(s), 0 hook(s)
exit=0 (--ci)
```

## Operational constraints

- All normal code work stays under `/home/hermes/.hermes/profiles/sven/workspace`.
- Never use `/tmp` for code repositories, edits, builds, tests, or artifacts.
- Do not overwrite Alex's default profile.
- Do not print or transfer credentials.
- Do not run a duplicate inbound A2A gateway alongside the standalone plugin.
- Keep plugin version, release tag, and clean-checkout Doctor output consistent before catalog/review handoff.
