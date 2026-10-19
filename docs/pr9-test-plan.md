# PR #9 test plan: ownership, artifact delivery, lifecycle, and the plugin API gate

One runnable matrix for every defect raised against this branch: Amy's four findings on
`f3ddaaa`, the four local review rounds that followed, and the Hermes plugin API gate that the
first container run failed.

Every row names a committed regression test. Nothing in this plan needs a probe script that is
not in the repository.

## Environment

Host (Linux, Hermes source checkout on `PYTHONPATH`):

```sh
cd hermes-agent-a2a-async-plugin
PYTHONPATH=/home/hermes/workspace/hermes-agent-rriggs \
  /home/hermes/workspace/hermes-agent-rriggs/.venv/bin/python -m pytest tests -q
```

Container (real CLI + gateway, scripted model and A2A peer):

```sh
cd e2e/container
./run-container.sh /tmp/e2e-pr9           # image: localhost/hermes-e2e:local
```

Plugin API gate (uses the Hermes CLI, not pytest):

```sh
hermes plugins validate <clean plugin tree>
hermes plugins doctor   <clean plugin tree>
HERMES_HOME=<temp home> hermes plugins enable a2a-async
HERMES_HOME=<temp home> hermes plugins list --json
```

## A. Hermes plugin API gate

`validate` scans the directory it is given, including files git ignores. The working tree carries
`tests/interoperability/.venv-sdk/` (a test SDK venv with `.so`, `.exe` and interpreter symlinks),
so validate must run on a clean export:

```sh
rm -rf /tmp/pr9-clean && mkdir -p /tmp/pr9-clean
git archive HEAD | tar -x -C /tmp/pr9-clean
hermes plugins validate /tmp/pr9-clean
```

Expected: `Validation passed.` with two warnings, both accepted:

- `security scan caution: role_hijack (security.py:127)` -- the flagged string is one of the
  inbound injection patterns the plugin neutralises, not a payload. Caution, not failure.
- `private core imports` -- `hermes_cli.profiles`, `agent.redact.redact_for_egress` and
  `gateway.session_context` seams the plugin needs; catalog rule 9 warning, not failure.

`hermes plugins doctor` expected: runtime discovery, manifest parsing, import and registration
pass; 13 tools, 3 hooks.

`hermes plugins list --json` after `plugins enable` expected: `name=a2a-async`, `source=user`,
`status=enabled`, `version=0.5.0`.

Defect found by this gate and fixed here: `plugin.yaml` declared `provides_tools` but no
`provides_hooks`, while `register()` registers `on_session_start`, `on_session_reset` and
`on_session_finalize`. `validate` reported
`undeclared hooks registered (not in provides_hooks)` and failed. The manifest now declares all
three.

## B. Issue matrix

Run all rows in one pass:

```sh
PYTHONPATH=<hermes checkout> python -m pytest -q \
  tests/test_ownership.py::test_consume_if_owned_holds_the_write_lock_before_reading_ownership \
  ...
```

| ID | Sev | Issue | Regression test |
|---|---|---|---|
| Amy-1 | P1 | `consume_if_owned` read ownership outside the write transaction; release+adoption between SELECT and marks let the old owner consume the adopter's backlog | `tests/test_ownership.py::test_consume_if_owned_holds_the_write_lock_before_reading_ownership`, `::test_consuming_the_backlog_is_fenced_on_live_ownership` |
| Amy-2 | P1 | Artifact messages keyed on `artifactId` alone; a completed snapshot reusing a working snapshot's id delivered only the partial text | `tests/test_watches.py::test_a_completed_artifact_revision_is_not_dropped_as_a_duplicate`, `::test_the_blocking_recorder_keeps_a_new_revision_of_the_same_artifact` |
| Amy-3 | P2 | A refused publish followed by a direct backlog replay left the outbox row publishable: two accepted injections | `tests/test_watches.py::test_a_direct_replay_settles_the_pending_outbox_row`, `::test_publish_and_backlog_mark_commit_together` |
| Amy-4 | P2 | `stop()` dropped the thread handle after a timed-out join; `start()` cleared the shared stop flag and ran a second poller under one lease token | `tests/test_watches.py::test_stop_does_not_abandon_a_live_poller_for_a_second_one` |
| D1 | P1 | First fix acknowledged the backlog before writing the task row; a crash in between lost the only durable copy of the answer | `tests/test_watches.py::test_the_answer_survives_a_crash_after_the_fence` |
| D2 | P2 | Per-revision message ids turned one streaming artifact into one message per revision | `tests/test_watches.py::test_a_streaming_artifact_delivers_once_with_its_final_content`, `::test_a_changed_body_reopens_the_settled_outbox_row` |
| D3 | P2 | A deferred hand-over could start a poller after `stop()` had returned | `tests/test_watches.py::test_a_stop_that_returned_cannot_be_undone_by_a_queued_handover` |
| D4 | P3 | `json_extract` on malformed outbox metadata aborted a publish the host had already accepted | `tests/test_watches.py::test_unreadable_publication_metadata_does_not_abort_a_publish` |
| N1 | P1 | The fence's state write had no terminal guard: a later report overwrote the recorded answer and could un-terminalize a finished task | `tests/test_watches.py::test_a_later_poll_cannot_overwrite_a_recorded_answer` |
| N2 | P2 | Artifact keys collided with the history/status message-id namespace, dropping history text | `tests/test_watches.py::test_artifact_and_history_ids_do_not_collide` |
| N3 | P2 | Id-less artifacts were content-keyed and still fanned out one message per revision | `tests/test_watches.py::test_an_artifact_without_an_id_keeps_one_row` |
| N4 | P2 | `json_extract`/`json_each` in ownership's publication matching made `release()` raise on a malformed row, stranding ownership | `tests/test_ownership.py::test_a_malformed_publication_row_cannot_block_a_release` |
| N5 | P3 | Recovery and listing read TaskStore's ambient DB while the fence wrote the profile plugin DB | `tests/test_tools.py::test_task_store_uses_the_plugins_profile_database`, `::test_a2a_list_reads_the_plugins_profile_database` |
| R1 | P1 | The terminal guard consumed an answer it refused to record when the stored reply was empty; the placeholder "no text reply" then blocked the real answer | `tests/test_watches.py::test_a_terminal_row_without_an_answer_accepts_the_later_answer`, `::test_a_placeholder_failure_does_not_block_the_real_answer` |
| R2 | P2 | `artifact:{id}` and `artifact:{index}` shared a namespace: artifactId `1` aliased the id-less artifact at index 1 | `tests/test_watches.py::test_named_and_positional_artifacts_do_not_alias` |
| R3 | P2 | `a2a_list` still opened the ambient DB | `tests/test_tools.py::test_a2a_list_reads_the_plugins_profile_database` |
| R4 | P2 | The empty-reply escape hatch let a stale text-less report flip a finished task into the cancel sweep, and `a2a_list` took the write lock on construction (10s stall behind a publish) | `tests/test_watches.py::test_a_stale_report_cannot_flip_a_finished_task_with_no_text`, `tests/test_tools.py::test_a2a_list_does_not_wait_for_the_write_lock` |
| A2-1 | P1 | A replay acknowledged an artifact revision it never sent: a poll revised the artifact (and refreshed its outbox row) between the replay's snapshot and the publish, so the mark settled the new content while the host only saw the old text | `tests/test_watches.py::test_a_replay_must_not_acknowledge_a_revision_it_did_not_send`, `::test_flush_skips_an_outbox_body_the_artifact_has_outgrown` |
| A2-2 | P2 | Both history recorders lowercased the role and accepted only `agent`/`assistant`, dropping A2A v1.0 `ROLE_AGENT` messages | `tests/test_watches.py::test_v1_role_agent_history_reaches_both_recorders` |
| A2-3 | P2 | The blocking await's transport-error, RPC-error and timeout exits ignored the ownership-fence verdict and returned the old caller's backlog snapshot | `tests/test_watches.py::test_a_poll_error_after_losing_ownership_does_not_return_the_backlog`, `::test_a_peer_rpc_error_after_losing_ownership_does_not_return_the_backlog`, `::test_a_timeout_after_losing_ownership_does_not_return_the_backlog` |
| API-1 | -- | `provides_hooks` missing from `plugin.yaml`; plugin API validate failed | `hermes plugins validate` (section A) |
| API-2 | -- | Validate on a working tree flags the gitignored SDK venv as dangerous | environment: run validate on a clean export (section A) |
| ENV-1 | -- | In the e2e image, `test_real_runtime_registration_via_hermes_plugins_list` finds no installed plugin | environment limitation, identical on `f3ddaaa` (see D) |
| ENV-2 | -- | In the e2e image, the caller-hardkill worker cannot import the plugin (harness checkout path) | environment limitation, identical on `f3ddaaa` (see D) |

## C. Container end-to-end

`e2e/container/run-container.sh` runs a real `hermes chat` on a pty against a scripted
OpenAI-compatible model and a scripted A2A peer (`WORKING` then `COMPLETED` with an artifact), with
the plugin mounted from the working tree. Assertions:

- `a2a_await(background=true)` returned a `watch_id` that reached the model.
- The peer was polled at least once and never sent `CancelTask`.
- The plugin's completion reached the live session exactly once through
  `PluginContext.inject_message` and the model received it as a new turn.
- The plugin's own outbox row for the artifact is `published`, `attempts=0`.

Expected: `[e2e] PASS ...` and `[run] PASS`, with `evidence/summary.json` printed.

## D. Known container limitations

Two tests fail inside `localhost/hermes-e2e:local` on this branch **and identically on
`f3ddaaa`**, so they are image/harness limits, not regressions:

- `tests/integration/test_hermes_compatibility.py::test_real_runtime_registration_via_hermes_plugins_list`
  asserts `hermes plugins list --json` shows the plugin enabled, which needs a user install in the
  image's `HERMES_HOME`; the image has none. On the host, with the plugin installed, it passes.
- `tests/recovery/test_caller_restart.py::test_caller_hardkill_outbound_task_remains_queryable_and_nonterminal`
  spawns a worker that imports the plugin from a path the harness assumes; it fails in the image.

`tests/recovery/conftest.py` also hardcodes
`/home/hermes/.hermes/hermes-agent/.venv/bin/python`. To run the suite in the image, symlink the
image's source and venv into that path:

```sh
podman run --rm --entrypoint /bin/bash -v "$PWD":/plugin:ro,z localhost/hermes-e2e:local -c '
  mkdir -p /home/hermes/.hermes
  ln -sfn /src /home/hermes/.hermes/hermes-agent
  ln -sfn /opt/venv /src/.venv
  cd /plugin && /opt/venv/bin/python -m pytest tests -q -p no:cacheprovider'
```

## E. Results at this commit

Run 2026-10-19, branch `feat/inject-message-delivery`, plugin `0.5.0`.

| Check | Result |
|---|---|
| Host targeted matrix (section B) | 29 passed |
| Host full suite (`pytest tests -q`) | 263 passed, 3 skipped |
| Container full suite (same tree, image `localhost/hermes-e2e:local`) | 259 passed, 5 skipped, 2 known-limitation failures (D) |
| Container end-to-end (`run-container.sh`) | PASS, 8 model calls, 2 peer polls, outbox `attempts=0` |
| `hermes plugins validate` (clean export) | Validation passed (2 warnings, section A) |
| `hermes plugins doctor` (clean export) | OK: discovery, manifest, import, registration; 13 tools, 3 hooks |
| `hermes plugins list --json` (user install, enabled) | `a2a-async`, source `user`, status `enabled`, version `0.5.0` |

## F. Accepted trade-offs (documented, not defects)

1. Artifacts are delivered when a task turns terminal or needs input, not while it is working.
   A streaming peer therefore produces one message per artifact, not one per revision.
2. A text-less terminal report cannot rewrite an already-terminal row. The tool output still
   reports the peer's state; the durable row keeps the first terminal state.
3. `TaskStore._ensure_db` trusts `PRAGMA user_version` as the schema marker. A v6-labelled file
   missing v6 objects fails loudly instead of being silently repaired on construction; the plugin
   cannot create that state.
4. The gateway adapter keeps a long-lived `TaskStore` whose in-memory view can lag a direct DB
   write from the tool path. The database is the shared truth; this predates the branch.

## G. Out of scope

- macOS and Windows runtime behaviour (unit tests carry platform markers; no native run here).
- A real external A2A peer or a real model provider (the container uses scripted endpoints).
- Multiplexed-gateway load beyond the profile-scope tests.
- Catalog admission itself: this plan runs the admission checks, it does not submit the plugin.
