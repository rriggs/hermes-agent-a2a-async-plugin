# A2A Async Standalone Plugin Handoff

Date: 2026-09-25
Owner: Sven
Repository: https://github.com/rriggs/hermes-agent-a2a-async-plugin.git
Workspace: /home/hermes/.hermes/profiles/sven/workspace/hermes-agent-a2a-async-plugin

## Current status

The standalone plugin provides the inbound asynchronous A2A gateway adapter, durable protocol/task state, security helpers, and outbound synchronous/asynchronous client tools.

Current pushed HEAD:

- `a4d68e8 fix(a2a): log scoped agent name resolution errors`
- Branch: `main`
- Remote: `origin/main`
- The five commits after `61a95e3` were pushed successfully.

The plugin manifest declares:

- name: `a2a-async`
- version: `0.1.6`
- kind: `platform`
- requires Hermes: `>=0.21.3`
- ten tools in the shared `a2a` toolset

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

## Architecture

The root `__init__.py` is the directory-plugin entry point. It registers:

- all ten outbound/client tools under `toolset="a2a"`
- the inbound platform named `a2a`
- the `A2AAdapter` HTTP server through `ctx.register_platform`

Implementation package:

- `a2a_async_plugin/adapter.py` -- inbound HTTP server, JSON-RPC dispatch, SSE, task lifecycle, push notification handling, watchdog/recovery behavior
- `a2a_async_plugin/protocol.py` -- A2A protocol helpers, SQLite `TaskStore`, task state, persistence, listing, recovery, metrics
- `a2a_async_plugin/security.py` -- authentication, bind safety, redaction, injection filtering, audit, push signing
- `a2a_async_plugin/tools.py` -- all ten client tool schemas and handlers

The plugin must be the sole inbound A2A platform owner. Do not run a duplicate bundled inbound A2A adapter alongside it. Synchronous and asynchronous tools remain in the single shared `a2a` toolset.

## Verification

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

## Open items

These are still open and should be addressed before calling the plugin production-complete or catalog-ready:

1. Run a valid-task push notification delivery test in a temporary secondary profile. Verify callback payload, task ID/state, HMAC/signature behavior, and push metrics.
2. Run restart/recovery and remote-state reconciliation in a temporary secondary profile. Preserve a durable nonterminal task, restart only the temporary gateway, then verify `tasks/get`, `tasks/list`, orphan handling, and reconciliation.
3. Expand the repository test suite beyond the current seven tests. Add focused adapter, security, TaskStore, SSE, push, restart/recovery, and loader-boundary coverage.
4. Investigate or document the `hermes plugins validate` / `hermes_state_ids` discrepancy observed on Alex. Doctor passed, but validation previously failed in an environment whose capability subprocess could not import `hermes_state_ids`.
5. Re-run validation against a clean tagged checkout and create a release tag only after the acceptance tests pass.
6. Re-run the full Hermes-side plugin Doctor and deployment checks after any release/tag operation.

The plugin is usable and deployed, but it is not yet catalog-ready because push delivery, restart/recovery, expanded acceptance coverage, and clean tagged-checkout verification remain incomplete.

## Operational constraints

- All normal code work stays under `/home/hermes/.hermes/profiles/sven/workspace`.
- Never use `/tmp` for code repositories, edits, builds, tests, or artifacts.
- Do not overwrite Alex's default profile.
- Do not print or transfer credentials.
- Do not run a duplicate inbound A2A gateway alongside the standalone plugin.
- Keep plugin version, release tag, and clean-checkout Doctor output consistent before catalog/review handoff.
