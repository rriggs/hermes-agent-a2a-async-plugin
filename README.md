# hermes-agent-a2a-async-plugin

Standalone asynchronous A2A client tools for Hermes Agent.

This plugin extends the built-in A2A integration without replacing it. It contributes the async tools to Hermes' existing `a2a` toolset, so operators do not need to manage a second toolset:

- `a2a_submit` -- submit a task without waiting
- `a2a_get_task` -- retrieve a task snapshot
- `a2a_await` -- poll until a task reaches a terminal state
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
a2a_await(agent="researcher", task_id="...")
```

Use `a2a_steer` with the `context_id` returned by an earlier A2A exchange when a running conversation needs a correction or follow-up. It returns a distinct task ID immediately.

Peers are configured in `config.yaml` under `a2a_agents`. Authentication and endpoint configuration remain owned by the built-in A2A integration.

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
| `a2a_async/__init__.py` | `register(ctx)` entry point |
| `a2a_async/tools.py` | Async tool schemas, handlers, and registration |
| `a2a_async/protocol.py` | A2A protocol helpers and durable task store |
| `a2a_async/security.py` | Outbound redaction and audit helpers |

## Development

Run syntax checks with:

```bash
python -m py_compile a2a_async/*.py
```

The plugin is intended to be tested through Hermes' real plugin discovery path with a temporary `HERMES_HOME`. Do not run it alongside another plugin that registers the same async tool names.

## Safety

- Remote endpoints must be explicitly configured.
- Outbound messages pass through the plugin's redaction and audit path.
- Cancellation is best effort because a peer may already be processing the task.
- Do not put API tokens in this repository or in `config.yaml`; use Hermes secret handling.
