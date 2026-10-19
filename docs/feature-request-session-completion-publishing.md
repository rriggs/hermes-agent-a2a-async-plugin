Title: [Feature]: Expose a durable plugin API for delivering completions into an originating session
Labels: enhancement

## Problem or Use Case

Plugins can run work that outlives the tool call or agent turn, but they have no supported way to deliver the eventual result back into the Hermes session that initiated it. The plugin event bus (`ctx.emit` / `ctx.subscribe`) is plugin-scoped; it does not enqueue content into the agent's conversation. A plugin that needs to notify the caller is therefore forced to poll synchronously, depend on a transport-specific workaround, or reach into Hermes' private delivery machinery.

This blocks useful background workflows such as an asynchronous A2A task watcher: `a2a_await` could return promptly with a watch ID, but when the remote task later completes, the plugin cannot safely route that result back to the originating session. The session and delivery route must be captured at invocation time, rather than guessed later by a background worker.

Hermes already has internal durable completion-delivery paths for background work, but plugins cannot publish through them via a documented public API. Requiring plugins to import private queue or database internals would couple extensions to implementation details and bypass the core's routing, ownership, and delivery guarantees.

## Proposed Solution

Expose a public, documented completion-target and publisher API through `PluginContext`. For example:

```python
# During the tool invocation, using its dispatcher-provided session_id:
target = ctx.capture_completion_target(session_id=session_id)

# Later, when the background operation completes:
receipt = ctx.publish_completion(
    target=target,
    event_id=event_id,
    content="The remote task completed.",
    metadata={"source": "a2a", "task_id": task_id, "state": "completed"},
)
```

The target should be opaque (or safely serializable for durable plugin state), profile-scoped, and bound to the session/route that invoked the plugin. The API should validate that the publishing plugin is authorized to use that target and should not let a plugin redirect a completion to an arbitrary session.

The core publisher should persist the event before queueing it and deliver it through the existing session-delivery machinery as a new turn at an idle boundary. It must not interrupt a turn already in progress. The same supported API should work across supported CLI, TUI, and gateway session surfaces; if a surface cannot accept a completion, report that clearly rather than silently dropping it.

The API should provide durable, idempotent delivery semantics:

- Deduplicate by a caller-supplied event ID within the target's scope.
- Preserve pending events across process restart and replay them through the normal delivery policy.
- Track claims/acknowledgments and retry when delivery is refused or interrupted.
- Defer delivery while the target session is busy, without losing the event.
- Return a receipt/status that distinguishes accepted or queued from delivered. Acceptance by the queue is not proof that the model processed the content or that a user-facing reply was sent.
- Apply profile/session ownership checks, content-size limits, and appropriate redaction/validation to metadata and content.

The completion should become input to a subsequent agent turn in the captured session, with enough metadata for the agent to understand what completed and where the result came from. The API should document the lifecycle and limits of targets, including behavior when the originating session no longer exists or is unavailable.

## Alternatives Considered

- Keep background operations synchronous and poll until completion. This blocks the active agent turn and wastes time for long-running work.
- Use `ctx.emit` / `ctx.subscribe`. These are plugin-local events and do not inject content into the Hermes conversation.
- Import or write Hermes' internal completion queues, tables, or delegation-specific records directly. This binds plugins to private implementation details and bypasses the core's routing, persistence, ownership checks, and delivery policy.
- Send a message through a transport-specific gateway or start a separate session. This is not portable across CLI, TUI, and gateway use, and it does not continue the originating conversation.
- Add a generic agent-loop hook. Hooks can observe or enrich turns that have already started, but do not by themselves provide a supported way to enqueue a new turn after asynchronous work finishes.

## Feature Type

Gateway / messaging improvement

## Scope

Large (new public PluginContext contract plus durable event storage/routing integration and tests across supported session surfaces).

## Contribution

[ ] I'd like to implement this myself and submit a PR

## Debug Report (optional)

No debug report attached. This is a plugin API capability gap rather than a single runtime failure. A concrete motivating workflow is background `a2a_await`: capture the originating session when the watch starts, then publish a durable completion when the remote task reaches a terminal state.
