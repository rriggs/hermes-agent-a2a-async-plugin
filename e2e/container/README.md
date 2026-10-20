# Container end-to-end check: background `a2a_await`

Proves the whole path in a container with no mocks in the middle:

```
hermes chat (real CLI + autostarted gateway)
  -> model calls a2a_await(background=true)      (scripted OpenAI-compatible endpoint)
  -> plugin persists a watch, polls a peer        (scripted A2A peer, WORKING then COMPLETED)
  -> PluginContext.inject_message                 (real host injection, consent-gated)
  -> the live session admits the message as a new turn
  -> the model receives it                         (asserted on the outgoing request)
  -> the plugin outbox row is marked `published`
  -> the backlog row is marked `delivered` in a2a_tasks.db
```

## Prerequisites

- `podman` (tested with 5.8, rootless, SELinux enforcing: the mounts use `:z`).
- A Hermes checkout whose `PluginContext` exposes `inject_message`, with the a2a async plugin
  branch applied. The image is built from that checkout.
- The plugin entry must set `allow_gateway_injection: true`: injecting into a messaging/gateway
  session is consent-gated per plugin.
- Network for the image build (Python deps via `uv`). The first container run also bootstraps a
  PM environment for the fresh `HERMES_HOME`; later runs reuse the `/data` volume.

## Run

```bash
cd e2e/container
podman build --ignorefile ignore.txt -t hermes-e2e:local \
    -f Containerfile /path/to/hermes-agent     # the checkout with inject_message

E2E_IMAGE=localhost/hermes-e2e:local ./run-container.sh /tmp/e2e-pr9
```

Exit 0 and `[run] PASS` mean every assertion below held. Evidence (gateway log, the model
requests, the transcript) lands in `/tmp/e2e-pr9/evidence`; the summary is printed at the end.
The plugin's own state is `$DATA/hermes/a2a_tasks.db`; the core `state.db` is not part of this
harness.

## Assertions

- `a2a_await(background=true)` returned a `watch_id` in its tool result (the second model request
  carries it).
- The peer was polled at least once and never sent `CancelTask`.
- The model received a turn containing the peer's reply (`peer finished t1: all checks passed`)
  and the plugin's own framing (`A2A task t1 on peer 'peer'`).
- `a2a_watch_publications` holds the artifact's event row with `published_at` set. The summary
  reports `attempts`, expected `0` (the live host accepted on the first try).
- `summary.json` reports the model-call and peer-poll counts for the run.

## What the driver scripts

`driver.py` starts two loopback servers:

- an OpenAI-compatible endpoint (`/v1/chat/completions`, streaming and non-streaming): the first
  conversation request answers with a tool call to `a2a_await`, the next ones answer with plain
  text, and the request that already carries the injected message (`on peer 'peer'`) is the
  completion turn (also used by the title generator, which is why the driver looks for the
  completion in the request, not in a call count);
- an A2A peer: `.well-known/agent-card.json` plus JSON-RPC `GetTask`, replying `WORKING` on the
  first poll and `COMPLETED` (with an artifact) afterwards.

The config written for the home enables the plugin (with `allow_gateway_injection: true`), points
the model at the scripted endpoint, enables the `a2a` toolset for the `cli`/`local` platforms, and
turns `tool_search` off so the tools are eager (a deferred toolset would hide `a2a_await` behind
`tool_search`).
