# Container end-to-end check: background `a2a_await`

Proves the whole path in a container with no mocks in the middle:

```
hermes chat (real CLI + autostarted gateway)
  -> model calls a2a_await(background=true)      (scripted OpenAI-compatible endpoint)
  -> plugin persists a watch, polls a peer        (scripted A2A peer, WORKING then COMPLETED)
  -> PluginContext.publish_completion             (real Hermes publisher, real state.db)
  -> gateway admits the completion into the session
  -> the model receives it as a new turn          (asserted on the outgoing request)
  -> the ledger row is claimed, acknowledged and `delivered`
```

## Prerequisites

- `podman` (tested with 5.8, rootless, SELinux enforcing: the mounts use `:z`).
- A Hermes checkout that contains the plugin completion publisher
  (`PluginContext.capture_completion_target` / `publish_completion`), with the a2a async plugin
  branch applied. The image is built from that checkout.
- Network for the image build (Python deps via `uv`). The first container run also bootstraps a
  PM environment for the fresh `HERMES_HOME`; later runs reuse the `/data` volume.

## Run

```bash
cd e2e/container
podman build --ignorefile ignore.txt -t hermes-e2e:local \
    -f Containerfile /path/to/hermes-agent     # the checkout with the publisher

./run-container.sh /tmp/e2e-plugin-completions-data
```

Exit 0 and `[run] PASS` mean every assertion below held. Evidence (gateway log, the model
requests, the transcript) lands in `/tmp/e2e-plugin-completions-data/evidence`; the summary is
printed at the end.

## Assertions

- `a2a_await(background=true)` returned a `watch_id` in its tool result (the second model request
  carries it).
- The peer was polled at least once and never sent `CancelTask`.
- The model received a turn whose text contains the peer's reply and the `plugin a2a-async`
  provenance banner, exactly once.
- `plugin_completions` holds the row with `delivery_state='delivered'` and one delivery attempt.

## What the driver scripts

`driver.py` starts two loopback servers:

- an OpenAI-compatible endpoint (`/v1/chat/completions`, streaming and non-streaming): the first
  conversation request answers with a tool call to `a2a_await`, the one carrying the watch result
  answers with plain text, and any request already carrying `[PLUGIN COMPLETION` is the completion
  turn (also used by the title generator, which is why the driver looks for the completion in the
  request, not in a call count);
- an A2A peer: `.well-known/agent-card.json` plus JSON-RPC `GetTask`, replying `WORKING` on the
  first poll and `COMPLETED` (with an artifact) afterwards.

The config written for the home enables the plugin, points the model at the scripted endpoint,
enables the `a2a` toolset for the `cli`/`local` platforms, and turns `tool_search` off so the
tools are eager (a deferred toolset would hide `a2a_await` behind `tool_search`).
