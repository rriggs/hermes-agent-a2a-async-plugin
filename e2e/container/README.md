# Container end-to-end check: classic CLI background refusal

Proves the whole path in a container with no mocks in the middle:

```
hermes chat (real CLI + autostarted gateway)
  -> model calls a2a_await(background=true)      (scripted OpenAI-compatible endpoint)
  -> plugin asks core async_delivery_visible()
  -> classic CLI reports no visible async host
  -> tool refuses before claiming a task or persisting a watch
  -> the model receives the refusal as the tool result
```

This is the current contract for a classic CLI session. Core runs a plugin-injected turn in the
session transcript, but the attached terminal does not display it, so the plugin refuses rather
than promising a result nobody can see. TUI and Desktop have a visible host and are covered by
`tests/tui_gateway/test_plugin_inject_host.py`; messaging sessions deliver through their adapter.

## Prerequisites

- `podman` (tested with 5.8, rootless, SELinux enforcing: the mounts use `:z`).
- A Hermes checkout whose `PluginContext` exposes `inject_message` and
  `async_delivery_visible`. The image is built from that checkout.
- The plugin entry must set `allow_gateway_injection: true`; the refusal happens before consent
  is consulted, so this is only needed for the gateway configuration to be valid.
- Network for the image build (Python deps via `uv`). The first container run also bootstraps a
  PM environment for the fresh `HERMES_HOME`; later runs reuse the `/data` volume.

## Run

```bash
cd e2e/container
podman build --ignorefile ignore.txt -t hermes-e2e:local \
    -f Containerfile /path/to/hermes-agent     # the checkout with async_delivery_visible

E2E_IMAGE=localhost/hermes-e2e:local ./run-container.sh /tmp/e2e-pr9
```

Exit 0 and `[run] PASS` mean every assertion below held. Evidence (gateway log, the model
requests, the transcript) lands in `/tmp/e2e-pr9/evidence`; the summary is printed at the end.
The plugin's own state is `$DATA/hermes/a2a_tasks.db`; the core `state.db` is not part of this
harness.

## Assertions

- The model's tool result contains `cannot display a background result`.
- The refusal names the transcript and a usable surface (`TUI/Desktop/messaging`).
- `a2a_watches` has zero rows: the refusal happens before a watch is persisted.
- The scripted A2A peer is never polled.
- `summary.json` reports `refused: true`, the model-call count, zero peer polls and zero watch rows.

## What the driver scripts

`driver.py` starts two loopback servers:

- an OpenAI-compatible endpoint (`/v1/chat/completions`, streaming and non-streaming): the first
  conversation request answers with a tool call to `a2a_await`, the next answers with plain text
  after the refusal, and the title generator is ignored;
- an A2A peer: `.well-known/agent-card.json` plus JSON-RPC `GetTask`, which must receive nothing
  because the refusal happens before any watch exists.

The config written for the home enables the plugin (with `allow_gateway_injection: true`), points
the model at the scripted endpoint, enables the `a2a` toolset for the `cli`/`local` platforms, and
turns `tool_search` off so the tools are eager.
