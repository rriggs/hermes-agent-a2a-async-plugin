# Container end-to-end checks

Two harnesses, one image. Both drive real Hermes processes with two scripted loopback
servers (an OpenAI-compatible model and an A2A peer); nothing is mocked in the middle.

| Runner | Surface | What it proves |
|---|---|---|
| `run-container.sh` | classic CLI (`hermes chat`) | background mode refuses when the host cannot display a later turn |
| `run-tui.sh` | TUI/websocket (`hermes gateway run` + `source=tui`) | the background result is injected into the originating session as a new turn |

## Build (once)

```bash
cd e2e/container
podman build --ignorefile ignore.txt -t hermes-e2e:local \
    -f Containerfile /path/to/hermes-agent     # the checkout with inject_message + async_delivery_visible
```

`podman` 5.8 rootless, SELinux enforcing: the mounts use `:z`. The image builds the checkout's
PM environment; the plugin and the drivers are mounted at run time, never baked in.

## 1. Classic CLI: refusal

```bash
E2E_IMAGE=localhost/hermes-e2e:local ./run-container.sh /tmp/e2e-cli
```

```
hermes chat (real CLI + autostarted gateway)
  -> model calls a2a_await(background=true)      (scripted endpoint)
  -> plugin asks core async_delivery_visible()
  -> classic CLI reports no visible async host
  -> tool refuses before claiming a task or persisting a watch
  -> the model receives the refusal as the tool result
```

Exit 0 and `[run] PASS` mean: the tool result contains `cannot display a background result`,
names the transcript and a usable surface, `a2a_watches` has zero rows, and the peer was never
polled. Evidence lands in `$DATA/evidence`.

This is the contract for a classic CLI session: core stores a plugin-injected turn, but the
attached terminal does not display it.

## 2. TUI/websocket: delivery

```bash
E2E_IMAGE=localhost/hermes-e2e:local ./run-tui.sh /tmp/e2e-tui
```

```
gateway_bootstrap (same helper the Ink TUI runs) starts/attaches `hermes gateway run`
  -> ws client, session.create source=tui      (transport records client_surface=tui)
  -> prompt.submit                             (scripted model calls a2a_await(background=true))
  -> plugin asks async_delivery_visible() -> True (live GUI viewer)
  -> watch persisted; status line posted as a keyed notification.show
  -> peer: GetTask WORKING, then COMPLETED with an artifact
  -> poller publishes through ctx.inject_message -> gateway admits the injected turn
  -> same session runs a new turn whose prompt carries "[A2A result · peer · task t1]"
  -> model sees the label, the untrusted-input boundary and the peer text
```

Exit 0 and `[run-tui] PASS` mean:

- the first tool result contains `[watch watch_`, so background mode started rather than refused;
- the model request for the next turn contains `[A2A result · peer · task t1]`, the peer artifact
  text `peer finished t1: ...` and the untrusted-input boundary;
- that turn finished with `message.complete` in the same session;
- the TUI notification stream carried the plugin's live status line (`⧗ a2a peer · working` then
  `✓ a2a peer · completed`);
- `a2a_watches` is `complete` and `a2a_watch_publications.published_at` is set;
- the peer was polled at least twice and never received `CancelTask`.

Evidence: `$DATA/evidence-tui/` (`summary.json`, the model requests, the notification stream,
the DB rows). The gateway log is `$DATA/home/.hermes/logs/gateway.log`.

`tui_driver.py` uses `connect_gateway()`, the same client the product uses: it ensures the
gateway owner, mints a session ticket, and dials `/api/ws`. The session declares `source=tui` on
`session.create`, which is what makes `async_delivery_visible()` true for the plugin and what
routes `notification.show` / the injected turn to this client.

## What the drivers script

`driver.py` (classic CLI) starts an OpenAI-compatible endpoint whose first conversation request
answers with a tool call to `a2a_await`, then plain text after the refusal; its A2A peer must
receive nothing because the refusal happens before any watch exists.

`tui_driver.py` uses the same two servers plus the gateway bootstrap. Its model answers the
gateway's title prompt with a fixed title, calls `a2a_await(background=true)` once, then returns
`COMPLETION_RECEIVED` when the injected turn arrives. Its peer holds the first `GetTask` in
`WORKING` and completes the second with an artifact.

Both configs enable the plugin (with `allow_gateway_injection: true`), point the model at the
scripted endpoint, list the `a2a` toolset for the `cli`/`local` platforms, and turn `tool_search`
off so the tools are eager.

## Notes

- Both runners clear their data directory, and only a directory carrying a `.e2e-data` marker
  (or an empty one) may be cleared. A typo such as `./run-tui.sh .` fails instead of deleting code.
- A previous run's gateway can outlive a killed driver; each driver SIGTERMs processes whose
  environment names its `HERMES_HOME` and whose command line mentions `hermes`/`gateway` before
  starting, then again on exit.
- The gateway derives the session id (`local-<hash>`) from the profile and chat; the driver does
  not mint it. The scripted peer and model ports are random on every run.
