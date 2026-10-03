"""
Hermes-a2a-async gateway harness launcher — run as a subprocess.

Phase 1 of docs/restart-session-resilience-plan.md. Spawned by
``tests/recovery/conftest.py`` to provide a real Hermes gateway process
that loads this plugin and serves the A2A HTTP surface on a loopback
port. The harness can start, stop cleanly (SIGTERM), and the test
process can SIGKILL it without cleanup handlers running.

Why this launcher (chosen over the alternatives):

* ``hermes gateway run`` (the full multi-platform gateway) drags in Telegram
  / model-launching / profile-bootstrap code paths that are not exercisable
  in a hermetic test environment. It is *not* headless-reliable for our
  purposes.
* Direct adapter instantiation in a child process preserves the plugin's
  real HTTP server (``ThreadingHTTPServer`` + ``A2ARequestHandler``), the
  real ``TaskStore`` SQLite lifecycle, the real ``register(ctx)`` wiring
  the plugin loader calls, and a real PID we can SIGKILL — all the
  properties the plan calls out (real gateway process loading this
  plugin, exercising TaskStore durability across restart).

Boot sequence inside this subprocess:

1. ``HERMES_HOME`` / ``A2A_TASKS_DB`` / ``A2A_PORT`` / ``A2A_HOST`` come
   in from the environment passed by conftest.
2. Add the plugin repo to ``sys.path`` and import the plugin package
   (which performs the same ``register(ctx)`` registration that the
   Hermes plugin loader invokes for every discovered plugin).
3. Look up the registered platform factory for ``"a2a"`` and call it
   with a stub ``PlatformConfig`` whose ``extra={"port": PORT}``.
4. ``await adapter.connect()`` to bind the HTTP server in a thread.
5. Write the bound port to ``--ready-fd`` (a pipe the parent listens on)
   and the PID is already inherited from the OS — we just ``os.getpid()``.
6. Block on a stop event; signal handlers (SIGTERM/SIGINT) flip it,
   ``disconnect()`` cleans up, and we exit 0.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import traceback
from pathlib import Path
from types import SimpleNamespace
from typing import Any


# ---------------------------------------------------------------------------
# Logger — write to the per-test gateway log file. The harness sets
# GATEWAY_LOG_PATH in the subprocess env; we tee INFO+ there.
# ---------------------------------------------------------------------------

_LOG = logging.getLogger("recovery.gateway")
_LOG.setLevel(logging.DEBUG)


def _install_log_handler() -> None:
    path = os.environ.get("GATEWAY_LOG_PATH", "").strip()
    if not path:
        return
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, mode="w", encoding="utf-8")
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))
        _LOG.addHandler(handler)
        # Mirror A2A module logs to the same file so test output shows
        # the real plugin logs at INFO.
        for name in ("a2a_async_plugin", "a2a_async_plugin.adapter",
                     "a2a_async_plugin.protocol"):
            logging.getLogger(name).setLevel(logging.INFO)
            logging.getLogger(name).addHandler(handler)
    except Exception as exc:  # pragma: no cover - best effort
        _LOG.warning("could not install file log handler: %s", exc)


# ---------------------------------------------------------------------------
# Minimal context object — the plugin's ``register(ctx)`` only requires
# ``register_tool`` and ``register_platform`` methods. We do not exercise
# the tool handlers in the harness; the tools are present so the plugin's
# own check_fn / toolset bookkeeping runs without erroring out.
# ---------------------------------------------------------------------------


class _Context:
    def __init__(self) -> None:
        self.tools: list[dict[str, Any]] = []
        self.platforms: list[dict[str, Any]] = []

    def register_tool(self, **kwargs: Any) -> None:
        self.tools.append(kwargs)

    def register_platform(self, **kwargs: Any) -> None:
        self.platforms.append(kwargs)


def _make_platform_config(port: int) -> Any:
    """Build the minimal ``PlatformConfig`` the adapter constructor expects.

    The adapter only reads ``extra.get("port")`` and ``extra.get("agent_name")``;
    everything else can stay default.
    """
    # Avoid hard-importing gateway.config here — defer to import time so a
    # Hermes-side refactor surfaces as an import error rather than a
    # silent shape mismatch.
    from gateway.config import PlatformConfig  # type: ignore

    return PlatformConfig(
        enabled=True,
        extra={"port": port, "agent_name": "recovery-gateway"},
    )


async def _run(port: int, ready_fd: int) -> int:
    _install_log_handler()
    _LOG.info("launcher pid=%s, HERMES_HOME=%s, A2A_TASKS_DB=%s, port=%s",
              os.getpid(), os.environ.get("HERMES_HOME"),
              os.environ.get("A2A_TASKS_DB"), port)

    plugin_repo = os.environ.get("A2A_ASYNC_PLUGIN_REPO", "").strip()
    if plugin_repo and plugin_repo not in sys.path:
        sys.path.insert(0, plugin_repo)

    ctx = _Context()
    # The plugin's repo-root ``__init__.py`` defines ``register(ctx)``; the
    # inner ``a2a_async_plugin/__init__.py`` does not. Importing the repo
    # root as a package mirrors what the existing tests do
    # (``tests/test_plugin.py`` uses the same importlib trick) and what the
    # Hermes plugin loader does when it discovers the plugin via its
    # ``plugin.yaml`` manifest.
    import importlib.util
    repo_root = plugin_repo or os.getcwd()
    init_path = os.path.join(repo_root, "__init__.py")
    spec = importlib.util.spec_from_file_location(
        "a2a_async_plugin",
        init_path,
        submodule_search_locations=[repo_root],
    )
    if not spec or not spec.loader:
        raise SystemExit(f"could not load plugin __init__ at {init_path}")
    plugin_module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = plugin_module
    spec.loader.exec_module(plugin_module)
    _plugin_register = getattr(plugin_module, "register", None)
    if _plugin_register is None:
        raise SystemExit("plugin __init__.py has no register(ctx) symbol")
    _plugin_register(ctx)
    _LOG.info("plugin loaded: %d tools, %d platforms (%s)",
              len(ctx.tools), len(ctx.platforms),
              [p["name"] for p in ctx.platforms])

    # Build the A2AAdapter from the factory the plugin registered.
    factory = next(
        (p["adapter_factory"] for p in ctx.platforms if p["name"] == "a2a"),
        None,
    )
    if factory is None:
        raise SystemExit("plugin registered no A2A platform factory")

    config = _make_platform_config(port)
    adapter = factory(config)
    _LOG.info("adapter instance: %r port=%s host=%s",
              adapter, getattr(adapter, "port", "?"), getattr(adapter, "host", "?"))

    ok = await adapter.connect()
    if not ok:
        # _set_fatal_error was called by the adapter on bind failure.
        raise SystemExit(f"adapter.connect() returned False: "
                         f"{getattr(adapter, '_fatal_error', '?')}")

    bound_port = adapter.port  # actual port the server is bound on
    # Signal readiness through the pipe; the parent reads one line.
    try:
        with os.fdopen(ready_fd, "w", closefd=True) as ready:
            ready.write(json.dumps({"pid": os.getpid(), "port": bound_port}) + "\n")
            ready.flush()
    except OSError as exc:
        _LOG.error("could not signal readiness: %s", exc)
        await adapter.disconnect()
        return 2

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(signum: int) -> None:
        _LOG.info("received signal %s, stopping", signum)
        try:
            loop.call_soon_threadsafe(stop.set)
        except RuntimeError:
            pass  # loop already closed

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop, int(sig))
        except (NotImplementedError, RuntimeError):
            # add_signal_handler is unavailable on Windows / when no loop.
            signal.signal(sig, lambda *_: _request_stop(int(sig)))

    try:
        await stop.wait()
    finally:
        _LOG.info("disconnecting adapter")
        try:
            await adapter.disconnect()
        except Exception:  # pragma: no cover
            _LOG.warning("adapter.disconnect() raised", exc_info=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True,
                        help="loopback port to bind the A2A HTTP server on")
    parser.add_argument("--ready-fd", type=int, required=True,
                        help="file descriptor of a pipe to write the ready line to")
    args = parser.parse_args(argv)

    try:
        return asyncio.run(_run(args.port, args.ready_fd))
    except SystemExit:
        raise
    except Exception as exc:
        _LOG.error("launcher crashed: %s\n%s", exc, traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())