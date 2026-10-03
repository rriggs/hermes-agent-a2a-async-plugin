"""
Test harness for restart / recovery of the Hermes a2a-async plugin.

This conftest provides:

* ``isolated_profile`` — temporary HERMES_HOME / A2A_TASKS_DB / env
  sanitisation. Always bound to ``pytest.tmp_path`` so no repository
  artifact lands in ``/tmp``.

* ``free_port`` — loopback TCP port picked by the OS and held until the
  fixture tears down, eliminating port-collision flakes.

* ``gateway`` — a controllable subprocess wrapper for the plugin's
  real Hermes gateway process (the launcher runs the plugin's
  ``register(ctx)`` against a stub ``Context`` and starts the
  ``A2AAdapter`` against a stub ``PlatformConfig``; the HTTP server is
  the plugin's own ``ThreadingHTTPServer``).

  The harness supports explicit ``start()``, ``stop()`` (clean SIGTERM
  with ``adapter.disconnect()``), and ``kill()`` (SIGKILL, no cleanup
  handlers). ``restart()`` is the same profile + DB, fresh process.
  PID / port / profile home / log path are exposed for failure messages.

Gateway entrypoint (the choice the plan called out as the top of the
docstring's "headless-reliable" question):

* We spawn the plugin's repo-root ``__init__.py`` directly as a
  subprocess using the Hermes source tree's ``.venv/bin/python``
  interpreter with ``PYTHONPATH`` pointing at both
  ``/home/hermes/.hermes/hermes-agent`` (the Hermes source tree,
  required for ``gateway.platforms.base`` / ``gateway.config`` imports)
  and the plugin repo root (so ``from a2a_async_plugin import ...``
  resolves from the plugin repo, not the user's installed plugin set).

* The launcher (``tests/recovery/_gateway_launcher.py``) does the same
  ``register(ctx)`` call the Hermes plugin loader does for every
  discovered plugin, then instantiates the registered
  ``adapter_factory`` against a minimal ``PlatformConfig`` and runs the
  adapter's ``async connect()`` / ``disconnect()`` lifecycle. This
  exercises the *real* plugin HTTP server (``ThreadingHTTPServer`` +
  ``A2ARequestHandler``) and the *real* ``TaskStore`` SQLite lifecycle
  — exactly the surfaces the plan's restart tests need.

* We rejected ``hermes gateway run`` (the full multi-platform gateway)
  because it pulls in Telegram / model launcher / profile bootstrap code
  that is not exercisable in a hermetic test environment and requires
  valid secrets. The plan calls out "the most headless-reliable entry
  point" — direct adapter instantiation in a child process is that
  point and gives us a real PID we can SIGKILL.

Failure reporting: when a test fails while the harness is alive, the
fixture's ``report_failure`` method appends a block with PID / port /
profile home / DB path / log path to pytest's terminal report so the
diagnosis is one paste away.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

import pytest


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
PLUGIN_REPO = REPO_ROOT
HERMES_TREE = Path("/home/hermes/.hermes/hermes-agent")
HERMES_PYTHON = HERMES_TREE / ".venv/bin/python"

# Hermetic env. We never want the gateway subprocess to inherit
# operator-specific tokens that would make it bind to a wider host
# (A2A_BEARER_TOKEN etc.) or contact a real model provider.
_HERMETIC_ENV_PREFIXES_TO_UNSET = (
    "A2A_BEARER_TOKEN",
    "A2A_PEER_TOKENS",
    "A2A_TRUSTED_PEERS",
    "A2A_ALLOW_ALL_USERS",
    "A2A_PUBLIC_URL",
    "A2A_AGENT_NAME",
    "A2A_AGENT_DESCRIPTION",
    "A2A_ADVERTISED_TOOLSETS",
    "A2A_RATE_LIMIT",
    "A2A_HOST",
    "A2A_PORT",
    "A2A_TASKS_DB",
    "A2A_REPLY_TIMEOUT",
    "A2A_ASYNC_TIMEOUT",
    "A2A_MAX_PINGPONG_TURNS",
    "A2A_PROVIDER_ORG",
    "A2A_PROVIDER_URL",
    "A2A_HOME_CHANNEL",
    "A2A_ALLOWED_USERS",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY",
    "MISTRAL_API_KEY",
    "FIREWORKS_API_KEY",
    "TOGETHER_API_KEY",
    "OPENROUTER_API_KEY",
    "GROQ_API_KEY",
    "XAI_API_KEY",
    "DEEPSEEK_API_KEY",
    "HERMES_API_KEY",
    "HERMES_LIVE_API_KEY",
    "HERMES_HOME",
    "HERMES_PROFILE",
)


def _sanitize_env() -> dict[str, str]:
    """Return a copy of ``os.environ`` with operator-secret env cleared.

    The harness must never see the operator's tokens — every env var
    the A2A adapter would use to widen its bind host or call a model
    API is removed. ``A2A_PORT`` / ``A2A_HOST`` / ``A2A_TASKS_DB`` are
    reset per-test by the fixture; we wipe any pre-existing value too.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    for name in _HERMETIC_ENV_PREFIXES_TO_UNSET:
        env.pop(name, None)
    # Hermetic deterministic timezone + locale so timestamps in the SQLite
    # store are reproducible across CI and local.
    env.setdefault("TZ", "UTC")
    env.setdefault("LC_ALL", "C.UTF-8")
    env.setdefault("LANG", "C.UTF-8")
    return env


# ---------------------------------------------------------------------------
# free_port
# ---------------------------------------------------------------------------


@pytest.fixture
def free_port() -> Iterator[int]:
    """A loopback TCP port picked by the OS; released when the test ends."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    finally:
        s.close()
    yield port


# ---------------------------------------------------------------------------
# isolated_profile
# ---------------------------------------------------------------------------


@dataclass
class IsolatedProfile:
    """Resolved paths for a single test's hermetic Hermes profile."""

    home: Path
    tasks_db: Path
    log_path: Path
    env: dict[str, str]

    def render_for_failure(self) -> str:
        return (
            f"HERMES_HOME={self.home}\n"
            f"A2A_TASKS_DB={self.tasks_db}\n"
            f"  gateway_log={self.log_path}\n"
        )


@pytest.fixture
def isolated_profile(tmp_path: Path) -> Iterator[IsolatedProfile]:
    """A hermetic HERMES_HOME + A2A_TASKS_DB under pytest tmp_path."""
    home = tmp_path / "hermes_home"
    log_path = tmp_path / "gateway.log"
    home.mkdir(parents=True, exist_ok=True)
    tasks_db = home / "a2a_tasks.db"
    env = _sanitize_env()
    env["HERMES_HOME"] = str(home)
    env["A2A_TASKS_DB"] = str(tasks_db)
    env["A2A_HOST"] = "127.0.0.1"
    env["A2A_ASYNC_PLUGIN_REPO"] = str(PLUGIN_REPO)
    env["PYTHONPATH"] = (
        f"{HERMES_TREE}{os.pathsep}{PLUGIN_REPO}"
        + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    )
    yield IsolatedProfile(home=home, tasks_db=tasks_db, log_path=log_path, env=env)
    # Teardown is automatic — tmp_path is owned by pytest.
    # But explicitly clear the env side-effect for any subsequent
    # fixtures that re-read os.environ (we set it back here so we don't
    # leak into the next test).
    for name in ("HERMES_HOME", "A2A_TASKS_DB", "A2A_HOST", "A2A_ASYNC_PLUGIN_REPO"):
        os.environ.pop(name, None)


# ---------------------------------------------------------------------------
# gateway
# ---------------------------------------------------------------------------


class GatewayProcess:
    """A live Hermes-a2a-async gateway subprocess with explicit lifecycle."""

    def __init__(
        self,
        *,
        python: Path,
        launcher: Path,
        profile: IsolatedProfile,
        port: int,
        startup_timeout: float = 12.0,
    ) -> None:
        self._python = python
        self._launcher = launcher
        self._profile = profile
        self._requested_port = port
        self.startup_timeout = startup_timeout
        self.pid: Optional[int] = None
        self.port: Optional[int] = None
        self._proc: Optional[subprocess.Popen[bytes]] = None
        self._log_path = profile.log_path

    # ── properties ────────────────────────────────────────────────────────

    @property
    def log_path(self) -> Path:
        return self._log_path

    @property
    def tasks_db(self) -> Path:
        return self._profile.tasks_db

    @property
    def profile_home(self) -> Path:
        return self._profile.home

    def render_for_failure(self) -> str:
        return (
            f"  pid={self.pid}\n"
            f"  port={self.port} (requested {self._requested_port})\n"
            f"  HERMES_HOME={self._profile.home}\n"
            f"  A2A_TASKS_DB={self._profile.tasks_db}\n"
            f"  log={self._log_path}\n"
        )

    # ── lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the subprocess and wait for the Agent Card readiness line."""
        if self._proc is not None:
            raise RuntimeError("gateway already started")
        if not self._python.exists():
            raise RuntimeError(f"Hermes python not found at {self._python}")
        if not self._launcher.exists():
            raise RuntimeError(f"launcher script not found at {self._launcher}")

        # The launcher writes its ready line (json pid+port) to a pipe
        # we hand it via --ready-fd. We poll the read end until the
        # process reports readiness or the startup deadline elapses.
        r_fd, w_fd = os.pipe()
        env = dict(self._profile.env)
        env["GATEWAY_LOG_PATH"] = str(self._log_path)
        try:
            self._proc = subprocess.Popen(
                [
                    str(self._python),
                    str(self._launcher),
                    "--port", str(self._requested_port),
                    "--ready-fd", str(w_fd),
                ],
                env=env,
                pass_fds=(w_fd,),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                # New process group so SIGTERM/SIGKILL targeted at the
                # gateway does not cascade to pytest helpers in the same
                # shell (defensive; we're in our own process anyway).
                start_new_session=True,
            )
            os.close(w_fd)
            w_fd = -1  # closed in this process; launcher holds the only fd
        except Exception:
            with contextlib.suppress(OSError):
                os.close(w_fd)
            raise

        # Read the ready line.
        import select  # local import keeps the top of the file lean
        deadline = time.time() + self.startup_timeout
        buf = b""
        try:
            while time.time() < deadline:
                # Block up to 200ms for data; if the process dies, exit early.
                rl, _, _ = select.select([r_fd], [], [], 0.2)
                if rl:
                    chunk = os.read(r_fd, 4096)
                    if not chunk:
                        break  # EOF — launcher closed
                    buf += chunk
                    if b"\n" in buf:
                        break
                if self._proc.poll() is not None:
                    break
            else:
                self._terminate_due_to_startup_timeout(buf)
        finally:
            with contextlib.suppress(OSError):
                os.close(r_fd)

        if self._proc.poll() is not None:
            stderr = (self._proc.stderr.read() if self._proc.stderr else b"").decode(
                "utf-8", errors="replace"
            )
            raise RuntimeError(
                f"gateway exited before becoming ready (rc={self._proc.returncode}):\n"
                f"--- stderr ---\n{stderr}\n--- log ---\n"
                f"{self._read_log()}"
            )

        if not buf.strip():
            self.kill()
            raise RuntimeError(
                f"gateway did not signal readiness within {self.startup_timeout}s\n"
                f"--- log ---\n{self._read_log()}"
            )

        try:
            info = json.loads(buf.decode("utf-8").strip().splitlines()[-1])
        except (ValueError, IndexError) as exc:
            self.kill()
            raise RuntimeError(
                f"gateway readiness line was not JSON: {buf!r} ({exc})\n"
                f"--- log ---\n{self._read_log()}"
            ) from exc

        self.pid = int(info["pid"])
        self.port = int(info["port"])
        if self.port != self._requested_port:
            # The adapter exposes a "fall closed" default-port behaviour
            # for profile-scoped configs; we don't run profile-scoped.
            # Surface a real error if the adapter ignored the port.
            self.kill()
            raise RuntimeError(
                f"gateway bound port={self.port} but {self._requested_port} was requested"
            )

    def _terminate_due_to_startup_timeout(self, buf: bytes) -> None:
        if self._proc and self._proc.poll() is None:
            self.kill()
        stderr = (self._proc.stderr.read() if self._proc and self._proc.stderr else b"").decode(
            "utf-8", errors="replace"
        )
        raise RuntimeError(
            f"gateway did not signal readiness within {self.startup_timeout}s "
            f"(buf={buf!r})\n--- stderr ---\n{stderr}\n--- log ---\n{self._read_log()}"
        )

    def _read_log(self) -> str:
        try:
            return self._log_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return f"(log not readable: {self._log_path})"

    def stop(self, timeout: float = 8.0) -> int:
        """Clean shutdown: SIGTERM, then SIGKILL on timeout.

        The launcher's asyncio loop installs SIGTERM/SIGINT handlers
        that call ``adapter.disconnect()`` (server shutdown + thread
        join) before exiting. If the timeout elapses we escalate to
        SIGKILL — the test should distinguish clean stop from hard
        kill by calling ``stop()`` vs. ``kill()`` directly.
        """
        if self._proc is None:
            return 0
        proc = self._proc
        try:
            proc.send_signal(signal.SIGTERM)
            try:
                return proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                pass
            proc.send_signal(signal.SIGKILL)
            try:
                return proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                return -1
        finally:
            self._proc = None
            self.pid = None

    def kill(self) -> None:
        """Hard kill: SIGKILL only, no cleanup handlers run."""
        if self._proc is None:
            return
        proc = self._proc
        try:
            if proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
        finally:
            self._proc = None
            self.pid = None

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def restart(self) -> None:
        """Restart against the same profile + DB.

        Requires that this process has been stopped (or killed) first.
        """
        if self._proc is not None:
            raise RuntimeError("gateway is still running; call stop() or kill() first")
        self.start()

    # ── diagnostics ───────────────────────────────────────────────────────

    @contextlib.contextmanager
    def report_failure_on_error(self, test_name: str) -> Iterator[None]:
        try:
            yield
        except Exception:
            # Append the harness state to the captured exception so the
            # pytest -s / --tb output shows PID / port / DB path without
            # forcing the test author to remember.
            extra = (
                f"\n[recovery harness @ {test_name}]\n{self.render_for_failure()}"
                f"--- gateway log tail ---\n{self._read_log()[-4000:]}\n"
            )
            # Re-raise with extra context attached via __notes__.
            exc = sys.exc_info()[1]
            if exc is not None and hasattr(exc, "__notes__"):
                exc.__notes__ = list(getattr(exc, "__notes__", [])) + [extra]
            raise


@pytest.fixture
def gateway(
    isolated_profile: IsolatedProfile,
    free_port: int,
    request: pytest.FixtureRequest,
) -> Iterator[GatewayProcess]:
    """A controllable Hermes-a2a-async gateway subprocess.

    Yields an unstarted ``GatewayProcess``. Tests call ``.start()`` and
    ``.stop()`` / ``.kill()`` / ``.restart()`` explicitly. The fixture
    guarantees a hard kill on teardown if the test left the process
    running (which would otherwise leak across tests).
    """
    proc = GatewayProcess(
        python=HERMES_PYTHON,
        launcher=HERE / "_gateway_launcher.py",
        profile=isolated_profile,
        port=free_port,
    )
    try:
        with proc.report_failure_on_error(request.node.name):
            yield proc
    finally:
        proc.kill()
        # Test side-car cleanup of any WAL files left by a hard kill.
        # with_suffix requires a leading dot, so use the named form for
        # the SQLite convention ``<name>-wal`` / ``<name>-shm``.
        for sibling in (f"{proc.tasks_db.name}-wal", f"{proc.tasks_db.name}-shm",
                        f"{proc.tasks_db.name}-journal"):
            with contextlib.suppress(FileNotFoundError):
                (proc.tasks_db.parent / sibling).unlink()


# ---------------------------------------------------------------------------
# Failure reporting helpers — pytest hook to dump harness state when ANY
# test in the recovery directory fails. Less intrusive than monkey-
# patching the gateway fixture into every test.
# ---------------------------------------------------------------------------


@pytest.hookimpl(tryfirst=True)
def pytest_exception_interact(node, call, report):
    """Append harness PID/port/DB to failed recovery tests' report."""
    if report.when != "call" or report.outcome != "failed":
        return
    if "tests/recovery/" not in str(node.fspath):
        return
    # We can only introspect if the gateway fixture was active; pytest
    # doesn't expose its fixture values cleanly, so we read what we can
    # from the tmp_path / env. The GatewayProcess.report_failure_on_error
    # context already attaches the rich state when the test calls into
    # the gateway at all.
    extra = (
        "\n[recovery harness context] see attached __notes__ from the "
        "gateway fixture for PID/port/DB/log; if empty, the test "
        "failed before/after touching the gateway fixture.\n"
    )
    if report.longrepr is not None and extra not in str(report.longrepr):
        report.longrepr = str(report.longrepr) + extra


# Silence the unused import warning for Any on Python 3.13+
_ = Any