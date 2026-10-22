"""GitHub issue #3: profile-scoped consistency of the A2A artifacts.

On a multiplexed gateway the task DB resolved profile-scoped at adapter
init while _conv_dir() re-called get_hermes_home() from HTTP worker
threads and landed in the DEFAULT hermes home. Transcripts split across
roots, a2a_history returned "No persisted conversation" for contexts
whose jsonl demonstrably existed, a2a_list kind=conversations listed the
stale profile-dir set, and on multi-profile hosts every agent's
transcripts landed in ONE shared root (cross-profile leak).

Locked in here:

1. _conv_dir() honors the A2A_CONVERSATIONS_DIR env override (explicit
   deploy knob) before everything else.
2. When hermes_constants cannot be imported, the fallback chain reads
   HERMES_HOME before pinning to ~/.hermes (the old fallback ignored
   the env var entirely).
3. persist_message/load_conversation/list_conversations accept an
   explicit conv_dir= override; the adapter captures the profile-scoped
   dir at init and passes it on the inbound path — no process global (a
   global pin leaked across multiplex profiles and across tests).
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Load the plugin package under a private name: tests/integration registers the repository-root
# __init__.py as ``a2a_async_plugin`` in sys.modules, which would otherwise shadow the real
# package for every test that runs after it.
import importlib  # noqa: E402
import importlib.util  # noqa: E402

PKG = "a2a_profile_scope_under_test"
_spec = importlib.util.spec_from_file_location(
    PKG, ROOT / "a2a_async_plugin" / "__init__.py",
    submodule_search_locations=[str(ROOT / "a2a_async_plugin")])
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
sys.modules[PKG] = _module
_spec.loader.exec_module(_module)

protocol = importlib.import_module(f"{PKG}.protocol")  # noqa: E402


@pytest.fixture
def pinned_dir(tmp_path):
    d = tmp_path / "pinned" / "a2a_conversations"
    d.mkdir(parents=True)
    os.environ["A2A_CONVERSATIONS_DIR"] = str(d)
    yield d
    del os.environ["A2A_CONVERSATIONS_DIR"]


def test_env_override_wins(pinned_dir, monkeypatch):
    monkeypatch.delenv("HERMES_HOME", raising=False)
    assert protocol._conv_dir() == pinned_dir


def test_persist_load_list_resolve_under_pinned_dir(pinned_dir, monkeypatch):
    monkeypatch.delenv("HERMES_HOME", raising=False)
    protocol.persist_message("ctx-split-1", "user", "hello", "task-1", peer="dean")
    protocol.persist_message("ctx-split-1", "agent", "hi", "task-1")
    msgs = protocol.load_conversation("ctx-split-1", limit=10)
    assert len(msgs) == 2
    assert protocol.list_conversations() == ["ctx-split-1"]
    # The artifact physically lives under the pin — NOT under the
    # default hermes home the worker threads used to pick.
    assert (pinned_dir / "ctx-split-1.jsonl").exists()
    assert protocol._conv_dir().name == "a2a_conversations"


def test_explicit_dir_parameter_overrides_resolution(tmp_path, monkeypatch):
    """Issue #3's mechanism: the adapter (and any other caller with the
    right context) passes its captured dir explicitly; no process global
    is consulted or leaked."""
    monkeypatch.delenv("A2A_CONVERSATIONS_DIR", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)
    d = tmp_path / "adapter_captured" / "a2a_conversations"
    d.mkdir(parents=True)
    protocol.persist_message("ctx-explicit", "user", "hello", "task-1", peer="dean", conv_dir=d)
    # The default resolution wrote nothing anywhere else.
    assert protocol.load_conversation("ctx-explicit") == []
    assert "ctx-explicit" not in protocol.list_conversations()
    assert (d / "ctx-explicit.jsonl").exists()
    assert protocol.load_conversation("ctx-explicit", conv_dir=d)[0]["peer"] == "dean"
    assert "ctx-explicit" in protocol.list_conversations(conv_dir=d)


def test_fallback_reads_hermes_home_env_when_constants_missing(monkeypatch, tmp_path):
    """Without hermes_constants (bare environments), the fallback must
    honor HERMES_HOME — the old code expanded ~/.hermes unconditionally,
    which is what drove transcripts into the default home in the first
    place."""
    monkeypatch.delenv("A2A_CONVERSATIONS_DIR", raising=False)
    fake_home = tmp_path / "hermes_home"
    fake_home.mkdir(parents=True)
    # Simulate the import failure: hide hermes_constants/… from the lazy
    # import by pointing sys.modules lookup at a raising finder.
    import builtins
    real_import = builtins.__import__

    def _raising_import(name, *args, **kwargs):
        if name == "hermes_constants" or name.startswith("hermes_constants."):
            raise ImportError("simulated missing hermes_constants")
        return real_import(name, *args, **kwargs)

    monkeypatch.setenv("HERMES_HOME", str(fake_home))
    monkeypatch.setattr(builtins, "__import__", _raising_import)
    assert protocol._conv_dir() == fake_home / "a2a_conversations"


def test_adapter_installs_override_from_profile_resolver(monkeypatch):
    """The resolver the adapter calls at init honors A2A_CONVERSATIONS_DIR
    and the HERMES_HOME fallback chain. On a multiplexed gateway, adapter
    __init__ runs inside the profile's runtime scope, where
    get_hermes_home() resolves to the profile home — the same base the
    task DB resolver (protocol._resolve_db_path) uses in that scope."""
    pytest.importorskip("hermes_cli", reason="Hermes source not importable on this host")
    adapter_mod = importlib.import_module(f"{PKG}.adapter")  # noqa: E402  (imports gateway.*)

    monkeypatch.delenv("A2A_CONVERSATIONS_DIR", raising=False)
    # Default-profile gateway: resolver tracks get_hermes_home()'s context
    # (the real profile home in this session's process).
    resolved = adapter_mod._profile_scoped_conversations_dir()
    from hermes_constants import get_hermes_home
    assert resolved == Path(get_hermes_home()) / "a2a_conversations"
    # Same base path family as the task DB.
    assert resolved.parent == Path(get_hermes_home())