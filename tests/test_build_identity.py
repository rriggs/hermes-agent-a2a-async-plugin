"""Build identity: the running process must be able to say what it is running.

Before this, nothing reported the plugin version at runtime. `plugin.yaml` was
never read by the code, `AgentCard.version` was a hardcoded "1.0.0", and
`/metrics` carried counters only -- so after replacing files on disk, a caller
could not tell a freshly restarted gateway from one still serving the previous
build out of process memory.

Three independent surfaces now answer it: the startup log line, the Agent Card,
and `/metrics`.
"""

from __future__ import annotations

from pathlib import Path

from a2a_async_plugin import protocol


MANIFEST = Path(__file__).resolve().parents[1] / "plugin.yaml"


def _manifest_version() -> str:
    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("version:"):
            return line.split(":", 1)[1].strip().strip("'\"")
    raise AssertionError("plugin.yaml has no version:")


def test_plugin_version_matches_the_manifest():
    assert protocol.PLUGIN_VERSION == _manifest_version()
    assert protocol.PLUGIN_VERSION not in ("", "unknown")


def test_agent_card_version_reports_the_plugin_build_not_the_wire_protocol():
    """A2A v1.0: AgentCard.version is the agent implementation's version;
    the wire protocol version belongs in supportedInterfaces[].protocolVersion."""
    card = protocol.build_agent_card(
        name="identity-test", url="http://127.0.0.1:1/", description="d",
    )
    assert card["version"] == protocol.PLUGIN_VERSION
    iface = card["supportedInterfaces"][0]
    assert iface["protocolVersion"] == protocol.PROTOCOL_VERSION


def test_metrics_snapshot_reports_build_and_schema():
    snap = protocol.metrics.snapshot()
    assert snap["plugin_version"] == protocol.PLUGIN_VERSION
    assert snap["schema_version"] == protocol.A2A_TASK_SCHEMA_VERSION
    assert isinstance(snap["pid"], int) and snap["pid"] > 0


def test_version_survives_a_missing_manifest(monkeypatch):
    """A manifest that cannot be read degrades to 'unknown', never a load failure."""
    import builtins

    real_open = builtins.open

    def _boom(path, *args, **kwargs):
        if str(path).endswith("plugin.yaml"):
            raise OSError("simulated missing manifest")
        return real_open(path, *args, **kwargs)

    # _load_plugin_version uses Path.read_text; patch that instead.
    real_read_text = Path.read_text

    def _read_text(self, *args, **kwargs):
        if self.name == "plugin.yaml":
            raise OSError("simulated missing manifest")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _read_text)
    assert protocol._load_plugin_version() == "unknown"
