from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Context:
    def __init__(self) -> None:
        self.tools = []
        self.platforms = []

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        "a2a_async_plugin", ROOT / "__init__.py", submodule_search_locations=[str(ROOT)]
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_registers_all_tools_into_shared_a2a_toolset():
    plugin = load_plugin()
    context = Context()
    plugin.register(context)

    names = {entry["name"] for entry in context.tools}
    assert names == {
        "a2a_discover", "a2a_call", "a2a_list", "a2a_history", "a2a_orchestrate",
        "a2a_submit", "a2a_get_task", "a2a_await", "a2a_cancel", "a2a_steer",
    }
    assert {entry["toolset"] for entry in context.tools} == {"a2a"}
    assert all(callable(entry["handler"]) for entry in context.tools)
    assert all(entry["schema"]["name"] == entry["name"] for entry in context.tools)
    assert [entry["name"] for entry in context.platforms] == ["a2a"]


def test_plugin_manifest_declares_registered_tools():
    import yaml

    manifest = yaml.safe_load((ROOT / "plugin.yaml").read_text())
    declared = set(manifest["provides_tools"])
    assert declared == {
        "a2a_discover", "a2a_call", "a2a_list", "a2a_history", "a2a_orchestrate",
        "a2a_submit", "a2a_get_task", "a2a_await", "a2a_cancel", "a2a_steer",
    }


def test_async_tool_handlers_reject_missing_required_arguments():
    plugin = load_plugin()
    context = Context()
    plugin.register(context)
    handlers = {entry["name"]: entry["handler"] for entry in context.tools}

    assert "context_id" in handlers["a2a_steer"]({"agent": "peer", "message": "continue"})
    assert "agent" in handlers["a2a_submit"]({"message": "work"})
    assert "task_id" in handlers["a2a_get_task"]({"agent": "peer"})
    assert "task_id" in handlers["a2a_await"]({"agent": "peer"})
    assert "task_id" in handlers["a2a_cancel"]({"agent": "peer"})
