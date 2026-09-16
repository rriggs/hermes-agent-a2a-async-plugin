"""Standalone asynchronous A2A client tools for Hermes Agent.

The plugin contributes only async tools to the shared ``a2a`` toolset. The
built-in A2A plugin remains the owner of synchronous client and inbound
platform functionality.
"""

from .tools import register_tools


def register(ctx) -> None:
    register_tools(ctx)
