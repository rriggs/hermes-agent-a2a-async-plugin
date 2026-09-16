from .a2a_async_plugin.tools import register_tools

def register(ctx) -> None:
    register_tools(ctx)
