from pathlib import Path
import sys
_root = str(Path(__file__).resolve().parent)
if _root not in sys.path:
    sys.path.insert(0, _root)
from a2a_async_plugin.tools import register_tools

def register(ctx) -> None:
    register_tools(ctx)
