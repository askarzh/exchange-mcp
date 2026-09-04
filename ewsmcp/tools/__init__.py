"""Tool packs. Deliberately EMPTY of imports.

``build_registry`` and the packs live in ``tools/registry.py``: the thin MCP
imports ``tools.base``/``tools.cache_reads``/``tools.write_specs``, and any
import here would run for it too — pulling exchangelib (via writes.py) into
a process that must never load it (tests/test_mcp_import_boundary.py).

``from ewsmcp.tools import build_registry`` still works: it resolves lazily
through ``__getattr__`` below, for the daemon and for tests.
"""

from typing import Any

__all__ = ["build_registry"]


def __getattr__(name: str) -> Any:
    if name == "build_registry":
        from .registry import build_registry

        return build_registry
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
