"""Cache-first local mirror: store (Postgres) + sync engine.

``SyncEngine`` is exported lazily on purpose. ``sync.py`` imports exchangelib
at module top (correctly — see tests/test_no_lazy_imports.py), but the thin
MCP imports ``cache.store`` and must never load exchangelib at all
(tests/test_mcp_import_boundary.py proves it in a subprocess). Importing the
engine here would drag exchangelib into every process that touches the store.
"""

from typing import Any

from .store import CacheStore as CacheStore

__all__ = ["CacheStore", "SyncEngine"]


def __getattr__(name: str) -> Any:
    if name == "SyncEngine":
        from .sync import SyncEngine

        return SyncEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
