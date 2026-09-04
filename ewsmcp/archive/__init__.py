"""The archive pipeline: capture → verify → delete, plus the embedder.

Every worker is idempotent and driven by ``messages.archive_state``, so a
crashed or half-finished pass is simply repeated on the next cycle. Nothing
here is imported by the MCP process — the daemon owns Exchange, the blob
store and the Gemini key.

That last sentence is enforced, not aspirational: the MCP DOES import
``ewsmcp.archive.files`` (the blob store is how `get_attachment` serves
archived mail), which imports this package. So this module must not pull in
the workers — ``capture.py`` imports exchangelib at module top, and the thin
MCP must never load exchangelib at all (``tests/test_mcp_import_boundary.py``
proves it in a subprocess). ``ArchiveRunner`` therefore resolves lazily
through ``__getattr__``: ``from ewsmcp.archive import ArchiveRunner`` still
works for ewsd, and costs nothing for anyone who never asks for it.
"""

from typing import Any

__all__ = ["ArchiveRunner"]


def __getattr__(name: str) -> Any:
    if name == "ArchiveRunner":
        from .runner import ArchiveRunner

        return ArchiveRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
