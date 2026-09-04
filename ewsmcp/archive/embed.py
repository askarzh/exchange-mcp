"""Embed backlog drainer.

Runs over every message with ``embedded_at IS NULL`` — live and archived
alike, because semantic search must not have a hole where the archive starts.
Embedding is remote and blocking, so it runs on a worker thread; a failure
leaves the backlog exactly where it was and the next cycle retries.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

PAGE = 200


class EmbedWorker:
    def __init__(self, store: Any, index: Any, *, page: int = PAGE) -> None:
        self.store = store
        self.index = index
        self.page = max(1, int(page))

    async def run(self, *, limit: int | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {"embedded": 0, "backlog": 0, "error": None}
        if self.index is None:
            out["backlog"] = await asyncio.to_thread(self.store.embedding_backlog)
            return out
        budget = self.page if limit is None else int(limit)
        try:
            while budget > 0:
                rows = await asyncio.to_thread(
                    self.store.unembedded_messages, min(self.page, budget))
                if not rows:
                    break
                done = await asyncio.to_thread(self.index.index_messages, rows)
                out["embedded"] += done
                budget -= len(rows)
                if done == 0:
                    break  # nothing progressed — do not spin
        except Exception as exc:  # noqa: BLE001 - backlog grows, search degrades
            out["error"] = f"{type(exc).__name__}: {exc}"
            logger.warning("embedding pass failed: %s", out["error"])
        out["backlog"] = await asyncio.to_thread(self.store.embedding_backlog)
        return out
