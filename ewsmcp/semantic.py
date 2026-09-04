"""Semantic search: chunk → embed → pgvector, and the hybrid fusion on top.

Two rankings answer any query: the tsvector one (exact words, cheap, never
down) and the vector one (meaning, remote, sometimes down). Reciprocal Rank
Fusion combines them without needing their scores to be comparable — each
result scores ``sum(1 / (RRF_K + rank))`` over the lists it appears in — so a
message found by only one engine still surfaces.

When the embedder or the vector query fails, the caller gets keyword results
and ``degraded=True`` instead of an error: a mailbox search must not go dark
because a remote API is rate-limiting us.
"""

from __future__ import annotations

import logging
from typing import Any

from .cache.store import CacheStore
from .embeddings import (
    CHUNK_CHARS,
    MAX_BATCH,
    QUERY_PREFIX,
    Embedder,
    EmbeddingError,
    chunk_text,
)

logger = logging.getLogger(__name__)

RRF_K = 60
# How deep each engine is read before fusing. Fusing only `limit` rows per
# engine would make the union too shallow to re-rank meaningfully.
CANDIDATE_MULTIPLIER = 5
MAX_CANDIDATES = 100


class SemanticIndex:
    def __init__(self, store: CacheStore, embedder: Embedder, *,
                 chunk_chars: int = CHUNK_CHARS, batch: int = MAX_BATCH) -> None:
        self.store = store
        self.embedder = embedder
        self.chunk_chars = int(chunk_chars)
        self.batch = max(1, min(int(batch), MAX_BATCH))

    # ------------------------------------------------------------- indexing

    def index_messages(self, rows: list[dict[str, Any]]) -> int:
        """Chunk, embed and store `rows`; returns the number of messages done.

        Batching is by MESSAGE, never mid-message: ``replace_chunks`` is a full
        rewrite, so a message whose chunks straddled two API batches would have
        its first half deleted by its second half.
        """
        planned: list[tuple[str, list[str]]] = [
            (r["ews_id"], chunk_text(r.get("subject") or "",
                                     r.get("body_clean") or "", self.chunk_chars))
            for r in rows
        ]
        done: list[str] = []
        pending: list[tuple[str, list[str]]] = []
        pending_size = 0
        for ews_id, chunks in planned:
            if not chunks:                    # nothing to embed — still "done"
                self.store.replace_chunks(ews_id, [])
                done.append(ews_id)
                continue
            if pending and pending_size + len(chunks) > self.batch:
                done.extend(self._flush(pending))
                pending, pending_size = [], 0
            pending.append((ews_id, chunks))
            pending_size += len(chunks)
        if pending:
            done.extend(self._flush(pending))
        self.store.mark_embedded(done)
        return len(done)

    def _flush(self, pending: list[tuple[str, list[str]]]) -> list[str]:
        texts = [t for _i, chunks in pending for t in chunks]
        vectors = self.embedder.embed(texts)
        if len(vectors) != len(texts):
            raise EmbeddingError(
                f"embedder returned {len(vectors)} vectors for {len(texts)} chunks")
        cursor = 0
        for ews_id, chunks in pending:
            window = vectors[cursor:cursor + len(chunks)]
            cursor += len(chunks)
            self.store.replace_chunks(ews_id, [
                {"seq": seq, "source": "body", "text": text, "embedding": vec}
                for seq, (text, vec) in enumerate(zip(chunks, window))])
        return [i for i, _c in pending]

    # -------------------------------------------------------------- reading

    def vector_ids(self, text: str, *, limit: int, archived: str = "any",
                   exclude_ews_id: str | None = None) -> list[tuple[str, float]]:
        vector = self.embedder.embed([QUERY_PREFIX + (text or "")])[0]
        return self.store.similar_message_ids(
            vector, limit=limit, archived=archived, exclude_ews_id=exclude_ews_id)

    def similar_to_message(self, ews_id: str, *, limit: int,
                           archived: str = "any") -> list[dict[str, Any]]:
        seed = self.store.get_message(ews_id)
        if seed is None:
            return []
        text = f"{seed['subject'] or ''}\n{seed['body_clean'] or ''}"
        hits = self.vector_ids(text, limit=limit, archived=archived,
                               exclude_ews_id=seed["ews_id"])
        by_id = self.store.messages_by_ids([i for i, _d in hits])
        out = []
        for ews_id_hit, dist in hits:
            row = by_id.get(ews_id_hit)
            if row is not None:
                row = dict(row)
                row["similarity"] = round(1.0 - float(dist), 4)
                out.append(row)
        return out

    def hybrid_search(self, query: str, *, limit: int, offset: int = 0,
                      archived: str = "any",
                      **filters: Any) -> tuple[list[dict[str, Any]], bool]:
        depth = min(MAX_CANDIDATES, max(limit + offset, 1) * CANDIDATE_MULTIPLIER)
        keyword_rows, _total = self.store.search_messages(
            text=query, archived=archived, offset=0, limit=depth, **filters)
        keyword_ids = [r["ews_id"] for r in keyword_rows]
        degraded = False
        vector_ids: list[str] = []
        try:
            vector_ids = [i for i, _d in self.vector_ids(
                query, limit=depth, archived=archived)]
        except Exception as exc:  # noqa: BLE001 - degrade, never fail the search
            logger.warning("semantic half unavailable (%s) — keyword only", exc)
            degraded = True
        if degraded or not vector_ids:
            fused_ids = keyword_ids
        else:
            fused_ids = rrf([keyword_ids, vector_ids])
        # Structured filters live on the keyword side; a vector-only hit must
        # still satisfy them, so intersect with what the store would return.
        allowed = set(keyword_ids)
        if any(v is not None for v in filters.values()):
            fused_ids = [i for i in fused_ids if i in allowed]
        window = fused_ids[offset:offset + limit]
        by_id = self.store.messages_by_ids(window)
        return [by_id[i] for i in window if i in by_id], degraded


def rrf(rankings: list[list[str]], k: int = RRF_K) -> list[str]:
    """Reciprocal Rank Fusion over several ranked id lists (rank starts at 1)."""
    scores: dict[str, float] = {}
    first_seen: dict[str, int] = {}
    for ranking in rankings:
        for rank, ident in enumerate(ranking, start=1):
            scores[ident] = scores.get(ident, 0.0) + 1.0 / (k + rank)
            first_seen.setdefault(ident, rank)
    return sorted(scores, key=lambda i: (-scores[i], first_seen[i], i))
