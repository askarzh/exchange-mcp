"""Boilerplate detectors that run beside chunking (Phase 3 §2/§2b).

Two detectors look at the tail paragraphs of a cleaned body and say where
gateway/legal boilerplate begins. Every hit is logged; a paragraph is dropped
from the text that gets CHUNKED — never from the stored body — only when
ARCHIVE_BOILERPLATE_DROP names that detector. The first paragraph of a
message is never in the tail window, so it can never be dropped.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any

from .bodyclean import tail_paragraphs

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Hit:
    detector: str
    paragraph_index: int
    paragraph: str
    similarity: float | None
    ref_label: str | None


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(x * x for x in b)) or 1.0
    return dot / (na * nb)


class EmbeddingDetector:
    def __init__(self, refs: list[tuple[str, list[float]]], threshold: float) -> None:
        self.refs = list(refs)
        self.threshold = float(threshold)

    def detect(self, paragraphs: list[str], vectors: list[list[float]]) -> list[Hit]:
        hits: list[Hit] = []
        for i, (para, vec) in enumerate(zip(paragraphs, vectors)):
            best_label, best = None, -1.0
            for label, ref in self.refs:
                s = cosine(vec, ref)
                if s > best:
                    best_label, best = label, s
            if best_label is not None and best >= self.threshold:
                hits.append(Hit("embedding", i, para, round(best, 4), best_label))
        return hits


class BoilerplateHarness:
    def __init__(self, store: Any, embedder: Any, *, threshold: float, drop: str,
                 llm: Any | None = None) -> None:
        self.store, self.embedder, self.threshold, self.drop = store, embedder, threshold, drop
        self.llm = llm
        self._refs_stamp: Any = None
        self._detector = EmbeddingDetector([], threshold)
        self.refresh_refs()

    def refresh_refs(self) -> None:
        rows = self.store.boilerplate_refs()
        stamp = max((r["created_at"] for r in rows), default=None)
        if stamp != self._refs_stamp or not self._detector.refs:
            self._detector = EmbeddingDetector(
                [(r["label"], list(r["embedding"])) for r in rows], self.threshold)
            self._refs_stamp = stamp

    def analyse(self, ews_id: str, text: str) -> tuple[str, list[Hit]]:
        tail = tail_paragraphs(text)
        if not tail:
            return text, []
        self.refresh_refs()
        paragraphs = [p.strip() for _off, p in tail]
        hits: list[Hit] = []
        if self._detector.refs:
            vectors = self.embedder.embed(paragraphs)
            hits.extend(self._detector.detect(paragraphs, vectors))
        if self.llm is not None:
            hits.extend(self.llm.detect(ews_id, text.split("\n\n", 1)[0], paragraphs))
        cut_from: int | None = None
        for h in hits:
            if h.paragraph_index < 0:        # LLM error/no-cut marker rows
                continue
            allowed = self.drop == "both" or self.drop == h.detector
            if allowed and (cut_from is None or h.paragraph_index < cut_from):
                cut_from = h.paragraph_index
        dropped = cut_from is not None
        if hits:
            logger.info("boilerplate %s: %d hit(s) %s, dropped=%s", ews_id, len(hits),
                        [(h.detector, h.ref_label, h.similarity) for h in hits], dropped)
            self.store.log_boilerplate_hits(ews_id, hits, dropped)
        if not dropped:
            return text, hits
        off = tail[cut_from][0]
        return text[:off].rstrip(), hits
