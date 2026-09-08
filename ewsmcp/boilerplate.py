"""Boilerplate detectors that run beside chunking (Phase 3 §2/§2b).

Two detectors look at the tail paragraphs of a cleaned body and say where
gateway/legal boilerplate begins. Every hit is logged; a paragraph is dropped
from the text that gets CHUNKED — never from the stored body — only when
ARCHIVE_BOILERPLATE_DROP names that detector. The first paragraph of a
message is never in the tail window, so it can never be dropped.
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from typing import Any

import httpx

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
                 llm: Any | None = None, llm_per_cycle: int | None = None) -> None:
        self.store, self.embedder, self.threshold, self.drop = store, embedder, threshold, drop
        self.llm = llm
        # How many LLM calls one indexing pass may make (None = unlimited).
        # Stored here at construction so the pass itself only has to say
        # "a new pass starts now" — see begin_pass().
        self.llm_per_cycle = llm_per_cycle
        self._refs_stamp: Any = None
        self._detector = EmbeddingDetector([], threshold)
        self.refresh_refs()

    def begin_pass(self) -> None:
        """A new indexing pass starts: hand the LLM detector its call budget.

        Without this the detector would call out once per message for a whole
        200-message page, each call serial and blocking while the archive
        runner holds its lock."""
        if self.llm is not None and self.llm_per_cycle is not None:
            reset = getattr(self.llm, "reset", None)
            if reset is not None:
                reset(int(self.llm_per_cycle))

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


GEMINI_GENERATE_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:generateContent"
)
_PROMPT = (
    "You see the FIRST paragraph of an email and its LAST paragraphs, numbered. "
    "Answer with JSON {{\"drop_from\": <index or null>, \"reason\": <short string>}}: "
    "drop_from is the index of the first numbered paragraph where signature, "
    "legal disclaimer, or mail-gateway boilerplate begins and continues to the end. "
    "If the numbered paragraphs are all real message content, answer null. "
    "Never pick a paragraph that contains the writer's actual message.\n\n"
    "FIRST PARAGRAPH:\n{first}\n\nLAST PARAGRAPHS:\n{numbered}")


class GeminiCleaner:
    """Gemini generateContent, asked for one JSON object and nothing else.

    Like GeminiEmbedder, the key rides in the x-goog-api-key HEADER (a query
    string leaks into proxy logs, Referer and crash reports) and the client
    is injectable so tests drive it with an httpx MockTransport.
    """

    def __init__(self, api_key: str, *, model: str, client: Any | None = None,
                 timeout: float = 10.0) -> None:
        if not api_key:
            raise ValueError("GeminiCleaner needs an API key")
        self.model = model
        self.timeout = float(timeout)
        self.url = GEMINI_GENERATE_URL.format(model=model)
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None
        self._headers = {"x-goog-api-key": api_key,
                         "Content-Type": "application/json"}

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def boundary(self, first_paragraph: str, paragraphs: list[str]) -> dict[str, Any]:
        """The parsed answer dict. Raises on HTTP error/timeout/invalid JSON —
        LlmDetector turns any of those into a logged `error:` hit."""
        numbered = "\n\n".join(f"[{i}] {p}" for i, p in enumerate(paragraphs))
        body = {
            "contents": [{"parts": [{"text": _PROMPT.format(
                first=first_paragraph[:1000], numbered=numbered[:6000])}]}],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
                "responseSchema": {
                    "type": "OBJECT",
                    "properties": {
                        "drop_from": {"type": "INTEGER", "nullable": True},
                        "reason": {"type": "STRING"},
                    },
                    "required": ["reason"],
                },
            },
        }
        r = self._client.post(self.url, headers=self._headers, json=body,
                              timeout=self.timeout)
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(text)


class LlmDetector:
    """Wraps a cleaner so a bad answer can never move the cut.

    Anything that is not a validated in-range index becomes a Hit with
    paragraph_index=-1: the harness skips negative indexes in its cut loop,
    so the row is LOGGED (that is where `errors` in boilerplate_stats comes
    from) but can never drop a paragraph.

    `budget` caps how many calls one pass may make (None = unlimited, which
    is what a one-off script or a test wants). Once it is exhausted `detect`
    returns nothing at all — no call, no hit, and no error row: the pass
    simply stops asking, and the next `reset()` opens the budget again.
    """

    def __init__(self, cleaner: Any, budget: int | None = None) -> None:
        self.cleaner = cleaner
        self.budget = budget

    def reset(self, budget: int | None) -> None:
        self.budget = budget

    def detect(self, ews_id: str, first_paragraph: str,
               paragraphs: list[str]) -> list[Hit]:
        if self.budget is not None:
            if self.budget <= 0:
                return []
            self.budget -= 1
        try:
            ans = self.cleaner.boundary(first_paragraph, paragraphs)
            if not isinstance(ans, dict) or "reason" not in ans:
                raise ValueError("malformed answer")
            idx = ans.get("drop_from")
            if idx is None:
                return []
            idx = int(idx)
            reason = str(ans.get("reason") or "")[:200]
            if not 0 <= idx < len(paragraphs):
                return [Hit("llm", -1, "", None, f"error:index {idx} out of range")]
            return [Hit("llm", idx, paragraphs[idx], None, reason or "boilerplate")]
        except Exception as exc:  # noqa: BLE001 - a detector never breaks indexing
            logger.info("llm detector error for %s: %s", ews_id, type(exc).__name__)
            return [Hit("llm", -1, "", None, f"error:{type(exc).__name__}")]
