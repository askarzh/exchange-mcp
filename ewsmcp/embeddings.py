"""Text → vector, behind one small interface.

``Embedder`` is a Protocol on purpose: the daemon injects ``GeminiEmbedder``
(remote, rate-limited, occasionally down), tests inject a deterministic local
fake, and neither the ``SemanticIndex`` nor the archive worker knows or cares
which it got. That is what keeps the whole embedding path testable offline.

gemini-embedding-2 takes its task instruction IN THE PROMPT, not as a
parameter, so query text is prefixed with ``QUERY_PREFIX`` while indexed
document text is embedded bare.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Protocol, runtime_checkable

import httpx

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-embedding-2"
GEMINI_ENDPOINT = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:batchEmbedContents"
)
QUERY_PREFIX = "Retrieve email messages relevant to the query: "
MAX_BATCH = 100
CHUNK_CHARS = 1500
_RETRY_STATUS = frozenset({408, 429, 500, 502, 503, 504})


class EmbeddingError(RuntimeError):
    """The embedder could not produce vectors. Callers degrade to keyword."""


@runtime_checkable
class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one vector per input text, in order."""


def chunk_text(subject: str, body: str,
                chunk_chars: int = CHUNK_CHARS) -> list[str]:
    """``subject + "\\n" + body`` split into fixed-width character chunks."""
    subject = (subject or "").strip()
    body = body or ""
    text = f"{subject}\n{body}" if subject and body else (subject or body)
    text = text.strip("\n") if not subject or not body else text
    if not text.strip():
        return []
    size = max(1, int(chunk_chars))
    return [text[i:i + size] for i in range(0, len(text), size)]


class GeminiEmbedder:
    """Gemini's batchEmbedContents over plain httpx — no vendor SDK."""

    def __init__(self, api_key: str, *, dims: int = 768,
                 model: str = GEMINI_MODEL, batch: int = MAX_BATCH,
                 client: Any | None = None, max_attempts: int = 5,
                 base_delay: float = 1.0,
                 sleep: Callable[[float], None] = time.sleep,
                 timeout: float = 60.0) -> None:
        if not api_key:
            raise ValueError("GeminiEmbedder needs an API key")
        self.model = model
        self.dims = int(dims)
        self.batch = max(1, min(int(batch), MAX_BATCH))
        self.max_attempts = max(1, int(max_attempts))
        self.base_delay = float(base_delay)
        self._sleep = sleep
        # The key is a query parameter. It is never logged: error messages
        # below quote the status and the response body, never the URL.
        self.url = f"{GEMINI_ENDPOINT.format(model=model)}?key={api_key}"
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for start in range(0, len(texts), self.batch):
            out.extend(self._embed_batch(texts[start:start + self.batch]))
        return out

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        payload = {"requests": [
            {"model": f"models/{self.model}",
             "content": {"parts": [{"text": t}]},
             "outputDimensionality": self.dims}
            for t in texts
        ]}
        delay = self.base_delay
        last = ""
        for attempt in range(self.max_attempts):
            try:
                resp = self._client.post(self.url, json=payload)
            except httpx.HTTPError as exc:
                last = f"{type(exc).__name__}: {exc}"
                if attempt == self.max_attempts - 1:
                    break
                self._sleep(delay)
                delay *= 2
                continue
            if resp.status_code in _RETRY_STATUS:
                last = f"HTTP {resp.status_code}"
                if attempt == self.max_attempts - 1:
                    break
                logger.warning("gemini embed %s — retrying in %.0fs",
                                last, delay)
                self._sleep(delay)
                delay *= 2
                continue
            if resp.status_code >= 400:
                raise EmbeddingError(
                    f"gemini embed failed: HTTP {resp.status_code} "
                    f"{resp.text[:200]}")
            return self._vectors(resp, len(texts))
        raise EmbeddingError(
            f"gemini embed failed after {self.max_attempts} attempts ({last})")

    def _vectors(self, resp: Any, expected: int) -> list[list[float]]:
        try:
            data = resp.json()
        except ValueError as exc:
            raise EmbeddingError("gemini embed returned non-JSON") from exc
        vectors = [e.get("values") for e in (data.get("embeddings") or [])]
        if len(vectors) != expected:
            raise EmbeddingError(
                f"gemini embed returned {len(vectors)} vectors for "
                f"{expected} texts")
        for vec in vectors:
            if not isinstance(vec, list) or len(vec) != self.dims:
                raise EmbeddingError(
                    f"gemini embed returned a vector of width "
                    f"{len(vec) if isinstance(vec, list) else '?'}, "
                    f"expected {self.dims}")
        return [[float(x) for x in vec] for vec in vectors]
