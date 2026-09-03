"""Text normalisation shared by the indexer and the query side.

One function feeds ``messages.norm_text`` (from which Postgres generates
``search_tsv`` with the ``simple`` config) and every search query, so the two
sides always agree: NFKD-decompose, drop combining marks (é→e, ё→е), lowercase.
No stemming, no stopwords, no language-specific folding.
"""

import re
import unicodedata

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def normalize_text(text: str) -> str:
    if not text:
        return ""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()


def tsquery(query: str) -> str:
    """Safe ``to_tsquery('simple', …)`` expression: every word becomes a
    prefix term, terms are ANDed. Returns "" when nothing is searchable."""
    tokens = _TOKEN_RE.findall(normalize_text(query or ""))
    return " & ".join(f"{t}:*" for t in tokens if t)
