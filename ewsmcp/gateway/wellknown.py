"""Well-known folder keys and query pagination — deliberately exchangelib-free.

Both are pure data/slicing helpers that the READ tools need, and the read
tools are imported by the thin MCP process, which must never load
exchangelib (tests/test_mcp_import_boundary.py). They therefore live here
rather than in ``client.py``, which imports exchangelib at module top;
``client.py`` re-exports them so the gateway's own call sites are unchanged.
"""

from typing import Any, List, Optional, Tuple

WELL_KNOWN = {
    "f:inbox": "inbox", "f:sent": "sent", "f:drafts": "drafts",
    "f:trash": "trash", "f:junk": "junk", "f:outbox": "outbox",
    "f:calendar": "calendar", "f:contacts": "contacts", "f:tasks": "tasks",
}


def paginate(query: Any, *, offset: int, limit: int,
             chunk: int = 50) -> Tuple[List[Any], Optional[int]]:
    """Materialize query[offset:offset+limit] in chunks (sync, raises on
    mid-iteration failure — the caller's error mapper classifies it).

    Returns ``(items, next_offset)``. NEVER calls ``QuerySet.count()`` —
    in exchangelib that iterates every matching id server-side, so a 20k
    inbox paid ~20k ids of round trips on every "read 10 emails". Whether
    another page exists comes from a one-item lookahead instead; callers
    that want an exact total use a refreshed ``folder.total_count`` (only
    valid for unfiltered listings) or a local mirror count.
    """
    offset = max(0, offset)
    limit = max(0, limit)
    lookahead = limit + 1
    items: List[Any] = []
    cursor = offset
    chunk = max(1, min(chunk, 250))
    while len(items) < lookahead:
        want = min(chunk, lookahead - len(items))
        batch = list(query[cursor:cursor + want])
        if not batch:
            break
        items.extend(batch)
        cursor += len(batch)
        if len(batch) < want:
            break
    next_offset = offset + limit if len(items) > limit else None
    return items[:limit], next_offset
