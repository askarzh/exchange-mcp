"""The mail bridge (platform spec §3): a read-only window on the store
`exchange-mcp` already keeps, in the vocabulary Mindet speaks.

It is a separate app on a separate port from `ewsd` on purpose. The mail daemon
holds a live Exchange connection and is the more valuable of the two; a bridge
fault should cost Mindet its mail source and nothing else.
"""
from __future__ import annotations

import datetime as dt
import hmac

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from . import arrival, mapping

CONTRACT = 1
SOURCE = "ews"
CAPABILITIES = ["messages", "chats", "contacts"]
PAGE_LIMIT = 200


class _Bad(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message


def _parse_cursor(value: str | None, gen: int) -> int | None:
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 3 or parts[0] != "v1":
        raise _Bad(400, "bad_cursor", "cursor is not of this contract")
    try:
        cursor_gen, seq = int(parts[1]), int(parts[2])
    except ValueError:
        raise _Bad(400, "bad_cursor", "cursor is not of this contract") from None
    if cursor_gen != gen:
        raise _Bad(400, "cursor_generation", "the store was rebuilt; bootstrap again")
    return seq


def _parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        raise _Bad(400, "bad_request", "until must be ISO-8601") from None
    if parsed.tzinfo is None:
        raise _Bad(400, "bad_request", "until must carry an offset")
    return parsed


def build_app(pool, *, token: str, owner_email: str = "") -> Starlette:
    # The owner's own address, keyed the same way mapping keys everyone else,
    # so a mail he sent himself reads as his and closes the promise it makes
    # rather than being filed as if a stranger had sent it back to him.
    owner_key = mapping.identity(owner_email) if owner_email else None

    def guard(request: Request) -> None:
        header = request.headers.get("Authorization", "")
        given = header[7:] if header.startswith("Bearer ") else ""
        if not hmac.compare_digest(given, token):
            raise _Bad(401, "unauthorized", "bad token")

    async def health(request: Request):
        guard(request)
        with pool.conn() as c:
            arrival.sweep(c)
            row = c.execute("SELECT max(date_ts) FROM ews.messages"
                            " WHERE deleted_at IS NULL").fetchone()
        last = row["max"] if row else None
        return JSONResponse({
            "contract": CONTRACT, "source": SOURCE, "connected": True, "auth": "ok",
            "since": None,
            "last_message_at": (dt.datetime.fromtimestamp(last, dt.timezone.utc).isoformat()
                                if last else None),
            "capabilities": CAPABILITIES, "detail": None})

    async def messages(request: Request):
        guard(request)
        limit = min(int(request.query_params.get("limit", PAGE_LIMIT)), PAGE_LIMIT)
        until = _parse_time(request.query_params.get("until"))
        with pool.conn() as c:
            arrival.sweep(c)
            gen = arrival.generation(c)
            after = _parse_cursor(request.query_params.get("cursor"), gen)
            rows = arrival.page(c, after_seq=after, until=until, limit=limit)
            # Spec §3.2: an empty page still has to say where to resume, and
            # the terminal page echoes the cursor it was given rather than
            # moving it, so two polls with nothing between them never skip a
            # message.  With `until` set, a full page (== limit) means the
            # bounded stream is not exhausted yet — more sits inside the
            # bound, so the cursor stays on the last row returned, exactly as
            # it does without `until`. Only when `until` cut the page short
            # (fewer rows than asked for) is the bounded stream known to be
            # exhausted, and then the cursor must jump past everything
            # `until` excluded — to the live head — or a bounded replay
            # would re-offer the tail of history it just finished walking.
            if until is not None and len(rows) < limit:
                seq = arrival.head(c)
            elif rows:
                seq = rows[-1]["seq"]
            else:
                seq = after if after is not None else arrival.head(c)
            return JSONResponse({
                "messages": [mapping.message(r, owner_key=owner_key) for r in rows],
                "chats": [mapping.chat(r) for r in rows],
                "next": f"v1:{gen}:{seq}"})

    async def on_error(request: Request, exc: Exception):
        if isinstance(exc, _Bad):
            return JSONResponse({"error": {"code": exc.code, "message": exc.message}},
                                status_code=exc.status)
        raise exc

    return Starlette(routes=[Route("/bridge/v1/health", health),
                             Route("/bridge/v1/messages", messages)],
                     exception_handlers={_Bad: on_error})
