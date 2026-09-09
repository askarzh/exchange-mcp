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
PAGE_LIMIT = 500


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


def _parse_limit(value: str | None) -> int:
    if value is None:
        return PAGE_LIMIT
    try:
        parsed = int(value)
    except ValueError:
        raise _Bad(400, "bad_request", "limit must be a whole number") from None
    if parsed < 1:
        raise _Bad(400, "bad_request", "limit must be at least 1")
    return min(parsed, PAGE_LIMIT)


def build_app(pool, *, token: str, owner_email: str = "") -> Starlette:
    # An empty token would make `compare_digest` true against a request that
    # sends no header at all, so a missing environment variable would quietly
    # publish the mailbox rather than fail to start.
    if not token:
        raise ValueError("EWS_BRIDGE_TOKEN must not be empty")

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
        # Deliberately reports the mail store, not the arrival ledger: this
        # answer never reads bridge_arrival, so sweeping here would only take
        # write locks on every poll for no effect on the response. Don't add
        # it back.
        with pool.conn() as c:
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
        limit = _parse_limit(request.query_params.get("limit"))
        until = _parse_time(request.query_params.get("until"))
        with pool.conn() as c:
            arrival.sweep(c)
            gen = arrival.generation(c)
            after = _parse_cursor(request.query_params.get("since"), gen)
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
                "next": f"v1:{gen}:{seq}"})

    async def chats(request: Request):
        guard(request)
        # `since` is accepted and ignored (spec §3.4): mail conversations
        # change constantly and this store is small enough to answer in
        # full every time, so a cursor here would be a promise the bridge
        # does not keep.
        with pool.conn() as c:
            rows = c.execute(
                "SELECT coalesce(conversation_id, ews_id) AS native_id,"
                " max(subject) AS name, max(to_json) AS to_json"
                " FROM ews.messages WHERE deleted_at IS NULL"
                " GROUP BY coalesce(conversation_id, ews_id)"
                " ORDER BY max(date_ts) DESC LIMIT %s", (PAGE_LIMIT,)).fetchall()
        return JSONResponse({"chats": [
            {"native_id": r["native_id"],
             "kind": "group" if len(mapping.recipients(dict(r))) > 1 else "direct",
             "name": r["name"] or None,
             "member_count": len(mapping.recipients(dict(r))) + 1}
            for r in rows]})

    async def contacts(request: Request):
        guard(request)
        # Spec §3.4: a directory of tens of thousands of senders is never
        # dumped in full, so this list is capped at PAGE_LIMIT rather than
        # answering with everyone mail has ever seen.
        with pool.conn() as c:
            rows = c.execute(
                "SELECT lower(sender_email) AS email, max(sender_name) AS name"
                " FROM ews.messages"
                " WHERE deleted_at IS NULL AND sender_email IS NOT NULL"
                "   AND sender_email <> ''"
                " GROUP BY lower(sender_email) ORDER BY max(date_ts) DESC LIMIT %s",
                (PAGE_LIMIT,)).fetchall()
        return JSONResponse({"contacts": [
            {"native_id": r["email"], "key": f"email:{r['email']}",
             "name": r["name"] or r["email"], "aliases": []} for r in rows]})

    async def on_error(request: Request, exc: Exception):
        if isinstance(exc, _Bad):
            return JSONResponse({"error": {"code": exc.code, "message": exc.message}},
                                status_code=exc.status)
        raise exc

    return Starlette(routes=[Route("/bridge/v1/health", health),
                             Route("/bridge/v1/messages", messages),
                             Route("/bridge/v1/chats", chats),
                             Route("/bridge/v1/contacts", contacts)],
                     exception_handlers={_Bad: on_error})
