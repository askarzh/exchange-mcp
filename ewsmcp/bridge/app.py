"""The mail bridge (platform spec §3): a read-only window on the store
`exchange-mcp` already keeps, in the vocabulary Mindet speaks.

It is a separate app on a separate port from `ewsd` on purpose. The mail daemon
holds a live Exchange connection and is the more valuable of the two; a bridge
fault should cost Mindet its mail source and nothing else.
"""
from __future__ import annotations

import datetime as dt
import hmac
import logging
import time

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from . import arrival, mapping

logger = logging.getLogger(__name__)

CONTRACT = 1
SOURCE = "ews"
CAPABILITIES = ["messages", "chats", "contacts"]
PAGE_LIMIT = 500

# How stale `ews.sync_state` may get before the bridge stops calling itself
# connected. `ewsd` runs its item lane every EWS_CACHE_SYNC_SECONDS (45s by
# default), so twenty missed cycles is far past a transient Exchange hiccup or
# a container restart, and still short enough that a store frozen overnight is
# on the owner's morning list rather than reading as healthy.
STALE_AFTER_SECONDS = 15 * 60


class _Bad(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message


def _parse_cursor(value: str | None, gen: int) -> int | None:
    if not value:
        return None
    # Every log line here says the shape of what arrived, never the value: a
    # cursor is not secret, but a habit of printing whatever a caller sent is
    # how a token or an address ends up in a log file one refactor later.
    parts = value.split(":")
    if len(parts) != 3 or parts[0] != "v1":
        logger.warning("bridge: rejected a cursor of %d part(s), prefix %r",
                       len(parts), parts[0][:8])
        raise _Bad(400, "bad_cursor", "cursor is not of this contract")
    try:
        cursor_gen, seq = int(parts[1]), int(parts[2])
    except ValueError:
        logger.warning("bridge: rejected a cursor whose generation or sequence "
                       "is not a number")
        raise _Bad(400, "bad_cursor", "cursor is not of this contract") from None
    if cursor_gen != gen:
        # Worth an INFO rather than a warning: this is the designed signal that
        # the store was rebuilt, and the consumer's answer to it is to
        # bootstrap again. Seeing it once is healthy; seeing it every poll is
        # the thing to chase.
        logger.info("bridge: refused a cursor from generation %d; this store is "
                    "generation %d — the consumer should bootstrap again",
                    cursor_gen, gen)
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
        # Compared as bytes: Starlette decodes headers as latin-1, and
        # `compare_digest` raises TypeError on a str with a character above
        # U+007F, so `Authorization: Bearer café` would 500 out of the
        # credential check instead of being refused.
        if not hmac.compare_digest(given.encode("latin-1"), token.encode()):
            # The token itself is never logged, not even truncated, and neither
            # is the header — only whether one was offered in the right shape.
            # That is enough to tell a misconfigured consumer from a probe.
            logger.warning("bridge: refused %s on %s",
                           "a bearer token that did not match" if given
                           else "a request with no bearer token", request.url.path)
            raise _Bad(401, "unauthorized", "bad token")

    async def health(request: Request):
        guard(request)
        # Deliberately reports the mail store, not the arrival ledger: this
        # answer never reads bridge_arrival, so sweeping here would only take
        # write locks on every poll for no effect on the response. Don't add
        # it back.
        with pool.conn() as c:
            row = c.execute("SELECT max(date_ts) AS last_ts FROM ews.messages"
                            " WHERE deleted_at IS NULL").fetchone()
            state = c.execute(
                "SELECT max(as_of) AS as_of FROM ews.sync_state").fetchone()
        last = row["last_ts"] if row else None
        # `connected` is about the source, not about Postgres. Postgres
        # answering says only that the bridge can read a store; if `ewsd` died
        # a week ago that store is frozen, and Mindet gates its whole tick on
        # this field — so a bridge that always said `true` would let the mail
        # source read as healthy while nothing at all arrived. `sync_state`
        # carries the watermark `ewsd` stamps on every sync cycle; that is the
        # honest answer.
        as_of = state["as_of"] if state else None
        now = time.time()
        if as_of is None:
            connected, detail = False, "the mail store has never synced"
        elif now - float(as_of) > STALE_AFTER_SECONDS:
            age = int(now - float(as_of))
            connected, detail = False, f"the mail store last synced {age}s ago"
        else:
            connected, detail = True, None
        return JSONResponse({
            "contract": CONTRACT, "source": SOURCE, "connected": connected, "auth": "ok",
            "since": (dt.datetime.fromtimestamp(float(as_of), dt.timezone.utc).isoformat()
                      if as_of is not None else None),
            # A date_ts of 0 is a real (if absurd) timestamp and falsy, so this
            # asks whether there is a value, not whether it is truthy.
            "last_message_at": (dt.datetime.fromtimestamp(last, dt.timezone.utc).isoformat()
                                if last is not None else None),
            "capabilities": CAPABILITIES, "detail": detail})

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
            # message. `until` does not change that rule. It does not exclude
            # old mail from a page — it is how a consumer *walks* old mail:
            # bootstrap pages with `until` at the edge of its ingestion window,
            # posting nothing, and keeps the cursor the walk ends on so the
            # live cursor starts at that edge. A cursor that jumped to the live
            # head on the last bounded page would carry the consumer straight
            # past every message inside the window, for ever, with no error
            # anywhere. So: the last row returned, or the cursor we were given.
            #
            # And when there was no cursor, that is zero — never the head. A
            # page that returned nothing has not carried the consumer
            # anywhere, so the cursor must stay where it was, and for a
            # consumer that has never asked before "where it was" is the
            # beginning. Handing back the head instead loses a store with no
            # mail older than the window — a fresh mailbox, a store pruned to
            # the window, a re-bootstrap against a rebuilt one — because its
            # first bounded page is empty and every in-window message then
            # sits below the cursor for good.
            if rows:
                seq = rows[-1]["seq"]
            else:
                seq = after if after is not None else 0
            return JSONResponse({
                "messages": [mapping.message(r, owner_key=owner_key) for r in rows],
                "next": f"v1:{gen}:{seq}"})

    async def chats(request: Request):
        guard(request)
        # `since` is accepted and ignored (spec §3.4): mail conversations
        # change constantly and this store is small enough to answer in
        # full every time, so a cursor here would be a promise the bridge
        # does not keep.
        #
        # And answered in full it is — no cap, not even a large one. The
        # consumer classifies each message by its chat and *skips* a message
        # whose chat this route did not name, advancing its cursor past it;
        # the message is then gone for good. A cap by recency loses exactly
        # the case the arrival ledger exists for: a folder sync discovering
        # months-old mail lands a message in a conversation whose last
        # activity is far outside the most recent N. Every conversation the
        # `messages` stream can name must appear here, so the answer is every
        # conversation.
        #
        # A conversation's membership is likewise the union of every sender
        # and recipient it has ever had (see mapping.chats_from_rows), which
        # a bounded scan cannot decide either: it drops a long thread's older
        # senders and silently shrinks a group back to a false "direct", the
        # same way the byte-wise max() bug did.
        with pool.conn() as c:
            rows = c.execute(
                # The same pinned id `messages` hands out — from the arrival
                # ledger, not a fresh coalesce. If the two ever disagreed,
                # `chats` would name a conversation under one id while the
                # message stream named it under another; the consumer would
                # find that message's chat unknown, skip it, and advance its
                # cursor past it. The coalesce is the fallback for a row that
                # has not been swept yet, and applies the rule that row will be
                # given when it is.
                "SELECT coalesce(a.chat_id, m.conversation_id, m.ews_id) AS native_id,"
                " m.subject, m.sender_email, m.to_json"
                " FROM ews.messages m"
                " LEFT JOIN ews.bridge_arrival a ON a.ews_id = m.ews_id"
                " WHERE m.deleted_at IS NULL"
                # Most recent first, so the first row met for a conversation
                # carries its newest subject and the conversations come back
                # in recency order. ews_id DESC breaks a same-second tie
                # deterministically (spec §3.2 does the same for the cursor,
                # and for the same reason): without it, which subject becomes
                # a conversation's `name` would depend on Postgres's arbitrary
                # tie order and could change between polls.
                " ORDER BY m.date_ts DESC NULLS LAST, m.ews_id DESC").fetchall()
        return JSONResponse({"chats": mapping.chats_from_rows([dict(r) for r in rows])})

    async def contacts(request: Request):
        guard(request)
        # Spec §3.4: a directory of tens of thousands of senders is never
        # dumped in full, so this list is capped at PAGE_LIMIT rather than
        # answering with everyone mail has ever seen. Unlike `chats`, dropping
        # a contact costs a roster suggestion, not a message.
        with pool.conn() as c:
            rows = c.execute(
                # DISTINCT ON takes the name from the address's most recent
                # message. `max(sender_name)` picked it byte-wise instead —
                # alphabetically, i.e. arbitrarily — the same class of bug as
                # the `max(to_json)` one this branch already fixed.
                "SELECT * FROM ("
                "  SELECT DISTINCT ON (lower(sender_email))"
                "         lower(sender_email) AS email, sender_name AS name,"
                "         date_ts"
                "    FROM ews.messages"
                "   WHERE deleted_at IS NULL AND sender_email IS NOT NULL"
                "     AND sender_email <> ''"
                "   ORDER BY lower(sender_email), date_ts DESC NULLS LAST, ews_id DESC"
                ") t ORDER BY date_ts DESC NULLS LAST LIMIT %s",
                (PAGE_LIMIT,)).fetchall()
        out = []
        for r in rows:
            # Spec §4: a bridge never fabricates a key. Exchange stores legacy
            # distinguished names in sender_email for addresses it could not
            # resolve, and mapping.identity refuses those on purpose; minting
            # `email:/o=ExchangeLabs/...` here would put a phantom person on
            # the roster that can never match the real one.
            key = mapping.identity(r["email"])
            if key is None:
                continue
            out.append({"native_id": r["email"], "key": key,
                        "name": r["name"] or r["email"], "aliases": []})
        return JSONResponse({"contacts": out})

    async def on_error(request: Request, exc: Exception):
        if isinstance(exc, _Bad):
            return JSONResponse({"error": {"code": exc.code, "message": exc.message}},
                                status_code=exc.status)
        raise exc

    async def on_server_error(request: Request, exc: Exception):
        # Anything that is not a _Bad is a fault in this bridge, and without a
        # line here it reaches the owner as a bare 500 in a uvicorn access log
        # with no traceback attached to the request that caused it. Registered
        # against Exception rather than folded into on_error above, because
        # Starlette only ever routes _Bad to that one.
        #
        # The path is logged; the query string is not, because `since` and
        # `until` are the only things in it and neither is worth the habit. The
        # response body stays generic — a traceback belongs in the log, never
        # in an answer to a caller.
        logger.exception("bridge: unhandled error serving %s", request.url.path)
        return JSONResponse({"error": {"code": "internal", "message": "internal error"}},
                            status_code=500)

    return Starlette(routes=[Route("/bridge/v1/health", health),
                             Route("/bridge/v1/messages", messages),
                             Route("/bridge/v1/chats", chats),
                             Route("/bridge/v1/contacts", contacts)],
                     exception_handlers={_Bad: on_error, Exception: on_server_error})
