"""The walk a real consumer performs, end to end.

Every other test in `tests/bridge/` looks at one page in isolation, and a page
that is correct on its own can still belong to a stream that delivers nothing:
bootstrap walks history with `until`, keeps the cursor it ends on, and polls
live from there, so a defect in either half is invisible until the two are
driven together. That is what these tests do — seed a store, bootstrap it,
then poll — and they assert the only thing the owner actually cares about:
every mail inside his window reaches the ledger exactly once.
"""
import datetime as dt

from starlette.testclient import TestClient

from ewsmcp.bridge import app as bridge_app

AUTH = {"Authorization": "Bearer t"}
WINDOW_DAYS = 14


def _client(db, **kw):
    return TestClient(bridge_app.build_app(db, token="t", **kw))


def _msg(conn, ews_id, sent: dt.datetime, *, changekey="ck1"):
    conn.execute(
        "INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id,"
        " sender_email, subject, date_ts, body_clean)"
        " VALUES (%s,%s,'inbox',%s,'a@example.test','s',%s,'b')",
        (ews_id, changekey, "conv-" + ews_id, int(sent.timestamp())))


def _bootstrap(client, window_start: dt.datetime, *, limit: int) -> str:
    """Mindet's own `_bootstrap` (mindet/connectors/generic.py), transcribed:
    page to the window's edge with `until`, post nothing, keep the cursor the
    walk ends on. Guards against a cursor that goes nowhere the same way."""
    seen: set[str] = set()
    token: str | None = None
    walked: list[str] = []
    while True:
        params = {"until": window_start.isoformat(), "limit": str(limit)}
        if token:
            params["since"] = token
        page = client.get("/bridge/v1/messages", params=params, headers=AUTH).json()
        if not page["messages"] or page["next"] in seen:
            return page["next"], walked
        walked += [m["native_id"] for m in page["messages"]]
        seen.add(page["next"])
        token = page["next"]


def _poll(client, cursor: str, *, limit: int) -> list[str]:
    """The live tick: page from the stored cursor until a page comes back empty."""
    got: list[str] = []
    while True:
        page = client.get("/bridge/v1/messages",
                          params={"since": cursor, "limit": str(limit)},
                          headers=AUTH).json()
        cursor = page["next"]
        if not page["messages"]:
            return got, cursor
        got += [m["native_id"] for m in page["messages"]]


def test_bootstrap_then_poll_delivers_every_message_in_the_window_exactly_once(db):
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    old = {"old-1": now - dt.timedelta(days=200),
           "old-2": now - dt.timedelta(days=120),
           "old-3": now - dt.timedelta(days=40)}
    fresh = {"new-1": now - dt.timedelta(days=10),
             "new-2": now - dt.timedelta(days=3),
             "new-3": now - dt.timedelta(hours=2)}
    with db.conn() as conn:
        # Interleaved on insert, so nothing about the walk can be an artefact
        # of the order rows happened to be written in.
        for ews_id in ("new-2", "old-1", "new-3", "old-3", "new-1", "old-2"):
            _msg(conn, ews_id, {**old, **fresh}[ews_id])
    c = _client(db)

    # limit=2 so the bootstrap walk takes several pages: a walk that only ever
    # sees one page cannot catch a cursor that mis-advances between them.
    cursor, walked = _bootstrap(c, window_start, limit=2)
    assert walked == ["old-1", "old-2", "old-3"], (
        "bootstrap must walk the history it is bounding, oldest first")

    delivered, _ = _poll(c, cursor, limit=2)
    assert delivered == ["new-1", "new-2", "new-3"]


def test_a_second_poll_on_the_same_cursor_delivers_nothing_further(db):
    """The live cursor is stored and reused. If a poll ever re-offered what it
    just gave, the ledger would grow a duplicate promise every thirty seconds."""
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    with db.conn() as conn:
        _msg(conn, "old-1", now - dt.timedelta(days=90))
        _msg(conn, "new-1", now - dt.timedelta(days=1))
    c = _client(db)
    cursor, _ = _bootstrap(c, window_start, limit=500)
    delivered, cursor = _poll(c, cursor, limit=500)
    assert delivered == ["new-1"]
    again, _ = _poll(c, cursor, limit=500)
    assert again == []


def test_an_amended_mail_reaches_a_consumer_holding_a_live_cursor_once(db):
    """Exchange rewrites a message in place and moves its `changekey`. The
    consumer's cursor is already past the old row, so the only way the change
    is ever seen is a new sequence number — and exactly one of them, or the
    ledger reconciles the same mail twice."""
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    with db.conn() as conn:
        _msg(conn, "old-1", now - dt.timedelta(days=90))
        _msg(conn, "new-1", now - dt.timedelta(days=1))
    c = _client(db)
    cursor, _ = _bootstrap(c, window_start, limit=500)
    delivered, cursor = _poll(c, cursor, limit=500)
    assert delivered == ["new-1"]

    with db.conn() as conn:
        conn.execute("UPDATE ews.messages SET changekey='ck2', body_clean='amended'"
                     " WHERE ews_id='new-1'")
    delivered, cursor = _poll(c, cursor, limit=500)
    assert delivered == ["new-1"]
    again, _ = _poll(c, cursor, limit=500)
    assert again == []
