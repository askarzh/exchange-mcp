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


def _as_existing_history(db):
    """Make the mail already in the store into history the ledger reconstructs.

    This is the real shape of first contact and the only way history exists:
    the mailbox is years old, and the bridge's ledger arrives afterwards.
    Migration 005 reconstructs an arrival time from each mail's send date,
    once. `sweep()` never does — a mail it meets for the first time arrived
    now, whatever its send date says — so a test that seeds old mail and simply
    sweeps is testing a store with no history at all.
    """
    with db.conn() as c:
        c.execute("TRUNCATE ews.bridge_arrival")
    db.reapply_migration_for_tests(5)
    db.reapply_migration_for_tests(6)


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
    _as_existing_history(db)
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
    _as_existing_history(db)
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
    _as_existing_history(db)
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


def test_a_store_with_no_mail_older_than_the_window_still_delivers_all_of_it(db):
    """A fresh mailbox, a store pruned to the window, or a re-bootstrap after a
    `400 cursor_generation` against a rebuilt store holding only recent mail.
    Bootstrap's very first bounded page is empty — there is no history to walk —
    and if an empty page hands back the live head rather than the cursor it was
    given, the live cursor lands past everything and not one in-window message
    is ever delivered. Every other test here seeds old mail, so every other test
    is blind to it."""
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    with db.conn() as conn:
        _msg(conn, "new-1", now - dt.timedelta(days=9))
        _msg(conn, "new-2", now - dt.timedelta(days=4))
        _msg(conn, "new-3", now - dt.timedelta(minutes=20))
    c = _client(db)

    cursor, walked = _bootstrap(c, window_start, limit=2)
    assert walked == []                       # there is no history to walk

    delivered, cursor = _poll(c, cursor, limit=2)
    assert delivered == ["new-1", "new-2", "new-3"]
    again, _ = _poll(c, cursor, limit=2)
    assert again == []


def test_a_mail_with_no_send_date_reaches_the_consumer_exactly_once(db):
    """Exchange writes a null `date_ts` for drafts, calendar notices and
    headers it could not parse. Such a mail arrives now, so bootstrap must
    leave it alone and the live poll must deliver it. Sequenced before all of
    history instead — which is what `NULLS FIRST` did — it carried an arrival
    time of now(), so every bounded page excluded it while the bootstrap cursor
    ended far above its sequence: behind the cursor for ever, with no error."""
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    with db.conn() as conn:
        _msg(conn, "old-1", now - dt.timedelta(days=200))
        _msg(conn, "old-2", now - dt.timedelta(days=100))
        _msg(conn, "new-1", now - dt.timedelta(days=5))
    _as_existing_history(db)
    with db.conn() as conn:
        # …and the undated one turns up afterwards, so it is a discovery the
        # sweep has to place, not history the migration reconstructed.
        conn.execute(
            "INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id,"
            " sender_email, subject, date_ts, body_clean)"
            " VALUES ('undated','ck1','inbox','conv-undated','a@example.test','s',"
            " NULL,'b')")
    c = _client(db)

    cursor, walked = _bootstrap(c, window_start, limit=2)
    assert walked == ["old-1", "old-2"]

    delivered, cursor = _poll(c, cursor, limit=2)
    assert delivered == ["new-1", "undated"]
    again, _ = _poll(c, cursor, limit=2)
    assert again == []


def test_a_mail_amended_mid_bootstrap_does_not_skip_the_ingestion_window(db):
    """The owner marks a 45-day-old mail as read in Outlook while Mindet is
    still walking history. Exchange moves its changekey, the next sweep gives
    it a sequence at the live head — and if it kept its old arrival time, the
    next bounded page would return it, come back short, and end bootstrap with
    a live cursor at the head. Every message in the fourteen-day window would
    then sit below that cursor, lost for good with nothing to say so.

    Driven page by page rather than through `_bootstrap`, because the whole
    point is what happens *between* two pages of one walk."""
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    old = ["old-1", "old-2", "old-3"]
    fresh = ["new-1", "new-2", "new-3"]
    with db.conn() as conn:
        for i, ews_id in enumerate(old):
            _msg(conn, ews_id, now - dt.timedelta(days=60 - i))
        for i, ews_id in enumerate(fresh):
            _msg(conn, ews_id, now - dt.timedelta(days=10 - i * 3))
    _as_existing_history(db)
    c = _client(db)

    params = {"until": window_start.isoformat(), "limit": "2"}
    page = c.get("/bridge/v1/messages", params=params, headers=AUTH).json()
    assert [m["native_id"] for m in page["messages"]] == ["old-1", "old-2"]
    token = page["next"]

    # …and now Outlook touches an old mail, mid-walk.
    with db.conn() as conn:
        conn.execute("UPDATE ews.messages SET changekey='ck2' WHERE ews_id='old-1'")

    walked = []
    while True:
        params = {"until": window_start.isoformat(), "limit": "2", "since": token}
        page = c.get("/bridge/v1/messages", params=params, headers=AUTH).json()
        if not page["messages"]:
            break
        walked += [m["native_id"] for m in page["messages"]]
        token = page["next"]
    assert walked == ["old-3"], "the amended mail arrived now, so it is not history"

    delivered, token = _poll(c, token, limit=2)
    assert sorted(delivered) == sorted(fresh + ["old-1"])
    assert len(delivered) == len(set(delivered))
    again, _ = _poll(c, token, limit=2)
    assert again == []


def test_old_mail_discovered_mid_bootstrap_does_not_skip_the_ingestion_window(db):
    """The shape that has now caught three separate bugs, in its third form.

    A folder sync discovers genuinely old mail while the consumer is still
    walking history. If that mail entered the ledger with an arrival time
    reconstructed from its send date, it would take a sequence at the live head
    *and* an arrival inside the bound: the next bounded page would return it,
    come back short, and bootstrap would finish with a cursor above the whole
    fourteen-day window. Discovered today means arrived today, and then the
    bounded page correctly leaves it alone."""
    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    fresh = ["new-1", "new-2", "new-3"]
    with db.conn() as conn:
        for i, ews_id in enumerate(("old-1", "old-2")):
            _msg(conn, ews_id, now - dt.timedelta(days=60 - i))
        for i, ews_id in enumerate(fresh):
            _msg(conn, ews_id, now - dt.timedelta(days=10 - i * 3))
    _as_existing_history(db)
    c = _client(db)

    params = {"until": window_start.isoformat(), "limit": "1"}
    page = c.get("/bridge/v1/messages", params=params, headers=AUTH).json()
    assert [m["native_id"] for m in page["messages"]] == ["old-1"]
    token = page["next"]

    # …and a folder sync lands three-month-old mail, mid-walk.
    with db.conn() as conn:
        _msg(conn, "found-1", now - dt.timedelta(days=90))
        _msg(conn, "found-2", now - dt.timedelta(days=80))

    walked = []
    while True:
        params = {"until": window_start.isoformat(), "limit": "1", "since": token}
        page = c.get("/bridge/v1/messages", params=params, headers=AUTH).json()
        if not page["messages"]:
            break
        walked += [m["native_id"] for m in page["messages"]]
        token = page["next"]
    assert walked == ["old-2"], "mail discovered today is not history"

    delivered, token = _poll(c, token, limit=2)
    assert len(delivered) == len(set(delivered))
    for ews_id in fresh:
        assert delivered.count(ews_id) == 1
    assert set(delivered) == set(fresh) | {"found-1", "found-2"}
    again, _ = _poll(c, token, limit=2)
    assert again == []
