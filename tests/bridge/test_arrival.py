import datetime as dt

from ewsmcp.bridge import arrival


def _msg(conn, ews_id, *, changekey="ck1", date_ts=1_700_000_000, folder="inbox"):
    conn.execute(
        "INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id,"
        " sender_email, subject, date_ts, body_clean)"
        " VALUES (%s,%s,%s,%s,'a@example.test','s',%s,'b')",
        (ews_id, changekey, folder, "conv-" + ews_id, date_ts))


def test_sweep_assigns_one_sequence_per_message_and_is_idempotent(db):
    with db.conn() as c:
        _msg(c, "m1"); _msg(c, "m2")  # noqa: E702
        assert arrival.sweep(c) == 2
        assert arrival.sweep(c) == 0
        seqs = [r["seq"] for r in arrival.page(c, after_seq=None, until=None, limit=10)]
        assert seqs == sorted(seqs) and len(seqs) == 2


def test_a_message_discovered_late_arrives_after_one_sent_later(db):
    """The whole reason this table exists: a folder sync writes a three-week-old
    mail today, and a cursor over the send date would have stepped past it."""
    with db.conn() as c:
        _msg(c, "recent", date_ts=1_700_000_900)
        arrival.sweep(c)
        _msg(c, "old", date_ts=1_600_000_000)
        arrival.sweep(c)
        ids = [r["ews_id"] for r in arrival.page(c, after_seq=None, until=None, limit=10)]
        assert ids == ["recent", "old"]


def test_an_edited_message_is_re_emitted_with_a_new_sequence(db):
    with db.conn() as c:
        _msg(c, "m1")
        arrival.sweep(c)
        first = arrival.head(c)
        c.execute("UPDATE ews.messages SET changekey='ck2' WHERE ews_id='m1'")
        assert arrival.sweep(c) == 1
        assert arrival.head(c) > first
        rows = arrival.page(c, after_seq=first, until=None, limit=10)
        assert [r["ews_id"] for r in rows] == ["m1"]


def test_until_stops_the_page_at_the_send_time_it_names(db):
    with db.conn() as c:
        _msg(c, "early", date_ts=1_700_000_000)
        _msg(c, "late", date_ts=1_700_009_000)
        arrival.sweep(c)
        cut = dt.datetime.fromtimestamp(1_700_005_000, dt.timezone.utc)
        rows = arrival.page(c, after_seq=None, until=cut, limit=10)
        assert [r["ews_id"] for r in rows] == ["early"]
