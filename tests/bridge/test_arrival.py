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
        _msg(c, "m1")
        _msg(c, "m2")
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


def test_until_bounds_the_page_by_arrival_not_by_send_time(db):
    """Spec §3.2: `until` bounds arrival. A mail sent in March and discovered
    today arrived today, and a replay bounded by the send date would leave it
    out of a window it genuinely belongs to."""
    with db.conn() as c:
        # 'old' was sent long before 'recent' and learned of afterwards, which
        # is the ordinary shape of a folder sync catching up.
        _msg(c, "recent", date_ts=1_700_009_000)
        arrival.sweep(c)
        _msg(c, "old", date_ts=1_600_000_000)
        arrival.sweep(c)
        c.execute("UPDATE ews.bridge_arrival SET first_seen = %s WHERE ews_id = %s",
                  (dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), "recent"))
        c.execute("UPDATE ews.bridge_arrival SET first_seen = %s WHERE ews_id = %s",
                  (dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc), "old"))
        cut = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
        rows = arrival.page(c, after_seq=None, until=cut, limit=10)
        # only 'recent' arrived before the cut, even though 'old' was sent
        # thirteen years earlier by the send clock
        assert [r["ews_id"] for r in rows] == ["recent"]
