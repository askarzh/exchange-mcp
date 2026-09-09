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


def test_a_sweep_gives_a_pre_existing_store_the_arrival_times_it_can_reconstruct(db):
    """Spec §3.2. `first_seen` used to default to now(), so a store the ledger
    met for the first time had its whole history stamped as having arrived at
    that moment — and a consumer bootstrapping to the edge of its window would
    find nothing before that edge, take a cursor at the live head, and never be
    offered a message again. A mail that carries a send time arrived, as far as
    anyone can now reconstruct, when it was sent; and it enters the sequence in
    that order, so `until` stays a prefix of the stream."""
    months = {"jan": dt.datetime(2026, 1, 15, tzinfo=dt.timezone.utc),
              "mar": dt.datetime(2026, 3, 15, tzinfo=dt.timezone.utc),
              "jun": dt.datetime(2026, 6, 15, tzinfo=dt.timezone.utc),
              "sep": dt.datetime(2026, 9, 15, tzinfo=dt.timezone.utc)}
    with db.conn() as c:
        # written in a deliberately unhelpful order
        for name in ("jun", "jan", "sep", "mar"):
            _msg(c, name, date_ts=int(months[name].timestamp()))
        assert arrival.sweep(c) == 4
        cut = dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc)
        rows = arrival.page(c, after_seq=None, until=cut, limit=10)
        assert [r["ews_id"] for r in rows] == ["jan", "mar"]
        seqs = [r["seq"] for r in rows]
        assert seqs == sorted(seqs)
        # and the rest of history sits after them in the same order
        rest = arrival.page(c, after_seq=seqs[-1], until=None, limit=10)
        assert [r["ews_id"] for r in rest] == ["jun", "sep"]


def test_a_mail_with_no_send_time_arrives_now(db):
    """There is nothing better to say about it, and a ledger of deadlines
    cannot hold a message with no time at all."""
    before = dt.datetime.now(dt.timezone.utc)
    with db.conn() as c:
        c.execute(
            "INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id,"
            " sender_email, subject, date_ts, body_clean)"
            " VALUES ('undated','ck','inbox','conv','a@example.test','s',NULL,'b')")
        arrival.sweep(c)
        row = arrival.page(c, after_seq=None, until=None, limit=10)[0]
    assert row["first_seen"] >= before


def test_an_amendment_takes_a_new_sequence_but_keeps_its_arrival_time(db):
    """A re-arrival is a new place in the stream, not a new arrival time.
    Rewriting first_seen would drag an amended old mail forward into a window
    it does not belong to."""
    with db.conn() as c:
        _msg(c, "m1", date_ts=int(dt.datetime(2026, 1, 15,
                                              tzinfo=dt.timezone.utc).timestamp()))
        arrival.sweep(c)
        was = arrival.page(c, after_seq=None, until=None, limit=1)[0]
        c.execute("UPDATE ews.messages SET changekey='ck2' WHERE ews_id='m1'")
        arrival.sweep(c)
        now_row = arrival.page(c, after_seq=None, until=None, limit=1)[0]
    assert now_row["seq"] > was["seq"]
    assert now_row["first_seen"] == was["first_seen"]


def test_a_mail_with_no_send_date_is_sequenced_at_the_head_not_before_history(db):
    """Exchange writes a null date_ts for drafts, calendar notices and headers
    it could not parse. Such a mail arrives now, so it must be sequenced now.
    Ordered NULLS FIRST it took seq 1 while carrying first_seen = now(): every
    bounded bootstrap page excluded it (its arrival is after the window edge)
    and the bootstrap cursor ended far above it, so it sat behind the cursor
    for ever, undelivered, with no error anywhere."""
    with db.conn() as c:
        for i, month in enumerate((1, 3, 6)):
            _msg(c, f"dated-{i}", date_ts=int(
                dt.datetime(2026, month, 15, tzinfo=dt.timezone.utc).timestamp()))
        c.execute(
            "INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id,"
            " sender_email, subject, date_ts, body_clean)"
            " VALUES ('undated','ck','inbox','conv-undated','a@example.test','s',NULL,'b')")
        arrival.sweep(c)
        rows = arrival.page(c, after_seq=None, until=None, limit=10)
    assert [r["ews_id"] for r in rows] == ["dated-0", "dated-1", "dated-2", "undated"]


def test_the_first_migration_backfills_arrival_from_send_time_in_send_order(db):
    """The other half of the same rule, for the store that already exists when
    migration 005 lands. The sequence is assigned there with an explicit
    row_number() rather than by nextval() under an ORDER BY, because nextval is
    evaluated wherever the planner puts it — and if the pre-existing store's
    numbering did not follow send time, a bootstrap's cursor would leave
    in-window mail sitting below it, unreachable."""
    months = ["2026-01-15", "2026-03-15", "2026-06-15"]
    with db.conn() as c:
        for i, day in enumerate(months):
            _msg(c, f"m{i}", date_ts=int(
                dt.datetime.fromisoformat(day + "T00:00:00+00:00").timestamp()))
        c.execute("TRUNCATE ews.bridge_arrival")           # as if 005 had not run
    db.reapply_migration_for_tests(5)
    with db.conn() as c:
        rows = arrival.page(c, after_seq=None, until=None, limit=10)
        assert [r["ews_id"] for r in rows] == ["m0", "m1", "m2"]
        assert [r["first_seen"].date().isoformat() for r in rows] == months
        # and the sequence carries on from the backfill rather than colliding
        _msg(c, "m3", date_ts=1_800_000_000)
        arrival.sweep(c)
        after = arrival.page(c, after_seq=rows[-1]["seq"], until=None, limit=10)
        assert [r["ews_id"] for r in after] == ["m3"]


def test_the_cursor_generation_is_this_stores_own_and_survives_a_restart(db):
    """A rebuilt volume comes back with seq restarting at 1. With the
    generation fixed at 1, Mindet's stored `v1:1:2417` would be *accepted*
    against a ledger holding forty rows: every poll empty, the
    400-and-re-bootstrap path never firing, mail stopped for good with no
    error anywhere."""
    with db.conn() as c:
        first = arrival.generation(c)
    assert first != 1
    db.migrate()                                            # a restart re-migrates
    with db.conn() as c:
        assert arrival.generation(c) == first
        c.execute("DROP TABLE ews.bridge_meta")             # the volume was rebuilt
    db.reapply_migration_for_tests(5)
    with db.conn() as c:
        assert arrival.generation(c) != first
