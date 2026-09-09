import datetime as dt

from starlette.testclient import TestClient

from ewsmcp.bridge import app as bridge_app


def _client(db, **kw):
    return TestClient(bridge_app.build_app(db, token="t", **kw))


def _msg(conn, ews_id, *, date_ts=1_700_000_000, sender_email="a@example.test"):
    conn.execute(
        "INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id,"
        " sender_email, subject, date_ts, body_clean)"
        " VALUES (%s,'ck','inbox',%s,%s,'s',%s,'b')",
        (ews_id, "conv-" + ews_id, sender_email, date_ts))


def test_health_declares_version_one_and_no_send(db):
    r = _client(db).get("/bridge/v1/health", headers={"Authorization": "Bearer t"})
    assert r.status_code == 200
    h = r.json()
    assert h["contract"] == 1 and h["source"] == "ews"
    assert "send" not in h["capabilities"] and "login" not in h["capabilities"]
    assert h["auth"] == "ok"


def test_a_wrong_token_is_refused_everywhere(db):
    c = _client(db)
    for path in ("/bridge/v1/health", "/bridge/v1/messages"):
        assert c.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert c.get(path).status_code == 401


def test_an_empty_page_still_carries_a_cursor_to_come_back_with(db):
    """Spec §3.2. A page with no messages must still say where to resume, or a
    quiet mailbox makes Mindet start from the beginning on every poll."""
    c = _client(db)
    r = c.get("/bridge/v1/messages", headers={"Authorization": "Bearer t"}).json()
    assert r["messages"] == [] and r["next"]


def test_the_terminal_page_echoes_the_cursor_it_was_given(db):
    with db.conn() as conn:
        _msg(conn, "m1")
    c = _client(db)
    first = c.get("/bridge/v1/messages", headers={"Authorization": "Bearer t"}).json()
    assert [m["native_id"] for m in first["messages"]] == ["m1"]
    again = c.get("/bridge/v1/messages", params={"cursor": first["next"]},
                  headers={"Authorization": "Bearer t"}).json()
    assert again["messages"] == [] and again["next"] == first["next"]


def test_a_cursor_from_another_generation_is_refused(db):
    """Rebuilding the ledger renumbers everything. Accepting an old cursor would
    silently skip whatever now sits below that number; 400 makes Mindet
    bootstrap instead of going quietly blind."""
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"cursor": "v1:99:0"},
              headers={"Authorization": "Bearer t"})
    assert r.status_code == 400


def test_a_malformed_cursor_is_refused_rather_than_treated_as_the_beginning(db):
    c = _client(db)
    for bad in ("nonsense", "v1:1", "v2:1:0", "v1:x:1"):
        assert c.get("/bridge/v1/messages", params={"cursor": bad},
                     headers={"Authorization": "Bearer t"}).status_code == 400


def test_until_is_accepted_without_since(db):
    """`until` bounds by arrival time (spec §3.2, task-1's arrival.page), not
    by send time. `first_seen` defaults to now() on insert, so bounding it
    requires setting first_seen explicitly after sweep — the brief's own test
    used send-time reasoning and would not exercise the real rule."""
    with db.conn() as conn:
        _msg(conn, "early", date_ts=1_700_000_000)
        bridge_app.arrival.sweep(conn)
        _msg(conn, "late", date_ts=1_700_009_000)
        bridge_app.arrival.sweep(conn)
        conn.execute("UPDATE ews.bridge_arrival SET first_seen = %s WHERE ews_id = %s",
                    (dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), "early"))
        conn.execute("UPDATE ews.bridge_arrival SET first_seen = %s WHERE ews_id = %s",
                    (dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc), "late"))
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"until": "2026-03-01T00:00:00+00:00"},
              headers={"Authorization": "Bearer t"}).json()
    assert [m["native_id"] for m in r["messages"]] == ["early"]


def test_until_without_a_timezone_is_refused(db):
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"until": "2026-03-01T00:00:00"},
              headers={"Authorization": "Bearer t"})
    assert r.status_code == 400


def test_a_bounded_page_that_until_cut_short_jumps_the_cursor_past_the_exclusion(db):
    """Spec §3.2, learned in plan 4: the cursor a page with `until` returns
    jumps past everything `until` excluded, so a following poll on the live
    cursor never re-offers the excluded message. Two messages, an `until`
    that excludes the later one, and the page comes back short of `limit`
    (the bounded stream is exhausted) — so `next` must be at or past the
    excluded message's sequence, not merely at the last returned row."""
    with db.conn() as conn:
        _msg(conn, "in-window", date_ts=1_700_000_000)
        bridge_app.arrival.sweep(conn)
        conn.execute("UPDATE ews.bridge_arrival SET first_seen = %s WHERE ews_id = %s",
                    (dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), "in-window"))
        _msg(conn, "excluded", date_ts=1_700_009_000)
        bridge_app.arrival.sweep(conn)
        excluded_seq = bridge_app.arrival.head(conn)
        conn.execute("UPDATE ews.bridge_arrival SET first_seen = %s WHERE ews_id = %s",
                    (dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc), "excluded"))
    c = _client(db)
    r = c.get("/bridge/v1/messages",
              params={"until": "2026-03-01T00:00:00+00:00", "limit": "10"},
              headers={"Authorization": "Bearer t"}).json()
    assert [m["native_id"] for m in r["messages"]] == ["in-window"]
    _, _, next_seq = r["next"].split(":")
    assert int(next_seq) >= excluded_seq


def test_a_full_page_bounded_by_until_does_not_jump_past_the_bound(db):
    """The opposite side of the same rule: when `until` did not cut the page
    short (a full page came back), there is more inside the bound still to
    fetch, so `next` stays the last returned row's seq rather than jumping
    to the live head."""
    with db.conn() as conn:
        _msg(conn, "m1", date_ts=1_700_000_000)
        _msg(conn, "m2", date_ts=1_700_000_100)
        bridge_app.arrival.sweep(conn)
        conn.execute(
            "UPDATE ews.bridge_arrival SET first_seen = %s",
            (dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),))
        m1_seq = conn.execute(
            "SELECT seq FROM ews.bridge_arrival WHERE ews_id = 'm1'").fetchone()["seq"]
    c = _client(db)
    r = c.get("/bridge/v1/messages",
              params={"until": "2026-06-01T00:00:00+00:00", "limit": "1"},
              headers={"Authorization": "Bearer t"}).json()
    assert [m["native_id"] for m in r["messages"]] == ["m1"]
    assert r["next"] == f"v1:1:{m1_seq}"


def test_a_mail_from_the_owner_reads_as_the_owners_own(db):
    """Correction 2: mapping.message now takes an owner key. Without it, a
    mail the owner sent himself would read as someone else's, and Mindet
    would never close the promise it makes."""
    with db.conn() as conn:
        _msg(conn, "from-owner", sender_email="owner@example.test")
        _msg(conn, "from-other", sender_email="other@example.test")
    c = _client(db, owner_email="owner@example.test")
    r = c.get("/bridge/v1/messages", headers={"Authorization": "Bearer t"}).json()
    by_id = {m["native_id"]: m for m in r["messages"]}
    assert by_id["from-owner"]["author"]["is_owner"] is True
    assert by_id["from-other"]["author"]["is_owner"] is False
