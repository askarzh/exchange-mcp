import datetime as dt
import json

import pytest
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
    again = c.get("/bridge/v1/messages", params={"since": first["next"]},
                  headers={"Authorization": "Bearer t"}).json()
    assert again["messages"] == [] and again["next"] == first["next"]


def test_a_terminal_page_echoes_even_when_the_head_has_moved_past_it(db):
    """Fix round 1, item 4: with one message this test cannot fail, because a
    terminal page's `after` always equals `head(c)` — a mutant that always
    returns `head(c)` instead of echoing `after` would still pass. Force
    `after != head(c)` on a genuinely terminal page: seed two messages, sweep
    (so both get a seq and `head(c)` sits on the second), then soft-delete the
    second. A page asked from a cursor at the first message's seq now sees no
    live rows past it — terminal — while `head(c)` still reports the deleted
    message's seq. Only echoing `after` gets this right; jumping to
    `head(c)` would silently skip nothing (there is nothing left to skip),
    but it would still be answering the wrong rule."""
    with db.conn() as conn:
        _msg(conn, "m1")
        _msg(conn, "m2")
        bridge_app.arrival.sweep(conn)
        m1_seq = conn.execute(
            "SELECT seq FROM ews.bridge_arrival WHERE ews_id = 'm1'").fetchone()["seq"]
        head_seq = bridge_app.arrival.head(conn)
        conn.execute("UPDATE ews.messages SET deleted_at = now() WHERE ews_id = 'm2'")
    assert m1_seq != head_seq
    c = _client(db)
    cursor = f"v1:1:{m1_seq}"
    r = c.get("/bridge/v1/messages", params={"since": cursor},
              headers={"Authorization": "Bearer t"}).json()
    assert r["messages"] == []
    assert r["next"] == cursor
    assert r["next"] != f"v1:1:{head_seq}"


def test_a_cursor_from_another_generation_is_refused(db):
    """Rebuilding the ledger renumbers everything. Accepting an old cursor would
    silently skip whatever now sits below that number; 400 makes Mindet
    bootstrap instead of going quietly blind."""
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"since": "v1:99:0"},
              headers={"Authorization": "Bearer t"})
    assert r.status_code == 400


def test_a_malformed_cursor_is_refused_rather_than_treated_as_the_beginning(db):
    c = _client(db)
    for bad in ("nonsense", "v1:1", "v2:1:0", "v1:x:1"):
        assert c.get("/bridge/v1/messages", params={"since": bad},
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


def test_an_empty_token_refuses_to_build_the_app(db):
    """Fix round 1, item 1: `hmac.compare_digest("", "")` is True, so an
    empty configured token would make a request with no Authorization header
    at all pass the guard and serve the mailbox. Refuse loudly at
    construction rather than quietly at request time."""
    with pytest.raises(ValueError, match="EWS_BRIDGE_TOKEN"):
        bridge_app.build_app(db, token="")


def test_a_none_token_also_refuses_to_build_the_app(db):
    with pytest.raises(ValueError, match="EWS_BRIDGE_TOKEN"):
        bridge_app.build_app(db, token=None)


def test_limit_abc_is_a_400_not_a_500(db):
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"limit": "abc"},
              headers={"Authorization": "Bearer t"})
    assert r.status_code == 400


def test_limit_negative_is_a_400(db):
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"limit": "-5"},
              headers={"Authorization": "Bearer t"})
    assert r.status_code == 400


def test_limit_zero_is_a_400(db):
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"limit": "0"},
              headers={"Authorization": "Bearer t"})
    assert r.status_code == 400


def test_limit_above_the_page_limit_is_clamped_not_refused(db):
    c = _client(db)
    r = c.get("/bridge/v1/messages", params={"limit": "99999"},
              headers={"Authorization": "Bearer t"})
    assert r.status_code == 200


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


def test_chats_membership_is_the_union_across_the_thread_not_the_latest_message(db):
    """A naive `max(to_json)` in SQL sorts byte-wise, not by recency or
    membership. Here the early message's single-recipient list
    ("zzz@example.test") sorts higher than the later reply-all's two-address
    list ("aaa@...", "bbb@...") purely because 'z' > 'a' — so `max()` would
    pick the single-recipient list and report this thread as `direct` with
    `member_count == 2`, even though it was genuinely a four-person
    conversation. The union must count all four and call it a group.
    Also covers the pre-existing rule that one conversation appears once."""
    with db.conn() as conn:
        _msg(conn, "early", date_ts=1_700_000_000, sender_email="boss@example.test")
        conn.execute("UPDATE ews.messages SET conversation_id='shared',"
                     " to_json=%s WHERE ews_id='early'",
                     (json.dumps(["zzz@example.test"]),))
        _msg(conn, "late", date_ts=1_700_009_000, sender_email="boss@example.test")
        conn.execute("UPDATE ews.messages SET conversation_id='shared',"
                     " to_json=%s WHERE ews_id='late'",
                     (json.dumps(["aaa@example.test", "bbb@example.test"]),))
    r = _client(db).get("/bridge/v1/chats",
                        headers={"Authorization": "Bearer t"}).json()
    matches = [c for c in r["chats"] if c["native_id"] == "shared"]
    assert len(matches) == 1
    chat = matches[0]
    assert chat["member_count"] == 4
    assert chat["kind"] == "group"


def test_a_single_recipient_conversation_is_direct_with_two_members(db):
    with db.conn() as conn:
        _msg(conn, "m1", sender_email="a@example.test")
        conn.execute("UPDATE ews.messages SET to_json=%s WHERE ews_id='m1'",
                     (json.dumps(["single@example.test"]),))
    r = _client(db).get("/bridge/v1/chats",
                        headers={"Authorization": "Bearer t"}).json()
    matches = [c for c in r["chats"] if c["native_id"] == "conv-m1"]
    assert len(matches) == 1
    assert matches[0]["kind"] == "direct"
    assert matches[0]["member_count"] == 2


def test_a_malformed_to_json_does_not_fail_the_whole_page(db):
    """One malformed recipient header must not take down the endpoint, and
    its conversation still has to show up — with whatever membership the
    tolerant parser could recover (here, just the sender)."""
    with db.conn() as conn:
        _msg(conn, "bad", sender_email="a@example.test")
        conn.execute("UPDATE ews.messages SET to_json='not json' WHERE ews_id='bad'")
    r = _client(db).get("/bridge/v1/chats",
                        headers={"Authorization": "Bearer t"})
    assert r.status_code == 200
    ids = [c["native_id"] for c in r.json()["chats"]]
    assert "conv-bad" in ids


def test_a_same_second_tie_on_name_breaks_by_ews_id_and_stays_stable(db):
    """Two messages in one conversation sharing a `date_ts` (rapid replies
    are ordinary) with no tiebreaker would let Postgres pick either subject
    for `name`, and it could differ between polls. `ews_id DESC` makes the
    choice deterministic — always the higher native id — and repeated calls
    must agree."""
    with db.conn() as conn:
        _msg(conn, "aaa", date_ts=1_700_000_000)
        conn.execute("UPDATE ews.messages SET conversation_id='tie',"
                     " subject='from aaa' WHERE ews_id='aaa'")
        _msg(conn, "zzz", date_ts=1_700_000_000)
        conn.execute("UPDATE ews.messages SET conversation_id='tie',"
                     " subject='from zzz' WHERE ews_id='zzz'")
    c = _client(db)
    names = set()
    for _ in range(3):
        r = c.get("/bridge/v1/chats", headers={"Authorization": "Bearer t"}).json()
        names.add(next(x["name"] for x in r["chats"] if x["native_id"] == "tie"))
    assert names == {"from zzz"}


def test_contacts_are_addresses_seen_as_senders_with_their_names(db):
    with db.conn() as conn:
        _msg(conn, "m1")
        conn.execute("UPDATE ews.messages SET sender_email='Boss@Example.TEST',"
                     " sender_name='the boss' WHERE ews_id='m1'")
    r = _client(db).get("/bridge/v1/contacts",
                        headers={"Authorization": "Bearer t"}).json()
    one = [c for c in r["contacts"] if c["key"] == "email:boss@example.test"]
    assert len(one) == 1 and one[0]["name"] == "the boss"


def test_a_sender_with_no_address_is_not_offered_as_a_contact(db):
    """A contact with no key cannot be matched to anyone, and a roster
    candidate nobody can identify is noise the owner has to dismiss."""
    with db.conn() as conn:
        _msg(conn, "m1")
        conn.execute("UPDATE ews.messages SET sender_email=NULL WHERE ews_id='m1'")
    r = _client(db).get("/bridge/v1/contacts",
                        headers={"Authorization": "Bearer t"}).json()
    assert r["contacts"] == []
