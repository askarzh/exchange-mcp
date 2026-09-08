"""CacheStore on Postgres: schema, tsvector search, filters, write-through
patches, stats, and the accent/prefix folded search behaviour."""

import json
import time

import pytest
from conftest import INBOX_ID, SENT_ID, make_row, seed_folders

from ewsmcp.cache.store import CacheStore


@pytest.fixture
def store(db):
    s = CacheStore(db)
    seed_folders(s)
    return s


@pytest.fixture
def seeded_folders(store):
    from conftest import seed_folders
    seed_folders(store)
    return store


def test_upsert_and_fts_search(store):
    store.upsert_messages([
        make_row("M1", subject="Budget review", body="numbers attached"),
        make_row("M2", subject="Lunch", body="see you at noon"),
    ])
    rows, total = store.search_messages(text="budget")
    assert total == 1 and rows[0]["ews_id"] == "M1"
    rows, total = store.search_messages(text="noon")
    assert total == 1 and rows[0]["ews_id"] == "M2"
    # upsert replaces (same PK) and the tsvector shadow follows the generated column
    updated = make_row("M2", subject="Lunch moved", body="now at one")
    store.upsert_messages([updated])
    rows, total = store.search_messages(text="noon")
    assert total == 0
    rows, total = store.search_messages(text="moved")
    assert total == 1


def test_structured_filters_and_exact_total(store):
    now = int(time.time())
    store.upsert_messages([
        make_row("M1", sender_email="a@x.example", is_read=0, date_ts=now - 100),
        make_row("M2", sender_email="b@x.example", is_read=1, date_ts=now - 50,
                 has_attachments=1),
        make_row("M3", sender_email="a@x.example", is_read=1, date_ts=now,
                 folder_id=SENT_ID),
    ])
    rows, total = store.search_messages(folder_ids=[INBOX_ID])
    assert total == 2
    rows, total = store.search_messages(sender="a@x")
    assert total == 2
    rows, total = store.search_messages(is_unread=True)
    assert total == 1 and rows[0]["ews_id"] == "M1"
    rows, total = store.search_messages(has_attachments=True)
    assert total == 1 and rows[0]["ews_id"] == "M2"
    rows, total = store.search_messages(since_ts=now - 60)
    assert total == 2
    rows, total = store.search_messages(subject="Budget")
    assert total == 3
    # newest first + offset/limit paging
    rows, total = store.search_messages(offset=1, limit=1)
    assert total == 3 and rows[0]["ews_id"] == "M2"


def test_thread_join_and_get_message(store):
    store.upsert_messages([
        make_row("M1", conv="C9", date_ts=100),
        make_row("M2", conv="C9", date_ts=200, folder_id=SENT_ID),
        make_row("M3", conv="OTHER", date_ts=300),
    ])
    rows = store.thread("C9")
    assert [r["ews_id"] for r in rows] == ["M1", "M2"]  # chronological
    assert store.get_message("M3")["conversation_id"] == "OTHER"
    # secondary lookup by internet_message_id
    assert store.get_message("<M1@corp.example>")["ews_id"] == "M1"
    assert store.get_message("GONE") is None


def test_write_through_patches(store):
    store.upsert_messages([make_row("M1", is_read=0)])
    store.set_read_flag(["M1"], True)
    assert store.get_message("M1")["is_read"] == 1
    store.apply_categories("M1", ["Follow up"])
    assert json.loads(store.get_message("M1")["categories_json"]) == ["Follow up"]
    store.tombstone_messages(["M1"])
    assert store.get_message("M1") is None
    _rows, total = store.search_messages(text="budget")
    assert total == 0  # row (and its tsvector shadow) is gone


def test_unread_page_and_watermarks(store):
    store.upsert_messages([
        make_row("M1", is_read=0, date_ts=100),
        make_row("M2", is_read=0, date_ts=200),
        make_row("M3", is_read=1, date_ts=300),
    ])
    total, rows = store.unread_page(limit=1)
    assert total == 2
    assert rows[0]["ews_id"] == "M2"  # newest unread first
    store.set_sync_state(f"item:{INBOX_ID}", "TOKEN-1", 1234.0)
    assert store.get_sync_state(f"item:{INBOX_ID}") == "TOKEN-1"
    assert store.watermark(f"item:{INBOX_ID}") == 1234
    assert f"item:{INBOX_ID}" in store.watermarks()


def test_stats(store):
    store.upsert_messages([make_row("M1")])
    store.set_sync_state(f"item:{INBOX_ID}", "T", time.time())
    stats = store.stats()
    assert stats["rows"]["messages"] == 1
    assert stats["db_mb"] >= 0
    assert stats["watermarks"][f"item:{INBOX_ID}"] > 0


def test_contact_stats_and_senders(store):
    now = int(time.time())
    store.upsert_messages([
        make_row("M1", sender_email="boss@corp.example", sender_name="Boss",
                 date_ts=now - 500),
        make_row("M2", sender_email="boss@corp.example", sender_name="Boss",
                 date_ts=now - 100),
        make_row("M3", folder_id=SENT_ID, sender_email="exec@corp.example",
                 to=["boss@corp.example"], date_ts=now - 50),
    ])
    stats = store.contact_stats("boss@corp.example")
    assert stats["received_count"] == 2
    assert stats["sent_count"] == 1
    rows = store.senders_matching("boss")
    assert rows[0]["sender_email"] == "boss@corp.example"
    assert rows[0]["msgs"] == 2


def test_sent_without_reply(store):
    now = int(time.time())
    old = now - 6 * 86400
    store.upsert_messages([
        # thread A: we sent last, no reply for 6 days → waiting_on
        make_row("A1", conv="CA", folder_id=SENT_ID, date_ts=old,
                 subject="Waiting thread"),
        # thread B: we sent, then they replied → NOT waiting
        make_row("B1", conv="CB", folder_id=SENT_ID, date_ts=old),
        make_row("B2", conv="CB", folder_id=INBOX_ID, date_ts=old + 3600),
        # thread C: we sent recently (inside the window) → NOT waiting yet
        make_row("C1", conv="CC", folder_id=SENT_ID, date_ts=now - 3600),
    ])
    rows = store.sent_without_reply(days=5)
    assert [r["ews_id"] for r in rows] == ["A1"]


def test_task_rows(store):
    store.upsert_tasks([
        {"ews_id": "T1", "changekey": None, "subject": "File report",
         "due_ts": 100, "due_iso": "2026-07-01", "is_complete": 0,
         "status": "NotStarted"},
        {"ews_id": "T2", "changekey": None, "subject": "Done thing",
         "due_ts": 50, "due_iso": "2026-06-01", "is_complete": 1,
         "status": "Completed"},
    ])
    rows, total = store.task_rows()
    assert total == 1 and rows[0]["ews_id"] == "T1"
    rows, total = store.task_rows(include_completed=True)
    assert total == 2
    store.delete_tasks_by_id(["T1"])
    rows, total = store.task_rows(include_completed=True)
    assert total == 1


def test_schema_is_v2_with_folder_id_and_no_dead_columns(db):
    with db.conn() as c:
        cols = {r["column_name"] for r in c.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'ews' AND table_name = 'messages'")}
        tables = {r["table_name"] for r in c.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'ews'")}
    assert "folder_id" in cols and "folder" not in cols
    assert "norm_text" not in cols
    assert "search_tsv" in cols
    assert "sender_sigs" not in tables
    # the version-exact check lives in test_archive_schema.py, which tracks
    # the newest migration; this test only cares that migration 002's shape
    # survives later migrations.


def test_search_folds_accents_in_the_database(store):
    """No norm_text shadow: the generated column and the query both go
    through ews.immutable_unaccent."""
    store.upsert_messages([
        make_row("M1", subject="Résumé review", body="café numbers"),
        make_row("M2", subject="Отчёт за квартал", body="цифры во вложении"),
    ])
    rows, total = store.search_messages(text="resume")
    assert total == 1 and rows[0]["ews_id"] == "M1"
    rows, total = store.search_messages(text="отч")     # Cyrillic prefix
    assert total == 1 and rows[0]["ews_id"] == "M2"
    rows, total = store.search_messages(text="cafe numb")  # AND of prefixes
    assert total == 1


def test_search_does_not_raise_when_unaccent_emits_tsquery_metacharacters(store):
    """ews.immutable_unaccent can turn a single input character into tsquery
    metacharacters (a modifier apostrophe -> "'", a circled digit -> "(1)",
    a modifier colon -> ":") — the query must re-sanitise AFTER folding, in
    SQL, or to_tsquery raises a syntax error on these inputs."""
    store.upsert_messages([make_row("M1", subject="Budget approved")])
    for text in ("ʼhello", "aːb", "⑴budget"):
        store.search_messages(text=text)  # must not raise
    # the circled-digit token still folds down to a working "budget" prefix
    rows, total = store.search_messages(text="⑴budget")
    assert total == 1 and rows[0]["ews_id"] == "M1"


def test_search_folds_and_prefixes_together(store):
    store.upsert_messages([make_row("M1", subject="Café budget review")])
    rows, total = store.search_messages(text="Café bud")
    assert total == 1 and rows[0]["ews_id"] == "M1"


def test_search_folds_cyrillic_yo_to_ye(store):
    store.upsert_messages([make_row("M1", subject="Ёлка на праздник")])
    rows, total = store.search_messages(text="ёлка")
    assert total == 1 and rows[0]["ews_id"] == "M1"
    rows, total = store.search_messages(text="елка")  # е/ё fold both ways
    assert total == 1 and rows[0]["ews_id"] == "M1"


def test_search_ands_across_tokens_not_just_ors_within_one(store):
    """Each per-token OR group (the lexemes one input token folds/splits
    into) must be parenthesised: tsquery binds "&" tighter than "|", so an
    unparenthesised "'1':* | 'budget':* & 'review':*" parses as
    "1 | (budget & review)" and would wrongly match a document that only
    has "1"."""
    store.upsert_messages([
        make_row("ONE", subject="1 apples", body=""),
        make_row("BOTH", subject="budget review", body=""),
    ])
    # "⑴budget" folds/re-lexes to the group ('1':* | 'budget':*); ANDed with
    # 'review':* it must require BOTH "review" and (1 or budget) — ONE (just
    # "1") must NOT match.
    rows, total = store.search_messages(text="⑴budget review")
    assert total == 1 and rows[0]["ews_id"] == "BOTH"
    # plain multi-token search is unaffected by the parenthesisation.
    rows, total = store.search_messages(text="budget review")
    assert total == 1 and rows[0]["ews_id"] == "BOTH"


def test_search_underscore_token_group_stays_anded_with_the_next_token(store):
    """An ordinary token with an underscore (e.g. "report_v2") splits into
    more than one lexeme too ('report', 'v2') — its OR group must not leak
    into the AND with the following token."""
    store.upsert_messages([
        make_row("REPORT_ONLY", subject="report status update", body=""),
        make_row("BOTH", subject="report v2 budget numbers", body=""),
    ])
    rows, total = store.search_messages(text="report_v2 budget")
    assert total == 1 and rows[0]["ews_id"] == "BOTH"


def test_text_query_combines_with_structured_filters(store, seeded_folders):
    now = int(time.time())
    store.upsert_messages([
        make_row("M1", subject="Budget review", is_read=0, date_ts=now - 100),
        make_row("M2", subject="Budget review", is_read=1, date_ts=now - 50),
        make_row("M3", subject="Budget review", folder_id=SENT_ID, is_read=1,
                 date_ts=now),
    ])
    # the AQS-exclusivity rule is gone: text AND filters, together
    rows, total = store.search_messages(text="budget", is_unread=True)
    assert total == 1 and rows[0]["ews_id"] == "M1"
    rows, total = store.search_messages(text="budget", folder_ids=[SENT_ID])
    assert total == 1 and rows[0]["ews_id"] == "M3"
    rows, total = store.search_messages(text="budget")
    assert total == 3  # folder_ids=None means every mirrored folder


def test_inbox_and_sent_resolve_through_folders_wk(store, seeded_folders):
    now = int(time.time())
    store.upsert_messages([
        make_row("M1", is_read=0, date_ts=now),
        make_row("M2", folder_id=SENT_ID, sender_email="exec@corp.example",
                 to=["boss@corp.example"], date_ts=now),
    ])
    total, rows = store.unread_page()
    assert total == 1 and rows[0]["ews_id"] == "M1"
    assert store.folder_id_for_wk("f:sent") == SENT_ID
    assert store.folder_id_for_wk("f:nonexistent") is None
    stats = store.contact_stats("boss@corp.example")
    assert stats["sent_count"] == 1


def test_folder_disappearance_helpers(store, seeded_folders):
    store.upsert_messages([make_row("M1"), make_row("M2", folder_id=SENT_ID)])
    store.set_sync_state(f"item:{INBOX_ID}", "TOK", time.time())
    assert store.delete_live_messages_in_folder(INBOX_ID) == 1
    assert store.get_message("M1") is None
    assert store.get_message("M2") is not None
    store.drop_sync_state(f"item:{INBOX_ID}")
    assert store.get_sync_state(f"item:{INBOX_ID}") is None


def test_text_search_ranks_by_relevance_then_date(store):
    store.upsert_messages([
        make_row("M1", subject="budget", body="budget budget budget", date_ts=100),
        make_row("M2", subject="budget", body="unrelated", date_ts=200),
    ])
    rows, _ = store.search_messages(text="budget")
    assert [r["ews_id"] for r in rows] == ["M1", "M2"]


def test_archived_filter(store):
    store.upsert_messages([make_row("M1"), make_row("M2")])
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET archive_state='verified' WHERE ews_id='M2'")
    assert store.search_messages(archived="only")[1] == 1
    assert store.search_messages(archived="exclude")[1] == 1
    assert store.search_messages(archived="any")[1] == 2
    assert store.get_message("M2")["archive_state"] == "verified"


def test_update_bodies_extra_sets_item_class_and_inventory(store):
    store.upsert_messages([make_row("A1")])
    store.update_bodies({"A1": "body"}, None,
                        {"A1": {"item_class": "IPM.Schedule.Meeting.Resp.Pos",
                                "attachments_json": "[]"}})
    row = store.get_message("A1")
    assert row["item_class"] == "IPM.Schedule.Meeting.Resp.Pos"
    assert row["attachments_json"] == "[]"


def test_upsert_never_touches_archive_columns(store):
    store.upsert_messages([make_row("M1")])
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET archive_state='captured', "
                  "mime_sha256='abc' WHERE ews_id='M1'")
    store.upsert_messages([make_row("M1", subject="edited")])
    row = store.get_message("M1")
    assert row["subject"] == "edited"
    assert row["archive_state"] == "captured" and row["mime_sha256"] == "abc"
