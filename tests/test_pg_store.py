"""CacheStore on Postgres: schema, tsvector search, filters, write-through
patches, stats, and the accent/prefix folded search behaviour."""

import json
import time

import pytest
from conftest import make_row

from ewsmcp.cache.store import CacheStore


@pytest.fixture
def store(db):
    return CacheStore(db)


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
                 folder="sent"),
    ])
    rows, total = store.search_messages(folders=["inbox"])
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
        make_row("M2", conv="C9", date_ts=200, folder="sent"),
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
    store.set_sync_state("item:inbox", "TOKEN-1", 1234.0)
    assert store.get_sync_state("item:inbox") == "TOKEN-1"
    assert store.watermark("item:inbox") == 1234
    assert "item:inbox" in store.watermarks()


def test_stats(store):
    store.upsert_messages([make_row("M1")])
    store.set_sync_state("item:inbox", "T", time.time())
    stats = store.stats()
    assert stats["rows"]["messages"] == 1
    assert stats["db_mb"] >= 0
    assert stats["watermarks"]["item:inbox"] > 0


def test_contact_stats_and_senders(store):
    now = int(time.time())
    store.upsert_messages([
        make_row("M1", sender_email="boss@corp.example", sender_name="Boss",
                 date_ts=now - 500),
        make_row("M2", sender_email="boss@corp.example", sender_name="Boss",
                 date_ts=now - 100),
        make_row("M3", folder="sent", sender_email="exec@corp.example",
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
        make_row("A1", conv="CA", folder="sent", date_ts=old,
                 subject="Waiting thread"),
        # thread B: we sent, then they replied → NOT waiting
        make_row("B1", conv="CB", folder="sent", date_ts=old),
        make_row("B2", conv="CB", folder="inbox", date_ts=old + 3600),
        # thread C: we sent recently (inside the window) → NOT waiting yet
        make_row("C1", conv="CC", folder="sent", date_ts=now - 3600),
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


def test_prefix_and_accent_folded_search(store):
    store.upsert_messages([
        make_row("M1", subject="Résumé review", body="café numbers"),
        make_row("M2", subject="Отчёт за квартал", body="цифры во вложении"),
    ])
    rows, total = store.search_messages(text="resume")
    assert total == 1 and rows[0]["ews_id"] == "M1"
    rows, total = store.search_messages(text="отч")  # prefix, Cyrillic
    assert total == 1 and rows[0]["ews_id"] == "M2"
    rows, total = store.search_messages(text="cafe numb")  # AND of prefixes
    assert total == 1


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


def test_upsert_never_touches_archive_columns(store):
    store.upsert_messages([make_row("M1")])
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET archive_state='captured', "
                  "mime_sha256='abc' WHERE ews_id='M1'")
    store.upsert_messages([make_row("M1", subject="edited")])
    row = store.get_message("M1")
    assert row["subject"] == "edited"
    assert row["archive_state"] == "captured" and row["mime_sha256"] == "abc"
