"""cache_reads: None when the mirror cannot answer, stamped dict when it can."""

import asyncio
import time

from conftest import INBOX_ID, make_context, make_row, seed_folders

from ewsmcp.tools import cache_reads


def _seed(ctx):
    seed_folders(ctx.cache)
    now = int(time.time())
    ctx.cache.upsert_messages([make_row("RAW-1", subject="Budget", date_ts=now - 10)])
    ctx.cache.set_sync_state(f"item:{INBOX_ID}", "TOK", now)


def test_search_of_an_unsynced_folder_is_empty_not_an_error(db):
    ctx = make_context(db)
    _seed(ctx)
    out = asyncio.run(cache_reads.search_messages(
        ctx, folder="f:sent", query=None, sender=None, subject=None, since=None,
        until=None, is_unread=None, has_attachments=None, offset=0, limit=10))
    assert out["ok"] is True and out["count"] == 0


def test_resolve_folder_id_uses_the_folders_table(db):
    ctx = make_context(db)
    seed_folders(ctx.cache)
    assert cache_reads.resolve_folder_id(ctx, "f:inbox") == INBOX_ID
    assert cache_reads.resolve_folder_id(ctx, "inbox") == INBOX_ID
    assert cache_reads.resolve_folder_id(ctx, INBOX_ID) == INBOX_ID
    assert cache_reads.resolve_folder_id(ctx, "Inbox") == INBOX_ID  # path


def test_search_hit_is_stamped(db):
    ctx = make_context(db)
    _seed(ctx)
    out = asyncio.run(cache_reads.search_messages(
        ctx, folder="f:inbox", query="budget", sender=None, subject=None, since=None,
        until=None, is_unread=None, has_attachments=None, offset=0, limit=10))
    assert out["source"] == "cache" and out["as_of"] and out["count"] == 1


def test_get_message_none_when_missing(db):
    ctx = make_context(db)
    assert asyncio.run(cache_reads.get_message(ctx, "NOPE", "full")) is None
