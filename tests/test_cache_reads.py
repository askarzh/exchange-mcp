"""cache_reads: None when the mirror cannot answer, stamped dict when it can."""

import asyncio
import time

from conftest import make_context

from ewsmcp.tools import cache_reads
from test_pg_store import make_row


def _seed(ctx):
    now = int(time.time())
    ctx.cache.upsert_messages([make_row("RAW-1", subject="Budget", date_ts=now - 10)])
    ctx.cache.set_sync_state("item:inbox", "TOK", now)


def test_folder_key_uses_watermarks_not_settings(db):
    ctx = make_context(db)
    assert cache_reads.folder_key(ctx, "f:inbox") is None  # nothing synced yet
    _seed(ctx)
    assert cache_reads.folder_key(ctx, "f:inbox") == "inbox"
    assert cache_reads.folder_key(ctx, "inbox") == "inbox"
    assert cache_reads.folder_key(ctx, "f:sent") is None
    assert cache_reads.folder_key(ctx, None) == "inbox"


def test_search_returns_none_for_unmirrored_folder(db):
    ctx = make_context(db)
    _seed(ctx)
    out = asyncio.run(cache_reads.search_messages(
        ctx, folder="f:junk", query=None, sender=None, subject=None, since=None,
        until=None, is_unread=None, has_attachments=None, offset=0, limit=10))
    assert out is None


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
