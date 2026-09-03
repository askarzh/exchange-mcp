"""Cache-first read contract: mirror answers with provenance, fresh=true
forces live, cache errors fall back live, writes patch the mirror.

The gateway in these tests RAISES on contact — proving that a cache-served
read never touches Exchange.
"""

import asyncio
import time
from datetime import UTC, datetime

import psycopg
from conftest import (
    _FOLDER_ROWS,
    INBOX_ID,
    SENT_ID,
    FakeGateway,
    make_context,
    make_row,
    seed_folders,
)

from ewsmcp.cache.store import CacheStore
from ewsmcp.tools.base import Context, dispatch


def seeded_store(db):
    store = CacheStore(db)
    seed_folders(store)
    now = int(time.time())
    store.upsert_messages([
        make_row("RAW-1", subject="Budget review", sender_email="a@corp.example",
                 body="please review the numbers", date_ts=now - 300,
                 conv="C1", is_read=0),
        make_row("RAW-2", subject="Re: Budget review", folder_id=SENT_ID,
                 sender_email="exec@corp.example", body="looks good",
                 date_ts=now - 200, conv="C1"),
        make_row("RAW-3", subject="Lunch", sender_email="b@corp.example",
                 body="see you at noon", date_ts=now - 100, conv="C2"),
    ])
    store.set_sync_state(f"item:{INBOX_ID}", "TOK", now)
    store.set_sync_state(f"item:{SENT_ID}", "TOK", now)
    store.set_sync_state("events", None, now)
    store.replace_events([{
        "ews_id": "EV1", "changekey": None, "subject": "Standup",
        "start_ts": now - 60, "start_iso": "2026-07-10T09:00+03:00",
        "end_ts": now + 3600, "end_iso": "2026-07-10T10:00+03:00",
        "location": None, "organizer": None, "is_recurring": 0,
        "my_response": None,
    }])
    # seed_folders already wrote the full hierarchy (inbox/sent/junk/archive);
    # layer in the counts test_list_folders_from_mirror and
    # test_overview_pure_mirror assert on WITHOUT wiping junk/archive — the
    # excluded-folder and unknown-folder tests need them still present.
    rows = [dict(r) for r in _FOLDER_ROWS]
    for r in rows:
        if r["ews_id"] == INBOX_ID:
            r["total"], r["unread"] = 3, 1
    store.replace_folders(rows)
    store.set_sync_state("folders", None, now)  # what _sync_hierarchy stamps
    return store


def _ctx(tmp_path, db, gateway, **overrides) -> Context:
    ctx = make_context(db, gateway=gateway, audit_dir=str(tmp_path / "audit"),
                       **overrides)
    ctx.cache = seeded_store(db)
    return ctx


def _run(ctx, name, **kwargs):
    return asyncio.run(dispatch(ctx, ctx.registry[name], dict(kwargs)))


def test_search_served_from_mirror_with_provenance(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "search_messages", query="budget", folder="f:inbox")
    assert res["source"] == "cache" and res["as_of"]
    assert res["count"] == 1
    assert res["total_available"] == 1  # exact — COUNT(*) is free locally
    assert res["items"][0]["subject"] == "Budget review"
    assert res["items"][0]["id"].startswith("m")  # alias, never a raw id


def test_search_sender_filter_from_mirror(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "search_messages", sender="a@corp", limit=1)
    assert res["source"] == "cache"
    assert res["count"] == 1
    assert res["items"][0]["unread"] is True


def test_get_message_from_mirror(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "get_message", id="RAW-1")
    assert res["source"] == "cache"
    assert res["message"]["body"].startswith("please review")
    assert res["message"]["subject"] == "Budget review"


def test_get_thread_local_conversation_join(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "get_thread", id="RAW-1")
    assert res["source"] == "cache"
    assert res["count"] == 2  # inbox + sent halves of C1
    assert [e["from"] for e in res["items"]] == ["a@corp.example",
                                                 "exec@corp.example"]


def test_overview_pure_mirror(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "get_mailbox_overview")
    assert res["source"] == "cache" and res["as_of"]
    assert res["unread_total"] == 1
    assert res["recent_unread"][0]["subject"] == "Budget review"
    assert res["today_events"][0]["subject"] == "Standup"


def test_list_folders_from_mirror(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "list_folders")
    assert res["source"] == "cache"
    inbox = next(r for r in res["items"] if r.get("wk") == "f:inbox")
    assert inbox["unread"] == 1


def test_list_folders_provenance_is_the_hierarchy_lane_not_the_slow_lane(tmp_path, db):
    """as_of must come from the `folders` watermark. The slow (calendar/tasks)
    lane's `events` key says nothing about when the folder tree was read."""
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    now = int(time.time())
    ctx.cache.set_sync_state("folders", None, now - 900)
    ctx.cache.set_sync_state("events", None, now)
    res = _run(ctx, "list_folders")
    assert res["as_of"].startswith(
        datetime.fromtimestamp(now - 900, tz=UTC).isoformat(timespec="seconds")[:16])

    ctx.cache.drop_sync_state("folders")
    assert "as_of" not in _run(ctx, "list_folders")  # events must not stand in


def test_get_thread_with_a_dead_mirror_is_backend_unavailable(tmp_path, db):
    """A down Postgres is not a missing thread: get_thread must not report
    not_found when it simply could not look."""
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    assert _run(ctx, "get_thread", id="RAW-1")["ok"] is True
    db.close()
    res = _run(ctx, "get_thread", id="RAW-1")
    assert res["ok"] is False and res["error"]["code"] == "backend_unavailable"


def test_fresh_true_forces_live(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "get_message", id="RAW-1", fresh=True)
    # FakeGateway(raise_on_call=True) raises AssertionError → mapped upstream
    # error — which is exactly the proof that fresh=true went to Exchange.
    assert res["ok"] is False


def test_excluded_folder_is_a_validation_error(tmp_path, db):
    """f:junk IS in ews.folders (seed_folders seeds it) but is excluded from
    the mirror by the default EWS_MIRROR_EXCLUDE — naming it is a validation
    error, not a live fallback."""
    gateway = FakeGateway(raise_on_call=True)
    ctx = _ctx(tmp_path, db, gateway)
    res = _run(ctx, "search_messages", folder="f:junk")
    assert res["ok"] is False
    assert res["error"]["code"] == "validation"
    assert "EWS_MIRROR_EXCLUDE" in res["error"]["message"]
    assert gateway.calls == 0


def test_unknown_folder_is_not_found(tmp_path, db):
    gateway = FakeGateway(raise_on_call=True)
    ctx = _ctx(tmp_path, db, gateway)
    res = _run(ctx, "search_messages", folder="Nope/Missing")
    assert res["ok"] is False
    assert res["error"]["code"] == "not_found"
    assert gateway.calls == 0


def test_search_has_no_fresh_parameter(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    props = ctx.registry["search_messages"].input_schema["properties"]
    assert "fresh" not in props
    assert "fresh" not in ctx.registry["get_thread"].input_schema["properties"]
    # ...and they still stamp source=cache
    assert _run(ctx, "search_messages", query="budget")["source"] == "cache"


def test_search_never_touches_exchange_even_with_no_folder(tmp_path, db):
    """folder omitted = every mirrored folder, still zero EWS calls — and the
    hits really do come from two different mirrored folders (inbox + sent)."""
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "search_messages", query="budget")
    assert res["ok"] is True and res["count"] == 2  # inbox AND sent halves of C1
    subjects = {it["subject"] for it in res["items"]}
    assert subjects == {"Budget review", "Re: Budget review"}
    res = _run(ctx, "search_messages")           # no filters at all
    assert res["total_available"] == 3            # inbox + sent seeds


def test_query_combines_with_structured_filters(tmp_path, db):
    """The AQS-exclusivity rule is gone."""
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "search_messages", query="budget", is_unread=True)
    assert res["ok"] is True and res["count"] == 1
    assert res["items"][0]["subject"] == "Budget review"


def test_store_failure_is_backend_unavailable_not_a_live_read(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))

    def boom(**kwargs):
        raise psycopg.OperationalError("mirror unavailable")

    ctx.cache.search_messages = boom
    res = _run(ctx, "search_messages")
    assert res["ok"] is False and res["error"]["code"] == "backend_unavailable"


def test_get_message_cache_error_falls_back_to_live(tmp_path, db):
    """get_message still falls back to live on a mirror error — its
    cache_reads helper keeps its own try/except (unlike search_messages,
    which is store-only with no live path to fall back to)."""
    from types import SimpleNamespace

    class Item:
        id = "RAW-1"
        subject = "Live subject"
        sender = SimpleNamespace(email_address="a@corp.example", name="A")
        datetime_received = None
        is_read = True
        has_attachments = False
        conversation_id = None
        to_recipients = []
        text_body = "live body"
        message_id = None

    account = SimpleNamespace(fetch=lambda ids, only_fields=None: [Item()])
    gateway = FakeGateway(account)
    ctx = _ctx(tmp_path, db, gateway)

    def boom(raw_id):
        raise RuntimeError("mirror unavailable")

    ctx.cache.get_message = boom
    res = _run(ctx, "get_message", id="RAW-1", format="concise")
    assert res["ok"] is True
    assert res["source"] == "live"
    assert gateway.calls == 1


def test_write_through_update_and_delete(tmp_path, db):
    from types import SimpleNamespace

    class Item:
        def __init__(self, raw_id):
            self.id = raw_id
            self.is_read = False
            self.categories = None

        def save(self, **kwargs):
            return None

        def move_to_trash(self):
            return None

    items = {"RAW-1": Item("RAW-1")}
    account = SimpleNamespace()
    account.fetch = lambda pairs, only_fields=None: [items[i] for i, _ in pairs]
    gateway = FakeGateway(account)
    ctx = _ctx(tmp_path, db, gateway, ews_capability_tier="full")

    res = _run(ctx, "update_messages", ids=["RAW-1"], set_read=True)
    assert res["updated"] == 1
    assert ctx.cache.get_message("RAW-1")["is_read"] == 1  # mirror patched

    res = _run(ctx, "delete_messages", ids=["RAW-1"])
    assert res["deleted"] == 1
    assert ctx.cache.get_message("RAW-1") is None  # tombstoned
