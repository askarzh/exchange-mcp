"""Cache-first read contract: mirror answers with provenance, fresh=true
forces live, cache errors fall back live, writes patch the mirror.

The gateway in these tests RAISES on contact — proving that a cache-served
read never touches Exchange.
"""

import asyncio
import time

from conftest import INBOX_ID, SENT_ID, FakeGateway, make_context, make_row, seed_folders

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
    # seed_folders already wrote the inbox row; layer in the counts
    # test_list_folders_from_mirror and test_overview_pure_mirror assert on.
    store.replace_folders([
        {"ews_id": INBOX_ID, "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 3, "unread": 1, "children": 0},
    ])
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
    res = _run(ctx, "search_messages", query="budget")
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
    inbox = next(r for r in res["items"] if r["wk"] == "f:inbox")
    assert inbox["unread"] == 1


def test_fresh_true_forces_live(tmp_path, db):
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "get_message", id="RAW-1", fresh=True)
    # FakeGateway(raise_on_call=True) raises AssertionError → mapped upstream
    # error — which is exactly the proof that fresh=true went to Exchange.
    assert res["ok"] is False


def test_unmirrored_folder_is_a_validation_error(tmp_path, db):
    """f:junk isn't in ews.folders (only f:inbox is, after the seed above),
    so resolve_folder_id raises before the gateway is ever touched."""
    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True))
    res = _run(ctx, "search_messages", folder="f:junk")
    assert res["ok"] is False


def test_cache_error_falls_back_to_live(tmp_path, db):
    """search_messages now answers store-only (Task 4/6): a mirror error there
    propagates rather than falling back. get_message still falls back — its
    cache_reads helper keeps its own try/except — so it carries this test."""
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
