"""Phase F surface: tasks pack, get_contact, waiting_on, semantic mode
(reserved/unavailable), /metrics."""

import asyncio
import time
from datetime import date
from types import SimpleNamespace

from conftest import (
    INBOX_ID,
    SENT_ID,
    FakeGateway,
    make_context,
    make_row,
    make_settings,
    seed_folders,
)

from ewsmcp.cache.store import CacheStore
from ewsmcp.tools.base import dispatch


def _ctx(tmp_path, db, gateway=None, cache=None, **overrides):
    ctx = make_context(db, gateway=gateway or FakeGateway(), cache=False,
                       audit_dir=str(tmp_path / "audit"), **overrides)
    ctx.cache = cache
    return ctx


def _run(ctx, name, **kwargs):
    return asyncio.run(dispatch(ctx, ctx.registry[name], dict(kwargs)))


# --- registry counts (change DELIBERATELY) ----------------------------------


def test_registry_counts_per_tier_and_semantic(tmp_path, db):
    full = _ctx(tmp_path, db, ews_capability_tier="full")
    assert len(full.registry) == 35
    assert "find_similar" in full.registry  # the semantic tier is back, Gemini-backed
    draft = _ctx(tmp_path, db, ews_capability_tier="draft")
    assert len(draft.registry) == 29
    read = _ctx(tmp_path, db, ews_capability_tier="read")
    assert len(read.registry) == 18


# --- tasks pack ---------------------------------------------------------------


def _tasks_store(db):
    store = CacheStore(db)
    store.upsert_tasks([
        {"ews_id": "T1", "changekey": None, "subject": "File the report",
         "due_ts": 100, "due_iso": "2026-07-15", "is_complete": 0,
         "status": "NotStarted"},
    ])
    store.set_sync_state("item:tasks", "TOK", time.time())
    return store


def test_list_tasks_from_mirror(tmp_path, db):
    ctx = _ctx(tmp_path, db, cache=_tasks_store(db))
    res = _run(ctx, "list_tasks")
    assert res["source"] == "cache"
    assert res["count"] == 1
    assert res["items"][0]["subject"] == "File the report"
    assert res["items"][0]["id"].startswith("k")


def test_list_tasks_live_fallback_without_mirror(tmp_path, db):
    class Item:
        id = "T-RAW"
        changekey = None
        subject = "Live task"
        due_date = None
        is_complete = False
        status = "NotStarted"

    class Tasks:
        def all(self):
            return [Item()]

    account = SimpleNamespace(tasks=Tasks())
    ctx = _ctx(tmp_path, db, FakeGateway(account))
    res = _run(ctx, "list_tasks")
    assert res["source"] == "live"
    assert res["items"][0]["subject"] == "Live task"


def test_update_task_complete_and_due(tmp_path, db):
    class Item:
        def __init__(self):
            self.id = "T-RAW"
            self.due_date = None
            self.is_complete = False
            self.saved = []
            self.completed = 0

        def save(self, update_fields=None, **kw):
            self.saved.append(update_fields)

        def complete(self):
            self.completed += 1
            self.is_complete = True

    item = Item()
    account = SimpleNamespace(fetch=lambda ids, only_fields=None: [item])
    ctx = _ctx(tmp_path, db, FakeGateway(account))
    res = _run(ctx, "update_task", id="T-RAW", complete=True, due="2026-08-01")
    assert res["ok"] is True
    assert item.completed == 1
    assert item.due_date == date(2026, 8, 1)
    assert item.saved == [["due_date"]]
    nothing = _run(ctx, "update_task", id="T-RAW")
    assert nothing["error"]["code"] == "validation"


def test_waiting_on_from_mirror(tmp_path, db):
    store = CacheStore(db)
    seed_folders(store)
    now = int(time.time())
    store.upsert_messages([
        make_row("S1", conv="CW", folder_id=SENT_ID, date_ts=now - 6 * 86400,
                 subject="Pending decision", to=["boss@corp.example"]),
    ])
    store.set_sync_state(f"item:{SENT_ID}", "TOK", now)
    ctx = _ctx(tmp_path, db, cache=store)
    res = _run(ctx, "waiting_on", days=5)
    assert res["source"] == "cache"
    assert res["count"] == 1
    assert res["items"][0]["subject"] == "Pending decision"
    assert res["items"][0]["to"] == ["boss@corp.example"]
    assert res["items"][0]["thread"].startswith("t")


def test_waiting_on_requires_mirror(tmp_path, db):
    ctx = _ctx(tmp_path, db)  # no cache
    res = _run(ctx, "waiting_on")
    assert res["error"]["code"] == "upstream_unavailable"


# --- get_contact ----------------------------------------------------------------


def test_get_contact_by_email_with_history(tmp_path, db):
    store = CacheStore(db)
    now = int(time.time())
    store.upsert_messages([
        make_row("M1", sender_email="boss@corp.example", sender_name="Boss",
                 date_ts=now - 100),
    ])
    mailbox = SimpleNamespace(name="Boss Person", email_address="boss@corp.example")
    contact = SimpleNamespace(display_name="Boss Person", job_title="Director",
                              company_name="Acme", phone_numbers=[])
    account = SimpleNamespace(protocol=SimpleNamespace(
        resolve_names=lambda names, return_full_contact_data: [(mailbox, contact)]))
    ctx = _ctx(tmp_path, db, FakeGateway(account), cache=store)
    res = _run(ctx, "get_contact", id="boss@corp.example")
    assert res["ok"] is True
    person = res["person"]
    assert person["title"] == "Director"
    assert person["history"]["received_count"] == 1
    assert person["id"].startswith("p")


def test_get_contact_mirror_fallback_when_gal_down(tmp_path, db):
    store = CacheStore(db)
    store.upsert_messages([
        make_row("M1", sender_email="boss@corp.example", sender_name="Boss")])

    ctx = _ctx(tmp_path, db, FakeGateway(raise_on_call=True), cache=store)
    res = _run(ctx, "get_contact", id="boss@corp.example")
    assert res["ok"] is True
    assert res["person"]["source"] == "mirror"


# --- semantic mode ----------------------------------------------------------------


def _sem_store(db):
    store = CacheStore(db)
    seed_folders(store)
    now = int(time.time())
    store.upsert_messages([
        make_row("K1", subject="Vendor contract", body="terms agreed",
                 date_ts=now - 300),
        make_row("K2", subject="Vendor invoice", body="payment due",
                 date_ts=now - 200),
        make_row("K3", subject="Weekly report", body="numbers inside",
                 date_ts=now - 100),
    ])
    store.set_sync_state(f"item:{INBOX_ID}", "TOK", now)
    return store


def test_semantic_mode_degrades_to_keyword_without_an_embedder(tmp_path, db):
    """No GEMINI_API_KEY -> ctx.semantic is None -> degrade to keyword
    results, never a hard error (Task 13)."""
    ctx = _ctx(tmp_path, db, cache=_sem_store(db))
    res = _run(ctx, "search_messages", query="vendor", mode="semantic")
    assert res["ok"] is True
    assert res["meta"]["degraded"] is True
    assert "GEMINI_API_KEY" in res["meta"]["reason"]


# --- /metrics --------------------------------------------------------------------


def test_metrics_exposition(tmp_path, db):

    from ewsmcp.http import build_app

    ctx = _ctx(tmp_path, db, cache=_sem_store(db))
    ctx.counters["tool.search_messages"] = 4
    ctx.counters["err.validation"] = 1
    app = build_app(ctx, make_settings())
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    asyncio.run(app({"type": "http", "path": "/metrics", "method": "GET",
                     "headers": []}, receive, send))
    body = b"".join(m.get("body", b"") for m in sent
                    if m["type"] == "http.response.body").decode()
    assert 'ewsmcp_tool_calls_total{tool="search_messages"} 4' in body
    assert 'ewsmcp_errors_total{code="validation"} 1' in body
    assert 'ewsmcp_cache_rows{table="messages"} 3' in body
    assert "ewsmcp_uptime_seconds" in body
