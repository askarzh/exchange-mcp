"""Thin MCP: Postgres-first reads, daemon fallback, verbatim proxying of writes.
The gateway is None throughout — proof that the MCP never touches Exchange."""

import asyncio
import time

import httpx
from conftest import INBOX_ID, SENT_ID, make_context, make_row, make_settings, seed_folders

from ewsmcp.daemon import build_daemon_app
from ewsmcp.mcp.client import DaemonClient
from ewsmcp.mcp.dispatch import dispatch_mcp
from ewsmcp.mcp.registry import LOCAL_TOOLS, build_mcp_registry
from ewsmcp.tools.base import Context


class DeadDaemon:
    async def call_tool(self, name, arguments):
        from ewsmcp.errors import ToolError
        raise ToolError("daemon_unavailable", "down")

    async def status(self):
        from ewsmcp.errors import ToolError
        raise ToolError("daemon_unavailable", "down")


class RecordingDaemon:
    def __init__(self):
        self.calls = []

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        return {"ok": True, "proxied": name, "got": arguments}

    async def status(self):
        return {"ok": True, "version": "5.0.0a1", "connection": {"state": "warm"}}


def _mcp_ctx(db, daemon, **overrides):
    from ewsmcp.audit import NullAudit
    from ewsmcp.cache.store import CacheStore
    from ewsmcp.ids import IdAliaser
    ctx = Context(settings=make_settings(**overrides), gateway=None, manager=None,
                  aliaser=IdAliaser(db), audit=NullAudit(), cache=CacheStore(db),
                  db=db, daemon=daemon)
    build_mcp_registry(ctx)
    return ctx


def _seed(ctx):
    seed_folders(ctx.cache)
    now = int(time.time())
    ctx.cache.upsert_messages([
        make_row("RAW-1", subject="Budget review", body="please review", conv="C1",
                 is_read=0, date_ts=now - 300),
        make_row("RAW-2", subject="Re: Budget review", folder_id=SENT_ID, conv="C1",
                 sender_email="exec@corp.example", body="looks good", date_ts=now - 200),
    ])
    ctx.cache.set_sync_state(f"item:{INBOX_ID}", "T", now)
    ctx.cache.set_sync_state(f"item:{SENT_ID}", "T", now)
    ctx.cache.set_sync_state("events", None, now)


def _run(ctx, name, **kw):
    return asyncio.run(dispatch_mcp(ctx, ctx.registry[name], dict(kw)))


def test_registry_matches_daemon_counts(db):
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="full").registry) == 31
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="draft").registry) == 26
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="read").registry) == 15
    ctx = _mcp_ctx(db, DeadDaemon(), ews_capability_tier="full")
    assert LOCAL_TOOLS <= set(ctx.registry)
    assert "confirm_token" in ctx.registry["send_draft"].input_schema["properties"]
    assert all(not s.requires_ews and s.confirm is False for s in ctx.registry.values())


def test_local_reads_work_with_daemon_down(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    res = _run(ctx, "search_messages", query="budget", folder="f:inbox")
    assert res["ok"] and res["source"] == "cache" and res["count"] == 1
    alias = res["items"][0]["id"]
    assert alias.startswith("m")
    msg = _run(ctx, "get_message", id=alias)  # alias resolved locally
    assert msg["ok"] and msg["message"]["subject"] == "Budget review"
    thread = _run(ctx, "get_thread", id=alias)
    assert thread["count"] == 2
    ov = _run(ctx, "get_mailbox_overview")
    assert ov["unread_total"] == 1
    st = _run(ctx, "get_server_status")
    assert st["ok"] and st["daemon"]["reachable"] is False


def test_search_with_no_folder_spans_every_mirrored_folder(db):
    """folder omitted = every mirrored folder — on the MCP side too, a search
    with no folder returns rows from two different mirrored folders (inbox
    and sent)."""
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    res = _run(ctx, "search_messages", query="budget")
    assert res["ok"] and res["source"] == "cache"
    assert res["count"] == 2
    subjects = {it["subject"] for it in res["items"]}
    assert subjects == {"Budget review", "Re: Budget review"}


def test_fresh_and_misses_fall_through_to_daemon(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    _run(ctx, "get_message", id="RAW-1", fresh=True)
    _run(ctx, "get_message", id="UNKNOWN-RAW")
    assert [c[0] for c in daemon.calls] == ["get_message", "get_message"]
    assert daemon.calls[0][1]["fresh"] is True


def test_unmirrored_folder_is_a_validation_error_without_touching_the_daemon(db):
    """search_messages now resolves the folder against ews.folders and raises
    ToolError before ever reaching the mirror or the daemon (Task 4/6)."""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    res = _run(ctx, "search_messages", folder="f:doesnotexist")
    assert res["ok"] is False and res["error"]["code"] == "not_found"
    assert daemon.calls == []


def test_search_of_a_mirrored_but_unsynced_folder_is_empty(db):
    """The custom Archive folder is known (seed_folders wrote it, no wk) but
    never synced — an empty cache-served result, not a fall-through to the
    daemon. (f:junk is EWS_MIRROR_EXCLUDE'd, not merely unsynced — see the
    validation-error test below.)"""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    res = _run(ctx, "search_messages", folder="Archive 2024")
    assert res["ok"] is True and res["source"] == "cache" and res["count"] == 0
    assert daemon.calls == []


def test_search_of_an_excluded_folder_is_a_validation_error(db):
    """f:junk is in EWS_MIRROR_EXCLUDE — it is never item-synced at all, so
    searching it is refused up front rather than silently returning empty."""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    res = _run(ctx, "search_messages", folder="f:junk")
    assert res["ok"] is False and res["error"]["code"] == "validation"
    assert daemon.calls == []


def test_daemon_down_on_miss_is_daemon_unavailable(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    res = _run(ctx, "get_message", id="UNKNOWN-RAW")
    assert res["ok"] is False and res["error"]["code"] == "daemon_unavailable"


def test_writes_proxy_verbatim_including_confirm_token(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon, ews_capability_tier="full")
    res = _run(ctx, "send_draft", draft_id="d7", confirm_token="tok")
    assert res["proxied"] == "send_draft"
    assert daemon.calls[0][1] == {"draft_id": "d7", "confirm_token": "tok"}  # alias untouched


def test_end_to_end_through_real_daemon_app(db):
    """MCP → httpx → daemon app → dispatcher gate chain."""
    dctx = make_context(db, ewsd_api_key="k", ews_capability_tier="full", send_enabled=False)
    app = build_daemon_app(dctx, dctx.settings)
    client = DaemonClient("http://ewsd", "k", transport=httpx.ASGITransport(app=app))
    ctx = _mcp_ctx(db, client, ews_capability_tier="full")
    res = _run(ctx, "send_draft", draft_id="d1")
    assert res["ok"] is False and res["error"]["code"] == "kill_switch"


def test_db_down_is_backend_unavailable(db):
    """Mirror gone AND daemon gone → backend_unavailable (not a bare daemon error)."""
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    db.close()
    res = _run(ctx, "search_messages", query="budget")
    assert res["ok"] is False and res["error"]["code"] == "backend_unavailable"


def test_unknown_alias_is_validation_error_locally(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    res = _run(ctx, "get_message", id="m999")
    assert res["ok"] is False and res["error"]["code"] == "validation"


def test_semantic_search_is_validation_error_on_mcp_side(db):
    """mode='semantic' is rejected locally (mcp/local.py) before ever reaching
    the daemon or Exchange -- it is reserved, not implemented in this build."""
    ctx = _mcp_ctx(db, DeadDaemon())
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is False and res["error"]["code"] == "validation"
