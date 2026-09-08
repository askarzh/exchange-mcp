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
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="full").registry) == 35
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="draft").registry) == 29
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="read").registry) == 18
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


def test_semantic_search_is_forwarded_to_the_daemon(db):
    """mode='semantic' never runs locally -- the MCP never holds the Gemini
    key -- it is forwarded to ewsd verbatim."""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is True and res["proxied"] == "search_messages"
    assert daemon.calls[0] == ("search_messages",
                               {"query": "budget", "mode": "semantic"})


def test_semantic_search_reports_daemon_unavailable_when_ewsd_is_down(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is False and res["error"]["code"] == "daemon_unavailable"


def test_get_thread_with_a_dead_mirror_is_backend_unavailable(db):
    """The MCP surface makes the same distinction as the daemon: a closed
    pool is backend_unavailable, never a not_found."""
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    alias = _run(ctx, "search_messages", query="budget")["items"][0]["id"]
    assert _run(ctx, "get_thread", id=alias)["ok"] is True
    db.close()
    res = asyncio.run(dispatch_mcp(ctx, ctx.registry["get_thread"],
                                   {"id": alias}))
    assert res["ok"] is False and res["error"]["code"] == "backend_unavailable"


def test_arguments_are_validated_against_the_tool_schema(db):
    """The MCP dispatcher validates against spec.input_schema before any
    handler runs — mcp/local.py does not re-clamp limit/offset by hand, and
    an out-of-range argument must not reach the store or ewsd."""
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    over = _run(ctx, "search_messages", query="budget", limit=5000)
    assert over["ok"] is False and over["error"]["code"] == "validation"
    assert "50" in over["error"]["message"]

    negative = _run(ctx, "search_messages", query="budget", offset=-1)
    assert negative["ok"] is False and negative["error"]["code"] == "validation"

    thread_over = _run(ctx, "get_thread", id="m1", limit=5000)
    assert thread_over["ok"] is False and thread_over["error"]["code"] == "validation"

    # in-range arguments still work
    assert _run(ctx, "search_messages", query="budget", limit=50, offset=0)["ok"]


def test_validation_rejects_unknown_arguments_before_forwarding(db):
    """A proxied tool is validated too, so ewsd never sees junk arguments."""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon, ews_capability_tier="full")
    res = _run(ctx, "create_draft", subject="hi", nonsense=1)
    assert res["ok"] is False and res["error"]["code"] == "validation"
    assert daemon.calls == []


def test_archive_status_is_answered_locally_with_the_daemon_down(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    ctx.cache.mark_captured("RAW-1", mime_sha256="a" * 64, mime_path="/x.eml")
    res = _run(ctx, "archive_status")
    assert res["ok"] is True
    assert res["states"]["captured"] == 1
    assert "archive_status" in LOCAL_TOOLS


def test_archive_status_disk_figures_come_from_the_daemon_never_the_mcps_own_disk(
        db, monkeypatch):
    """blob_store_bytes/free_gb live under ewsd's DATA_DIR, which may not
    even be the same disk as the MCP's container -- so the MCP must copy
    them out of the daemon's status response and never touch its own
    filesystem to compute them."""
    from ewsmcp.archive import files

    def boom(data_dir):
        raise AssertionError("MCP must never compute blob_store_bytes itself")

    monkeypatch.setattr(files, "blob_store_bytes", boom)
    monkeypatch.setattr(files, "free_gb", boom)

    class StatusWithArchive(RecordingDaemon):
        async def status(self):
            return {"ok": True, "archive": {
                "running": True, "cycles": 9, "blob_store_bytes": 12345,
                "free_gb": 3.5, "state_counts": {"live": 1},
                "embedding_backlog": 0,
            }}

    ctx = _mcp_ctx(db, StatusWithArchive())
    _seed(ctx)
    res = _run(ctx, "archive_status")
    assert res["ok"] is True
    assert res["blob_store_bytes"] == 12345
    assert res["free_gb"] == 3.5
    assert res["runner"]["cycles"] == 9
    assert "state_counts" not in res["runner"]  # already reported as res["states"]
    assert "disk_stats" not in res


def test_archive_status_policy_and_delete_switch_come_from_the_daemon(db):
    """The MCP container carries no ARCHIVE_* environment, so computing the
    policy from its own settings reports defaults that can contradict what
    ewsd actually runs (seen in production: top-level delete_enabled=false
    beside runner delete_enabled=true)."""
    ewsd_policy = {"folders": ["f:inbox"], "after_days": 120, "grace_days": 1,
                   "exclude_categories": [], "max_delete_per_run": 200,
                   "min_free_gb": 5.0}

    class StatusWithPolicy(RecordingDaemon):
        async def status(self):
            return {"ok": True, "archive": {
                "running": True, "cycles": 1, "delete_enabled": True,
                "delete_auto": True, "policy": ewsd_policy,
                "semantic_enabled": True,
            }}

    ctx = _mcp_ctx(db, StatusWithPolicy())
    _seed(ctx)
    res = _run(ctx, "archive_status")
    assert res["policy"] == ewsd_policy
    assert res["policy_source"] == "ewsd"
    assert res["delete_enabled"] is True
    assert res["semantic_enabled"] is True   # the MCP itself has no Gemini key
    assert res["runner"]["delete_enabled"] is True
    assert "policy" not in res["runner"]
    assert "semantic_enabled" not in res["runner"]


def test_archive_status_omits_disk_figures_with_the_daemon_down(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    res = _run(ctx, "archive_status")
    assert res["ok"] is True
    assert "blob_store_bytes" not in res
    assert "free_gb" not in res
    assert res["disk_stats"] == "unavailable — ewsd unreachable"
    assert res["policy_source"].startswith("mcp defaults")


def test_the_mcp_never_holds_the_gemini_key(db):
    """find_similar and mode=semantic are FORWARDED: only ewsd embeds."""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon, ews_capability_tier="full")
    _seed(ctx)
    assert "find_similar" not in LOCAL_TOOLS

    res = _run(ctx, "find_similar", text="budget")
    assert res["proxied"] == "find_similar"

    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["proxied"] == "search_messages"
    assert daemon.calls[-1][1]["mode"] == "semantic"


def test_keyword_search_stays_local_and_honours_archived(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    ctx.cache.mark_captured("RAW-1", mime_sha256="a" * 64, mime_path="/x.eml")
    res = _run(ctx, "search_messages", query="budget", archived="only")
    assert res["source"] == "cache"
    assert [i["archive_state"] for i in res["items"]] == ["captured"]
    assert daemon.calls == []


def test_archive_run_and_get_raw_message_proxy_to_the_daemon(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon, ews_capability_tier="full")
    assert _run(ctx, "archive_run", dry_run=True)["proxied"] == "archive_run"
    assert _run(ctx, "get_raw_message", id="RAW-1")["proxied"] == "get_raw_message"


def test_archive_status_surfaces_skipped_too_large_from_the_daemon(db):
    """`skipped_too_large` is a per-process counter on ewsd's runner with no
    DB row behind it. The MCP drops the runner's `state_counts` wholesale
    (the DB-derived counts are already in `states`), so it has to lift that
    one key across or an operator can never see items stuck behind
    ARCHIVE_MAX_ITEM_MB."""
    class StatusWithTooLarge(RecordingDaemon):
        async def status(self):
            return {"ok": True, "archive": {
                "running": True, "cycles": 2,
                "state_counts": {"skipped_too_large": 3},
            }}

    ctx = _mcp_ctx(db, StatusWithTooLarge())
    _seed(ctx)
    res = _run(ctx, "archive_status")
    assert res["ok"] is True
    assert res["states"]["skipped_too_large"] == 3
    assert res["states"]["live"] == 2          # DB-derived counts still there
    assert "state_counts" not in res["runner"]


def test_keyword_search_honours_include_calendar_items_locally(db):
    """The MCP builds the cache_reads call itself, so every tool argument has
    to be handed over explicitly — `include_calendar_items` was dropped on
    the floor, silently ignoring what the caller asked for (the daemon path
    in tools/mail_read.py passes it)."""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    ctx.cache.upsert_messages([make_row("CAL-1", subject="Accepted: Budget review",
                                        body="", conv="C1")])
    ctx.cache.update_bodies({}, None,
                            {"CAL-1": {"item_class": "IPM.Schedule.Meeting.Resp.Pos"}})

    res = _run(ctx, "search_messages", query="budget")
    assert "Accepted: Budget review" not in [i["subject"] for i in res["items"]]

    res = _run(ctx, "search_messages", query="budget", include_calendar_items=True)
    assert "Accepted: Budget review" in [i["subject"] for i in res["items"]]
    assert res["source"] == "cache" and daemon.calls == []
