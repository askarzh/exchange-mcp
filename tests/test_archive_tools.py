"""The four new tools: gates, envelopes, and what each one reads."""

import asyncio
import json
import time
from types import SimpleNamespace

from conftest import FakeEmbedder, FakeGateway, make_context, make_row

from ewsmcp.archive import files
from ewsmcp.semantic import SemanticIndex
from ewsmcp.tools.base import dispatch

NOW = int(time.time())
DAY = 86400


def _run(ctx, name, **kw):
    return asyncio.run(dispatch(ctx, ctx.registry[name], dict(kw)))


def _ctx(db, **over):
    over.setdefault("ews_capability_tier", "full")
    ctx = make_context(db, **over)
    ctx.cache.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0}])
    return ctx


# --- registry -----------------------------------------------------------------


def test_the_four_tools_are_registered_at_the_right_tiers(db):
    full = _ctx(db)
    assert len(full.registry) == 35
    assert full.registry["archive_run"].side_effect_class == "destructive"
    for name in ("archive_status", "get_raw_message", "find_similar"):
        assert full.registry[name].side_effect_class == "read"
    read = _ctx(db, ews_capability_tier="read")
    assert len(read.registry) == 18
    assert "archive_run" not in read.registry
    assert "find_similar" in read.registry     # always registered now
    assert len(_ctx(db, ews_capability_tier="draft").registry) == 29


# --- archive_status -----------------------------------------------------------


def test_archive_status_reports_states_runs_blobs_and_backlog(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.cache.upsert_messages([make_row("A1"), make_row("A2")])
    ctx.cache.mark_captured("A1", mime_sha256="a" * 64, mime_path="/x.eml")
    files.store_mime(ctx.settings.data_dir, b"x" * 100)
    run_id = ctx.cache.start_run("capture", dry_run=True, policy={})
    ctx.cache.finish_run(run_id, captured=1)

    res = _run(ctx, "archive_status")
    assert res["ok"] is True
    assert res["states"] == {"live": 1, "captured": 1, "verified": 0, "deleted": 0}
    assert res["blob_store_bytes"] == 100
    assert res["embedding"]["backlog"] == 2
    assert res["recent_runs"][0]["id"] == run_id
    assert res["policy"]["folders"] == ["f:inbox", "f:sent"]
    assert res["delete_enabled"] is False
    # Regression: archive_runs timestamps are tz-aware datetimes from psycopg
    # and used to make the whole envelope unserialisable over MCP.
    run = res["recent_runs"][0]
    assert isinstance(run["started_at"], str) and "T" in run["started_at"]
    assert isinstance(run["finished_at"], str)
    json.dumps(res)


def test_archive_status_needs_no_exchange(db):
    spec = _ctx(db).registry["archive_status"]
    assert spec.requires_ews is False


# --- archive_run --------------------------------------------------------------


def test_archive_run_dry_run_needs_no_confirmation(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.archive = _FakeRunner()
    res = _run(ctx, "archive_run", dry_run=True)
    assert res["ok"] and "confirm_token" not in res
    assert ctx.archive.calls == [{"kind": "all", "dry_run": True,
                                  "before": None, "folders": None}]


def test_archive_run_for_real_is_two_phase(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.archive = _FakeRunner()
    phase1 = _run(ctx, "archive_run", dry_run=False)
    assert phase1["requires_confirmation"] is True and phase1["confirm_token"]
    # Phase 1 previews with a REAL dry run — exactly one runner call, and it
    # is a dry run — so nothing is executed, but the preview carries the
    # resolved policy and the dry-run counts, not just the caller's args.
    assert len(ctx.archive.calls) == 1
    assert ctx.archive.calls[0]["dry_run"] is True
    assert phase1["preview"]["policy"]["folders"] == ["f:inbox", "f:sent"]
    assert phase1["preview"]["candidates"] == 3

    phase2 = _run(ctx, "archive_run", dry_run=False,
                  confirm_token=phase1["confirm_token"])
    assert phase2["ok"] is True
    # Phase 2 makes the real (dry_run=False) call.
    real_calls = [c for c in ctx.archive.calls if c["dry_run"] is False]
    assert len(real_calls) == 1


def test_archive_run_is_blocked_below_the_full_tier(db):
    ctx = _ctx(db, ews_capability_tier="draft")
    assert "archive_run" not in ctx.registry


def test_archive_run_without_a_runner_is_unavailable(db):
    ctx = _ctx(db)
    ctx.archive = None
    res = _run(ctx, "archive_run", dry_run=True)
    assert res["ok"] is False
    assert res["error"]["code"] == "upstream_unavailable"


def test_archive_run_rejects_an_unknown_kind(db):
    ctx = _ctx(db)
    ctx.archive = _FakeRunner()
    res = _run(ctx, "archive_run", dry_run=True, kind="nuke")
    assert res["ok"] is False and res["error"]["code"] == "validation"


class _FakeRunner:
    def __init__(self):
        self.calls = []

    async def run_once(self, *, kind, dry_run, before, folders):
        self.calls.append({"kind": kind, "dry_run": dry_run, "before": before,
                           "folders": folders})
        return {"ok": True, "run_id": 7, "kind": kind, "dry_run": dry_run,
                "candidates": 3, "captured": 0, "verified": 0, "reset": 0,
                "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
                "blocked": None, "stopped": None, "error": None, "sample": []}


# --- get_raw_message ----------------------------------------------------------


def test_get_raw_message_returns_a_capability_url(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"),
               external_url="https://ews.example.com")
    ctx.cache.upsert_messages([make_row("A1", subject="Fwd: Прогноз - инвестиции")])
    sha, path = files.store_mime(ctx.settings.data_dir, b"RAW-MIME")
    ctx.cache.mark_captured("A1", mime_sha256=sha, mime_path=str(path))

    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is True
    assert res["download_url"].startswith("https://ews.example.com/download/")
    # The real subject, not the ASCII reduction ("Fwd_ _ - _.eml") — the
    # download itself serves it through filename*.
    assert res["name"] == "Fwd: Прогноз - инвестиции.eml"
    assert res["name"] in res["curl"]
    assert res["size_bytes"] == len(b"RAW-MIME")
    assert res["expires_in_minutes"] == 15

    from ewsmcp import downloads
    token = res["download_url"].rsplit("/", 1)[1]
    assert downloads.redeem(ctx.settings.data_dir, token)["path"] == str(path)


def test_get_raw_message_without_external_url_is_relative(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.cache.upsert_messages([make_row("A1")])
    sha, path = files.store_mime(ctx.settings.data_dir, b"M")
    ctx.cache.mark_captured("A1", mime_sha256=sha, mime_path=str(path))
    assert _run(ctx, "get_raw_message", id="A1")["download_url"].startswith(
        "/download/")


class _FetchAccount:
    """A minimal account double: only `fetch(ids=..., only_fields=...)`."""

    def __init__(self, results):
        self.results = results

    def fetch(self, ids, only_fields=None):
        return list(self.results)


def test_get_raw_message_on_live_mail_fetches_mime_via_ewsd(db, tmp_path):
    item = SimpleNamespace(mime_content=b"LIVE-MIME")
    gateway = FakeGateway(_FetchAccount([item]))
    ctx = _ctx(db, data_dir=str(tmp_path / "data"), gateway=gateway)
    ctx.cache.upsert_messages([make_row("A1", subject="Still live")])

    res = _run(ctx, "get_raw_message", id="A1")

    assert res["ok"] is True
    assert res["archive_state"] == "live"
    assert res["sha256"] == files.sha256_bytes(b"LIVE-MIME")
    assert gateway.calls == 1
    # store_mime is content-addressed and does NOT flip archive_state or
    # mime_sha256 on the row — this is a fetch-and-cache, not a capture.
    row = ctx.cache.get_message("A1")
    assert row["archive_state"] == "live"
    assert row["mime_sha256"] is None


def test_get_raw_message_for_an_unknown_id_is_not_found(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    res = _run(ctx, "get_raw_message", id="NOPE")
    assert res["ok"] is False and res["error"]["code"] == "not_found"


def test_get_raw_message_when_exchange_says_not_found(db, tmp_path):
    class ErrorItemNotFound(Exception):
        pass

    gateway = FakeGateway(_FetchAccount([ErrorItemNotFound("gone")]))
    ctx = _ctx(db, data_dir=str(tmp_path / "data"), gateway=gateway)
    ctx.cache.upsert_messages([make_row("A1")])
    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is False and res["error"]["code"] == "not_found"


def test_get_raw_message_when_the_file_vanished(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.cache.upsert_messages([make_row("A1")])
    ctx.cache.mark_captured("A1", mime_sha256="a" * 64,
                            mime_path=str(tmp_path / "gone.eml"))
    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is False and res["error"]["code"] == "not_found"


# --- find_similar -------------------------------------------------------------


def _semantic(ctx):
    ctx.cache.upsert_messages([
        make_row("S-BUDGET", subject="Quarterly budget",
                 body="finance forecast spreadsheet"),
        make_row("S-FORECAST", subject="Forecast update",
                 body="finance forecast numbers"),
        make_row("S-LUNCH", subject="Lunch", body="shawarma at noon"),
    ])
    ctx.semantic = SemanticIndex(ctx.cache, FakeEmbedder())
    ctx.semantic.index_messages(ctx.cache.unembedded_messages(100))
    return ctx


def test_find_similar_by_message_id(db):
    ctx = _semantic(_ctx(db))
    res = _run(ctx, "find_similar", id="S-BUDGET", limit=2)
    assert res["ok"] is True and res["count"] >= 1
    assert all(item["id"] != "S-BUDGET" for item in res["items"])
    assert "similarity" in res["items"][0]


def test_find_similar_by_free_text(db):
    ctx = _semantic(_ctx(db))
    res = _run(ctx, "find_similar", text="finance forecast", limit=3)
    ids = [i["subject"] for i in res["items"]]
    assert "Lunch" not in ids[:1]


def test_find_similar_needs_exactly_one_of_id_or_text(db):
    ctx = _semantic(_ctx(db))
    assert _run(ctx, "find_similar")["error"]["code"] == "validation"
    assert _run(ctx, "find_similar", id="S-BUDGET",
                text="x")["error"]["code"] == "validation"


def test_find_similar_without_a_key_is_a_clear_error(db):
    ctx = _ctx(db)
    ctx.semantic = None
    res = _run(ctx, "find_similar", text="budget")
    assert res["ok"] is False and res["error"]["code"] == "validation"
    assert "GEMINI_API_KEY" in res["error"]["hint"]


# --- get_raw_message and a cold Exchange --------------------------------------


class _ColdManager:
    """Exchange still warming up — what the dispatcher's cold gate sees."""

    state = "connecting"

    def status(self):
        return {"state": "connecting", "attempts": 4,
                "last_error": "TransportError: Failed to get auth type",
                "next_retry_in_s": 30, "last_success_age_s": None}


def test_archived_mail_is_served_while_exchange_is_cold(db, tmp_path):
    """The archive earns its keep exactly when Exchange is unreachable, so
    get_raw_message is requires_ews=False and an archived row still mints a
    link during warm-up."""
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.manager = _ColdManager()
    ctx.cache.upsert_messages([make_row("A1", subject="Contract")])
    sha, path = files.store_mime(ctx.settings.data_dir, b"RAW-MIME")
    ctx.cache.mark_captured("A1", mime_sha256=sha, mime_path=str(path))
    ctx.cache.mark_verified("A1")

    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is True
    assert res["archive_state"] == "verified"
    assert res["download_url"].startswith("/download/")


def test_live_mail_still_gets_the_cold_gate(db, tmp_path):
    """The live branch needs Exchange, so it re-applies by hand the gate the
    dispatcher no longer applies for this tool — same code, same message."""
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.manager = _ColdManager()
    ctx.cache.upsert_messages([make_row("A1")])
    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is False
    assert res["error"]["code"] == "upstream_unavailable"
    assert "auth type" in res["error"]["message"]


def test_get_raw_message_is_not_ews_gated_in_the_registry(db):
    from ewsmcp.tools import archive as archive_tools
    spec = {s.name: s for s in archive_tools.TOOLS}["get_raw_message"]
    assert spec.requires_ews is False
