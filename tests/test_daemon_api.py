"""ewsd HTTP API: bearer gate, tool listing, status, tool dispatch, no /mcp."""

import asyncio
import json
import logging

from conftest import make_context, make_row, make_settings

from ewsmcp.daemon import build_daemon_app
from ewsmcp.server import build_context


def _drive(app, path, method="GET", body=None, headers=()):
    scope = {"type": "http", "path": path, "method": method,
             "headers": [(k.encode(), v.encode()) for k, v in headers]}
    msgs = [{"type": "http.request", "body": json.dumps(body).encode() if body is not None
             else b"", "more_body": False}]
    sent = []

    async def receive():
        return msgs.pop(0)

    async def send(m):
        sent.append(m)

    asyncio.run(app(scope, receive, send))
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return status, json.loads(raw or b"{}")


def _drive_raw(app, path, method="GET", headers=()):
    """Like _drive but returns (status, headers, raw bytes) — the download
    route answers with file bytes, not JSON."""
    scope = {"type": "http", "path": path, "method": method,
             "headers": [(k.encode(), v.encode()) for k, v in headers]}
    msgs = [{"type": "http.request", "body": b"", "more_body": False}]
    sent = []

    async def receive():
        return msgs.pop(0)

    async def send(m):
        sent.append(m)

    asyncio.run(app(scope, receive, send))
    start = next(m for m in sent if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in sent
                   if m["type"] == "http.response.body")
    return start["status"], [tuple(h) for h in start["headers"]], raw


AUTH = [("authorization", "Bearer k")]


def test_bearer_required_except_health(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive(app, "/livez")[0] == 200
    assert _drive(app, "/v1/tools")[0] == 401
    assert _drive(app, "/v1/tools", headers=AUTH)[0] == 200


def test_openapi_paths_use_daemon_tools_prefix(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    _status, body = _drive(app, "/openapi.json", headers=AUTH)
    assert body["paths"]
    assert all(p.startswith("/v1/tools/") or p.startswith("/v1/archive/")
              for p in body["paths"])


def test_openapi_archive_run_path_follows_the_registry_tier(db):
    full = make_context(db, ewsd_api_key="k", ews_capability_tier="full")
    app = build_daemon_app(full, full.settings)
    _status, body = _drive(app, "/openapi.json", headers=AUTH)
    assert "/v1/archive/run" in body["paths"]
    assert "/v1/archive/runs/{id}" in body["paths"]
    schema = body["paths"]["/v1/archive/run"]["post"]["requestBody"][
        "content"]["application/json"]["schema"]
    assert "confirm_token" in schema["properties"]

    below_full = make_context(db, ewsd_api_key="k", ews_capability_tier="read")
    app = build_daemon_app(below_full, below_full.settings)
    _status, body = _drive(app, "/openapi.json", headers=AUTH)
    assert "/v1/archive/run" not in body["paths"]
    assert "/v1/archive/runs/{id}" in body["paths"]


def test_tools_listing_carries_public_schemas(db):
    ctx = make_context(db, ewsd_api_key="k", ews_capability_tier="full")
    app = build_daemon_app(ctx, ctx.settings)
    _status, body = _drive(app, "/v1/tools", headers=AUTH)
    names = {t["name"] for t in body["tools"]}
    assert "send_draft" in names and "search_messages" in names
    send = next(t for t in body["tools"] if t["name"] == "send_draft")
    assert "confirm_token" in send["inputSchema"]["properties"]


def test_status_answers_cold(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/status", headers=AUTH)
    assert status == 200 and body["ok"] and body["version"].startswith("5.1.")
    assert body["cache"]["ready"] is True


class _StatusRunner:
    """A fake ArchiveRunner exposing only .status()/.disk_stats() —
    status/metrics never call run_once."""

    def status(self):
        return {"running": True, "cycles": 4, "cycle_seconds": 300,
                "last_cycle_age_s": 12, "last_run_id": 7,
                "last_error": "boom: disk full", "delete_enabled": False}

    async def disk_stats(self):
        return {"blob_store_bytes": 4096, "free_gb": 12.5}


def test_status_route_includes_the_archive_block(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _StatusRunner()
    ctx.cache.upsert_messages([make_row("A1"), make_row("A2")])
    ctx.cache.mark_captured("A1", mime_sha256="a" * 64, mime_path="/x.eml")
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/status", headers=AUTH)
    assert status == 200
    archive = body["archive"]
    assert archive["cycles"] == 4
    assert archive["last_error"] == "boom: disk full"
    assert archive["state_counts"] == {"live": 1, "captured": 1, "verified": 0,
                                       "deleted": 0}
    assert archive["embedding_backlog"] == 2
    # blob_store_bytes/free_gb come from THIS process's DATA_DIR (ewsd's own)
    # via ArchiveRunner.disk_stats() — never computed by the MCP.
    assert archive["blob_store_bytes"] == 4096
    assert archive["free_gb"] == 12.5


def test_status_route_merges_runner_state_counts_with_db_state_counts(db):
    """The runner's own state_counts (e.g. skipped_too_large, a per-process
    counter with no DB row) must survive the merge with the DB-derived
    live/captured/verified/deleted counts, not be clobbered by it."""
    class _RunnerWithTooLarge:
        def status(self):
            return {"running": True, "cycles": 4, "cycle_seconds": 300,
                    "last_cycle_age_s": 12, "last_run_id": 7,
                    "last_error": None, "delete_enabled": False,
                    "state_counts": {"skipped_too_large": 3}}

    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _RunnerWithTooLarge()
    ctx.cache.upsert_messages([make_row("A1"), make_row("A2")])
    ctx.cache.mark_captured("A1", mime_sha256="a" * 64, mime_path="/x.eml")
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/status", headers=AUTH)
    assert status == 200
    assert body["archive"]["state_counts"] == {
        "skipped_too_large": 3, "live": 1, "captured": 1, "verified": 0,
        "deleted": 0}


def test_status_route_omits_disk_stats_when_the_runner_lacks_them(db):
    """A runner stub without disk_stats() (older/fake) must not crash the
    status route — the archive block simply lacks blob_store_bytes/free_gb."""
    class _RunnerNoDisk:
        def status(self):
            return {"running": True, "cycles": 1, "cycle_seconds": 300,
                    "last_cycle_age_s": None, "last_run_id": None,
                    "last_error": None, "delete_enabled": False}

    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _RunnerNoDisk()
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/status", headers=AUTH)
    assert status == 200
    assert "blob_store_bytes" not in body["archive"]
    assert "free_gb" not in body["archive"]


def test_metrics_route_includes_the_archive_gauges(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _StatusRunner()
    ctx.cache.upsert_messages([make_row("A1"), make_row("A2")])
    ctx.cache.mark_captured("A1", mime_sha256="a" * 64, mime_path="/x.eml")
    app = build_daemon_app(ctx, ctx.settings)
    status, headers, raw = _drive_raw(app, "/metrics", headers=AUTH)
    text = raw.decode()
    assert status == 200
    assert "ewsmcp_archive_cycles_total 4" in text
    assert "ewsmcp_archive_degraded 1" in text
    assert 'ewsmcp_archive_messages{state="live"} 1' in text
    assert 'ewsmcp_archive_messages{state="captured"} 1' in text
    assert "ewsmcp_archive_embedding_backlog 2" in text


def test_tool_dispatch_runs_gate_chain(db):
    ctx = make_context(db, ewsd_api_key="k", ews_capability_tier="full",
                       send_enabled=False)
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/tools/send_draft", "POST", {"draft_id": "d1"},
                          headers=AUTH)
    assert status == 403 and body["error"]["code"] == "kill_switch"


def test_no_mcp_route_on_daemon(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive(app, "/mcp", "POST", {}, headers=AUTH)[0] == 404


def test_download_route_serves_the_file_ahead_of_the_bearer_gate(db, tmp_path):
    from pathlib import Path

    from ewsmcp import downloads

    ctx = make_context(db, ewsd_api_key="k")
    mime = Path(ctx.settings.data_dir) / "mime"
    mime.mkdir(parents=True, exist_ok=True)
    (mime / "m.eml").write_bytes(b"RAW-MIME")
    token = downloads.mint(ctx.settings.data_dir, path=str(mime / "m.eml"),
                           name="m.eml", content_type="message/rfc822")["token"]
    app = build_daemon_app(ctx, ctx.settings)
    status, headers, body = _drive_raw(app, f"/download/{token}")   # NO bearer
    assert status == 200 and body == b"RAW-MIME"
    assert (b"content-type", b"message/rfc822") in headers
    assert any(b"attachment" in v for k, v in headers
               if k == b"content-disposition")
    # single use
    assert _drive_raw(app, f"/download/{token}")[0] == 404


def test_unknown_download_tokens_are_an_opaque_404(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive_raw(app, "/download/" + "0" * 64)[0] == 404
    assert _drive_raw(app, "/download/nonsense")[0] == 404


class _Runner:
    def __init__(self):
        self.calls = []

    async def run_once(self, *, kind, dry_run, before, folders):
        self.calls.append((kind, dry_run, before, folders))
        return {"ok": True, "run_id": 11, "kind": kind, "dry_run": dry_run,
                "candidates": 3, "captured": 0, "verified": 0, "reset": 0,
                "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
                "blocked": None, "stopped": None, "error": None, "sample": []}


def _full_ctx(db, **over):
    over.setdefault("ews_capability_tier", "full")
    ctx = make_context(db, **over)
    ctx.cache.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0}])
    return ctx


def test_archive_run_route_needs_the_bearer(db):
    ctx = _full_ctx(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive(app, "/v1/archive/run", "POST", {})[0] == 401


def test_archive_run_route_defaults_to_a_dry_run(db):
    ctx = _full_ctx(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {}, headers=AUTH)
    assert status == 200 and body["run_id"] == 11
    assert ctx.archive.calls == [("all", True, None, None)]


def test_archive_run_route_dry_run_passes_the_arguments_through(db):
    ctx = _full_ctx(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    _drive(app, "/v1/archive/run", "POST",
           {"kind": "capture", "dry_run": True, "before": "2026-01-01",
            "folders": ["inbox"]}, headers=AUTH)
    assert ctx.archive.calls == [("capture", True, "2026-01-01", ["inbox"])]


def test_archive_run_route_rejects_an_unknown_kind(db):
    ctx = _full_ctx(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {"kind": "nuke"},
                          headers=AUTH)
    assert status == 400 and body["error"]["code"] == "validation"


def test_archive_run_route_without_a_runner_is_503(db):
    ctx = _full_ctx(db, ewsd_api_key="k")
    ctx.archive = None
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {}, headers=AUTH)
    assert status == 503 and body["error"]["code"] == "upstream_unavailable"


def test_archive_run_route_for_real_is_two_phase_confirmed(db):
    """The bearer alone must never execute a real archive pass — dry_run=false
    goes through the archive_run tool's own preview + confirm_token gate,
    same as POST /v1/tools/archive_run."""
    ctx = _full_ctx(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)

    status, phase1 = _drive(app, "/v1/archive/run", "POST", {"dry_run": False},
                            headers=AUTH)
    assert status == 200
    assert phase1["requires_confirmation"] is True and phase1["confirm_token"]
    # Nothing executed: every call so far was a dry run (the preview hook's
    # own real dry pass), and zero real (dry_run=False) calls happened.
    assert all(call[1] is True for call in ctx.archive.calls)
    assert not any(call[1] is False for call in ctx.archive.calls)

    status, phase2 = _drive(
        app, "/v1/archive/run", "POST",
        {"dry_run": False, "confirm_token": phase1["confirm_token"]},
        headers=AUTH)
    assert status == 200 and phase2["ok"] is True
    real_calls = [c for c in ctx.archive.calls if c[1] is False]
    assert len(real_calls) == 1


def test_archive_run_route_below_full_tier_is_the_tools_404_envelope(db):
    ctx = _full_ctx(db, ewsd_api_key="k", ews_capability_tier="read")
    assert "archive_run" not in ctx.registry
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {}, headers=AUTH)
    assert status == 404
    assert body["error"]["code"] == "validation"
    assert "archive_run" in body["error"]["message"]


def test_archive_run_status_route(db):
    ctx = make_context(db, ewsd_api_key="k")
    run_id = ctx.cache.start_run("capture", dry_run=True, policy={})
    ctx.cache.finish_run(run_id, captured=2)
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, f"/v1/archive/runs/{run_id}", headers=AUTH)
    assert status == 200 and body["captured"] == 2 and body["kind"] == "capture"
    assert _drive(app, "/v1/archive/runs/999999", headers=AUTH)[0] == 404
    assert _drive(app, "/v1/archive/runs/abc", headers=AUTH)[0] == 400


def test_download_disposition_carries_a_non_ascii_filename(db):
    """The header must offer the real name (RFC 5987) and still be a single
    ASCII line: mail-derived filenames are attacker-influenced."""
    from pathlib import Path

    from ewsmcp import downloads

    ctx = make_context(db, ewsd_api_key="k")
    mime = Path(ctx.settings.data_dir) / "mime"
    mime.mkdir(parents=True, exist_ok=True)
    (mime / "r.eml").write_bytes(b"RAW")
    token = downloads.mint(ctx.settings.data_dir, path=str(mime / "r.eml"),
                           name="Отчёт.pdf")["token"]
    app = build_daemon_app(ctx, ctx.settings)
    status, headers, _body = _drive_raw(app, f"/download/{token}")
    assert status == 200
    value = next(v for k, v in headers if k == b"content-disposition")
    assert value.startswith(b'attachment; filename="pdf"; ')
    assert b"filename*=UTF-8''%D0%9E" in value
    assert b"\r" not in value and b"\n" not in value


def test_build_context_sizes_pool_from_settings_and_warns_when_undersized(
        pg_dsn, caplog):
    """The daemon's Database must honor DB_POOL_MAX (not the psycopg default
    of 4), and warn loudly when it's set below the concurrency the daemon
    itself expects to throw at it (EWS thread pool + archive lanes + HTTP)."""
    settings = make_settings(database_url=pg_dsn, db_pool_max=3,
                              ews_max_concurrency=8)
    with caplog.at_level(logging.WARNING, logger="ewsmcp.server"):
        ctx = build_context(settings)
    try:
        assert ctx.db.pool.max_size == 3
        warnings = [r.getMessage() for r in caplog.records
                    if r.levelno >= logging.WARNING]
        assert any("pool" in m and "consumers" in m for m in warnings)
    finally:
        ctx.db.close()
