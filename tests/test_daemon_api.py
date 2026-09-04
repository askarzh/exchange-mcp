"""ewsd HTTP API: bearer gate, tool listing, status, tool dispatch, no /mcp."""

import asyncio
import json

from conftest import make_context

from ewsmcp.daemon import build_daemon_app


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
    assert all(p.startswith("/v1/tools/") for p in body["paths"])


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
    assert status == 200 and body["ok"] and body["version"].startswith("5.0.")
    assert body["cache"]["ready"] is True


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
                "candidates": 5, "captured": 0, "verified": 0, "reset": 0,
                "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
                "blocked": None, "stopped": None, "error": None, "sample": []}


def test_archive_run_route_needs_the_bearer(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive(app, "/v1/archive/run", "POST", {})[0] == 401


def test_archive_run_route_defaults_to_a_dry_run(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {}, headers=AUTH)
    assert status == 200 and body["run_id"] == 11
    assert ctx.archive.calls == [("all", True, None, None)]


def test_archive_run_route_passes_the_arguments_through(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    _drive(app, "/v1/archive/run", "POST",
           {"kind": "capture", "dry_run": False, "before": "2026-01-01",
            "folders": ["inbox"]}, headers=AUTH)
    assert ctx.archive.calls == [("capture", False, "2026-01-01", ["inbox"])]


def test_archive_run_route_rejects_an_unknown_kind(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {"kind": "nuke"},
                          headers=AUTH)
    assert status == 400 and body["error"]["code"] == "validation"


def test_archive_run_route_without_a_runner_is_503(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = None
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {}, headers=AUTH)
    assert status == 503 and body["error"]["code"] == "upstream_unavailable"


def test_archive_run_status_route(db):
    ctx = make_context(db, ewsd_api_key="k")
    run_id = ctx.cache.start_run("capture", dry_run=True, policy={})
    ctx.cache.finish_run(run_id, captured=2)
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, f"/v1/archive/runs/{run_id}", headers=AUTH)
    assert status == 200 and body["captured"] == 2 and body["kind"] == "capture"
    assert _drive(app, "/v1/archive/runs/999999", headers=AUTH)[0] == 404
    assert _drive(app, "/v1/archive/runs/abc", headers=AUTH)[0] == 400
