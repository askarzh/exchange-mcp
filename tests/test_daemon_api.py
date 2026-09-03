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
