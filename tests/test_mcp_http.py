"""ewsmcp's Streamable-HTTP transport wrapper: public health, bearer gate on
/mcp, and /readyz reflecting DB + daemon reachability."""

import asyncio
import json

from test_mcp_thin import DeadDaemon, RecordingDaemon, _mcp_ctx

from ewsmcp import __version__
from ewsmcp.mcp.http import build_mcp_http_app


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


def test_livez_is_public(db):
    ctx = _mcp_ctx(db, DeadDaemon(), mcp_api_key="k")
    app = build_mcp_http_app(ctx, ctx.settings, None)
    status, body = _drive(app, "/livez")
    assert status == 200 and body["status"] == "ok"


def test_mcp_requires_bearer_when_api_key_set(db):
    ctx = _mcp_ctx(db, DeadDaemon(), mcp_api_key="k")
    app = build_mcp_http_app(ctx, ctx.settings, None)
    status, body = _drive(app, "/mcp", method="POST")
    assert status == 401
    assert body["error"]["code"] == "auth_failed"


def test_readyz_reports_unreachable_daemon_with_db_up(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    app = build_mcp_http_app(ctx, ctx.settings, None)
    status, body = _drive(app, "/readyz")
    assert status == 200
    assert body["status"] == "ok"
    assert body["daemon"]["reachable"] is False


def test_readyz_reports_healthy_daemon(db):
    ctx = _mcp_ctx(db, RecordingDaemon())
    app = build_mcp_http_app(ctx, ctx.settings, None)
    status, body = _drive(app, "/readyz")
    assert status == 200
    assert body["daemon"].get("ok") is True


def test_readyz_503_after_db_closed(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    app = build_mcp_http_app(ctx, ctx.settings, None)
    ctx.db.close()
    status, body = _drive(app, "/readyz")
    assert status == 503
    assert body["status"] == "unavailable"


def test_version(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    app = build_mcp_http_app(ctx, ctx.settings, None)
    status, body = _drive(app, "/version")
    assert status == 200
    assert body["version"] == __version__ == "5.2.0a2"
