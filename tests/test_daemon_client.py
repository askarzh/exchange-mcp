"""DaemonClient against the real daemon app via httpx.ASGITransport."""

import asyncio

import httpx
import pytest
from conftest import make_context

from ewsmcp.daemon import build_daemon_app
from ewsmcp.errors import ToolError
from ewsmcp.mcp.client import DaemonClient


def _client(db, **overrides):
    ctx = make_context(db, ewsd_api_key="k", **overrides)
    app = build_daemon_app(ctx, ctx.settings)
    return DaemonClient("http://ewsd", "k", transport=httpx.ASGITransport(app=app)), ctx


def test_call_tool_passes_envelopes_through(db):
    client, _ctx = _client(db, ews_capability_tier="full", send_enabled=False)
    out = asyncio.run(client.call_tool("send_draft", {"draft_id": "d1"}))
    assert out["ok"] is False and out["error"]["code"] == "kill_switch"
    st = asyncio.run(client.status())
    assert st["ok"] and st["tier"] == "full"
    asyncio.run(client.aclose())


def test_wrong_key_is_auth_failed_envelope(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    client = DaemonClient("http://ewsd", "wrong", transport=httpx.ASGITransport(app=app))
    out = asyncio.run(client.call_tool("get_server_status", {}))
    assert out["ok"] is False and out["error"]["code"] == "auth_failed"
    asyncio.run(client.aclose())


def test_unreachable_daemon_maps_to_daemon_unavailable():
    client = DaemonClient("http://127.0.0.1:9", "k", timeout=0.5)
    try:
        with pytest.raises(ToolError) as e:
            asyncio.run(client.call_tool("get_server_status", {}))
        assert e.value.code == "daemon_unavailable"
    finally:
        asyncio.run(client.aclose())
