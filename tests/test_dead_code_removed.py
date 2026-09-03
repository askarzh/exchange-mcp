"""Phase 1.5 removed these names. This test is the guard against them
creeping back in on a merge."""

import importlib

import pytest

REMOVED = [
    ("ewsmcp.server", "run_stdio"),
    ("ewsmcp.server", "build_mcp_server"),
    ("ewsmcp.server", "_NullAudit"),
    ("ewsmcp.tools.base", "mint_token"),
    ("ewsmcp.tools.base", "_resolve_ids"),
    ("ewsmcp.tools.mail_read", "_cache_folder_key"),
    ("ewsmcp.tools.mail_read", "_cache_watermark"),
    ("ewsmcp.tools.mail_read", "_row_body"),
    ("ewsmcp.tools.mail_read", "_row_card"),
    ("ewsmcp.tools.mail_read", "_row_full"),
    ("ewsmcp.ids", "NullAliaser"),
    # unused Python-side preview of the SQL-built tsquery (final review)
    ("ewsmcp.cache.store", "prefix_tsquery"),
    # no callers: search_messages resolves one folder, never a whole list
    ("ewsmcp.tools.cache_reads", "mirrored_folder_ids"),
]


@pytest.mark.parametrize("module_name,attr", REMOVED)
def test_symbol_is_gone(module_name, attr):
    module = importlib.import_module(module_name)
    assert not hasattr(module, attr), f"{module_name}.{attr} came back"


def test_tool_error_has_no_http_status():
    from ewsmcp.errors import ToolError
    assert not hasattr(ToolError("validation", "x"), "http_status")


def test_connection_manager_has_no_is_warm():
    from ewsmcp.gateway.connection import ConnectionManager
    assert not hasattr(ConnectionManager, "is_warm")


def test_cache_store_has_no_purge_or_close():
    from ewsmcp.cache.store import CacheStore
    assert not hasattr(CacheStore, "purge")
    assert not hasattr(CacheStore, "close")


def test_null_audit_lives_in_audit_module():
    from ewsmcp.audit import NullAudit
    assert NullAudit().record("t", "read", "ok", 1) is None


def test_live_smoke_script_is_gone():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    assert not (root / "scripts" / "live_smoke.py").exists()


def test_build_app_defaults_to_the_v1_prefix_and_has_no_mcp_branch(db, tmp_path):
    import asyncio
    import inspect
    import json

    from conftest import make_context, make_settings

    from ewsmcp.http import build_app

    params = inspect.signature(build_app).parameters
    assert "streamable" not in params
    assert "mount_mcp" not in params
    assert params["tools_prefix"].default == "/v1/tools"

    ctx = make_context(db, audit_dir=str(tmp_path / "audit"))
    app = build_app(ctx, make_settings())
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    asyncio.run(app({"type": "http", "path": "/v1/tools", "method": "GET",
                     "headers": []}, receive, send))
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    body = json.loads(b"".join(m.get("body", b"") for m in sent
                               if m["type"] == "http.response.body"))
    assert status == 200
    assert len(body["tools"]) == len(ctx.registry)

    sent.clear()
    asyncio.run(app({"type": "http", "path": "/mcp", "method": "POST",
                     "headers": []}, receive, send))
    assert next(m["status"] for m in sent
                if m["type"] == "http.response.start") == 404
