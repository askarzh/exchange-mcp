"""Thin MCP over Streamable HTTP: /mcp plus health. REST for scripts lives on ewsd."""

import logging
from typing import Any

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from .. import __version__
from ..http import _authorized, _send_json
from .server import build_mcp_context, build_mcp_server

logger = logging.getLogger(__name__)


def build_mcp_http_app(ctx, settings, streamable):
    api_key = settings.mcp_api_key or ""

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            return
        path, method = scope["path"], scope["method"]
        if path == "/livez" and method == "GET":
            return await _send_json(send, 200, {"status": "ok"})
        if path == "/version" and method == "GET":
            return await _send_json(send, 200, {"version": __version__})
        if path in ("/health", "/readyz") and method == "GET":
            status: dict[str, Any] = {"status": "ok", "tools": len(ctx.registry)}
            code = 200
            try:
                ctx.db.schema_version()
            except Exception as exc:  # noqa: BLE001
                status.update(status="unavailable", database=str(exc))
                code = 503
            if path == "/readyz":
                try:
                    status["daemon"] = await ctx.daemon.status()
                except Exception as exc:  # noqa: BLE001
                    status["daemon"] = {"reachable": False, "error": str(exc)}
            return await _send_json(send, code, status)
        if api_key and not _authorized(scope.get("headers"), api_key):
            return await _send_json(send, 401, {"ok": False, "error": {
                "code": "auth_failed", "message": "missing or invalid bearer token"}})
        if path == "/mcp":
            return await streamable.handle_request(scope, receive, send)
        return await _send_json(send, 404, {"ok": False, "error": {
            "code": "validation", "message": "not found"}})

    return app


async def serve_http(settings) -> None:
    import uvicorn
    if settings.mcp_host not in ("127.0.0.1", "localhost", "::1") and not settings.mcp_api_key:
        raise SystemExit("refusing to bind on a non-loopback address without MCP_API_KEY")
    ctx = build_mcp_context(settings)
    mcp_server = build_mcp_server(ctx)
    streamable = StreamableHTTPSessionManager(app=mcp_server, json_response=False,
                                              stateless=True)
    app = build_mcp_http_app(ctx, settings, streamable)
    config = uvicorn.Config(app, host=settings.mcp_host, port=settings.mcp_port,
                            log_level=settings.log_level.lower(), http="h11")
    async with streamable.run():
        await uvicorn.Server(config).serve()
