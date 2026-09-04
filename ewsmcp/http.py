"""ewsd's HTTP app: REST tool shim, capability-URL uploads, health and
status (DESIGN.md §Transports). No /mcp here — that's ewsmcp/mcp/http.py,
the only module that speaks MCP over Streamable HTTP."""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import jsonschema

from . import __version__, downloads, uploads
from .errors import HTTP_BY_CODE

# Re-exported: the thin MCP's HTTP transport imports them from here today,
# but they must not drag ewsd's gateway/tool imports along (see httputil.py).
from .httputil import _authorized as _authorized
from .httputil import _send_json as _send_json
from .server import start_connection_manager
from .tools.base import dispatch, validator_for
from .tools.calendar_people import _get_server_status

logger = logging.getLogger(__name__)

MAX_BODY_BYTES = 1_048_576  # 1 MiB — tool arguments, not attachments
_DOWNLOAD_CHUNK = 1024 * 1024  # stream files in 1 MiB chunks, not whole into memory


async def _metrics_text(ctx) -> str:
    """Prometheus exposition (text format 0.0.4). Behind the API key like
    every non-health endpoint — scrape with a bearer token.

    Async because the cache/archive gauges are Postgres COUNT queries: they
    go through asyncio.to_thread rather than blocking the event loop of a
    process that is also serving tool calls."""
    import time as _t
    lines = [
        "# TYPE ewsmcp_uptime_seconds gauge",
        f"ewsmcp_uptime_seconds {int(_t.time() - ctx.started_at)}",
    ]
    state = ctx.manager.state if ctx.manager else "unmanaged"
    lines += ["# TYPE ewsmcp_connection_warm gauge",
              f"ewsmcp_connection_warm {1 if state in ('warm', 'unmanaged') else 0}"]
    lines.append("# TYPE ewsmcp_tool_calls_total counter")
    lines.append("# TYPE ewsmcp_errors_total counter")
    for key, value in sorted(ctx.counters.items()):
        if key.startswith("tool."):
            lines.append(f'ewsmcp_tool_calls_total{{tool="{key[5:]}"}} {value}')
        elif key.startswith("err."):
            lines.append(f'ewsmcp_errors_total{{code="{key[4:]}"}} {value}')
    if ctx.cache is not None:
        try:
            stats = await asyncio.to_thread(ctx.cache.stats)
            lines.append("# TYPE ewsmcp_cache_rows gauge")
            for table, n in stats.get("rows", {}).items():
                lines.append(f'ewsmcp_cache_rows{{table="{table}"}} {n}')
            lines.append("# TYPE ewsmcp_cache_db_mb gauge")
            lines.append(f"ewsmcp_cache_db_mb {stats.get('db_mb', 0)}")
        except Exception:
            pass
    if ctx.sync is not None:
        status = ctx.sync.status()
        lines.append("# TYPE ewsmcp_sync_cycles_total counter")
        lines.append(f"ewsmcp_sync_cycles_total {status.get('cycles', 0)}")
        age = status.get("last_cycle_age_s")
        if age is not None:
            lines.append("# TYPE ewsmcp_sync_last_cycle_age_seconds gauge")
            lines.append(f"ewsmcp_sync_last_cycle_age_seconds {age}")
        lines.append("# TYPE ewsmcp_sync_degraded gauge")
        lines.append(f"ewsmcp_sync_degraded {1 if status.get('last_error') else 0}")
    if ctx.archive is not None:
        status = ctx.archive.status()
        lines.append("# TYPE ewsmcp_archive_cycles_total counter")
        lines.append(f"ewsmcp_archive_cycles_total {status.get('cycles', 0)}")
        lines.append("# TYPE ewsmcp_archive_degraded gauge")
        lines.append(f"ewsmcp_archive_degraded {1 if status.get('last_error') else 0}")
        if ctx.cache is not None:
            try:
                counts, backlog = await asyncio.to_thread(
                    lambda: (ctx.cache.archive_state_counts(),
                             ctx.cache.embedding_backlog()))
                lines.append("# TYPE ewsmcp_archive_messages gauge")
                for state, n in counts.items():
                    lines.append(f'ewsmcp_archive_messages{{state="{state}"}} {n}')
                lines.append("# TYPE ewsmcp_archive_embedding_backlog gauge")
                lines.append(f"ewsmcp_archive_embedding_backlog {backlog}")
            except Exception:
                pass
    return "\n".join(lines) + "\n"


def _openapi(ctx, tools_prefix: str) -> dict[str, Any]:
    paths = {}
    for name, spec in ctx.registry.items():
        schema = spec.public_schema()
        paths[f"{tools_prefix}/{name}"] = {"post": {
            "operationId": name,
            "summary": schema["description"][:120],
            "requestBody": {"content": {"application/json": {"schema": schema["inputSchema"]}}},
            "responses": {"200": {"description": "tool result"}},
        }}
    # /v1/archive/run is a thin alias for POST /v1/tools/archive_run (see
    # _dispatch_tool_route) — only documented when the tool is actually
    # registered at this server's tier, same as every other tools_prefix
    # entry above.
    archive_run_spec = ctx.registry.get("archive_run")
    if archive_run_spec is not None:
        schema = archive_run_spec.public_schema()
        paths["/v1/archive/run"] = {"post": {
            "operationId": "archive_run_route",
            "summary": schema["description"][:120],
            "requestBody": {"content": {"application/json": {"schema": schema["inputSchema"]}}},
            "responses": {"200": {"description": "archive_run result or "
                                                  "two-phase confirmation"}},
        }}
    paths["/v1/archive/runs/{id}"] = {"get": {
        "operationId": "get_archive_run",
        "summary": "Read one archive_runs row by id.",
        "parameters": [{"name": "id", "in": "path", "required": True,
                        "schema": {"type": "integer"}}],
        "responses": {"200": {"description": "the archive_runs row"},
                     "404": {"description": "no such run"}},
    }}
    return {"openapi": "3.0.3",
            "info": {"title": "ews-mcp v5", "version": __version__},
            "paths": paths}


async def _read_json_body(receive, send) -> Any | None:
    """Drain the request body (bounded) and parse JSON.

    Returns the parsed value, or None after having already sent an error
    response. A client disconnect mid-body returns None without sending
    (the old loop hung forever waiting for more http.request messages).
    """
    chunks = []
    size = 0
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return None
        if message["type"] == "http.request":
            body = message.get("body", b"")
            size += len(body)
            if size > MAX_BODY_BYTES:
                await _send_json(send, 413, {"ok": False, "error": {
                    "code": "validation",
                    "message": f"request body exceeds {MAX_BODY_BYTES} bytes"}})
                return None
            chunks.append(body)
            if not message.get("more_body"):
                break
    try:
        return json.loads(b"".join(chunks) or b"{}")
    except Exception as e:
        await _send_json(send, 400, {"ok": False, "error": {
            "code": "validation", "message": f"invalid JSON body: {e}"}})
        return None


async def _dispatch_tool_route(ctx, name: str, receive, send) -> None:
    """The REST tool shim's body, factored out so `/v1/archive/run` can be a
    thin alias for `POST /v1/tools/archive_run` — same registry lookup
    (404/tier envelope when the tool isn't registered at this server's
    tier), same schema validation, same `dispatch()` call, so `dry_run=false`
    goes through the tool's own two-phase confirm (spec.preview) exactly
    like every other destructive tool. No route may execute a real archive
    pass with only the bearer."""
    spec = ctx.registry.get(name)
    if spec is None:
        return await _send_json(send, 404, {"ok": False, "error": {
            "code": "validation", "message": f"Unknown tool: {name}"}})
    arguments = await _read_json_body(receive, send)
    if arguments is None:
        return
    if not isinstance(arguments, dict):
        return await _send_json(send, 400, {"ok": False, "error": {
            "code": "validation",
            "message": "request body must be a JSON object of tool arguments"}})
    error = jsonschema.exceptions.best_match(
        validator_for(spec).iter_errors(arguments))
    if error is not None:
        return await _send_json(send, 400, {"ok": False, "error": {
            "code": "validation", "message": error.message,
            "hint": f"See the {name} schema in /openapi.json."}})
    result = await dispatch(ctx, spec, arguments, transport="rest")
    status = 200
    if isinstance(result, dict) and result.get("ok") is False:
        status = HTTP_BY_CODE.get(result.get("error", {}).get("code", ""), 500)
    return await _send_json(send, status, result)


def build_app(ctx, settings, *, tools_prefix: str = "/v1/tools",
             api_key: str | None = None):
    """ASGI app closure, driven directly by tests (no uvicorn needed).

    Serves health, /metrics, /openapi.json, the capability-URL upload route
    and the REST tool routes under `tools_prefix`. The MCP transport is NOT
    here — it lives in ewsmcp/mcp/http.py, the only process that speaks MCP.
    `api_key` overrides `settings.mcp_api_key` when given (ewsd passes
    `settings.ewsd_api_key`).
    """
    key = (settings.mcp_api_key if api_key is None else api_key) or ""

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await start_connection_manager(ctx)
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            return
        path, method = scope["path"], scope["method"]

        if path == "/livez" and method == "GET":
            return await _send_json(send, 200, {"status": "ok"})
        if path == "/health" and method == "GET":
            return await _send_json(send, 200, {"status": "ok", "tools": len(ctx.registry)})
        if path == "/version" and method == "GET":
            return await _send_json(send, 200, {"version": __version__})
        if path == "/readyz" and method == "GET":
            conn = ctx.manager.status() if ctx.manager else {"state": "unmanaged"}
            warm = conn.get("state") in ("warm", "unmanaged")
            return await _send_json(send, 200 if warm else 503, {
                "status": "ok" if warm else "unavailable",
                "connection": conn, "tools": len(ctx.registry),
            })

        # Capability-URL upload: PUT /upload/<token>. Deliberately ahead of the
        # bearer gate — the unguessable single-use token IS the credential (see
        # ewsmcp/uploads.py). Every failure renders as an identical opaque 404 so
        # probing cannot distinguish expired / used / never-existed.
        if path.startswith("/upload/") and method in ("PUT", "POST"):
            token = path[len("/upload/"):]
            body = b""
            while True:
                message = await receive()
                if message["type"] != "http.request":
                    break
                body += message.get("body", b"")
                if len(body) > uploads.MAX_UPLOAD_BYTES:
                    return await _send_json(send, 404, {"ok": False, "error": {
                        "code": "not_found", "message": "not found"}})
                if not message.get("more_body"):
                    break
            try:
                out = uploads.redeem(settings.data_dir, token, body)
            except uploads.UploadRejected:
                return await _send_json(send, 404, {"ok": False, "error": {
                    "code": "not_found", "message": "not found"}})
            return await _send_json(send, 200, {"ok": True, **out})

        # Capability-URL download: GET /download/<token>. Same model as
        # /upload — deliberately ahead of the bearer gate, single use, and
        # every failure is an identical opaque 404. The file is streamed in
        # chunks rather than read whole into memory, since blobs may be
        # tens of MB.
        if path.startswith("/download/") and method == "GET":
            token = path[len("/download/"):]
            try:
                rec = downloads.redeem(settings.data_dir, token)
                file_path = Path(rec["path"])
                size = file_path.stat().st_size
            except (downloads.DownloadRejected, OSError):
                return await _send_json(send, 404, {"ok": False, "error": {
                    "code": "not_found", "message": "not found"}})
            # Defense in depth: re-sanitize the header values here too, even
            # though downloads.redeem() already did — these ride verbatim
            # into HTTP headers and are ultimately attacker-influenced
            # (mail-derived names/types).
            safe_ct = downloads.safe_content_type(rec["content_type"])
            # One line, two forms: an ASCII-safe `filename=` plus the real
            # (possibly non-ASCII) name percent-encoded in `filename*`
            # (RFC 5987) — otherwise "Отчёт.pdf" is served as "pdf".
            disposition = downloads.content_disposition(
                rec.get("orig_name") or rec["name"]).encode("ascii")
            await send({"type": "http.response.start", "status": 200, "headers": [
                [b"content-type", safe_ct.encode()],
                [b"content-length", str(size).encode()],
                [b"content-disposition", disposition],
            ]})
            try:
                with file_path.open("rb") as fh:
                    chunk = fh.read(_DOWNLOAD_CHUNK)
                    while True:
                        nxt = fh.read(_DOWNLOAD_CHUNK)
                        await send({"type": "http.response.body", "body": chunk,
                                    "more_body": bool(nxt)})
                        if not nxt:
                            break
                        chunk = nxt
            except Exception:
                # The client hung up mid-stream (broken pipe / connection
                # reset). Nothing left to serve — stop quietly rather than
                # raising into the ASGI server.
                logger.debug("download %s: client disconnected mid-stream", token)
            return None

        if key and not _authorized(scope.get("headers"), key):
            return await _send_json(send, 401, {"ok": False, "error": {
                "code": "auth_failed", "message": "missing or invalid bearer token"}})

        if path == "/v1/status" and method == "GET":
            return await _send_json(send, 200, await _get_server_status(ctx))

        if path == "/v1/archive/run" and method == "POST":
            # Thin alias for POST /v1/tools/archive_run — NOT a shortcut
            # around the tool's own gates. dry_run=false is two-phase
            # confirmed by the registered archive_run ToolSpec's preview
            # hook (see ewsmcp/tools/archive.py); this route never executes
            # a real pass on the bearer alone.
            return await _dispatch_tool_route(ctx, "archive_run", receive, send)

        if path.startswith("/v1/archive/runs/") and method == "GET":
            raw = path.removeprefix("/v1/archive/runs/")
            if not raw.isdigit():
                return await _send_json(send, 400, {"ok": False, "error": {
                    "code": "validation", "message": "run id must be an integer"}})
            row = ctx.cache.get_run(int(raw)) if ctx.cache is not None else None
            if row is None:
                return await _send_json(send, 404, {"ok": False, "error": {
                    "code": "not_found", "message": f"no archive run {raw}"}})
            return await _send_json(send, 200, {"ok": True, **dict(row)})

        if path == "/metrics" and method == "GET":
            body = (await _metrics_text(ctx)).encode()
            await send({"type": "http.response.start", "status": 200, "headers": [
                [b"content-type", b"text/plain; version=0.0.4; charset=utf-8"],
                [b"content-length", str(len(body)).encode()],
            ]})
            return await send({"type": "http.response.body", "body": body})
        if path == "/openapi.json" and method == "GET":
            return await _send_json(send, 200, _openapi(ctx, tools_prefix))
        if path == tools_prefix and method == "GET":
            return await _send_json(send, 200, {"tools": [
                {"name": s.name, "class": s.side_effect_class,
                 "description": s.description[:140],
                 "inputSchema": s.public_schema()["inputSchema"]}
                for s in ctx.registry.values()
            ]})
        if path.startswith(tools_prefix + "/") and method == "POST":
            name = path.removeprefix(tools_prefix + "/")
            return await _dispatch_tool_route(ctx, name, receive, send)

        return await _send_json(send, 404, {"ok": False, "error": {
            "code": "validation", "message": "not found"}})

    return app
