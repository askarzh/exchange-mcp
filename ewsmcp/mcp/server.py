"""Thin MCP process wiring: Postgres + DaemonClient, MCP Server over stdio."""

import logging
from typing import Any

import psycopg
import psycopg_pool
from mcp.server import Server
from mcp.types import Tool

from ..annotations import ANNOTATIONS
from ..audit import NullAudit
from ..cache.store import CacheStore
from ..config import Settings
from ..db import SCHEMA_VERSION, Database, SchemaOutdated
from ..ids import IdAliaser
from ..tools.base import Context
from .client import DaemonClient
from .dispatch import dispatch_mcp
from .registry import build_mcp_registry

logger = logging.getLogger(__name__)


def build_mcp_context(settings: Settings) -> Context:
    db = Database(settings.database_url, max_size=4)  # behaviour unchanged, just explicit
    try:
        db.require_version(SCHEMA_VERSION)
    except SchemaOutdated:
        # A too-old schema is a hard misconfiguration: fail loudly rather
        # than serve tools against a database this build can't read.
        raise
    except (psycopg.OperationalError, psycopg_pool.PoolTimeout) as exc:
        # Postgres is unreachable at boot. Don't kill the process: the
        # runtime read paths already degrade to backend_unavailable, and
        # psycopg_pool reconnects on its own once Postgres comes back.
        logger.error("could not reach Postgres at boot (%s); continuing "
                      "degraded, will retry in the background", exc)
    ctx = Context(settings=settings, gateway=None, manager=None, aliaser=IdAliaser(db),
                  audit=NullAudit(), cache=CacheStore(db), db=db,
                  daemon=DaemonClient(settings.ewsd_url, settings.ewsd_api_key))
    build_mcp_registry(ctx)
    return ctx


def build_mcp_server(ctx: Context) -> Server:
    server = Server("ews-mcp")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [Tool(name=s.name, description=s.description, inputSchema=s.input_schema,
                     annotations=ANNOTATIONS.get(s.side_effect_class, ANNOTATIONS["write"]))
                for s in ctx.registry.values()]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]):
        spec = ctx.registry.get(name)
        if spec is None:
            return {"ok": False, "error": {
                "code": "validation",
                "message": f"Unknown tool: {name}",
                "hint": f"Available: {', '.join(sorted(ctx.registry))}"}}
        return await dispatch_mcp(ctx, spec, dict(arguments or {}))

    return server


async def run_stdio(settings: Settings) -> None:
    from mcp.server.stdio import stdio_server
    ctx = build_mcp_context(settings)
    server = build_mcp_server(ctx)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())
