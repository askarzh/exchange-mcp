"""MCP wiring: low-level Server, annotations, structured output, lifecycle."""

import logging

from mcp.types import ToolAnnotations

from .audit import AuditLog, NullAudit
from .cache import CacheStore
from .config import Settings
from .db import Database
from .gateway.client import EWSGateway
from .gateway.connection import ConnectionManager
from .ids import IdAliaser
from .tools import build_registry
from .tools.base import Context

logger = logging.getLogger(__name__)

ANNOTATIONS = {
    "read": ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                            idempotentHint=True, openWorldHint=False),
    "write": ToolAnnotations(readOnlyHint=False, destructiveHint=False,
                             idempotentHint=False, openWorldHint=False),
    "destructive": ToolAnnotations(readOnlyHint=False, destructiveHint=True,
                                   idempotentHint=False, openWorldHint=False),
    "send": ToolAnnotations(readOnlyHint=False, destructiveHint=True,
                            idempotentHint=False, openWorldHint=True),
}


def build_context(settings: Settings) -> Context:
    gateway = EWSGateway(settings)
    db = Database(settings.database_url)
    db.migrate()
    aliaser = IdAliaser(db)
    # Audit is a quality-of-life layer — its storage failing (bad volume,
    # permissions) degrades it to pass-through, never prevents boot: the
    # never-exit contract covers local disks too.
    try:
        audit = AuditLog(settings.data_dir)
    except Exception as exc:  # noqa: BLE001 - audit is best-effort, never blocks boot
        logger.error("audit init failed (%s) — audit disabled", exc)
        audit = NullAudit()
    ctx = Context(
        settings=settings,
        gateway=gateway,
        manager=None,
        aliaser=aliaser,
        audit=audit,
        cache=CacheStore(db),
        db=db,
    )
    if settings.semantic_enabled():
        from .embeddings import GeminiEmbedder
        from .semantic import SemanticIndex
        ctx.semantic = SemanticIndex(
            ctx.cache, GeminiEmbedder(settings.gemini_api_key,
                                      dims=settings.embed_dims))
    else:
        logger.info("GEMINI_API_KEY unset — semantic search disabled, "
                    "keyword search unaffected")
    from .archive import ArchiveRunner
    ctx.archive = ArchiveRunner(settings, gateway, ctx.cache, audit,
                                index=ctx.semantic)
    build_registry(ctx)
    return ctx


async def start_connection_manager(ctx: Context) -> None:
    manager = ConnectionManager(
        ctx.gateway,
        max_backoff=float(ctx.settings.ews_warmup_max_backoff_seconds),
        heartbeat_seconds=int(ctx.settings.ews_heartbeat_seconds),
    )
    ctx.manager = manager

    async def on_warm() -> None:
        # The sync engine is owned by the warm state: it starts only once
        # Exchange has answered (and keeps running through later outages —
        # its own cycles degrade gracefully).
        if ctx.cache is not None and ctx.sync is None:
            try:
                from .cache import SyncEngine
                ctx.sync = SyncEngine(ctx.settings, ctx.gateway, ctx.cache)
                await ctx.sync.start()
            except Exception as exc:  # noqa: BLE001 - sync is best-effort; cache stays stale
                logger.error("sync engine start failed (%s) — cache stays "
                             "stale; reads fall back to live EWS", exc)
        if ctx.archive is not None:
            try:
                await ctx.archive.start()
            except Exception as exc:  # noqa: BLE001 - archive is best-effort
                logger.error("archive runner start failed (%s) — capture/verify "
                             "will not run until ewsd restarts", exc)

    await manager.start(on_warm=on_warm)
    logger.info("Exchange warmup running in background (see /readyz)")

