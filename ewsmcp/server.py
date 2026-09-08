"""MCP wiring: low-level Server, annotations, structured output, lifecycle."""

import logging

# Re-exported for the daemon's own wiring; the definition lives in
# ewsmcp/annotations.py so the thin MCP can import it without exchangelib.
from .annotations import ANNOTATIONS as ANNOTATIONS
from .audit import AuditLog, NullAudit
from .cache import CacheStore

# Eagerly, at module top: cache/__init__ exports SyncEngine lazily now
# (the thin MCP must not load exchangelib), and resolving it inside
# on_warm's swallowing try would turn an exchangelib drift into a
# green boot that silently never syncs. ewsd owns Exchange, so this
# import belongs here; the MCP never calls build_context.
from .cache.sync import SyncEngine
from .config import Settings
from .db import Database
from .gateway.client import EWSGateway
from .gateway.connection import ConnectionManager
from .ids import IdAliaser
from .tools.base import Context
from .tools.registry import build_registry

logger = logging.getLogger(__name__)


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
        from .boilerplate import BoilerplateHarness, GeminiCleaner, LlmDetector
        from .embeddings import GeminiEmbedder
        from .semantic import SemanticIndex
        embedder = GeminiEmbedder(settings.gemini_api_key, dims=settings.embed_dims)
        # The LLM detector is optional and always additive: it only ever
        # LOGS unless ARCHIVE_BOILERPLATE_DROP names it.
        llm = None
        if settings.archive_boilerplate_llm and settings.gemini_api_key:
            llm = LlmDetector(GeminiCleaner(settings.gemini_api_key,
                                            model=settings.gemini_clean_model))
        harness = BoilerplateHarness(
            ctx.cache, embedder,
            threshold=settings.embed_boilerplate_threshold,
            drop=settings.archive_boilerplate_drop, llm=llm)
        ctx.semantic = SemanticIndex(ctx.cache, embedder, harness=harness)
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

