"""Local handlers: answer from Postgres via tools.cache_reads, else forward to ewsd.

Deviation from the brief's literal `_try`: `tools.cache_reads` (Task 5) and
`cache.store.CacheStore.watermark(s)` (Task 5) deliberately swallow
`psycopg.Error` themselves for the fall-back-capable reads (get_message,
get_thread, list_folders, get_mailbox_overview, list_tasks) — a stale/
unreachable mirror degrades to a clean cache miss ("live fallback"), not an
exception, so live EWS reads keep working through the combined 4.5 process.
That means a closed pool never raises through those helpers; it just
returns None like an unsynced folder would. To still distinguish "mirror
unreachable" from "not synced yet" (the `test_db_down_is_backend_unavailable`
contract), `_try` follows a clean miss with one cheap liveness probe
(`ctx.db.schema_version()`, uncaught by any store-level swallowing) and
raises `_MirrorDown` only when THAT fails too.

`search_messages` and `get_thread` are store-only (Task 6): there is no live
fallback for either, so neither goes through `_try`/`_cache_then_forward`.
Both let `cache_reads`' `psycopg.Error`/`RuntimeError` propagate and map it
straight to `backend_unavailable` themselves; `get_thread` treats a clean
mirror miss (and ONLY that) as `not_found` — never a forward to ewsd.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import psycopg

from .. import __version__
from ..errors import ToolError
from ..tools import cache_reads
from ..tools.base import Context
from ..tools.tasks import _waiting_on

logger = logging.getLogger(__name__)


async def _forward(ctx: Context, name: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    return await ctx.daemon.call_tool(name, kwargs)


class _MirrorDown(Exception):
    """Postgres itself failed (pool closed, connection refused), not a bad query."""


async def _mirror_alive(ctx: Context) -> None:
    """Raise _MirrorDown if Postgres cannot answer even this trivial query."""
    if ctx.db is None:
        return
    try:
        await asyncio.to_thread(ctx.db.schema_version)
    except (psycopg.Error, RuntimeError) as exc:
        raise _MirrorDown(str(exc)) from exc


async def _try(ctx: Context, coro_factory) -> dict[str, Any] | None:
    """Run a cache read. None -> the mirror cannot answer (checked for a
    genuine outage via `_mirror_alive`); _MirrorDown -> the database is
    unreachable (the caller then forwards, and if ewsd is down too the
    model gets backend_unavailable rather than a misleading daemon error)."""
    try:
        result = await coro_factory()
    except ToolError:
        raise
    except (psycopg.Error, RuntimeError) as exc:  # psycopg_pool.PoolClosed is a RuntimeError
        logger.warning("mirror unreachable (%s) — forwarding to ewsd", exc)
        raise _MirrorDown(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 — bad row/shape -> daemon answers instead
        logger.warning("local read failed (%s) — forwarding to ewsd", exc)
        return None
    if result is None:
        await _mirror_alive(ctx)  # tell a clean miss apart from a swallowed outage
    return result


async def _forward_after(ctx: Context, name: str, kwargs: dict[str, Any],
                          mirror_error: str | None) -> dict[str, Any]:
    try:
        return await _forward(ctx, name, kwargs)
    except ToolError as err:
        if mirror_error and err.code == "daemon_unavailable":
            raise ToolError("backend_unavailable",
                             f"Postgres unreachable ({mirror_error}) and ewsd unreachable "
                             f"({err.message})", hint="Check DATABASE_URL and EWSD_URL.",
                             retry_after_s=15)
        raise


CacheCall = Callable[[], Awaitable[dict[str, Any] | None]]


async def _cache_then_forward(ctx: Context, name: str, kw: dict[str, Any],
                               cache_call: CacheCall | None,
                               post: Callable[[dict[str, Any]], None] | None = None,
                               ) -> dict[str, Any]:
    """Shared skeleton for every cache-backed handler: try the mirror (when
    `cache_call` is not None — the caller has already evaluated its guard,
    e.g. `fresh`), apply `post` to a hit (only `get_mailbox_overview` needs
    this, to stamp `connection`), else forward to ewsd — turning a bare
    `daemon_unavailable` into `backend_unavailable` when the mirror was
    unreachable too."""
    mirror_error = None
    if cache_call is not None:
        try:
            hit = await _try(ctx, cache_call)
        except _MirrorDown as down:
            hit, mirror_error = None, str(down)
        if hit is not None:
            if post is not None:
                post(hit)
            return hit
    return await _forward_after(ctx, name, kw, mirror_error)


async def list_folders(ctx: Context, **kw) -> dict[str, Any]:
    cache_call = None
    if not kw.get("fresh") and kw.get("parent") is None:
        cache_call = lambda: cache_reads.list_folders(
            ctx, int(kw.get("depth", 2)), bool(kw.get("include_empty", True)))
    return await _cache_then_forward(ctx, "list_folders", kw, cache_call)


async def search_messages(ctx: Context, **kw) -> dict[str, Any]:
    """Store-only for keyword mode — never forwarded: the mirror is the whole
    mailbox and ewsd has no live keyword search path to fall back to.
    mode='semantic' is forwarded to ewsd: the MCP never holds the Gemini key
    (``ctx.semantic`` is daemon-only), so it cannot run the hybrid itself."""
    if kw.get("mode", "keyword") == "semantic":
        return await _forward(ctx, "search_messages", kw)
    sender = cache_reads.validate_search_args(kw.get("sender"), kw.get("from_"))
    try:
        return await cache_reads.search_messages(
            ctx, folder=kw.get("folder"), query=kw.get("query"), sender=sender,
            subject=kw.get("subject"), since=kw.get("since"), until=kw.get("until"),
            is_unread=kw.get("is_unread"), has_attachments=kw.get("has_attachments"),
            offset=int(kw.get("offset", 0)), limit=int(kw.get("limit", 20)),
            archived=kw.get("archived", "any"))
    except (psycopg.Error, RuntimeError) as exc:
        raise ToolError("backend_unavailable", f"Postgres unreachable ({exc})",
                         hint="Check DATABASE_URL.", retry_after_s=15) from exc


async def get_message(ctx: Context, **kw) -> dict[str, Any]:
    cache_call = None
    if not kw.get("fresh") and not kw.get("include_html"):
        cache_call = lambda: cache_reads.get_message(
            ctx, kw["id"], kw.get("format", "full"))
    return await _cache_then_forward(ctx, "get_message", kw, cache_call)


async def get_thread(ctx: Context, **kw) -> dict[str, Any]:
    """Store-only: every mail folder is mirrored, so a miss never forwards
    — it means the seed is in an excluded folder or not synced yet."""
    try:
        hit = await cache_reads.get_thread(
            ctx, kw["id"], int(kw.get("limit", 20)), int(kw.get("offset", 0)))
    except (psycopg.Error, RuntimeError) as exc:
        raise ToolError("backend_unavailable", f"Postgres unreachable ({exc})",
                        hint="Check DATABASE_URL.", retry_after_s=15) from exc
    if hit is not None:
        return hit
    raise ToolError(
        "not_found",
        "That message is not in the mirror (excluded folder or not synced yet).",
        hint="Use get_message with fresh=true, or wait for the next sync cycle.",
    )


async def get_mailbox_overview(ctx: Context, **kw) -> dict[str, Any]:
    cache_call = None
    if not kw.get("fresh"):
        cache_call = lambda: cache_reads.overview(
            ctx, int(kw.get("horizon_days", 1)))

    def _mark_via_ewsd(hit: dict[str, Any]) -> None:
        hit["connection"] = "via-ewsd"

    return await _cache_then_forward(ctx, "get_mailbox_overview", kw, cache_call,
                                     post=_mark_via_ewsd)


async def list_tasks(ctx: Context, **kw) -> dict[str, Any]:
    cache_call = None
    if not kw.get("fresh"):
        cache_call = lambda: cache_reads.list_tasks(
            ctx, bool(kw.get("include_completed", False)), int(kw.get("offset", 0)),
            int(kw.get("limit", 25)))
    return await _cache_then_forward(ctx, "list_tasks", kw, cache_call)


async def waiting_on(ctx: Context, **kw) -> dict[str, Any]:
    try:
        return await _waiting_on(ctx, **kw)
    except (psycopg.Error, RuntimeError) as exc:
        raise ToolError("backend_unavailable",
                         f"Postgres unreachable ({exc})",
                         hint="Check DATABASE_URL.", retry_after_s=15) from exc


_ARCHIVE_RUNNER_KEYS_TO_DROP = ("state_counts", "embedding_backlog",
                               "blob_store_bytes", "free_gb", "policy",
                               "semantic_enabled")


async def archive_status(ctx: Context, **kw) -> dict[str, Any]:
    """Every number in the core body lives in Postgres, so the MCP answers
    it locally even while ewsd is down — which is exactly when you want to
    ask. `blob_store_bytes`/`free_gb` are NOT computed here: they live under
    ewsd's DATA_DIR, which may not even be the same disk as the MCP's own
    container, so this handler never touches its own filesystem. When ewsd
    is reachable those two numbers (and its runner block) are copied
    verbatim out of its `GET /v1/status` `archive` block; when ewsd is down
    they are simply omitted, with `disk_stats` noting why. The import is
    deliberately inside the function: tools/archive.py pulls in
    ewsmcp.archive.files, and keeping the MCP's module graph free of the
    archive package at import time keeps test_no_lazy_imports honest about
    what the MCP touches (it imports no exchangelib either way)."""
    from ..tools.archive import _archive_status_core
    try:
        out = await _archive_status_core(ctx)
    except (psycopg.Error, RuntimeError) as exc:
        raise ToolError("backend_unavailable", f"Postgres unreachable ({exc})",
                         hint="Check DATABASE_URL.", retry_after_s=15) from exc
    try:
        status = await ctx.daemon.status()
    except ToolError:
        out["disk_stats"] = "unavailable — ewsd unreachable"
        out["policy_source"] = ("mcp defaults — ewsd unreachable; the policy "
                                "ewsd actually runs may differ")
        return out
    archive_block = status.get("archive") or {}
    for key in ("blob_store_bytes", "free_gb"):
        if key in archive_block:
            out[key] = archive_block[key]
    # The policy and the delete switch are ewsd's configuration, not this
    # process's: its ARCHIVE_* environment is the one that counts.
    if "policy" in archive_block:
        out["policy"] = archive_block["policy"]
        out["policy_source"] = "ewsd"
    if "delete_enabled" in archive_block:
        out["delete_enabled"] = bool(archive_block["delete_enabled"])
    # Likewise GEMINI_API_KEY lives on ewsd only, so the MCP's own
    # semantic_enabled() is always false and would misreport the service.
    if "semantic_enabled" in archive_block:
        out["semantic_enabled"] = bool(archive_block["semantic_enabled"])
    runner_keys = {k: v for k, v in archive_block.items()
                   if k not in _ARCHIVE_RUNNER_KEYS_TO_DROP}
    if runner_keys:
        out["runner"] = runner_keys
    return out


async def get_server_status(ctx: Context, **kw) -> dict[str, Any]:
    cache_block: dict[str, Any] = {"ready": ctx.cache is not None}
    try:
        cache_block.update(ctx.cache.stats())
    except Exception as exc:  # noqa: BLE001
        cache_block["error"] = str(exc)
    try:
        daemon: dict[str, Any] = {"reachable": True, **(await ctx.daemon.status())}
    except ToolError as err:
        daemon = {"reachable": False, "error": err.message}
    return {
        "ok": True, "version": __version__, "process": "ewsmcp",
        "uptime_s": int(time.time() - ctx.started_at),
        "tier": ctx.settings.ews_capability_tier,
        "tools": len(ctx.registry), "counters": dict(ctx.counters),
        "alias_stats": ctx.aliaser.stats(), "cache": cache_block, "daemon": daemon,
    }


HANDLERS = {
    "list_folders": list_folders, "search_messages": search_messages,
    "get_message": get_message, "get_thread": get_thread,
    "get_mailbox_overview": get_mailbox_overview, "list_tasks": list_tasks,
    "waiting_on": waiting_on, "get_server_status": get_server_status,
    "archive_status": archive_status,
}
