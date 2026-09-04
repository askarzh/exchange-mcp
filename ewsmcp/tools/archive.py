"""Tool pack: archive + semantic search.

`archive_run` is the only tool here with teeth. It is class `destructive`
(minimum tier `full`) and `dry_run=false` is two-phase confirmed, so removing
mail from Exchange takes a deliberate second model decision on top of
ARCHIVE_DELETE_ENABLED and the verified-plus-grace rail inside the deleter.

`get_raw_message` never returns bytes: a .eml is exactly the kind of payload
that must not travel through the model's context, so it mints a single-use
capability URL the same way `create_upload_link` does in the other direction.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from .. import downloads
from ..archive import files
from ..archive.policy import ArchivePolicy
from ..dto import envelope
from ..errors import ToolError
from .base import Context, ToolSpec
from .cache_reads import _row_card

logger = logging.getLogger(__name__)

KINDS = ("capture", "verify", "delete", "embed", "all")
ARCHIVED_MODES = ("any", "only", "exclude")


def _require_cache(ctx: Context) -> Any:
    if ctx.cache is None:
        raise ToolError("backend_unavailable", "the Postgres mirror is not available",
                        hint="Check DATABASE_URL.", retry_after_s=15)
    return ctx.cache


# --------------------------------------------------------------------------
# archive_run
# --------------------------------------------------------------------------


async def _archive_run(ctx: Context, *, dry_run: bool = True, kind: str = "all",
                       before: str | None = None,
                       folders: list[str] | None = None) -> dict[str, Any]:
    if kind not in KINDS:
        raise ToolError("validation",
                        f"kind must be one of {', '.join(KINDS)} (got {kind!r})")
    if ctx.archive is None:
        raise ToolError("upstream_unavailable",
                        "the archive runner is not started on this server",
                        hint="Only ewsd runs the archive; check /readyz.",
                        retry_after_s=30)
    result = await ctx.archive.run_once(kind=kind, dry_run=bool(dry_run),
                                        before=before, folders=folders)
    if not result.get("ok", True) and result.get("blocked") == "cycle in progress":
        raise ToolError("upstream_unavailable",
                        "an archive cycle is already in progress",
                        hint="Retry shortly — only one archive pass runs at a time.",
                        retry_after_s=result.get("retry_after_s", 30))
    return result


# --------------------------------------------------------------------------
# archive_status
# --------------------------------------------------------------------------


async def _archive_status(ctx: Context) -> dict[str, Any]:
    cache = _require_cache(ctx)

    def read() -> dict[str, Any]:
        return {
            "states": cache.archive_state_counts(),
            "recent_runs": [
                {"id": r["id"], "kind": r["kind"], "dry_run": bool(r["dry_run"]),
                 "started_at": r["started_at"], "finished_at": r["finished_at"],
                 "captured": r["captured"], "verified": r["verified"],
                 "deleted": r["deleted"], "failed": r["failed"],
                 "error": r["error"]}
                for r in cache.recent_runs(5)
            ],
            "embedding": {"backlog": cache.embedding_backlog(),
                          "embedded": cache.embedded_count()},
        }

    out = await asyncio.to_thread(read)
    out["blob_store_bytes"] = await asyncio.to_thread(
        files.blob_store_bytes, ctx.settings.data_dir)
    out["free_gb"] = round(await asyncio.to_thread(
        files.free_gb, ctx.settings.data_dir), 2)
    policy = ArchivePolicy.from_settings(ctx.settings)
    out["policy"] = {"folders": list(policy.folders),
                     "after_days": policy.after_days,
                     "grace_days": policy.grace_days,
                     "exclude_categories": list(policy.exclude_categories),
                     "max_delete_per_run": policy.max_delete_per_run,
                     "min_free_gb": policy.min_free_gb}
    out["delete_enabled"] = policy.delete_enabled
    out["semantic_enabled"] = ctx.settings.semantic_enabled()
    if ctx.archive is not None:
        out["runner"] = ctx.archive.status()
    out["ok"] = True
    return out


# --------------------------------------------------------------------------
# get_raw_message
# --------------------------------------------------------------------------


async def _get_raw_message(ctx: Context, *, id: str,
                           ttl_minutes: int = 15) -> dict[str, Any]:
    cache = _require_cache(ctx)
    row = await asyncio.to_thread(cache.get_message, id)
    if row is None:
        raise ToolError("not_found", f"No mirrored message matches {id!r}.",
                        hint="Re-run search_messages for a fresh id.")
    if not row["mime_path"]:
        raise ToolError(
            "not_found",
            "That message is not archived yet, so there is no raw MIME to serve.",
            hint="Raw MIME exists only for captured/verified/deleted mail — "
                 "check archive_status, or use get_message for the text.")
    path = Path(row["mime_path"])
    if not path.is_file():
        raise ToolError(
            "not_found", f"The archived MIME file is missing at {path}.",
            hint="The next verify pass will reset this message to live and "
                 "re-capture it.")
    ttl = max(1, min(int(ttl_minutes), 1440))
    subject = (row["subject"] or "message").strip() or "message"
    name = f"{subject[:60]}.eml"
    rec = downloads.mint(ctx.settings.data_dir, path=str(path), name=name,
                         content_type="message/rfc822", ttl_seconds=ttl * 60)
    await asyncio.to_thread(downloads.sweep, ctx.settings.data_dir)
    base = (getattr(ctx.settings, "external_url", "") or "").rstrip("/")
    url = f"{base}/download/{rec['token']}" if base else f"/download/{rec['token']}"
    return {
        "ok": True,
        "download_url": url,
        "name": rec["name"],
        "content_type": "message/rfc822",
        "size_bytes": path.stat().st_size,
        "sha256": row["mime_sha256"],
        "archive_state": row["archive_state"],
        "expires_in_minutes": ttl,
        "curl": f'curl -o {rec["name"]!r} "{url}"',
        "note": "single use — the link is spent by the first successful GET",
    }


# --------------------------------------------------------------------------
# find_similar
# --------------------------------------------------------------------------


async def _find_similar(ctx: Context, *, id: str | None = None,
                        text: str | None = None, limit: int = 10,
                        archived: str = "any") -> dict[str, Any]:
    if bool(id) == bool(text):
        raise ToolError("validation",
                        "pass exactly one of `id` (find mail like this message) "
                        "or `text` (find mail like this description)")
    if archived not in ARCHIVED_MODES:
        raise ToolError("validation",
                        f"archived must be one of {', '.join(ARCHIVED_MODES)}")
    if ctx.semantic is None:
        raise ToolError(
            "validation", "semantic search is not configured on this server",
            hint="Set GEMINI_API_KEY on ewsd; keyword search "
                 "(search_messages) works regardless.")
    cache = _require_cache(ctx)
    limit = max(1, min(int(limit), 50))

    def query() -> list[dict[str, Any]]:
        if id:
            return ctx.semantic.similar_to_message(id, limit=limit,
                                                   archived=archived)
        hits = ctx.semantic.vector_ids(text or "", limit=limit,
                                       archived=archived)
        by_id = cache.messages_by_ids([i for i, _d in hits])
        out = []
        for ews_id, dist in hits:
            row = by_id.get(ews_id)
            if row is not None:
                row = dict(row)
                row["similarity"] = round(1.0 - float(dist), 4)
                out.append(row)
        return out

    try:
        rows = await asyncio.to_thread(query)
    except ToolError:
        raise
    except Exception as exc:  # noqa: BLE001 - a dead embedder is not a 502
        raise ToolError(
            "upstream_unavailable", f"the embedding service failed: {exc}",
            hint="Keyword search (search_messages) is unaffected.",
            retry_after_s=60) from exc
    cards = []
    for row in rows:
        card = _row_card(ctx, row)
        card["similarity"] = row["similarity"]
        cards.append(card)
    return envelope(cards, total_available=len(cards), offset=0)


# --------------------------------------------------------------------------
# Specs
# --------------------------------------------------------------------------


def _schema(properties: dict[str, Any],
            required: list[str] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "additionalProperties": False,
                              "properties": properties}
    if required:
        schema["required"] = required
    return schema


TOOLS: list[ToolSpec] = [
    ToolSpec(
        name="archive_run",
        description=(
            "Run one archive pass. dry_run=true (the default) only REPORTS: "
            "how many messages match the policy and which would go, touching "
            "neither Exchange nor disk — always start there. dry_run=false "
            "actually captures (raw MIME + attachment blobs to the server's "
            "data dir), verifies, embeds, and — only when "
            "ARCHIVE_DELETE_ENABLED=true and a message is verified, older than "
            "the cutoff and past the grace period — hard-deletes it from "
            "Exchange, capped per run. dry_run=false is two-phase confirmed. "
            "`before` and `folders` narrow this pass only."
        ),
        side_effect_class="destructive",
        requires_ews=True,
        input_schema=_schema({
            "dry_run": {
                "type": "boolean", "default": True,
                "description": "true reports without changing anything.",
            },
            "kind": {
                "type": "string", "enum": list(KINDS), "default": "all",
                "description": "Which workers to run in this pass.",
            },
            "before": {
                "type": "string",
                "description": "Override the age cutoff for this pass: "
                               "YYYY-MM-DD, an ISO datetime, or '-Nd'.",
            },
            "folders": {
                "type": "array", "items": {"type": "string"},
                "description": "Override ARCHIVE_FOLDERS for this pass "
                               "(well-known keys, e.g. ['inbox']). Calendar, "
                               "contacts, tasks, drafts and outbox are never "
                               "archived.",
            },
        }),
        handler=_archive_run,
        confirm=lambda kw: not kw.get("dry_run", True),
    ),
    ToolSpec(
        name="archive_status",
        description=(
            "Where the archive stands: message counts per state (live, "
            "captured, verified, deleted), the last five runs with their "
            "counts and errors, blob-store size and free disk, the embedding "
            "backlog, the active policy, and whether deletion is enabled. "
            "Answered from Postgres — it works while Exchange is down."
        ),
        side_effect_class="read",
        requires_ews=False,
        input_schema=_schema({}),
        handler=_archive_status,
    ),
    ToolSpec(
        name="get_raw_message",
        description=(
            "Get the original RFC822 message of an ARCHIVED mail as a "
            "single-use download URL (the bytes never travel through the "
            "conversation). Works for captured, verified and deleted "
            "messages; live mail has no stored MIME yet. The link expires and "
            "is spent by the first successful download."
        ),
        side_effect_class="read",
        requires_ews=False,
        input_schema=_schema({
            "id": {"type": "string", "description": "Message id (m-alias or raw)."},
            "ttl_minutes": {"type": "integer", "minimum": 1, "maximum": 1440,
                            "default": 15},
        }, required=["id"]),
        handler=_get_raw_message,
    ),
    ToolSpec(
        name="find_similar",
        description=(
            "Find mail that MEANS the same thing, not mail that shares words: "
            "pass `id` to find messages like that one, or `text` to describe "
            "what you are looking for. Ranked by embedding similarity over "
            "live and archived mail alike; each card carries `similarity` "
            "(1.0 is identical). Use search_messages for exact terms, names "
            "and dates."
        ),
        side_effect_class="read",
        requires_ews=False,
        input_schema=_schema({
            "id": {"type": "string",
                   "description": "Seed message id (m-alias or raw)."},
            "text": {"type": "string",
                     "description": "Free-text description of what to find."},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50,
                      "default": 10},
            "archived": {"type": "string", "enum": list(ARCHIVED_MODES),
                         "default": "any",
                         "description": "any (default) | only | exclude."},
        }),
        handler=_find_similar,
    ),
]
