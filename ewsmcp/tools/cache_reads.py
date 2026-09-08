"""Cache-reads: the "serve from mirror" half of the read-class handlers.

Extracted out of ``mail_read.py`` / ``tasks.py`` so both the daemon's
`ToolSpec` handlers and the thin MCP (Task 8) can hit the same mirror
logic. ``search_messages`` and ``get_thread`` answer only from the store:
they raise ``ToolError`` on a bad folder rather than falling back, and let
``psycopg.Error``/``RuntimeError`` (a closed pool) propagate so the caller
can report `backend_unavailable` instead of passing a dead database off as
a miss. Every other function here returns ``None`` when the mirror cannot
answer (folder not synced, row missing, cache disabled, or an unexpected
error) — the caller then falls through to the live EWS path. Nothing here
ever imports exchangelib or touches the gateway.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg

from .. import shared
from ..archive import files
from ..dates import parse_when
from ..dto import envelope
from ..errors import ToolError
from .base import Context

logger = logging.getLogger(__name__)


def _stamp(result: dict[str, Any], source: str,
           as_of_ts: int | None = None) -> dict[str, Any]:
    result["source"] = source
    if source == "cache" and as_of_ts:
        result["as_of"] = datetime.fromtimestamp(
            as_of_ts, tz=UTC).isoformat(timespec="seconds")
    return result


def _row_body(ctx: Context, row: Any) -> str:
    """The cleaned body exactly as ``bodyclean`` produced it at sync time."""
    return row["body_clean"] or ""


def _row_card(ctx: Context, row: Any) -> dict[str, Any]:
    """Mirror row → the same MsgCard shape the live path emits."""
    body = _row_body(ctx, row)
    card: dict[str, Any] = {
        "id": ctx.aliaser.alias_for(row["ews_id"], "m",
                                    changekey=row["changekey"],
                                    internet_message_id=row["internet_message_id"]),
        "from": (f"{row['sender_name']} <{row['sender_email']}>"
                 if row["sender_name"] and row["sender_name"] != row["sender_email"]
                 else (row["sender_email"] or "")),
        "subject": row["subject"] or "",
        "date": row["date_iso"],
        "snippet": body[:200],
    }
    if row["conversation_id"]:
        card["thread"] = ctx.aliaser.alias_for(row["conversation_id"], "t")
    if not row["is_read"]:
        card["unread"] = True
    if row["has_attachments"]:
        card["attach"] = True
    if (row["importance"] or "").lower() == "high":
        card["importance"] = "high"
    if row["archive_state"] != "live":
        card["archive_state"] = row["archive_state"]
    return card


def _row_full(ctx: Context, row: Any) -> dict[str, Any]:
    full = _row_card(ctx, row)
    full.pop("snippet", None)
    try:
        full["to"] = json.loads(row["to_json"] or "[]")
    except ValueError:
        full["to"] = []
    body = _row_body(ctx, row)
    limit = int(ctx.settings.body_max_chars)
    full["body"] = body[:limit]
    if len(body) > limit:
        full["body_truncated"] = True
    if row["internet_message_id"]:
        full["internet_message_id"] = row["internet_message_id"]
    try:
        cats = json.loads(row["categories_json"] or "[]")
    except ValueError:
        cats = []
    if cats:
        full["categories"] = cats
    if row["has_attachments"]:
        if row["archive_state"] != "live":
            # Archived mail has no live item to re-fetch — the inventory
            # lives in ews.attachments, not behind fresh=true.
            atts = ctx.cache.attachments_for(row["ews_id"])
            full["attachments"] = [{
                "name": a["name"], "size": a["size"],
                "content_type": a["content_type"],
                "downloadable": bool(a["sha256"]),
            } for a in atts]
        else:
            try:
                inv = json.loads(row["attachments_json"] or "[]")
            except ValueError:
                inv = []
            if inv:
                full["attachments"] = inv
            else:
                full["attachments_hint"] = ("inventory not synced yet — call again "
                                            "with fresh=true, or get_attachment")
    return full


def _thread_from_cache(ctx: Context, raw_id: str, limit: int,
                       offset: int) -> tuple[dict[str, Any], int | None] | None:
    """Local conversation_id join — sync helper, runs on a worker thread."""
    seed = ctx.cache.get_message(raw_id)
    if seed is None or not seed["conversation_id"]:
        return None
    rows = ctx.cache.thread(seed["conversation_id"])
    if not rows:
        return None
    total = len(rows)
    hi = max(0, total - offset)
    lo = max(0, hi - limit)
    window = rows[lo:hi]
    next_offset = offset + len(window) if lo > 0 else None
    counts: dict[str, int] = {}
    for r in rows:
        who = r["sender_email"] or "unknown"
        counts[who] = counts.get(who, 0) + 1
    entries: list[dict[str, Any]] = []
    for r in window:
        entry: dict[str, Any] = {
            "id": ctx.aliaser.alias_for(r["ews_id"], "m",
                                        internet_message_id=r["internet_message_id"]),
            "from": r["sender_email"] or "unknown",
            "date": r["date_iso"],
            "body": _row_body(ctx, r)[:1500],
        }
        if r["has_attachments"]:
            entry["attach"] = True
        entries.append(entry)
    participants = [
        {"name_or_email": who, "msgs": n}
        for who, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    marks = ctx.cache.watermarks()
    seen = [marks[f"item:{r['folder_id']}"] for r in rows
            if f"item:{r['folder_id']}" in marks]
    as_of = min(seen) if seen else None
    return ({
        "ok": True,
        "thread_id": ctx.aliaser.alias_for(seed["conversation_id"], "t"),
        "subject": seed["subject"] or "",
        "participants": participants,
        "items": entries,
        "count": len(entries),
        "total_available": total,
        "next_offset": next_offset,
    }, as_of)


def _excluded_wks(ctx: Context) -> set[str]:
    raw = getattr(ctx.settings, "ews_mirror_exclude", "") or ""
    return {f"f:{part.strip().lower()}" for part in raw.split(",") if part.strip()}


async def resolve_folder_id(ctx: Context, folder_ref: str) -> str:
    """well-known alias (f:inbox / inbox) | folder alias (f7) | path | raw
    EWS id → the folder's EWS id, resolved against ``ews.folders``.

    Raises ToolError("validation") for a folder excluded from the mirror,
    ToolError("upstream_unavailable") when the hierarchy lane has not synced
    ANY folders yet (cold boot — degrading, not a claim that the folder
    itself is wrong), and ToolError("not_found") when the hierarchy IS
    populated but nothing matches.
    """
    ref = (folder_ref or "").strip()
    rows = await asyncio.to_thread(ctx.cache.folder_rows)
    if not rows:
        raise ToolError(
            "upstream_unavailable", "folder hierarchy not synced yet",
            hint="ewsd syncs the folder tree shortly after boot; check "
                 "get_server_status", retry_after_s=30)
    wk = ref.lower() if ref.lower().startswith("f:") else f"f:{ref.lower()}"
    row = next((r for r in rows if r["wk"] == wk), None)
    if row is None:
        try:
            raw = ctx.aliaser.resolve(ref)
        except KeyError as exc:
            raise ToolError("validation", str(exc.args[0] if exc.args else exc))
        row = next((r for r in rows if r["ews_id"] == raw), None)
    if row is None:
        row = next((r for r in rows
                    if (r["path"] or "").lower() == ref.lower()), None)
    if row is None:
        raise ToolError(
            "not_found", f"No mirrored folder matches {folder_ref!r}.",
            hint="Call list_folders and pass one of its ids, paths or wk aliases.")
    if (row["wk"] or "") in _excluded_wks(ctx):
        raise ToolError(
            "validation",
            f"{folder_ref!r} is not mirrored (EWS_MIRROR_EXCLUDE="
            f"{ctx.settings.ews_mirror_exclude}).",
            hint="Search a mirrored folder, or omit `folder` to search all of them.")
    return row["ews_id"]


def folder_watermark(ctx: Context, folder_id: str) -> int | None:
    return None if ctx.cache is None else ctx.cache.watermark(f"item:{folder_id}")


def wk_watermark(ctx: Context, wk: str) -> int | None:
    """Watermark of a well-known folder, or None when it is not synced."""
    if ctx.cache is None:
        return None
    folder_id = ctx.cache.folder_id_for_wk(wk)
    return None if folder_id is None else ctx.cache.watermark(f"item:{folder_id}")


def validate_search_args(sender: str | None, from_: str | None) -> str | None:
    """Resolve `sender`/`from_`. Phase 1.5 removed the AQS-exclusivity rule:
    `query` is full-text over the mirror and combines freely with the
    structured filters."""
    if sender and from_:
        raise ToolError("validation",
                        "pass `sender` only — `from_` is its deprecated alias.")
    return sender or from_


async def list_folders(ctx: Context, depth: int,
                       include_empty: bool) -> dict[str, Any] | None:
    if ctx.cache is None:
        return None
    try:
        folder_rows = await asyncio.to_thread(ctx.cache.folder_rows)
    except Exception as exc:  # noqa: BLE001 - mirror error → live fallback
        logger.warning("cache list_folders failed (%s) — live", exc)
        return None
    if not folder_rows:
        return None
    archived = await asyncio.to_thread(ctx.cache.archived_counts_by_folder)
    rows = []
    for r in folder_rows:
        if (r["path"] or "").count("/") + 1 > depth:
            continue
        if not include_empty and not r["total"]:
            continue
        row: dict[str, Any] = {
            "id": ctx.aliaser.alias_for(r["ews_id"], "f"),
            "name": r["name"], "path": r["path"],
            "total": r["total"], "unread": r["unread"],
            "children": r["children"],
        }
        if r["wk"]:
            row["wk"] = r["wk"]
        row["archived"] = archived.get(r["ews_id"], 0)
        rows.append(row)
    # The hierarchy lane's own watermark — `events` belongs to the slow
    # (calendar/tasks) lane and says nothing about when these rows were read.
    as_of = await asyncio.to_thread(ctx.cache.watermark, "folders")
    return _stamp(envelope(rows, total_available=len(rows), offset=0),
                  "cache", as_of)


_ARCHIVED_MODES = ("any", "only", "exclude")


def _semantic_rows(ctx: Context, query: str, archived: str, offset: int,
                   limit: int, filters: dict[str, Any]
                   ) -> tuple[list[Any], int, dict[str, Any]]:
    """Hybrid when an index exists, keyword otherwise — never an error.

    A missing key or a dead Gemini must degrade the ANSWER, not remove the
    tool: the model asked for meaning and gets words, clearly labelled."""
    if ctx.semantic is None:
        rows, total = ctx.cache.search_messages(
            text=query, archived=archived, offset=offset, limit=limit, **filters)
        return rows, total, {"degraded": True,
                             "reason": "semantic search is not configured "
                                       "(GEMINI_API_KEY unset) — keyword results"}
    rows, degraded = ctx.semantic.hybrid_search(
        query, limit=limit, offset=offset, archived=archived, **filters)
    meta = {"degraded": True,
            "reason": "the embedding service failed — keyword results"} \
        if degraded else {"degraded": False, "mode": "hybrid_rrf"}
    return rows, len(rows) + offset, meta


async def search_messages(ctx: Context, *, folder: str | None,
                          query: str | None, sender: str | None,
                          subject: str | None, since: str | None,
                          until: str | None, is_unread: bool | None,
                          has_attachments: bool | None, offset: int,
                          limit: int, archived: str = "any",
                          mode: str = "keyword",
                          include_calendar_items: bool = False
                          ) -> dict[str, Any] | None:
    """Store-only search over the mirror. None only when there is no cache
    at all (``ctx.cache is None``) — the caller then goes straight to live
    EWS, same as every other cache_reads helper. Otherwise raises ToolError
    on a bad `folder` or `archived`; psycopg errors propagate (the caller
    maps them to backend_unavailable). `mode="semantic"` runs the hybrid
    (keyword + embedding, RRF-fused) and degrades to keyword with
    `meta.degraded` when the embedder is missing or fails."""
    if archived not in _ARCHIVED_MODES:
        raise ToolError("validation",
                        "archived must be 'any', 'only' or 'exclude'")
    if ctx.cache is None:
        return None
    folder_ids = [await resolve_folder_id(ctx, folder)] if folder else None
    marks = ctx.cache.watermarks()
    keys = ([f"item:{fid}" for fid in folder_ids] if folder_ids
            else [k for k in marks if k.startswith("item:")])
    seen = [marks[k] for k in keys if k in marks]
    as_of = min(seen) if seen else None
    tz = ctx.settings.ews_tz
    since_ts = int(parse_when(since, "since", tz).timestamp()) if since else None
    until_ts = int(parse_when(until, "until", tz).timestamp()) if until else None
    filters: dict[str, Any] = dict(
        sender=sender, subject=subject, since_ts=since_ts, until_ts=until_ts,
        is_unread=is_unread, has_attachments=has_attachments,
        folder_ids=folder_ids, include_calendar_items=include_calendar_items,
    )
    meta: dict[str, Any] | None = None
    if mode == "semantic":
        rows, total, meta = await asyncio.to_thread(
            _semantic_rows, ctx, query or "", archived, offset, limit, filters)
    else:
        rows, total = await asyncio.to_thread(
            ctx.cache.search_messages, text=query, archived=archived,
            offset=offset, limit=limit, **filters)
    cards = await asyncio.to_thread(lambda: [_row_card(ctx, r) for r in rows])
    out = _stamp(envelope(cards, total_available=total, offset=offset),
                 "cache", as_of)
    if meta:
        out["meta"] = meta
    return out


async def get_message(ctx: Context, raw_id: str,
                      format: str) -> dict[str, Any] | None:
    if ctx.cache is None:
        return None
    try:
        row = await asyncio.to_thread(ctx.cache.get_message, raw_id)
    except Exception as exc:  # noqa: BLE001 - mirror error → live fallback
        logger.warning("cache get_message failed (%s) — live", exc)
        return None
    if row is None:
        return None
    as_of = folder_watermark(ctx, row["folder_id"])
    message = _row_card(ctx, row) if format == "concise" else _row_full(ctx, row)
    return _stamp({"ok": True, "message": message}, "cache", as_of)


async def get_thread(ctx: Context, raw_id: str, limit: int,
                     offset: int) -> dict[str, Any] | None:
    """Store-only, like ``search_messages``: None means a genuine miss (the
    seed is not mirrored), which the callers turn into `not_found`. A dead
    Postgres must NOT look like a miss, so psycopg/pool errors propagate for
    the callers to map to `backend_unavailable`."""
    if ctx.cache is None:
        return None
    try:
        cached = await asyncio.to_thread(
            _thread_from_cache, ctx, raw_id, limit, offset)
    except (psycopg.Error, RuntimeError):  # pool closed/unreachable — never a miss
        raise
    except Exception as exc:  # noqa: BLE001 - bad row/shape → clean miss
        logger.warning("cache get_thread failed (%s)", exc)
        return None
    if cached is None:
        return None
    payload, as_of = cached
    return _stamp(payload, "cache", as_of)


async def overview(ctx: Context, horizon_days: int) -> dict[str, Any] | None:
    if ctx.cache is None:
        return None
    as_of = wk_watermark(ctx, "f:inbox")
    if not as_of:
        return None
    tz = ctx.settings.ews_tz
    now = datetime.now(ZoneInfo(tz))
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=horizon_days)
    try:
        def read_mirror():
            unread_total, rows = ctx.cache.unread_page(limit=10)
            events = ctx.cache.events_window(
                int(day_start.timestamp()), int(day_end.timestamp()), limit=10)
            return unread_total, rows, events

        unread_total, rows, event_rows = await asyncio.to_thread(read_mirror)
        cards = await asyncio.to_thread(lambda: [_row_card(ctx, r) for r in rows])
        events = [{
            "id": ctx.aliaser.alias_for(r["ews_id"], "e"),
            "subject": r["subject"],
            "start": r["start_iso"],
            "end": r["end_iso"],
            **({"location": r["location"]} if r["location"] else {}),
            **({"recurring": True} if r["is_recurring"] else {}),
        } for r in event_rows]
        return _stamp({
            "ok": True,
            "generated_at": now.isoformat(timespec="seconds"),
            "unread_total": unread_total,
            "recent_unread": cards,
            "today_events": events,
            "connection": ctx.manager.state if ctx.manager else "unmanaged",
        }, "cache", as_of)
    except Exception as exc:  # noqa: BLE001 - mirror error → live fallback
        logger.warning("cache overview failed (%s) — live", exc)
        return None


async def list_tasks(ctx: Context, include_completed: bool, offset: int,
                     limit: int) -> dict[str, Any] | None:
    if ctx.cache is None or not ctx.cache.watermark("item:tasks"):
        return None
    try:
        rows, total = await asyncio.to_thread(
            ctx.cache.task_rows, include_completed, offset, limit)
        items = [_task_row_dto(ctx, r) for r in rows]
        out = envelope(items, total, offset)
        out["source"] = "cache"
        return out
    except Exception as exc:  # noqa: BLE001 - mirror error → live fallback
        logger.warning("cache list_tasks failed (%s) — live", exc)
        return None


def _task_row_dto(ctx: Context, row: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": ctx.aliaser.alias_for(row["ews_id"], "k"),
        "subject": row["subject"] or "",
        "complete": bool(row["is_complete"]),
    }
    if row["due_iso"]:
        out["due"] = row["due_iso"]
    if row["status"]:
        out["status"] = row["status"]
    return out


# --------------------------------------------------------------------------
# Archived attachments — served from the blob store, never from Exchange
# --------------------------------------------------------------------------

_TEXTY_TYPES = ("text/",)
_TEXTY_SUFFIXES = (".txt", ".csv", ".md", ".log", ".json")


def _pick_row(rows: list[dict[str, Any]], selector: str | None) -> dict[str, Any]:
    if selector is None:
        if len(rows) != 1:
            raise ToolError(
                "validation",
                f"this message has {len(rows)} attachments — pass `attachment` "
                "with a name or a zero-based index as a string",
                hint="Names: " + ", ".join(str(r["name"]) for r in rows[:10]))
        return rows[0]
    if selector.isdigit() and int(selector) < len(rows):
        return rows[int(selector)]
    for row in rows:
        if (row["name"] or "").lower() == selector.lower():
            return row
    raise ToolError("not_found", f"No attachment named {selector!r} on this message.",
                    hint="Names: " + ", ".join(str(r["name"]) for r in rows[:10]))


async def attachment_from_archive(ctx: Context, raw_id: str,
                                  attachment: str | None,
                                  mode: str) -> dict[str, Any] | None:
    """Serve an attachment of ARCHIVED mail from the blob store, or None
    (either a live message, or no cache at all — the caller then falls
    through to the live Exchange path)."""
    if ctx.cache is None:
        return None
    row = await asyncio.to_thread(ctx.cache.get_message, raw_id)
    if row is None or row["archive_state"] == "live":
        return None
    rows = await asyncio.to_thread(ctx.cache.attachments_for, row["ews_id"])
    if not rows:
        raise ToolError("not_found", "This archived message has no attachments.")
    att = _pick_row(rows, attachment)
    name = att["name"] or "attachment"
    out: dict[str, Any] = {"ok": True, "name": name, "size_bytes": att["size"],
                           "content_type": att["content_type"],
                           "source": "archive"}
    if not att["sha256"]:
        # A nested message: it exists only inside the raw MIME.
        out["mode"] = "info"
        out["hint"] = ("Nested message attachment — it lives inside the raw "
                       "MIME. Call get_raw_message for the original .eml.")
        return out
    path = files.blob_path(ctx.settings.data_dir, att["sha256"])
    if not path.is_file():
        raise ToolError("not_found", f"The archived blob is missing at {path}.",
                        hint="The next verify pass re-captures this message.")
    texty = (att["content_type"] or "").lower().startswith(_TEXTY_TYPES) or \
        name.lower().endswith(_TEXTY_SUFFIXES)
    chosen = mode
    if mode == "auto":
        chosen = "text" if texty else "info"
        if chosen == "info":
            out["hint"] = ("Binary attachment — metadata only. Call again with "
                           "mode='save' to write it to disk.")
    if chosen == "info":
        out["mode"] = "info"
        return out
    data = await asyncio.to_thread(path.read_bytes)
    if chosen == "text":
        text = data.decode("utf-8", errors="replace")
        out["mode"] = "text"
        out["text"] = text[:20_000]
        if len(text) > 20_000:
            out["truncated"] = True
        return out
    safe = shared.safe_name(name)
    dest = Path(ctx.settings.data_dir) / "attachments"
    dest.mkdir(parents=True, exist_ok=True)
    saved = dest / safe
    await asyncio.to_thread(saved.write_bytes, data)
    out["mode"] = "save"
    out["saved_path"] = str(saved)
    try:
        published = shared.publish(ctx.settings.shared_dir, saved)
    except OSError as exc:
        logger.warning("could not publish %s to the shared space: %s", safe, exc)
        published = None
    if published:
        out["shared_name"] = published
        out["shared_path"] = str(Path(ctx.settings.shared_dir) / published)
    return out
