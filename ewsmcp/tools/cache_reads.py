"""Cache-reads: the "serve from mirror" half of the read-class handlers.

Extracted out of ``mail_read.py`` / ``tasks.py`` so both the daemon's
`ToolSpec` handlers and the thin MCP (Task 8) can hit the same mirror
logic. ``search_messages`` answers only from the store (it raises
``ToolError`` on a bad folder rather than falling back). Every other
function here returns ``None`` when the mirror cannot answer (folder not
synced, row missing, cache disabled, or an unexpected error) — the caller
then falls through to the live EWS path. Nothing here ever imports
exchangelib or touches the gateway.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

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
        full["attachments_hint"] = ("message has attachments — call again "
                                    "with fresh=true for the inventory, or "
                                    "get_attachment to read one")
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


def resolve_folder_id(ctx: Context, folder_ref: str) -> str:
    """well-known alias (f:inbox / inbox) | folder alias (f7) | path | raw
    EWS id → the folder's EWS id, resolved against ``ews.folders``.

    Raises ToolError("validation") for a folder excluded from the mirror and
    ToolError("not_found") when nothing matches.
    """
    ref = (folder_ref or "").strip()
    rows = ctx.cache.folder_rows()
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


def mirrored_folder_ids(ctx: Context) -> list[str]:
    """Every folder the sync engine has actually populated (an ``item:<id>``
    watermark exists). Empty means nothing has synced yet."""
    marks = ctx.cache.watermarks()
    return [k[len("item:"):] for k in marks if k.startswith("item:")]


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
        rows.append(row)
    as_of = ctx.cache.watermark("events")  # slow-lane watermark
    return _stamp(envelope(rows, total_available=len(rows), offset=0),
                  "cache", as_of)


async def search_messages(ctx: Context, *, folder: str | None,
                          query: str | None, sender: str | None,
                          subject: str | None, since: str | None,
                          until: str | None, is_unread: bool | None,
                          has_attachments: bool | None, offset: int,
                          limit: int) -> dict[str, Any] | None:
    """Store-only search over the mirror. None only when there is no cache
    at all (``ctx.cache is None``) — the caller then goes straight to live
    EWS, same as every other cache_reads helper. Otherwise raises ToolError
    on a bad `folder`; psycopg errors propagate (the caller maps them to
    backend_unavailable)."""
    if ctx.cache is None:
        return None
    folder_ids = [resolve_folder_id(ctx, folder)] if folder else None
    marks = ctx.cache.watermarks()
    keys = ([f"item:{fid}" for fid in folder_ids] if folder_ids
            else [k for k in marks if k.startswith("item:")])
    seen = [marks[k] for k in keys if k in marks]
    as_of = min(seen) if seen else None
    tz = ctx.settings.ews_tz
    since_ts = int(parse_when(since, "since", tz).timestamp()) if since else None
    until_ts = int(parse_when(until, "until", tz).timestamp()) if until else None
    rows, total = await asyncio.to_thread(
        ctx.cache.search_messages,
        folder_ids=folder_ids, text=query, sender=sender,
        subject=subject, since_ts=since_ts, until_ts=until_ts,
        is_unread=is_unread, has_attachments=has_attachments,
        offset=offset, limit=limit, archived="any",
    )
    cards = await asyncio.to_thread(lambda: [_row_card(ctx, r) for r in rows])
    return _stamp(envelope(cards, total_available=total, offset=offset),
                  "cache", as_of)


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
    if ctx.cache is None:
        return None
    try:
        cached = await asyncio.to_thread(
            _thread_from_cache, ctx, raw_id, limit, offset)
    except Exception as exc:  # noqa: BLE001 - mirror error → live fallback
        logger.warning("cache get_thread failed (%s) — live", exc)
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
