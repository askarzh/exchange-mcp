"""Cache-reads: the "serve from mirror" half of the read-class handlers.

Extracted out of ``mail_read.py`` / ``tasks.py`` so both the daemon's
`ToolSpec` handlers and the thin MCP (Task 8) can hit the same mirror
logic. Every function here returns ``None`` when the mirror cannot
answer (folder not synced, row missing, cache disabled, or an
unexpected error) — the caller then falls through to the live EWS
path. Nothing here ever imports exchangelib or touches the gateway.
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
    """Cleaned body with the sender's LEARNED signature stripped (the
    deterministic per-sender trailing-block learning from sync time)."""
    body = row["body_clean"] or ""
    if ctx.cache is not None and body:
        try:
            body = ctx.cache.strip_learned_signature(row["sender_email"], body)
        except Exception as exc:  # noqa: BLE001 - best effort, body stays uncleaned
            logger.debug("strip_learned_signature failed (%s) — body unstripped", exc)
    return body


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
                       offset: int) -> dict[str, Any] | None:
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
    return {
        "ok": True,
        "thread_id": ctx.aliaser.alias_for(seed["conversation_id"], "t"),
        "subject": seed["subject"] or "",
        "participants": participants,
        "items": entries,
        "count": len(entries),
        "total_available": total,
        "next_offset": next_offset,
    }


def folder_key(ctx: Context, folder_ref: str | None) -> str | None:
    """Map a folder argument onto a mirrored folder key, or None (→ live).

    Mirrored means ``f"item:{key}"`` is present in ``ctx.cache.watermarks()``
    — i.e. the sync engine has actually populated that folder, not merely
    that it is listed in settings.
    """
    if ctx.cache is None:
        return None
    key = (folder_ref or "f:inbox").strip().lower()
    key = key.removeprefix("f:")
    return key if f"item:{key}" in ctx.cache.watermarks() else None


def watermark(ctx: Context, key: str) -> int | None:
    return None if ctx.cache is None else ctx.cache.watermark(f"item:{key}")


def validate_search_args(sender: str | None, from_: str | None,
                         subject: str | None, since: str | None,
                         until: str | None, is_unread: bool | None,
                         has_attachments: bool | None,
                         query: str | None) -> str | None:
    """Resolve `sender`/`from_` and raise the shared 4.5 validation errors.

    Returns the effective sender (sender or from_)."""
    if sender and from_:
        raise ToolError("validation",
                        "pass `sender` only — `from_` is its deprecated alias.")
    sender = sender or from_
    structured = any(v is not None for v in
                     (sender, subject, since, until, is_unread, has_attachments))
    if query and structured:
        raise ToolError(
            "validation",
            "`query` (AQS) cannot be combined with the structured filters "
            "(sender/subject/since/until/is_unread/has_attachments) — Exchange "
            "runs them on different engines.",
            hint="Either fold everything into the AQS string (e.g. 'from:ahmed "
                 "subject:rfp received>=2026-06-01') or drop `query` and use "
                 "only structured filters.",
        )
    return sender


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
    key = folder_key(ctx, folder)
    if not key:
        return None
    as_of = watermark(ctx, key)
    if not as_of:
        return None
    tz = ctx.settings.ews_tz
    try:
        since_ts = (int(parse_when(since, "since", tz).timestamp())
                    if since else None)
        until_ts = (int(parse_when(until, "until", tz).timestamp())
                    if until else None)
        rows, total = await asyncio.to_thread(
            ctx.cache.search_messages,
            folders=[key], text=query, sender=sender,
            subject=subject, since_ts=since_ts, until_ts=until_ts,
            is_unread=is_unread, has_attachments=has_attachments,
            offset=offset, limit=limit, archived="any",
        )
        cards = await asyncio.to_thread(lambda: [_row_card(ctx, r) for r in rows])
        return _stamp(envelope(cards, total_available=total, offset=offset),
                      "cache", as_of)
    except ToolError:
        raise
    except Exception as exc:  # noqa: BLE001 - mirror error → live fallback
        logger.warning("cache search failed (%s) — falling back live", exc)
        return None


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
    as_of = watermark(ctx, row["folder"])
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
    as_of = min(filter(None, (watermark(ctx, k) for k in ("inbox", "sent"))),
                default=None)
    return _stamp(cached, "cache", as_of)


async def overview(ctx: Context, horizon_days: int) -> dict[str, Any] | None:
    if ctx.cache is None:
        return None
    as_of = watermark(ctx, "inbox")
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
