"""Background delta-sync: exchangelib's native SyncFolderItems primitives.

Owned by the ConnectionManager's warm state: the engine starts when the
connection first warms up and loops forever.

Two cadences:

* The HIERARCHY lane refreshes `ews.folders` and recomputes the set of
  folders to item-sync. exchangelib caches the whole subfolder tree on the
  account's root for the life of the ``Account`` (``Root._subfolders``), so
  walking ``msg_folder_root.children`` re-reads that cache and would never
  see a new folder, a vanished one, or fresh ``total_count`` /
  ``unread_count``. The lane therefore calls ``account.root.clear_cache()``
  and forces a re-fetch — an expensive FindFolder round trip — at most once
  every ``EWS_CACHE_HIERARCHY_SECONDS`` (default 600s), and on the very
  first cycle. It also runs BEFORE item sync, so a folder discovered by a
  refresh is mirrored in the same cycle.
* The ITEM lane runs EVERY cycle (``EWS_CACHE_SYNC_SECONDS``, default 45s)
  against the last known folder set, so mail latency does not depend on the
  hierarchy cadence.

Only mail folders are item-synced: a folder is mirrored when its
``folder_class`` is ``IPF.Note`` (or it is a well-known mail folder, whose
class exchangelib may leave unset), minus the ones EWS_MIRROR_EXCLUDE
names. Non-mail folders (Contacts, Calendar, Recipient Cache, Sharing,
Yammer Root, …) stay in ``ews.folders`` for `list_folders` but are never
handed the mail item projection. Each mirrored folder carries its own
resumable `sync_state` token keyed `item:<folder ews id>`, so a container
restart resumes instead of re-downloading. A slower lane refreshes the
expanded calendar window and the tasks folder.

Failure posture: ANY exception marks the engine degraded and is retried
next cycle. All EWS work runs on the gateway's bounded pool.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from exchangelib.folders import Messages

from ..bodyclean import clean_body
from ..dto import fmt_dt
from ..gateway.wellknown import WELL_KNOWN
from .store import CacheStore

logger = logging.getLogger(__name__)

# Fields pulled per synced message — everything a card/full DTO needs,
# never the MIME. Cleaning happens here, once, at sync time.
ITEM_FIELDS = [
    "id", "changekey", "subject", "sender", "datetime_received", "is_read",
    "has_attachments", "importance", "categories", "conversation_id",
    "message_id", "to_recipients", "text_body",
]
TASK_FIELDS = ["id", "changekey", "subject", "due_date", "is_complete", "status"]
BODY_CLEAN_MAX = 20_000
CALENDAR_WINDOW_DAYS = 14
CALENDAR_MAX_ITEMS = 200
# Rows written to the store per flush during a folder's item sync. Bounds the
# memory a first full sync of a 200k-item folder can take (see
# _sync_one_folder): without it every change is buffered until the generator
# is exhausted.
FLUSH_EVERY = 200

# EWS FolderClass of a mail folder. exchangelib exposes it as
# ``Folder.folder_class`` (the ``folder:FolderClass`` field), and every mail
# folder class it models declares ``CONTAINER_CLASS = "IPF.Note"``.
MAIL_CONTAINER_CLASS = "IPF.Note"

# Folders that hold Calendar/Contacts/Tasks items live under msg_folder_root
# too. They are mirrored by their own lanes (or not at all); running the
# mail ITEM_FIELDS projection against them would fail every cycle. Used only
# as the tie-breaker for a folder whose folder_class the server left unset.
NON_MAIL_WK = frozenset({"f:calendar", "f:contacts", "f:tasks"})


def is_mail_folder(folder: Any, wk: str | None) -> bool:
    """True when the mail ITEM_FIELDS projection is valid for this folder.

    ``folder_class`` is authoritative when the server sets it: "IPF.Note" is
    mail, and everything else — "IPF.Contact" (Contacts, Recipient Cache, GAL
    Contacts, Companies), "IPF.Appointment", "IPF.Task", "IPF.Note.OutlookHomepage",
    "IPF.Configuration" (Quick Step Settings, Conversation Action Settings) —
    is not. A folder with no class at all is only mirrored when it is a
    well-known MAIL folder: exchangelib models those (Inbox, Sent Items, …) as
    ``Messages`` subclasses, so their CONTAINER_CLASS is "IPF.Note" even when
    the instance's field is unset.
    """
    folder_class = getattr(folder, "folder_class", None)
    if folder_class:
        return folder_class == MAIL_CONTAINER_CLASS
    if isinstance(folder, Messages):
        return True
    return wk is not None and wk not in NON_MAIL_WK


def _ts(dt: Any) -> int | None:
    try:
        return int(dt.timestamp())
    except (AttributeError, OSError, OverflowError, ValueError, TypeError):
        return None


BODY_FETCH_CHUNK = 100
# What SyncFolderItems leaves empty and GetItem must supply.
HYDRATE_FIELDS = ["text_body", "to_recipients"]


def hydrate_bodies(account: Any, items: list[Any]) -> int:
    """Fill `text_body` and `to_recipients` on synced items with a bulk GetItem.

    SyncFolderItems never carries them: Exchange answers `item:TextBody` and
    `message:ToRecipients` only through GetItem, so every item that comes out
    of `sync_items` has both empty however it was projected (verified live
    against Exchange 2016, build 15.2.1748: sync → 0 chars / no recipients,
    fetch → the full text and the list). Items are handed to `Account.fetch`
    as they are (id + changekey); exchangelib chunks the call and yields, in
    order, either the fetched item or an exception for that one id — a
    failed id keeps what it had and is retried on its next change, never
    failing the folder. Returns the number of items that received a body."""
    if not items:
        return 0
    filled = 0
    for start in range(0, len(items), BODY_FETCH_CHUNK):
        batch = items[start:start + BODY_FETCH_CHUNK]
        try:
            fetched = list(account.fetch(batch, only_fields=HYDRATE_FIELDS))
        except Exception as exc:  # noqa: BLE001 - a body is never worth a folder
            logger.warning("body fetch for %d items failed: %s", len(batch), exc)
            continue
        for item, res in zip(batch, fetched):
            if isinstance(res, Exception):
                logger.debug("body fetch for %s failed: %s", getattr(item, "id", "?"), res)
                continue
            text = getattr(res, "text_body", None)
            if isinstance(text, str):
                item.text_body = text
                filled += 1
            to = getattr(res, "to_recipients", None)
            if to is not None:
                item.to_recipients = list(to)
    return filled


def recipients_json(item: Any) -> str:
    to = [
        r.email_address
        for r in (getattr(item, "to_recipients", None) or [])
        if getattr(r, "email_address", None)
    ]
    return json.dumps(to, ensure_ascii=False)


def row_from_message(item: Any, folder_id: str, tz: str) -> dict[str, Any]:
    """Message item → mirror row. The body is cleaned HERE, once, at sync time."""
    sender = getattr(item, "sender", None)
    sender_email = getattr(sender, "email_address", None) or ""
    sender_name = getattr(sender, "name", None) or ""
    subject = getattr(item, "subject", None) or ""
    text = getattr(item, "text_body", None) or ""
    body_clean = ""
    if text:
        try:
            body_clean = clean_body(text, max_chars=BODY_CLEAN_MAX)["text"]
        except Exception:  # noqa: BLE001 - fall back to the raw body, never fail sync
            body_clean = text[:BODY_CLEAN_MAX]
    received = getattr(item, "datetime_received", None)
    conv = getattr(item, "conversation_id", None)
    imid = getattr(item, "message_id", None)
    return {
        "ews_id": str(item.id),
        "changekey": getattr(item, "changekey", None),
        "folder_id": folder_id,
        "conversation_id": getattr(conv, "id", None),
        "sender_name": sender_name,
        "sender_email": sender_email,
        "to_json": recipients_json(item),
        "subject": subject,
        "date_ts": _ts(received),
        "date_iso": fmt_dt(received, tz),
        "is_read": 1 if getattr(item, "is_read", True) else 0,
        "has_attachments": 1 if getattr(item, "has_attachments", False) else 0,
        "importance": str(getattr(item, "importance", "") or "") or None,
        "categories_json": json.dumps(list(getattr(item, "categories", None) or []),
                                      ensure_ascii=False),
        "body_clean": body_clean,
        "internet_message_id": imid if isinstance(imid, str) else None,
    }


def row_from_event(item: Any, tz: str) -> dict[str, Any]:
    organizer = getattr(item, "organizer", None)
    return {
        "ews_id": str(getattr(item, "id", "") or ""),
        "changekey": getattr(item, "changekey", None),
        "subject": getattr(item, "subject", "") or "",
        "start_ts": _ts(getattr(item, "start", None)),
        "start_iso": fmt_dt(getattr(item, "start", None), tz),
        "end_ts": _ts(getattr(item, "end", None)),
        "end_iso": fmt_dt(getattr(item, "end", None), tz),
        "location": str(getattr(item, "location", None) or "") or None,
        "organizer": getattr(organizer, "email_address", None),
        "is_recurring": 1 if (getattr(item, "is_recurring", False)
                              or getattr(item, "recurrence", None)) else 0,
        "my_response": str(getattr(item, "my_response_type", None) or "") or None,
    }


def row_from_task(item: Any, tz: str) -> dict[str, Any]:
    due = getattr(item, "due_date", None)
    return {
        "ews_id": str(getattr(item, "id", "") or ""),
        "changekey": getattr(item, "changekey", None),
        "subject": getattr(item, "subject", "") or "",
        "due_ts": _ts(due),
        "due_iso": fmt_dt(due, tz) if hasattr(due, "astimezone") else (
            due.isoformat() if due is not None else None),
        "is_complete": 1 if getattr(item, "is_complete", False) else 0,
        "status": str(getattr(item, "status", None) or "") or None,
    }


class SyncEngine:
    def __init__(self, settings: Any, gateway: Any, store: CacheStore):
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.excluded_wks = {
            f"f:{part.strip().lower()}"
            for part in (settings.ews_mirror_exclude or "").split(",")
            if part.strip()
        }
        self.last_error: str | None = None
        self.last_cycle_ts: float | None = None
        self.cycles = 0
        self._task: asyncio.Task | None = None
        self._stopped = False
        self._last_slow_ts = 0.0
        self._last_hierarchy_ts = 0.0
        self._folders: dict[str, Any] = {}  # {ews id: Folder} to item-sync
        self.dropped_rows = 0
        self.tombstoned_rows = 0

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="cache-sync")
            logger.info("cache sync engine started (excluding %s, every %ss)",
                        sorted(self.excluded_wks), self.settings.ews_cache_sync_seconds)

    async def stop(self) -> None:
        self._stopped = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001, S110
                pass

    def status(self) -> dict[str, Any]:
        return {
            "cycles": self.cycles,
            "last_cycle_age_s": (int(time.time() - self.last_cycle_ts)
                                 if self.last_cycle_ts else None),
            "last_error": self.last_error,
            "dropped": self.dropped_rows,
            "tombstoned": self.tombstoned_rows,
        }

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                await self._cycle()
                self.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - degrade, never die
                # Degrade, never die: tools keep answering from live EWS
                # (or from the last good mirror state) while we retry.
                self.last_error = f"{type(exc).__name__}: {exc}"[:500]
                logger.warning("cache sync cycle failed: %s", self.last_error)
            self.last_cycle_ts = time.time()
            self.cycles += 1
            await asyncio.sleep(max(5, int(self.settings.ews_cache_sync_seconds)))

    # --------------------------------------------------------------- cycle

    def _hierarchy_due(self) -> bool:
        """True on the first cycle and every EWS_CACHE_HIERARCHY_SECONDS after.

        The walk itself is cheap, but seeing anything NEW in it costs a full
        FindFolder re-fetch (see _sync_hierarchy), so it is rate-limited while
        the item lane keeps running every cycle.
        """
        every = max(60, int(self.settings.ews_cache_hierarchy_seconds))
        return time.time() - self._last_hierarchy_ts >= every

    async def _cycle(self) -> None:
        if self._hierarchy_due():
            await self.gateway.call(self._sync_hierarchy)
            self._last_hierarchy_ts = time.time()
        slow_every = max(60, int(self.settings.ews_cache_hierarchy_seconds))
        await self.gateway.call(self._sync_mail_folders)
        if time.time() - self._last_slow_ts >= slow_every:
            await self.gateway.call(self._sync_slow_lane)
            self._last_slow_ts = time.time()

    def _sync_hierarchy(self, account: Any) -> None:
        """Refresh ews.folders and decide what to item-sync from now on.

        Runs BEFORE item sync, so a folder discovered by this refresh is
        mirrored in the same cycle. A folder that disappeared from the
        hierarchy has its sync token dropped and its `live` rows deleted;
        archived rows stay (they are Phase 2's archive, not a mirror).

        exchangelib caches the entire subfolder tree on the account's Root
        (``Root._subfolders``, filled once by ``Root._folders_map`` and never
        invalidated), so ``msg_folder_root.children`` re-reads a snapshot
        taken at boot: without clearing it, a folder created or deleted since
        then — and every folder's total_count/unread_count — is frozen for the
        life of the process. ``Root.clear_cache()`` drops it; the next
        ``children`` access re-fetches the tree.
        """
        root = getattr(account, "root", None)
        clear = getattr(root, "clear_cache", None)
        if callable(clear):
            clear()
        wk_by_raw: dict[str, str] = {}
        for wk_alias, attr in WELL_KNOWN.items():
            try:
                fid = getattr(getattr(account, attr, None), "id", None)
                if fid:
                    wk_by_raw.setdefault(str(fid), wk_alias)
            except Exception:  # noqa: BLE001, S112 - best-effort well-known lookup
                continue

        rows: list[dict[str, Any]] = []
        folders: dict[str, Any] = {}

        def walk(folder: Any, prefix: str) -> None:
            for child in list(getattr(folder, "children", None) or []):
                name = getattr(child, "name", "") or ""
                path = f"{prefix}/{name}" if prefix else name
                raw_id = getattr(child, "id", None)
                if raw_id:
                    fid = str(raw_id)
                    wk = wk_by_raw.get(fid)
                    rows.append({
                        "ews_id": fid,
                        "name": name,
                        "path": path,
                        "wk": wk,
                        "total": getattr(child, "total_count", None) or 0,
                        "unread": getattr(child, "unread_count", None) or 0,
                        "children": len(list(getattr(child, "children", None) or [])),
                    })
                    # Non-mail folders (Contacts and its Recipient Cache /
                    # GAL Contacts / Companies children, Calendar and its
                    # subfolders, Sharing, Yammer Root, Quick Step Settings,
                    # Conversation Action Settings, …) stay in ews.folders for
                    # list_folders but are NEVER handed the mail projection —
                    # sync_items(only_fields=ITEM_FIELDS) against them raises
                    # every cycle.
                    if wk not in self.excluded_wks and is_mail_folder(child, wk):
                        folders[fid] = child
                walk(child, path)

        walk(account.msg_folder_root, "")
        if not rows:
            return  # a failed walk must not look like "every folder vanished"

        known_before = {r["ews_id"] for r in self.store.folder_rows()}
        self.store.replace_folders(rows)
        # `folders` is this lane's own provenance — list_folders stamps its
        # as_of from it, NOT from the slow lane's `events` watermark.
        self.store.set_sync_state("folders", None, time.time())
        self._folders = folders

        for gone in known_before - {r["ews_id"] for r in rows}:
            removed = self.store.delete_live_messages_in_folder(gone)
            self.store.drop_sync_state(f"item:{gone}")
            logger.info("folder %s disappeared — dropped token, %s live rows",
                        gone, removed)

    def _sync_mail_folders(self, account: Any) -> None:
        """Apply item deltas for every mirrored folder (runs on the EWS pool).

        One folder failing degrades only that folder: the rest of the mailbox
        keeps syncing and the bad one is retried next cycle.
        """
        tz = self.settings.ews_tz
        for folder_id, folder in list(self._folders.items()):
            try:
                self._sync_one_folder(folder_id, folder, tz, account)
            except Exception as exc:  # noqa: BLE001 - per-folder degrade
                logger.warning("folder %s delta failed: %s", folder_id, exc)

    def _sync_one_folder(self, folder_id: str, folder: Any, tz: str,
                         account: Any) -> None:
        """Apply one folder's item delta, flushing every FLUSH_EVERY changes.

        Created/updated items are buffered as items (not rows) so each flush
        can pull their bodies in one bulk GetItem (`hydrate_bodies`) before
        `row_from_message` cleans them — the sync delta itself has no body.

        The token is persisted ONCE, at the end. exchangelib only learns the
        new sync state when SyncFolderItems reports the last item in range: it
        raises SyncCompleted out of FolderCollection.sync_items, and
        Folder.sync_items catches it and assigns self.item_sync_state AFTER
        the generator is exhausted (exchangelib/folders/base.py:653-677) — so
        there is no per-page token to save. Flushing rows in batches anyway
        keeps a first full sync of a huge folder bounded in memory; the worst
        case on a crash mid-folder is that the same pages are re-fetched and
        re-upserted next boot, which is idempotent.
        """
        token = self.store.get_sync_state(f"item:{folder_id}")
        upserts: list[Any] = []
        deletes: list[str] = []
        read_flags: list[tuple] = []

        def flush() -> None:
            if upserts:
                hydrate_bodies(account, upserts)
                self.store.upsert_messages(
                    [row_from_message(item, folder_id, tz) for item in upserts])
                upserts.clear()
            if deletes:
                # Spec §3: an archived row that disappears upstream (our own
                # deleter, or a hand-delete in Outlook) is KEPT and marked
                # deleted — we hold the only copy now. Only live rows are
                # dropped.
                dropped, tombstoned = self.store.apply_server_deletes(deletes)
                self.dropped_rows += dropped
                self.tombstoned_rows += tombstoned
                deletes.clear()
            for ews_id, is_read in read_flags:
                self.store.set_read_flag([ews_id], is_read)
            read_flags.clear()

        pending = 0
        for change_type, payload in folder.sync_items(
            sync_state=token, only_fields=ITEM_FIELDS,
        ):
            if change_type in ("create", "update"):
                if getattr(payload, "id", None):
                    upserts.append(payload)
                    pending += 1
            elif change_type == "delete":
                deletes.append(str(payload.id))
                pending += 1
            elif change_type == "read_flag_change":
                item_id, is_read = payload
                read_flags.append((str(item_id.id), bool(is_read)))
                pending += 1
            if pending >= FLUSH_EVERY:
                flush()
                pending = 0
        flush()
        self.store.set_sync_state(f"item:{folder_id}", folder.item_sync_state,
                                  time.time())

    def _sync_slow_lane(self, account: Any) -> None:
        """Expanded calendar window + tasks folder (every ~10 min)."""
        tz = self.settings.ews_tz
        # Expanded calendar occurrences for the overview window.
        try:
            now = datetime.now(ZoneInfo(tz))
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            events = list(account.calendar.view(
                start=day_start,
                end=day_start + timedelta(days=CALENDAR_WINDOW_DAYS),
                max_items=CALENDAR_MAX_ITEMS,
            ))
            self.store.replace_events(
                [row_from_event(ev, tz) for ev in events
                 if getattr(ev, "id", None)])
            self.store.set_sync_state("events", None, time.time())
        except Exception as exc:  # noqa: BLE001 - calendar sync is best-effort
            logger.debug("calendar window sync failed: %s", exc)

        # Tasks folder (small; delta-synced like mail).
        try:
            tasks_folder = getattr(account, "tasks", None)
            if tasks_folder is not None:
                token = self.store.get_sync_state("item:tasks")
                upserts: list[dict[str, Any]] = []
                deletes: list[str] = []
                for change_type, payload in tasks_folder.sync_items(
                    sync_state=token, only_fields=TASK_FIELDS,
                ):
                    if change_type in ("create", "update"):
                        if getattr(payload, "id", None):
                            upserts.append(row_from_task(payload, tz))
                    elif change_type == "delete":
                        deletes.append(str(payload.id))
                self.store.upsert_tasks(upserts)
                self.store.delete_tasks_by_id(deletes)
                self.store.set_sync_state("item:tasks",
                                          tasks_folder.item_sync_state,
                                          time.time())
        except Exception as exc:  # noqa: BLE001 - tasks sync is best-effort
            logger.debug("tasks sync failed: %s", exc)
