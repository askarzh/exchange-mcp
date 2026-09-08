"""The archive cycle: one asyncio task, one ledger row per pass.

Started by the daemon AFTER Exchange warms up (like the sync engine), then
every ``ARCHIVE_CYCLE_SECONDS`` it captures, verifies, embeds and — only when
the rails allow — deletes. Every pass writes an ``ews.archive_runs`` row so
``archive_status`` and ``GET /v1/archive/runs/<id>`` can say exactly what
happened to the mailbox and when.

The background loop runs the DELETE lane only when ``ARCHIVE_DELETE_AUTO``
is true (on top of ``ARCHIVE_DELETE_ENABLED``, which gates every deletion
including tool-driven ones). By default deletion is therefore MANUAL: an
operator calls ``archive_run(kind="delete", dry_run=false)``, which is
confirm-gated. Turning ARCHIVE_DELETE_AUTO on means the loop hard-deletes up
to ARCHIVE_MAX_DELETE_PER_RUN messages every ARCHIVE_CYCLE_SECONDS.

``run_once`` and the background loop share one asyncio.Lock so a manual
``archive_run`` call and a mid-cycle background pass never run concurrently
against the same mailbox. ``run_once`` bounds its wait on that lock
(``wait_seconds``, default 5s): a caller blocked longer than that gets back
a ``blocked`` result instead of hanging on an in-progress cycle. The
background loop itself always waits for the lock without a bound — it has
nowhere else to be.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from . import files
from .capture import Capturer
from .delete import Deleter
from .embed import EmbedWorker
from .gc import GcWorker
from .policy import ArchivePolicy
from .verify import Verifier

logger = logging.getLogger(__name__)

KINDS = ("capture", "verify", "delete", "embed", "gc", "all")
MIN_CYCLE_SECONDS = 30


class ArchiveRunner:
    def __init__(self, settings: Any, gateway: Any, store: Any, audit: Any,
                 index: Any = None) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.audit = audit
        self.index = index
        self.cycles = 0
        self.last_error: str | None = None
        self.last_cycle_ts: float | None = None
        self.last_run_id: int | None = None
        self._task: asyncio.Task | None = None
        self._stopped = False
        self._lock = asyncio.Lock()
        self._disk_cache: dict[str, Any] | None = None
        self._disk_cache_ts: float = 0.0
        # status() is sync (an HTTP handler calls it), so the boilerplate
        # counters are read once per cycle and served from here.
        self._boilerplate_cache: dict[str, Any] = {}
        # The GC lane runs on its own (weekly by default) interval, never
        # inside a regular cycle. Seeded with "now" so a freshly started
        # daemon does not walk the whole blob store on its first pass.
        self._last_gc_ts: float = time.time()
        self._gc_status: dict[str, Any] = {}
        # Reset each cycle by the capture lane — a per-process counter, not
        # a durable state (the row itself stays `live`, so nothing is
        # tracked in Postgres for it).
        self._too_large_last: int = 0
        # The ids behind that counter, remembered for the life of the
        # process so they stop re-filling every candidate page (see
        # Capturer.__init__). Cleared by a restart.
        self._too_large_ids: set[str] = set()

    # ------------------------------------------------------------ one pass

    async def run_once(self, *, kind: str = "all", dry_run: bool = True,
                       before: str | None = None,
                       folders: list[str] | None = None,
                       wait_seconds: float = 5.0) -> dict[str, Any]:
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {', '.join(KINDS)}")
        try:
            await asyncio.wait_for(self._lock.acquire(), wait_seconds)
        except TimeoutError:
            return {
                "ok": False, "run_id": None, "kind": kind, "dry_run": dry_run,
                "candidates": 0, "captured": 0, "verified": 0, "reset": 0,
                "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
                "too_large": 0,
                "blocked": "cycle in progress", "retry_after_s": 30,
                "stopped": None, "error": None, "sample": [], "gc": None,
            }
        try:
            return await self._run_once_locked(kind=kind, dry_run=dry_run,
                                               before=before, folders=folders)
        finally:
            self._lock.release()

    async def _run_once_locked(self, *, kind: str, dry_run: bool,
                               before: str | None,
                               folders: list[str] | None,
                               allow_delete: bool = True) -> dict[str, Any]:
        policy = ArchivePolicy.from_settings(self.settings).with_overrides(
            before=before, folders=folders, tz=self.settings.ews_tz)
        # start_run/finish_run are blocking DB calls — run them off the
        # event loop like embed.py does, so a slow archive_runs write never
        # stalls whatever else is on this loop.
        run_id = await asyncio.to_thread(
            self.store.start_run, kind, dry_run=dry_run, policy=policy.as_dict())
        self.last_run_id = run_id
        out: dict[str, Any] = {
            "ok": True, "run_id": run_id, "kind": kind, "dry_run": dry_run,
            "candidates": 0, "captured": 0, "verified": 0, "reset": 0,
            "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
            "too_large": 0,
            "blocked": None, "stopped": None, "error": None, "sample": [],
            "gc": None,
        }
        try:
            try:
                if kind in ("capture", "all"):
                    res = await Capturer(self.settings, self.gateway, self.store,
                                         policy, self._too_large_ids
                                         ).run(dry_run=dry_run)
                    out["candidates"] = res["candidates"]
                    out["captured"] = res["captured"]
                    out["failed"] += res["failed"]
                    out["too_large"] = res["too_large"]
                    self._too_large_last = len(self._too_large_ids)
                    out["stopped"] = res["stopped"]
                    out["sample"] = res["sample"]
                if kind in ("verify", "all") and not dry_run:
                    res = await Verifier(self.settings, self.gateway,
                                         self.store).run()
                    out["verified"] = res["verified"]
                    out["reset"] = res["reset"]
                    out["failed"] += res["failed"]
                if kind in ("embed", "all") and not dry_run:
                    res = await EmbedWorker(self.store, self.index).run()
                    out["embedded"] = res["embedded"]
                    if res["error"]:
                        out["error"] = res["error"]
                if kind == "gc":
                    # Never part of "all": the orphan sweep walks the whole
                    # blob store, so it runs on its own interval.
                    res = await GcWorker(self.settings, self.store).run(
                        dry_run=dry_run)
                    out["gc"] = {k: res[k] for k in ("scanned", "removed_files",
                                                     "removed_bytes", "kept_recent")}
                    out["sample"] = [{"removed_files": res["removed_files"],
                                      "removed_bytes": res["removed_bytes"]}]
                    if res["error"]:
                        out["error"] = res["error"]
                if kind in ("delete", "all") and not allow_delete:
                    out["blocked"] = (
                        "ARCHIVE_DELETE_AUTO=false — the background cycle never "
                        "deletes. Run archive_run(kind='delete', dry_run=false) "
                        "to delete deliberately.")
                elif kind in ("delete", "all"):
                    res = await Deleter(self.settings, self.gateway, self.store,
                                        policy, self.audit).run(dry_run=dry_run,
                                                                run_id=run_id)
                    out["eligible"] = res["eligible"]
                    out["deleted"] = res["deleted"]
                    out["failed"] += res["failed"]
                    out["blocked"] = res["blocked"]
                    if not out["sample"]:
                        out["sample"] = res["sample"]
            except Exception as exc:  # noqa: BLE001 - a pass never takes ewsd down
                out["ok"] = False
                out["error"] = f"{type(exc).__name__}: {exc}"
                logger.error("archive run %s (%s) failed: %s", run_id, kind,
                             out["error"])
        finally:
            # Always close the archive_runs row — even if a worker raised —
            # so a crashed pass never leaves one unfinished (finished_at
            # NULL forever).
            await asyncio.to_thread(
                self.store.finish_run, run_id, captured=out["captured"],
                verified=out["verified"], deleted=out["deleted"],
                failed=out["failed"], error=out["error"] or out["stopped"],
                sample=out["sample"])
        return out

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        if self._task is None:
            self._stopped = False
            self._task = asyncio.create_task(self._loop(), name="archive")
            logger.info("archive runner started (every %ss, delete_enabled=%s, "
                        "delete_auto=%s)",
                        self.settings.archive_cycle_seconds,
                        self.settings.archive_delete_enabled,
                        self.settings.archive_delete_auto)

    async def stop(self) -> None:
        self._stopped = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001 - stop() must never raise
                pass
        self._task = None

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                async with self._lock:
                    result = await self._run_once_locked(
                        kind="all", dry_run=False, before=None, folders=None,
                        allow_delete=bool(self.settings.archive_delete_auto))
                self.last_error = result.get("error")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - degrade, never die
                self.last_error = f"{type(exc).__name__}: {exc}"[:500]
                logger.warning("archive cycle failed: %s", self.last_error)
            try:
                self._boilerplate_cache = {
                    **await asyncio.to_thread(self.store.boilerplate_stats),
                    "drop_detector": self.settings.archive_boilerplate_drop,
                    "threshold": float(self.settings.embed_boilerplate_threshold),
                }
            except Exception as exc:  # noqa: BLE001 - counters never break a cycle
                logger.warning("boilerplate stats failed: %s", exc)
            await self._maybe_gc()
            self.cycles += 1
            self.last_cycle_ts = time.time()
            await asyncio.sleep(max(MIN_CYCLE_SECONDS,
                                    int(self.settings.archive_cycle_seconds)))

    async def _maybe_gc(self) -> None:
        """The orphan sweep, on its own interval (ARCHIVE_GC_INTERVAL_HOURS).

        Separate from the cycle because it walks every file under DATA_DIR:
        weekly is plenty, and 0 means "every cycle" (which is what the tests
        use)."""
        due = float(self.settings.archive_gc_interval_hours) * 3600
        if time.time() - self._last_gc_ts < due:
            return
        self._last_gc_ts = time.time()
        try:
            async with self._lock:
                result = await self._run_once_locked(
                    kind="gc", dry_run=False, before=None, folders=None)
            self._gc_status = {"last_run_ts": self._last_gc_ts,
                               **(result.get("gc") or {})}
            if result.get("error"):
                self.last_error = result["error"]
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - GC never breaks the loop
            logger.warning("archive gc failed: %s", exc)

    def status(self) -> dict[str, Any]:
        return {
            "running": self._task is not None and not self._task.done(),
            "cycles": self.cycles,
            "cycle_seconds": int(self.settings.archive_cycle_seconds),
            "last_cycle_age_s": (int(time.time() - self.last_cycle_ts)
                                 if self.last_cycle_ts else None),
            # Lets an observer tell "idle between cycles" from "stuck": the
            # embed/capture lanes only move once per cycle, so a backlog
            # that is flat for four minutes is the normal state, not a fault.
            "next_cycle_in_s": (max(0, int(self.last_cycle_ts
                                           + self.settings.archive_cycle_seconds
                                           - time.time()))
                                if self.last_cycle_ts else None),
            "last_run_id": self.last_run_id,
            "last_error": self.last_error,
            "delete_enabled": bool(self.settings.archive_delete_enabled),
            "delete_auto": bool(self.settings.archive_delete_auto),
            # The policy ewsd actually runs. The MCP process has no ARCHIVE_*
            # environment of its own, so this is the only truthful source.
            "policy": self._policy_dict(),
            "semantic_enabled": bool(self.settings.semantic_enabled()),
            # Filled at the end of each cycle; empty before the first one.
            "boilerplate": dict(self._boilerplate_cache),
            # Last orphan sweep; empty until one has run.
            "gc": dict(self._gc_status),
            # Per-process counter: how many distinct items this process has
            # skipped for size since it started. The rows stay `live` (and
            # are held out of later candidate pages), so this is the only
            # place an operator sees what is stuck behind
            # ARCHIVE_MAX_ITEM_MB.
            "state_counts": {"skipped_too_large": self._too_large_last},
        }

    def _policy_dict(self) -> dict[str, Any]:
        p = ArchivePolicy.from_settings(self.settings)
        return {"folders": list(p.folders), "after_days": p.after_days,
                "grace_days": p.grace_days,
                "exclude_categories": list(p.exclude_categories),
                "max_delete_per_run": p.max_delete_per_run,
                "min_free_gb": p.min_free_gb}

    _DISK_STATS_TTL_S = 60

    async def disk_stats(self) -> dict[str, Any]:
        """Blob-store size and free space under THIS process's DATA_DIR —
        only ewsd calls this (the MCP never touches its own filesystem for
        archive_status). Cached for ~60s so a status/metrics poller never
        pays for a directory walk plus statvfs on every request."""
        now = time.time()
        if self._disk_cache is not None and now - self._disk_cache_ts < self._DISK_STATS_TTL_S:
            return self._disk_cache
        data_dir = self.settings.data_dir
        blob_bytes = await asyncio.to_thread(files.blob_store_bytes, data_dir)
        free = await asyncio.to_thread(files.free_gb, data_dir)
        self._disk_cache = {"blob_store_bytes": blob_bytes, "free_gb": round(free, 2)}
        self._disk_cache_ts = now
        return self._disk_cache
