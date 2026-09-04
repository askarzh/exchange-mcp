"""The archive cycle: one asyncio task, one ledger row per pass.

Started by the daemon AFTER Exchange warms up (like the sync engine), then
every ``ARCHIVE_CYCLE_SECONDS`` it captures, verifies, embeds and — only when
the rails allow — deletes. Every pass writes an ``ews.archive_runs`` row so
``archive_status`` and ``GET /v1/archive/runs/<id>`` can say exactly what
happened to the mailbox and when.

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

from .capture import Capturer
from .delete import Deleter
from .embed import EmbedWorker
from .policy import ArchivePolicy
from .verify import Verifier

logger = logging.getLogger(__name__)

KINDS = ("capture", "verify", "delete", "embed", "all")
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
                "blocked": "cycle in progress", "retry_after_s": 30,
                "stopped": None, "error": None, "sample": [],
            }
        try:
            return await self._run_once_locked(kind=kind, dry_run=dry_run,
                                               before=before, folders=folders)
        finally:
            self._lock.release()

    async def _run_once_locked(self, *, kind: str, dry_run: bool,
                               before: str | None,
                               folders: list[str] | None) -> dict[str, Any]:
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
            "blocked": None, "stopped": None, "error": None, "sample": [],
        }
        try:
            try:
                if kind in ("capture", "all"):
                    res = await Capturer(self.settings, self.gateway, self.store,
                                         policy).run(dry_run=dry_run)
                    out["candidates"] = res["candidates"]
                    out["captured"] = res["captured"]
                    out["failed"] += res["failed"]
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
                if kind in ("delete", "all"):
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
            logger.info("archive runner started (every %ss, delete_enabled=%s)",
                        self.settings.archive_cycle_seconds,
                        self.settings.archive_delete_enabled)

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
                        kind="all", dry_run=False, before=None, folders=None)
                self.last_error = result.get("error")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - degrade, never die
                self.last_error = f"{type(exc).__name__}: {exc}"[:500]
                logger.warning("archive cycle failed: %s", self.last_error)
            self.cycles += 1
            self.last_cycle_ts = time.time()
            await asyncio.sleep(max(MIN_CYCLE_SECONDS,
                                    int(self.settings.archive_cycle_seconds)))

    def status(self) -> dict[str, Any]:
        return {
            "running": self._task is not None and not self._task.done(),
            "cycles": self.cycles,
            "cycle_seconds": int(self.settings.archive_cycle_seconds),
            "last_cycle_age_s": (int(time.time() - self.last_cycle_ts)
                                 if self.last_cycle_ts else None),
            "last_run_id": self.last_run_id,
            "last_error": self.last_error,
            "delete_enabled": bool(self.settings.archive_delete_enabled),
        }
