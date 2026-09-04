"""Delete: the only code in this repository that removes mail from Exchange
without a human naming the message.

Three INDEPENDENT rails must all hold (spec §5) before a single item.delete()
is issued:

1. `ARCHIVE_DELETE_ENABLED` is true (checked here; the `full`-tier + confirm
   gate for the tool path is the caller's job).
2. The row is `verified`.
3. The row is older than the delete cutoff (capture cutoff + grace) AND was
   verified at least a grace period ago — `CacheStore.deletable_rows` is the
   only query that selects candidates, so both halves of rail 3 live there.

On top of that, right before each item is actually deleted: the changekey
re-fetched from Exchange must still match `captured_changekey` (the snapshot
taken at capture/verify time). A mismatch — or either side missing — means
the item changed after it was verified and is skipped, never deleted; it
counts as `failed` with a reason, not silently dropped.

Deletes go to Exchange in batches of `BATCH_SIZE`, and `mark_deleted` plus
one destructive-class audit record per successfully deleted item are written
IMMEDIATELY after each batch — not accumulated and written only at the end —
so a failure fetching/deleting a later batch (a network blip, an EWS 5xx)
never loses the record of what an earlier batch already did to Exchange.
`mark_deleted` is only ever called with ids whose `item.delete()` did not
raise; its rowcount is checked against what was asked for, and any shortfall
(a concurrent state change raced us) is reported as `unmarked` rather than
silently assumed.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .policy import ArchivePolicy

logger = logging.getLogger(__name__)

BATCH_SIZE = 25


class Deleter:
    def __init__(self, settings: Any, gateway: Any, store: Any,
                 policy: ArchivePolicy, audit: Any) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.policy = policy
        self.audit = audit

    async def run(self, *, dry_run: bool = True,
                  run_id: int | None = None) -> dict[str, Any]:
        result: dict[str, Any] = {"eligible": 0, "deleted": 0, "failed": 0,
                                  "unmarked": 0, "blocked": None, "error": None,
                                  "sample": []}
        if not self.policy.delete_enabled:
            result["blocked"] = (
                "ARCHIVE_DELETE_ENABLED=false — nothing is deleted from Exchange. "
                "Flip it deliberately once you trust the archive.")
            return result
        rows = self.store.deletable_rows(
            before_ts=self.policy.delete_cutoff_ts(),
            verified_before=self.policy.grace_instant_ts(),
            limit=int(self.policy.max_delete_per_run))
        result["eligible"] = len(rows)
        result["sample"] = [{"ews_id": r["ews_id"], "subject": r.get("subject"),
                             "date": r.get("date_iso")} for r in rows[:10]]
        if dry_run or not rows:
            return result
        by_id = {r["ews_id"]: r for r in rows}
        ids = list(by_id)
        deleted_count = 0
        for start in range(0, len(ids), BATCH_SIZE):
            batch = ids[start:start + BATCH_SIZE]
            try:
                deleted, timings = await self.gateway.call(
                    lambda account, b=batch: self._delete_batch(account, b, by_id))
            except Exception as exc:  # noqa: BLE001 - stop, but keep what we already persisted
                result["error"] = f"{type(exc).__name__}: {exc}"
                logger.error("archive delete batch failed, stopping run: %s",
                            result["error"])
                break
            # Persisted IMMEDIATELY, one batch at a time: an exception in a
            # LATER batch must never lose the record of what THIS batch did.
            if deleted:
                marked = self.store.mark_deleted(deleted)
                if marked != len(deleted):
                    shortfall = len(deleted) - marked
                    result["unmarked"] += shortfall
                    logger.warning(
                        "archive delete: %d item(s) deleted from Exchange but "
                        "not marked deleted in the store (state changed "
                        "concurrently) — %s", shortfall, deleted)
                for ews_id in deleted:
                    row = by_id[ews_id]
                    self.audit.record(
                        tool="archive_delete", side_effect_class="destructive",
                        outcome="ok", latency_ms=timings.get(ews_id, 0),
                        transport="archive",
                        detail={"ews_id": ews_id,
                                "internet_message_id": row.get("internet_message_id"),
                                "mime_sha256": row.get("mime_sha256"),
                                "run_id": run_id})
                deleted_count += len(deleted)
        result["deleted"] = deleted_count
        result["failed"] = len(rows) - deleted_count
        return result

    # Runs on the EWS pool (sync). Returns (deleted_ids, {ews_id: latency_ms}).
    def _delete_batch(self, account: Any, ids: list[str],
                      by_id: dict[str, Any]) -> tuple[list[str], dict[str, int]]:
        done: list[str] = []
        timings: dict[str, int] = {}
        started = time.time()
        fetched = account.fetch(ids=[(i, None) for i in ids],
                                only_fields=["id", "changekey"])
        for raw_id, item in zip(ids, fetched, strict=True):
            item_started = time.time()
            try:
                if isinstance(item, Exception):
                    raise item
                # Rail check, re-done right before deletion: the changekey
                # re-fetched here must still match the snapshot taken at
                # capture time. A mismatch — or either side missing — means
                # the item moved after it was verified; skip it, never
                # delete it on a stale verification.
                live_ck = getattr(item, "changekey", None)
                captured_ck = by_id[raw_id].get("captured_changekey")
                if not captured_ck or not live_ck or live_ck != captured_ck:
                    raise ValueError(
                        f"changed since capture (captured={captured_ck!r}, "
                        f"live={live_ck!r})")
                item.delete()  # exchangelib 5.0.3: Item.delete() IS HardDelete
                done.append(raw_id)
                timings[raw_id] = int((time.time() - item_started) * 1000)
            except Exception as exc:  # noqa: BLE001 - one failure never stops the batch
                logger.warning("archive delete failed for %s: %s: %s",
                               raw_id, type(exc).__name__, exc)
        logger.info("archive deleted %d/%d items in %.1fs",
                    len(done), len(ids), time.time() - started)
        return done, timings
