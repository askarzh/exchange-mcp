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
the item changed after it was verified and is skipped, never deleted; it is
counted in `failed` and gets a line in `reasons`, not silently dropped.

Deletes go to Exchange in batches of `BATCH_SIZE`. `mark_deleted` plus one
destructive-class audit record per successfully deleted item are written
IMMEDIATELY after each batch — never accumulated and written only at the
end — for two independent reasons:

- an exception fetching/deleting a LATER batch (`gateway.call` itself
  raising) must not lose the record of what an EARLIER batch already did;
- the persist step (mark + audit) for THIS batch is itself wrapped in its
  own try/except: if it raises (a DB outage right after Exchange already
  hard-deleted the batch) the batch's ids are mail that is genuinely gone
  from the mailbox with nothing recorded about it yet. Those ids are never
  silently dropped — they land in `result["deleted_unrecorded"]`, `error` is
  set, and the run stops making further deletions while persistence is
  failing (a store that can't record deletions cannot be trusted to record
  the next batch's either).

`mark_deleted` is only ever called with ids whose `item.delete()` did not
raise; its rowcount is checked against what was asked for, and any shortfall
(a concurrent state change raced us — the row was still deleted from Exchange
but didn't transition in the store) is resolved to exact ids via
`messages_by_ids` and reported through `unmarked` and `deleted_unrecorded`
rather than silently assumed complete.

**Result accounting.** `deleted`, `failed` and `remaining` partition the
`eligible` rows and always satisfy the invariant
``deleted + failed + remaining == eligible``:

- `deleted` = ids whose Exchange `item.delete()` call SUCCEEDED — the mail is
  gone from the mailbox — regardless of what happened to the store record of
  that afterwards. `deleted_unrecorded` (persist raised) and the `unmarked`
  ids (a rowcount shortfall) are both SUBSETS of `deleted`, not separate
  buckets: the item was still deleted, only the bookkeeping about it is
  incomplete or missing, which is exactly why those ids are surfaced rather
  than silently folded into a generic failure count.
- `failed` = ids from an ATTEMPTED batch (its `gateway.call` returned) whose
  deletion was skipped (stale changekey) or raised — each has its own line in
  `reasons`.
- `remaining` = eligible ids never attempted at all, because the run stopped
  early (a `gateway.call` or persist failure) or never started (`dry_run`).
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
        result: dict[str, Any] = {
            "eligible": 0, "deleted": 0, "failed": 0, "remaining": 0,
            "unmarked": 0, "deleted_unrecorded": [], "reasons": [],
            "blocked": None, "error": None, "sample": [],
        }
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
            # Nothing was attempted — every eligible row is `remaining`, so
            # the deleted + failed + remaining == eligible invariant holds
            # even for a preview run.
            result["remaining"] = len(rows)
            return result
        by_id = {r["ews_id"]: r for r in rows}
        ids = list(by_id)
        deleted_count = 0
        failed_count = 0
        processed_upto = 0  # index into `ids`: how much we actually attempted
        for start in range(0, len(ids), BATCH_SIZE):
            batch = ids[start:start + BATCH_SIZE]
            try:
                deleted, timings, reasons = await self.gateway.call(
                    lambda account, b=batch: self._delete_batch(account, b, by_id))
            except Exception as exc:  # noqa: BLE001 - stop, but keep what we already persisted
                result["error"] = f"{type(exc).__name__}: {exc}"
                logger.error("archive delete batch failed, stopping run: %s",
                            result["error"])
                break
            processed_upto = start + len(batch)
            for raw_id, reason in reasons.items():
                result["reasons"].append(f"{raw_id}: {reason}")
            failed_count += len(reasons)
            if not deleted:
                continue
            # Counted as `deleted` the moment Exchange's item.delete() has
            # succeeded — the mail is gone either way; deleted_unrecorded/
            # unmarked below only track how completely that fact got
            # recorded, they do not change whether it happened.
            deleted_count += len(deleted)
            # Persisted IMMEDIATELY, one batch at a time (see module docstring).
            try:
                marked = self.store.mark_deleted(deleted)
                unmarked_ids: list[str] = []
                if marked != len(deleted):
                    after = self.store.messages_by_ids(deleted)
                    unmarked_ids = [i for i in deleted
                                    if after.get(i, {}).get("archive_state") != "deleted"]
                    result["unmarked"] += len(unmarked_ids)
                    result["deleted_unrecorded"].extend(unmarked_ids)
                    logger.warning(
                        "archive delete: %d item(s) deleted from Exchange but "
                        "not marked deleted in the store (state changed "
                        "concurrently) — %s", len(unmarked_ids), unmarked_ids)
                recorded = [i for i in deleted if i not in unmarked_ids]
                for ews_id in recorded:
                    row = by_id[ews_id]
                    self.audit.record(
                        tool="archive_delete", side_effect_class="destructive",
                        outcome="ok", latency_ms=timings.get(ews_id, 0),
                        transport="archive",
                        detail={"ews_id": ews_id,
                                "internet_message_id": row.get("internet_message_id"),
                                "mime_sha256": row.get("mime_sha256"),
                                "run_id": run_id})
            except Exception as exc:  # noqa: BLE001 - mail is gone; record it or stop trying
                result["deleted_unrecorded"].extend(deleted)
                result["error"] = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "archive delete: %d item(s) were hard-deleted from Exchange "
                    "but could NOT be recorded (mark/audit failed) — %s: %s",
                    len(deleted), deleted, result["error"])
                break
        result["deleted"] = deleted_count
        result["failed"] = failed_count
        result["remaining"] = len(ids) - processed_upto
        return result

    # Runs on the EWS pool (sync).
    # Returns (deleted_ids, {ews_id: latency_ms}, {ews_id: failure_reason}).
    def _delete_batch(
        self, account: Any, ids: list[str], by_id: dict[str, Any]
    ) -> tuple[list[str], dict[str, int], dict[str, str]]:
        done: list[str] = []
        timings: dict[str, int] = {}
        reasons: dict[str, str] = {}
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
                reason = f"{type(exc).__name__}: {exc}"
                reasons[raw_id] = reason
                logger.warning("archive delete failed for %s: %s", raw_id, reason)
        logger.info("archive deleted %d/%d items in %.1fs",
                    len(done), len(ids), time.time() - started)
        return done, timings, reasons
