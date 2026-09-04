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

On top of that: a per-run cap, deletion in batches of 25 against Exchange,
and one audit record per successfully deleted item carrying enough identity
(`ews_id`, `internet_message_id`, `mime_sha256`, run id) to reconstruct what
went and which archive copy replaced it. `mark_deleted` is only ever called
with ids whose `item.delete()` did not raise.
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
                                  "blocked": None, "sample": []}
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
        deleted = await self.gateway.call(
            lambda account: self._delete_all(account, ids))
        if deleted:
            self.store.mark_deleted(deleted)
        for ews_id in deleted:
            row = by_id[ews_id]
            self.audit.record(
                tool="archive_delete", side_effect_class="destructive",
                outcome="ok", latency_ms=0, transport="archive",
                detail={"ews_id": ews_id,
                        "internet_message_id": row.get("internet_message_id"),
                        "mime_sha256": row.get("mime_sha256"),
                        "run_id": run_id})
        result["deleted"] = len(deleted)
        result["failed"] = len(rows) - len(deleted)
        return result

    # Runs on the EWS pool (sync).
    def _delete_all(self, account: Any, ids: list[str]) -> list[str]:
        done: list[str] = []
        for start in range(0, len(ids), BATCH_SIZE):
            done.extend(self._delete_batch(account, ids[start:start + BATCH_SIZE]))
        return done

    def _delete_batch(self, account: Any, ids: list[str]) -> list[str]:
        done: list[str] = []
        started = time.time()
        fetched = account.fetch(ids=[(i, None) for i in ids],
                                only_fields=["id", "changekey"])
        for raw_id, item in zip(ids, fetched, strict=True):
            try:
                if isinstance(item, Exception):
                    raise item
                item.delete()  # exchangelib 5.0.3: Item.delete() IS HardDelete
                done.append(raw_id)
            except Exception as exc:  # noqa: BLE001 - one failure never stops the pass
                logger.warning("archive delete failed for %s: %s: %s",
                               raw_id, type(exc).__name__, exc)
        logger.info("archive deleted %d/%d items in %.1fs",
                    len(done), len(ids), time.time() - started)
        return done
