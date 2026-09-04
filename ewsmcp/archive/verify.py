"""Verify: prove the archive copy is good BEFORE anything is deleted upstream.

Four independent checks against a `captured` row — the item still exists with
the same changekey it had at capture time, the MIME file on disk still hashes
to `mime_sha256`, every recorded blob exists, and its size matches. Any
failure resets the row to `live` (dropping the capture entirely), so the next
cycle re-captures it. The row is only promoted to `verified` when all four
pass. A row the server no longer has (`ErrorItemNotFound` or similar on
re-fetch) is a `failed` outcome, not a reset — it stays `captured` and is
retried next cycle; only `apply_server_deletes` (driven by sync, not the
verifier) may move a captured row to `deleted`.
"""

from __future__ import annotations

import logging
from typing import Any

from . import files

logger = logging.getLogger(__name__)

VERIFY_FIELDS = ["changekey", "attachments"]
BATCH_SIZE = 25


class Verifier:
    def __init__(self, settings: Any, gateway: Any, store: Any) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store

    async def run(self, *, limit: int = BATCH_SIZE) -> dict[str, Any]:
        rows = self.store.captured_rows(int(limit))
        result: dict[str, Any] = {"verified": 0, "reset": 0, "failed": 0,
                                  "reasons": []}
        if not rows:
            return result
        by_id = {r["ews_id"]: r for r in rows}
        ids = list(by_id)
        fetched = await self.gateway.call(
            lambda account: account.fetch(
                ids=[(i, None) for i in ids], only_fields=VERIFY_FIELDS))
        for ews_id, item in zip(ids, fetched, strict=True):
            row = by_id[ews_id]
            if isinstance(item, Exception):
                result["failed"] += 1
                logger.warning("verify could not re-fetch %s: %s", ews_id, item)
                continue
            reason = self._mismatch(row, item)
            if reason is None:
                self.store.mark_verified(ews_id)
                result["verified"] += 1
            else:
                self.store.reset_to_live(ews_id)
                result["reset"] += 1
                result["reasons"].append({"ews_id": ews_id, "reason": reason})
                logger.warning("verify reset %s to live: %s", ews_id, reason)
        return result

    def _mismatch(self, row: dict[str, Any], item: Any) -> str | None:
        live_ck = getattr(item, "changekey", None)
        # Prefer the changekey snapshotted at capture time — `messages.changekey`
        # is overwritten by the ordinary sync loop, so comparing against it
        # would just compare the live item to itself. A legacy row with no
        # snapshot falls back to the synced value.
        captured_ck = row.get("captured_changekey") or row.get("changekey")
        if captured_ck and live_ck and live_ck != captured_ck:
            return (f"changekey changed since capture "
                    f"({captured_ck} → {live_ck})")
        path = files.mime_path(self.settings.data_dir, row["mime_sha256"] or "")
        if not path.is_file():
            return f"mime file missing at {path}"
        if files.sha256_file(path) != row["mime_sha256"]:
            return f"mime hash mismatch at {path}"
        for att in self.store.attachments_for(row["ews_id"]):
            if not att["sha256"]:
                continue  # ItemAttachment — lives inside the verified MIME
            blob = files.blob_path(self.settings.data_dir, att["sha256"])
            if not blob.is_file():
                return f"blob missing for {att['name']!r} at {blob}"
            if att["size"] is not None and blob.stat().st_size != int(att["size"]):
                return (f"blob size mismatch for {att['name']!r}: "
                        f"{blob.stat().st_size} on disk vs {att['size']} recorded")
        return None
