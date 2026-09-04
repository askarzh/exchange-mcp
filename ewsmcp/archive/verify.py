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

The changekey check fails CLOSED: a row with no `captured_changekey` snapshot
(the capturer always writes one; only a corrupted row would lack it) or an
item whose live changekey cannot be read is treated as unverifiable and
reset, never silently promoted. A row that raises while being evaluated (a
poisoned `mime_sha256`/`sha256` that isn't a valid hex sha, for instance) is
counted `failed` rather than aborting the whole batch.
"""

from __future__ import annotations

import asyncio
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
        rows = await asyncio.to_thread(self.store.captured_rows, int(limit))
        result: dict[str, Any] = {"verified": 0, "reset": 0, "failed": 0,
                                  "reasons": []}
        if not rows:
            return result
        by_id = {r["ews_id"]: r for r in rows}
        ids = list(by_id)
        fetched = await self.gateway.call(
            lambda account: account.fetch(
                ids=[(i, None) for i in ids], only_fields=VERIFY_FIELDS))
        pairs: list[tuple[str, Any]] = []
        for ews_id, item in zip(ids, fetched, strict=True):
            if isinstance(item, Exception):
                result["failed"] += 1
                logger.warning("verify could not re-fetch %s: %s", ews_id, item)
                continue
            pairs.append((ews_id, item))
        # Two hops off the event loop, one per phase: `_evaluate` hashes every
        # .eml and reads each row's attachment inventory, `_apply` writes the
        # state transitions. Both are blocking and both are batched, so a
        # 25-row pass costs two to_thread hops, not fifty.
        verdicts = await asyncio.to_thread(self._evaluate, pairs, by_id)
        verified, reset, failed, reasons = await asyncio.to_thread(
            self._apply, verdicts)
        result["verified"] += verified
        result["reset"] += reset
        result["failed"] += failed
        result["reasons"].extend(reasons)
        return result

    # Runs on a worker thread. Returns (ews_id, verdict, detail) per row,
    # where verdict is "ok" (promote), "mismatch" (reset) or "failed".
    def _evaluate(self, pairs: list[tuple[str, Any]],
                  by_id: dict[str, Any]) -> list[tuple[str, str, Any]]:
        out: list[tuple[str, str, Any]] = []
        for ews_id, item in pairs:
            try:
                reason = self._mismatch(by_id[ews_id], item)
            except Exception as exc:  # noqa: BLE001 - a poisoned row must not abort the pass
                out.append((ews_id, "failed", f"{type(exc).__name__}: {exc}"))
                continue
            out.append((ews_id, "ok" if reason is None else "mismatch", reason))
        return out

    # Runs on a worker thread: every branch here is a store write.
    def _apply(self, verdicts: list[tuple[str, str, Any]]
               ) -> tuple[int, int, int, list[dict[str, Any]]]:
        verified = reset = failed = 0
        reasons: list[dict[str, Any]] = []
        for ews_id, verdict, detail in verdicts:
            if verdict == "failed":
                failed += 1
                logger.warning("verify could not evaluate %s: %s", ews_id, detail)
            elif verdict == "ok":
                if not self.store.mark_verified(ews_id):
                    # Row moved out from under us (concurrent run, race with
                    # delete) between the read and this write — this is not a
                    # promotion that actually happened.
                    failed += 1
                    logger.warning(
                        "verify: %s no longer captured when promoting", ews_id)
                    continue
                verified += 1
            else:
                if not self.store.reset_to_live(ews_id):
                    failed += 1
                    logger.warning(
                        "verify: %s no longer captured when resetting (%s)",
                        ews_id, detail)
                    continue
                reset += 1
                reasons.append({"ews_id": ews_id, "reason": detail})
                logger.warning("verify reset %s to live: %s", ews_id, detail)
        return verified, reset, failed, reasons

    def _mismatch(self, row: dict[str, Any], item: Any) -> str | None:
        live_ck = getattr(item, "changekey", None)
        captured_ck = row.get("captured_changekey")
        # Fails CLOSED: no snapshot, or an item that failed to report a
        # changekey, means the check cannot be trusted — never promote.
        if not captured_ck or not live_ck:
            return "changekey unavailable (no captured snapshot or no live value)"
        if live_ck != captured_ck:
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
