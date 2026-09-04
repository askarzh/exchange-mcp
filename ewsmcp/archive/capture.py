"""Capture: pull the raw MIME and every attachment of one batch of live mail.

The MIME is the durable artifact — it round-trips into any mail client and
carries nested ItemAttachments that have no standalone bytes. Attachment blobs
are stored SEPARATELY as well, deduplicated by hash, so `get_attachment` on
archived mail costs one file read instead of a MIME parse.

Failure posture: one bad item is logged and skipped (its row stays `live` and
is retried next cycle); a full disk stops the whole run before any fetch.
"""

from __future__ import annotations

import logging
from typing import Any

from . import files
from .policy import ArchivePolicy

logger = logging.getLogger(__name__)

# Never fetch the full item: without a projection exchangelib pulls every
# field, and `attachments` alone already carries the bytes we want.
CAPTURE_FIELDS = ["mime_content", "changekey", "attachments", "has_attachments"]
BATCH_SIZE = 25


class Capturer:
    def __init__(self, settings: Any, gateway: Any, store: Any,
                 policy: ArchivePolicy) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.policy = policy

    async def run(self, *, limit: int = BATCH_SIZE,
                  dry_run: bool = True) -> dict[str, Any]:
        folder_ids = self.policy.folder_ids(self.store)
        cutoff = self.policy.capture_cutoff_ts()
        selector = {"folder_ids": folder_ids, "before_ts": cutoff,
                    "exclude_categories": list(self.policy.exclude_categories)}
        total = self.store.archive_candidate_count(**selector)
        rows = self.store.archive_candidates(**selector, limit=int(limit))
        sample = [{"ews_id": r["ews_id"], "subject": r["subject"],
                   "date": r["date_iso"]} for r in rows[:10]]
        result: dict[str, Any] = {"candidates": total, "captured": 0, "failed": 0,
                                  "sample": sample, "stopped": None}
        if dry_run or not rows:
            return result
        try:
            files.ensure_free_space(self.settings.data_dir, self.policy.min_free_gb)
        except files.DiskFull as exc:
            logger.error("capture stopped: %s", exc)
            result["stopped"] = str(exc)
            return result
        ids = [r["ews_id"] for r in rows]
        captured, failed = await self.gateway.call(
            lambda account: self._capture_batch(account, ids))
        result["captured"], result["failed"] = captured, failed
        return result

    # Runs on the EWS pool (sync).
    def _capture_batch(self, account: Any, ids: list[str]) -> tuple[int, int]:
        captured = failed = 0
        fetched = account.fetch(ids=[(i, None) for i in ids],
                                only_fields=CAPTURE_FIELDS)
        for raw_id, item in zip(ids, fetched):
            try:
                if isinstance(item, Exception):
                    raise item
                self._capture_one(raw_id, item)
                captured += 1
            except Exception as exc:  # noqa: BLE001 - skip one, keep the batch
                failed += 1
                logger.warning("capture failed for %s: %s: %s", raw_id,
                               type(exc).__name__, exc)
        return captured, failed

    def _capture_one(self, raw_id: str, item: Any) -> None:
        mime = getattr(item, "mime_content", None)
        if not isinstance(mime, (bytes, bytearray)):
            raise ValueError(f"no mime_content for {raw_id}")
        data_dir = self.settings.data_dir
        sha, path = files.store_mime(data_dir, bytes(mime))
        rows: list[dict[str, Any]] = []
        for att in list(getattr(item, "attachments", None) or []):
            name = getattr(att, "name", None) or "attachment"
            inline = 1 if getattr(att, "is_inline", False) else 0
            # A FileAttachment always carries its bytes on `.content`; an
            # ItemAttachment (a nested message) never does. isinstance()
            # against the real exchangelib class would also work in
            # production but excludes any FileAttachment-shaped double in
            # tests, so key off the shape instead.
            if hasattr(att, "content"):
                content = getattr(att, "content", None)
                if not isinstance(content, (bytes, bytearray)):
                    raise ValueError(f"attachment {name!r} of {raw_id} has no bytes")
                blob_sha, _blob_path = files.store_blob(data_dir, bytes(content))
                rows.append({"name": name,
                             "content_type": getattr(att, "content_type", None),
                             "size": len(content), "sha256": blob_sha,
                             "is_inline": inline})
            else:
                # ItemAttachment (a nested message): it lives inside the MIME
                # only, so there is no blob and nothing to hash.
                rows.append({"name": name, "content_type": "message/rfc822",
                             "size": getattr(att, "size", None), "sha256": None,
                             "is_inline": inline})
        # Flip the row FIRST: mark_captured is guarded to only transition a
        # `live` row, so a 0 here means something else already moved it
        # (concurrent capture pass, a race with delete) — treat that as a
        # skip rather than recording attachments against a row we no longer
        # own. The mime/blob bytes already written are harmless: they are
        # content-addressed and deduplicated, so nothing is left dangling in
        # a way that costs space or corrupts another row.
        changed = self.store.mark_captured(raw_id, mime_sha256=sha, mime_path=str(path))
        if not changed:
            raise RuntimeError(
                f"{raw_id} was no longer live when capture finished (state "
                "changed underneath) — skipped")
        self.store.replace_attachments(raw_id, rows)
