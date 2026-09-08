"""Capture: pull the raw MIME and every attachment of one batch of live mail.

The MIME is the durable artifact — it round-trips into any mail client and
carries nested ItemAttachments that have no standalone bytes. Attachment blobs
are stored SEPARATELY as well, deduplicated by hash, so `get_attachment` on
archived mail costs one file read instead of a MIME parse.

The `attachments` projection returns Attachment objects (id, name, metadata)
but NOT file bytes: each `FileAttachment.content` access below is its own
GetAttachment round-trip to EWS. A batch of 25 messages with several
attachments each is therefore several times that many EWS calls, not one —
worth remembering before raising BATCH_SIZE.

Failure posture: one bad item is logged and skipped (its row stays `live` and
is retried next cycle); a full disk stops the whole run before any fetch.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from exchangelib.attachments import FileAttachment

from . import files
from .policy import ArchivePolicy

logger = logging.getLogger(__name__)

# Never fetch the full item: without a projection exchangelib pulls every
# field. `attachments` here is the metadata list (see module docstring) and
# `changekey` is recorded so the verifier can detect a post-capture edit.
CAPTURE_FIELDS = ["mime_content", "changekey", "attachments", "has_attachments",
                  "size"]
BATCH_SIZE = 25


class Capturer:
    def __init__(self, settings: Any, gateway: Any, store: Any,
                 policy: ArchivePolicy,
                 too_large_ids: set[str] | None = None) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.policy = policy
        # Items over ARCHIVE_MAX_ITEM_MB stay `live` forever, and the
        # candidate query is date-ordered and limited: 25 of them at the head
        # of the queue would fill every page and stall capture for good. The
        # runner owns this set so it survives across passes (and is dropped
        # on restart, which is the intended way to retry after raising the
        # cap).
        self.too_large_ids = set() if too_large_ids is None else too_large_ids

    async def run(self, *, limit: int = BATCH_SIZE,
                  dry_run: bool = True) -> dict[str, Any]:
        # Folder resolution and both candidate queries are blocking DB
        # calls: one hop off the event loop for the whole selection.
        def select() -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
            sel = {"folder_ids": self.policy.folder_ids(self.store),
                   "before_ts": self.policy.capture_cutoff_ts(),
                   "exclude_categories": list(self.policy.exclude_categories),
                   "exclude_ids": sorted(self.too_large_ids)}
            return (self.store.archive_candidate_count(**sel),
                    self.store.archive_candidates(**sel, limit=int(limit)), sel)

        total, rows, _selector = await asyncio.to_thread(select)
        sample = [{"ews_id": r["ews_id"], "subject": r["subject"],
                   "date": r["date_iso"]} for r in rows[:10]]
        result: dict[str, Any] = {"candidates": total, "captured": 0, "failed": 0,
                                  "too_large": 0, "sample": sample, "stopped": None}
        if dry_run or not rows:
            return result
        try:
            await asyncio.to_thread(files.ensure_free_space,
                                    self.settings.data_dir, self.policy.min_free_gb)
        except files.DiskFull as exc:
            logger.error("capture stopped: %s", exc)
            result["stopped"] = str(exc)
            return result
        ids = [r["ews_id"] for r in rows]
        captured, failed, too_large = await self.gateway.call(
            lambda account: self._capture_batch(account, ids))
        result["captured"], result["failed"], result["too_large"] = (
            captured, failed, len(too_large))
        for ews_id in too_large:
            result["sample"].append({"ews_id": ews_id, "reason": "too_large"})
        return result

    # Runs on the EWS pool (sync).
    def _capture_batch(self, account: Any,
                       ids: list[str]) -> tuple[int, int, list[str]]:
        captured = failed = 0
        too_large: list[str] = []
        max_bytes = self.settings.archive_max_item_mb * 1024 * 1024
        fetched = account.fetch(ids=[(i, None) for i in ids],
                                only_fields=CAPTURE_FIELDS)
        # zip(strict=True): a `fetched` shorter than `ids` (a malformed
        # double, or exchangelib silently dropping a bogus id) must not
        # silently skip the tail — every id gets a captured/failed verdict.
        for raw_id, item in zip(ids, fetched, strict=True):
            try:
                if isinstance(item, Exception):
                    raise item
                size = getattr(item, "size", None)
                if size and size > max_bytes:
                    # Left `live`, retried never automatically — it stays a
                    # visible skip (not a failure) until either the item
                    # shrinks (unlikely) or an operator raises the cap.
                    too_large.append(raw_id)
                    self.too_large_ids.add(raw_id)
                    continue
                self._capture_one(raw_id, item)
                captured += 1
            except Exception as exc:  # noqa: BLE001 - skip one, keep the batch
                failed += 1
                logger.warning("capture failed for %s: %s: %s", raw_id,
                               type(exc).__name__, exc)
        return captured, failed, too_large

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
            if isinstance(att, FileAttachment):
                # `.content` is a lazy property: with an attachment_id it is
                # its own GetAttachment round-trip (see module docstring).
                content = att.content
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
        changekey = getattr(item, "changekey", None)
        changed = self.store.mark_captured(raw_id, mime_sha256=sha, mime_path=str(path),
                                           changekey=changekey)
        if not changed:
            raise RuntimeError(
                f"{raw_id} was no longer live when capture finished (state "
                "changed underneath) — skipped")
        try:
            self.store.replace_attachments(raw_id, rows)
        except Exception:
            # The row is already flipped to `captured` with no attachment
            # inventory — undo that so it is retried whole next cycle rather
            # than stranded `captured` with nothing to show for it.
            self.store.reset_to_live(raw_id)
            raise
