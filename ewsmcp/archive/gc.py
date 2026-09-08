"""Orphan blob GC: files under mime/ and blobs/ that no row references.

A verification failure resets a row to live and forgets its capture; the
content-addressed files stay behind. Once a week this lane removes every
unreferenced file older than GC_MIN_AGE_S (a capture in flight is never that
old). Referenced = every messages.mime_sha256 and every attachments.sha256.
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from . import files

logger = logging.getLogger(__name__)
GC_MIN_AGE_S = 24 * 3600


class GcWorker:
    def __init__(self, settings: Any, store: Any) -> None:
        self.settings, self.store = settings, store

    async def run(self, *, dry_run: bool = True) -> dict[str, Any]:
        return await asyncio.to_thread(self._run, dry_run)

    def _run(self, dry_run: bool) -> dict[str, Any]:
        out = {"scanned": 0, "removed_files": 0, "removed_bytes": 0,
               "kept_recent": 0, "error": None}
        root = Path(self.settings.data_dir)
        keep_mime = set(self.store.referenced_mime_shas())
        keep_blob = set(self.store.referenced_blob_shas())
        now = time.time()
        for dirname, keep, key in ((files.MIME_DIRNAME, keep_mime, lambda p: p.name.split(".")[0]),
                                   (files.BLOB_DIRNAME, keep_blob, lambda p: p.name)):
            base = root / dirname
            if not base.is_dir():
                continue
            for p in base.rglob("*"):
                if not p.is_file():
                    continue
                out["scanned"] += 1
                if key(p) in keep:
                    continue
                if now - p.stat().st_mtime < GC_MIN_AGE_S:
                    out["kept_recent"] += 1
                    continue
                size = p.stat().st_size
                out["removed_files"] += 1
                out["removed_bytes"] += size
                if not dry_run:
                    try:
                        p.unlink()
                    except OSError as exc:
                        logger.warning("gc could not remove %s: %s", p, exc)
        return out
