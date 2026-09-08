"""The GC lane: unreferenced mime/blob files, and only those, eventually go."""

import asyncio
import os
import time

from conftest import make_row, make_settings

from ewsmcp.archive import files
from ewsmcp.archive.gc import GcWorker
from ewsmcp.cache.store import CacheStore


def _old(path, hours=48):
    t = time.time() - hours * 3600
    os.utime(path, (t, t))


def test_gc_removes_only_unreferenced_files_older_than_a_day(db, tmp_path):
    settings = make_settings(data_dir=str(tmp_path / "data"))
    store = CacheStore(db)
    ref_sha, ref_path = files.store_mime(settings.data_dir, b"referenced")
    orphan_sha, orphan_path = files.store_mime(settings.data_dir, b"orphan")
    fresh_sha, fresh_path = files.store_mime(settings.data_dir, b"fresh orphan")
    blob_sha, blob_path = files.store_blob(settings.data_dir, b"att")
    orphan_blob_sha, orphan_blob = files.store_blob(settings.data_dir, b"att-orphan")
    for p in (ref_path, orphan_path, blob_path, orphan_blob):
        _old(p)
    store.upsert_messages([make_row("A1")])
    store.mark_captured("A1", mime_sha256=ref_sha, mime_path=str(ref_path))
    store.replace_attachments("A1", [{"name": "a", "content_type": "x", "size": 3,
                                      "sha256": blob_sha, "is_inline": 0}])

    dry = asyncio.run(GcWorker(settings, store).run(dry_run=True))
    assert dry["removed_files"] == 2 and orphan_path.exists()
    out = asyncio.run(GcWorker(settings, store).run(dry_run=False))
    assert out["removed_files"] == 2 and out["removed_bytes"] == len(b"orphan") + len(b"att-orphan")
    assert ref_path.exists() and blob_path.exists() and fresh_path.exists()
    assert not orphan_path.exists() and not orphan_blob.exists()
    assert out["kept_recent"] == 1


def test_gc_on_an_empty_data_dir_is_a_noop(db, tmp_path):
    settings = make_settings(data_dir=str(tmp_path / "nothing-here"))
    out = asyncio.run(GcWorker(settings, CacheStore(db)).run(dry_run=False))
    assert out == {"scanned": 0, "removed_files": 0, "removed_bytes": 0,
                   "kept_recent": 0, "error": None}
