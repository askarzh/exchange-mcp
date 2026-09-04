"""Content-addressed storage for raw MIME and attachment blobs.

Two invariants, both scar tissue from the failure modes in spec §5:

1. **Temp-then-rename inside the same directory.** The bytes are written to
   ``<name>.tmp-<random>`` next to their final home, re-hashed FROM DISK, and
   only then ``os.replace``d. A crash therefore never leaves wrong bytes under
   a right (content-addressed) name, and the rename is atomic because source
   and destination share a filesystem.
2. **Free space is checked before a batch, not after a failure.** A full disk
   stops the run with a clear error instead of writing truncated files.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import shutil
from pathlib import Path

MIME_DIRNAME = "mime"
BLOB_DIRNAME = "blobs"
_READ_CHUNK = 1024 * 1024


class DiskFull(RuntimeError):
    """Free space fell below ARCHIVE_MIN_FREE_GB — the run must stop."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(_READ_CHUNK)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def mime_path(data_dir: str, sha: str) -> Path:
    return Path(data_dir) / MIME_DIRNAME / f"{sha}.eml"


def blob_path(data_dir: str, sha: str) -> Path:
    return Path(data_dir) / BLOB_DIRNAME / sha[:2] / sha


def _write_verified(dest: Path, data: bytes, sha: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and sha256_file(dest) == sha:
        return  # already stored, byte-identical — dedup across messages
    tmp = dest.parent / f"{dest.name}.tmp-{secrets.token_hex(8)}"
    try:
        tmp.write_bytes(data)
        if sha256_file(tmp) != sha:
            raise OSError(f"hash mismatch after writing {tmp}")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def store_mime(data_dir: str, data: bytes) -> tuple[str, Path]:
    sha = sha256_bytes(data)
    dest = mime_path(data_dir, sha)
    _write_verified(dest, data, sha)
    return sha, dest


def store_blob(data_dir: str, data: bytes) -> tuple[str, Path]:
    sha = sha256_bytes(data)
    dest = blob_path(data_dir, sha)
    _write_verified(dest, data, sha)
    return sha, dest


def free_gb(data_dir: str) -> float:
    root = Path(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(str(root))[2] / 1024**3


def ensure_free_space(data_dir: str, min_free_gb: float) -> None:
    free = free_gb(data_dir)
    if free < float(min_free_gb):
        raise DiskFull(
            f"only {free:.2f} GB free under {data_dir}; ARCHIVE_MIN_FREE_GB is "
            f"{min_free_gb}. Capture stopped — free space or lower the floor."
        )


def blob_store_bytes(data_dir: str) -> int:
    total = 0
    for name in (MIME_DIRNAME, BLOB_DIRNAME):
        root = Path(data_dir) / name
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if path.is_file():
                total += path.stat().st_size
    return total
