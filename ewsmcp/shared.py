"""Publishing saved files into the shared space.

DATA_DIR is deliberately private: it holds the cache, the audit chain and the
alias map, none of which may leak. But an attachment the user asked us to save
is useless there — files-mcp lists /shared's root only, so a file in
{DATA_DIR}/attachments can neither be listed, linked, nor read by another
service. `publish` puts a copy where the rest of the stack can see it, without
moving anything else out of DATA_DIR.

Copy, not hardlink: DATA_DIR is typically a named volume and SHARED_DIR a bind
mount, so they are different filesystems and os.link would fail with EXDEV.
"""
from __future__ import annotations

import filecmp
import logging
import re
import shutil
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

MAX_SUFFIX_ATTEMPTS = 100


def safe_name(name: str) -> str:
    """Reduce a name to a single harmless filename component."""
    base = Path(str(name or "")).name
    cleaned = re.sub(r"[^\w.\-]+", "_", base).strip("._")
    return cleaned or "attachment.bin"


def publish(shared_dir: str, src: Path) -> Optional[str]:
    """Copy `src` into the flat root of `shared_dir`; return the name used.

    Returns None when no shared space is configured or present — publishing is
    a convenience, never a precondition for having saved the file.
    """
    if not shared_dir:
        return None
    root = Path(shared_dir)
    if not root.is_dir():
        log.warning("SHARED_DIR %s is not a directory — not publishing %s",
                    shared_dir, src.name)
        return None

    name = safe_name(src.name)
    stem, dot, ext = name.partition(".")
    for i in range(MAX_SUFFIX_ATTEMPTS):
        candidate = name if i == 0 else f"{stem}_{i}{dot}{ext}"
        dest = root / candidate
        if dest.exists():
            # Same bytes already published (the caller saved this attachment
            # before) — reuse it. A DIFFERENT file under that name belongs to
            # someone else and must not be overwritten.
            if dest.is_file() and filecmp.cmp(src, dest, shallow=False):
                return candidate
            continue
        shutil.copyfile(src, dest)
        return candidate

    log.warning("no free name for %s in %s after %d attempts",
                name, shared_dir, MAX_SUFFIX_ATTEMPTS)
    return None
