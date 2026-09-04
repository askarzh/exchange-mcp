"""Capability-URL downloads — get a file OUT of DATA_DIR without a standing secret.

The mirror image of ``uploads.py``, and for the same reason: MCP has no file
channel, so a 4 MB .eml would otherwise travel as base64 through the model's
context. ``get_raw_message`` mints a link instead:

    get_raw_message(id)  ->  https://<host>/download/<token>
    curl -O -J "<url>"

The URL IS the credential, so the token is 256 bits, single use, short-lived,
bound to ONE path that must live under DATA_DIR, and every rejection renders
as an identical opaque 404 — probing must not distinguish expired from used
from never-existed.
"""

from __future__ import annotations

import json
import re
import secrets
import time
from pathlib import Path
from typing import Any

TOKEN_BYTES = 32
DEFAULT_TTL_SECONDS = 15 * 60
MAX_TTL_SECONDS = 24 * 60 * 60

MIME_DIRNAME = "mime"
BLOB_DIRNAME = "blobs"

_TOKEN_RE = re.compile(r"^[0-9a-f]{32,128}$")
# Header values ride verbatim into `content-disposition`/`content-type` in
# http.py, and the name/type here are ultimately attacker-influenced (mail
# subjects, attachment filenames). Keep only what's safe on an HTTP header
# line — no CR/LF, no quote, no control characters.
_NAME_RE = re.compile(r"[^A-Za-z0-9._ -]+")
_CONTENT_TYPE_RE = re.compile(r"^[\w.+-]+/[\w.+-]+$")


class DownloadRejected(Exception):
    """Any redemption failure. Callers MUST render this as an opaque 404."""


def _links_dir(data_dir: str) -> Path:
    return Path(data_dir) / "download-links"


def safe_header_name(name: str) -> str:
    """Reduce a caller-supplied name to something safe to embed in a
    `content-disposition` header — no path separators, no CR/LF, no quotes."""
    base = Path(str(name or "")).name           # strips any directory part
    cleaned = _NAME_RE.sub("_", base).strip("._ ")
    return cleaned or "download.bin"


def safe_content_type(content_type: str) -> str:
    """Reduce a caller-supplied content type to a bare `type/subtype` token,
    or fall back to a safe default — no header-injection payloads survive."""
    ct = str(content_type or "")
    return ct if _CONTENT_TYPE_RE.match(ct) else "application/octet-stream"


def _allowed_root(data_dir: str, target: Path) -> bool:
    """A download may only ever hand out a file that lives under
    ``{DATA_DIR}/mime/`` or ``{DATA_DIR}/blobs/`` — never audit logs,
    upload/download link records, or anything else under DATA_DIR."""
    root = Path(data_dir).resolve()
    mime_root = root / MIME_DIRNAME
    blob_root = root / BLOB_DIRNAME
    return target.is_relative_to(mime_root) or target.is_relative_to(blob_root)


def mint(data_dir: str, *, path: str, name: str,
         content_type: str = "application/octet-stream",
         ttl_seconds: int = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
    target = Path(path).resolve()
    if not _allowed_root(data_dir, target):
        raise ValueError("download path must live under mime/ or blobs/")
    ttl = min(int(ttl_seconds), MAX_TTL_SECONDS)
    token = secrets.token_hex(TOKEN_BYTES)
    safe_name = safe_header_name(name)
    safe_ct = safe_content_type(content_type)
    record = {"path": str(target),
              "name": safe_name,
              "content_type": safe_ct,
              "expires_at": time.time() + ttl, "used": False}
    links = _links_dir(data_dir)
    links.mkdir(parents=True, exist_ok=True)
    (links / f"{token}.json").write_text(json.dumps(record))
    return {"token": token, "name": safe_name,
            "content_type": safe_ct, "expires_at": record["expires_at"]}


def redeem(data_dir: str, token: str) -> dict[str, Any]:
    """Consume a link exactly once and return what to serve."""
    # Shape first: the token is used as a filename, so `../x` must never
    # become a path lookup.
    if not isinstance(token, str) or not _TOKEN_RE.match(token):
        raise DownloadRejected("bad token")
    record_path = _links_dir(data_dir) / f"{token}.json"
    try:
        record = json.loads(record_path.read_text())
    except Exception as exc:                      # missing/corrupt — same answer
        raise DownloadRejected("no such link") from exc
    if record.get("used"):
        raise DownloadRejected("already used")
    if float(record.get("expires_at", 0)) < time.time():
        record_path.unlink(missing_ok=True)
        raise DownloadRejected("expired")

    target = Path(str(record.get("path", ""))).resolve()
    # Containment: a link may only ever hand out something under mime/ or
    # blobs/ — never audit/, upload-links/, download-links/, etc.
    if not _allowed_root(data_dir, target) or not target.is_file():
        raise DownloadRejected("not servable")

    record["used"] = True
    record_path.write_text(json.dumps(record))
    # Defense in depth: re-sanitize on the way out too, in case a record was
    # ever written by a path that bypassed mint()'s sanitizing.
    return {"path": str(target),
            "name": safe_header_name(record.get("name") or target.name),
            "content_type": safe_content_type(record.get("content_type"))}


def sweep(data_dir: str) -> int:
    """Delete expired/used link records. Best-effort housekeeping."""
    links = _links_dir(data_dir)
    if not links.is_dir():
        return 0
    now, removed = time.time(), 0
    for f in links.glob("*.json"):
        try:
            rec = json.loads(f.read_text())
            if rec.get("used") or float(rec.get("expires_at", 0)) < now:
                f.unlink(missing_ok=True)
                removed += 1
        except Exception:  # noqa: BLE001 - a corrupt record is worth removing
            f.unlink(missing_ok=True)
            removed += 1
    return removed
