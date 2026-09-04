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

_TOKEN_RE = re.compile(r"^[0-9a-f]{32,128}$")


class DownloadRejected(Exception):
    """Any redemption failure. Callers MUST render this as an opaque 404."""


def _links_dir(data_dir: str) -> Path:
    return Path(data_dir) / "download-links"


def mint(data_dir: str, *, path: str, name: str,
         content_type: str = "application/octet-stream",
         ttl_seconds: int = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
    ttl = min(int(ttl_seconds), MAX_TTL_SECONDS)
    token = secrets.token_hex(TOKEN_BYTES)
    record = {"path": str(Path(path).resolve()),
              "name": Path(str(name or "download.bin")).name,
              "content_type": content_type,
              "expires_at": time.time() + ttl, "used": False}
    links = _links_dir(data_dir)
    links.mkdir(parents=True, exist_ok=True)
    (links / f"{token}.json").write_text(json.dumps(record))
    return {"token": token, "name": record["name"],
            "content_type": content_type, "expires_at": record["expires_at"]}


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
    root = Path(data_dir).resolve()
    # Containment: a link may only ever hand out something inside DATA_DIR.
    if not target.is_relative_to(root) or not target.is_file():
        raise DownloadRejected("not servable")

    record["used"] = True
    record_path.write_text(json.dumps(record))
    return {"path": str(target), "name": record.get("name") or target.name,
            "content_type": record.get("content_type")
            or "application/octet-stream"}


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
