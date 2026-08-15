"""Capability-URL uploads — get a local file into DATA_DIR without a standing secret.

Why this exists
---------------
MCP has no file-upload primitive: the only client→server channel is JSON-RPC
arguments, so bytes would have to travel as base64 through the model's context
(~1.37x expansion, then tokenised — a 1 MB file costs ~350k tokens). That is fine
for small generated content and hopeless for a real document.

So the model mints a link instead of carrying the bytes:

    create_upload_link(name)  ->  https://<host>/upload/<token>
    curl -T report.pdf "<url>"                      # plain PUT, no headers
    add_attachment(draft_id, path="/data/uploads/report.pdf")

The URL *is* the credential — the same model as the candy gateway's OTA_FW_TOKEN.
That means the token must be unguessable and every rejection must look identical
to "no such thing", so probing cannot confirm that uploads exist at all.

Deliberate limits: single use, short TTL, one pre-declared filename, one
directory, and a size cap. A leaked link is therefore worth at most one write of
one known name into a directory that only ever feeds add_attachment.
"""
from __future__ import annotations

import json
import re
import secrets
import time
from pathlib import Path
from typing import Any, Dict

TOKEN_BYTES = 32                       # 256 bits — not guessable
DEFAULT_TTL_SECONDS = 15 * 60
MAX_TTL_SECONDS = 24 * 60 * 60
MAX_UPLOAD_BYTES = 25 * 1024 * 1024    # mirrors the nginx client_max_body_size

_TOKEN_RE = re.compile(r"^[0-9a-f]{32,128}$")


class UploadRejected(Exception):
    """Any redemption failure. Callers MUST render this as an opaque 404.

    One exception type on purpose: an attacker must not be able to tell
    "expired" from "already used" from "never existed".
    """


def _links_dir(data_dir: str) -> Path:
    return Path(data_dir) / "upload-links"


def _uploads_dir(data_dir: str) -> Path:
    return Path(data_dir) / "uploads"


def safe_name(name: str) -> str:
    """Reduce a caller-supplied name to a single harmless filename component."""
    base = Path(str(name or "")).name           # strips any directory part
    cleaned = re.sub(r"[^\w.\-]+", "_", base).strip("._")
    return cleaned or "upload.bin"


def mint(data_dir: str, name: str, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> Dict[str, Any]:
    """Create a single-use upload link bound to one sanitised filename."""
    ttl = min(int(ttl_seconds), MAX_TTL_SECONDS)
    token = secrets.token_hex(TOKEN_BYTES)
    fname = safe_name(name)
    expires_at = time.time() + ttl
    links = _links_dir(data_dir)
    links.mkdir(parents=True, exist_ok=True)
    record = {"name": fname, "expires_at": expires_at, "used": False}
    (links / f"{token}.json").write_text(json.dumps(record))
    return {"token": token, "name": fname, "expires_at": expires_at,
            "path": str(_uploads_dir(data_dir) / fname)}


def redeem(data_dir: str, token: str, body: bytes) -> Dict[str, Any]:
    """Consume a link exactly once and write the body. Raises UploadRejected."""
    # Validate the shape BEFORE touching the filesystem: the token is used as a
    # filename, so `../x` must never become a path lookup.
    if not isinstance(token, str) or not _TOKEN_RE.match(token):
        raise UploadRejected("bad token")
    if len(body) > MAX_UPLOAD_BYTES:
        raise UploadRejected("too large")

    record_path = _links_dir(data_dir) / f"{token}.json"
    try:
        record = json.loads(record_path.read_text())
    except Exception as exc:                      # missing/corrupt — same answer
        raise UploadRejected("no such link") from exc

    if record.get("used"):
        raise UploadRejected("already used")
    if float(record.get("expires_at", 0)) < time.time():
        record_path.unlink(missing_ok=True)       # expired links do not linger
        raise UploadRejected("expired")

    dest_dir = _uploads_dir(data_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / safe_name(record.get("name") or "upload.bin")
    dest.write_bytes(body)

    # Burn the link only after the write succeeds, so a failed write can be retried.
    record["used"] = True
    record_path.write_text(json.dumps(record))
    return {"path": str(dest), "name": dest.name, "size": len(body)}


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
        except Exception:
            f.unlink(missing_ok=True)
            removed += 1
    return removed
