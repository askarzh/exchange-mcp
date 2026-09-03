"""Capability-URL uploads: mint a short-lived single-use link, redeem it once.

The link IS the credential (same model as OTA_FW_TOKEN on the candy gateway), so
every rejection path must be indistinguishable from "no such thing" — a wrong
token must never confirm that uploads exist.
"""
import time
from pathlib import Path

import pytest

from ewsmcp import uploads


def test_mint_returns_token_and_records_it(tmp_path):
    rec = uploads.mint(str(tmp_path), "report.pdf", ttl_seconds=900)
    assert len(rec["token"]) >= 32
    assert rec["name"] == "report.pdf"
    assert rec["path"] == str(Path(tmp_path) / "uploads" / "report.pdf")
    assert rec["expires_at"] > time.time()
    assert (Path(tmp_path) / "upload-links" / f"{rec['token']}.json").is_file()


def test_redeem_writes_the_file(tmp_path):
    rec = uploads.mint(str(tmp_path), "notes.txt")
    out = uploads.redeem(str(tmp_path), rec["token"], b"file bytes")
    assert out["path"] == rec["path"]
    assert out["size"] == 10
    assert Path(rec["path"]).read_bytes() == b"file bytes"


def test_redeem_is_single_use(tmp_path):
    rec = uploads.mint(str(tmp_path), "a.txt")
    uploads.redeem(str(tmp_path), rec["token"], b"one")
    with pytest.raises(uploads.UploadRejected):
        uploads.redeem(str(tmp_path), rec["token"], b"two")


def test_redeem_rejects_expired_link(tmp_path):
    rec = uploads.mint(str(tmp_path), "a.txt", ttl_seconds=-1)
    with pytest.raises(uploads.UploadRejected):
        uploads.redeem(str(tmp_path), rec["token"], b"x")


def test_redeem_rejects_unknown_token(tmp_path):
    with pytest.raises(uploads.UploadRejected):
        uploads.redeem(str(tmp_path), "deadbeef" * 8, b"x")


def test_redeem_rejects_token_with_path_separators(tmp_path):
    """A token is a filename component — traversal must not reach the FS."""
    for bad in ("../etc/passwd", "a/b", ".."):
        with pytest.raises(uploads.UploadRejected):
            uploads.redeem(str(tmp_path), bad, b"x")


def test_redeem_rejects_oversized_body(tmp_path):
    rec = uploads.mint(str(tmp_path), "big.bin")
    with pytest.raises(uploads.UploadRejected):
        uploads.redeem(str(tmp_path), rec["token"],
                       b"x" * (uploads.MAX_UPLOAD_BYTES + 1))


def test_mint_sanitises_the_filename(tmp_path):
    """The declared name must not escape the uploads directory."""
    rec = uploads.mint(str(tmp_path), "../../evil.sh")
    written = Path(rec["path"]).resolve()
    assert (Path(tmp_path) / "uploads").resolve() in written.parents
    assert "/" not in rec["name"]


# --- the MCP tool + the HTTP route -------------------------------------------

def test_create_upload_link_tool_returns_absolute_url(tmp_path, db, monkeypatch):
    import sys
    sys.path.insert(0, "tests")
    from test_writes import call, make_account, make_ctx
    account = make_account()
    ctx = make_ctx(tmp_path, db, account)
    ctx.settings.external_url = "https://ews.example.com"
    res = call(ctx, "create_upload_link", {"name": "report.pdf", "ttl_minutes": 5})
    assert res["ok"] is True
    assert res["upload_url"].startswith("https://ews.example.com/upload/")
    assert res["path"].endswith("/uploads/report.pdf")
    assert res["expires_in_minutes"] == 5


def test_upload_route_redeems_then_404s_on_reuse(tmp_path):
    """End-to-end through the ASGI app: first PUT wins, replay is an opaque 404."""
    import asyncio

    from ewsmcp.http import build_app

    class S:
        data_dir = str(tmp_path)
        mcp_api_key = "secret-bearer"     # route must work WITHOUT this
    ctx = type("C", (), {"registry": {}, "manager": None, "counters": {}})()
    app = build_app(ctx, S())
    rec = uploads.mint(str(tmp_path), "x.txt")

    async def put(token, body):
        sent = {}
        msgs = [{"type": "http.request", "body": body, "more_body": False}]
        async def receive():
            return msgs.pop(0)

        async def send(m):
            if m["type"] == "http.response.start":
                sent["status"] = m["status"]
            else:
                sent.setdefault("body", b"")
            sent["body"] = sent.get("body", b"") + m.get("body", b"")
        await app({"type": "http", "path": f"/upload/{token}", "method": "PUT",
                   "headers": []}, receive, send)
        return sent

    first = asyncio.run(put(rec["token"], b"hello"))
    assert first["status"] == 200
    assert (Path(tmp_path) / "uploads" / "x.txt").read_bytes() == b"hello"
    replay = asyncio.run(put(rec["token"], b"again"))
    assert replay["status"] == 404
    bad = asyncio.run(put("f" * 64, b"x"))
    assert bad["status"] == 404
