"""Capability-URL downloads: unguessable, single use, short-lived, contained."""

import json
import time
from pathlib import Path

import pytest

from ewsmcp import downloads


def _file(tmp_path, name="msg.eml", body=b"RAW"):
    data = Path(tmp_path) / "mime"
    data.mkdir(parents=True, exist_ok=True)
    path = data / name
    path.write_bytes(body)
    return path


def test_mint_returns_an_unguessable_token(tmp_path):
    path = _file(tmp_path)
    rec = downloads.mint(str(tmp_path), path=str(path), name="msg.eml",
                         content_type="message/rfc822")
    assert len(rec["token"]) == 64 and rec["name"] == "msg.eml"
    assert rec["content_type"] == "message/rfc822"
    assert rec["expires_at"] > time.time()


def test_redeem_returns_the_file_once(tmp_path):
    path = _file(tmp_path)
    token = downloads.mint(str(tmp_path), path=str(path), name="msg.eml")["token"]
    got = downloads.redeem(str(tmp_path), token)
    assert Path(got["path"]).read_bytes() == b"RAW"
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_expired_links_are_rejected_and_removed(tmp_path):
    path = _file(tmp_path)
    token = downloads.mint(str(tmp_path), path=str(path), name="msg.eml",
                           ttl_seconds=1)["token"]
    link = Path(tmp_path) / "download-links" / f"{token}.json"
    rec = json.loads(link.read_text())
    rec["expires_at"] = time.time() - 1
    link.write_text(json.dumps(rec))
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)
    assert not link.exists()


@pytest.mark.parametrize("token", ["../../etc/passwd", "", "ZZZZ", "a" * 200])
def test_malformed_tokens_never_reach_the_filesystem(tmp_path, token):
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_a_link_pointing_outside_data_dir_is_refused(tmp_path):
    outside = tmp_path.parent / "secret.txt"
    outside.write_bytes(b"nope")
    token = downloads.mint(str(tmp_path), path=str(outside), name="x")["token"]
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_a_vanished_file_is_refused(tmp_path):
    path = _file(tmp_path)
    token = downloads.mint(str(tmp_path), path=str(path), name="msg.eml")["token"]
    path.unlink()
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_sweep_removes_used_and_expired_records(tmp_path):
    path = _file(tmp_path)
    used = downloads.mint(str(tmp_path), path=str(path), name="a")["token"]
    downloads.redeem(str(tmp_path), used)
    downloads.mint(str(tmp_path), path=str(path), name="b")
    assert downloads.sweep(str(tmp_path)) == 1
