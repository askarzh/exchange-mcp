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
    with pytest.raises(ValueError):
        downloads.mint(str(tmp_path), path=str(outside), name="x")


def test_a_path_under_audit_is_refused_at_mint(tmp_path):
    audit = Path(tmp_path) / "audit" / "log.json"
    audit.parent.mkdir(parents=True, exist_ok=True)
    audit.write_bytes(b"x")
    with pytest.raises(ValueError):
        downloads.mint(str(tmp_path), path=str(audit), name="log.json")


def test_a_path_under_audit_is_refused_at_redeem(tmp_path):
    # Simulate a record that bypassed mint()'s containment check (e.g. a
    # stale/tampered record file) — redeem() must narrow the same way.
    audit = Path(tmp_path) / "audit" / "log.json"
    audit.parent.mkdir(parents=True, exist_ok=True)
    audit.write_bytes(b"x")
    links = Path(tmp_path) / "download-links"
    links.mkdir(parents=True, exist_ok=True)
    token = "a" * 64
    record = {"path": str(audit.resolve()), "name": "log.json",
              "content_type": "application/octet-stream",
              "expires_at": time.time() + 60, "used": False}
    (links / f"{token}.json").write_text(json.dumps(record))
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_header_injection_in_name_is_stripped(tmp_path):
    path = _file(tmp_path)
    rec = downloads.mint(str(tmp_path), path=str(path),
                         name="evil.eml\r\nX-Injected: 1")
    assert "\r" not in rec["name"] and "\n" not in rec["name"]
    got = downloads.redeem(str(tmp_path), rec["token"])
    assert "\r" not in got["name"] and "\n" not in got["name"]


def test_bad_content_type_falls_back_to_octet_stream(tmp_path):
    path = _file(tmp_path)
    rec = downloads.mint(str(tmp_path), path=str(path), name="x",
                         content_type="text/plain\r\nX: y")
    assert rec["content_type"] == "application/octet-stream"
    got = downloads.redeem(str(tmp_path), rec["token"])
    assert got["content_type"] == "application/octet-stream"


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


def test_a_non_ascii_name_survives_in_filename_star(tmp_path):
    """RFC 5987: `filename=` keeps an ASCII reduction for old clients, but
    the real name rides in `filename*` percent-encoded — otherwise
    "Отчёт.pdf" is served as "pdf"."""
    path = _file(tmp_path)
    rec = downloads.mint(str(tmp_path), path=str(path), name="Отчёт.pdf")
    assert rec["name"] == "pdf"                      # the ASCII reduction
    got = downloads.redeem(str(tmp_path), rec["token"])
    assert got["orig_name"] == "Отчёт.pdf"           # the original, intact
    value = downloads.content_disposition(got["orig_name"])
    assert value.startswith('attachment; filename="pdf"; ')
    assert "filename*=UTF-8''%D0%9E" in value and value.endswith(".pdf")


def test_the_disposition_header_is_always_one_ascii_line(tmp_path):
    for name in ("evil.eml\r\nX-Injected: 1", 'q"uote.pdf', "Отчёт.pdf",
                 "../../etc/passwd", ""):
        value = downloads.content_disposition(name)
        value.encode("ascii")                        # never raises
        assert "\r" not in value and "\n" not in value
        assert value.count('"') == 2                 # only the two we wrote


def test_a_unicode_content_type_does_not_pass_as_a_token(tmp_path):
    """\\w is Unicode-aware by default; the regex is re.ASCII so a type like
    "application/pdfč" falls back instead of riding into the header."""
    path = _file(tmp_path)
    rec = downloads.mint(str(tmp_path), path=str(path), name="x",
                         content_type="application/pdfč")
    assert rec["content_type"] == "application/octet-stream"
