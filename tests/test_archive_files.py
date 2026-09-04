"""Blob/MIME storage: content-addressed, temp-then-rename, free-space guard."""

import hashlib

import pytest

from ewsmcp.archive import files


def test_store_mime_is_content_addressed(tmp_path):
    data = b"From: a@b\r\nSubject: hi\r\n\r\nbody"
    sha, path = files.store_mime(str(tmp_path), data)
    assert sha == hashlib.sha256(data).hexdigest()
    assert path == tmp_path / "mime" / f"{sha}.eml"
    assert path.read_bytes() == data


def test_store_blob_shards_on_the_first_two_hex_chars(tmp_path):
    sha, path = files.store_blob(str(tmp_path), b"pdf-bytes")
    assert path == tmp_path / "blobs" / sha[:2] / sha
    assert path.read_bytes() == b"pdf-bytes"
    assert files.sha256_file(path) == sha


def test_storing_the_same_bytes_twice_is_idempotent(tmp_path):
    sha1, path1 = files.store_blob(str(tmp_path), b"same")
    sha2, path2 = files.store_blob(str(tmp_path), b"same")
    assert (sha1, path1) == (sha2, path2)
    assert len(list((tmp_path / "blobs" / sha1[:2]).iterdir())) == 1


def test_no_temp_files_survive_a_successful_write(tmp_path):
    files.store_blob(str(tmp_path), b"x")
    assert not list(tmp_path.rglob("*.tmp-*"))


def test_a_corrupt_existing_file_under_the_right_name_is_rewritten(tmp_path):
    sha, path = files.store_blob(str(tmp_path), b"good")
    path.write_bytes(b"CORRUPT")
    sha2, path2 = files.store_blob(str(tmp_path), b"good")
    assert (sha2, path2) == (sha, path)
    assert path.read_bytes() == b"good"


def test_ensure_free_space_raises_when_below_the_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(files.shutil, "disk_usage",
                        lambda p: (100, 99, 1 * 1024 ** 3))  # 1 GiB free
    with pytest.raises(files.DiskFull, match="ARCHIVE_MIN_FREE_GB"):
        files.ensure_free_space(str(tmp_path), 2.0)


def test_ensure_free_space_passes_when_above_the_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(files.shutil, "disk_usage",
                        lambda p: (100, 1, 50 * 1024 ** 3))
    files.ensure_free_space(str(tmp_path), 2.0)  # no raise


def test_mime_path_rejects_a_non_hex_sha(tmp_path):
    with pytest.raises(ValueError, match="invalid sha256"):
        files.mime_path(str(tmp_path), "../etc/passwd")


def test_blob_path_rejects_a_short_sha(tmp_path):
    with pytest.raises(ValueError, match="invalid sha256"):
        files.blob_path(str(tmp_path), "ABCD")


def test_a_valid_sha_still_resolves_to_the_documented_layout(tmp_path):
    sha = "a" * 64
    assert files.mime_path(str(tmp_path), sha) == tmp_path / "mime" / f"{sha}.eml"
    assert files.blob_path(str(tmp_path), sha) == tmp_path / "blobs" / sha[:2] / sha


def test_blob_store_bytes_sums_mime_and_blobs(tmp_path):
    files.store_mime(str(tmp_path), b"a" * 10)
    files.store_blob(str(tmp_path), b"b" * 25)
    assert files.blob_store_bytes(str(tmp_path)) == 35
