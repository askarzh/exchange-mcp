"""Saved attachments have to leave the container's private DATA_DIR.

get_attachment(mode="save") writes into {DATA_DIR}/attachments, which is a named
volume nothing else can read — files-mcp cannot list it, no /dl/ link can be
minted for it, and gemini-mcp cannot open it. When SHARED_DIR is configured we
also publish a copy into its flat root, which is the level files-mcp lists.
"""
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ewsmcp import shared as shared_mod
from tests.test_mail_read import _account, _att, _ctx, _msg, _run


# --- the publish helper in isolation ----------------------------------------


def test_publish_copies_into_the_flat_root(tmp_path):
    src = tmp_path / "data" / "attachments" / "report.pdf"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"%PDF-1.7")
    shared = tmp_path / "shared"
    shared.mkdir()

    name = shared_mod.publish(str(shared), src)

    assert name == "report.pdf"
    assert (shared / name).read_bytes() == b"%PDF-1.7"


def test_publish_is_idempotent_for_identical_content(tmp_path):
    src = tmp_path / "a" / "report.pdf"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"same")
    shared = tmp_path / "shared"
    shared.mkdir()

    first = shared_mod.publish(str(shared), src)
    second = shared_mod.publish(str(shared), src)

    assert first == second == "report.pdf"
    assert len([p for p in shared.iterdir() if p.is_file()]) == 1


def test_publish_never_clobbers_a_different_file(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "report.pdf").write_bytes(b"SOMEONE ELSES FILE")

    src = tmp_path / "a" / "report.pdf"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"mine")

    name = shared_mod.publish(str(shared), src)

    assert name != "report.pdf"
    assert (shared / "report.pdf").read_bytes() == b"SOMEONE ELSES FILE"
    assert (shared / name).read_bytes() == b"mine"


def test_publish_returns_none_when_shared_dir_is_absent(tmp_path):
    src = tmp_path / "a" / "report.pdf"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"x")

    assert shared_mod.publish("", src) is None
    assert shared_mod.publish(str(tmp_path / "does-not-exist"), src) is None


def test_publish_sanitises_the_name(tmp_path):
    src = tmp_path / "a" / "weird name?.csv"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"a,b\n")
    shared = tmp_path / "shared"
    shared.mkdir()

    name = shared_mod.publish(str(shared), src)

    assert "?" not in name and " " not in name
    assert (shared / name).exists()


# --- wired into get_attachment ----------------------------------------------


def test_save_publishes_to_shared(tmp_path, db):
    shared = tmp_path / "shared"
    shared.mkdir()
    item = _msg("RAW-A=", attachments=[_att("contract.pdf", b"%PDF-junk",
                                            "application/pdf")])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account, shared_dir=str(shared))

    res = _run(ctx, "get_attachment", message_id="RAW-A=", mode="save")

    assert res["ok"] is True
    assert res["shared_name"] == "contract.pdf"
    assert (shared / "contract.pdf").read_bytes() == b"%PDF-junk"
    # the canonical copy still lands in DATA_DIR
    assert Path(res["saved_path"]).read_bytes() == b"%PDF-junk"


def test_save_without_shared_dir_omits_shared_name(tmp_path, db):
    item = _msg("RAW-A=", attachments=[_att("contract.pdf", b"%PDF-junk",
                                            "application/pdf")])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account)  # no shared_dir configured

    res = _run(ctx, "get_attachment", message_id="RAW-A=", mode="save")

    assert res["ok"] is True
    assert "shared_name" not in res
    assert Path(res["saved_path"]).exists()


def test_publish_failure_does_not_fail_the_save(tmp_path, db):
    """The attachment is already safely on disk — publishing is a convenience."""
    shared = tmp_path / "shared"
    shared.mkdir()
    item = _msg("RAW-A=", attachments=[_att("contract.pdf", b"%PDF-junk",
                                            "application/pdf")])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account, shared_dir=str(shared))

    def boom(*a, **kw):
        raise OSError("disk full")

    import ewsmcp.tools.mail_read as mr
    original = mr.shared.publish
    mr.shared.publish = boom
    try:
        res = _run(ctx, "get_attachment", message_id="RAW-A=", mode="save")
    finally:
        mr.shared.publish = original

    assert res["ok"] is True
    assert "shared_name" not in res
    assert Path(res["saved_path"]).exists()
