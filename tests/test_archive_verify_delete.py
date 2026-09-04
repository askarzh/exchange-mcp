"""Verifier and deleter: the reset-on-mismatch rule and the three delete rails."""

import asyncio
import time

import pytest
from conftest import make_row, make_settings
from test_archive_capture import FakeAccount, FakeAttachment, FakeGatewayFor, FakeItem

from ewsmcp.archive import files
from ewsmcp.archive.delete import Deleter
from ewsmcp.archive.policy import ArchivePolicy
from ewsmcp.archive.verify import VERIFY_FIELDS, Verifier
from ewsmcp.cache.store import CacheStore

NOW = int(time.time())
DAY = 86400


class RecordingAudit:
    def __init__(self):
        self.records = []

    def record(self, **kw):
        self.records.append(kw)


class DeletableItem(FakeItem):
    def __init__(self, raw_id, **kw):
        super().__init__(raw_id, **kw)
        self.deleted = False

    def delete(self):
        self.deleted = True


@pytest.fixture
def captured(db, tmp_path):
    """One captured message with one attachment, files present on disk."""
    settings = make_settings(data_dir=str(tmp_path / "data"))
    store = CacheStore(db)
    store.upsert_messages([make_row("CAP-1", date_ts=NOW - 300 * DAY)])
    sha, path = files.store_mime(settings.data_dir, b"RAW-MIME")
    blob_sha, _ = files.store_blob(settings.data_dir, b"PDFBYTES")
    store.replace_attachments("CAP-1", [
        {"name": "q3.pdf", "content_type": "application/pdf", "size": 8,
         "sha256": blob_sha, "is_inline": 0}])
    store.mark_captured("CAP-1", mime_sha256=sha, mime_path=str(path))
    return store, settings, sha, blob_sha


# --- verifier -----------------------------------------------------------------


def test_verify_promotes_a_matching_capture(captured):
    store, settings, _sha, _blob = captured
    account = FakeAccount({"CAP-1": FakeItem(
        "CAP-1", attachments=[FakeAttachment("q3.pdf", b"PDFBYTES")])})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result == {"verified": 1, "reset": 0, "failed": 0, "reasons": []}
    assert store.get_message("CAP-1")["archive_state"] == "verified"
    assert account.fetch_calls[0][1] == VERIFY_FIELDS


def test_verify_resets_when_the_changekey_moved(captured):
    store, settings, _sha, _blob = captured
    account = FakeAccount({"CAP-1": FakeItem("CAP-1", changekey="CK-DIFFERENT")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1 and result["verified"] == 0
    assert store.get_message("CAP-1")["archive_state"] == "live"
    assert "changekey" in result["reasons"][0]["reason"]


def test_verify_resets_when_the_mime_file_hash_is_wrong(captured):
    store, settings, sha, _blob = captured
    files.mime_path(settings.data_dir, sha).write_bytes(b"TAMPERED")
    account = FakeAccount({"CAP-1": FakeItem("CAP-1")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1
    assert "mime" in result["reasons"][0]["reason"]
    assert store.get_message("CAP-1")["archive_state"] == "live"


def test_verify_resets_when_a_blob_is_missing(captured):
    store, settings, _sha, blob_sha = captured
    files.blob_path(settings.data_dir, blob_sha).unlink()
    account = FakeAccount({"CAP-1": FakeItem("CAP-1")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1 and "blob" in result["reasons"][0]["reason"]


def test_verify_resets_when_a_blob_size_disagrees(captured):
    store, settings, _sha, blob_sha = captured
    with store.db.conn() as c:
        c.execute("UPDATE ews.attachments SET size = 999")
    account = FakeAccount({"CAP-1": FakeItem("CAP-1")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1 and "size" in result["reasons"][0]["reason"]


def test_a_vanished_item_is_a_failure_not_a_promotion(captured):
    store, settings, _sha, _blob = captured
    account = FakeAccount({"CAP-1": ValueError("ErrorItemNotFound")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["failed"] == 1 and result["verified"] == 0
    assert store.get_message("CAP-1")["archive_state"] == "captured"


# --- deleter ------------------------------------------------------------------


def _verified(store, settings, n=3, verified_age_days=30):
    store.upsert_messages([make_row(f"V{i}", date_ts=NOW - 300 * DAY)
                           for i in range(n)])
    for i in range(n):
        store.mark_captured(f"V{i}", mime_sha256="a" * 64, mime_path="/x.eml")
        store.mark_verified(f"V{i}")
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET verified_at = now() - %s * interval '1 day' "
                  "WHERE archive_state = 'verified'", (verified_age_days,))
    return store


def _deleter(store, settings, account, audit=None, **over):
    policy = ArchivePolicy.from_settings(make_settings(**over))
    return Deleter(settings, FakeGatewayFor(account), store, policy,
                   audit or RecordingAudit())


def test_deletion_is_blocked_unless_explicitly_enabled(captured):
    store, settings, _s, _b = captured
    _verified(store, settings)
    account = FakeAccount({})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=False).run(dry_run=False))
    assert result["deleted"] == 0
    assert "ARCHIVE_DELETE_ENABLED" in result["blocked"]
    assert account.fetch_calls == []


def test_dry_run_reports_eligibility_without_deleting(captured):
    store, settings, _s, _b = captured
    _verified(store, settings)
    account = FakeAccount({})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True).run(dry_run=True))
    assert result["eligible"] == 3 and result["deleted"] == 0
    assert len(result["sample"]) == 3
    assert account.fetch_calls == []


def test_grace_period_protects_freshly_verified_mail(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, verified_age_days=1)
    account = FakeAccount({})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True,
                                  archive_grace_days=7).run(dry_run=True))
    assert result["eligible"] == 0


def test_the_per_run_cap_is_enforced(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, n=5)
    items = {f"V{i}": DeletableItem(f"V{i}") for i in range(5)}
    account = FakeAccount(items)
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True,
                                  archive_max_delete_per_run=2).run(dry_run=False))
    assert result["deleted"] == 2
    assert sum(1 for i in items.values() if i.deleted) == 2
    assert store.archive_state_counts()["deleted"] == 2


def test_delete_hard_deletes_marks_the_row_and_audits_each_one(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, n=2)
    items = {f"V{i}": DeletableItem(f"V{i}") for i in range(2)}
    audit = RecordingAudit()
    result = asyncio.run(_deleter(store, settings, FakeAccount(items), audit,
                                  archive_delete_enabled=True).run(
                                      dry_run=False, run_id=42))
    assert result["deleted"] == 2 and result["failed"] == 0
    assert all(i.deleted for i in items.values())
    assert store.get_message("V0")["archive_state"] == "deleted"
    assert len(audit.records) == 2
    detail = audit.records[0]["detail"]
    assert set(detail) >= {"ews_id", "internet_message_id", "mime_sha256", "run_id"}
    assert detail["run_id"] == 42
    assert audit.records[0]["side_effect_class"] == "destructive"


def test_one_failed_delete_does_not_mark_the_row(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, n=2)

    class Stubborn(DeletableItem):
        def delete(self):
            raise RuntimeError("ErrorAccessDenied")

    account = FakeAccount({"V0": Stubborn("V0"), "V1": DeletableItem("V1")})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True).run(dry_run=False))
    assert result["deleted"] == 1 and result["failed"] == 1
    assert store.get_message("V0")["archive_state"] == "verified"
    assert store.get_message("V1")["archive_state"] == "deleted"
