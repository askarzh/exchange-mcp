"""The capturer: policy selection, MIME + blob writing, attachment rows."""

import asyncio
import time
from types import SimpleNamespace

import pytest
from conftest import make_row, make_settings

from ewsmcp.archive import files
from ewsmcp.archive.capture import CAPTURE_FIELDS, Capturer
from ewsmcp.archive.policy import NEVER_ARCHIVED, ArchivePolicy
from ewsmcp.cache.store import CacheStore

NOW = int(time.time())
DAY = 86400


class FakeAttachment:
    """Stands in for exchangelib.FileAttachment."""

    def __init__(self, name, content, content_type="application/pdf",
                 is_inline=False):
        self.name = name
        self.content = content
        self.content_type = content_type
        self.size = len(content)
        self.is_inline = is_inline


class FakeItem:
    def __init__(self, raw_id, mime=b"MIME", attachments=(), changekey="CK"):
        self.id = raw_id
        self.changekey = changekey
        self.mime_content = mime
        self.attachments = list(attachments)
        self.has_attachments = bool(attachments)


class FakeAccount:
    def __init__(self, items):
        self.items = items                 # {raw_id: FakeItem | Exception}
        self.fetch_calls = []

    def fetch(self, ids=None, only_fields=None, **kw):
        self.fetch_calls.append((list(ids), list(only_fields or [])))
        return [self.items[i] for i, _ck in ids]


class FakeGatewayFor:
    def __init__(self, account):
        self.account = account

    async def call(self, fn):
        return fn(self.account)


@pytest.fixture
def seeded(db, tmp_path):
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0},
        {"ews_id": "FID-PROJ", "name": "Projects", "path": "Projects", "wk": None,
         "total": 0, "unread": 0, "children": 0}])
    store.upsert_messages([
        make_row("OLD-1", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY),
        make_row("OLD-2", folder_id="FID-INBOX", date_ts=NOW - 290 * DAY),
        make_row("NEW-1", folder_id="FID-INBOX", date_ts=NOW - 3 * DAY),
        make_row("OTHER", folder_id="FID-PROJ", date_ts=NOW - 300 * DAY),
    ])
    settings = make_settings(data_dir=str(tmp_path / "data"))
    return store, settings


def _policy(settings, **over):
    return ArchivePolicy.from_settings(settings).with_overrides(**over) \
        if over else ArchivePolicy.from_settings(settings)


# --- policy -------------------------------------------------------------------


def test_policy_normalises_folder_keys(seeded):
    _store, settings = seeded
    policy = ArchivePolicy.from_settings(
        make_settings(archive_folders="inbox, f:sent "))
    assert policy.folders == ("f:inbox", "f:sent")


def test_policy_never_archives_calendar_contacts_or_tasks():
    with pytest.raises(ValueError, match="never archived"):
        ArchivePolicy.from_settings(make_settings(archive_folders="inbox,calendar"))
    assert "f:calendar" in NEVER_ARCHIVED


def test_capture_cutoff_is_after_days_ago():
    policy = ArchivePolicy.from_settings(make_settings(archive_after_days=180))
    assert abs(policy.capture_cutoff_ts(now=NOW) - (NOW - 180 * DAY)) < 2


def test_delete_cutoff_adds_the_grace_period():
    policy = ArchivePolicy.from_settings(
        make_settings(archive_after_days=180, archive_grace_days=7))
    assert abs(policy.delete_cutoff_ts(now=NOW) - (NOW - 187 * DAY)) < 2
    assert abs(policy.grace_instant_ts(now=NOW) - (NOW - 7 * DAY)) < 2


def test_overrides_narrow_the_window_and_the_folders():
    policy = ArchivePolicy.from_settings(make_settings()).with_overrides(
        before="2026-01-01", folders=["sent"], tz="Asia/Riyadh")
    assert policy.folders == ("f:sent",)
    assert policy.capture_cutoff_ts(now=NOW) < NOW - 200 * DAY


def test_folder_ids_resolve_through_the_folders_table(seeded):
    store, settings = seeded
    assert _policy(settings).folder_ids(store) == ["FID-INBOX"]


# --- capture ------------------------------------------------------------------


def test_dry_run_touches_neither_exchange_nor_disk(seeded):
    store, settings = seeded
    account = FakeAccount({})
    cap = Capturer(settings, FakeGatewayFor(account), store, _policy(settings))
    result = asyncio.run(cap.run(limit=25, dry_run=True))
    assert result["candidates"] == 2 and result["captured"] == 0
    assert {s["ews_id"] for s in result["sample"]} == {"OLD-1", "OLD-2"}
    assert account.fetch_calls == []
    assert store.get_message("OLD-1")["archive_state"] == "live"


def test_capture_writes_mime_blobs_and_rows(seeded):
    store, settings = seeded
    pdf = b"%PDF-1.4 quarterly"
    account = FakeAccount({
        "OLD-1": FakeItem("OLD-1", mime=b"RAW-MIME-1",
                          attachments=[FakeAttachment("q3.pdf", pdf)]),
        "OLD-2": FakeItem("OLD-2", mime=b"RAW-MIME-2"),
    })
    cap = Capturer(settings, FakeGatewayFor(account), store, _policy(settings))
    result = asyncio.run(cap.run(limit=25, dry_run=False))
    assert result["captured"] == 2 and result["failed"] == 0

    row = store.get_message("OLD-1")
    assert row["archive_state"] == "captured"
    assert row["mime_sha256"] == files.sha256_bytes(b"RAW-MIME-1")
    assert files.mime_path(settings.data_dir, row["mime_sha256"]).read_bytes() \
        == b"RAW-MIME-1"

    atts = store.attachments_for("OLD-1")
    assert len(atts) == 1
    assert atts[0]["name"] == "q3.pdf" and atts[0]["size"] == len(pdf)
    assert files.blob_path(settings.data_dir, atts[0]["sha256"]).read_bytes() == pdf


def test_capture_projects_only_the_fields_it_needs(seeded):
    store, settings = seeded
    account = FakeAccount({"OLD-1": FakeItem("OLD-1"), "OLD-2": FakeItem("OLD-2")})
    asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                         _policy(settings)).run(dry_run=False))
    assert account.fetch_calls[0][1] == CAPTURE_FIELDS


def test_item_attachments_are_recorded_without_a_blob(seeded):
    store, settings = seeded
    nested = SimpleNamespace(name="Fwd: contract", is_inline=False, size=900)
    account = FakeAccount({
        "OLD-1": FakeItem("OLD-1", attachments=[nested]),
        "OLD-2": FakeItem("OLD-2"),
    })
    asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                         _policy(settings)).run(dry_run=False))
    att = store.attachments_for("OLD-1")[0]
    assert att["content_type"] == "message/rfc822"
    assert att["sha256"] is None
    assert att["name"] == "Fwd: contract"


def test_one_bad_item_is_skipped_not_fatal(seeded):
    store, settings = seeded
    account = FakeAccount({
        "OLD-1": ValueError("ErrorItemNotFound"),
        "OLD-2": FakeItem("OLD-2"),
    })
    result = asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                                  _policy(settings)).run(dry_run=False))
    assert result["captured"] == 1 and result["failed"] == 1
    assert store.get_message("OLD-1")["archive_state"] == "live"
    assert store.get_message("OLD-2")["archive_state"] == "captured"


def test_capture_is_idempotent(seeded):
    store, settings = seeded
    account = FakeAccount({
        "OLD-1": FakeItem("OLD-1", attachments=[FakeAttachment("a.pdf", b"AAA")]),
        "OLD-2": FakeItem("OLD-2"),
    })
    cap = Capturer(settings, FakeGatewayFor(account), store, _policy(settings))
    asyncio.run(cap.run(dry_run=False))
    store.reset_to_live("OLD-1")
    asyncio.run(cap.run(dry_run=False))
    assert len(store.attachments_for("OLD-1")) == 1


def test_low_disk_stops_the_run_before_fetching(seeded, monkeypatch):
    store, settings = seeded
    monkeypatch.setattr(files.shutil, "disk_usage", lambda p: (100, 99, 1))
    account = FakeAccount({})
    result = asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                                  _policy(settings)).run(dry_run=False))
    assert result["captured"] == 0
    assert "ARCHIVE_MIN_FREE_GB" in result["stopped"]
    assert account.fetch_calls == []
