"""Capture on arrival, and the rails that make it safe to delete behind it."""

import asyncio
import os
import time

from conftest import make_row, make_settings, seed_folders
from test_archive_capture import FakeAccount, FakeGatewayFor
from test_archive_verify_delete import (
    DAY,
    NOW,
    DeletableItem,
    _assert_invariant,
    _deleter,
    _verified,
    captured,  # noqa: F401 - a pytest fixture
)

from ewsmcp.archive import files
from ewsmcp.archive.capture import Capturer
from ewsmcp.archive.policy import ArchivePolicy
from ewsmcp.archive.verify import Verifier

STAMP = "backup-ok"


def _stamp(settings, age_hours):
    path = os.path.join(settings.data_dir, STAMP)
    os.makedirs(settings.data_dir, exist_ok=True)
    with open(path, "w"):
        pass
    then = time.time() - age_hours * 3600
    os.utime(path, (then, then))


def _gated(store, settings, account, **over):
    return _deleter(store, settings, account, archive_delete_enabled=True,
                    archive_backup_stamp=STAMP, data_dir=settings.data_dir, **over)


# --- two ages -----------------------------------------------------------------


def test_capture_can_start_long_before_deletion():
    policy = ArchivePolicy.from_settings(make_settings(
        archive_after_days=75, archive_grace_days=3, archive_capture_after_days=0))
    assert abs(policy.capture_cutoff_ts(now=NOW) - NOW) < 2
    assert abs(policy.delete_cutoff_ts(now=NOW) - (NOW - 78 * DAY)) < 2


def test_with_no_capture_age_both_follow_after_days():
    policy = ArchivePolicy.from_settings(make_settings(
        archive_after_days=75, archive_grace_days=3))
    assert abs(policy.capture_cutoff_ts(now=NOW) - (NOW - 75 * DAY)) < 2
    assert abs(policy.delete_cutoff_ts(now=NOW) - (NOW - 78 * DAY)) < 2


def test_capture_never_comes_later_than_deletion_could():
    """A capture age above the delete age would leave old mail uncaptured and
    so undeletable, which is how a mailbox with a quota fills up."""
    policy = ArchivePolicy.from_settings(make_settings(
        archive_after_days=75, archive_capture_after_days=200))
    assert abs(policy.capture_cutoff_ts(now=NOW) - (NOW - 75 * DAY)) < 2


def test_an_explicit_before_still_binds_both():
    policy = ArchivePolicy.from_settings(make_settings(
        archive_after_days=75, archive_capture_after_days=0))
    policy = policy.with_overrides(before="2020-01-01")
    assert policy.capture_cutoff_ts() == policy.before_ts
    assert policy.delete_cutoff_ts() == policy.before_ts - policy.grace_days * DAY


def test_early_capture_never_makes_young_mail_deletable(db, tmp_path):
    """The whole design in one test: captured and verified today, older than
    the grace period since verification, and still not deleted — it is young."""
    from ewsmcp.cache.store import CacheStore
    settings = make_settings(data_dir=str(tmp_path / "data"))
    store = CacheStore(db)
    store.upsert_messages([make_row("YOUNG", date_ts=NOW - 10 * DAY)])
    sha, path = files.store_mime(settings.data_dir, b"RAW")
    store.mark_captured("YOUNG", mime_sha256=sha, mime_path=str(path), changekey="CK")
    store.mark_verified("YOUNG")
    with db.conn() as c:
        c.execute("UPDATE ews.messages SET verified_at = now() - interval '9 days'")
    item = DeletableItem("YOUNG")
    result = asyncio.run(_deleter(
        store, settings, FakeAccount({"YOUNG": item}), archive_delete_enabled=True,
        archive_after_days=75, archive_grace_days=3,
        archive_capture_after_days=0).run(dry_run=False))
    assert result["eligible"] == 0 and item.deleted is False


# --- mail that changed after it was captured ----------------------------------


def test_mail_changed_after_capture_is_requeued_not_stuck(captured):  # noqa: F811
    """Reading a message moves its changekey. Left `verified`, such a row fails
    every run for ever and stays at the head of the oldest-first queue."""
    store, settings, _s, _b = captured
    _verified(store, settings, n=2)
    items = {"V0": DeletableItem("V0", changekey="CK-MOVED"),
             "V1": DeletableItem("V1")}
    result = asyncio.run(_deleter(store, settings, FakeAccount(items),
                                  archive_delete_enabled=True).run(dry_run=False))
    assert items["V0"].deleted is False
    assert store.get_message("V0")["archive_state"] == "captured"
    assert result["requeued"] == 1
    assert items["V1"].deleted is True
    _assert_invariant(result)


def test_a_requeued_message_is_recaptured_and_then_deleted(captured):  # noqa: F811
    """End to end, because each step alone proves nothing: stale at deletion,
    reset by the verifier, recaptured at its new changekey, verified, deleted."""
    store, settings, _s, _b = captured
    seed_folders(store)
    _verified(store, settings, n=1)
    item = DeletableItem("V0", changekey="CK-MOVED", mime=b"RAW-MIME-NEW")
    account = FakeAccount({"V0": item, "CAP-1": DeletableItem("CAP-1")})
    asyncio.run(_deleter(store, settings, account, archive_delete_enabled=True
                         ).run(dry_run=False))
    assert item.deleted is False
    asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert store.get_message("V0")["archive_state"] == "live"
    policy = ArchivePolicy.from_settings(make_settings(archive_after_days=75))
    asyncio.run(Capturer(settings, FakeGatewayFor(account), store, policy
                         ).run(dry_run=False))
    assert store.get_message("V0")["captured_changekey"] == "CK-MOVED"
    asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert store.get_message("V0")["archive_state"] == "verified"
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET verified_at = now() - interval '30 days'"
                  " WHERE ews_id = 'V0'")
    asyncio.run(_deleter(store, settings, account, archive_delete_enabled=True
                         ).run(dry_run=False))
    assert item.deleted is True
    assert store.get_message("V0")["archive_state"] == "deleted"


# --- the backup rail ----------------------------------------------------------


def test_a_fresh_backup_lets_deletion_run(captured):  # noqa: F811
    store, settings, _s, _b = captured
    _verified(store, settings, n=1)
    _stamp(settings, age_hours=2)
    item = DeletableItem("V0")
    result = asyncio.run(_gated(store, settings, FakeAccount({"V0": item})
                                ).run(dry_run=False))
    assert result["deleted"] == 1 and result["blocked"] is None


def test_a_stale_backup_holds_deletion(captured):  # noqa: F811
    store, settings, _s, _b = captured
    _verified(store, settings, n=1)
    _stamp(settings, age_hours=72)
    item = DeletableItem("V0")
    result = asyncio.run(_gated(store, settings, FakeAccount({"V0": item})
                                ).run(dry_run=False))
    assert item.deleted is False and "backup" in result["blocked"]
    assert result["held"] and result["eligible"] == 0
    _assert_invariant(result)


def test_a_missing_marker_holds_deletion_for_good(captured):  # noqa: F811
    """No marker is not an old backup: it may mean the volume itself was lost,
    so the time limit below does not release it."""
    store, settings, _s, _b = captured
    _verified(store, settings, n=1)
    item = DeletableItem("V0")
    result = asyncio.run(_gated(store, settings, FakeAccount({"V0": item}),
                                archive_backup_hold_max_days=0).run(dry_run=False))
    assert item.deleted is False and result["held"]


def test_the_hold_has_a_time_limit_so_the_mailbox_cannot_fill(captured):  # noqa: F811
    store, settings, _s, _b = captured
    _verified(store, settings, n=1)
    _stamp(settings, age_hours=11 * 24)
    item = DeletableItem("V0")
    result = asyncio.run(_gated(store, settings, FakeAccount({"V0": item})
                                ).run(dry_run=False))
    assert item.deleted is True and result["held"] is None
    assert "backup" in result["warning"]


def test_with_no_marker_configured_nothing_is_held(captured):  # noqa: F811
    store, settings, _s, _b = captured
    _verified(store, settings, n=1)
    item = DeletableItem("V0")
    asyncio.run(_deleter(store, settings, FakeAccount({"V0": item}),
                         archive_delete_enabled=True).run(dry_run=False))
    assert item.deleted is True


def test_a_preview_shows_the_hold(captured):  # noqa: F811
    store, settings, _s, _b = captured
    _verified(store, settings, n=1)
    result = asyncio.run(_gated(store, settings, FakeAccount({})).run(dry_run=True))
    assert result["held"]


def test_a_held_cycle_is_recorded_on_the_run(db, tmp_path):
    from test_archive_runner import _runner  # noqa: PLC0415
    runner, store = _runner(db, tmp_path, archive_delete_enabled=True,
                            archive_backup_stamp=STAMP)
    out = asyncio.run(runner.run_once(kind="delete", dry_run=False))
    assert "backup" in out["stopped"]
    assert "backup" in store.get_run(out["run_id"])["error"]
    assert runner.status()["backup"]["state"] == "missing"
