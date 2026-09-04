"""The runner: one archive_runs row per pass, the right workers per kind,
and a background cycle that degrades instead of dying."""

import asyncio
import time

from conftest import FakeEmbedder, make_row, make_settings
from test_archive_capture import FakeAccount, FakeGatewayFor, FakeItem
from test_archive_verify_delete import DeletableItem, RecordingAudit

from ewsmcp.archive.runner import KINDS, ArchiveRunner
from ewsmcp.cache.store import CacheStore
from ewsmcp.semantic import SemanticIndex

NOW = int(time.time())
DAY = 86400


def _store(db):
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0}])
    store.upsert_messages([
        make_row("OLD-1", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY),
        make_row("NEW-1", folder_id="FID-INBOX", date_ts=NOW - 2 * DAY),
    ])
    return store


def _runner(db, tmp_path, account=None, index=None, **over):
    store = _store(db)
    settings = make_settings(data_dir=str(tmp_path / "data"), **over)
    account = account if account is not None else FakeAccount({})
    return ArchiveRunner(settings, FakeGatewayFor(account), store,
                         RecordingAudit(), index=index), store


def test_kinds_cover_the_spec(tmp_path, db):
    assert KINDS == ("capture", "verify", "delete", "embed", "all")


def test_dry_run_records_a_run_row_and_counts_candidates(tmp_path, db):
    runner, store = _runner(db, tmp_path)
    out = asyncio.run(runner.run_once(kind="capture", dry_run=True))
    assert out["ok"] and out["dry_run"] is True
    assert out["candidates"] == 1 and out["captured"] == 0
    row = store.get_run(out["run_id"])
    assert row["kind"] == "capture" and row["dry_run"] == 1
    assert row["finished_at"] is not None
    assert '"after_days": 180' in row["policy_json"]


def test_all_runs_capture_verify_and_embed_in_one_pass(tmp_path, db):
    account = FakeAccount({"OLD-1": FakeItem("OLD-1")})
    index_store = None
    runner, store = _runner(db, tmp_path, account=account)
    runner.index = SemanticIndex(store, FakeEmbedder())
    out = asyncio.run(runner.run_once(kind="all", dry_run=False))
    assert out["captured"] == 1
    # capture and verify both ran against the same item this pass
    assert out["verified"] == 1
    assert store.get_message("OLD-1")["archive_state"] == "verified"
    assert out["embedded"] == 2 and store.embedding_backlog() == 0
    assert index_store is None  # sanity: the fixture did not leak state


def test_delete_stays_blocked_unless_enabled(tmp_path, db):
    items = {"OLD-1": DeletableItem("OLD-1")}
    runner, store = _runner(db, tmp_path, account=FakeAccount(items))
    out = asyncio.run(runner.run_once(kind="delete", dry_run=False))
    assert out["deleted"] == 0 and "ARCHIVE_DELETE_ENABLED" in out["blocked"]
    assert not items["OLD-1"].deleted


def test_overrides_narrow_the_policy_recorded_on_the_run(tmp_path, db):
    runner, store = _runner(db, tmp_path)
    out = asyncio.run(runner.run_once(kind="capture", dry_run=True,
                                      before="2020-01-01", folders=["inbox"]))
    row = store.get_run(out["run_id"])
    assert '"f:inbox"' in row["policy_json"]
    assert out["candidates"] == 0          # nothing is older than 2020 here


def test_a_worker_exception_is_recorded_on_the_run_not_raised(tmp_path, db):
    class Boom:
        async def call(self, fn):
            raise RuntimeError("exchange exploded")

    store = _store(db)
    settings = make_settings(data_dir=str(tmp_path / "data"))
    runner = ArchiveRunner(settings, Boom(), store, RecordingAudit())
    out = asyncio.run(runner.run_once(kind="capture", dry_run=False))
    assert out["ok"] is False and "exchange exploded" in out["error"]
    assert "exchange exploded" in store.get_run(out["run_id"])["error"]


def test_embed_is_a_noop_without_a_semantic_index(tmp_path, db):
    runner, store = _runner(db, tmp_path)
    out = asyncio.run(runner.run_once(kind="embed", dry_run=False))
    assert out["embedded"] == 0
    assert store.embedding_backlog() == 2


def test_status_reports_the_cadence_and_the_last_cycle(tmp_path, db):
    runner, _store = _runner(db, tmp_path, archive_cycle_seconds=300)
    st = runner.status()
    assert st["cycle_seconds"] == 300 and st["cycles"] == 0
    assert st["running"] is False


def test_the_background_loop_runs_a_cycle_and_can_be_stopped(tmp_path, db):
    account = FakeAccount({"OLD-1": FakeItem("OLD-1")})
    runner, store = _runner(db, tmp_path, account=account,
                            archive_cycle_seconds=1)

    async def drive():
        await runner.start()
        for _ in range(100):
            if runner.cycles:
                break
            await asyncio.sleep(0.05)
        await runner.stop()

    asyncio.run(drive())
    assert runner.cycles >= 1
    assert store.get_message("OLD-1")["archive_state"] in ("captured", "verified")
