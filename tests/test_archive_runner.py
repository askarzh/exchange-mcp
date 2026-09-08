"""The runner: one archive_runs row per pass, the right workers per kind,
and a background cycle that degrades instead of dying."""

import asyncio
import time

from conftest import FakeEmbedder, make_row, make_settings
from test_archive_capture import FakeAccount, FakeGatewayFor, FakeItem
from test_archive_verify_delete import DeletableItem, RecordingAudit

from ewsmcp.archive import runner as runner_module
from ewsmcp.archive.capture import Capturer
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
    assert st["policy"]["after_days"] == runner.settings.archive_after_days
    assert st["policy"]["grace_days"] >= 1
    assert st["policy"]["folders"] == ["f:inbox", "f:sent"]
    assert st["next_cycle_in_s"] is None   # no cycle yet
    runner.last_cycle_ts = time.time()
    assert 295 <= runner.status()["next_cycle_in_s"] <= 300


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


def test_run_once_returns_blocked_instead_of_hanging_on_a_held_lock(tmp_path, db):
    runner, store = _runner(db, tmp_path)
    called = False

    async def hold_and_call():
        nonlocal called

        async def hold():
            async with runner._lock:
                await asyncio.sleep(0.5)

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.05)  # let the holder actually grab the lock
        out = await runner.run_once(kind="capture", dry_run=True,
                                    wait_seconds=0.1)
        holder.cancel()
        try:
            await holder
        except asyncio.CancelledError:
            pass
        return out

    orig_run = Capturer.run

    async def spy_run(self, *a, **kw):
        nonlocal called
        called = True
        return await orig_run(self, *a, **kw)

    Capturer.run = spy_run
    try:
        out = asyncio.run(hold_and_call())
    finally:
        Capturer.run = orig_run
    assert out["ok"] is False
    assert out["blocked"] == "cycle in progress"
    assert "retry_after_s" in out
    assert called is False


def test_start_run_failure_does_not_kill_the_background_loop(tmp_path, db):
    account = FakeAccount({"OLD-1": FakeItem("OLD-1")})
    runner, store = _runner(db, tmp_path, account=account,
                            archive_cycle_seconds=1)

    orig_start_run = store.start_run
    calls = {"n": 0}

    def boom(*a, **kw):
        calls["n"] += 1
        raise RuntimeError("db is on fire")

    store.start_run = boom

    async def drive():
        await runner.start()
        for _ in range(100):
            if runner.cycles:
                break
            await asyncio.sleep(0.05)
        await runner.stop()

    asyncio.run(drive())
    store.start_run = orig_start_run
    assert calls["n"] >= 1
    assert runner.cycles >= 1
    assert runner.last_error is not None and "db is on fire" in runner.last_error


def test_disk_stats_reports_this_process_own_data_dir_and_is_cached(tmp_path, db):
    """ArchiveRunner.disk_stats() is the ONLY place blob_store_bytes/free_gb
    are computed on the daemon side of the status/metrics path — cached for
    ~60s so a status poller never re-walks the blob store on every call."""
    from ewsmcp.archive import files

    runner, _ = _runner(db, tmp_path)
    files.store_blob(runner.settings.data_dir, b"x" * 250)

    first = asyncio.run(runner.disk_stats())
    assert first["blob_store_bytes"] == 250
    assert isinstance(first["free_gb"], float)

    calls = {"n": 0}
    orig = files.blob_store_bytes

    def counting(data_dir):
        calls["n"] += 1
        return orig(data_dir)

    files.blob_store_bytes = counting
    try:
        second = asyncio.run(runner.disk_stats())
    finally:
        files.blob_store_bytes = orig
    assert second == first
    assert calls["n"] == 0  # served from the 60s cache, not recomputed


# --- ARCHIVE_DELETE_AUTO: the loop never deletes by default --------------------


class _RecordingDeleter:
    """Stands in for the real Deleter so the test observes whether the lane
    ran at all, not what it would have done."""

    calls: list = []

    def __init__(self, *a, **kw):
        pass

    async def run(self, *, dry_run=True, run_id=None):
        _RecordingDeleter.calls.append(dry_run)
        return {"eligible": 0, "deleted": 0, "failed": 0, "remaining": 0,
                "unmarked": 0, "deleted_unrecorded": [], "reasons": [],
                "blocked": None, "error": None, "sample": []}


def _drive_one_cycle(runner):
    async def drive():
        await runner.start()
        for _ in range(200):
            if runner.cycles:
                break
            await asyncio.sleep(0.05)
        await runner.stop()

    asyncio.run(drive())


def test_the_background_loop_never_deletes_unless_delete_auto(tmp_path, db, monkeypatch):
    """ARCHIVE_DELETE_ENABLED alone is not enough for the BACKGROUND cycle:
    unattended deletion needs ARCHIVE_DELETE_AUTO on top of it."""
    monkeypatch.setattr(runner_module, "Deleter", _RecordingDeleter)
    _RecordingDeleter.calls = []
    runner, _store_ = _runner(db, tmp_path, account=FakeAccount({"OLD-1": FakeItem("OLD-1")}),
                              archive_cycle_seconds=1,
                              archive_delete_enabled=True,
                              archive_delete_auto=False)
    _drive_one_cycle(runner)
    assert runner.cycles >= 1
    assert _RecordingDeleter.calls == []


def test_delete_auto_lets_the_background_loop_run_the_delete_lane(
        tmp_path, db, monkeypatch):
    monkeypatch.setattr(runner_module, "Deleter", _RecordingDeleter)
    _RecordingDeleter.calls = []
    runner, _store_ = _runner(db, tmp_path, account=FakeAccount({"OLD-1": FakeItem("OLD-1")}),
                              archive_cycle_seconds=1,
                              archive_delete_enabled=True,
                              archive_delete_auto=True)
    _drive_one_cycle(runner)
    assert _RecordingDeleter.calls and _RecordingDeleter.calls[0] is False


def test_the_loop_says_why_it_skipped_the_delete_lane(tmp_path, db, monkeypatch):
    monkeypatch.setattr(runner_module, "Deleter", _RecordingDeleter)
    _RecordingDeleter.calls = []
    runner, _store_ = _runner(db, tmp_path, account=FakeAccount({"OLD-1": FakeItem("OLD-1")}),
                              archive_delete_enabled=True,
                              archive_delete_auto=False)
    out = asyncio.run(runner._run_once_locked(
        kind="all", dry_run=False, before=None, folders=None,
        allow_delete=False))
    assert "ARCHIVE_DELETE_AUTO" in out["blocked"]
    assert _RecordingDeleter.calls == []


def test_a_manual_run_still_deletes_while_delete_auto_is_off(tmp_path, db, monkeypatch):
    """The manual, confirm-gated path is exactly what ARCHIVE_DELETE_AUTO=false
    leaves you with — archive_run(kind='delete', dry_run=false) must still work."""
    monkeypatch.setattr(runner_module, "Deleter", _RecordingDeleter)
    _RecordingDeleter.calls = []
    runner, _store_ = _runner(db, tmp_path, account=FakeAccount({"OLD-1": FakeItem("OLD-1")}),
                              archive_delete_enabled=True,
                              archive_delete_auto=False)
    out = asyncio.run(runner.run_once(kind="delete", dry_run=False))
    assert _RecordingDeleter.calls == [False]
    assert out["blocked"] is None


def test_delete_auto_defaults_off_and_shows_in_status(tmp_path, db):
    runner, _store_ = _runner(db, tmp_path)
    assert runner.settings.archive_delete_auto is False
    assert runner.status()["delete_auto"] is False


def test_status_carries_the_boilerplate_counters_after_a_cycle(tmp_path, db):
    account = FakeAccount({"OLD-1": FakeItem("OLD-1")})
    runner, _store = _runner(db, tmp_path, account=account,
                             archive_cycle_seconds=1)
    assert runner.status()["boilerplate"] == {}      # nothing read yet

    async def drive():
        await runner.start()
        for _ in range(100):
            if runner.cycles:
                break
            await asyncio.sleep(0.05)
        await runner.stop()

    asyncio.run(drive())
    block = runner.status()["boilerplate"]
    assert block["drop_detector"] == "off"
    assert block["threshold"] == float(runner.settings.embed_boilerplate_threshold)
    assert block["embedding"] == {"hits": 0, "dropped": 0}
    assert block["llm"] == {"hits": 0, "dropped": 0, "errors": 0}
