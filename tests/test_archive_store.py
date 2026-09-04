"""CacheStore's archive half: candidate selection, state transitions,
attachment rows, and the archive_runs ledger."""

import time

from conftest import make_row

NOW = int(time.time())
DAY = 86400


def _folders(store):
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0},
        {"ews_id": "FID-SENT", "name": "Sent", "path": "Sent", "wk": "f:sent",
         "total": 0, "unread": 0, "children": 0},
        {"ews_id": "FID-PROJ", "name": "Projects", "path": "Projects", "wk": None,
         "total": 0, "unread": 0, "children": 0},
    ])


def test_folder_ids_for_wk(store_with_message):
    store, _ = store_with_message
    _folders(store)
    assert sorted(store.folder_ids_for_wk(["f:inbox", "f:sent"])) == \
        ["FID-INBOX", "FID-SENT"]
    assert store.folder_ids_for_wk(["f:nope"]) == []


def test_candidates_respect_folder_age_and_categories(store_with_message):
    store, _ = store_with_message
    _folders(store)
    store.upsert_messages([
        make_row("OLD-IN", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY),
        make_row("NEW-IN", folder_id="FID-INBOX", date_ts=NOW - 5 * DAY),
        make_row("OLD-PROJ", folder_id="FID-PROJ", date_ts=NOW - 300 * DAY),
        make_row("OLD-KEEP", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY,
                 categories=["Keep"]),
    ])
    cutoff = NOW - 180 * DAY
    rows = store.archive_candidates(folder_ids=["FID-INBOX", "FID-SENT"],
                                    before_ts=cutoff, exclude_categories=["keep"],
                                    limit=25)
    assert [r["ews_id"] for r in rows] == ["OLD-IN"]
    assert store.archive_candidate_count(
        folder_ids=["FID-INBOX", "FID-SENT"], before_ts=cutoff,
        exclude_categories=["keep"]) == 1


def test_candidates_across_all_folders_when_folder_ids_is_none(store_with_message):
    store, _ = store_with_message
    _folders(store)
    store.upsert_messages([make_row("OLD-PROJ", folder_id="FID-PROJ",
                                    date_ts=NOW - 300 * DAY)])
    rows = store.archive_candidates(folder_ids=None, before_ts=NOW - 180 * DAY,
                                    exclude_categories=[], limit=25)
    assert "OLD-PROJ" in {r["ews_id"] for r in rows}


def test_capture_verify_delete_state_machine(store_with_message):
    store, ews_id = store_with_message
    assert store.mark_captured(ews_id, mime_sha256="b" * 64,
                               mime_path="/data/mime/b.eml",
                               changekey="CK-CAPTURED") == 1
    row = store.get_message(ews_id)
    assert row["archive_state"] == "captured" and row["mime_sha256"] == "b" * 64
    assert row["archived_at"] is not None
    assert row["captured_changekey"] == "CK-CAPTURED"
    assert [r["ews_id"] for r in store.captured_rows(10)] == [ews_id]

    assert store.mark_verified(ews_id) == 1
    assert store.get_message(ews_id)["archive_state"] == "verified"
    assert store.get_message(ews_id)["verified_at"] is not None

    assert store.mark_deleted([ews_id]) == 1
    row = store.get_message(ews_id)
    assert row["archive_state"] == "deleted" and row["deleted_at"] is not None
    # the row itself SURVIVES — that is the whole point of the archive
    assert row["subject"]


def test_reset_to_live_clears_the_capture_fields(store_with_message):
    store, ews_id = store_with_message
    store.mark_captured(ews_id, mime_sha256="c" * 64, mime_path="/x.eml",
                        changekey="CK-1")
    assert store.reset_to_live(ews_id) == 1
    row = store.get_message(ews_id)
    assert row["archive_state"] == "live"
    assert row["mime_sha256"] is None and row["mime_path"] is None
    assert row["archived_at"] is None
    assert row["captured_changekey"] is None


def test_state_transitions_are_guarded_by_the_current_archive_state(store_with_message):
    store, ews_id = store_with_message

    # mark_verified on a live row: no-op
    assert store.mark_verified(ews_id) == 0
    assert store.get_message(ews_id)["archive_state"] == "live"

    # mark_deleted on a live row: no-op
    assert store.mark_deleted([ews_id]) == 0
    assert store.get_message(ews_id)["archive_state"] == "live"

    store.mark_captured(ews_id, mime_sha256="g" * 64, mime_path="/x.eml")
    store.mark_verified(ews_id)
    store.mark_deleted([ews_id])
    assert store.get_message(ews_id)["archive_state"] == "deleted"

    # mark_captured on a deleted row: no-op, row stays deleted, no capture fields touched
    assert store.mark_captured(ews_id, mime_sha256="h" * 64, mime_path="/y.eml") == 0
    row = store.get_message(ews_id)
    assert row["archive_state"] == "deleted"
    assert row["mime_sha256"] == "g" * 64

    # reset_to_live on a deleted row: no-op, its attachments and mime columns survive
    store.replace_attachments(ews_id, [
        {"name": "keep.pdf", "content_type": "application/pdf", "size": 1,
         "sha256": "i" * 64, "is_inline": 0}])
    assert store.reset_to_live(ews_id) == 0
    row = store.get_message(ews_id)
    assert row["archive_state"] == "deleted"
    assert row["mime_sha256"] == "g" * 64
    assert len(store.attachments_for(ews_id)) == 1


def test_candidates_with_an_empty_folder_id_list_select_nothing(store_with_message):
    store, _ = store_with_message
    _folders(store)
    store.upsert_messages([make_row("OLD-IN", folder_id="FID-INBOX",
                                    date_ts=NOW - 300 * DAY)])
    cutoff = NOW - 180 * DAY
    assert store.archive_candidates(folder_ids=[], before_ts=cutoff,
                                    exclude_categories=[], limit=25) == []
    assert store.archive_candidate_count(folder_ids=[], before_ts=cutoff,
                                         exclude_categories=[]) == 0
    # None (not an empty list) is what means "every folder"
    assert store.archive_candidate_count(folder_ids=None, before_ts=cutoff,
                                         exclude_categories=[]) == 1


def test_candidates_exclude_categories_regardless_of_case_and_whitespace(store_with_message):
    store, _ = store_with_message
    store.upsert_messages([make_row("OLD-KEEP", date_ts=NOW - 300 * DAY,
                                    categories=[" Keep "])])
    rows = store.archive_candidates(folder_ids=None, before_ts=NOW - 180 * DAY,
                                    exclude_categories=["keep"], limit=25)
    assert "OLD-KEEP" not in {r["ews_id"] for r in rows}


def test_deletable_rows_need_verified_plus_grace(store_with_message):
    store, _ = store_with_message
    store.upsert_messages([
        make_row("V-OLD", date_ts=NOW - 300 * DAY),
        make_row("V-YOUNG", date_ts=NOW - 10 * DAY),
        make_row("CAPTURED-ONLY", date_ts=NOW - 300 * DAY),
    ])
    for i in ("V-OLD", "V-YOUNG", "CAPTURED-ONLY"):
        store.mark_captured(i, mime_sha256="d" * 64, mime_path="/x.eml")
    store.mark_verified("V-OLD")
    store.mark_verified("V-YOUNG")
    rows = store.deletable_rows(before_ts=NOW - 187 * DAY,
                                verified_before=NOW + DAY, limit=100)
    assert [r["ews_id"] for r in rows] == ["V-OLD"]
    # grace not yet elapsed: nothing verified before that instant
    assert store.deletable_rows(before_ts=NOW - 187 * DAY,
                                verified_before=NOW - DAY, limit=100) == []


def test_deletable_rows_honour_the_limit(store_with_message):
    store, _ = store_with_message
    store.upsert_messages([make_row(f"V{i}", date_ts=NOW - 300 * DAY)
                           for i in range(5)])
    for i in range(5):
        store.mark_captured(f"V{i}", mime_sha256="e" * 64, mime_path="/x.eml")
        store.mark_verified(f"V{i}")
    assert len(store.deletable_rows(before_ts=NOW, verified_before=NOW + DAY,
                                    limit=2)) == 2


def test_replace_attachments_is_idempotent(store_with_message):
    store, ews_id = store_with_message
    rows = [{"name": "q3.pdf", "content_type": "application/pdf", "size": 120,
             "sha256": "f" * 64, "is_inline": 0},
            {"name": "logo.png", "content_type": "image/png", "size": 40,
             "sha256": "0" * 64, "is_inline": 1}]
    store.replace_attachments(ews_id, rows)
    store.replace_attachments(ews_id, rows)
    got = store.attachments_for(ews_id)
    assert len(got) == 2
    assert {r["name"] for r in got} == {"q3.pdf", "logo.png"}
    assert got[0]["size"] == 120


def test_apply_server_deletes_follows_the_archive_state(store_with_message):
    store, _ = store_with_message
    store.upsert_messages([make_row("LIVE-1"), make_row("CAP-1"),
                           make_row("VER-1"), make_row("DEL-1")])
    store.mark_captured("CAP-1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_captured("VER-1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("VER-1")
    store.mark_captured("DEL-1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("DEL-1")
    store.mark_deleted(["DEL-1"])

    dropped, tombstoned = store.apply_server_deletes(
        ["LIVE-1", "CAP-1", "VER-1", "DEL-1"])
    assert (dropped, tombstoned) == (1, 2)
    assert store.get_message("LIVE-1") is None            # live rows go
    for i in ("CAP-1", "VER-1", "DEL-1"):                  # archived rows stay
        assert store.get_message(i)["archive_state"] == "deleted"


def test_state_counts_and_per_folder_archived_counts(store_with_message):
    store, ews_id = store_with_message
    _folders(store)
    store.upsert_messages([make_row("A1", folder_id="FID-INBOX"),
                           make_row("A2", folder_id="FID-SENT")])
    store.mark_captured("A1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_captured("A2", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("A2")
    counts = store.archive_state_counts()
    assert counts["captured"] == 1 and counts["verified"] == 1
    assert counts["live"] >= 1
    assert store.archived_counts_by_folder() == {"FID-INBOX": 1, "FID-SENT": 1}


def test_messages_by_ids_returns_a_lookup(store_with_message):
    store, ews_id = store_with_message
    store.upsert_messages([make_row("B1"), make_row("B2")])
    got = store.messages_by_ids(["B1", "B2", "MISSING"])
    assert set(got) == {"B1", "B2"}
    assert got["B1"]["ews_id"] == "B1"


def test_run_ledger_round_trip(store_with_message):
    store, _ = store_with_message
    run_id = store.start_run("capture", dry_run=True,
                             policy={"folders": ["f:inbox"], "after_days": 180})
    assert isinstance(run_id, int) and run_id > 0
    open_row = store.get_run(run_id)
    assert open_row["finished_at"] is None and open_row["dry_run"] == 1
    store.finish_run(run_id, captured=3, failed=1, sample=[{"id": "RAW-1"}])
    row = store.get_run(run_id)
    assert row["captured"] == 3 and row["failed"] == 1
    assert row["finished_at"] is not None
    assert '"RAW-1"' in row["sample_json"]
    assert '"after_days": 180' in row["policy_json"]
    assert [r["id"] for r in store.recent_runs(5)] == [run_id]


def test_get_run_of_an_unknown_id_is_none(store_with_message):
    store, _ = store_with_message
    assert store.get_run(999999) is None
