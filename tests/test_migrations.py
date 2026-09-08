"""Migration 004 (Phase 3): item_class/attachments_json columns, the
boilerplate tables, the widened archive_runs.kind check, and the one-time
re-queue of short in-thread replies."""

from conftest import make_row

from ewsmcp.cache.store import CacheStore
from ewsmcp.db import SCHEMA_VERSION


def test_004_adds_phase3_columns_and_tables_and_requeues_short_replies(db):
    store = CacheStore(db)
    store.upsert_messages([
        make_row("P1", conv="C1", body="x" * 700, date_ts=1000),
        make_row("R1", conv="C1", body="short reply", date_ts=2000),
        make_row("S1", conv=None, body="short", date_ts=3000),
    ])
    store.mark_embedded(["P1", "R1", "S1"])
    db.reapply_last_migration_for_tests()
    with db.conn() as c:
        cols = {r["column_name"] for r in c.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='ews' AND table_name='messages'")}
        assert {"item_class", "attachments_json"} <= cols
        tables = {r["table_name"] for r in c.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='ews'")}
        assert {"boilerplate_refs", "boilerplate_hits"} <= tables
        requeued = {r["ews_id"] for r in c.execute(
            "SELECT ews_id FROM ews.messages WHERE embedded_at IS NULL")}
    assert requeued == {"R1"}          # short AND in a conversation
    assert db.schema_version() == 4


def test_schema_version_is_four(db):
    assert SCHEMA_VERSION == 4
    assert db.schema_version() == 4


def test_004_widens_archive_runs_kind_check_to_allow_gc(db):
    with db.conn() as c:
        c.execute("INSERT INTO ews.archive_runs (kind) VALUES ('gc')")
        row = c.execute(
            "SELECT id FROM ews.archive_runs WHERE kind = 'gc'").fetchone()
    assert row is not None
