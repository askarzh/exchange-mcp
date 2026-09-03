"""Database: migrations apply once, are idempotent, and gate the schema version."""

import psycopg
import pytest

from ewsmcp.db import SCHEMA_VERSION, Database, SchemaOutdated


def test_migrate_creates_schema_and_is_idempotent(pg_dsn):
    d = Database(pg_dsn, min_size=1, max_size=2)
    try:
        with d.conn() as c:
            c.execute("DROP SCHEMA IF EXISTS ews CASCADE")
        assert d.schema_version() == 0
        assert d.migrate() == SCHEMA_VERSION
        assert d.schema_version() == SCHEMA_VERSION
        assert d.migrate() == 0  # second run applies nothing
        with d.conn() as c:
            tables = {r["table_name"] for r in c.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='ews'")}
        assert {"messages", "events", "tasks", "folders", "sync_state",
                "aliases", "alias_counters", "meta",
                "schema_migrations"} <= tables
        assert "sender_sigs" not in tables
    finally:
        d.close()


def test_require_version_raises_when_behind(db):
    with db.conn() as c:
        c.execute("DELETE FROM ews.schema_migrations")
    with pytest.raises(SchemaOutdated):
        db.require_version(SCHEMA_VERSION)


def test_conn_rolls_back_on_exception(db):
    with pytest.raises(RuntimeError), db.conn() as c:
        c.execute("INSERT INTO ews.meta(key, value) VALUES ('k', 'v')")
        raise RuntimeError("boom")
    with db.conn() as c:
        assert c.execute("SELECT COUNT(*) AS n FROM ews.meta").fetchone()["n"] == 0


def test_generated_tsvector_and_gin_index(db):
    with db.conn() as c:
        c.execute("INSERT INTO ews.messages(ews_id, folder_id, subject) "
                  "VALUES ('M1', 'inbox', 'budget review numbers')")
        hit = c.execute(
            "SELECT ews_id FROM ews.messages WHERE search_tsv @@ "
            "to_tsquery('simple', 'budg:*')").fetchone()
        assert hit["ews_id"] == "M1"
        idx = c.execute("SELECT indexname FROM pg_indexes WHERE schemaname='ews' "
                        "AND tablename='messages' AND indexname='ix_msg_tsv'").fetchone()
        assert idx is not None
    with pytest.raises(psycopg.errors.CheckViolation), db.conn() as c:
        c.execute("INSERT INTO ews.messages(ews_id, folder_id, archive_state) "
                  "VALUES ('M2', 'inbox', 'bogus')")
