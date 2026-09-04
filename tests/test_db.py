"""Database: migrations apply once, are idempotent, and gate the schema version."""

from urllib.parse import urlsplit

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


# --------------------------------------------------------------------------
# Regression: migration 002 must not create `unaccent` in a schema named after
# the connecting role. Production connects as role `ews` to a database that
# also has an `ews` schema, so the default search_path's `"$user"` element
# resolves and an unqualified CREATE EXTENSION lands in `ews` — after which
# every `public.unaccent(...)` reference in the migration is undefined and
# ewsd/ewsmcp crash-loop on boot.
# --------------------------------------------------------------------------

def _role_dsn(pg_dsn: str, user: str, password: str, dbname: str) -> str:
    parts = urlsplit(pg_dsn)
    host = parts.hostname or "127.0.0.1"
    port = f":{parts.port}" if parts.port else ""
    return f"postgresql://{user}:{password}@{host}{port}/{dbname}"


def test_migrations_apply_as_a_role_named_after_an_existing_schema(pg_dsn):
    """The production shape: role `ews`, database owned by it, schema `ews`
    already present so `"$user"` in the default search_path resolves."""
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        if not admin.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
        ).fetchone()[0]:
            pytest.skip("needs a superuser connection to create a role/database")
        admin.execute("DROP DATABASE IF EXISTS ewsmigtest")
        admin.execute("DROP ROLE IF EXISTS ewsmigrole")
        admin.execute("CREATE ROLE ewsmigrole LOGIN PASSWORD 'x'")
        admin.execute("CREATE DATABASE ewsmigtest OWNER ewsmigrole")
    try:
        # The `ews` schema exists before the role ever connects, exactly as it
        # does on a production box that has already run migration 001.
        admin_parts = urlsplit(pg_dsn)
        with psycopg.connect(
            _role_dsn(pg_dsn, admin_parts.username or "postgres",
                      admin_parts.password or "", "ewsmigtest"),
            autocommit=True,
        ) as c:
            c.execute("CREATE SCHEMA IF NOT EXISTS ewsmigrole AUTHORIZATION ewsmigrole")
            c.execute("CREATE SCHEMA IF NOT EXISTS ews AUTHORIZATION ewsmigrole")
            # Unlike unaccent, pgvector is not a trusted extension: CREATE
            # EXTENSION vector requires superuser, so a DBA installs it up
            # front exactly as they would in production. Once installed,
            # migration 003's CREATE EXTENSION IF NOT EXISTS is a no-op that
            # the app role can run without elevated privilege.
            c.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
        d = Database(_role_dsn(pg_dsn, "ewsmigrole", "x", "ewsmigtest"),
                     min_size=1, max_size=2)
        try:
            with d.conn() as c:
                # `"$user"` really does resolve for this session — otherwise
                # the test would pass even with the bug present.
                path = c.execute("SELECT current_schemas(false) AS s").fetchone()["s"]
                assert "ewsmigrole" in path
            assert d.migrate() == SCHEMA_VERSION
            with d.conn() as c:
                schema = c.execute(
                    "SELECT n.nspname AS s FROM pg_extension e "
                    "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    "WHERE e.extname = 'unaccent'").fetchone()["s"]
                assert schema == "public"
                # migration 003 has the identical trap for pgvector: a bare
                # CREATE EXTENSION vector would land in schema `ews` under
                # this role's search_path.
                vec_schema = c.execute(
                    "SELECT n.nspname AS s FROM pg_extension e "
                    "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    "WHERE e.extname = 'vector'").fetchone()["s"]
                assert vec_schema == "public"
                # the generated column evaluates ews.immutable_unaccent under
                # this session's search_path — it must not depend on it
                c.execute("INSERT INTO ews.messages(ews_id, folder_id, subject) "
                          "VALUES ('M1', 'inbox', 'budget café')")
                hit = c.execute(
                    "SELECT ews_id FROM ews.messages WHERE search_tsv @@ "
                    "to_tsquery('simple', 'cafe:*')").fetchone()
                assert hit["ews_id"] == "M1"
        finally:
            d.close()
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute("DROP DATABASE IF EXISTS ewsmigtest")
            admin.execute("DROP ROLE IF EXISTS ewsmigrole")
