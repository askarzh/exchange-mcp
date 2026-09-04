"""Migration 003: the archive tables, pgvector, and the indexes search needs."""

from ewsmcp.db import SCHEMA_VERSION


def _cols(db, table):
    with db.conn() as c:
        rows = c.execute(
            "SELECT column_name, data_type, udt_name FROM information_schema.columns "
            "WHERE table_schema = 'ews' AND table_name = %s", (table,)).fetchall()
    return {r["column_name"]: (r["data_type"], r["udt_name"]) for r in rows}


def _indexes(db, table):
    with db.conn() as c:
        rows = c.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'ews' AND tablename = %s", (table,)).fetchall()
    return {r["indexname"]: r["indexdef"] for r in rows}


def test_schema_version_is_three(db):
    assert SCHEMA_VERSION == 3
    assert db.schema_version() == 3


def test_vector_extension_is_installed(db):
    """...and in `public`. Production connects as role `ews` with a schema
    of the same name, so a bare CREATE EXTENSION lands it in `ews`
    ("$user" wins the search_path) and every later public.vector(768) /
    <=> reference breaks. Migration 003 pins WITH SCHEMA public."""
    with db.conn() as c:
        row = c.execute(
            "SELECT n.nspname AS schema FROM pg_extension e "
            "JOIN pg_namespace n ON n.oid = e.extnamespace "
            "WHERE e.extname = 'vector'").fetchone()
    assert row is not None
    assert row["schema"] == "public"


def test_messages_carries_the_captured_changekey_column(db):
    cols = _cols(db, "messages")
    assert cols["captured_changekey"][0] == "text"


def test_attachments_table_shape(db):
    cols = _cols(db, "attachments")
    assert set(cols) == {"id", "message_ews_id", "name", "content_type", "size",
                         "sha256", "is_inline", "name_tsv"}
    assert cols["name_tsv"][1] == "tsvector"
    assert "ix_att_name_tsv" in _indexes(db, "attachments")


def test_attachments_cascade_when_a_live_row_is_dropped(db, store_with_message):
    store, ews_id = store_with_message
    store.replace_attachments(ews_id, [
        {"name": "q3.pdf", "content_type": "application/pdf", "size": 12,
         "sha256": "a" * 64, "is_inline": 0},
    ])
    with db.conn() as c:
        c.execute("DELETE FROM ews.messages WHERE ews_id = %s", (ews_id,))
        left = c.execute("SELECT COUNT(*) AS n FROM ews.attachments").fetchone()["n"]
    assert left == 0


def test_chunks_embedding_is_a_768_vector_with_an_hnsw_index(db):
    cols = _cols(db, "chunks")
    assert cols["embedding"][1] == "vector"
    with db.conn() as c:
        dims = c.execute(
            "SELECT a.atttypmod AS m FROM pg_attribute a "
            "JOIN pg_class t ON t.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "WHERE n.nspname = 'ews' AND t.relname = 'chunks' "
            "AND a.attname = 'embedding'").fetchone()["m"]
    assert dims == 768
    defs = _indexes(db, "chunks")
    assert any("hnsw" in d and "vector_cosine_ops" in d for d in defs.values())


def test_archive_runs_table_shape(db):
    cols = _cols(db, "archive_runs")
    assert set(cols) == {"id", "kind", "dry_run", "policy_json", "started_at",
                         "finished_at", "captured", "verified", "deleted",
                         "failed", "error", "sample_json"}
    with db.conn() as c:
        c.execute("INSERT INTO ews.archive_runs (kind) VALUES ('capture')")
        row = c.execute("SELECT id, dry_run, captured FROM ews.archive_runs").fetchone()
    assert row["id"] > 0 and row["dry_run"] == 1 and row["captured"] == 0
