"""build_mcp_context must not die when Postgres is unreachable at boot: the
runtime read paths already degrade to backend_unavailable, and psycopg_pool
reconnects on its own once Postgres comes back. It must still fail loudly
when the schema is outdated -- that's a real misconfiguration, not a
transient outage."""

import pytest
from conftest import make_settings

from ewsmcp.db import SchemaOutdated
from ewsmcp.mcp.server import build_mcp_context


def test_build_mcp_context_survives_unreachable_postgres():
    settings = make_settings(database_url="postgresql://postgres:x@127.0.0.1:9/nope")
    ctx = build_mcp_context(settings)  # must not raise
    assert ctx.db is not None
    ctx.db.close()


def test_build_mcp_context_raises_on_outdated_schema(db, pg_dsn):
    with db.conn() as c:
        c.execute("DELETE FROM ews.schema_migrations")
    settings = make_settings(database_url=pg_dsn)
    with pytest.raises(SchemaOutdated):
        build_mcp_context(settings)
