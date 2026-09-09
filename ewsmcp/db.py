"""Postgres access: one connection pool per process, numbered SQL migrations.

The daemon applies migrations at boot (under an advisory lock, so two daemons
racing on an empty database cannot both create the schema). The MCP only
checks the version and refuses to start when the schema is older than the
build expects. Connections come from ``Database.conn()``: dict rows, commit on
clean exit, rollback on exception (psycopg's connection context manager).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import resources

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

SCHEMA = "ews"
SCHEMA_VERSION = 5  # bump together with the newest migrations/NNN_*.sql
_MIGRATION_RE = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")
_MIGRATE_LOCK_KEY = 7355608  # arbitrary, stable advisory-lock id


class SchemaOutdated(RuntimeError):
    """The database schema is older than this build expects."""


class Database:
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 4,
                 open_timeout: float = 5.0):
        self.dsn = dsn
        self.pool = ConnectionPool(
            dsn, min_size=min_size, max_size=max_size, open=True, timeout=open_timeout,
            kwargs={"row_factory": dict_row, "autocommit": False},
        )

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with self.pool.connection() as c:
            yield c

    def close(self) -> None:
        self.pool.close()

    # ------------------------------------------------------------ migrations

    @staticmethod
    def migrations() -> list[tuple[int, str, str]]:
        out: list[tuple[int, str, str]] = []
        for entry in resources.files("ewsmcp.migrations").iterdir():
            m = _MIGRATION_RE.match(entry.name)
            if m:
                out.append((int(m.group(1)), entry.name,
                            entry.read_text(encoding="utf-8")))
        return sorted(out)

    def migrate(self) -> int:
        """Apply every unapplied migration in order. Returns the count applied."""
        applied_now = 0
        with self.conn() as c:
            c.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            c.execute(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.schema_migrations ("
                "version integer PRIMARY KEY, name text NOT NULL, "
                "applied_at timestamptz NOT NULL DEFAULT now())"
            )
            c.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATE_LOCK_KEY,))
            done = {r["version"] for r in
                    c.execute(f"SELECT version FROM {SCHEMA}.schema_migrations")}
            for version, name, sql in self.migrations():
                if version in done:
                    continue
                logger.info("applying migration %s", name)
                c.execute(sql)
                c.execute(
                    f"INSERT INTO {SCHEMA}.schema_migrations (version, name) "
                    "VALUES (%s, %s)", (version, name),
                )
                applied_now += 1
        return applied_now

    def schema_version(self) -> int:
        with self.conn() as c:
            try:
                row = c.execute(
                    f"SELECT COALESCE(MAX(version), 0) AS v "
                    f"FROM {SCHEMA}.schema_migrations").fetchone()
            except psycopg.errors.UndefinedTable:
                c.rollback()
                return 0
        return int(row["v"])

    def require_version(self, expected: int) -> None:
        have = self.schema_version()
        if have < expected:
            raise SchemaOutdated(
                f"database schema is v{have}; this build needs v{expected}. "
                "Start ewsd once to migrate.")

    def reapply_last_migration_for_tests(self) -> None:
        """Delete the newest schema_migrations row and re-run `migrate()`.

        Test-only: lets a test exercise a migration's data-shaping statements
        (e.g. the Phase 3 re-queue UPDATE) against rows it has already
        inserted, on top of a DB that `migrate()` already brought fully
        current. Every migration must be idempotent (IF NOT EXISTS, etc.) for
        this re-application to be safe.
        """
        with self.conn() as c:
            c.execute(
                f"DELETE FROM {SCHEMA}.schema_migrations WHERE version = "
                f"(SELECT MAX(version) FROM {SCHEMA}.schema_migrations)"
            )
        self.migrate()
