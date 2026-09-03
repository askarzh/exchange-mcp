# Phase 1: Postgres store + daemon/MCP split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SQLite mirror and alias store with Postgres, and split the single `ewsmcp` process into `ewsd` (owns Exchange, sync, uploads, audit, HTTP API) and a thin `ewsmcp` (reads Postgres, proxies everything else to `ewsd`).

**Architecture:** Phase 1 keeps every tool name, schema and envelope. The existing `CacheStore` and `IdAliaser` classes keep their method surface but get a psycopg backend behind a shared `Database` object. Read tools are refactored into "cache function returning `Optional[dict]`" + "live fallback"; the daemon's fallback is Exchange, the MCP's fallback is an HTTP call to the daemon. The daemon reuses today's dispatcher and REST shim; the MCP gets its own tiny dispatcher because confirm tokens, rate windows and audit are process-local state that must live in exactly one process (the daemon).

**Tech Stack:** Python 3.11+, exchangelib 5.0.3 (daemon only at runtime), psycopg 3 + psycopg_pool, httpx (MCP→daemon), mcp SDK 1.x, uvicorn, pytest against a real Postgres (`pgvector/pgvector:pg16` via Docker, or `EWS_TEST_DATABASE_URL`).

**Spec:** `docs/superpowers/specs/2026-09-03-postgres-archive-daemon-design.md` — this plan implements its Phase 1 (section 5, "Rollout"). Sections 1, 2 (minus `attachments`, `chunks`, `archive_runs`, which are Phase 2) and the read-tool half of section 4 are in scope. No archive capture, no embeddings, no deletion.

## Deviations from the spec (deliberate, recorded here)

1. **Package layout.** The spec sketches `ewsmcp/shared/`, `ewsmcp/daemon/`, `ewsmcp/mcp/`. A module `ewsmcp/shared.py` (attachment publishing) already exists, so no `shared/` package. Phase 1 keeps `ewsmcp/cache/`, `ewsmcp/ids.py`, `ewsmcp/tools/` where they are (backend swapped in place, names kept), adds `ewsmcp/db.py`, `ewsmcp/migrations/`, `ewsmcp/daemon.py`, `ewsmcp/mcp/`. Fewer moved files, same boundaries.
2. **Where the gate chain runs.** The spec says the MCP runs the gate chain and the daemon re-checks. Confirm tokens, the consumed-token set, the send-rate window and the audit chain are process-local; two copies would diverge. In this plan the **daemon runs the full chain** for every proxied call and the MCP only tier-filters its registry, resolves aliases for its local reads, and forwards `confirm_token` verbatim. The model-visible behaviour is identical.
3. **MCP imports exchangelib transitively.** Tool schemas live next to their handlers in `ewsmcp/tools/*.py`, which import exchangelib at module top (a hard rule enforced by `test_no_lazy_imports.py`). The MCP imports those modules for their `ToolSpec`s. exchangelib is installed but never *called* from the MCP; a test pins that (`ctx.gateway is None`, no Exchange contact).
4. **`schema_migrations` table** instead of a version row in `meta`.
5. **`unaccent` is not used.** `unaccent()` is STABLE, not IMMUTABLE, so it cannot feed a generated tsvector column. Accent folding is done in Python (`unicodedata` NFKD) into `norm_text`, exactly where the old Arabic normaliser used to run; `search_tsv` is generated from `norm_text` with the `simple` config.
6. **`find_similar` is not registered** and `mode="semantic"` is a validation error until Phase 2 adds `chunks` + embeddings. Registry counts stay 31 / 26 / 15 (full / draft / read).

## Global Constraints

- `requires-python = ">=3.11"`; `exchangelib==5.0.3` stays pinned; `mcp>=1.27,<2`.
- No `exchangelib` import inside a function body anywhere under `v5/ewsmcp` (`tests/test_no_lazy_imports.py`).
- Alias grammar `^[a-z]{1,2}[0-9]+$`; every id the model sees is an alias.
- Envelope contract: list tools return `{ok, items, count, total_available, next_offset}`; reads are stamped `source: cache|live` (+ `as_of` for cache).
- Tool count is asserted (31 full / 26 draft / 15 read) and `docs/API.md` is generated — regenerate with `python scripts/dump_tool_table.py --write` whenever a spec changes.
- `ruff` line length 100, target py311. Run `python -m ruff check .` before every commit.
- Postgres schema name is `ews`. All SQL is parameterised (`%s` / `%(name)s`), never f-stringed values.
- `DATA_DIR` guard (no cloud-synced paths) stays; only the daemon writes under it.
- Version becomes `5.0.0a1` (`ewsmcp/__init__.py` and `pyproject.toml` must agree; `test_docs_match_registry.py` checks the prefix).
- Commit after every task with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx` trailers. Work on a branch `feat/5.0-postgres-daemon` off `main`.

## Runtime facts the executor needs

- Live deployment: `/home/askar/stack/compose/personal.yml` builds `stack/ews-mcp:${EWS_MCP_TAG}` from `../src/ews-mcp/v5`, container `ews-mcp` on network `proxy`, env includes `EXTERNAL_URL=https://ews.lab.zhakenov.pro`, `SHARED_DIR=/shared`, `EWS_CACHE_WINDOW_DAYS=30`, `EWS_TZ=Asia/Almaty`, tier `full`, `SEND_ENABLED=true`. The OAuth proxy `ews-mcp-auth` forwards to `http://ews-mcp:8000`.
- Shared Postgres: container `postgres` (`postgres:16-alpine`, network `backend`, data at `/home/askar/stack/postgres/data`, superuser from `.env` `POSTGRES_USER`/`POSTGRES_PASSWORD`). It has `unaccent` but **not** `vector`. Phase 1 needs nothing beyond a database and a role. Before Phase 2 the image must become `pgvector/pgvector:pg16` (drop-in for the same data directory).
- No Python venv exists in the repo; `python3` is 3.12 and has no pytest. Task 1 creates `.venv`.
- Docker is available locally; the test fixture starts a throwaway `pgvector/pgvector:pg16` unless `EWS_TEST_DATABASE_URL` is set.

## File map (create / modify / delete)

| Path | Action | Responsibility |
|---|---|---|
| `v5/pyproject.toml` | modify | deps (`psycopg[binary]`, `psycopg_pool`, `httpx`), scripts `ewsd` + `ewsmcp`, package data for `*.sql`, version 5.0.0a1 |
| `v5/ewsmcp/__init__.py` | modify | version |
| `v5/ewsmcp/db.py` | create | `Database`: pool, migrations, schema version guard |
| `v5/ewsmcp/migrations/__init__.py`, `001_init.sql` | create | schema `ews` v1 |
| `v5/ewsmcp/normalize.py` | rewrite | `normalize_text`, `tsquery` (no Arabic) |
| `v5/ewsmcp/cache/store.py` | rewrite | `CacheStore(db)` on Postgres, same method surface |
| `v5/ewsmcp/cache/sync.py` | modify | drop `semantic`, row uses `normalize_text` |
| `v5/ewsmcp/ids.py` | rewrite | `IdAliaser(db)` on Postgres, `NullAliaser` kept |
| `v5/ewsmcp/semantic.py` | delete | Phase 2 re-adds a Gemini-backed index |
| `v5/ewsmcp/config.py` | modify | `database_url`, `ewsd_*`, optional EWS creds, remove cache-enabled/semantic knobs |
| `v5/ewsmcp/errors.py` | modify | `daemon_unavailable`, `backend_unavailable` codes |
| `v5/ewsmcp/server.py` | modify | `build_context` on Postgres |
| `v5/ewsmcp/tools/cache_reads.py` | create | cache functions returning `Optional[dict]`, shared by daemon and MCP |
| `v5/ewsmcp/tools/mail_read.py`, `tasks.py`, `calendar_people.py` | modify | use `cache_reads`; semantic removed; status block |
| `v5/ewsmcp/tools/base.py` | modify | `Context.daemon`, public `resolve_ids` |
| `v5/ewsmcp/http.py` | modify | `build_app(..., mount_mcp, prefix)`; `/v1/tools*`, `/v1/status` |
| `v5/ewsmcp/daemon.py` | create | `ewsd` entrypoint |
| `v5/ewsmcp/mcp/__init__.py`, `client.py`, `registry.py`, `dispatch.py`, `local.py`, `server.py`, `http.py` | create | thin MCP |
| `v5/ewsmcp/main.py` | modify | `ewsmcp` entrypoint → `mcp.server` / `mcp.http` |
| `v5/Dockerfile` | modify | one image, default CMD `ewsmcp`, `ewsd` selectable |
| `v5/docker-compose.yml` | create | dev stack: postgres (pgvector) + ewsd + ewsmcp |
| `v5/tests/conftest.py` | modify | Postgres fixtures, `make_context` |
| `v5/tests/test_db.py`, `test_pg_store.py`, `test_daemon_api.py`, `test_daemon_client.py`, `test_mcp_thin.py` | create | new coverage |
| `v5/tests/test_cache_store.py`, `test_arabic_search.py` | delete | replaced / removed |
| every other `v5/tests/test_*.py` | modify | fixtures → Postgres |
| `v5/scripts/boot_smoke.py` | modify | boots ewsd + ewsmcp against Postgres |
| `v5/DESIGN.md`, `README.md`, `docs/API.md`, `CHANGELOG.md`, `.env.example` | modify | 5.0 docs |
| `/home/askar/stack/compose/personal.yml`, nginx template | modify (Task 11, outside repo) | deploy |

---

### Task 1: Dev environment, `Database` layer, migration 001, Postgres test fixture

**Files:**
- Modify: `v5/pyproject.toml`
- Create: `v5/ewsmcp/db.py`, `v5/ewsmcp/migrations/__init__.py`, `v5/ewsmcp/migrations/001_init.sql`
- Modify: `v5/tests/conftest.py`
- Test: `v5/tests/test_db.py`

**Interfaces:**
- Produces: `ewsmcp.db.Database(dsn, min_size=1, max_size=4)` with `.conn()` context manager yielding a psycopg connection (dict rows, commit on clean exit, rollback on exception), `.migrate() -> int`, `.schema_version() -> int`, `.require_version(n)`, `.close()`; constant `ewsmcp.db.SCHEMA_VERSION = 1`; exception `ewsmcp.db.SchemaOutdated`.
- Produces: pytest fixtures `pg_dsn` (session) and `db` (function-scoped, fresh schema each test) in `conftest.py`; `make_settings(**overrides)` now includes `database_url`.

- [ ] **Step 1: Branch and venv**

```bash
cd /home/askar/src/ews-mcp && git checkout -b feat/5.0-postgres-daemon
cd v5 && python3 -m venv .venv && .venv/bin/pip install -q --upgrade pip
```

- [ ] **Step 2: Add dependencies and package data to `v5/pyproject.toml`**

Replace the `dependencies` list and add the package-data section:

```toml
dependencies = [
    "exchangelib==5.0.3",
    "httpx>=0.27",
    "jsonschema>=4.21",
    "mcp>=1.27,<2",
    "psycopg[binary]>=3.2",
    "psycopg_pool>=3.2",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "tzdata",
    "uvicorn>=0.30",
]

[tool.setuptools.package-data]
"ewsmcp.migrations" = ["*.sql"]
```

Then `.venv/bin/pip install -q -e '.[dev]'`.

- [ ] **Step 3: Write the failing test `v5/tests/test_db.py`**

```python
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
                "sender_sigs", "aliases", "alias_counters", "meta",
                "schema_migrations"} <= tables
    finally:
        d.close()


def test_require_version_raises_when_behind(db):
    with db.conn() as c:
        c.execute("DELETE FROM ews.schema_migrations")
    with pytest.raises(SchemaOutdated):
        db.require_version(SCHEMA_VERSION)


def test_conn_rolls_back_on_exception(db):
    with pytest.raises(RuntimeError):
        with db.conn() as c:
            c.execute("INSERT INTO ews.meta(key, value) VALUES ('k', 'v')")
            raise RuntimeError("boom")
    with db.conn() as c:
        assert c.execute("SELECT COUNT(*) AS n FROM ews.meta").fetchone()["n"] == 0


def test_generated_tsvector_and_gin_index(db):
    with db.conn() as c:
        c.execute("INSERT INTO ews.messages(ews_id, folder, norm_text) "
                  "VALUES ('M1', 'inbox', 'budget review numbers')")
        hit = c.execute(
            "SELECT ews_id FROM ews.messages WHERE search_tsv @@ "
            "to_tsquery('simple', 'budg:*')").fetchone()
        assert hit["ews_id"] == "M1"
        idx = c.execute("SELECT indexname FROM pg_indexes WHERE schemaname='ews' "
                        "AND tablename='messages' AND indexname='ix_msg_tsv'").fetchone()
        assert idx is not None
    with pytest.raises(psycopg.errors.CheckViolation):
        with db.conn() as c:
            c.execute("INSERT INTO ews.messages(ews_id, folder, archive_state) "
                      "VALUES ('M2', 'inbox', 'bogus')")
```

- [ ] **Step 4: Rewrite `v5/tests/conftest.py`**

```python
"""v5 test fixtures: import path, a real Postgres, per-test schema isolation."""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ewsmcp.confirm import reset_consumed_tokens  # noqa: E402
from ewsmcp.tools.base import reset_send_rate_window  # noqa: E402
from ewsmcp.tools.writes import reset_idempotency_store  # noqa: E402

PG_IMAGE = "pgvector/pgvector:pg16"


def _wait_ready(dsn: str, deadline_s: float = 60.0) -> None:
    end = time.time() + deadline_s
    last = None
    while time.time() < end:
        try:
            with psycopg.connect(dsn, connect_timeout=2):
                return
        except Exception as exc:  # noqa: BLE001 - startup race
            last = exc
            time.sleep(0.5)
    raise RuntimeError(f"postgres never became ready: {last}")


@pytest.fixture(scope="session")
def pg_dsn():
    """A Postgres to test against: EWS_TEST_DATABASE_URL, else a throwaway
    docker container that is removed at session end."""
    dsn = os.environ.get("EWS_TEST_DATABASE_URL")
    name = None
    if not dsn:
        if shutil.which("docker") is None:
            pytest.skip("no EWS_TEST_DATABASE_URL and no docker — Postgres tests skipped")
        name = f"ewsmcp-test-pg-{os.getpid()}"
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", name,
             "-e", "POSTGRES_PASSWORD=test", "-p", "127.0.0.1:0:5432", PG_IMAGE],
            check=True, capture_output=True,
        )
        port_line = subprocess.run(
            ["docker", "port", name, "5432/tcp"], check=True,
            capture_output=True, text=True,
        ).stdout.strip().splitlines()[0]
        port = port_line.rsplit(":", 1)[1]
        dsn = f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
        _wait_ready(dsn)
    os.environ["DATABASE_URL"] = dsn
    yield dsn
    if name:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


@pytest.fixture
def db(pg_dsn):
    """Fresh `ews` schema per test."""
    from ewsmcp.db import Database
    d = Database(pg_dsn, min_size=1, max_size=2)
    with d.conn() as c:
        c.execute("DROP SCHEMA IF EXISTS ews CASCADE")
    d.migrate()
    yield d
    d.close()


@pytest.fixture(autouse=True)
def _isolate_stores(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    reset_send_rate_window()
    reset_consumed_tokens()
    reset_idempotency_store()
    yield
    reset_send_rate_window()
    reset_consumed_tokens()
    reset_idempotency_store()


def make_settings(**overrides):
    """Settings with synthetic Exchange endpoints and the test DATABASE_URL
    (exported by the pg_dsn fixture)."""
    from ewsmcp.config import Settings
    base = dict(
        ews_server_url="https://mail.corp.example/EWS/Exchange.asmx",
        ews_email="exec@corp.example",
        ews_username="svc",
        ews_password="pw",
        mcp_transport="stdio",
        database_url=os.environ.get("DATABASE_URL", "postgresql://unused"),
    )
    base.update(overrides)
    return Settings(**base)
```

(`Settings.database_url` does not exist until Task 4; `extra="ignore"` in the model config means the kwarg is dropped silently until then, so this file works from Task 1 on.)

- [ ] **Step 5: Run the test to see it fail**

Run: `cd v5 && .venv/bin/python -m pytest tests/test_db.py -q`
Expected: `ModuleNotFoundError: No module named 'ewsmcp.db'`

- [ ] **Step 6: Write `v5/ewsmcp/migrations/__init__.py`** (empty file) **and `v5/ewsmcp/migrations/001_init.sql`**

```sql
-- ews schema v1: the mirror tables (ported from the 4.5 SQLite schema),
-- the alias map, and the archive columns Phase 2 will fill.
CREATE TABLE ews.meta (
    key   text PRIMARY KEY,
    value text
);

CREATE TABLE ews.messages (
    ews_id              text PRIMARY KEY,
    changekey           text,
    folder              text NOT NULL,
    conversation_id     text,
    sender_name         text,
    sender_email        text,
    to_json             text,
    subject             text,
    date_ts             bigint,
    date_iso            text,
    is_read             smallint NOT NULL DEFAULT 1,
    has_attachments     smallint NOT NULL DEFAULT 0,
    importance          text,
    categories_json     text,
    body_clean          text,
    internet_message_id text,
    norm_text           text NOT NULL DEFAULT '',
    archive_state       text NOT NULL DEFAULT 'live'
                        CHECK (archive_state IN ('live', 'captured', 'verified', 'deleted')),
    archived_at         timestamptz,
    verified_at         timestamptz,
    deleted_at          timestamptz,
    mime_sha256         text,
    mime_path           text,
    embedded_at         timestamptz,
    search_tsv          tsvector GENERATED ALWAYS AS (to_tsvector('simple', norm_text)) STORED
);
CREATE INDEX ix_msg_folder_date  ON ews.messages (folder, date_ts DESC);
CREATE INDEX ix_msg_conversation ON ews.messages (conversation_id);
CREATE INDEX ix_msg_sender       ON ews.messages (lower(sender_email));
CREATE INDEX ix_msg_imid         ON ews.messages (internet_message_id);
CREATE INDEX ix_msg_state        ON ews.messages (archive_state);
CREATE INDEX ix_msg_tsv          ON ews.messages USING GIN (search_tsv);

CREATE TABLE ews.events (
    ews_id       text PRIMARY KEY,
    changekey    text,
    subject      text,
    start_ts     bigint,
    start_iso    text,
    end_ts       bigint,
    end_iso      text,
    location     text,
    organizer    text,
    is_recurring smallint NOT NULL DEFAULT 0,
    my_response  text
);
CREATE INDEX ix_events_start ON ews.events (start_ts);

CREATE TABLE ews.tasks (
    ews_id      text PRIMARY KEY,
    changekey   text,
    subject     text,
    due_ts      bigint,
    due_iso     text,
    is_complete smallint NOT NULL DEFAULT 0,
    status      text
);

CREATE TABLE ews.folders (
    ews_id   text PRIMARY KEY,
    name     text,
    path     text,
    wk       text,
    total    integer,
    unread   integer,
    children integer
);

CREATE TABLE ews.sync_state (
    key   text PRIMARY KEY,
    token text,
    as_of bigint
);

CREATE TABLE ews.sender_sigs (
    sender_email text NOT NULL,
    sig_hash     text NOT NULL,
    hits         integer NOT NULL DEFAULT 1,
    PRIMARY KEY (sender_email, sig_hash)
);

CREATE TABLE ews.aliases (
    alias               text PRIMARY KEY,
    kind                text NOT NULL,
    ews_id              text NOT NULL UNIQUE,
    changekey           text,
    internet_message_id text,
    first_seen          double precision,
    last_seen           double precision
);
CREATE INDEX ix_aliases_imid ON ews.aliases (internet_message_id);

CREATE TABLE ews.alias_counters (
    kind text PRIMARY KEY,
    n    integer NOT NULL
);
```

- [ ] **Step 7: Write `v5/ewsmcp/db.py`**

```python
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
from contextlib import contextmanager
from importlib import resources
from typing import Iterator, List, Tuple

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

SCHEMA = "ews"
SCHEMA_VERSION = 1  # bump together with the newest migrations/NNN_*.sql
_MIGRATION_RE = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")
_MIGRATE_LOCK_KEY = 7355608  # arbitrary, stable advisory-lock id


class SchemaOutdated(RuntimeError):
    """The database schema is older than this build expects."""


class Database:
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 4):
        self.dsn = dsn
        self.pool = ConnectionPool(
            dsn, min_size=min_size, max_size=max_size, open=True,
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
    def migrations() -> List[Tuple[int, str, str]]:
        out: List[Tuple[int, str, str]] = []
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
```

- [ ] **Step 8: Run the tests**

Run: `cd v5 && .venv/bin/python -m pytest tests/test_db.py -q`
Expected: 4 passed (first run pulls the pgvector image; allow a minute).

- [ ] **Step 9: Commit**

```bash
git add v5/pyproject.toml v5/ewsmcp/db.py v5/ewsmcp/migrations v5/tests/conftest.py v5/tests/test_db.py
git commit -m "feat(db): Postgres Database layer, migration 001, test fixture"
```

### Task 2: `normalize.py` without Arabic, `CacheStore` on Postgres

**Files:**
- Rewrite: `v5/ewsmcp/normalize.py`
- Rewrite: `v5/ewsmcp/cache/store.py`
- Modify: `v5/ewsmcp/cache/sync.py` (row builder + drop `semantic`)
- Delete: `v5/tests/test_cache_store.py`, `v5/tests/test_arabic_search.py`
- Create: `v5/tests/test_pg_store.py`

**Interfaces:**
- Consumes: `ewsmcp.db.Database`.
- Produces: `ewsmcp.normalize.normalize_text(str) -> str` (NFKD, combining marks stripped, lowercased) and `ewsmcp.normalize.tsquery(str) -> str` (`"budg:* & rev:*"`, `""` when nothing searchable).
- Produces: `ewsmcp.cache.store.CacheStore(db: Database)` with exactly the 4.5 method surface: `norm_for_row`, `upsert_messages`, `delete_messages_by_id` (= `tombstone_messages`), `set_read_flag`, `apply_categories`, `replace_events`, `upsert_tasks`, `delete_tasks_by_id`, `replace_folders`, `get_sync_state`, `set_sync_state`, `purge`, `watermark`, `watermarks`, `stats`, `search_messages(..., archived="any")`, `strip_learned_signature`, `get_message`, `thread`, `unread_page`, `events_window`, `folder_rows`, `task_rows`, `contact_stats`, `senders_matching`, `sent_without_reply`, `close`. Rows are plain dicts. `make_row(...)` test helper lives in `tests/test_pg_store.py` and is imported by other tests.
- Produces: `SyncEngine(settings, gateway, store)` (no `semantic` argument).

- [ ] **Step 1: Write the failing tests `v5/tests/test_pg_store.py`**

Port `tests/test_cache_store.py` verbatim with these changes: the fixture becomes `def store(db): return CacheStore(db)` (no close), `sqlite3` import and `test_reads_are_read_only_connections` are removed, `make_row` computes `norm_text` via `CacheStore.norm_for_row`, and these tests are added:

```python
def test_prefix_and_accent_folded_search(store):
    store.upsert_messages([
        make_row("M1", subject="Résumé review", body="café numbers"),
        make_row("M2", subject="Отчёт за квартал", body="цифры во вложении"),
    ])
    rows, total = store.search_messages(text="resume")
    assert total == 1 and rows[0]["ews_id"] == "M1"
    rows, total = store.search_messages(text="отч")  # prefix, Cyrillic
    assert total == 1 and rows[0]["ews_id"] == "M2"
    rows, total = store.search_messages(text="cafe numb")  # AND of prefixes
    assert total == 1


def test_text_search_ranks_by_relevance_then_date(store):
    store.upsert_messages([
        make_row("M1", subject="budget", body="budget budget budget", date_ts=100),
        make_row("M2", subject="budget", body="unrelated", date_ts=200),
    ])
    rows, _ = store.search_messages(text="budget")
    assert [r["ews_id"] for r in rows] == ["M1", "M2"]


def test_archived_filter(store):
    store.upsert_messages([make_row("M1"), make_row("M2")])
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET archive_state='verified' WHERE ews_id='M2'")
    assert store.search_messages(archived="only")[1] == 1
    assert store.search_messages(archived="exclude")[1] == 1
    assert store.search_messages(archived="any")[1] == 2
    assert store.get_message("M2")["archive_state"] == "verified"


def test_upsert_never_touches_archive_columns(store):
    store.upsert_messages([make_row("M1")])
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET archive_state='captured', "
                  "mime_sha256='abc' WHERE ews_id='M1'")
    store.upsert_messages([make_row("M1", subject="edited")])
    row = store.get_message("M1")
    assert row["subject"] == "edited"
    assert row["archive_state"] == "captured" and row["mime_sha256"] == "abc"
```

- [ ] **Step 2: Run to verify failure**

Run: `cd v5 && .venv/bin/python -m pytest tests/test_pg_store.py -q`
Expected: failures (`CacheStore.__init__` expects a path / `normalize_ar` import errors).

- [ ] **Step 3: Rewrite `v5/ewsmcp/normalize.py`**

```python
"""Text normalisation shared by the indexer and the query side.

One function feeds ``messages.norm_text`` (from which Postgres generates
``search_tsv`` with the ``simple`` config) and every search query, so the two
sides always agree: NFKD-decompose, drop combining marks (é→e, ё→е), lowercase.
No stemming, no stopwords, no language-specific folding.
"""

import re
import unicodedata

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def normalize_text(text: str) -> str:
    if not text:
        return ""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()


def tsquery(query: str) -> str:
    """Safe ``to_tsquery('simple', …)`` expression: every word becomes a
    prefix term, terms are ANDed. Returns "" when nothing is searchable."""
    tokens = _TOKEN_RE.findall(normalize_text(query or ""))
    return " & ".join(f"{t}:*" for t in tokens if t)
```

- [ ] **Step 4: Rewrite `v5/ewsmcp/cache/store.py`**

```python
"""Per-mailbox Postgres mirror — the "fetch email very fast" core, shared by
the daemon (writer) and every MCP process (readers).

- Cleaned bodies are stored ONCE at sync time (``bodyclean`` output).
- Full-text search: ``messages.search_tsv`` is a generated tsvector over
  ``norm_text`` (``normalize.normalize_text`` of subject + sender + body);
  queries go through ``normalize.tsquery``. Ranked by ``ts_rank_cd`` then date.
- Timestamps are stored twice: epoch seconds (filter/sort) and the display
  ISO string in the server timezone.
- Archive columns (``archive_state``, ``mime_*`` …) are owned by the Phase 2
  archiver; ``upsert_messages`` never touches them.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg

from ..db import Database
from ..normalize import normalize_text, tsquery

logger = logging.getLogger(__name__)

SIG_MIN_HITS = 3
_SIG_MAX_LINES = 6
_SIG_MAX_CHARS = 400
_ARCHIVED = {"any": "TRUE", "only": "m.archive_state <> 'live'",
             "exclude": "m.archive_state = 'live'"}


def trailing_block(body: str) -> Optional[str]:
    """The candidate signature block: the last blank-line-separated block,
    when it is short enough to be a signature and is not the whole body."""
    body = (body or "").rstrip()
    if not body:
        return None
    head, sep, tail = body.rpartition("\n\n")
    if not sep or not head.strip():
        return None
    tail = tail.strip()
    if not tail or len(tail) > _SIG_MAX_CHARS:
        return None
    if tail.count("\n") + 1 > _SIG_MAX_LINES:
        return None
    return tail


def _sig_hash(sender_email: str, block: str) -> str:
    return hashlib.sha256(
        f"{sender_email.lower()}|{normalize_text(block)}".encode()).hexdigest()


_UPSERT_MESSAGE = """
INSERT INTO ews.messages (ews_id, changekey, folder, conversation_id, sender_name,
    sender_email, to_json, subject, date_ts, date_iso, is_read, has_attachments,
    importance, categories_json, body_clean, internet_message_id, norm_text)
VALUES (%(ews_id)s, %(changekey)s, %(folder)s, %(conversation_id)s, %(sender_name)s,
    %(sender_email)s, %(to_json)s, %(subject)s, %(date_ts)s, %(date_iso)s, %(is_read)s,
    %(has_attachments)s, %(importance)s, %(categories_json)s, %(body_clean)s,
    %(internet_message_id)s, %(norm_text)s)
ON CONFLICT (ews_id) DO UPDATE SET
    changekey = EXCLUDED.changekey, folder = EXCLUDED.folder,
    conversation_id = EXCLUDED.conversation_id, sender_name = EXCLUDED.sender_name,
    sender_email = EXCLUDED.sender_email, to_json = EXCLUDED.to_json,
    subject = EXCLUDED.subject, date_ts = EXCLUDED.date_ts, date_iso = EXCLUDED.date_iso,
    is_read = EXCLUDED.is_read, has_attachments = EXCLUDED.has_attachments,
    importance = EXCLUDED.importance, categories_json = EXCLUDED.categories_json,
    body_clean = EXCLUDED.body_clean, internet_message_id = EXCLUDED.internet_message_id,
    norm_text = EXCLUDED.norm_text
"""


class CacheStore:
    """Owner of the mirror queries. One instance per process; the pool is the db's."""

    def __init__(self, db: Database):
        self.db = db

    def close(self) -> None:  # kept for call-site compatibility
        return None

    # ------------------------------------------------------------- writers

    @staticmethod
    def norm_for_row(subject: str, sender_name: str, sender_email: str,
                     body_clean: str) -> str:
        return normalize_text(
            " ".join(p for p in (subject, sender_name, sender_email, body_clean) if p))

    def upsert_messages(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.db.conn() as c:
            for r in rows:
                block = trailing_block(r.get("body_clean") or "")
                sender = (r.get("sender_email") or "").lower()
                if block and sender:
                    c.execute(
                        "INSERT INTO ews.sender_sigs (sender_email, sig_hash, hits) "
                        "VALUES (%s, %s, 1) ON CONFLICT (sender_email, sig_hash) "
                        "DO UPDATE SET hits = ews.sender_sigs.hits + 1",
                        (sender, _sig_hash(sender, block)))
            c.executemany(_UPSERT_MESSAGE, rows)
        return len(rows)

    def delete_messages_by_id(self, ews_ids: List[str]) -> int:
        if not ews_ids:
            return 0
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.messages WHERE ews_id = ANY(%s)", (list(ews_ids),))
        return len(ews_ids)

    tombstone_messages = delete_messages_by_id

    def set_read_flag(self, ews_ids: List[str], is_read: bool) -> None:
        if not ews_ids:
            return
        with self.db.conn() as c:
            c.execute("UPDATE ews.messages SET is_read = %s WHERE ews_id = ANY(%s)",
                      (1 if is_read else 0, list(ews_ids)))

    def apply_categories(self, ews_id: str, categories: Optional[List[str]]) -> None:
        with self.db.conn() as c:
            c.execute("UPDATE ews.messages SET categories_json = %s WHERE ews_id = %s",
                      (json.dumps(categories or []), ews_id))

    def replace_events(self, rows: List[Dict[str, Any]]) -> None:
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.events")
            c.executemany(
                "INSERT INTO ews.events (ews_id, changekey, subject, start_ts, start_iso, "
                "end_ts, end_iso, location, organizer, is_recurring, my_response) VALUES "
                "(%(ews_id)s, %(changekey)s, %(subject)s, %(start_ts)s, %(start_iso)s, "
                "%(end_ts)s, %(end_iso)s, %(location)s, %(organizer)s, %(is_recurring)s, "
                "%(my_response)s) ON CONFLICT (ews_id) DO UPDATE SET "
                "changekey = EXCLUDED.changekey, subject = EXCLUDED.subject, "
                "start_ts = EXCLUDED.start_ts, start_iso = EXCLUDED.start_iso, "
                "end_ts = EXCLUDED.end_ts, end_iso = EXCLUDED.end_iso, "
                "location = EXCLUDED.location, organizer = EXCLUDED.organizer, "
                "is_recurring = EXCLUDED.is_recurring, my_response = EXCLUDED.my_response",
                rows)

    def upsert_tasks(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        with self.db.conn() as c:
            c.executemany(
                "INSERT INTO ews.tasks (ews_id, changekey, subject, due_ts, due_iso, "
                "is_complete, status) VALUES (%(ews_id)s, %(changekey)s, %(subject)s, "
                "%(due_ts)s, %(due_iso)s, %(is_complete)s, %(status)s) "
                "ON CONFLICT (ews_id) DO UPDATE SET changekey = EXCLUDED.changekey, "
                "subject = EXCLUDED.subject, due_ts = EXCLUDED.due_ts, "
                "due_iso = EXCLUDED.due_iso, is_complete = EXCLUDED.is_complete, "
                "status = EXCLUDED.status", rows)

    def delete_tasks_by_id(self, ews_ids: List[str]) -> None:
        if not ews_ids:
            return
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.tasks WHERE ews_id = ANY(%s)", (list(ews_ids),))

    def replace_folders(self, rows: List[Dict[str, Any]]) -> None:
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.folders")
            c.executemany(
                "INSERT INTO ews.folders (ews_id, name, path, wk, total, unread, children) "
                "VALUES (%(ews_id)s, %(name)s, %(path)s, %(wk)s, %(total)s, %(unread)s, "
                "%(children)s) ON CONFLICT (ews_id) DO UPDATE SET name = EXCLUDED.name, "
                "path = EXCLUDED.path, wk = EXCLUDED.wk, total = EXCLUDED.total, "
                "unread = EXCLUDED.unread, children = EXCLUDED.children", rows)

    def get_sync_state(self, key: str) -> Optional[str]:
        with self.db.conn() as c:
            row = c.execute("SELECT token FROM ews.sync_state WHERE key = %s",
                            (key,)).fetchone()
        return row["token"] if row else None

    def set_sync_state(self, key: str, token: Optional[str],
                       as_of_ts: Optional[float] = None) -> None:
        with self.db.conn() as c:
            c.execute(
                "INSERT INTO ews.sync_state (key, token, as_of) VALUES (%s, %s, %s) "
                "ON CONFLICT (key) DO UPDATE SET token = EXCLUDED.token, "
                "as_of = EXCLUDED.as_of",
                (key, token, int(as_of_ts if as_of_ts is not None else time.time())))

    def purge(self) -> None:
        with self.db.conn() as c:
            c.execute("TRUNCATE ews.messages, ews.events, ews.tasks, ews.folders, "
                      "ews.sync_state, ews.sender_sigs")
        logger.warning("mirror purged")

    # -------------------------------------------------------------- reads

    def watermark(self, key: str) -> Optional[int]:
        try:
            with self.db.conn() as c:
                row = c.execute("SELECT as_of FROM ews.sync_state WHERE key = %s",
                                (key,)).fetchone()
            return int(row["as_of"]) if row and row["as_of"] is not None else None
        except psycopg.Error:
            return None

    def watermarks(self) -> Dict[str, int]:
        try:
            with self.db.conn() as c:
                rows = c.execute("SELECT key, as_of FROM ews.sync_state").fetchall()
            return {r["key"]: int(r["as_of"]) for r in rows if r["as_of"] is not None}
        except psycopg.Error:
            return {}

    def stats(self) -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        db_mb = 0.0
        try:
            with self.db.conn() as c:
                for table in ("messages", "events", "tasks", "folders"):
                    counts[table] = c.execute(
                        f"SELECT COUNT(*) AS n FROM ews.{table}").fetchone()["n"]  # noqa: S608
                size = c.execute(
                    "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0) AS b "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = 'ews' AND c.relkind = 'r'").fetchone()["b"]
                db_mb = round(int(size) / 1_048_576, 2)
        except psycopg.Error:
            pass
        return {"rows": counts, "db_mb": db_mb, "watermarks": self.watermarks()}

    def search_messages(
        self, *, folders: Optional[List[str]] = None, text: Optional[str] = None,
        sender: Optional[str] = None, subject: Optional[str] = None,
        since_ts: Optional[int] = None, until_ts: Optional[int] = None,
        is_unread: Optional[bool] = None, has_attachments: Optional[bool] = None,
        archived: str = "any", offset: int = 0, limit: int = 20,
    ) -> Tuple[List[Dict[str, Any]], int]:
        where: List[str] = [_ARCHIVED.get(archived, "TRUE")]
        params: List[Any] = []
        q = tsquery(text) if text else ""
        if q:
            where.append("m.search_tsv @@ to_tsquery('simple', %s)")
            params.append(q)
        if folders:
            where.append("m.folder = ANY(%s)")
            params.append(list(folders))
        if sender:
            needle = f"%{sender.strip().lower()}%"
            where.append("(lower(m.sender_email) LIKE %s OR lower(m.sender_name) LIKE %s)")
            params.extend([needle, needle])
        if subject:
            where.append("lower(m.subject) LIKE %s")
            params.append(f"%{subject.strip().lower()}%")
        if since_ts is not None:
            where.append("m.date_ts >= %s")
            params.append(int(since_ts))
        if until_ts is not None:
            where.append("m.date_ts <= %s")
            params.append(int(until_ts))
        if is_unread is not None:
            where.append("m.is_read = %s")
            params.append(0 if is_unread else 1)
        if has_attachments is not None:
            where.append("m.has_attachments = %s")
            params.append(1 if has_attachments else 0)
        base = "FROM ews.messages m WHERE " + " AND ".join(where)
        order, order_params = "m.date_ts DESC", []
        if q:
            order = "ts_rank_cd(m.search_tsv, to_tsquery('simple', %s)) DESC, m.date_ts DESC"
            order_params = [q]
        with self.db.conn() as c:
            total = c.execute(f"SELECT COUNT(*) AS n {base}", params).fetchone()["n"]  # noqa: S608
            rows = c.execute(
                f"SELECT m.* {base} ORDER BY {order} LIMIT %s OFFSET %s",  # noqa: S608
                [*params, *order_params, int(limit), int(offset)]).fetchall()
        return rows, int(total)

    def strip_learned_signature(self, sender_email: str, body: str) -> str:
        block = trailing_block(body)
        sender = (sender_email or "").lower()
        if not block or not sender:
            return body
        try:
            with self.db.conn() as c:
                row = c.execute(
                    "SELECT hits FROM ews.sender_sigs WHERE sender_email = %s "
                    "AND sig_hash = %s", (sender, _sig_hash(sender, block))).fetchone()
        except psycopg.Error:
            return body
        if row is not None and row["hits"] >= SIG_MIN_HITS:
            return body.rstrip().rpartition("\n\n")[0].rstrip()
        return body

    def get_message(self, ews_id: str) -> Optional[Dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.messages WHERE ews_id = %s OR internet_message_id = %s "
                "LIMIT 1", (ews_id, ews_id)).fetchone()

    def thread(self, conversation_id: str) -> List[Dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.messages WHERE conversation_id = %s ORDER BY date_ts ASC",
                (conversation_id,)).fetchall()

    def unread_page(self, limit: int = 10) -> Tuple[int, List[Dict[str, Any]]]:
        with self.db.conn() as c:
            total = c.execute(
                "SELECT COUNT(*) AS n FROM ews.messages WHERE folder = 'inbox' "
                "AND is_read = 0 AND archive_state = 'live'").fetchone()["n"]
            rows = c.execute(
                "SELECT * FROM ews.messages WHERE folder = 'inbox' AND is_read = 0 "
                "AND archive_state = 'live' ORDER BY date_ts DESC LIMIT %s",
                (int(limit),)).fetchall()
        return int(total), rows

    def events_window(self, start_ts: int, end_ts: int,
                      limit: int = 25) -> List[Dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.events WHERE start_ts < %s AND end_ts > %s "
                "ORDER BY start_ts ASC LIMIT %s",
                (int(end_ts), int(start_ts), int(limit))).fetchall()

    def folder_rows(self) -> List[Dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute("SELECT * FROM ews.folders ORDER BY path ASC").fetchall()

    def task_rows(self, include_completed: bool = False, offset: int = 0,
                  limit: int = 50) -> Tuple[List[Dict[str, Any]], int]:
        clause = "" if include_completed else " WHERE is_complete = 0"
        with self.db.conn() as c:
            total = c.execute(f"SELECT COUNT(*) AS n FROM ews.tasks{clause}").fetchone()["n"]  # noqa: S608
            rows = c.execute(
                f"SELECT * FROM ews.tasks{clause} ORDER BY COALESCE(due_ts, 1e15) ASC "  # noqa: S608
                "LIMIT %s OFFSET %s", (int(limit), int(offset))).fetchall()
        return rows, int(total)

    def contact_stats(self, email: str) -> Dict[str, Any]:
        needle = (email or "").strip().lower()
        if not needle:
            return {}
        with self.db.conn() as c:
            received = c.execute(
                "SELECT COUNT(*) AS n, MIN(date_iso) AS first, MAX(date_iso) AS last "
                "FROM ews.messages WHERE lower(sender_email) = %s AND folder <> 'sent'",
                (needle,)).fetchone()
            sent = c.execute(
                "SELECT COUNT(*) AS n, MAX(date_iso) AS last FROM ews.messages "
                "WHERE folder = 'sent' AND lower(to_json) LIKE %s",
                (f"%{needle}%",)).fetchone()
        out: Dict[str, Any] = {}
        if received and received["n"]:
            out.update({"received_count": received["n"], "first_seen": received["first"],
                        "last_received": received["last"]})
        if sent and sent["n"]:
            out.update({"sent_count": sent["n"], "last_sent": sent["last"]})
        return out

    def senders_matching(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        needle = f"%{(query or '').strip().lower()}%"
        with self.db.conn() as c:
            return c.execute(
                "SELECT lower(sender_email) AS sender_email, MAX(sender_name) AS sender_name, "
                "COUNT(*) AS msgs, MAX(date_iso) AS last_seen FROM ews.messages "
                "WHERE folder <> 'sent' AND (lower(sender_email) LIKE %s OR "
                "lower(sender_name) LIKE %s) GROUP BY lower(sender_email) "
                "ORDER BY msgs DESC LIMIT %s", (needle, needle, int(limit))).fetchall()

    def sent_without_reply(self, days: int = 5, limit: int = 25) -> List[Dict[str, Any]]:
        cutoff = int(time.time() - days * 86400)
        with self.db.conn() as c:
            return c.execute(
                """
                SELECT s.* FROM ews.messages s
                WHERE s.folder = 'sent' AND s.date_ts <= %s
                  AND s.conversation_id IS NOT NULL
                  AND s.date_ts = (SELECT MAX(x.date_ts) FROM ews.messages x
                                   WHERE x.conversation_id = s.conversation_id
                                     AND x.folder = 'sent')
                  AND NOT EXISTS (SELECT 1 FROM ews.messages i
                                  WHERE i.conversation_id = s.conversation_id
                                    AND i.folder <> 'sent' AND i.date_ts > s.date_ts)
                ORDER BY s.date_ts DESC LIMIT %s
                """, (cutoff, int(limit))).fetchall()
```

- [ ] **Step 5: Update `v5/ewsmcp/cache/sync.py`**

Remove the `semantic` constructor parameter and attribute, delete the `if self.semantic is not None ...` block at the end of `_sync_mail_folders`, and leave `row_from_message` unchanged (it already calls `CacheStore.norm_for_row`). Update `v5/ewsmcp/cache/__init__.py` if it re-exports anything removed.

- [ ] **Step 6: Delete the old tests, fix `test_sync_engine.py`**

```bash
git rm -q v5/tests/test_cache_store.py v5/tests/test_arabic_search.py
```

In `v5/tests/test_sync_engine.py`: `_engine(tmp_path, account, **overrides)` becomes `_engine(db, account, **overrides)` with `store = CacheStore(db)`; every test takes `db` instead of `tmp_path`; `test_cycle_failure_degrades_not_dies` builds `CacheStore(db)`. In `test_row_from_message_cleans_body_once` keep the `norm_text` assertion.

- [ ] **Step 7: Run**

Run: `cd v5 && .venv/bin/python -m pytest tests/test_pg_store.py tests/test_sync_engine.py -q`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add -A v5/ewsmcp/normalize.py v5/ewsmcp/cache v5/tests
git commit -m "feat(store): CacheStore on Postgres with tsvector search; drop Arabic normaliser"
```

### Task 3: `IdAliaser` on Postgres

**Files:**
- Rewrite: `v5/ewsmcp/ids.py`
- Rewrite: `v5/tests/test_ids.py`

**Interfaces:**
- Produces: `ewsmcp.ids.IdAliaser(db: Database)` with `alias_for`, `alias_many`, `resolve`, `rebind`, `imid_for`, `stats` (same signatures and semantics as 4.5: fail-open on storage errors, `KeyError` on unknown alias-shaped input); `NullAliaser`; `kind_for_key`. `get_aliaser` / `reset_aliaser_cache` are removed.

- [ ] **Step 1: Rewrite `v5/tests/test_ids.py`**

Keep the module docstring intent, `RAW_A`/`RAW_B`, and these tests from the 4.5 file unchanged in body: `test_mint_and_idempotent_realias`, `test_per_kind_counters_are_independent`, `test_resolve_roundtrip`, `test_resolve_passes_through_non_alias_values`, `test_resolve_unknown_alias_raises_helpful_keyerror`, `test_rebind_keeps_alias_and_resolves_to_new_id`, `test_rebind_unknown_old_id_returns_none`, `test_rebind_onto_id_already_aliased_elsewhere`, `test_imid_storage_and_retrieval`, `test_thread_safety_smoke`, `test_kind_for_key_mapping`. Replace the fixture and the storage tests:

```python
import pytest

from ewsmcp.ids import IdAliaser, NullAliaser, kind_for_key  # noqa: F401


@pytest.fixture
def aliaser(db) -> IdAliaser:
    return IdAliaser(db)


def test_persistence_across_instances_on_same_db(db):
    first = IdAliaser(db)
    alias = first.alias_for(RAW_A, kind="m")
    second = IdAliaser(db)
    assert second.resolve(alias) == RAW_A
    assert second.alias_for("ID-NEW=", kind="m") == "m2"  # counter continues


def test_alias_many_mints_in_one_transaction(aliaser):
    out = aliaser.alias_many([("A=", "m", None, None), ("B=", "e", "CK", "<b@x>"),
                              ("", "m", None, None)])
    assert out == {"A=": "m1", "B=": "e1"}
    assert aliaser.imid_for("e1") == "<b@x>"


def test_alias_for_and_rebind_swallow_storage_errors(db, monkeypatch):
    aliaser = IdAliaser(db)
    aliaser.alias_for(RAW_A)
    db.close()  # every later query fails
    assert aliaser.alias_for("NEW=") == "NEW="  # fail open: raw id back
    assert aliaser.rebind(RAW_A, RAW_B) is None
    assert aliaser.stats() == {}
    assert aliaser.resolve("m1") == "m1"  # lookup failed → pass through


def test_concurrent_mint_of_same_id_yields_one_alias(db):
    import threading
    aliaser = IdAliaser(db)
    results = []

    def mint():
        results.append(aliaser.alias_for("SAME=", kind="m"))

    threads = [threading.Thread(target=mint) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(results) == {"m1"}
    assert aliaser.stats() == {"m": 1}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd v5 && .venv/bin/python -m pytest tests/test_ids.py -q` → import/constructor errors.

- [ ] **Step 3: Rewrite `v5/ewsmcp/ids.py`**

Keep the module docstring's design goals, `_ALIAS_RE`, `_KIND_RE`, `_KIND_BY_KEY`, `kind_for_key`, and `NullAliaser` verbatim. Replace the class:

```python
import logging
import re
import time
from typing import Optional

import psycopg

from .db import Database

_LOG = logging.getLogger(__name__)

_MINT = """
INSERT INTO ews.alias_counters (kind, n) VALUES (%s, 1)
ON CONFLICT (kind) DO UPDATE SET n = ews.alias_counters.n + 1
RETURNING n
"""


class IdAliaser:
    """Postgres-backed bidirectional map between short aliases and EWS ids.
    Shared by every process on the same database: the daemon and all MCPs
    mint from one counter sequence."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def _find(self, c, ews_id: str):
        return c.execute(
            "SELECT alias, changekey, internet_message_id FROM ews.aliases "
            "WHERE ews_id = %s", (ews_id,)).fetchone()

    def _mint_locked(self, c, ews_id: str, kind: str, changekey, imid) -> str:
        """Inside a transaction: return the existing alias (refreshing metadata)
        or mint a new one. A concurrent minter loses on UNIQUE(ews_id) and
        re-reads the winner's row."""
        now = time.time()
        row = self._find(c, ews_id)
        if row:
            c.execute(
                "UPDATE ews.aliases SET last_seen = %s, changekey = COALESCE(%s, changekey), "
                "internet_message_id = COALESCE(%s, internet_message_id) WHERE alias = %s",
                (now, changekey, imid, row["alias"]))
            return row["alias"]
        n = c.execute(_MINT, (kind,)).fetchone()["n"]
        alias = f"{kind}{n}"
        try:
            with c.transaction():  # savepoint: a lost race must not poison the txn
                c.execute(
                    "INSERT INTO ews.aliases (alias, kind, ews_id, changekey, "
                    "internet_message_id, first_seen, last_seen) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (alias, kind, ews_id, changekey, imid, now, now))
        except psycopg.errors.UniqueViolation:
            row = self._find(c, ews_id)
            return row["alias"] if row else ews_id
        return alias

    def alias_for(self, ews_id: str, kind: str = "m", changekey: Optional[str] = None,
                  internet_message_id: Optional[str] = None) -> str:
        if not ews_id:
            return ews_id
        if not _KIND_RE.match(kind):
            _LOG.warning("id_alias: invalid kind %r; using 'x'", kind)
            kind = "x"
        try:
            with self.db.conn() as c:
                row = self._find(c, ews_id)
                if row is not None and (
                    changekey is None or row["changekey"] == changekey
                ) and (
                    internet_message_id is None
                    or row["internet_message_id"] == internet_message_id
                ):
                    return row["alias"]
                return self._mint_locked(c, ews_id, kind, changekey, internet_message_id)
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: alias_for failed (%s); returning raw id", exc)
            return ews_id

    def alias_many(self, entries) -> dict:
        out: dict = {}
        if not entries:
            return out
        try:
            with self.db.conn() as c:
                for ews_id, kind, changekey, imid in entries:
                    if not ews_id:
                        continue
                    if not _KIND_RE.match(kind):
                        kind = "x"
                    out[ews_id] = self._mint_locked(c, ews_id, kind, changekey, imid)
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: alias_many failed (%s); falling back", exc)
        return out

    def resolve(self, value: str) -> str:
        if not isinstance(value, str) or not _ALIAS_RE.match(value):
            return value
        try:
            with self.db.conn() as c:
                row = c.execute("SELECT ews_id FROM ews.aliases WHERE alias = %s",
                                (value,)).fetchone()
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: resolve lookup failed for %r (%s); passing through",
                         value, exc)
            return value
        if row is None:
            raise KeyError(
                f"Unknown alias {value!r}: it is stale or from a previous session. "
                "EWS ids change when items move; re-run the search/list tool to get "
                "fresh ids, then retry.")
        return row["ews_id"]

    def rebind(self, old_ews_id: str, new_ews_id: str,
               changekey: Optional[str] = None) -> Optional[str]:
        try:
            with self.db.conn() as c:
                row = c.execute("SELECT alias FROM ews.aliases WHERE ews_id = %s FOR UPDATE",
                                (old_ews_id,)).fetchone()
                if not row:
                    return None
                alias = row["alias"]
                c.execute("DELETE FROM ews.aliases WHERE ews_id = %s AND alias <> %s",
                          (new_ews_id, alias))
                c.execute(
                    "UPDATE ews.aliases SET ews_id = %s, changekey = COALESCE(%s, changekey), "
                    "last_seen = %s WHERE alias = %s",
                    (new_ews_id, changekey, time.time(), alias))
                return alias
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: rebind failed (%s); returning None", exc)
            return None

    def imid_for(self, alias_or_id: str) -> Optional[str]:
        try:
            with self.db.conn() as c:
                row = c.execute(
                    "SELECT internet_message_id FROM ews.aliases "
                    "WHERE alias = %s OR ews_id = %s", (alias_or_id, alias_or_id)).fetchone()
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: imid_for failed (%s)", exc)
            return None
        return row["internet_message_id"] if row else None

    def stats(self) -> dict:
        try:
            with self.db.conn() as c:
                rows = c.execute(
                    "SELECT kind, COUNT(*) AS c FROM ews.aliases GROUP BY kind").fetchall()
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: stats failed (%s)", exc)
            return {}
        return {r["kind"]: r["c"] for r in rows}
```

`RuntimeError` is caught because a closed `ConnectionPool` raises `psycopg_pool.PoolClosed`, a `RuntimeError` subclass — that is the fail-open path the test exercises.

- [ ] **Step 4: Run** `cd v5 && .venv/bin/python -m pytest tests/test_ids.py -q` → all pass.

- [ ] **Step 5: Commit**

```bash
git add v5/ewsmcp/ids.py v5/tests/test_ids.py
git commit -m "feat(ids): alias map on Postgres, shared across processes"
```

### Task 4: Settings, wiring, and the whole suite green on Postgres (single process)

This is the first milestone: today's one-process server, storing in Postgres. Deployable on its own.

**Files:**
- Modify: `v5/ewsmcp/config.py`, `v5/ewsmcp/errors.py`, `v5/ewsmcp/server.py`, `v5/ewsmcp/tools/base.py`, `v5/ewsmcp/tools/mail_read.py`, `v5/ewsmcp/tools/calendar_people.py`, `v5/ewsmcp/tools/__init__.py`, `v5/ewsmcp/__init__.py`, `v5/pyproject.toml`
- Delete: `v5/ewsmcp/semantic.py`
- Modify: `v5/tests/conftest.py` (add `make_context`), `test_cache_first_reads.py`, `test_calendar_people.py`, `test_http_shim.py`, `test_mail_read.py`, `test_dispatcher.py`, `test_envelope_contract.py`, `test_surface_completion.py`, `test_north_star.py`, `test_writes.py`, `test_config_guards.py`, `test_docs_match_registry.py`

**Interfaces:**
- Produces: `Settings.database_url: str` (required), `Settings.ewsd_host/ewsd_port/ewsd_api_key/ewsd_url`, `Settings.require_exchange()`; `ews_server_url`, `ews_password` become `Optional`. Removed: `ews_cache_enabled`, `ews_cache_purge_on_boot`, `ews_semantic_*`.
- Produces: `errors.HTTP_BY_CODE` gains `"daemon_unavailable": 503`, `"backend_unavailable": 503`.
- Produces: `tools.base.Context.db: Any = None`, `Context.daemon: Any = None`; `tools.base.resolve_ids` (public name of `_resolve_ids`, old name kept as alias).
- Produces: `server.build_context(settings) -> Context` opening `Database(settings.database_url)`, running `db.migrate()`, wiring `CacheStore(db)` and `IdAliaser(db)`.
- Produces: `conftest.make_context(db, gateway=None, cache=True, **overrides) -> Context`.

- [ ] **Step 1: `v5/ewsmcp/config.py`**

Replace the Exchange, serving, and cache/semantic blocks:

```python
    # --- Exchange upstream (the daemon needs these; the MCP does not) --------
    ews_server_url: Optional[str] = None
    ews_email: str  # both processes: own-domain checks, confirm tokens, audit
    ews_username: Optional[str] = None
    ews_password: Optional[str] = None
    ews_auth_type_force: Optional[Literal["basic", "ntlm", "digest"]] = None
    ews_insecure_skip_verify: bool = False
    ews_tz: str = "Asia/Riyadh"
    request_timeout: int = 30

    # --- Storage: Postgres (both processes) -----------------------------------
    database_url: str  # postgresql://user:pass@host:5432/ews

    # --- Daemon HTTP API (ewsd serves; ewsmcp calls) ---------------------------
    ewsd_host: str = "127.0.0.1"
    ewsd_port: int = 8790
    ewsd_api_key: Optional[str] = None  # bearer the MCP presents; required off-loopback
    ewsd_url: str = "http://127.0.0.1:8790"

    # --- Mirror sync (daemon) ---------------------------------------------------
    ews_cache_folders: str = "inbox,sent"
    ews_cache_sync_seconds: int = 45
    ews_cache_hierarchy_seconds: int = 600
    ews_cache_window_days: int = 365
```

Keep `mcp_transport/mcp_host/mcp_port/mcp_api_key/log_level/external_url`, the storage block, the safety block, the response-economy block, and `_resolve_data_dir`. Add:

```python
    def require_exchange(self) -> None:
        """ewsd boot guard: the daemon cannot run without an Exchange endpoint."""
        missing = [n for n in ("ews_server_url", "ews_email", "ews_password")
                   if not getattr(self, n)]
        if missing:
            raise ValueError("ewsd needs " + ", ".join(m.upper() for m in missing))
```

- [ ] **Step 2: `v5/ewsmcp/errors.py`** — add to `HTTP_BY_CODE`: `"daemon_unavailable": 503, "backend_unavailable": 503`.

- [ ] **Step 3: `v5/ewsmcp/tools/base.py`** — add fields `db: Any = None` and `daemon: Any = None` to `Context` (after `semantic`; keep `semantic` for now so old call sites compile, remove it in Step 5). Rename `_resolve_ids` to `resolve_ids` and add `_resolve_ids = resolve_ids` below it.

- [ ] **Step 4: `v5/ewsmcp/server.py::build_context`**

```python
def build_context(settings: Settings) -> Context:
    from .cache import CacheStore
    from .db import Database
    from .ids import IdAliaser

    gateway = EWSGateway(settings)
    db = Database(settings.database_url)
    db.migrate()
    aliaser = IdAliaser(db)
    try:
        audit = AuditLog(settings.data_dir)
    except Exception as exc:
        logger.error("audit init failed (%s) — audit disabled", exc)
        audit = _NullAudit()
    ctx = Context(settings=settings, gateway=gateway, manager=None, aliaser=aliaser,
                  audit=audit, cache=CacheStore(db), db=db)
    build_registry(ctx)
    return ctx
```

`Database`/`CacheStore`/`IdAliaser` imports go to module top (they are not exchangelib, the lazy-import rule does not apply, but module-top is cleaner). In `start_connection_manager.on_warm`, construct `SyncEngine(ctx.settings, ctx.gateway, ctx.cache)` (no `semantic`). Drop the semantic block entirely.

- [ ] **Step 5: Remove the semantic tier**

- `git rm v5/ewsmcp/semantic.py`.
- `tools/__init__.py`: drop the `SEMANTIC_TOOLS` extension; registry is the four packs, tier-filtered.
- `tools/mail_read.py`: delete `_search_semantic`, `_find_similar`, `SEMANTIC_TOOLS`; in `_search_messages` replace the `mode == "semantic"` branch with
  ```python
  if mode == "semantic":
      raise ToolError("validation",
                      "semantic search is not available in this build (it returns "
                      "with the archive tier).", hint="Use mode='keyword'.")
  ```
  keep the `mode` schema property (description: "semantic is reserved; keyword only in this build").
- `tools/base.py`: remove `semantic` from `Context`.
- `tools/calendar_people.py::_get_server_status`: `cache_block = {"enabled": True, "ready": ctx.cache is not None}`.
- `scripts/dump_tool_table.py::_packs`: drop the semantic pack tuple.
- `http.py::_metrics_text`: drop the `ctx.semantic` lines.

- [ ] **Step 6: Version bump** — `ewsmcp/__init__.py` `__version__ = "5.0.0a1"`, `pyproject.toml` `version = "5.0.0a1"`, and in `tests/test_docs_match_registry.py` rename `test_version_is_45_line` → `test_version_is_50_line` asserting `startswith("5.0.")`.

- [ ] **Step 7: Test helpers in `conftest.py`**

Append:

```python
def make_context(db, gateway=None, cache=True, **overrides):
    """A Context wired to the test database (aliases + mirror), no audit disk,
    registry built. `cache=False` leaves ctx.cache None (pure-EWS reads)."""
    from ewsmcp.cache.store import CacheStore
    from ewsmcp.ids import IdAliaser
    from ewsmcp.server import _NullAudit
    from ewsmcp.tools import build_registry
    from ewsmcp.tools.base import Context
    ctx = Context(settings=make_settings(**overrides), gateway=gateway, manager=None,
                  aliaser=IdAliaser(db), audit=_NullAudit(),
                  cache=CacheStore(db) if cache else None, db=db)
    build_registry(ctx)
    return ctx
```

- [ ] **Step 8: Port every remaining test file**

Mechanical rules, apply to each listed file:

1. Add `db` to the test function / helper signature wherever `tmp_path` was only used for aliases or the mirror.
2. `get_aliaser(str(tmp_path / "..."))` → `IdAliaser(db)`; import `from ewsmcp.ids import IdAliaser`.
3. `CacheStore(tmp_path / "mirror.db")` → `CacheStore(db)`; `store.close()` calls may stay (no-op).
4. `from test_cache_store import make_row` → `from test_pg_store import make_row`.
5. `AuditLog(str(tmp_path / "audit"))` may stay (tests that inspect audit files need it).
6. `test_surface_completion.py`: delete `from ewsmcp.semantic import rrf_merge` and the tests that use `rrf_merge`, `semantic=`, or `find_similar`; in `test_registry_counts_per_tier_and_semantic` keep 31/26/15 and assert `"find_similar" not in full.registry`, drop the `with_sem` block; the `mode="semantic"` test now asserts `res["error"]["code"] == "validation"`.
7. `test_config_guards.py`: `Settings(...)` constructions gain `database_url="postgresql://x"`; add
   ```python
   def test_require_exchange_lists_missing():
       from ewsmcp.config import Settings
       s = Settings(ews_email="a@b.c", database_url="postgresql://x")
       with pytest.raises(ValueError) as e:
           s.require_exchange()
       assert "EWS_SERVER_URL" in str(e.value) and "EWS_PASSWORD" in str(e.value)
   ```
8. `test_cache_first_reads.py::test_cache_error_falls_back_to_live`: replace `ctx.cache.close()` + monkeypatching with `ctx.cache.search_messages = boom` only.
9. `test_north_star.py`: the token-budget and warm-latency tests stay; only fixtures change. If `test_north_star_search_is_fast_warm` asserts a wall-clock bound, keep it (Postgres on localhost answers in single-digit ms).

Grep to confirm nothing SQLite-shaped remains: `grep -rn "sqlite\|mirror.db\|get_aliaser\|reset_aliaser_cache\|semantic" v5/ewsmcp v5/tests v5/scripts` → only the `mode` description string in `mail_read.py`.

- [ ] **Step 9: Regenerate docs table and run everything**

```bash
cd v5 && .venv/bin/python scripts/dump_tool_table.py --write
.venv/bin/python -m ruff check . && .venv/bin/python -m pytest tests -q
```
Expected: all pass (count will be slightly below 4.5's because the Arabic and semantic tests are gone).

- [ ] **Step 10: Commit**

```bash
git add -A v5
git commit -m "feat(5.0): single process on Postgres — settings, wiring, semantic tier removed, tests ported"
```

### Task 5: Extract cache reads into `tools/cache_reads.py`

Pure refactor: the "serve from mirror" halves of `list_folders`, `search_messages`, `get_message`, `get_thread`, `get_mailbox_overview`, `list_tasks` move into functions that return `Optional[dict]` (`None` = the mirror cannot answer). Daemon handlers call them first, then fall back to Exchange. Task 8 reuses them from the MCP. No behaviour change; `tests/test_cache_first_reads.py` is the regression net.

**Files:**
- Create: `v5/ewsmcp/tools/cache_reads.py`
- Modify: `v5/ewsmcp/tools/mail_read.py`, `v5/ewsmcp/tools/tasks.py`
- Test: `v5/tests/test_cache_reads.py` (new, small) + existing `test_cache_first_reads.py`

**Interfaces:**
- Produces (all `async`, all take `ctx` first, all return `Optional[Dict[str, Any]]` already stamped `source=cache`):
  - `folder_key(ctx, folder_ref: Optional[str]) -> Optional[str]` — mirrored folder key or `None`; mirrored means `f"item:{key}"` is present in `ctx.cache.watermarks()` (no longer read from settings).
  - `list_folders(ctx, depth: int, include_empty: bool)`
  - `validate_search_args(sender, from_, subject, since, until, is_unread, has_attachments, query) -> str` — returns the effective `sender`, raises the two 4.5 validation errors (sender/from_ clash; AQS + structured clash).
  - `search_messages(ctx, *, folder, query, sender, subject, since, until, is_unread, has_attachments, offset, limit)`
  - `get_message(ctx, raw_id: str, format: str)`
  - `get_thread(ctx, raw_id: str, limit: int, offset: int)`
  - `overview(ctx, horizon_days: int)`
  - `list_tasks(ctx, include_completed: bool, offset: int, limit: int)`
  - Helpers moved here and re-exported by `mail_read` for compatibility: `_stamp`, `_row_card`, `_row_full`, `_row_body`.

- [ ] **Step 1: Write `v5/tests/test_cache_reads.py`**

```python
"""cache_reads: None when the mirror cannot answer, stamped dict when it can."""

import asyncio
import time

from conftest import make_context

from ewsmcp.tools import cache_reads
from test_pg_store import make_row


def _seed(ctx):
    now = int(time.time())
    ctx.cache.upsert_messages([make_row("RAW-1", subject="Budget", date_ts=now - 10)])
    ctx.cache.set_sync_state("item:inbox", "TOK", now)


def test_folder_key_uses_watermarks_not_settings(db):
    ctx = make_context(db)
    assert cache_reads.folder_key(ctx, "f:inbox") is None  # nothing synced yet
    _seed(ctx)
    assert cache_reads.folder_key(ctx, "f:inbox") == "inbox"
    assert cache_reads.folder_key(ctx, "inbox") == "inbox"
    assert cache_reads.folder_key(ctx, "f:sent") is None
    assert cache_reads.folder_key(ctx, None) == "inbox"


def test_search_returns_none_for_unmirrored_folder(db):
    ctx = make_context(db)
    _seed(ctx)
    out = asyncio.run(cache_reads.search_messages(
        ctx, folder="f:junk", query=None, sender=None, subject=None, since=None,
        until=None, is_unread=None, has_attachments=None, offset=0, limit=10))
    assert out is None


def test_search_hit_is_stamped(db):
    ctx = make_context(db)
    _seed(ctx)
    out = asyncio.run(cache_reads.search_messages(
        ctx, folder="f:inbox", query="budget", sender=None, subject=None, since=None,
        until=None, is_unread=None, has_attachments=None, offset=0, limit=10))
    assert out["source"] == "cache" and out["as_of"] and out["count"] == 1


def test_get_message_none_when_missing(db):
    ctx = make_context(db)
    assert asyncio.run(cache_reads.get_message(ctx, "NOPE", "full")) is None
```

- [ ] **Step 2: Run → fails** (`ModuleNotFoundError: ewsmcp.tools.cache_reads`).

- [ ] **Step 3: Create `v5/ewsmcp/tools/cache_reads.py`**

Move `_stamp`, `_row_body`, `_row_card`, `_row_full`, `_thread_from_cache` from `mail_read.py` unchanged. Add:

```python
def folder_key(ctx: Context, folder_ref: Optional[str]) -> Optional[str]:
    if ctx.cache is None:
        return None
    key = (folder_ref or "f:inbox").strip().lower()
    if key.startswith("f:"):
        key = key[2:]
    return key if f"item:{key}" in ctx.cache.watermarks() else None


def watermark(ctx: Context, key: str) -> Optional[int]:
    return None if ctx.cache is None else ctx.cache.watermark(f"item:{key}")


def validate_search_args(sender, from_, subject, since, until, is_unread,
                         has_attachments, query) -> Optional[str]:
    if sender and from_:
        raise ToolError("validation", "pass `sender` only — `from_` is its deprecated alias.")
    sender = sender or from_
    structured = any(v is not None for v in
                     (sender, subject, since, until, is_unread, has_attachments))
    if query and structured:
        raise ToolError(
            "validation",
            "`query` (AQS) cannot be combined with the structured filters "
            "(sender/subject/since/until/is_unread/has_attachments) — Exchange runs "
            "them on different engines.",
            hint="Either fold everything into the AQS string (e.g. 'from:ahmed "
                 "subject:rfp received>=2026-06-01') or drop `query` and use only "
                 "structured filters.")
    return sender
```

and the six async functions, each being the body of the corresponding `if not fresh and ctx.cache is not None:` block from the 4.5 handlers, returning `None` where the old code fell through to live and returning the stamped dict where it returned. `search_messages` calls `folder_key`, `watermark`, `ctx.cache.search_messages(..., archived="any")`. On any non-`ToolError` exception: log a warning and return `None` (the old fallback contract).

- [ ] **Step 4: Rewire `mail_read.py` and `tasks.py`**

Each daemon handler becomes: validate → `if not fresh: hit = await cache_reads.X(...); if hit is not None: return hit` → live code unchanged. `mail_read` re-exports `_stamp`, `_row_card`, `_row_full`, `_row_body` from `cache_reads` (other tests import them). `_cache_folder_key`/`_cache_watermark` in `mail_read` become thin wrappers around `cache_reads.folder_key`/`watermark`.

- [ ] **Step 5: Run the full suite** — `cd v5 && .venv/bin/python -m ruff check . && .venv/bin/python -m pytest tests -q` → all pass, including `test_cache_first_reads.py` untouched.

- [ ] **Step 6: Commit** — `git commit -am "refactor(tools): cache reads extracted for reuse by the thin MCP"`.

### Task 6: The daemon: `ewsd` entrypoint and `/v1` API

The daemon is today's process without the `/mcp` transport. `http.build_app` gains two knobs: whether to mount MCP and which route prefix the tool REST lives under. Every gate (kill-switch, tier, recipient guard, confirm, rate cap, alias resolution, audit) keeps running inside `dispatch()` exactly as now.

**Files:**
- Modify: `v5/ewsmcp/http.py`
- Create: `v5/ewsmcp/daemon.py`
- Modify: `v5/pyproject.toml` (script `ewsd`)
- Test: `v5/tests/test_daemon_api.py`

**Interfaces:**
- Produces: `http.build_app(ctx, settings, streamable=None, *, mount_mcp=True, tools_prefix="/api/tools", api_key: Optional[str] = None)`. When `api_key` is `None` it falls back to `settings.mcp_api_key` (4.5 behaviour).
- Produces: routes on the daemon app: `GET /v1/tools`, `POST /v1/tools/<name>`, `GET /v1/status` (the `get_server_status` payload, no dispatch), `PUT|POST /upload/<token>`, `GET /livez|/readyz|/health|/version|/metrics|/openapi.json`.
- Produces: `ewsmcp.daemon.serve(settings)` (async) and `ewsmcp.daemon.main()` console entry `ewsd`; `daemon.build_daemon_app(ctx, settings)` for tests.

- [ ] **Step 1: Write `v5/tests/test_daemon_api.py`**

```python
"""ewsd HTTP API: bearer gate, tool listing, status, tool dispatch, no /mcp."""

import asyncio
import json

from conftest import make_context, make_settings

from ewsmcp.daemon import build_daemon_app


def _drive(app, path, method="GET", body=None, headers=()):
    scope = {"type": "http", "path": path, "method": method,
             "headers": [(k.encode(), v.encode()) for k, v in headers]}
    msgs = [{"type": "http.request", "body": json.dumps(body).encode() if body is not None
             else b"", "more_body": False}]
    sent = []

    async def receive():
        return msgs.pop(0)

    async def send(m):
        sent.append(m)

    asyncio.run(app(scope, receive, send))
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return status, json.loads(raw or b"{}")


AUTH = [("authorization", "Bearer k")]


def test_bearer_required_except_health(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive(app, "/livez")[0] == 200
    assert _drive(app, "/v1/tools")[0] == 401
    assert _drive(app, "/v1/tools", headers=AUTH)[0] == 200


def test_tools_listing_carries_public_schemas(db):
    ctx = make_context(db, ewsd_api_key="k", ews_capability_tier="full")
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/tools", headers=AUTH)
    names = {t["name"] for t in body["tools"]}
    assert "send_draft" in names and "search_messages" in names
    send = next(t for t in body["tools"] if t["name"] == "send_draft")
    assert "confirm_token" in send["inputSchema"]["properties"]


def test_status_answers_cold(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/status", headers=AUTH)
    assert status == 200 and body["ok"] and body["version"].startswith("5.0.")
    assert body["cache"]["ready"] is True


def test_tool_dispatch_runs_gate_chain(db):
    ctx = make_context(db, ewsd_api_key="k", ews_capability_tier="full",
                       send_enabled=False)
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/tools/send_draft", "POST", {"draft_id": "d1"},
                          headers=AUTH)
    assert status == 403 and body["error"]["code"] == "kill_switch"


def test_no_mcp_route_on_daemon(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive(app, "/mcp", "POST", {}, headers=AUTH)[0] == 404
```

- [ ] **Step 2: Run → fails** (`ewsmcp.daemon` missing).

- [ ] **Step 3: Generalise `http.build_app`**

Signature and gate:

```python
def build_app(ctx, settings, streamable: Optional[Any] = None, *,
              mount_mcp: bool = True, tools_prefix: str = "/api/tools",
              api_key: Optional[str] = None):
    key = (settings.mcp_api_key if api_key is None else api_key) or ""
```

Inside: use `key` in `_authorized`; replace the literal `/api/tools` and `/api/tools/` matches with `tools_prefix` and `tools_prefix + "/"`; the tools listing rows gain `"inputSchema": s.public_schema()["inputSchema"]`; when `mount_mcp` is False the `/mcp` branch returns 404 (fall through to the final 404). Add before the bearer gate check nothing; after it:

```python
        if path == "/v1/status" and method == "GET":
            from .tools.calendar_people import _get_server_status
            return await _send_json(send, 200, await _get_server_status(ctx))
```

(`_get_server_status` is a plain coroutine that never touches Exchange.) Move that import to module top — it is not exchangelib, the lazy rule does not apply, but keep the file consistent.

- [ ] **Step 4: Create `v5/ewsmcp/daemon.py`**

```python
"""ewsd — the Exchange daemon: sync engine, uploads, audit, and the /v1 API
the thin MCP calls. Runs once per mailbox. No MCP transport here."""

import asyncio
import logging
import sys

from .config import Settings, get_settings
from .http import build_app
from .server import build_context

logger = logging.getLogger(__name__)


def build_daemon_app(ctx, settings: Settings):
    return build_app(ctx, settings, None, mount_mcp=False, tools_prefix="/v1/tools",
                     api_key=settings.ewsd_api_key or "")


async def serve(settings: Settings) -> None:
    import uvicorn
    settings.require_exchange()
    if settings.ewsd_host not in ("127.0.0.1", "localhost", "::1") and not settings.ewsd_api_key:
        raise SystemExit("refusing to bind ewsd on a non-loopback address without EWSD_API_KEY")
    ctx = build_context(settings)
    app = build_daemon_app(ctx, settings)
    config = uvicorn.Config(app, host=settings.ewsd_host, port=settings.ewsd_port,
                            log_level=settings.log_level.lower(), http="h11")
    await uvicorn.Server(config).serve()


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO),
                        stream=sys.stderr,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        asyncio.run(serve(settings))
    except KeyboardInterrupt:
        print("ewsd shutting down", file=sys.stderr)


if __name__ == "__main__":
    main()
```

`pyproject.toml` `[project.scripts]`: add `ewsd = "ewsmcp.daemon:main"`.

- [ ] **Step 5: Run** `cd v5 && .venv/bin/python -m pytest tests/test_daemon_api.py tests/test_http_shim.py -q` → pass (the 4.5 shim tests still use the default prefix).

- [ ] **Step 6: Commit** — `git add -A v5 && git commit -m "feat(daemon): ewsd entrypoint with /v1 tool API, status and uploads"`.

### Task 7: `DaemonClient`

**Files:**
- Create: `v5/ewsmcp/mcp/__init__.py` (empty), `v5/ewsmcp/mcp/client.py`
- Test: `v5/tests/test_daemon_client.py`

**Interfaces:**
- Produces: `ewsmcp.mcp.client.DaemonClient(base_url: str, api_key: Optional[str], timeout: float = 90.0, transport=None)` with `async call_tool(name, arguments) -> dict` (envelope passed through verbatim, `ok` True or False), `async status() -> dict`, `async aclose()`. Network failures raise `ToolError("daemon_unavailable", …)`; malformed bodies raise `ToolError("upstream_error", …)`.

- [ ] **Step 1: Write `v5/tests/test_daemon_client.py`**

```python
"""DaemonClient against the real daemon app via httpx.ASGITransport."""

import asyncio

import httpx
import pytest
from conftest import make_context

from ewsmcp.daemon import build_daemon_app
from ewsmcp.errors import ToolError
from ewsmcp.mcp.client import DaemonClient


def _client(db, **overrides):
    ctx = make_context(db, ewsd_api_key="k", **overrides)
    app = build_daemon_app(ctx, ctx.settings)
    return DaemonClient("http://ewsd", "k", transport=httpx.ASGITransport(app=app)), ctx


def test_call_tool_passes_envelopes_through(db):
    client, ctx = _client(db, ews_capability_tier="full", send_enabled=False)
    out = asyncio.run(client.call_tool("send_draft", {"draft_id": "d1"}))
    assert out["ok"] is False and out["error"]["code"] == "kill_switch"
    st = asyncio.run(client.status())
    assert st["ok"] and st["tier"] == "full"


def test_wrong_key_is_auth_failed_envelope(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    client = DaemonClient("http://ewsd", "wrong", transport=httpx.ASGITransport(app=app))
    out = asyncio.run(client.call_tool("get_server_status", {}))
    assert out["ok"] is False and out["error"]["code"] == "auth_failed"


def test_unreachable_daemon_maps_to_daemon_unavailable():
    client = DaemonClient("http://127.0.0.1:9", "k", timeout=0.5)
    with pytest.raises(ToolError) as e:
        asyncio.run(client.call_tool("get_server_status", {}))
    assert e.value.code == "daemon_unavailable"
```

- [ ] **Step 2: Run → fails** (module missing).

- [ ] **Step 3: Create `v5/ewsmcp/mcp/client.py`**

```python
"""HTTP client for ewsd's /v1 API. The MCP forwards tool calls it cannot
answer from Postgres; envelopes come back verbatim so the model sees exactly
what the daemon's dispatcher produced (previews, confirm tokens, errors)."""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from ..errors import ToolError


class DaemonClient:
    def __init__(self, base_url: str, api_key: Optional[str], timeout: float = 90.0,
                 transport: Any = None):
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout,
                                         headers=headers, transport=transport)

    async def _request(self, method: str, path: str, **kw) -> Dict[str, Any]:
        try:
            resp = await self._client.request(method, path, **kw)
        except httpx.HTTPError as exc:
            raise ToolError(
                "daemon_unavailable",
                f"ewsd unreachable: {type(exc).__name__}: {exc}",
                hint="Check that ewsd is running and EWSD_URL / EWSD_API_KEY are set.",
                retry_after_s=15)
        try:
            data = resp.json()
        except ValueError:
            raise ToolError("upstream_error",
                            f"ewsd returned non-JSON (HTTP {resp.status_code})")
        if not isinstance(data, dict):
            raise ToolError("upstream_error", "ewsd returned a non-object body")
        return data

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        return await self._request("POST", f"/v1/tools/{name}", json=arguments or {})

    async def status(self) -> Dict[str, Any]:
        return await self._request("GET", "/v1/status")

    async def aclose(self) -> None:
        await self._client.aclose()
```

- [ ] **Step 4: Run** `cd v5 && .venv/bin/python -m pytest tests/test_daemon_client.py -q` → pass.

- [ ] **Step 5: Commit** — `git add -A v5 && git commit -m "feat(mcp): DaemonClient for the ewsd /v1 API"`.

### Task 8: The thin MCP: registry, dispatcher, local reads, stdio + HTTP

**Files:**
- Create: `v5/ewsmcp/mcp/local.py`, `v5/ewsmcp/mcp/registry.py`, `v5/ewsmcp/mcp/dispatch.py`, `v5/ewsmcp/mcp/server.py`, `v5/ewsmcp/mcp/http.py`
- Modify: `v5/ewsmcp/main.py`
- Test: `v5/tests/test_mcp_thin.py`

**Interfaces:**
- Produces: `mcp.registry.LOCAL_TOOLS: frozenset` = `{"list_folders","search_messages","get_message","get_thread","get_mailbox_overview","list_tasks","waiting_on","get_server_status"}`; `mcp.registry.build_mcp_registry(ctx) -> Dict[str, ToolSpec]` (tier-filtered like the daemon's, every spec has `requires_ews=False`, `confirm=False`, `preview=None`, `input_schema` = the daemon spec's **public** schema so `confirm_token` is accepted and forwarded).
- Produces: `mcp.local.<name>(ctx, **kwargs) -> dict` for each LOCAL tool: try `cache_reads`, else `await ctx.daemon.call_tool(name, kwargs)`.
- Produces: `mcp.dispatch.dispatch_mcp(ctx, spec, kwargs) -> dict`.
- Produces: `mcp.server.build_mcp_context(settings) -> Context` (db opened, `db.require_version(SCHEMA_VERSION)`, `IdAliaser(db)`, `CacheStore(db)`, `DaemonClient(settings.ewsd_url, settings.ewsd_api_key)`, gateway `None`, manager `None`, audit `_NullAudit`); `mcp.server.build_mcp_server(ctx) -> mcp.server.Server`; `mcp.server.run_stdio(settings)`.
- Produces: `mcp.http.serve_http(settings)` mounting `/mcp` plus `/livez`, `/readyz`, `/health`, `/version` behind `MCP_API_KEY`.

- [ ] **Step 1: Write `v5/tests/test_mcp_thin.py`**

```python
"""Thin MCP: Postgres-first reads, daemon fallback, verbatim proxying of writes.
The gateway is None throughout — proof that the MCP never touches Exchange."""

import asyncio
import time

import httpx
from conftest import make_context, make_settings

from ewsmcp.daemon import build_daemon_app
from ewsmcp.mcp.client import DaemonClient
from ewsmcp.mcp.dispatch import dispatch_mcp
from ewsmcp.mcp.registry import LOCAL_TOOLS, build_mcp_registry
from ewsmcp.tools.base import Context
from test_pg_store import make_row


class DeadDaemon:
    async def call_tool(self, name, arguments):
        from ewsmcp.errors import ToolError
        raise ToolError("daemon_unavailable", "down")

    async def status(self):
        from ewsmcp.errors import ToolError
        raise ToolError("daemon_unavailable", "down")


class RecordingDaemon:
    def __init__(self):
        self.calls = []

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        return {"ok": True, "proxied": name, "got": arguments}

    async def status(self):
        return {"ok": True, "version": "5.0.0a1", "connection": {"state": "warm"}}


def _mcp_ctx(db, daemon, **overrides):
    from ewsmcp.cache.store import CacheStore
    from ewsmcp.ids import IdAliaser
    from ewsmcp.server import _NullAudit
    ctx = Context(settings=make_settings(**overrides), gateway=None, manager=None,
                  aliaser=IdAliaser(db), audit=_NullAudit(), cache=CacheStore(db),
                  db=db, daemon=daemon)
    build_mcp_registry(ctx)
    return ctx


def _seed(ctx):
    now = int(time.time())
    ctx.cache.upsert_messages([
        make_row("RAW-1", subject="Budget review", body="please review", conv="C1",
                 is_read=0, date_ts=now - 300),
        make_row("RAW-2", subject="Re: Budget review", folder="sent", conv="C1",
                 sender_email="exec@corp.example", body="looks good", date_ts=now - 200),
    ])
    ctx.cache.set_sync_state("item:inbox", "T", now)
    ctx.cache.set_sync_state("item:sent", "T", now)
    ctx.cache.set_sync_state("events", None, now)


def _run(ctx, name, **kw):
    return asyncio.run(dispatch_mcp(ctx, ctx.registry[name], dict(kw)))


def test_registry_matches_daemon_counts(db):
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="full").registry) == 31
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="draft").registry) == 26
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="read").registry) == 15
    ctx = _mcp_ctx(db, DeadDaemon(), ews_capability_tier="full")
    assert LOCAL_TOOLS <= set(ctx.registry)
    assert "confirm_token" in ctx.registry["send_draft"].input_schema["properties"]
    assert all(not s.requires_ews and s.confirm is False for s in ctx.registry.values())


def test_local_reads_work_with_daemon_down(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    res = _run(ctx, "search_messages", query="budget")
    assert res["ok"] and res["source"] == "cache" and res["count"] == 1
    alias = res["items"][0]["id"]
    assert alias.startswith("m")
    msg = _run(ctx, "get_message", id=alias)  # alias resolved locally
    assert msg["ok"] and msg["message"]["subject"] == "Budget review"
    thread = _run(ctx, "get_thread", id=alias)
    assert thread["count"] == 2
    ov = _run(ctx, "get_mailbox_overview")
    assert ov["unread_total"] == 1
    st = _run(ctx, "get_server_status")
    assert st["ok"] and st["daemon"]["reachable"] is False


def test_fresh_and_misses_fall_through_to_daemon(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    _run(ctx, "get_message", id="RAW-1", fresh=True)
    _run(ctx, "search_messages", folder="f:junk")
    _run(ctx, "get_message", id="UNKNOWN-RAW")
    assert [c[0] for c in daemon.calls] == ["get_message", "search_messages", "get_message"]
    assert daemon.calls[0][1]["fresh"] is True


def test_daemon_down_on_miss_is_daemon_unavailable(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    res = _run(ctx, "search_messages", folder="f:junk")
    assert res["ok"] is False and res["error"]["code"] == "daemon_unavailable"


def test_writes_proxy_verbatim_including_confirm_token(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon, ews_capability_tier="full")
    res = _run(ctx, "send_draft", draft_id="d7", confirm_token="tok")
    assert res["proxied"] == "send_draft"
    assert daemon.calls[0][1] == {"draft_id": "d7", "confirm_token": "tok"}  # alias untouched


def test_end_to_end_through_real_daemon_app(db):
    """MCP → httpx → daemon app → dispatcher gate chain."""
    dctx = make_context(db, ewsd_api_key="k", ews_capability_tier="full", send_enabled=False)
    app = build_daemon_app(dctx, dctx.settings)
    client = DaemonClient("http://ewsd", "k", transport=httpx.ASGITransport(app=app))
    ctx = _mcp_ctx(db, client, ews_capability_tier="full")
    res = _run(ctx, "send_draft", draft_id="d1")
    assert res["ok"] is False and res["error"]["code"] == "kill_switch"


def test_db_down_is_backend_unavailable(db):
    """Mirror gone AND daemon gone → backend_unavailable (not a bare daemon error)."""
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    db.close()
    res = _run(ctx, "search_messages", query="budget")
    assert res["ok"] is False and res["error"]["code"] == "backend_unavailable"


def test_unknown_alias_is_validation_error_locally(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    res = _run(ctx, "get_message", id="m999")
    assert res["ok"] is False and res["error"]["code"] == "validation"
```

- [ ] **Step 2: Run → fails** (modules missing).

- [ ] **Step 3: Create `v5/ewsmcp/mcp/local.py`**

```python
"""Local handlers: answer from Postgres via tools.cache_reads, else forward to ewsd."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import psycopg

from .. import __version__
from ..errors import ToolError
from ..tools import cache_reads
from ..tools.base import Context

logger = logging.getLogger(__name__)


async def _forward(ctx: Context, name: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    return await ctx.daemon.call_tool(name, kwargs)


class _MirrorDown(Exception):
    """Postgres itself failed (pool closed, connection refused), not a bad query."""


async def _try(coro_factory) -> Optional[Dict[str, Any]]:
    """Run a cache read. None → the mirror cannot answer; _MirrorDown → the
    database is unreachable (the caller then forwards, and if ewsd is down too
    the model gets backend_unavailable rather than a misleading daemon error)."""
    try:
        return await coro_factory()
    except ToolError:
        raise
    except (psycopg.Error, RuntimeError) as exc:  # psycopg_pool.PoolClosed is a RuntimeError
        logger.warning("mirror unreachable (%s) — forwarding to ewsd", exc)
        raise _MirrorDown(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 — bad row/shape → daemon answers instead
        logger.warning("local read failed (%s) — forwarding to ewsd", exc)
        return None


async def _forward_after(ctx: Context, name: str, kwargs: Dict[str, Any],
                         mirror_error: Optional[str]) -> Dict[str, Any]:
    try:
        return await _forward(ctx, name, kwargs)
    except ToolError as err:
        if mirror_error and err.code == "daemon_unavailable":
            raise ToolError("backend_unavailable",
                            f"Postgres unreachable ({mirror_error}) and ewsd unreachable "
                            f"({err.message})", hint="Check DATABASE_URL and EWSD_URL.",
                            retry_after_s=15)
        raise


async def list_folders(ctx: Context, **kw) -> Dict[str, Any]:
    if not kw.get("fresh") and kw.get("parent") is None:
        hit = await _try(lambda: cache_reads.list_folders(
            ctx, int(kw.get("depth", 2)), bool(kw.get("include_empty", True))))
        if hit is not None:
            return hit
    return await _forward(ctx, "list_folders", kw)


async def search_messages(ctx: Context, **kw) -> Dict[str, Any]:
    if kw.get("mode", "keyword") == "semantic":
        raise ToolError("validation", "semantic search is not available in this build "
                        "(it returns with the archive tier).", hint="Use mode='keyword'.")
    if not kw.get("fresh"):
        sender = cache_reads.validate_search_args(
            kw.get("sender"), kw.get("from_"), kw.get("subject"), kw.get("since"),
            kw.get("until"), kw.get("is_unread"), kw.get("has_attachments"), kw.get("query"))
        hit = await _try(lambda: cache_reads.search_messages(
            ctx, folder=kw.get("folder", "f:inbox"), query=kw.get("query"), sender=sender,
            subject=kw.get("subject"), since=kw.get("since"), until=kw.get("until"),
            is_unread=kw.get("is_unread"), has_attachments=kw.get("has_attachments"),
            offset=int(kw.get("offset", 0)), limit=int(kw.get("limit", 20))))
        if hit is not None:
            return hit
    return await _forward(ctx, "search_messages", kw)


async def get_message(ctx: Context, **kw) -> Dict[str, Any]:
    if not kw.get("fresh") and not kw.get("include_html"):
        hit = await _try(lambda: cache_reads.get_message(ctx, kw["id"], kw.get("format", "full")))
        if hit is not None:
            return hit
    return await _forward(ctx, "get_message", kw)


async def get_thread(ctx: Context, **kw) -> Dict[str, Any]:
    if not kw.get("fresh"):
        hit = await _try(lambda: cache_reads.get_thread(
            ctx, kw["id"], int(kw.get("limit", 20)), int(kw.get("offset", 0))))
        if hit is not None:
            return hit
    return await _forward(ctx, "get_thread", kw)


async def get_mailbox_overview(ctx: Context, **kw) -> Dict[str, Any]:
    if not kw.get("fresh"):
        hit = await _try(lambda: cache_reads.overview(ctx, int(kw.get("horizon_days", 1))))
        if hit is not None:
            hit["connection"] = "via-ewsd"
            return hit
    return await _forward(ctx, "get_mailbox_overview", kw)


async def list_tasks(ctx: Context, **kw) -> Dict[str, Any]:
    if not kw.get("fresh"):
        hit = await _try(lambda: cache_reads.list_tasks(
            ctx, bool(kw.get("include_completed", False)), int(kw.get("offset", 0)),
            int(kw.get("limit", 25))))
        if hit is not None:
            return hit
    return await _forward(ctx, "list_tasks", kw)


async def waiting_on(ctx: Context, **kw) -> Dict[str, Any]:
    from ..tools.tasks import _waiting_on  # pure mirror query, no gateway
    return await _waiting_on(ctx, **kw)


async def get_server_status(ctx: Context, **kw) -> Dict[str, Any]:
    cache_block: Dict[str, Any] = {"ready": ctx.cache is not None}
    try:
        cache_block.update(ctx.cache.stats())
    except Exception as exc:  # noqa: BLE001
        cache_block["error"] = str(exc)
    try:
        daemon: Dict[str, Any] = {"reachable": True, **(await ctx.daemon.status())}
    except ToolError as err:
        daemon = {"reachable": False, "error": err.message}
    return {
        "ok": True, "version": __version__, "process": "ewsmcp",
        "uptime_s": int(time.time() - ctx.started_at),
        "tier": ctx.settings.ews_capability_tier,
        "tools": len(ctx.registry), "counters": dict(ctx.counters),
        "alias_stats": ctx.aliaser.stats(), "cache": cache_block, "daemon": daemon,
    }


Every handler above wraps its cache attempt so that a `_MirrorDown` is caught and
turned into a forward with the error remembered; write each as:

```python
    mirror_error = None
    if not kw.get("fresh"):
        try:
            hit = await _try(lambda: cache_reads.get_message(ctx, kw["id"], kw.get("format", "full")))
        except _MirrorDown as down:
            hit, mirror_error = None, str(down)
        if hit is not None:
            return hit
    return await _forward_after(ctx, "get_message", kw, mirror_error)
```

(The bodies shown earlier use the short `_forward` form for readability; the
implementation uses this shape for all six cache-backed handlers. `waiting_on`
raises `backend_unavailable` directly on `psycopg.Error`/`RuntimeError`.)

HANDLERS = {
    "list_folders": list_folders, "search_messages": search_messages,
    "get_message": get_message, "get_thread": get_thread,
    "get_mailbox_overview": get_mailbox_overview, "list_tasks": list_tasks,
    "waiting_on": waiting_on, "get_server_status": get_server_status,
}
```

(`from ..tools.tasks import _waiting_on` inside a function is allowed: the lazy-import rule is exchangelib-only. Move it to module top anyway for consistency; `tools.tasks` does not import exchangelib.)

- [ ] **Step 4: Create `v5/ewsmcp/mcp/registry.py`**

```python
"""The MCP's registry: the daemon packs' specs, re-homed. Local tools get the
Postgres-first handlers; everything else proxies to ewsd. Gates live in ewsd."""

from __future__ import annotations

import copy
from typing import Dict

from ..tools import calendar_people, mail_read, tasks, writes
from ..tools.base import CLASS_TIER, TIER_RANK, Context, ToolSpec
from . import local

LOCAL_TOOLS = frozenset(local.HANDLERS)


def _proxy(name: str):
    async def handler(ctx: Context, **kwargs):
        return await ctx.daemon.call_tool(name, kwargs)
    handler.__name__ = f"proxy_{name}"
    return handler


def build_mcp_registry(ctx: Context) -> Dict[str, ToolSpec]:
    tier = ctx.settings.ews_capability_tier
    registry: Dict[str, ToolSpec] = {}
    for spec in [*mail_read.TOOLS, *calendar_people.TOOLS, *tasks.TOOLS, *writes.TOOLS]:
        need = CLASS_TIER.get(spec.side_effect_class, "draft")
        if TIER_RANK[need] > TIER_RANK.get(tier, 2):
            continue
        schema = copy.deepcopy(spec.public_schema()["inputSchema"])
        registry[spec.name] = ToolSpec(
            name=spec.name, description=spec.description,
            side_effect_class=spec.side_effect_class, input_schema=schema,
            handler=local.HANDLERS.get(spec.name) or _proxy(spec.name),
            requires_ews=False, confirm=False, output_schema=spec.output_schema,
        )
    ctx.registry = registry
    return registry
```

- [ ] **Step 5: Create `v5/ewsmcp/mcp/dispatch.py`**

```python
"""MCP-side dispatcher: alias resolution for local reads, verbatim forwarding
for everything else. No kill-switch/tier/confirm here — ewsd owns those."""

from __future__ import annotations

import time
from typing import Any, Dict

from ..errors import ToolError, map_exception
from ..tools.base import Context, ToolSpec, resolve_ids
from .registry import LOCAL_TOOLS


async def dispatch_mcp(ctx: Context, spec: ToolSpec, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    outcome = "ok"
    try:
        if spec.name in LOCAL_TOOLS:
            kwargs = resolve_ids(ctx, kwargs)
        result = await spec.handler(ctx, **kwargs)
        if isinstance(result, dict):
            result.setdefault("ok", True)
            if result.get("ok") is False:
                outcome = result.get("error", {}).get("code", "error")
        return result
    except ToolError as err:
        outcome = err.code
        return err.to_dict()
    except (TypeError, ValueError) as exc:
        outcome = "validation"
        return ToolError("validation", f"{type(exc).__name__}: {exc}",
                         hint="Check the argument names and types against the tool schema."
                         ).to_dict()
    except Exception as exc:  # noqa: BLE001
        err = map_exception(exc)
        outcome = err.code
        return err.to_dict()
    finally:
        ctx.bump(f"tool.{spec.name}")
        if outcome != "ok":
            ctx.bump(f"err.{outcome}")
```

`resolve_ids` raises `ToolError("validation", …)` on an unknown alias (4.5 behaviour), which is what `test_unknown_alias_is_validation_error_locally` pins. Proxied tools skip resolution on purpose: the daemon resolves against the same `ews.aliases` table.

- [ ] **Step 6: Create `v5/ewsmcp/mcp/server.py` and `v5/ewsmcp/mcp/http.py`**

`server.py`:

```python
"""Thin MCP process wiring: Postgres + DaemonClient, MCP Server over stdio."""

import logging
from typing import Any, Dict, List

from mcp.server import Server
from mcp.types import Tool

from ..cache.store import CacheStore
from ..config import Settings
from ..db import SCHEMA_VERSION, Database
from ..ids import IdAliaser
from ..server import ANNOTATIONS, _NullAudit
from ..tools.base import Context
from .client import DaemonClient
from .dispatch import dispatch_mcp
from .registry import build_mcp_registry

logger = logging.getLogger(__name__)


def build_mcp_context(settings: Settings) -> Context:
    db = Database(settings.database_url)
    db.require_version(SCHEMA_VERSION)
    ctx = Context(settings=settings, gateway=None, manager=None, aliaser=IdAliaser(db),
                  audit=_NullAudit(), cache=CacheStore(db), db=db,
                  daemon=DaemonClient(settings.ewsd_url, settings.ewsd_api_key))
    build_mcp_registry(ctx)
    return ctx


def build_mcp_server(ctx: Context) -> Server:
    server = Server("ews-mcp")

    @server.list_tools()
    async def list_tools() -> List[Tool]:
        return [Tool(name=s.name, description=s.description, inputSchema=s.input_schema,
                     annotations=ANNOTATIONS.get(s.side_effect_class, ANNOTATIONS["write"]))
                for s in ctx.registry.values()]

    @server.call_tool()
    async def call_tool(name: str, arguments: Dict[str, Any]):
        spec = ctx.registry.get(name)
        if spec is None:
            return {"ok": False, "error": {"code": "validation",
                                           "message": f"Unknown tool: {name}",
                                           "hint": f"Available: {', '.join(sorted(ctx.registry))}"}}
        return await dispatch_mcp(ctx, spec, dict(arguments or {}))

    return server


async def run_stdio(settings: Settings) -> None:
    from mcp.server.stdio import stdio_server
    ctx = build_mcp_context(settings)
    server = build_mcp_server(ctx)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())
```

`http.py`:

```python
"""Thin MCP over Streamable HTTP: /mcp plus health. REST for scripts lives on ewsd."""

import logging
from typing import Any, Dict

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from .. import __version__
from ..http import _authorized, _send_json
from .server import build_mcp_context, build_mcp_server

logger = logging.getLogger(__name__)


def build_mcp_http_app(ctx, settings, streamable):
    api_key = settings.mcp_api_key or ""

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            return
        path, method = scope["path"], scope["method"]
        if path == "/livez" and method == "GET":
            return await _send_json(send, 200, {"status": "ok"})
        if path == "/version" and method == "GET":
            return await _send_json(send, 200, {"version": __version__})
        if path in ("/health", "/readyz") and method == "GET":
            status: Dict[str, Any] = {"status": "ok", "tools": len(ctx.registry)}
            code = 200
            try:
                ctx.db.schema_version()
            except Exception as exc:  # noqa: BLE001
                status.update(status="unavailable", database=str(exc))
                code = 503
            if path == "/readyz":
                try:
                    status["daemon"] = await ctx.daemon.status()
                except Exception as exc:  # noqa: BLE001
                    status["daemon"] = {"reachable": False, "error": str(exc)}
            return await _send_json(send, code, status)
        if api_key and not _authorized(scope.get("headers"), api_key):
            return await _send_json(send, 401, {"ok": False, "error": {
                "code": "auth_failed", "message": "missing or invalid bearer token"}})
        if path == "/mcp":
            return await streamable.handle_request(scope, receive, send)
        return await _send_json(send, 404, {"ok": False, "error": {
            "code": "validation", "message": "not found"}})

    return app


async def serve_http(settings) -> None:
    import uvicorn
    if settings.mcp_host not in ("127.0.0.1", "localhost", "::1") and not settings.mcp_api_key:
        raise SystemExit("refusing to bind on a non-loopback address without MCP_API_KEY")
    ctx = build_mcp_context(settings)
    mcp_server = build_mcp_server(ctx)
    streamable = StreamableHTTPSessionManager(app=mcp_server, json_response=False,
                                              stateless=True)
    app = build_mcp_http_app(ctx, settings, streamable)
    config = uvicorn.Config(app, host=settings.mcp_host, port=settings.mcp_port,
                            log_level=settings.log_level.lower(), http="h11")
    async with streamable.run():
        await uvicorn.Server(config).serve()
```

- [ ] **Step 7: Point `v5/ewsmcp/main.py` at the thin MCP**

```python
        if settings.mcp_transport == "http":
            from .mcp.http import serve_http
            asyncio.run(serve_http(settings))
        else:
            from .mcp.server import run_stdio
            asyncio.run(run_stdio(settings))
```

The 4.5 `ewsmcp.server.run_stdio` and `ewsmcp.http.serve_http` stay importable (the daemon and tests use `build_context`/`build_app`), but nothing launches the old combined mode any more.

- [ ] **Step 8: Run** `cd v5 && .venv/bin/python -m ruff check . && .venv/bin/python -m pytest tests -q` → all pass.

- [ ] **Step 9: Commit** — `git add -A v5 && git commit -m "feat(mcp): thin MCP — Postgres-first reads, ewsd proxy, stdio and streamable HTTP"`.

### Task 9: Image, dev compose, boot smoke

**Files:**
- Modify: `v5/Dockerfile`, `v5/scripts/boot_smoke.py`, `v5/.env.example`
- Create: `v5/docker-compose.yml`
- Delete: `v5/docker-compose.nas.yml` (superseded)

**Interfaces:**
- Produces: one image `stack/ews-mcp` with both console scripts; `CMD ["ewsmcp"]`; compose overrides `command: ewsd` for the daemon.

- [ ] **Step 1: `v5/Dockerfile`** — keep the multi-stage build. Change the build gate and default env:

```dockerfile
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    MCP_TRANSPORT=http \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8000 \
    EWSD_HOST=0.0.0.0 \
    EWSD_PORT=8790
RUN python -c "import ewsmcp.main, ewsmcp.daemon, ewsmcp.mcp.server"
```

Healthcheck stays on `/livez` port 8000 (the compose daemon service overrides it to 8790). `EXPOSE 8000 8790`.

- [ ] **Step 2: Create `v5/docker-compose.yml`** (self-contained dev stack)

```yaml
# ews-mcp 5.0 dev stack: Postgres (pgvector-ready for Phase 2) + ewsd + ewsmcp.
# Copy .env.example to .env first. Production lives in the operator's own stack.
services:
  postgres:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_USER: ews
      POSTGRES_PASSWORD: ${PGPASSWORD:?set PGPASSWORD in .env}
      POSTGRES_DB: ews
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ews -d ews"]
      interval: 5s
      timeout: 3s
      retries: 20

  ewsd:
    build: .
    image: ews-mcp:dev
    command: ["ewsd"]
    env_file: .env
    environment:
      DATABASE_URL: postgresql://ews:${PGPASSWORD}@postgres:5432/ews
      EWSD_HOST: 0.0.0.0
      EWSD_PORT: "8790"
      EWSD_API_KEY: ${EWSD_API_KEY:?set EWSD_API_KEY in .env}
      DATA_DIR: /data
    volumes:
      - ewsdata:/data
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8790/livez', timeout=3).status==200 else 1)"]
      interval: 30s
      timeout: 5s
      start_period: 20s
      retries: 3

  ewsmcp:
    image: ews-mcp:dev
    command: ["ewsmcp"]
    env_file: .env
    environment:
      DATABASE_URL: postgresql://ews:${PGPASSWORD}@postgres:5432/ews
      EWSD_URL: http://ewsd:8790
      EWSD_API_KEY: ${EWSD_API_KEY}
      MCP_TRANSPORT: http
      MCP_HOST: 0.0.0.0
      MCP_PORT: "8000"
      MCP_API_KEY: ${MCP_API_KEY:?set MCP_API_KEY in .env}
    ports:
      - "8000:8000"
    depends_on:
      ewsd:
        condition: service_started

volumes:
  pgdata:
  ewsdata:
```

- [ ] **Step 3: `v5/.env.example`** — add a Postgres block and the daemon block; remove `EWS_CACHE_ENABLED` and the semantic lines:

```
# --- Storage: Postgres (required by BOTH processes) --------------------------
DATABASE_URL=postgresql://ews:change-me@127.0.0.1:5432/ews
# --- Daemon (ewsd) -----------------------------------------------------------
#EWSD_HOST=127.0.0.1
#EWSD_PORT=8790
#EWSD_API_KEY=generate-a-long-random-string   # required off-loopback
# --- MCP → daemon ------------------------------------------------------------
#EWSD_URL=http://127.0.0.1:8790
```

- [ ] **Step 4: `v5/scripts/boot_smoke.py`** — boot **both** processes: start `ewsd` (env as before plus `DATABASE_URL` from `EWS_TEST_DATABASE_URL` or a docker Postgres started the same way `conftest.pg_dsn` does — factor that helper into `scripts/_pg.py` and import it from `conftest.py` too), wait for `/livez` on 8790; start `ewsmcp` in HTTP mode on 8124 with `EWSD_URL=http://127.0.0.1:8790`; assert: ewsd `/readyz` is 503 `connecting`; ewsmcp `/readyz` 200 with `daemon.reachable`-style info; `POST /v1/tools/get_server_status` on ewsd works cold; `POST /v1/tools/search_messages` on ewsd returns `upstream_unavailable` (no mirror, cold Exchange); `send_draft` returns `kill_switch`; ewsd `/openapi.json` lists `confirm_token` for `send_draft`; ewsmcp `/mcp` answers `initialize` (JSON-RPC over HTTP with `Accept: application/json, text/event-stream`) and `tools/list` returns 31 tools at tier full. Exit non-zero on any failure, tear both processes down.

- [ ] **Step 5: Verify**

```bash
cd v5 && docker build -t ews-mcp:dev . && .venv/bin/python scripts/boot_smoke.py full
```
Expected: `boot smoke OK` and exit 0.

- [ ] **Step 6: Commit** — `git add -A v5 && git rm -q v5/docker-compose.nas.yml && git commit -m "build: single image with ewsd+ewsmcp, dev compose, two-process boot smoke"`.

### Task 10: Documentation for the 5.0 line

**Files:**
- Modify: `v5/DESIGN.md`, `v5/README.md`, `v5/docs/API.md` (generated), `CHANGELOG.md`, `docs/README.md`, `README.md` (root version table)

- [ ] **Step 1: `v5/DESIGN.md`** — rewrite law #2 ("Indexing = SQLite + FTS5 in core") to "Storage = Postgres in core: one `ews` schema holds the mirror, aliases and (Phase 2) the archive; full-text via a generated tsvector, `simple` config, accent-folded in Python". Add a §Processes section: ewsd owns Exchange, sync, uploads, audit and every gate; ewsmcp reads Postgres and forwards to ewsd; why the gate chain lives in one process (process-local confirm/rate/audit state). Update §Cache → §Store (watermarks decide what is mirrored; `fresh` forwards). Remove the semantic paragraph; add "Phase 2: archive + Gemini embeddings" as a forward pointer to the spec.

- [ ] **Step 2: `v5/README.md`** — quick start becomes: (1) Postgres (`docker compose up postgres` or an existing one), (2) `ewsd` with Exchange creds + `DATABASE_URL`, (3) `ewsmcp` over stdio for Claude Code/Desktop with `DATABASE_URL` + `EWSD_URL` + `EWSD_API_KEY`, (4) HTTP mode. Config reference table: add the new keys, delete the removed ones. Note that `/upload/<token>` and `POST /v1/tools/*` are served by ewsd, `/mcp` by ewsmcp.

- [ ] **Step 3: Regenerate `v5/docs/API.md`** with `python scripts/dump_tool_table.py --write`; hand-edit its prose header where it says "SQLite" or "semantic tier".

- [ ] **Step 4: `CHANGELOG.md`** — add `[5.0.0a1] - <date>` under Unreleased: Postgres store, daemon/MCP split, Arabic normalisation removed, semantic tier removed pending Phase 2, alias ids re-minted (breaking), settings added/removed (list them), `docker-compose.nas.yml` removed.

- [ ] **Step 5: Root `README.md` and `docs/README.md`** — version table: `5.0 (pre-release) → v5/`, `4.5 → tag v4.5.0a1`.

- [ ] **Step 6: Run** `cd v5 && .venv/bin/python -m pytest tests/test_docs_match_registry.py -q` → pass. Commit: `git add -A && git commit -m "docs: 5.0 two-process architecture, Postgres store, config reference"`.

### Task 11: Deploy to the lab stack (outside this repo)

Stack repo: `/home/askar/stack`. Confirm with the operator before `docker compose up`; this replaces the running `ews-mcp` container.

- [ ] **Step 1: Database and role** (on the shared `postgres` container; password goes into `/home/askar/stack/.env` as `EWS_PG_PASSWORD`):

```bash
cd /home/askar/stack && source .env
docker exec -i postgres psql -U "$POSTGRES_USER" -v ON_ERROR_STOP=1 <<SQL
CREATE ROLE ews LOGIN PASSWORD '<EWS_PG_PASSWORD>';
CREATE DATABASE ews OWNER ews;
SQL
```

- [ ] **Step 2: `compose/personal.yml`** — replace the `ews-mcp` service with two:

```yaml
  ewsd:
    build:
      context: ../src/ews-mcp/v5
    image: stack/ews-mcp:${EWS_MCP_TAG:-v5-<sha>}
    container_name: ewsd
    command: ["ewsd"]
    restart: unless-stopped
    environment:
      EWS_SERVER_URL: ${EWS_SERVER_URL:?}
      EWS_EMAIL: ${EWS_EMAIL:?}
      EWS_USERNAME: ${EWS_USERNAME:?}
      EWS_PASSWORD: ${EWS_PASSWORD:?}
      DATABASE_URL: postgresql://ews:${EWS_PG_PASSWORD:?}@postgres:5432/ews
      EWSD_HOST: 0.0.0.0
      EWSD_PORT: "8790"
      EWSD_API_KEY: ${EWSD_API_KEY:?}
      EWS_CACHE_WINDOW_DAYS: ${EWS_CACHE_WINDOW_DAYS:-30}
      EWS_CAPABILITY_TIER: ${EWS_CAPABILITY_TIER:-full}
      SEND_ENABLED: ${EWS_SEND_ENABLED:-false}
      EWS_TZ: ${EWS_TZ:-Asia/Almaty}
      EXTERNAL_URL: ${EWS_MCP_AUTH_EXTERNAL_URL:?}
      SHARED_DIR: /shared
      DATA_DIR: /data
    volumes:
      - ews-mcp-data:/data
      - ./shared:/shared
    group_add: ["2000"]
    expose: ["8790"]
    networks: [proxy, backend]
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8790/livez', timeout=3).status==200 else 1)"]
      interval: 30s
      timeout: 5s
      start_period: 20s
      retries: 3

  ews-mcp:
    image: stack/ews-mcp:${EWS_MCP_TAG:-v5-<sha>}
    container_name: ews-mcp
    command: ["ewsmcp"]
    restart: unless-stopped
    environment:
      EWS_EMAIL: ${EWS_EMAIL:?}
      DATABASE_URL: postgresql://ews:${EWS_PG_PASSWORD:?}@postgres:5432/ews
      EWSD_URL: http://ewsd:8790
      EWSD_API_KEY: ${EWSD_API_KEY:?}
      MCP_TRANSPORT: http
      MCP_HOST: 0.0.0.0
      MCP_PORT: "8000"
      MCP_API_KEY: ${MCP_API_KEY:?}
      EWS_CAPABILITY_TIER: ${EWS_CAPABILITY_TIER:-full}
      EWS_TZ: ${EWS_TZ:-Asia/Almaty}
      DATA_DIR: /data
    expose: ["8000"]
    depends_on: [ewsd]
    networks: [proxy, backend]
```

`ews-mcp-auth` keeps forwarding to `http://ews-mcp:8000` (unchanged). Add `EWSD_API_KEY=<random>` and `EWS_PG_PASSWORD` to `.env` (encrypted copy via `bin/env-encrypt.sh`). Set `EWS_MCP_TAG=v5-$(git -C ../src/ews-mcp rev-parse --short HEAD)`.

- [ ] **Step 3: nginx** — the `/upload/*` route on `ews.lab.zhakenov.pro` currently targets `ews-mcp:8000`. Find it and repoint to `ewsd:8790`:

```bash
grep -rn "upload" /home/askar/stack/nginx/templates/
```

- [ ] **Step 4: Bring it up and verify**

```bash
cd /home/askar/stack && docker compose up -d --build ewsd ews-mcp && docker compose restart nginx
docker logs -f ewsd   # expect "applying migration 001_init.sql", warm-up, "cache sync engine started"
curl -fsS -H "Authorization: Bearer $EWSD_API_KEY" http://$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ewsd | cut -d' ' -f1):8790/v1/status | jq .cache
```

Then from claude.ai: `get_server_status` (shows `process: ewsmcp`, `daemon.reachable: true`), `get_mailbox_overview` (`source: cache` once `cache.rows.messages` is non-zero), `search_messages` with a Cyrillic prefix, `create_draft` + `send_draft` preview (proxied; `requires_confirmation: true`). Remove the old `stack_ews-mcp-data` SQLite files only after a week of clean operation.

- [ ] **Step 5: Record** — commit the stack change in `/home/askar/stack` (its own repo) and tag `ews-mcp` `v5.0.0a1`.

---

## Self-review

**Spec coverage (Phase 1 scope).** §1 process split → Tasks 6, 7, 8, 9, 11. §2 schema (`messages` with archive columns, `events`, `tasks`, `folders`, `sync_state`, `sender_sigs`, `aliases`, `meta`, migrations, no data migration) → Tasks 1–3; `attachments`/`chunks`/`archive_runs` deferred to Phase 2 by design. §2 FTS (`simple`, prefix, `ts_rank_cd`, Arabic removed) → Task 2 (+ deviation 5 on `unaccent`). §4 read tools on Postgres, `fresh` forwarded, write tools proxied, daemon routes `/v1/tools*`, `/v1/status`, `/upload` → Tasks 6, 8. §4 settings added/removed → Task 4. §5 failure modes: Postgres down → `backend_unavailable` (Task 8 `_MirrorDown`/`_forward_after` + `test_db_down_is_backend_unavailable`). Daemon down → Task 8 tests. `get_server_status` merge → Task 8. Tests against real Postgres → Task 1 fixture; contract tests kept → Task 4 Step 8; boot smoke → Task 9. Docs → Task 10. Rollout Phase 1 → Task 11.

**Placeholder scan.** `<sha>` in Task 11 is the operator's current commit, set by the command in Step 2; `<EWS_PG_PASSWORD>` is a secret the operator generates. No other TBDs.

**Type consistency.** `Database.conn()` used identically in `store.py`, `ids.py`, tests. `CacheStore(db)` / `IdAliaser(db)` constructors match `make_context`, `build_context`, `build_mcp_context`. `make_row` moves to `test_pg_store.py` and every importer is updated in Task 4 Step 8 rule 4. `Context.db`/`Context.daemon` added in Task 4 Step 3 before Task 8 uses them. `build_app(..., mount_mcp, tools_prefix, api_key)` keyword names match `build_daemon_app`. `resolve_ids` public name introduced in Task 4, consumed in Task 8. `cache_reads` function names in Task 5 match the calls in `mcp/local.py`. `DaemonClient.status()` used by `local.get_server_status` and `mcp/http.py`. Error codes `daemon_unavailable` / `backend_unavailable` added in Task 4 before use in Tasks 7–8.
