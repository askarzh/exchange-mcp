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

from ewsmcp.confirm import reset_consumed_tokens
from ewsmcp.tools.base import reset_send_rate_window
from ewsmcp.tools.writes import reset_idempotency_store

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
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


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
