"""v5 test fixtures: import path, a real Postgres, per-test schema isolation."""

import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from _pg import ThrowawayPostgres

from ewsmcp.confirm import reset_consumed_tokens
from ewsmcp.tools.base import reset_send_rate_window
from ewsmcp.tools.writes import reset_idempotency_store


@pytest.fixture(scope="session")
def pg_dsn():
    """A Postgres to test against: EWS_TEST_DATABASE_URL, else a throwaway
    docker container that is removed at session end."""
    if not os.environ.get("EWS_TEST_DATABASE_URL") and shutil.which("docker") is None:
        pytest.skip("no EWS_TEST_DATABASE_URL and no docker — Postgres tests skipped")
    with ThrowawayPostgres(name_suffix=f"test-{os.getpid()}") as dsn:
        os.environ["DATABASE_URL"] = dsn
        yield dsn


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


class FakeGateway:
    """The ONE gateway double.

    `raise_on_call=True` is the NoTouchGateway posture: any attempt to reach
    Exchange is an assertion failure, which is how the mirror-served read
    tests prove they never contact EWS. `folders` maps a folder ref onto the
    object `resolve_folder` should hand back.
    """

    def __init__(self, account: Any = None, *, raise_on_call: bool = False,
                 folders: dict[str, Any] | None = None):
        self.account = account
        self.calls = 0
        self.raise_on_call = raise_on_call
        self.folders = dict(folders or {})

    async def call(self, fn):
        if self.raise_on_call:
            raise AssertionError("EWS was contacted — the mirror path failed")
        self.calls += 1
        return fn(self.account)

    def resolve_folder(self, account, ref, aliaser):
        if self.raise_on_call:
            raise AssertionError("EWS folder resolution — the mirror path failed")
        if ref in self.folders:
            return self.folders[ref]
        return getattr(self.account, "inbox", None)


def make_row(ews_id, *, folder="inbox", subject="Budget review",
             sender_email="a@corp.example", sender_name="Ahmed",
             body="please review the numbers", date_ts=None, is_read=1,
             has_attachments=0, conv="CONV-1", imid=None, to=None):
    """One `CacheStore.upsert_messages` row."""
    from ewsmcp.cache.store import CacheStore
    return {
        "ews_id": ews_id,
        "changekey": "CK",
        "folder": folder,
        "conversation_id": conv,
        "sender_name": sender_name,
        "sender_email": sender_email,
        "to_json": json.dumps(to or []),
        "subject": subject,
        "date_ts": int(date_ts if date_ts is not None else time.time()),
        "date_iso": "2026-07-01T09:00+03:00",
        "is_read": is_read,
        "has_attachments": has_attachments,
        "importance": None,
        "categories_json": "[]",
        "body_clean": body,
        "internet_message_id": imid or f"<{ews_id}@corp.example>",
        "norm_text": CacheStore.norm_for_row(subject, sender_name,
                                             sender_email, body),
    }


def make_context(db, gateway=None, cache=True, audit_dir=None, **overrides):
    """A Context wired to the test database (aliases + mirror), registry built.

    `cache=False` leaves ctx.cache None. `audit_dir` swaps the null audit for
    a real hash-chained AuditLog rooted there.
    """
    from ewsmcp.audit import AuditLog, NullAudit
    from ewsmcp.cache.store import CacheStore
    from ewsmcp.ids import IdAliaser
    from ewsmcp.tools import build_registry
    from ewsmcp.tools.base import Context
    audit = AuditLog(audit_dir) if audit_dir else NullAudit()
    ctx = Context(settings=make_settings(**overrides), gateway=gateway, manager=None,
                  aliaser=IdAliaser(db), audit=audit,
                  cache=CacheStore(db) if cache else None, db=db)
    build_registry(ctx)
    return ctx
