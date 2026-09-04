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


INBOX_ID = "F-INBOX"
SENT_ID = "F-SENT"
JUNK_ID = "F-JUNK"
ARCHIVE_ID = "F-ARCHIVE"

_FOLDER_ROWS = [
    {"ews_id": INBOX_ID, "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
     "total": 0, "unread": 0, "children": 0},
    {"ews_id": SENT_ID, "name": "Sent Items", "path": "Sent Items", "wk": "f:sent",
     "total": 0, "unread": 0, "children": 0},
    {"ews_id": JUNK_ID, "name": "Junk Email", "path": "Junk Email", "wk": "f:junk",
     "total": 0, "unread": 0, "children": 0},
    # a custom, non-well-known folder: mirrored (not excluded) but never
    # synced — the "known but empty" case, distinct from f:junk which is
    # excluded from the mirror entirely by EWS_MIRROR_EXCLUDE.
    {"ews_id": ARCHIVE_ID, "name": "Archive 2024", "path": "Archive 2024", "wk": None,
     "total": 0, "unread": 0, "children": 0},
]


def seed_folders(store):
    """The hierarchy rows every folder-id lookup resolves against."""
    store.replace_folders([dict(r) for r in _FOLDER_ROWS])


def make_row(ews_id, *, folder_id=INBOX_ID, subject="Budget review",
             sender_email="a@corp.example", sender_name="Ahmed",
             body="please review the numbers", date_ts=None, is_read=1,
             has_attachments=0, conv="CONV-1", imid=None, to=None,
             categories=None):
    """One `CacheStore.upsert_messages` row."""
    return {
        "ews_id": ews_id,
        "changekey": "CK",
        "folder_id": folder_id,
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
        "categories_json": json.dumps(categories or []),
        "body_clean": body,
        "internet_message_id": imid or f"<{ews_id}@corp.example>",
    }


@pytest.fixture
def store_with_message(db):
    """A CacheStore holding exactly one live inbox message, plus its ews_id."""
    from ewsmcp.cache.store import CacheStore
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 1, "unread": 0, "children": 0},
    ])
    store.upsert_messages([make_row("RAW-1")])
    return store, "RAW-1"


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


class FakeEmbedder:
    """Deterministic, offline stand-in for GeminiEmbedder.

    Hashes each whitespace token into one of `dims` buckets, so texts that
    share vocabulary land near each other under cosine distance and the same
    text always yields the same vector. No network, ever.
    """

    def __init__(self, dims: int = 768):
        self.dims = dims
        self.calls: list[list[str]] = []

    def embed(self, texts):
        import hashlib
        import math
        self.calls.append(list(texts))
        out = []
        for text in texts:
            vec = [0.0] * self.dims
            for token in (text or "").lower().split():
                h = int(hashlib.sha256(token.encode()).hexdigest()[:8], 16)
                vec[h % self.dims] += 1.0
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            out.append([v / norm for v in vec])
        return out
