"""The shared fixtures every other test module builds on."""

import pytest
from conftest import FakeGateway, make_context, make_row


def test_fake_gateway_records_and_can_refuse(db):
    account = object()
    gw = FakeGateway(account)
    assert gw.calls == 0

    import asyncio
    assert asyncio.run(gw.call(lambda a: a)) is account
    assert gw.calls == 1

    dead = FakeGateway(raise_on_call=True)
    with pytest.raises(AssertionError):
        asyncio.run(dead.call(lambda a: a))
    with pytest.raises(AssertionError):
        dead.resolve_folder(None, "f:inbox", None)


def test_fake_gateway_resolves_registered_folders():
    sentinel = object()
    gw = FakeGateway(object(), folders={"f:inbox": sentinel})
    assert gw.resolve_folder(None, "f:inbox", None) is sentinel


def test_make_row_is_a_complete_upsert_row(db):
    from ewsmcp.cache.store import CacheStore
    store = CacheStore(db)
    assert store.upsert_messages([make_row("M1")]) == 1
    assert store.get_message("M1")["subject"] == "Budget review"


def test_make_context_accepts_an_audit_dir(db, tmp_path):
    ctx = make_context(db, audit_dir=str(tmp_path / "audit"))
    assert ctx.registry
    assert ctx.cache is not None
    ctx.audit.record("t", "read", "ok", 1)  # a real AuditLog, not the null one
    assert (tmp_path / "audit").exists()
