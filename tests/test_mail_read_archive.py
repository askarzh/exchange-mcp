"""Archived mail through the ordinary read tools: filters, cards, folders,
attachments from the blob store, and the semantic mode."""

import asyncio
import time

from conftest import FakeEmbedder, FakeGateway, make_context, make_row

from ewsmcp.archive import files
from ewsmcp.semantic import SemanticIndex
from ewsmcp.tools.base import dispatch

NOW = int(time.time())


def _run(ctx, name, **kw):
    return asyncio.run(dispatch(ctx, ctx.registry[name], dict(kw)))


def _ctx(db, tmp_path, **over):
    over.setdefault("ews_capability_tier", "full")
    over.setdefault("data_dir", str(tmp_path / "data"))
    over.setdefault("gateway", FakeGateway(raise_on_call=True))
    ctx = make_context(db, **over)
    ctx.cache.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 3, "unread": 0, "children": 0},
        {"ews_id": "FID-SENT", "name": "Sent", "path": "Sent", "wk": "f:sent",
         "total": 1, "unread": 0, "children": 0}])
    ctx.cache.upsert_messages([
        make_row("LIVE-1", subject="Budget live", folder_id="FID-INBOX"),
        make_row("ARCH-1", subject="Budget archived", folder_id="FID-INBOX"),
        make_row("ARCH-2", subject="Budget sent", folder_id="FID-SENT"),
    ])
    ctx.cache.mark_captured("ARCH-1", mime_sha256="a" * 64, mime_path="/x.eml")
    ctx.cache.mark_captured("ARCH-2", mime_sha256="a" * 64, mime_path="/x.eml")
    ctx.cache.mark_verified("ARCH-2")
    ctx.cache.set_sync_state("item:FID-INBOX", "T", NOW)
    ctx.cache.set_sync_state("item:FID-SENT", "T", NOW)
    ctx.cache.set_sync_state("events", None, NOW)
    return ctx


def test_search_defaults_to_any_and_cards_carry_archive_state(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    res = _run(ctx, "search_messages", query="budget")
    assert res["count"] == 3
    states = {i["subject"]: i.get("archive_state") for i in res["items"]}
    assert states["Budget live"] is None  # live rows carry NO archive_state key
    assert states["Budget archived"] == "captured"
    assert states["Budget sent"] == "verified"


def test_search_archived_only_and_exclude(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    only = _run(ctx, "search_messages", query="budget", archived="only")
    assert {i["subject"] for i in only["items"]} == {"Budget archived",
                                                    "Budget sent"}
    excl = _run(ctx, "search_messages", query="budget", archived="exclude")
    assert {i["subject"] for i in excl["items"]} == {"Budget live"}


def test_search_rejects_an_unknown_archived_value(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    res = _run(ctx, "search_messages", query="budget", archived="sometimes")
    assert res["ok"] is False and res["error"]["code"] == "validation"


def test_list_folders_reports_archived_counts(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    rows = {r["name"]: r for r in _run(ctx, "list_folders")["items"]}
    assert rows["Inbox"]["archived"] == 1
    assert rows["Sent"]["archived"] == 1


def test_get_attachment_of_archived_mail_reads_the_blob_store(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    sha, _path = files.store_blob(ctx.settings.data_dir, b"col1,col2\n1,2\n")
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "data.csv", "content_type": "text/csv", "size": 14,
         "sha256": sha, "is_inline": 0}])
    res = _run(ctx, "get_attachment", message_id="ARCH-1")
    assert res["ok"] is True and res["source"] == "archive"
    assert res["mode"] == "text" and "col1,col2" in res["text"]
    assert ctx.gateway.calls == 0          # Exchange was never touched


def test_get_attachment_save_mode_writes_and_publishes(db, tmp_path):
    ctx = _ctx(db, tmp_path, shared_dir=str(tmp_path / "shared"))
    (tmp_path / "shared").mkdir()
    sha, _p = files.store_blob(ctx.settings.data_dir, b"%PDF-1.4")
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "q3.pdf", "content_type": "application/pdf", "size": 8,
         "sha256": sha, "is_inline": 0}])
    res = _run(ctx, "get_attachment", message_id="ARCH-1", mode="save")
    from pathlib import Path
    assert Path(res["saved_path"]).read_bytes() == b"%PDF-1.4"
    assert res["shared_name"] == "q3.pdf"


def test_get_attachment_picks_by_name_among_several(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    a, _ = files.store_blob(ctx.settings.data_dir, b"AAA")
    b, _ = files.store_blob(ctx.settings.data_dir, b"BBB")
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "a.txt", "content_type": "text/plain", "size": 3, "sha256": a,
         "is_inline": 0},
        {"name": "b.txt", "content_type": "text/plain", "size": 3, "sha256": b,
         "is_inline": 0}])
    assert _run(ctx, "get_attachment", message_id="ARCH-1",
                attachment="b.txt")["text"] == "BBB"
    assert _run(ctx, "get_attachment", message_id="ARCH-1",
                attachment="1")["text"] == "BBB"


def test_an_item_attachment_of_archived_mail_points_at_the_raw_mime(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "Fwd: contract", "content_type": "message/rfc822", "size": 900,
         "sha256": None, "is_inline": 0}])
    res = _run(ctx, "get_attachment", message_id="ARCH-1")
    assert res["mode"] == "info"
    assert "get_raw_message" in res["hint"]


def test_semantic_mode_runs_the_hybrid_and_is_not_degraded(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    ctx.semantic = SemanticIndex(ctx.cache, FakeEmbedder())
    ctx.semantic.index_messages(ctx.cache.unembedded_messages(100))
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is True and res["count"] == 3
    assert res.get("meta", {}).get("degraded") is not True


def test_semantic_mode_degrades_to_keyword_when_the_embedder_dies(db, tmp_path):
    ctx = _ctx(db, tmp_path)

    class Broken:
        def embed(self, texts):
            raise RuntimeError("gemini down")

    ctx.semantic = SemanticIndex(ctx.cache, Broken())
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is True and res["count"] == 3
    assert res["meta"]["degraded"] is True


def test_semantic_mode_without_a_key_degrades_rather_than_failing(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    ctx.semantic = None
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is True and res["meta"]["degraded"] is True
    assert "GEMINI_API_KEY" in res["meta"]["reason"]
