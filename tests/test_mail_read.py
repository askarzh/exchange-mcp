"""mail_read pack tests — all six read tools driven through dispatch().

No network, no exchangelib objects: a MagicMock account behind a fake
gateway, list-subclass query doubles, and SimpleNamespace messages with
tz-aware datetimes (Asia/Riyadh).
"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from conftest import FakeGateway, make_context

from ewsmcp.tools import mail_read
from ewsmcp.tools.base import dispatch

TZ = ZoneInfo("Asia/Riyadh")
SPECS = {spec.name: spec for spec in mail_read.TOOLS}

QUOTED_TAIL = (
    "\n\nFrom: Exec <exec@corp.example>\nSent: Monday, June 8, 2026\n"
    "To: Ahmed\nSubject: Re: RFP\n\nOLD QUOTED LINE ONE\nOLD QUOTED LINE TWO"
)


# --- doubles ----------------------------------------------------------------


class _Query(list):
    """List-like stand-in for a Folder/QuerySet: chainable, records calls."""

    def __init__(self, items=()):
        super().__init__(items)
        self.filter_calls = []
        self.only_calls = []
        self.order_calls = []
        self.count_calls = 0
        self.refresh_calls = 0
        self.total_count = len(items)

    def filter(self, *args, **kwargs):
        self.filter_calls.append((args, kwargs))
        return self

    def only(self, *args):
        self.only_calls.append(args)
        return self

    def order_by(self, *args):
        self.order_calls.append(args)
        return self

    def count(self):
        # The perf contract says this must NEVER run (full-folder scan).
        self.count_calls += 1
        return len(self)

    def refresh(self):
        self.refresh_calls += 1


def _msg(raw_id, subject="Subj", sender="ahmed@corp.example", *, sender_name=None,
         dt=None, is_read=True, has_attachments=False, text_body="",
         conv="CONV-RAW-1=", message_id=None, attachments=None, to=None):
    return SimpleNamespace(
        id=raw_id,
        subject=subject,
        sender=SimpleNamespace(name=sender_name or sender, email_address=sender),
        datetime_received=dt or datetime(2026, 6, 10, 9, 0, tzinfo=TZ),
        is_read=is_read,
        has_attachments=has_attachments,
        text_body=text_body,
        message_id=message_id or f"<{raw_id}@corp.example>",
        conversation_id=SimpleNamespace(id=conv),
        to_recipients=to or [],
        cc_recipients=[],
        attachments=list(attachments or []),
        importance="Normal",
        body=None,
    )


def _account(inbox=None, sent=None):
    account = MagicMock(name="account")
    account.inbox = inbox if inbox is not None else _Query()
    account.sent = sent if sent is not None else _Query()
    return account


def _ctx(tmp_path, db, account, **overrides):
    overrides.setdefault("data_dir", str(tmp_path / "data"))
    return make_context(db, gateway=FakeGateway(account), cache=False,
                        audit_dir=str(tmp_path / "audit"), **overrides)


def _run(ctx, name, **kwargs):
    return asyncio.run(dispatch(ctx, SPECS[name], dict(kwargs)))


# --- pack shape ---------------------------------------------------------------


def test_pack_exports_six_read_specs():
    assert len(mail_read.TOOLS) == 6
    assert set(SPECS) == {
        "list_folders", "search_messages", "get_message",
        "get_thread", "get_attachment", "get_mailbox_overview",
    }
    for spec in mail_read.TOOLS:
        assert spec.side_effect_class == "read"
        assert spec.requires_ews is True
        assert spec.input_schema["additionalProperties"] is False


# --- search_messages -----------------------------------------------------------


def test_search_messages_no_longer_has_a_live_path(tmp_path, db):
    """The handler never calls the gateway: with no mirror it errors, it
    does not fall through to Exchange."""
    import inspect

    from ewsmcp.tools import mail_read
    source = inspect.getsource(mail_read._search_messages)
    assert "ctx.gateway" not in source
    assert "paginate" not in source


def test_search_messages_without_a_mirror_is_backend_unavailable(tmp_path, db):
    """This pack's `_ctx` always builds with cache=False — search_messages is
    store-only now, so with no mirror it errors rather than falling through
    to Exchange."""
    account = _account()
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "search_messages")
    assert res["ok"] is False
    assert res["error"]["code"] == "backend_unavailable"
    account.fetch.assert_not_called()


def test_search_sender_and_deprecated_alias_conflict_is_validation(tmp_path, db):
    ctx = _ctx(tmp_path, db, _account())
    res = _run(ctx, "search_messages", sender="ahmed", from_="ahmed")
    assert res["ok"] is False
    assert res["error"]["code"] == "validation"


# --- get_message -----------------------------------------------------------------


def test_get_message_full_shape_and_alias_roundtrip(tmp_path, db):
    item = _msg("RAW-1=", subject="RFP timeline",
                text_body="Body line.\n\nRegards", has_attachments=True,
                attachments=[SimpleNamespace(name="contract.pdf", size=9,
                                             content_type="application/pdf")])
    inbox = _Query([item])
    account = _account(inbox=inbox)
    ctx = _ctx(tmp_path, db, account)
    assert ctx.aliaser.alias_for("RAW-1=", "m") == "m1"  # mints m1

    account.fetch = MagicMock(return_value=[item])
    res = _run(ctx, "get_message", id="m1")  # dispatcher resolves m1 -> RAW-1=
    assert res["ok"] is True
    assert account.fetch.call_args.kwargs["ids"] == [("RAW-1=", None)]
    message = res["message"]
    assert message["id"] == "m1"  # same alias back out
    assert message["internet_message_id"] == "<RAW-1=@corp.example>"
    assert message["body"].startswith("Body line.")
    assert message["to"] == []
    assert message["attachments"][0]["name"] == "contract.pdf"
    assert "snippet" not in message


def test_get_message_concise_returns_card(tmp_path, db):
    item = _msg("RAW-1=", text_body="Hello.")
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_message", id="RAW-1=", format="concise")
    assert res["ok"] is True
    assert res["message"]["snippet"] == "Hello."
    assert "body" not in res["message"]


def test_get_message_stale_id_maps_to_not_found(tmp_path, db):
    class ErrorItemNotFound(Exception):
        pass

    account = _account()
    account.fetch = MagicMock(return_value=[ErrorItemNotFound("gone")])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_message", id="RAW-STALE=")
    assert res["ok"] is False
    assert res["error"]["code"] == "not_found"
    assert "search" in res["error"]["hint"].lower()


# --- get_thread --------------------------------------------------------------------


def test_get_thread_has_no_live_fallback(tmp_path, db):
    """Every mail folder is mirrored now, so a miss on the mirror means the
    seed is in an excluded folder or not synced yet — not_found, and the
    gateway is never touched to try to rebuild the thread live."""
    account = _account()
    ctx = _ctx(tmp_path, db, account)  # cache=False: always a mirror miss
    res = _run(ctx, "get_thread", id="RAW-A=")
    assert res["ok"] is False
    assert res["error"]["code"] == "not_found"
    account.fetch.assert_not_called()


# --- get_attachment --------------------------------------------------------------


def _att(name="notes.txt", content=b"hello world", content_type="text/plain"):
    return SimpleNamespace(name=name, size=len(content),
                           content_type=content_type, content=content)


def test_get_attachment_text_mode(tmp_path, db):
    item = _msg("RAW-A=", attachments=[_att()])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_attachment", message_id="RAW-A=", mode="text")
    assert res["ok"] is True
    assert res["mode"] == "text"
    assert res["text"] == "hello world"
    assert res["name"] == "notes.txt"
    assert res["size_bytes"] == 11
    assert "truncated" not in res


def test_get_attachment_multiple_without_selector_is_validation(tmp_path, db):
    item = _msg("RAW-A=", attachments=[_att("a.txt"), _att("b.csv")])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_attachment", message_id="RAW-A=")
    assert res["ok"] is False
    assert res["error"]["code"] == "validation"
    assert "a.txt" in res["error"]["message"] and "b.csv" in res["error"]["message"]


def test_get_attachment_index_selector_string(tmp_path, db):
    item = _msg("RAW-A=", attachments=[_att("a.txt", b"first"),
                                       _att("b.txt", b"second")])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_attachment", message_id="RAW-A=", attachment="1",
               mode="text")
    assert res["ok"] is True
    assert res["text"] == "second"


def test_get_attachment_auto_on_binary_returns_info_with_hint(tmp_path, db):
    item = _msg("RAW-A=", attachments=[
        _att("contract.pdf", b"%PDF-junk", "application/pdf"),
    ])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_attachment", message_id="RAW-A=")
    assert res["ok"] is True
    assert res["mode"] == "info"
    assert "text" not in res
    assert "save" in res["hint"]


def test_get_attachment_save_writes_sanitized_file(tmp_path, db):
    item = _msg("RAW-A=", attachments=[
        _att("quarterly report?.csv", b"a,b\n1,2\n", "text/csv"),
    ])
    account = _account()
    account.fetch = MagicMock(return_value=[item])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_attachment", message_id="RAW-A=", mode="save")
    assert res["ok"] is True
    saved = res["saved_path"]
    assert str(tmp_path / "data") in saved
    assert "?" not in saved and " " not in saved.rsplit("attachments", 1)[1]
    with open(saved, "rb") as fh:
        assert fh.read() == b"a,b\n1,2\n"


# --- get_mailbox_overview ----------------------------------------------------------


def test_overview_shape(tmp_path, db):
    inbox = _Query([
        _msg("RAW-1=", is_read=False, text_body="Urgent one."),
        _msg("RAW-2=", is_read=False, text_body="Urgent two."),
    ])
    inbox.unread_count = 2
    account = _account(inbox=inbox)
    event = SimpleNamespace(id="EV-RAW-1=", subject="Standup",
                            start=datetime(2026, 6, 13, 9, 0, tzinfo=TZ),
                            end=datetime(2026, 6, 13, 9, 30, tzinfo=TZ))
    account.calendar.view.return_value = [event]
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "get_mailbox_overview")
    assert res["ok"] is True
    assert res["unread_total"] == 2
    assert res["connection"] == "unmanaged"
    assert "generated_at" in res
    assert [card["unread"] for card in res["recent_unread"]] == [True, True]
    assert res["recent_unread"][0]["id"].startswith("m")
    assert res["today_events"][0]["id"].startswith("e")
    assert res["today_events"][0]["subject"] == "Standup"
    view_kwargs = account.calendar.view.call_args.kwargs
    assert view_kwargs["start"].hour == 0 and view_kwargs["start"].minute == 0
    assert (view_kwargs["end"] - view_kwargs["start"]).days == 1
    assert inbox.filter_calls[0][1] == {"is_read": False}


# --- list_folders --------------------------------------------------------------------


def _folder(raw_id, name, total=1, unread=0, children=()):
    return SimpleNamespace(id=raw_id, name=name, total_count=total,
                           unread_count=unread, children=list(children))


def test_list_folders_walk_with_paths_and_wk(tmp_path, db):
    sub = _folder("FLD-SUB=", "2026", total=4)
    inbox_folder = _folder("FLD-INBOX=", "Inbox", total=5, unread=2,
                           children=[sub])
    empty = _folder("FLD-EMPTY=", "Archive Old", total=0)
    account = _account()
    account.msg_folder_root = _folder("FLD-ROOT=", "root",
                                      children=[inbox_folder, empty])
    account.inbox = inbox_folder  # lets the wk map identify f:inbox
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "list_folders")
    assert res["ok"] is True
    by_path = {row["path"]: row for row in res["items"]}
    assert set(by_path) == {"Inbox", "Inbox/2026", "Archive Old"}
    assert by_path["Inbox"]["wk"] == "f:inbox"
    assert by_path["Inbox"]["unread"] == 2
    assert by_path["Inbox"]["children"] == 1
    assert by_path["Inbox"]["id"].startswith("f")
    assert "wk" not in by_path["Inbox/2026"]
    # alias roundtrip: the f-alias resolves back to the raw folder id
    assert ctx.aliaser.resolve(by_path["Inbox"]["id"]) == "FLD-INBOX="


def test_list_folders_depth_and_include_empty(tmp_path, db):
    sub = _folder("FLD-SUB=", "2026", total=4)
    inbox_folder = _folder("FLD-INBOX=", "Inbox", total=5, children=[sub])
    empty = _folder("FLD-EMPTY=", "Archive Old", total=0)
    account = _account()
    account.msg_folder_root = _folder("FLD-ROOT=", "root",
                                      children=[inbox_folder, empty])
    ctx = _ctx(tmp_path, db, account)
    res = _run(ctx, "list_folders", depth=1, include_empty=False)
    assert [row["name"] for row in res["items"]] == ["Inbox"]
