"""SyncEngine: delta application, token persistence, degrade-not-die."""

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from conftest import FakeGateway, make_settings

from ewsmcp.cache.store import CacheStore
from ewsmcp.cache.sync import SyncEngine, row_from_message

TZ = ZoneInfo("Asia/Riyadh")
NOW = datetime(2026, 7, 10, 9, 0, tzinfo=TZ)


class FakeFolder:
    """Scripted sync_items + a hierarchy node in one object: the engine walks
    msg_folder_root and then syncs the very folders it found."""

    def __init__(self, name, folder_id, *, total=0, unread=0, children=()):
        self.name = name
        self.id = folder_id
        self.total_count = total
        self.unread_count = unread
        self.children = list(children)
        self.batches = []
        self.item_sync_state = None
        self.seen_tokens = []

    def queue(self, changes, new_token):
        self.batches.append((changes, new_token))

    def sync_items(self, sync_state=None, only_fields=None, **kw):
        self.seen_tokens.append(sync_state)
        if not self.batches:
            return
        changes, new_token = self.batches.pop(0)
        yield from changes
        self.item_sync_state = new_token


def _msg(raw_id, *, subject="Subj", body="Body text", dt=None, is_read=False,
         conv="CONV-1"):
    return SimpleNamespace(
        id=raw_id, changekey="CK", subject=subject,
        sender=SimpleNamespace(name="Ahmed", email_address="ahmed@corp.example"),
        datetime_received=dt or NOW,
        is_read=is_read, has_attachments=False, importance="Normal",
        categories=None, conversation_id=SimpleNamespace(id=conv),
        message_id=f"<{raw_id}@corp.example>",
        to_recipients=[SimpleNamespace(email_address="exec@corp.example")],
        text_body=body,
    )


def _account(extra=()):
    inbox = FakeFolder("Inbox", "F-IN", total=5, unread=2)
    sent = FakeFolder("Sent Items", "F-SENT")
    junk = FakeFolder("Junk Email", "F-JUNK")
    archive = FakeFolder("Archive 2024", "F-ARCH")
    account = SimpleNamespace()
    account.inbox, account.sent, account.junk = inbox, sent, junk
    account.drafts = FakeFolder("Drafts", "F-DRAFT")
    account.trash = FakeFolder("Deleted Items", "F-TRASH")
    account.outbox = FakeFolder("Outbox", "F-OUT")
    account.calendar = FakeFolder("Calendar", "F-CAL")
    account.contacts = FakeFolder("Contacts", "F-CON")
    account.tasks = FakeFolder("Tasks", "F-TASK")
    account.msg_folder_root = FakeFolder(
        "root", "F-ROOT",
        children=[inbox, sent, junk, archive, account.drafts, account.trash,
                  account.outbox, account.calendar, account.contacts,
                  account.tasks, *extra])
    return account


def _engine(db, account, **overrides):
    settings = make_settings(**overrides)
    store = CacheStore(db)
    return SyncEngine(settings, FakeGateway(account), store), store


def _row_in(folder_id, ews_id):
    from conftest import make_row
    return make_row(ews_id, folder_id=folder_id)


def test_cycle_applies_creates_updates_deletes_and_read_flags(db):
    account = _account()
    account.inbox.queue([
        ("create", _msg("M1", subject="First", is_read=False)),
        ("create", _msg("M2", subject="Second")),
    ], "TOK-1")
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())
    assert store.get_message("M1")["subject"] == "First"
    assert store.get_sync_state("item:F-IN") == "TOK-1"

    account.inbox.queue([
        ("update", _msg("M1", subject="First (edited)")),
        ("delete", SimpleNamespace(id="M2")),
        ("read_flag_change", (SimpleNamespace(id="M1"), True)),
    ], "TOK-2")
    asyncio.run(engine._cycle())
    row = store.get_message("M1")
    assert row["subject"] == "First (edited)"
    assert row["is_read"] == 1
    assert store.get_message("M2") is None
    assert store.get_sync_state("item:F-IN") == "TOK-2"
    # the second sync resumed FROM the first token
    assert account.inbox.seen_tokens[-1] == "TOK-1"


def test_every_mail_folder_is_mirrored_except_the_excluded_ones(db):
    account = _account()
    account.inbox.queue([("create", _msg("M1", subject="In inbox"))], "TOK-IN")
    account.msg_folder_root.children[3].queue(  # the custom Archive folder
        [("create", _msg("M2", subject="Archived"))], "TOK-ARCH")
    account.junk.queue([("create", _msg("M3", subject="Spam"))], "TOK-JUNK")
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())

    assert store.get_message("M1")["folder_id"] == "F-IN"
    assert store.get_message("M2")["folder_id"] == "F-ARCH"   # no folder list
    assert store.get_message("M3") is None                    # junk is excluded
    assert store.get_sync_state("item:F-IN") == "TOK-IN"
    assert store.get_sync_state("item:F-ARCH") == "TOK-ARCH"
    assert store.get_sync_state("item:F-JUNK") is None
    # the excluded and non-mail folders are still in the hierarchy for
    # list_folders, they are just never item-synced
    paths = {r["path"]: r for r in store.folder_rows()}
    assert "Junk Email" in paths and paths["Junk Email"]["wk"] == "f:junk"
    assert "Calendar" in paths


def test_hierarchy_runs_before_item_sync(db):
    """A folder that appears for the first time is item-synced in the SAME
    cycle — the hierarchy lane is not a slow lane any more."""
    account = _account()
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())
    new = FakeFolder("Project X", "F-NEW")
    new.queue([("create", _msg("M9", subject="New folder message"))], "TOK-NEW")
    account.msg_folder_root.children.append(new)
    asyncio.run(engine._cycle())
    assert store.get_message("M9") is not None
    assert store.get_sync_state("item:F-NEW") == "TOK-NEW"


def test_disappearing_folder_drops_its_token_and_live_rows(db):
    account = _account()
    archive = account.msg_folder_root.children[3]
    archive.queue([("create", _msg("M2", subject="Archived"))], "TOK-ARCH")
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())
    assert store.get_message("M2") is not None

    # keep one archived row: it must SURVIVE the folder going away
    store.upsert_messages([_row_in("F-ARCH", "M-KEPT")])
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET archive_state='verified' "
                  "WHERE ews_id='M-KEPT'")

    account.msg_folder_root.children.remove(archive)
    asyncio.run(engine._cycle())
    assert store.get_message("M2") is None            # live row deleted
    assert store.get_message("M-KEPT") is not None    # archived row stays
    assert store.get_sync_state("item:F-ARCH") is None


def test_no_time_window_ancient_messages_are_mirrored(db):
    """EWS_CACHE_WINDOW_DAYS is gone: the mirror is the whole mailbox."""
    account = _account()
    account.inbox.queue([
        ("create", _msg("OLD", dt=NOW - timedelta(days=4000))),
        ("create", _msg("NEW")),
    ], "TOK-1")
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())
    assert store.get_message("OLD") is not None
    assert store.get_message("NEW") is not None


def test_settings_no_longer_carry_the_removed_knobs():
    s = make_settings()
    assert s.ews_mirror_exclude == "drafts,junk,trash,outbox"
    assert not hasattr(s, "ews_cache_folders")
    assert not hasattr(s, "ews_cache_window_days")


def test_cycle_failure_degrades_not_dies(db):
    settings = make_settings()
    store = CacheStore(db)
    engine = SyncEngine(settings, FakeGateway(raise_on_call=True), store)

    async def one_iteration():
        try:
            await engine._cycle()
        except Exception as exc:  # noqa: BLE001 - mirrors SyncEngine._loop's catch-all
            engine.last_error = f"{type(exc).__name__}: {exc}"

    asyncio.run(one_iteration())
    assert "the mirror path failed" in engine.last_error
    assert engine.status()["last_error"]


def test_row_from_message_cleans_body_once(tmp_path):
    quoted = ("Latest reply only.\n\nFrom: Someone <s@corp.example>\n"
              "Sent: Monday\nTo: Exec\nSubject: Re: X\n\nOLD QUOTED TEXT")
    row = row_from_message(_msg("M1", body=quoted), "F-INBOX", "Asia/Riyadh")
    assert "OLD QUOTED" not in row["body_clean"]
    assert row["body_clean"].startswith("Latest reply only.")
    assert row["folder_id"] == "F-INBOX"
    assert row["internet_message_id"] == "<M1@corp.example>"


def test_slow_lane_syncs_calendar_and_tasks(db):
    account = _account()

    class CalendarStub:
        def __init__(self):
            self.calls = []

        def view(self, *, start, end, max_items=None):
            self.calls.append((start, end, max_items))
            return [SimpleNamespace(
                id="EV1", changekey=None, subject="Standup",
                start=NOW, end=NOW + timedelta(minutes=30),
                location=None, organizer=None, is_recurring=False,
                recurrence=None, my_response_type=None,
            )]

    account.calendar = CalendarStub()
    account.tasks.queue([("create", SimpleNamespace(
        id="T1", changekey=None, subject="File report",
        due_date=None, is_complete=False, status="NotStarted"))], "TT-1")

    engine, store = _engine(db, account)
    engine._sync_slow_lane(account)
    assert store.events_window(0, 2**40)
    _rows, total = store.task_rows()
    assert total == 1
    assert store.get_sync_state("item:tasks") == "TT-1"
