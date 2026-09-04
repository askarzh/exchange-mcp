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
    msg_folder_root and then syncs the very folders it found.

    `folder_class` mirrors exchangelib's ``Folder.folder_class`` (the EWS
    ``folder:FolderClass``): "IPF.Note" is mail, anything else is not.
    """

    def __init__(self, name, folder_id, *, total=0, unread=0, children=(),
                 folder_class="IPF.Note"):
        self.name = name
        self.id = folder_id
        self.total_count = total
        self.unread_count = unread
        self.children = list(children)
        self.folder_class = folder_class
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
    account.calendar = FakeFolder("Calendar", "F-CAL",
                                  folder_class="IPF.Appointment")
    account.contacts = FakeFolder("Contacts", "F-CON",
                                  folder_class="IPF.Contact")
    account.tasks = FakeFolder("Tasks", "F-TASK", folder_class="IPF.Task")
    account.msg_folder_root = FakeFolder(
        "root", "F-ROOT", folder_class=None,
        children=[inbox, sent, junk, archive, account.drafts, account.trash,
                  account.outbox, account.calendar, account.contacts,
                  account.tasks, *extra])
    account.root = FakeRoot(account.msg_folder_root)
    return account


class FakeRoot:
    """exchangelib's Root: holds the cached subfolder tree and clears it."""

    def __init__(self, msg_folder_root):
        self._msg_folder_root = msg_folder_root
        self.clear_cache_calls = 0
        self.pending = []  # folders that only appear after a cache clear

    def clear_cache(self):
        self.clear_cache_calls += 1
        self._msg_folder_root.children.extend(self.pending)
        self.pending = []


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
    """When the hierarchy lane runs, a folder it discovers is item-synced in
    the SAME cycle — the walk is never one cycle behind the item lane."""
    account = _account()
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())
    new = FakeFolder("Project X", "F-NEW")
    new.queue([("create", _msg("M9", subject="New folder message"))], "TOK-NEW")
    account.root.pending.append(new)  # only visible once the cache is cleared
    engine._last_hierarchy_ts = 0.0   # the refresh interval has elapsed
    asyncio.run(engine._cycle())
    assert store.get_message("M9") is not None
    assert store.get_sync_state("item:F-NEW") == "TOK-NEW"


def test_hierarchy_refresh_clears_exchangelibs_folder_cache(db):
    """exchangelib caches the subfolder tree on Root for the life of the
    Account, so a new folder is invisible until the cache is cleared — and
    that clear is rate-limited to EWS_CACHE_HIERARCHY_SECONDS while the item
    lane keeps running every cycle."""
    account = _account()
    engine, store = _engine(db, account, ews_cache_hierarchy_seconds=600)
    asyncio.run(engine._cycle())
    assert account.root.clear_cache_calls == 1  # the first cycle always walks

    new = FakeFolder("Project X", "F-NEW")
    new.queue([("create", _msg("M9", subject="New folder message"))], "TOK-NEW")
    account.root.pending.append(new)
    account.inbox.queue([("create", _msg("M8", subject="Meanwhile"))], "TOK-IN2")

    asyncio.run(engine._cycle())  # inside the interval: no re-walk...
    assert account.root.clear_cache_calls == 1
    assert store.get_message("M9") is None
    assert store.get_message("M8") is not None  # ...but the item lane still ran

    engine._last_hierarchy_ts -= 601  # the interval elapses
    asyncio.run(engine._cycle())
    assert account.root.clear_cache_calls == 2
    assert store.get_message("M9") is not None
    assert {r["path"] for r in store.folder_rows()} >= {"Project X"}


def test_hierarchy_lane_stamps_its_own_watermark(db):
    """list_folders' provenance comes from the `folders` key, not the slow
    lane's `events` one."""
    account = _account()
    engine, store = _engine(db, account)
    assert store.watermark("folders") is None
    asyncio.run(engine._cycle())
    assert store.watermark("folders") is not None


def test_non_mail_folders_are_listed_but_never_item_synced(db):
    """A Contacts child such as `Recipient Cache` carries folder_class
    IPF.Contact: the mail ITEM_FIELDS projection would raise against it every
    cycle, so it is mirrored into ews.folders and nothing else."""
    account = _account()
    cache = FakeFolder("Recipient Cache", "F-RECIP", folder_class="IPF.Contact")
    cache.queue([("create", _msg("MX", subject="never"))], "TOK-RECIP")
    account.contacts.children.append(cache)
    quick = FakeFolder("Quick Step Settings", "F-QSS",
                       folder_class="IPF.Configuration")
    account.msg_folder_root.children.append(quick)
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())

    paths = {r["path"] for r in store.folder_rows()}
    assert "Contacts/Recipient Cache" in paths
    assert "Quick Step Settings" in paths
    assert store.get_sync_state("item:F-RECIP") is None
    assert store.get_sync_state("item:F-QSS") is None
    assert cache.seen_tokens == []      # sync_items was never called on it
    assert store.get_message("MX") is None
    assert set(engine._folders) == {"F-IN", "F-SENT", "F-ARCH"}


def test_a_classless_well_known_mail_folder_is_still_mirrored(db):
    """Some servers leave folder:FolderClass unset on the distinguished
    folders; exchangelib models those as Messages subclasses, so the
    well-known mail aliases stay mirrored."""
    account = _account()
    account.inbox.folder_class = None
    account.contacts.folder_class = None
    account.inbox.queue([("create", _msg("M1"))], "TOK-IN")
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())
    assert store.get_message("M1") is not None
    assert "F-CON" not in engine._folders


def test_large_first_sync_flushes_in_batches(db):
    """450 creates must not be buffered in one list: the store sees several
    upsert batches, and every row lands."""
    account = _account()
    account.inbox.queue(
        [("create", _msg(f"M{i}", subject=f"Msg {i}")) for i in range(450)],
        "TOK-BIG")
    engine, store = _engine(db, account)
    batches = []
    real_upsert = store.upsert_messages

    def spy(rows):
        batches.append(len(rows))
        return real_upsert(rows)

    store.upsert_messages = spy
    asyncio.run(engine._cycle())
    assert len(batches) >= 3
    assert max(batches) <= 200
    assert sum(batches) == 450
    with store.db.conn() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM ews.messages "
                      "WHERE folder_id = 'F-IN'").fetchone()["n"]
    assert n == 450
    assert store.get_sync_state("item:F-IN") == "TOK-BIG"


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
    engine._last_hierarchy_ts = 0.0  # the hierarchy refresh interval elapsed
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


# --- archive interaction (spec §3) -------------------------------------------


def test_a_server_delete_of_a_live_row_still_drops_it(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1"))], "TOK-1")
    asyncio.run(engine._cycle())
    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())
    assert store.get_message("M1") is None


def test_a_server_delete_of_a_captured_row_keeps_it_and_marks_deleted(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1", subject="Contract"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M1", mime_sha256="a" * 64, mime_path="/x.eml")

    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())

    row = store.get_message("M1")
    assert row is not None                    # the archive copy is ours now
    assert row["archive_state"] == "deleted"
    assert row["deleted_at"] is not None
    assert row["subject"] == "Contract"       # still searchable


def test_a_server_delete_of_a_verified_row_keeps_it(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("M1")
    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())
    assert store.get_message("M1")["archive_state"] == "deleted"


def test_our_own_archive_deletion_is_ignored_when_it_echoes_back(db):
    """The deleter already marked the row; the sync event must not disturb it."""
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("M1")
    store.mark_deleted(["M1"])
    before = store.get_message("M1")["deleted_at"]

    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())

    row = store.get_message("M1")
    assert row["archive_state"] == "deleted" and row["deleted_at"] == before


def test_status_reports_what_the_deletes_did(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1")), ("create", _msg("M2"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M2", mime_sha256="a" * 64, mime_path="/x.eml")
    account.inbox.queue([("delete", SimpleNamespace(id="M1")),
                         ("delete", SimpleNamespace(id="M2"))], "TOK-2")
    asyncio.run(engine._cycle())
    st = engine.status()
    assert st["dropped"] == 1 and st["tombstoned"] == 1
