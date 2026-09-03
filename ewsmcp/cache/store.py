"""Per-mailbox Postgres mirror — the "fetch email very fast" core, shared by
the daemon (writer) and every MCP process (readers).

- Cleaned bodies are stored ONCE at sync time (``bodyclean`` output).
- Folders are identified by their EWS id (``messages.folder_id``);
  ``ews.folders.wk`` is the only place a well-known key like ``f:inbox``
  is resolved.
- Full-text search: ``messages.search_tsv`` is generated from subject,
  sender and body through ``ews.immutable_unaccent(lower(...))``. Queries
  use the same wrapper, with the prefix expression (``tok:* & tok2:*``)
  built in Python by ``prefix_tsquery``. Ranked by ``ts_rank_cd`` then date.
- Timestamps are stored twice: epoch seconds (filter/sort) and the display
  ISO string in the server timezone.
- Archive columns (``archive_state``, ``mime_*`` …) are owned by the Phase 2
  archiver; ``upsert_messages`` never touches them.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

import psycopg

from ..db import Database

_ARCHIVED = {"any": "TRUE", "only": "m.archive_state <> 'live'",
             "exclude": "m.archive_state = 'live'"}
_TOKEN_RE = re.compile(r"\w+", re.UNICODE)

# Well-known folder ids resolved in SQL, so no Python-side lookup is needed
# and a missing folders row degrades to "matches nothing" rather than an error.
_INBOX_ID = "(SELECT ews_id FROM ews.folders WHERE wk = 'f:inbox' LIMIT 1)"
_SENT_ID = "(SELECT ews_id FROM ews.folders WHERE wk = 'f:sent' LIMIT 1)"
_TSQUERY = "to_tsquery('simple', ews.immutable_unaccent(lower(%s)))"


def prefix_tsquery(query: str) -> str:
    """Safe ``to_tsquery('simple', …)`` expression: every word becomes a
    prefix term, terms are ANDed. Accent folding happens in SQL, so the
    tokens are passed through as written (only lowercased there).
    Returns "" when nothing is searchable."""
    tokens = _TOKEN_RE.findall((query or "").lower())
    return " & ".join(f"{t}:*" for t in tokens if t)


_UPSERT_MESSAGE = """
INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id, sender_name,
    sender_email, to_json, subject, date_ts, date_iso, is_read, has_attachments,
    importance, categories_json, body_clean, internet_message_id)
VALUES (%(ews_id)s, %(changekey)s, %(folder_id)s, %(conversation_id)s, %(sender_name)s,
    %(sender_email)s, %(to_json)s, %(subject)s, %(date_ts)s, %(date_iso)s, %(is_read)s,
    %(has_attachments)s, %(importance)s, %(categories_json)s, %(body_clean)s,
    %(internet_message_id)s)
ON CONFLICT (ews_id) DO UPDATE SET
    changekey = EXCLUDED.changekey, folder_id = EXCLUDED.folder_id,
    conversation_id = EXCLUDED.conversation_id, sender_name = EXCLUDED.sender_name,
    sender_email = EXCLUDED.sender_email, to_json = EXCLUDED.to_json,
    subject = EXCLUDED.subject, date_ts = EXCLUDED.date_ts, date_iso = EXCLUDED.date_iso,
    is_read = EXCLUDED.is_read, has_attachments = EXCLUDED.has_attachments,
    importance = EXCLUDED.importance, categories_json = EXCLUDED.categories_json,
    body_clean = EXCLUDED.body_clean, internet_message_id = EXCLUDED.internet_message_id
"""


class CacheStore:
    """Owner of the mirror queries. One instance per process; the pool is the db's."""

    def __init__(self, db: Database):
        self.db = db

    # ------------------------------------------------------------- writers

    def upsert_messages(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.db.conn() as c:
            c.cursor().executemany(_UPSERT_MESSAGE, rows)
        return len(rows)

    def delete_messages_by_id(self, ews_ids: list[str]) -> int:
        if not ews_ids:
            return 0
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.messages WHERE ews_id = ANY(%s)", (list(ews_ids),))
        return len(ews_ids)

    tombstone_messages = delete_messages_by_id

    def set_read_flag(self, ews_ids: list[str], is_read: bool) -> None:
        if not ews_ids:
            return
        with self.db.conn() as c:
            c.execute("UPDATE ews.messages SET is_read = %s WHERE ews_id = ANY(%s)",
                      (1 if is_read else 0, list(ews_ids)))

    def apply_categories(self, ews_id: str, categories: list[str] | None) -> None:
        with self.db.conn() as c:
            c.execute("UPDATE ews.messages SET categories_json = %s WHERE ews_id = %s",
                      (json.dumps(categories or []), ews_id))

    def replace_events(self, rows: list[dict[str, Any]]) -> None:
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.events")
            c.cursor().executemany(
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

    def upsert_tasks(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.db.conn() as c:
            c.cursor().executemany(
                "INSERT INTO ews.tasks (ews_id, changekey, subject, due_ts, due_iso, "
                "is_complete, status) VALUES (%(ews_id)s, %(changekey)s, %(subject)s, "
                "%(due_ts)s, %(due_iso)s, %(is_complete)s, %(status)s) "
                "ON CONFLICT (ews_id) DO UPDATE SET changekey = EXCLUDED.changekey, "
                "subject = EXCLUDED.subject, due_ts = EXCLUDED.due_ts, "
                "due_iso = EXCLUDED.due_iso, is_complete = EXCLUDED.is_complete, "
                "status = EXCLUDED.status", rows)

    def delete_tasks_by_id(self, ews_ids: list[str]) -> None:
        if not ews_ids:
            return
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.tasks WHERE ews_id = ANY(%s)", (list(ews_ids),))

    def replace_folders(self, rows: list[dict[str, Any]]) -> None:
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.folders")
            c.cursor().executemany(
                "INSERT INTO ews.folders (ews_id, name, path, wk, total, unread, children) "
                "VALUES (%(ews_id)s, %(name)s, %(path)s, %(wk)s, %(total)s, %(unread)s, "
                "%(children)s) ON CONFLICT (ews_id) DO UPDATE SET name = EXCLUDED.name, "
                "path = EXCLUDED.path, wk = EXCLUDED.wk, total = EXCLUDED.total, "
                "unread = EXCLUDED.unread, children = EXCLUDED.children", rows)

    def get_sync_state(self, key: str) -> str | None:
        with self.db.conn() as c:
            row = c.execute("SELECT token FROM ews.sync_state WHERE key = %s",
                            (key,)).fetchone()
        return row["token"] if row else None

    def set_sync_state(self, key: str, token: str | None,
                       as_of_ts: float | None = None) -> None:
        with self.db.conn() as c:
            c.execute(
                "INSERT INTO ews.sync_state (key, token, as_of) VALUES (%s, %s, %s) "
                "ON CONFLICT (key) DO UPDATE SET token = EXCLUDED.token, "
                "as_of = EXCLUDED.as_of",
                (key, token, int(as_of_ts if as_of_ts is not None else time.time())))

    def folder_id_for_wk(self, wk: str) -> str | None:
        """The EWS id of a well-known folder ('f:inbox'), or None when the
        hierarchy lane has not recorded it yet."""
        with self.db.conn() as c:
            row = c.execute(
                "SELECT ews_id FROM ews.folders WHERE wk = %s LIMIT 1",
                (wk,)).fetchone()
        return row["ews_id"] if row else None

    def drop_sync_state(self, key: str) -> None:
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.sync_state WHERE key = %s", (key,))

    def delete_live_messages_in_folder(self, folder_id: str) -> int:
        """Forget a vanished folder's un-archived rows. Archived rows stay:
        they are the Phase 2 archive, not a mirror of a live folder."""
        with self.db.conn() as c:
            cur = c.execute(
                "DELETE FROM ews.messages WHERE folder_id = %s "
                "AND archive_state = 'live'", (folder_id,))
            return cur.rowcount

    # -------------------------------------------------------------- reads

    def watermark(self, key: str) -> int | None:
        try:
            with self.db.conn() as c:
                row = c.execute("SELECT as_of FROM ews.sync_state WHERE key = %s",
                                (key,)).fetchone()
            return int(row["as_of"]) if row and row["as_of"] is not None else None
        except psycopg.Error:
            return None

    def watermarks(self) -> dict[str, int]:
        try:
            with self.db.conn() as c:
                rows = c.execute("SELECT key, as_of FROM ews.sync_state").fetchall()
            return {r["key"]: int(r["as_of"]) for r in rows if r["as_of"] is not None}
        except psycopg.Error:
            return {}

    def stats(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        db_mb = 0.0
        try:
            with self.db.conn() as c:
                for table in ("messages", "events", "tasks", "folders"):
                    counts[table] = c.execute(
                        f"SELECT COUNT(*) AS n FROM ews.{table}").fetchone()["n"]
                size = c.execute(
                    "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0) AS b "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = 'ews' AND c.relkind = 'r'").fetchone()["b"]
                db_mb = round(int(size) / 1_048_576, 2)
        except psycopg.Error:
            pass
        return {"rows": counts, "db_mb": db_mb, "watermarks": self.watermarks()}

    def search_messages(
        self, *, folder_ids: list[str] | None = None, text: str | None = None,
        sender: str | None = None, subject: str | None = None,
        since_ts: int | None = None, until_ts: int | None = None,
        is_unread: bool | None = None, has_attachments: bool | None = None,
        archived: str = "any", offset: int = 0, limit: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """Full-text + structured search over the mirror. Every argument is
        optional and freely combinable; `folder_ids=None` searches every
        mirrored folder. Returns (page rows, exact total)."""
        where: list[str] = [_ARCHIVED.get(archived, "TRUE")]
        params: list[Any] = []
        q = prefix_tsquery(text) if text else ""
        if q:
            where.append(f"m.search_tsv @@ {_TSQUERY}")
            params.append(q)
        if folder_ids:
            where.append("m.folder_id = ANY(%s)")
            params.append(list(folder_ids))
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
            order = f"ts_rank_cd(m.search_tsv, {_TSQUERY}) DESC, m.date_ts DESC"
            order_params = [q]
        with self.db.conn() as c:
            total = c.execute(f"SELECT COUNT(*) AS n {base}", params).fetchone()["n"]
            rows = c.execute(
                f"SELECT m.* {base} ORDER BY {order} LIMIT %s OFFSET %s",
                [*params, *order_params, int(limit), int(offset)]).fetchall()
        return rows, int(total)

    def get_message(self, ews_id: str) -> dict[str, Any] | None:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.messages WHERE ews_id = %s OR internet_message_id = %s "
                "LIMIT 1", (ews_id, ews_id)).fetchone()

    def thread(self, conversation_id: str) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.messages WHERE conversation_id = %s ORDER BY date_ts ASC",
                (conversation_id,)).fetchall()

    def unread_page(self, limit: int = 10) -> tuple[int, list[dict[str, Any]]]:
        with self.db.conn() as c:
            total = c.execute(
                f"SELECT COUNT(*) AS n FROM ews.messages WHERE folder_id = {_INBOX_ID} "
                "AND is_read = 0 AND archive_state = 'live'").fetchone()["n"]
            rows = c.execute(
                f"SELECT * FROM ews.messages WHERE folder_id = {_INBOX_ID} "
                "AND is_read = 0 AND archive_state = 'live' "
                "ORDER BY date_ts DESC LIMIT %s", (int(limit),)).fetchall()
        return int(total), rows

    def events_window(self, start_ts: int, end_ts: int,
                      limit: int = 25) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.events WHERE start_ts < %s AND end_ts > %s "
                "ORDER BY start_ts ASC LIMIT %s",
                (int(end_ts), int(start_ts), int(limit))).fetchall()

    def folder_rows(self) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute("SELECT * FROM ews.folders ORDER BY path ASC").fetchall()

    def task_rows(self, include_completed: bool = False, offset: int = 0,
                  limit: int = 50) -> tuple[list[dict[str, Any]], int]:
        clause = "" if include_completed else " WHERE is_complete = 0"
        with self.db.conn() as c:
            total = c.execute(f"SELECT COUNT(*) AS n FROM ews.tasks{clause}").fetchone()["n"]
            rows = c.execute(
                f"SELECT * FROM ews.tasks{clause} ORDER BY COALESCE(due_ts, 1e15) ASC "
                "LIMIT %s OFFSET %s", (int(limit), int(offset))).fetchall()
        return rows, int(total)

    def contact_stats(self, email: str) -> dict[str, Any]:
        needle = (email or "").strip().lower()
        if not needle:
            return {}
        with self.db.conn() as c:
            received = c.execute(
                "SELECT COUNT(*) AS n, MIN(date_iso) AS first, MAX(date_iso) AS last "
                "FROM ews.messages WHERE lower(sender_email) = %s "
                f"AND folder_id IS DISTINCT FROM {_SENT_ID}", (needle,)).fetchone()
            sent = c.execute(
                f"SELECT COUNT(*) AS n, MAX(date_iso) AS last FROM ews.messages "
                f"WHERE folder_id = {_SENT_ID} AND lower(to_json) LIKE %s",
                (f"%{needle}%",)).fetchone()
        out: dict[str, Any] = {}
        if received and received["n"]:
            out.update({"received_count": received["n"], "first_seen": received["first"],
                        "last_received": received["last"]})
        if sent and sent["n"]:
            out.update({"sent_count": sent["n"], "last_sent": sent["last"]})
        return out

    def senders_matching(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        needle = f"%{(query or '').strip().lower()}%"
        with self.db.conn() as c:
            return c.execute(
                "SELECT lower(sender_email) AS sender_email, MAX(sender_name) AS sender_name, "
                "COUNT(*) AS msgs, MAX(date_iso) AS last_seen FROM ews.messages "
                f"WHERE folder_id IS DISTINCT FROM {_SENT_ID} AND "
                "(lower(sender_email) LIKE %s OR lower(sender_name) LIKE %s) "
                "GROUP BY lower(sender_email) ORDER BY msgs DESC LIMIT %s",
                (needle, needle, int(limit))).fetchall()

    def sent_without_reply(self, days: int = 5, limit: int = 25) -> list[dict[str, Any]]:
        cutoff = int(time.time() - days * 86400)
        with self.db.conn() as c:
            return c.execute(
                f"""
                SELECT s.* FROM ews.messages s
                WHERE s.folder_id = {_SENT_ID} AND s.date_ts <= %s
                  AND s.conversation_id IS NOT NULL
                  AND s.date_ts = (SELECT MAX(x.date_ts) FROM ews.messages x
                                   WHERE x.conversation_id = s.conversation_id
                                     AND x.folder_id = {_SENT_ID})
                  AND NOT EXISTS (SELECT 1 FROM ews.messages i
                                  WHERE i.conversation_id = s.conversation_id
                                    AND i.folder_id IS DISTINCT FROM {_SENT_ID}
                                    AND i.date_ts > s.date_ts)
                ORDER BY s.date_ts DESC LIMIT %s
                """, (cutoff, int(limit))).fetchall()
