"""Per-mailbox Postgres mirror — the "fetch email very fast" core, shared by
the daemon (writer) and every MCP process (readers).

- Cleaned bodies are stored ONCE at sync time (``bodyclean`` output).
- Folders are identified by their EWS id (``messages.folder_id``);
  ``ews.folders.wk`` is the only place a well-known key like ``f:inbox``
  is resolved.
- Full-text search: ``messages.search_tsv`` is generated from subject,
  sender and body through ``ews.immutable_unaccent(lower(...))``. Queries
  tokenize in Python (``\\w+``) but fold AND re-sanitise in SQL, per token,
  via ``_TSQUERY`` — ``ews.immutable_unaccent`` can turn a single input
  character into tsquery metacharacters (e.g. a modifier apostrophe →
  ``'``), so the prefix expression is only safe to build after folding.
  Ranked by ``ts_rank_cd`` then date.
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

# %s is a text[] of raw (lowercased) \w+ tokens. Folding (immutable_unaccent)
# can turn a single input character into tsquery metacharacters — e.g. a
# modifier apostrophe folds to "'", a circled digit to "(1)" — so the folded
# text is never handed to to_tsquery directly (it would raise a syntax
# error). Instead each token is re-lexed with to_tsvector (which, unlike
# to_tsquery, never errors on odd input) to recover its real word-lexeme(s)
# post-folding; a token that splits into more than one lexeme (e.g. the
# circled digit example, "(1)budget" -> '1','budget') ORs its lexemes
# together — either could be "the word" the user meant — and distinct
# original tokens AND together, same as plain prefix search. Each per-token
# OR group is wrapped in parens: tsquery binds "&" tighter than "|", so an
# unparenthesised "'1':* | 'budget':* & 'review':*" parses as
# "1 | (budget & review)" and would match a document containing only "1" —
# ("1":* | "budget":*) & "review":* is what "and across tokens" actually
# requires. A token that folds to nothing (pure punctuation) drops out; if
# every token does, string_agg returns NULL and to_tsquery(NULL) is NULL
# (matches nothing, never raises).
_TSQUERY = """
(SELECT to_tsquery('simple', string_agg(grp, ' & '))
   FROM (SELECT '(' || string_agg(lex || ':*', ' | ') || ')' AS grp
           FROM unnest(%s::text[]) WITH ORDINALITY AS tok(word, ord)
           CROSS JOIN LATERAL unnest(tsvector_to_array(to_tsvector(
               'simple', ews.immutable_unaccent(lower(tok.word))))) AS lex
          GROUP BY tok.ord) s
  WHERE grp IS NOT NULL)
"""


def _tokens(query: str | None) -> list[str]:
    """Raw (lowercased) \\w+ tokens — no folding, no sanitising: that all
    happens in SQL, in ``_TSQUERY``, after unaccent (see its docstring)."""
    return [t for t in _TOKEN_RE.findall((query or "").lower()) if t]


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

    # tombstone_messages is set below, after apply_server_deletes is defined,
    # so a server-side delete becomes a state transition for archived rows
    # instead of a hard delete (delete_messages_by_id stays for callers that
    # genuinely want the row gone, e.g. a vanished folder).

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
        tokens = _tokens(text) if text else []
        if tokens:
            where.append(f"m.search_tsv @@ {_TSQUERY}")
            params.append(tokens)
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
        if tokens:
            order = f"ts_rank_cd(m.search_tsv, {_TSQUERY}) DESC, m.date_ts DESC"
            order_params = [tokens]
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

    # --------------------------------------------------------- archive state

    def folder_ids_for_wk(self, wk_keys: list[str]) -> list[str]:
        """Well-known keys (f:inbox, …) → EWS folder ids, via ews.folders."""
        if not wk_keys:
            return []
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT ews_id FROM ews.folders WHERE wk = ANY(%s)",
                (list(wk_keys),)).fetchall()
        return [r["ews_id"] for r in rows]

    _CANDIDATE_WHERE = """
        m.archive_state = 'live'
        AND m.date_ts IS NOT NULL AND m.date_ts <= %(before_ts)s
        AND (%(folder_ids)s::text[] IS NULL OR m.folder_id = ANY(%(folder_ids)s))
        AND NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(
                COALESCE(NULLIF(m.categories_json, ''), '[]')::jsonb) AS cat
            WHERE lower(btrim(cat)) = ANY(%(exclude_categories)s))
    """

    def _candidate_params(self, folder_ids, before_ts, exclude_categories):
        return {
            "before_ts": int(before_ts),
            # None means "every folder"; [] must mean "no folder" — do not
            # collapse an explicit empty list into None.
            "folder_ids": None if folder_ids is None else list(folder_ids),
            "exclude_categories": [c.strip().lower()
                                   for c in (exclude_categories or []) if c.strip()],
        }

    def archive_candidates(self, *, folder_ids: list[str] | None, before_ts: int,
                           exclude_categories: list[str],
                           limit: int) -> list[dict[str, Any]]:
        params = self._candidate_params(folder_ids, before_ts, exclude_categories)
        params["limit"] = int(limit)
        with self.db.conn() as c:
            return c.execute(
                "SELECT m.ews_id, m.changekey, m.folder_id, m.subject, m.date_iso, "
                "m.date_ts, m.internet_message_id, m.has_attachments "
                f"FROM ews.messages m WHERE {self._CANDIDATE_WHERE} "
                "ORDER BY m.date_ts ASC LIMIT %(limit)s", params).fetchall()

    def archive_candidate_count(self, *, folder_ids: list[str] | None,
                                before_ts: int,
                                exclude_categories: list[str]) -> int:
        params = self._candidate_params(folder_ids, before_ts, exclude_categories)
        with self.db.conn() as c:
            return int(c.execute(
                "SELECT COUNT(*) AS n FROM ews.messages m "
                f"WHERE {self._CANDIDATE_WHERE}", params).fetchone()["n"])

    def mark_captured(self, ews_id: str, *, mime_sha256: str,
                      mime_path: str) -> int:
        with self.db.conn() as c:
            cur = c.execute(
                "UPDATE ews.messages SET archive_state = 'captured', "
                "archived_at = now(), mime_sha256 = %s, mime_path = %s "
                "WHERE ews_id = %s AND archive_state = 'live'",
                (mime_sha256, mime_path, ews_id))
        return cur.rowcount

    def captured_rows(self, limit: int) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.messages WHERE archive_state = 'captured' "
                "ORDER BY archived_at ASC LIMIT %s", (int(limit),)).fetchall()

    def mark_verified(self, ews_id: str) -> int:
        with self.db.conn() as c:
            cur = c.execute(
                "UPDATE ews.messages SET archive_state = 'verified', "
                "verified_at = now() WHERE ews_id = %s AND archive_state = 'captured'",
                (ews_id,))
        return cur.rowcount

    def reset_to_live(self, ews_id: str) -> int:
        """Verification failed — forget the capture entirely so it is retried."""
        with self.db.conn() as c:
            cur = c.execute(
                "UPDATE ews.messages SET archive_state = 'live', archived_at = NULL, "
                "verified_at = NULL, mime_sha256 = NULL, mime_path = NULL "
                "WHERE ews_id = %s AND archive_state = 'captured'", (ews_id,))
            if cur.rowcount:
                c.execute("DELETE FROM ews.attachments WHERE message_ews_id = %s",
                          (ews_id,))
        return cur.rowcount

    def deletable_rows(self, *, before_ts: int, verified_before: int,
                       limit: int) -> list[dict[str, Any]]:
        """Rail 3: verified, older than the cutoff, and verified long enough ago."""
        with self.db.conn() as c:
            return c.execute(
                "SELECT ews_id, internet_message_id, mime_sha256, subject, date_iso "
                "FROM ews.messages WHERE archive_state = 'verified' "
                "AND date_ts IS NOT NULL AND date_ts <= %s "
                "AND verified_at IS NOT NULL AND verified_at <= to_timestamp(%s) "
                "ORDER BY date_ts ASC LIMIT %s",
                (int(before_ts), int(verified_before), int(limit))).fetchall()

    def mark_deleted(self, ews_ids: list[str]) -> int:
        if not ews_ids:
            return 0
        with self.db.conn() as c:
            cur = c.execute(
                "UPDATE ews.messages SET archive_state = 'deleted', "
                "deleted_at = now() WHERE ews_id = ANY(%s) "
                "AND archive_state = 'verified'", (list(ews_ids),))
        return cur.rowcount

    def apply_server_deletes(self, ews_ids: list[str]) -> tuple[int, int]:
        """A delete event arrived from Exchange (sync, or our own tool).

        Spec §3: an archived row (captured/verified) is KEPT and marked
        deleted — the mail is ours now; a live row is dropped as before; an
        already-deleted row is left alone. Returns (dropped, tombstoned)."""
        if not ews_ids:
            return 0, 0
        ids = list(ews_ids)
        with self.db.conn() as c:
            tombstoned = c.execute(
                "UPDATE ews.messages SET archive_state = 'deleted', "
                "deleted_at = now() WHERE ews_id = ANY(%s) "
                "AND archive_state IN ('captured', 'verified')", (ids,)).rowcount
            dropped = c.execute(
                "DELETE FROM ews.messages WHERE ews_id = ANY(%s) "
                "AND archive_state = 'live'", (ids,)).rowcount
        return dropped, tombstoned

    tombstone_messages = apply_server_deletes

    def archive_state_counts(self) -> dict[str, int]:
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT archive_state, COUNT(*) AS n FROM ews.messages "
                "GROUP BY archive_state").fetchall()
        counts = {"live": 0, "captured": 0, "verified": 0, "deleted": 0}
        counts.update({r["archive_state"]: int(r["n"]) for r in rows})
        return counts

    def archived_counts_by_folder(self) -> dict[str, int]:
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT folder_id, COUNT(*) AS n FROM ews.messages "
                "WHERE archive_state <> 'live' GROUP BY folder_id").fetchall()
        return {r["folder_id"]: int(r["n"]) for r in rows}

    def messages_by_ids(self, ews_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not ews_ids:
            return {}
        with self.db.conn() as c:
            rows = c.execute("SELECT * FROM ews.messages WHERE ews_id = ANY(%s)",
                             (list(ews_ids),)).fetchall()
        return {r["ews_id"]: r for r in rows}

    # ---------------------------------------------------------- attachments

    def replace_attachments(self, ews_id: str,
                            rows: list[dict[str, Any]]) -> None:
        """Idempotent: capture is re-runnable, so the inventory is rewritten."""
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.attachments WHERE message_ews_id = %s",
                      (ews_id,))
            if rows:
                c.cursor().executemany(
                    "INSERT INTO ews.attachments (message_ews_id, name, "
                    "content_type, size, sha256, is_inline) VALUES "
                    "(%(message_ews_id)s, %(name)s, %(content_type)s, %(size)s, "
                    "%(sha256)s, %(is_inline)s)",
                    [{"message_ews_id": ews_id, "name": r.get("name"),
                      "content_type": r.get("content_type"), "size": r.get("size"),
                      "sha256": r.get("sha256"),
                      "is_inline": int(r.get("is_inline") or 0)} for r in rows])

    def attachments_for(self, ews_id: str) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT id, name, content_type, size, sha256, is_inline "
                "FROM ews.attachments WHERE message_ews_id = %s ORDER BY id ASC",
                (ews_id,)).fetchall()

    # -------------------------------------------------------- archive_runs

    def start_run(self, kind: str, *, dry_run: bool, policy: dict[str, Any]) -> int:
        with self.db.conn() as c:
            row = c.execute(
                "INSERT INTO ews.archive_runs (kind, dry_run, policy_json) "
                "VALUES (%s, %s, %s) RETURNING id",
                (kind, 1 if dry_run else 0,
                 json.dumps(policy, sort_keys=True, default=str))).fetchone()
        return int(row["id"])

    def finish_run(self, run_id: int, *, captured: int = 0, verified: int = 0,
                   deleted: int = 0, failed: int = 0, error: str | None = None,
                   sample: list[Any] | None = None) -> None:
        with self.db.conn() as c:
            c.execute(
                "UPDATE ews.archive_runs SET finished_at = now(), captured = %s, "
                "verified = %s, deleted = %s, failed = %s, error = %s, "
                "sample_json = %s WHERE id = %s",
                (int(captured), int(verified), int(deleted), int(failed),
                 (error or None) and str(error)[:2000],
                 json.dumps(sample or [], ensure_ascii=False, default=str),
                 int(run_id)))

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self.db.conn() as c:
            return c.execute("SELECT * FROM ews.archive_runs WHERE id = %s",
                             (int(run_id),)).fetchone()

    def recent_runs(self, limit: int = 5) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.archive_runs ORDER BY started_at DESC, id DESC "
                "LIMIT %s", (int(limit),)).fetchall()
