"""Arrival order for mail (platform spec §3.2).

`ews.messages` says when a mail was sent. The contract asks for the order the
store learned of it, and those differ every time a folder sync discovers old
mail — which is most syncs. This module owns that second order and nothing
else.
"""
from __future__ import annotations

import datetime as dt

_COLS = ("m.ews_id, m.changekey, m.folder_id, m.conversation_id, m.sender_name,"
         " m.sender_email, m.to_json, m.subject, m.date_ts, m.body_clean,"
         " m.has_attachments, m.attachments_json, m.internet_message_id, m.item_class")


def generation(conn) -> int:
    return conn.execute(
        "SELECT generation FROM ews.bridge_meta WHERE id=1").fetchone()["generation"]


def head(conn) -> int:
    return conn.execute(
        "SELECT coalesce(max(seq), 0) AS head FROM ews.bridge_arrival").fetchone()["head"]


def sweep(conn) -> int:
    """Give a sequence to everything that has none, and a new one to anything
    whose `changekey` moved. An edit is a new arrival: Mindet's cursor has
    already passed the old row, so re-emitting is the only way an amended mail
    is ever seen again."""
    cur = conn.execute(
        "INSERT INTO ews.bridge_arrival (ews_id, seq, changekey)"
        " SELECT m.ews_id, nextval('ews.bridge_arrival_seq'), m.changekey"
        "   FROM ews.messages m"
        "   LEFT JOIN ews.bridge_arrival a ON a.ews_id = m.ews_id"
        "  WHERE m.deleted_at IS NULL"
        "    AND (a.ews_id IS NULL OR a.changekey IS DISTINCT FROM m.changekey)"
        "  ORDER BY m.ews_id"
        " ON CONFLICT (ews_id) DO UPDATE"
        "   SET seq = excluded.seq, changekey = excluded.changekey, updated_at = now()")
    return cur.rowcount


def page(conn, *, after_seq: int | None, until: dt.datetime | None,
         limit: int) -> list[dict]:
    sql = ("SELECT " + _COLS + ", a.seq FROM ews.bridge_arrival a"
           " JOIN ews.messages m ON m.ews_id = a.ews_id"
           " WHERE m.deleted_at IS NULL")
    args: list = []
    if after_seq is not None:
        sql += " AND a.seq > %s"
        args.append(after_seq)
    if until is not None:
        # Bound by arrival, not send (spec §3.2): a mail sent in March and
        # discovered today arrived today, and a replay bounded by send time
        # would drop it from a window it genuinely belongs to.
        sql += " AND a.first_seen < %s"
        args.append(until)
    sql += " ORDER BY a.seq LIMIT %s"
    args.append(limit)
    return [dict(r) for r in conn.execute(sql, args)]
