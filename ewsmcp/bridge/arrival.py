"""Arrival order for mail (platform spec §3.2).

`ews.messages` says when a mail was sent. The contract asks for the order the
store learned of it, and those differ every time a folder sync discovers old
mail — which is most syncs. This module owns that second order and nothing
else.
"""
from __future__ import annotations

import datetime as dt

# Only what a contract object is built from. `changekey`, `folder_id`,
# `internet_message_id` and `item_class` used to ride along here; no consumer
# ever reads them, and every one of them is mail plumbing that the contract
# deliberately does not speak.
_COLS = ("m.ews_id, m.conversation_id, m.sender_name, m.sender_email,"
         " m.to_json, m.subject, m.date_ts, m.body_clean")

# Any 64-bit constant unique to this lane; see sweep().
_SWEEP_LOCK_KEY = 0x6577735F73776570        # "ews_swep"


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
    is ever seen again.

    Serialised on an advisory lock held to the end of the transaction, because
    `nextval` is deliberately non-transactional: two concurrent sweeps can take
    seq 10 and seq 11, and if 11 commits first a reader hands out a cursor at
    11 and the row at 10 is permanently behind it — silent, unrecoverable
    message loss. Today the handlers do blocking work under a single uvicorn
    worker, so requests happen to serialise anyway; that is an accident of
    deployment, not a property of the code, and a second worker would end it.
    """
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SWEEP_LOCK_KEY,))
    cur = conn.execute(
        "INSERT INTO ews.bridge_arrival (ews_id, seq, changekey, first_seen)"
        # A mail this sweep is meeting for the first time arrived now, whatever
        # its send date says — that is the whole reason this ledger exists,
        # since a folder sync discovers three-week-old mail today and a cursor
        # over the send date would step straight past it. Reconstructing an
        # arrival from the send time is a *first migration* concern (005 does
        # exactly that, once, for the history that was already there); doing it
        # here as well would hand freshly discovered old mail a high sequence
        # with an old arrival time, and a bootstrap that met one would end with
        # its cursor above the whole ingestion window.
        " SELECT m.ews_id, nextval('ews.bridge_arrival_seq'), m.changekey,"
        "        now()"
        "   FROM ews.messages m"
        "   LEFT JOIN ews.bridge_arrival a ON a.ews_id = m.ews_id"
        "  WHERE m.deleted_at IS NULL"
        "    AND (a.ews_id IS NULL OR a.changekey IS DISTINCT FROM m.changekey)"
        # Oldest first, so a batch of newly-discovered mail enters the ledger
        # in send order and `until` stays a prefix of the sequence stream. Note
        # that nextval is evaluated wherever the planner puts it, so this is a
        # strong hint rather than a guarantee — which is exactly why migration
        # 005 assigns the pre-existing store's sequence with an explicit
        # row_number() instead of relying on an ORDER BY here.
        #
        # NULLS LAST keeps a mail with no send date at the end of the batch
        # rather than before five months of history, which matters because
        # `date_ts` is still what orders a batch even though it no longer sets
        # the arrival time.
        "  ORDER BY m.date_ts NULLS LAST, m.ews_id"
        " ON CONFLICT (ews_id) DO UPDATE"
        # first_seen moves to now() together with the new seq, and the two must
        # always move together. `seq` and `first_seen` are one fact seen two
        # ways — where this message sits in the arrival stream — because
        # `until` filters on first_seen while paging orders by seq, so the
        # moment they disagree `until` stops being a prefix of the stream.
        # Concretely: leave first_seen at the old send time and a mail amended
        # while a consumer is mid-bootstrap gets a sequence at the live head
        # and an arrival time inside the bound. The next bounded page returns
        # it, comes back short, and bootstrap finishes with a live cursor at
        # the head — with the whole ingestion window sitting below it, lost.
        # An amendment genuinely is a new arrival; that is why it earns a new
        # sequence in the first place. Any path that moves one column without
        # the other reopens this.
        "   SET seq = excluded.seq, changekey = excluded.changekey,"
        "       first_seen = now(), updated_at = now()")
    return cur.rowcount


def page(conn, *, after_seq: int | None, until: dt.datetime | None,
         limit: int) -> list[dict]:
    sql = ("SELECT " + _COLS + ", a.seq, a.first_seen FROM ews.bridge_arrival a"
           " JOIN ews.messages m ON m.ews_id = a.ews_id"
           " WHERE m.deleted_at IS NULL")
    args: list = []
    if after_seq is not None:
        sql += " AND a.seq > %s"
        args.append(after_seq)
    if until is not None:
        # Bound by arrival, not send (spec §3.2): a mail sent in March and
        # discovered today arrived today, and a replay bounded by send time
        # would drop it from a window it genuinely belongs to. Inclusive, to
        # match the contract's reference implementation (mindet's
        # `contract/fake.py` compares `arrived <= until`).
        sql += " AND a.first_seen <= %s"
        args.append(until)
    sql += " ORDER BY a.seq LIMIT %s"
    args.append(limit)
    return [dict(r) for r in conn.execute(sql, args)]
