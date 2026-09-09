"""One `ews.messages` row into one contract object (platform spec §3, §4).

Nothing here reads the database and nothing here is mail-specific beyond the
column names: the vocabulary Mindet receives is the same one every other bridge
speaks.
"""
from __future__ import annotations

import datetime as dt
import json


def identity(email: str | None) -> str | None:
    """`email:<lower-cased address>` (spec §4), and nothing else.

    Exchange puts legacy distinguished names in this column for senders it
    could not resolve. Minting a key from one would give a person an identifier
    no other source can ever match, which is worse than having none.
    """
    value = (email or "").strip().lower()
    if "@" not in value:
        return None
    local, _, domain = value.partition("@")
    return f"email:{value}" if local and domain else None


def recipients(row: dict) -> list[dict]:
    """The people a mail went to, in one shape whatever the store holds.

    `to_json` is a JSON array of plain addresses in this store, but older rows
    and other writers have used objects, so both are accepted. A row this
    cannot parse yields no recipients rather than raising: one malformed
    header must not stop a whole page of mail from reaching the owner.
    """
    raw = row.get("to_json")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    out = []
    for p in parsed:
        if isinstance(p, str) and p.strip():
            out.append({"name": None, "email": p.strip()})
        elif isinstance(p, dict) and (p.get("email") or p.get("name")):
            out.append({"name": p.get("name"), "email": p.get("email")})
    return out


def chats_from_rows(rows: list[dict]) -> list[dict]:
    """Fold a page of `ews.messages` rows (most recent first) into the
    conversations they belong to.

    A chat's membership is the union of every sender and recipient ever seen
    on it, not the latest message's recipient list. `to_json` is a text
    column, so a naive `max(to_json)` in SQL sorts byte-wise — alphabetical,
    not by recency or membership — and a ten-person thread can come back as
    `direct` with `member_count: 2` purely by which address happens to sort
    last. A thread that was ever between four people stays a group
    conversation even when somebody later replies to the sender alone, and
    the union never flips between polls, so Mindet's chat records don't
    churn for no reason.

    `rows` is ordered most-recent-first, so the first row we meet for a
    conversation carries its most recent subject — that becomes `name`.
    """
    order: list[str] = []
    convos: dict[str, dict] = {}
    for row in rows:
        native_id = row["native_id"]
        convo = convos.get(native_id)
        if convo is None:
            convo = {"name": row.get("subject") or None, "members": set()}
            convos[native_id] = convo
            order.append(native_id)
        sender = row.get("sender_email")
        if sender and sender.strip():
            convo["members"].add(sender.strip().lower())
        for r in recipients({"to_json": row.get("to_json")}):
            email = r.get("email")
            if email:
                convo["members"].add(email.strip().lower())
    out = []
    for native_id in order:
        convo = convos[native_id]
        count = len(convo["members"])
        out.append({
            "native_id": native_id,
            # Two people or fewer is a direct conversation; more is a group.
            "kind": "group" if count > 2 else "direct",
            "name": convo["name"],
            "member_count": count,
        })
    return out


def chat_native_id(row: dict) -> str:
    """Which conversation a mail belongs to, from a raw `ews.messages` row.

    This is the rule the arrival ledger applies once, at first sight, and then
    stores in `bridge_arrival.chat_id` — it is not applied again on every read.
    Exchange fills a conversation id in late for a draft or an unindexed item,
    and recomputing this per read handed the same mail to the consumer under
    one chat and later under another, which the consumer records as two items:
    one mail, two directives in the owner's morning list. `message()` therefore
    reads the pinned column and never calls this; `arrival.page()` names it in
    SQL for the one case a row predates migration 006.

    A mail with no conversation id is its own thread. Bucketing every such mail
    under one nameless chat would put unrelated correspondents in one
    conversation, and Mindet links promises to a chat.
    """
    return row.get("conversation_id") or row["ews_id"]


def message(row: dict, *, owner_key: str | None = None) -> dict:
    # Prefer date_ts; fall back to first_seen when a mail's send time couldn't
    # be parsed. A message with no timestamp at all is not something a ledger
    # of deadlines can hold: it corrupts the timeline or loses all signal that
    # the time was unknown.
    ts = row.get("date_ts")
    if ts is None:
        # Fall back to when the store first met this mail at all — deliberately
        # `first_arrival`, which is written once, and never `first_seen`, which
        # moves to now() on every amendment. Reading the column that moves made
        # an undated draft look as though it had been sent a little later every
        # time the owner flagged it in Outlook.
        first_arrival = row.get("first_arrival")
        if first_arrival is None:
            raise ValueError(f"message {row['ews_id']!r} has no date_ts or first_arrival")
        if isinstance(first_arrival, str):
            sent = dt.datetime.fromisoformat(first_arrival)
        else:
            sent = first_arrival
    else:
        sent = dt.datetime.fromtimestamp(int(ts), dt.timezone.utc)

    author_key = identity(row.get("sender_email"))
    sender_email = row.get("sender_email")
    if sender_email:
        native_id = sender_email.strip().lower()
    elif row.get("sender_name"):
        native_id = row["sender_name"].strip().lower()
    else:
        # An ews_id is case-sensitive — it is an opaque Exchange handle, not a
        # name — so this last resort is left exactly as the store holds it.
        native_id = row["ews_id"]
    raw = [{"type": "smtp", "value": sender_email}] if sender_email else []
    return {
        "native_id": row["ews_id"],
        # The pinned id from the arrival ledger, not a fresh coalesce over
        # this row: see chat_native_id's docstring for what recomputing costs.
        "chat": row["chat_id"],
        "author": {
            "native_id": native_id,
            "key": author_key,
            "name": row.get("sender_name") or sender_email,
            "raw": raw,
            "is_owner": bool(owner_key) and author_key == owner_key,
        },
        "sent_at": sent.isoformat(),
        # Mail says a great deal in the subject alone; an empty text would hide
        # the whole message from triage and from search.
        "text": row.get("body_clean") or row.get("subject") or "",
        "kind": "mail",
        "files": [],
    }
