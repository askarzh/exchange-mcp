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
    """Which conversation a single mail belongs to.

    Only the id: `kind`, `name` and `member_count` for a conversation come from
    `chats_from_rows`, which folds every row of a thread together. This used to
    return a whole chat object as well, with its own (different) idea of
    membership — one row's recipients plus the sender — and nothing ever read
    it. Two definitions of `member_count` in one module is a bug waiting to be
    exported, so there is now one, and it lives where the union is computed.

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
        # Fall back to when the store first saw it if send time is unknown.
        first_seen = row.get("first_seen")
        if first_seen is None:
            raise ValueError(f"message {row['ews_id']!r} has no date_ts or first_seen")
        if isinstance(first_seen, str):
            sent = dt.datetime.fromisoformat(first_seen)
        else:
            sent = first_seen
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
        "chat": chat_native_id(row),
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
