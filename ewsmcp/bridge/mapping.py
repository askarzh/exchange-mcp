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


def chat(row: dict) -> dict:
    # A mail with no conversation id is its own thread. Bucketing every such
    # mail under one nameless chat would put unrelated correspondents in one
    # conversation, and Mindet links promises to a chat.
    recips = recipients(row)
    return {"native_id": row.get("conversation_id") or row["ews_id"],
            # Zero recipients collapse into direct deliberately, since mail only
            # uses direct and group, and a mail with genuinely no named recipients
            # is better modelled as a message with an unknown counterpart than as
            # an error.
            "kind": "group" if len(recips) > 1 else "direct",
            "name": row.get("subject") or None,
            "member_count": len(recips) + 1}


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
    native_id = (sender_email or row.get("sender_name") or row["ews_id"]).strip().lower()
    raw = [{"type": "smtp", "value": sender_email}] if sender_email else []
    return {
        "native_id": row["ews_id"],
        "chat": chat(row)["native_id"],
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
