"""One `ews.messages` row into one contract object (platform spec §3, §4).

Nothing here reads the database and nothing here is mail-specific beyond the
column names: the vocabulary Mindet receives is the same one every other bridge
speaks.
"""
from __future__ import annotations

import datetime as dt
import json


def identity(email: str | None) -> str | None:
    return f"email:{email.strip().lower()}" if email and email.strip() else None


def recipients(row: dict) -> list[dict]:
    raw = row.get("to_json")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return [p for p in parsed if isinstance(p, dict)]


def chat(row: dict) -> dict:
    # A mail with no conversation id is its own thread. Bucketing every such
    # mail under one nameless chat would put unrelated correspondents in one
    # conversation, and Mindet links promises to a chat.
    return {"native_id": row.get("conversation_id") or row["ews_id"],
            "kind": "group" if len(recipients(row)) > 1 else "direct",
            "name": row.get("subject") or None,
            "participants": len(recipients(row)) + 1}


def message(row: dict) -> dict:
    sent = dt.datetime.fromtimestamp(int(row["date_ts"] or 0), dt.timezone.utc)
    return {
        "native_id": row["ews_id"],
        "chat": {"native_id": chat(row)["native_id"]},
        "author": {"native_id": f"{row['ews_id']}:sender",
                   "key": identity(row.get("sender_email")),
                   "name": row.get("sender_name") or row.get("sender_email"),
                   "is_owner": False},
        "sent_at": sent.isoformat(),
        # Mail says a great deal in the subject alone; an empty body would hide
        # the whole message from triage and from search.
        "body": row.get("body_clean") or row.get("subject") or "",
        "item_type": "mail",
        "media": [],
    }
