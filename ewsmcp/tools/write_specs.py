"""Write-tool METADATA: names, descriptions, schemas, confirm classes.

Split out of ``writes.py`` because the thin MCP process needs the specs to
build its registry but must never import exchangelib, and ``writes.py``
imports it at module top for the handlers (Message, CalendarItem,
OofSettings, the send-invitation constants). Keeping the metadata here is
what lets ``ewsmcp/mcp/registry.py`` describe every write tool without
pulling the EWS machinery into a process that only proxies to ewsd
(tests/test_mcp_import_boundary.py).

``writes.py`` is still the single source of truth for BEHAVIOUR: it imports
``SPECS`` from here and binds the real handler, preview hook and confirm
predicate onto each one (``writes.TOOLS``). Nothing in this module may
import exchangelib, and nothing here decides whether a call is safe — the
gates live in the daemon's dispatcher, against ``writes.TOOLS``.
"""

from typing import Any, Dict, List, Optional

from .base import ToolSpec

MAX_BULK_IDS = 50

DRAFT_MODES = ("new", "reply", "reply_all", "forward")
RESPONSE_METHOD = {"accept": "accept", "tentative": "tentatively_accept",
                   "decline": "decline"}
# Names only: the exchangelib OofSettings constants they map to live in
# writes.py, which asserts the two sets still agree.
OOF_STATES = ("disabled", "enabled", "scheduled")


async def _unbound(ctx: Any, **kwargs: Any) -> Dict[str, Any]:
    """Placeholder handler. Every spec here is re-bound to its real handler
    in ``writes.py``; the MCP replaces it with a proxy to ewsd. Reaching
    this is a wiring bug, never a user-visible path."""
    raise RuntimeError("write spec used without a bound handler")


# --- schemas ------------------------------------------------------------------


def _obj(props: Dict[str, Any], required: Optional[List[str]] = None) -> Dict[str, Any]:
    return {"type": "object", "properties": props,
            "required": required or [], "additionalProperties": False}


_STR = {"type": "string"}
_BOOL = {"type": "boolean"}
_EMAILS = {"type": "array", "items": {"type": "string"}}
_IDS = {"type": "array", "items": {"type": "string"},
        "minItems": 1, "maxItems": MAX_BULK_IDS}


# --- the pack -----------------------------------------------------------------

SPECS: List[ToolSpec] = [
    ToolSpec(
        name="create_draft",
        description=(
            "Author mail as a DRAFT (never sends). mode=new needs to+body; "
            "reply/reply_all/forward need reply_to (the original message id) "
            "and quote the original server-side; forward also needs to. "
            "body is plain text by default (escaped, wrapped in minimal HTML); "
            "set body_format='html' to supply real markup verbatim for rich "
            "formatting (bold, lists, links, tables). "
            "importance applies to mode=new only. Use send_draft to send."
        ),
        side_effect_class="write",
        input_schema=_obj({
            "mode": {"type": "string", "enum": list(DRAFT_MODES), "default": "new"},
            "reply_to": _STR, "to": _EMAILS, "cc": _EMAILS, "bcc": _EMAILS,
            "subject": _STR, "body": _STR,
            "body_format": {"type": "string", "enum": ["text", "html"], "default": "text"},
            "importance": {"type": "string", "enum": ["normal", "high"]},
        }, required=["body"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="update_draft",
        description=("Update fields of an existing draft (to/cc/bcc/subject/body). "
                     "Only the supplied fields change. body_format='html' sends "
                     "the new body as verbatim markup instead of escaped text."),
        side_effect_class="write",
        input_schema=_obj({
            "draft_id": _STR, "to": _EMAILS, "cc": _EMAILS, "bcc": _EMAILS,
            "subject": _STR, "body": _STR,
            "body_format": {"type": "string", "enum": ["text", "html"], "default": "text"},
        }, required=["draft_id"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="create_upload_link",
        description=(
            "Mint a SINGLE-USE, short-lived URL that a human can PUT a local file "
            "to (plain `curl -T file <url>` — no headers). Use this instead of "
            "content_base64 for anything non-tiny: base64 travels through the "
            "model's context and costs ~350k tokens per MB. After the upload, "
            "pass the returned `path` to add_attachment."
        ),
        side_effect_class="write",
        input_schema=_obj({
            "name": _STR,
            "ttl_minutes": {"type": "integer", "minimum": 1, "maximum": 1440,
                            "default": 15},
        }, required=["name"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="add_attachment",
        description=(
            "Attach a file to a DRAFT. Exactly one source: `path` (a file inside "
            "DATA_DIR — pair with get_attachment(mode='save') to forward an "
            "attachment between messages at zero token cost), or `content_base64` "
            "+ `name` for small inline content. Paths outside DATA_DIR are "
            "refused. The draft is still not sent — use send_draft."
        ),
        side_effect_class="write",
        input_schema=_obj({
            "draft_id": _STR, "path": _STR, "content_base64": _STR, "name": _STR,
        }, required=["draft_id"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="delete_attachment",
        description=("Remove one attachment from a DRAFT, by exact name or "
                     "zero-based index as a string."),
        side_effect_class="write",
        input_schema=_obj({"draft_id": _STR, "attachment": _STR},
                          required=["draft_id", "attachment"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="delete_draft",
        description=("Move a draft to trash (recoverable). Refuses items that "
                     "are not in f:drafts."),
        side_effect_class="write",
        input_schema=_obj({"draft_id": _STR}, required=["draft_id"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="update_messages",
        description=(
            "Bulk-update up to 50 messages: set_read and/or categories_add/"
            "categories_remove. Per-item failures are isolated and reported. "
            "There is no follow-up-flag support in this backend — use "
            "categories_add (e.g. ['Follow up']) as the visible marker."
        ),
        side_effect_class="write",
        input_schema=_obj({
            "ids": _IDS, "set_read": _BOOL,
            "categories_add": _EMAILS, "categories_remove": _EMAILS,
        }, required=["ids"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="move_messages",
        description=("Move up to 50 messages to a folder (id, f:alias or path). "
                     "Message ids are re-bound automatically: the SAME id keeps "
                     "working after the move."),
        side_effect_class="write",
        input_schema=_obj({"ids": _IDS, "to_folder": _STR},
                          required=["ids", "to_folder"]),
        handler=_unbound,
        confirm=False,
    ),
    ToolSpec(
        name="create_event",
        description=(
            "Create a calendar event. Default send_invitations=false saves a "
            "placeholder WITHOUT emailing attendees (no confirm needed). With "
            "send_invitations=true, invitations leave the org: two-phase "
            "confirm + send kill-switch apply."
        ),
        side_effect_class="write",  # tier: draft; invite path confirm-gated below
        input_schema=_obj({
            "subject": _STR, "start": _STR, "end": _STR,
            "attendees": _EMAILS, "location": _STR, "body": _STR,
            "send_invitations": {"type": "boolean", "default": False},
        }, required=["subject", "start", "end"]),
        handler=_unbound,
        confirm=lambda kw: bool(kw.get("send_invitations")),
    ),
    ToolSpec(
        name="update_event",
        description=(
            "Update event fields (subject/start/end/location/body). Default "
            "notify_attendees=false is a silent metadata edit; true notifies "
            "changed attendees → two-phase confirm + send kill-switch apply."
        ),
        side_effect_class="write",
        input_schema=_obj({
            "event_id": _STR, "subject": _STR, "start": _STR, "end": _STR,
            "location": _STR, "body": _STR,
            "notify_attendees": {"type": "boolean", "default": False},
        }, required=["event_id"]),
        handler=_unbound,
        confirm=lambda kw: bool(kw.get("notify_attendees")),
    ),
    ToolSpec(
        name="send_draft",
        description=(
            "Send an existing draft (the ONLY way mail leaves this mailbox). "
            "Two-phase: the first call fetches the draft and returns its REAL "
            "recipients/subject/body as a preview + confirm_token bound to "
            "that content; the second call with the token re-verifies the "
            "content and sends (editing the draft in between invalidates the "
            "token). Optional idempotency_key dedupes retries (replays return "
            "the stored result)."
        ),
        side_effect_class="send",
        input_schema=_obj({"draft_id": _STR, "idempotency_key": _STR},
                          required=["draft_id"]),
        handler=_unbound,
        # send_draft's real predicate (which skips the gate on an
        # idempotent REPLAY) and its preview hook are bound in
        # writes.py; True here is the conservative shape the
        # public schema needs.
        confirm=True,
    ),
    ToolSpec(
        name="respond_to_event",
        description=("Accept/tentative/decline a meeting — this SENDS a response "
                     "to the organizer, so it is two-phase confirmed."),
        side_effect_class="send",
        input_schema=_obj({
            "event_id": _STR,
            "response": {"type": "string", "enum": sorted(RESPONSE_METHOD)},
            "message": _STR,
        }, required=["event_id", "response"]),
        handler=_unbound,
        confirm=True,
    ),
    ToolSpec(
        name="cancel_event",
        description=("Cancel a meeting you organize — sends cancellations to all "
                     "attendees (destructive, two-phase confirmed)."),
        side_effect_class="destructive",
        input_schema=_obj({"event_id": _STR, "message": _STR},
                          required=["event_id"]),
        handler=_unbound,
        confirm=True,
    ),
    ToolSpec(
        name="delete_messages",
        description=(
            "Delete up to 50 messages. disposition: 'trash' (default, "
            "recoverable), 'soft' (dumpster) or 'permanent' (UNRECOVERABLE — "
            "requires two-phase confirm). Class is destructive, so ALL "
            "dispositions need the full tier in v5.0 (conservative)."
        ),
        side_effect_class="destructive",
        input_schema=_obj({
            "ids": _IDS,
            "disposition": {"type": "string",
                            "enum": ["trash", "soft", "permanent"],
                            "default": "trash"},
        }, required=["ids"]),
        handler=_unbound,
        confirm=lambda kw: kw.get("disposition") == "permanent",
    ),
    ToolSpec(
        name="set_oof",
        description=(
            "Set out-of-office auto-replies (externally visible → send class, "
            "two-phase confirmed). state: disabled|enabled|scheduled; "
            "scheduled needs start+end. external_reply defaults to "
            "internal_reply when omitted."
        ),
        side_effect_class="send",
        input_schema=_obj({
            "state": {"type": "string", "enum": sorted(OOF_STATES)},
            "internal_reply": _STR, "external_reply": _STR,
            "start": _STR, "end": _STR,
        }, required=["state"]),
        handler=_unbound,
        confirm=True,
    ),
]
