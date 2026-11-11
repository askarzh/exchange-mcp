"""Reply and forward drafts for mail that exists only in the archive.

Exchange builds a reply or forward from the ORIGINAL item (create_reply /
create_forward). Once the archiver has deleted that item from Exchange
(archive_state='deleted'), those calls fail with ErrorItemNotFound — and the
generic "re-run search_messages" hint just finds the same archived message
again. Here the draft is built from the archived .eml instead:

- forward: the original .eml is attached (forward-as-attachment, which carries
  every original attachment byte-for-byte), and the body repeats its header
  block and text so the reader does not have to open it;
- reply / reply_all: addressed from the archived headers (Reply-To or From;
  reply_all adds To + Cc minus this mailbox), subject "Re: …", the original
  quoted, and In-Reply-To / References set so clients thread it.

Pure functions plus one builder that runs on the EWS pool.
"""

from __future__ import annotations

import email
import email.policy
import email.utils
from typing import Any, Dict, List, Optional

from ..bodyclean import html_to_text

QUOTE_CAP = 20_000  # characters of original text repeated in the body


def parse_eml(raw: bytes) -> Dict[str, Any]:
    """Headers, recipients and readable text of an archived message."""
    msg = email.message_from_bytes(raw, policy=email.policy.default)

    def addrs(*headers: str) -> List[str]:
        values = [str(v) for h in headers for v in (msg.get_all(h) or [])]
        return [a for _n, a in email.utils.getaddresses(values) if a]

    text = ""
    part = msg.get_body(preferencelist=("plain", "html"))
    if part is not None:
        try:
            content = part.get_content()
        except (LookupError, ValueError):
            content = part.get_payload(decode=True).decode("utf-8", "replace")
        text = html_to_text(content) if part.get_content_subtype() == "html" else content
    text = text.strip()
    if len(text) > QUOTE_CAP:
        text = (text[:QUOTE_CAP].rstrip()
                + "\n[… shortened; the full message is attached or archived]")
    return {
        # Folded headers keep their CRLF+tab; collapse so "RE:" is seen.
        "subject": " ".join(str(msg.get("Subject") or "").split()),
        "from": str(msg.get("From") or ""),
        "date": str(msg.get("Date") or ""),
        "to_header": str(msg.get("To") or ""),
        "cc_header": str(msg.get("Cc") or ""),
        "reply_target": (addrs("Reply-To") or addrs("From"))[:1],
        "to": addrs("To"),
        "cc": addrs("Cc"),
        "message_id": str(msg.get("Message-ID") or "").strip(),
        "references": " ".join(str(msg.get("References") or "").split()),
        "text": text,
    }


def _dedupe(addresses: List[str], drop: List[str]) -> List[str]:
    seen = {a.lower() for a in drop if a}
    out: List[str] = []
    for a in addresses:
        if a and a.lower() not in seen:
            seen.add(a.lower())
            out.append(a)
    return out


def forward_text(orig: Dict[str, Any]) -> str:
    lines = ["---------- Forwarded message ----------", f"From: {orig['from']}",
             f"Date: {orig['date']}", f"Subject: {orig['subject']}", f"To: {orig['to_header']}"]
    if orig["cc_header"]:
        lines.append(f"Cc: {orig['cc_header']}")
    return "\n".join(lines) + "\n\n" + orig["text"]


def reply_text(orig: Dict[str, Any]) -> str:
    quoted = "\n".join("> " + line for line in orig["text"].splitlines())
    return f"On {orig['date']}, {orig['from']} wrote:\n{quoted}"


def recipients(mode: str, orig: Dict[str, Any], self_address: str,
               to: Optional[List[str]], cc: Optional[List[str]]) -> tuple:
    """(to, cc) for the draft, following the Exchange path's rules: a reply
    may override `to`; reply_all goes to the sender and copies everyone else
    on the original except this mailbox; forward takes the caller's `to`."""
    if mode == "forward":
        return list(to or []), list(cc or [])
    target = list(to) if (mode == "reply" and to) else orig["reply_target"]
    if mode == "reply":
        return target, list(cc or [])
    others = _dedupe(orig["to"] + orig["cc"] + list(cc or []), [self_address, *target])
    return target, others
