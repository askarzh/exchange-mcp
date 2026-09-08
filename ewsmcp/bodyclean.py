"""Deterministic, dependency-free email body cleaning (stdlib only).

Email bodies returned to an LLM are bloated by quoted reply chains,
signatures and disclaimers. This module strips them without any third
party dependency and without importing exchangelib. It handles the
English Outlook and Gmail reply conventions and ``>`` quote prefixes.

Public API:
    strip_quoted_history(text) -> (latest_reply_text, markers_stripped)
    strip_signature(text)      -> text without a trailing signature
    clean_body(text, max_chars) -> {"text", "quoted_blocks_stripped",
                                    "truncated", "original_chars",
                                    "disclaimer_cut"}
    html_to_text(html)         -> minimal readable plain text
"""

from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser

# --------------------------------------------------------------------------
# Line normalization helpers
# --------------------------------------------------------------------------

# One or more leading '>' quote markers ("> ", ">> ", "> > " ...).
_QUOTE_PREFIX_RE = re.compile(r"^(?:>\s?)+")


# Invisible code points mail clients leave behind (Apple Mail's U+FEFF after
# a forward header, zero-width spaces/joiners from HTML). NBSP becomes a
# plain space so word matching and tokenising see it as one.
_INVISIBLE_RE = re.compile("[\ufeff\u200b\u200c\u200d\u2060]")


def _normalize_newlines(text: str) -> str:
    text = _INVISIBLE_RE.sub("", text).replace("\u00a0", " ")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _norm_line(line: str) -> tuple[str, bool]:
    """Return (normalized_line, was_quoted).

    Normalization: strip surrounding whitespace and remove leading '>' quote
    prefixes (recording that they were there).
    """
    s = line.strip()
    m = _QUOTE_PREFIX_RE.match(s)
    quoted = m is not None
    if m:
        s = s[m.end():].strip()
    return s, quoted


# --------------------------------------------------------------------------
# Reply-history markers
# --------------------------------------------------------------------------

# "-----Original Message-----" separator (Outlook EN).
_ORIG_EN_RE = re.compile(r"^-{2,}\s*original message\s*-{2,}$", re.IGNORECASE)

# Outlook EN header block: "From:" paired with a following "Sent:"/"Date:".
_FROM_EN_RE = re.compile(r"^from\s*:", re.IGNORECASE)
_PAIR_EN_RE = re.compile(r"^(?:sent|date)\s*:", re.IGNORECASE)

# Apple Mail / Outlook RU attribution: "29 мая 2026 г., в 12:20, Имя
# <a@b> написал(а):" — the direct equivalent of the Gmail EN line below,
# and the only marker that closes a Russian quoted chain. The trailing
# "$" is load-bearing: "Он написал: посмотри вложение" keeps text after
# the colon and is ordinary prose, not a quote header.
_WROTE_RU_RE = re.compile(r"^.{4,200}?(?:напис|пис)ал(?:\(а\)|а)?\s*:$",
                          re.IGNORECASE)
# Kazakh equivalent, same shape ("… жазды:").
_WROTE_KZ_RE = re.compile(r"^.{4,200}?жазды\s*:$", re.IGNORECASE)

# Gmail-style EN attribution: "On Mon, Jun 1, 2026 ... <a@b> wrote:".
# Up to 200 chars: Outlook-rendered addresses ("<a@b<mailto:a@b>>") push the
# attribution line well past 80.
_GMAIL_EN_RE = re.compile(r"^On .{4,200} wrote:\s*$", re.IGNORECASE)

# Minimum length of a run of '>'-prefixed lines treated as a quoted block.
_MIN_QUOTE_RUN = 3


def _find_markers(
    lines: list[str],
) -> tuple[list[tuple[int, str]], list[tuple[int, int]]]:
    """Return (markers, quote_runs).

    markers: sorted list of (line_index, kind) where kind is one of
        "original", "outlook_en", "gmail_en", "quote_run".
    quote_runs: list of (start_index, end_index) inclusive spans of runs of
        3+ consecutive '>'-quoted lines.
    """
    n = len(lines)
    norm = [_norm_line(ln) for ln in lines]
    markers: list[tuple[int, str]] = []
    runs: list[tuple[int, int]] = []

    # Runs of 3+ consecutive '>'-quoted lines.
    i = 0
    while i < n:
        if norm[i][1]:
            j = i
            while j < n and norm[j][1]:
                j += 1
            if j - i >= _MIN_QUOTE_RUN:
                runs.append((i, j - 1))
                markers.append((i, "quote_run"))
            i = j
        else:
            i += 1

    for idx in range(n):
        s = norm[idx][0]
        if not s:
            continue
        if _ORIG_EN_RE.match(s):
            markers.append((idx, "original"))
            continue
        if _FROM_EN_RE.match(s) and any(
            _PAIR_EN_RE.match(norm[k][0]) for k in range(idx + 1, min(idx + 5, n))
        ):
            markers.append((idx, "outlook_en"))
            continue
        if _GMAIL_EN_RE.match(s):
            markers.append((idx, "gmail_en"))
            continue
        if _WROTE_RU_RE.match(s) or _WROTE_KZ_RE.match(s):
            markers.append((idx, "wrote_ru"))

    markers.sort(key=lambda m: m[0])
    return markers, runs


def strip_quoted_history(text: str) -> tuple[str, int]:
    """Cut the body at the first reply-history marker.

    Returns (latest-reply-only text, count of quoted blocks/markers found in
    the stripped tail). A marker sitting on the *first* non-empty line is
    ignored (a body that genuinely starts with a marker-like line survives)
    — except an "Original Message" separator, which may cut even that
    early. The second non-empty line is NOT protected: a one-line reply
    directly followed by the quoted chain is the most common email shape.
    Trailing whitespace lines are stripped from the result.
    """
    if not text:
        return ("", 0)
    t = _normalize_newlines(text)
    lines = t.split("\n")
    markers, runs = _find_markers(lines)

    nonempty = [i for i, ln in enumerate(lines) if ln.strip()]
    protected = set(nonempty[:1])

    cut: int | None = None
    for idx, kind in markers:
        if idx in protected and kind != "original":
            continue
        cut = idx
        break
    if cut is None:
        return (t.rstrip(), 0)

    # Count markers in the removed tail; markers that sit inside a counted
    # '>'-quoted run are absorbed by the run (counted once).
    run_spans = [(a, b) for (a, b) in runs if a >= cut]
    count = 0
    for idx, kind in markers:
        if idx < cut:
            continue
        if kind != "quote_run" and any(a <= idx <= b for (a, b) in run_spans):
            continue
        count += 1
    return ("\n".join(lines[:cut]).rstrip(), max(count, 1))


# --------------------------------------------------------------------------
# Signature stripping
# --------------------------------------------------------------------------

# (prefix, max length allowed AFTER the prefix on the same line).
# The remainder cap keeps lines like "Thanks, that works for me." safe.
# Longest remainder allowed after a closer prefix on the same line.
_MAX_CLOSER_LINE_LEN = 200

_CLOSERS_EN: list[tuple[str, int]] = [
    ("best regards", 4),
    ("kind regards", 4),
    ("regards", 2),
    ("thanks,", 2),
    ("sent from my", 40),
]

# Russian sign-off closers, same mechanism as _CLOSERS_EN.
# Russian business mail routinely puts the closer, name, title and company
# on ONE line ("С уважением, Имя Фамилия Старший эксперт …"), so unlike the
# EN closers these allow a long remainder: the phrases are unambiguous
# sign-offs, and `strip_signature` still only looks at the trailing block.
_CLOSERS_RU: list[tuple[str, int]] = [
    ("с уважением", _MAX_CLOSER_LINE_LEN),
    ("с наилучшими пожеланиями", _MAX_CLOSER_LINE_LEN),
    ("с благодарностью", _MAX_CLOSER_LINE_LEN),
]

_CLOSERS: list[tuple[str, int]] = _CLOSERS_EN + _CLOSERS_RU

_MAX_SIG_LINES = 6
_MAX_SIG_LINE_LEN = 80


def _is_closer_line(s: str) -> bool:
    cf = s.casefold()
    return any(cf.startswith(prefix) and len(cf) - len(prefix) <= max_rest
               for prefix, max_rest in _CLOSERS)


def strip_signature(text: str) -> str:
    """Remove a trailing signature block when one is confidently detected.

    Two detectors: the RFC "-- " delimiter line, and a trailing block of at
    most 6 short lines that begins with a common EN closer. Conservative:
    only strips when at least 2 non-empty body lines remain above.
    """
    if not text:
        return ""
    t = _normalize_newlines(text).rstrip()
    if not t:
        return ""
    lines = t.split("\n")

    # RFC signature delimiter: a line that is exactly "--" / "-- ".
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].rstrip() == "--":
            above = sum(1 for ln in lines[:i] if ln.strip())
            below = sum(1 for ln in lines[i + 1:] if ln.strip())
            if above >= 2 and below <= 10:
                return "\n".join(lines[:i]).rstrip()
            break

    nonempty_idx = [i for i, ln in enumerate(lines) if ln.strip()]
    for i in nonempty_idx[-_MAX_SIG_LINES:]:
        s, _quoted = _norm_line(lines[i])
        if not _is_closer_line(s):
            continue
        block_ne = [ln for ln in lines[i:] if ln.strip()]
        if len(block_ne) > _MAX_SIG_LINES:
            continue
        if any(len(ln.strip()) > _MAX_SIG_LINE_LEN for ln in block_ne[1:]):
            continue
        if len(block_ne[0].strip()) > _MAX_CLOSER_LINE_LEN:
            continue
        if sum(1 for ln in lines[:i] if ln.strip()) < 2:
            continue
        return "\n".join(lines[:i]).rstrip()
    return t


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------

_TRUNC_SUFFIX = "… [truncated]"
# 3+ consecutive blank lines (possibly whitespace-only) -> a single blank.
_BLANK_RUN_RE = re.compile(r"\n(?:[ \t]*\n){3,}")

# Russian-localised forward/reply header lines (Outlook, Apple Mail). Unlike
# the English "From:/Sent:" block these do NOT mark quoted history — a
# forwarded message's content is often the only copy in the mailbox and must
# stay — but the header lines themselves are boilerplate that dominates
# embedding similarity ("every forward looks like every other forward"), so
# they are dropped line by line.
_RU_HEADER_LINE_RE = re.compile(
    r"^\s*(?:начало переадресованного письма|от|отправлено|дата|кому|копия|тема)"
    r"\s*:.*$",
    re.IGNORECASE | re.MULTILINE)


# Corporate "external sender" banners the mail gateway prepends to every
# inbound message (KZ / RU / EN variants of the same four lines). Identical
# on ~15% of the mailbox, they make unrelated external mail look alike to the
# embedder and add nothing a reader wants. Matched per line, anywhere in the
# body (they also ride inside quoted history).
_BANNER_LINE_RE = re.compile(
    r"^.*(?:"
    r"назар аударыңыз|бұл хатты сыртқы адресат|егер хатты күдікті деп|"
    r"данное письмо отправлено внешним|если считаете письмо подозрительным|"
    r"будьте осторожны при работе с вложениями|"
    r"this (?:e-?mail|message) (?:was|is) sent (?:by|from) an external|"
    r"if you (?:consider|find) this (?:e-?mail|message) suspicious"
    r").*$",
    re.IGNORECASE | re.MULTILINE)


# Outlook's auto-inserted horizontal rule line ("________________________")
# separating a forwarded/replied message from the rest of the body. Purely
# decorative, so it is dropped line by line like the RU header lines.
# Outlook indents the separator in some layouts, so the leading-whitespace
# allowance is load-bearing.
_OUTLOOK_RULE_LINE_RE = re.compile(r"^[ \t]*_{10,}[ \t]*$", re.MULTILINE)


def strip_header_lines(text: str) -> str:
    text = _RU_HEADER_LINE_RE.sub("", text)
    text = _BANNER_LINE_RE.sub("", text)
    return _OUTLOOK_RULE_LINE_RE.sub("", text)


# --------------------------------------------------------------------------
# Disclaimer tail cut
# --------------------------------------------------------------------------

_PARA_SPLIT_RE = re.compile(r"\n[ \t]*\n+")
TAIL_MIN_CHARS = 400
TAIL_MAX_PARAGRAPHS = 8
TAIL_START_FRACTION = 0.6

_DISCLAIMER_ANCHOR_RE = re.compile(
    r"(?:не является предложением|предназначено только для получател|"
    r"является конфиденциальн|если вы не являетесь адресатом|"
    r"получили это сообщение по ошибке|"
    r"тек хабарламада көрсетілген алушыларға|құпия ақпарат|"
    r"қателесіп алсаңыз|"
    r"intended solely for|intended only for the|confidentiality notice|"
    r"if you are not the intended recipient|"
    r"received this (?:e-?mail|message) in error|privileged and confidential)",
    re.IGNORECASE)


# Auto-generated meeting and notification footers. None of these phrases
# occurs in prose a person writes, so they are cut from the tail window
# exactly like a legal disclaimer.
_MEETING_TAIL_ANCHOR_RE = re.compile(
    r"(?:teams\.microsoft\.com/meetingoptions|"
    r"aka\.ms/jointeamsmeeting|"
    r"play\.google\.com/store/apps/details\?id=com\.microsoft\.teams|"
    r"for organizers\s*:|для организаторов\s*:|"
    r"собрание microsoft teams|microsoft teams meeting|"
    r"получить outlook для|get outlook for|"
    r"письмо отправлено автоматически|"
    r"you are receiving this (?:e-?mail|message) because)",
    re.IGNORECASE)


def _paragraphs(text: str) -> list[tuple[int, str]]:
    """(char offset, paragraph) for every non-empty blank-line-separated block."""
    out, pos = [], 0
    for m in _PARA_SPLIT_RE.finditer(text):
        chunk = text[pos:m.start()]
        if chunk.strip():
            out.append((pos, chunk))
        pos = m.end()
    if text[pos:].strip():
        out.append((pos, text[pos:]))
    return out


def tail_paragraphs(text: str) -> list[tuple[int, str]]:
    """The tail window the disclaimer rules and detectors may act on: the
    last TAIL_MAX_PARAGRAPHS paragraphs that start in the last 40% of the
    text. Never includes the first paragraph. Empty for short messages."""
    if len(text) < TAIL_MIN_CHARS:
        return []
    paras = _paragraphs(text)
    if len(paras) < 2:
        return []
    floor = int(len(text) * TAIL_START_FRACTION)
    tail = [(off, p) for off, p in paras[1:] if off >= floor]
    return tail[-TAIL_MAX_PARAGRAPHS:]


def strip_disclaimer_tail(text: str) -> tuple[str, bool]:
    """Cut from the first boilerplate paragraph to the end.

    Two anchor sets with deliberately different reach. A legal disclaimer
    shares its vocabulary with ordinary prose ("это письмо является
    конфиденциальным" can be the point of the message), so it may only cut
    inside the tail window. A Teams/Outlook auto-footer cannot occur in
    prose at all, so it cuts wherever it starts — a short mail can carry a
    400-character invitation block that never reaches the tail window.
    Neither may touch the first paragraph.
    """
    cut: int | None = None
    for off, para in tail_paragraphs(text):
        if _DISCLAIMER_ANCHOR_RE.search(para):
            cut = off
            break
    for off, para in _paragraphs(text)[1:]:
        if _MEETING_TAIL_ANCHOR_RE.search(para):
            cut = off if cut is None else min(cut, off)
            break
    if cut is None:
        return text, False
    return text[:cut].rstrip(), True


def clean_body(text: str, max_chars: int = 4000) -> dict:
    """Full cleaning pipeline for an email body.

    normalize newlines -> strip quoted history -> strip signature ->
    collapse 3+ blank lines to 1 -> rstrip -> truncate at a word boundary.
    """
    raw = text or ""
    original_chars = len(raw)
    t = _normalize_newlines(raw)
    t, quoted_blocks = strip_quoted_history(t)
    t = strip_header_lines(t)
    t, disclaimer_cut = strip_disclaimer_tail(t)
    t = strip_signature(t)
    t = _BLANK_RUN_RE.sub("\n\n", t)
    t = t.strip()

    truncated = False
    if max_chars and len(t) > max_chars:
        truncated = True
        budget = max(1, max_chars - len(_TRUNC_SUFFIX))
        cut = t[:budget]
        if budget < len(t) and not t[budget].isspace():
            # Mid-word: back up to the previous whitespace boundary.
            ws = max(cut.rfind(" "), cut.rfind("\n"), cut.rfind("\t"))
            if ws > 0:
                cut = cut[:ws]
        t = cut.rstrip() + _TRUNC_SUFFIX

    return {
        "text": t,
        "quoted_blocks_stripped": quoted_blocks,
        "truncated": truncated,
        "original_chars": original_chars,
        "disclaimer_cut": disclaimer_cut,
    }


# --------------------------------------------------------------------------
# Minimal HTML -> text (for reading)
# --------------------------------------------------------------------------

_BLOCK_TAGS = {
    "p", "div", "tr", "li", "table",
    "h1", "h2", "h3", "h4", "h5", "h6",
}
_SKIP_TAGS = ("style", "script", "head")
_MAX_HREF_SHOWN = 80


def _norm_url(u: str) -> str:
    u = u.strip().casefold()
    u = re.sub(r"^(?:https?:)?//", "", u)
    u = re.sub(r"^mailto:", "", u)
    if u.startswith("www."):
        u = u[4:]
    return u.rstrip("/")


def _render_link(href: str, label: str) -> str:
    if not label:
        return href if href and len(href) <= _MAX_HREF_SHOWN else ""
    if not href or len(href) > _MAX_HREF_SHOWN or _norm_url(href) == _norm_url(label):
        return label
    return f"{label} ({href})"


class _HTMLToText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._out: list[str] = []
        self._skip = 0
        self._href: str = ""
        self._label: list[str] | None = None

    def _append(self, s: str) -> None:
        if self._label is not None:
            self._label.append(" " if s == "\n" else s)
        else:
            self._out.append(s)

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            self._skip += 1
            return
        if self._skip:
            return
        if tag == "br":
            self._append("\n")
            return
        if tag in _BLOCK_TAGS:
            self._append("\n")
        if tag == "a":
            self._flush_link()
            self._href = (dict(attrs).get("href") or "").strip()
            self._label = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            if self._skip:
                self._skip -= 1
            return
        if self._skip:
            return
        if tag == "a":
            self._flush_link()
            return
        if tag in ("td", "th"):
            self._append(" ")
        if tag in _BLOCK_TAGS:
            self._append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        self._append(data)

    def _flush_link(self) -> None:
        if self._label is None:
            return
        label = " ".join("".join(self._label).split())
        href, self._href, self._label = self._href, "", None
        self._out.append(_render_link(href, label))

    def finish(self) -> str:
        self._flush_link()
        return "".join(self._out)


def html_to_text(html: str) -> str:
    """Minimal stdlib HTML -> text conversion for reading.

    Block tags (p, div, br, tr, li, h1-h6, table) emit newlines; style /
    script / head contents are dropped; entities are unescaped; links keep
    their href only when it is short and differs from the label. Whitespace
    is collapsed per line.
    """
    if not html:
        return ""
    parser = _HTMLToText()
    try:
        parser.feed(html)
        parser.close()
        raw = parser.finish()
    except Exception:
        # HTMLParser is lenient, but never let junk markup crash a read
        # path: fall back to a crude tag strip.
        raw = unescape(re.sub(r"<[^>]*>", " ", html))

    lines = [" ".join(ln.split()) for ln in raw.split("\n")]
    out: list[str] = []
    for ln in lines:
        if ln:
            out.append(ln)
        elif out and out[-1] != "":
            out.append("")
    while out and out[-1] == "":
        out.pop()
    return "\n".join(out)
