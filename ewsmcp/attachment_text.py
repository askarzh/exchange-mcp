"""What get_attachment(mode='text') may decode as text, for the live and the
archive path alike.

Binary files are refused with the way to actually read them: their raw bytes
decoded as UTF-8 are thousands of tokens of noise that look like a successful
read. Parsing them here is deliberately out of scope — ewsd holds the Exchange
connection, and a hostile PDF from outside belongs in files-mcp's sandboxed
extractor, not in this process.
"""

from __future__ import annotations

from .errors import ToolError

_BINARY_TYPES = ("application/pdf", "application/zip", "application/msword",
                 "application/vnd.", "application/octet-stream",
                 "image/", "audio/", "video/")
_BINARY_SUFFIXES = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
                    ".zip", ".rar", ".7z", ".png", ".jpg", ".jpeg", ".gif",
                    ".heic", ".mp3", ".m4a", ".ogg", ".wav", ".mp4", ".mov")

READ_BINARY_HINT = ("Call again with mode='save' — it returns shared_name — then "
                    "read it with the Files MCP: extract_content(path=<shared_name>) "
                    "handles PDF, Office documents and images.")


def _looks_binary(data: bytes) -> bool:
    head = data[:8192]
    if b"\x00" in head:
        return True
    try:
        head.decode("utf-8")
    except UnicodeDecodeError as exc:
        # A multi-byte character cut at the 8 KiB boundary is still text.
        return exc.start < len(head) - 4
    return False


def refuse_if_binary(name: str, content_type: str | None, data: bytes, *,
                     texty: bool) -> None:
    """Raise a validation ToolError when mode='text' is asked of a binary file."""
    if texty:
        return
    ctype = (content_type or "").lower()
    known = ctype.startswith(_BINARY_TYPES) or (name or "").lower().endswith(_BINARY_SUFFIXES)
    if known or _looks_binary(data):
        kind = "a PDF" if ctype == "application/pdf" or name.lower().endswith(".pdf") \
            else f"a binary file ({content_type or 'unknown type'})"
        raise ToolError("validation",
                        f"Attachment {name!r} is {kind}, not text — mode='text' "
                        "cannot read it.",
                        hint=READ_BINARY_HINT)
