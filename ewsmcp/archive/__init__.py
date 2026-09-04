"""The archive pipeline: capture → verify → delete, plus the embedder.

Every worker is idempotent and driven by ``messages.archive_state``, so a
crashed or half-finished pass is simply repeated on the next cycle. Nothing
here is imported by the MCP process — the daemon owns Exchange, the blob
store and the Gemini key.
"""
