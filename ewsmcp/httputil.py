"""ASGI helpers shared by ewsd's REST app and the thin MCP's HTTP transport.

They live in their own module because ``ewsmcp/http.py`` (ewsd's app) imports
the gateway and the tool packs, and the MCP process must not pull those in —
see tests/test_mcp_import_boundary.py.
"""

import hmac
import json
from typing import Any


def _authorized(headers, api_key: str) -> bool:
    expected = api_key.encode()
    for name, value in headers or []:
        lname = name.lower() if isinstance(name, bytes) else str(name).encode().lower()
        raw = value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
        if lname == b"authorization" and raw.lower().startswith("bearer "):
            if hmac.compare_digest(raw[7:].strip().encode(), expected):
                return True
        elif lname == b"x-api-key":
            if hmac.compare_digest(raw.strip().encode(), expected):
                return True
    return False


async def _send_json(send, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode()
    await send({"type": "http.response.start", "status": status, "headers": [
        [b"content-type", b"application/json"],
        [b"content-length", str(len(body)).encode()],
    ]})
    await send({"type": "http.response.body", "body": body})
