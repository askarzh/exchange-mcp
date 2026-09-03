"""HTTP client for ewsd's /v1 API. The MCP forwards tool calls it cannot
answer from Postgres; envelopes come back verbatim so the model sees exactly
what the daemon's dispatcher produced (previews, confirm tokens, errors)."""

from __future__ import annotations

from typing import Any

import httpx

from ..errors import ToolError


class DaemonClient:
    def __init__(self, base_url: str, api_key: str | None, timeout: float = 90.0,
                 transport: Any = None):
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout,
                                         headers=headers, transport=transport)

    async def _request(self, method: str, path: str, **kw) -> dict[str, Any]:
        try:
            resp = await self._client.request(method, path, **kw)
        except httpx.HTTPError as exc:
            raise ToolError(
                "daemon_unavailable",
                f"ewsd unreachable: {type(exc).__name__}: {exc}",
                hint="Check that ewsd is running and EWSD_URL / EWSD_API_KEY are set.",
                retry_after_s=15) from exc
        try:
            data = resp.json()
        except ValueError as exc:
            raise ToolError("upstream_error",
                            f"ewsd returned non-JSON (HTTP {resp.status_code})") from exc
        if not isinstance(data, dict):
            raise ToolError("upstream_error", "ewsd returned a non-object body")
        return data

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", f"/v1/tools/{name}", json=arguments or {})

    async def status(self) -> dict[str, Any]:
        return await self._request("GET", "/v1/status")

    async def aclose(self) -> None:
        await self._client.aclose()
