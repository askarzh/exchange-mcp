"""MCP-side dispatcher: alias resolution for local reads, verbatim forwarding
for everything else. No kill-switch/tier/confirm here — ewsd owns those."""

from __future__ import annotations

from typing import Any

from ..errors import ToolError, map_exception
from ..tools.base import Context, ToolSpec, resolve_ids
from .registry import LOCAL_TOOLS


async def dispatch_mcp(ctx: Context, spec: ToolSpec, kwargs: dict[str, Any]) -> dict[str, Any]:
    outcome = "ok"
    try:
        if spec.name in LOCAL_TOOLS:
            kwargs = resolve_ids(ctx, kwargs)
        result = await spec.handler(ctx, **kwargs)
        if isinstance(result, dict):
            result.setdefault("ok", True)
            if result.get("ok") is False:
                outcome = result.get("error", {}).get("code", "error")
        return result
    except ToolError as err:
        outcome = err.code
        return err.to_dict()
    except (TypeError, ValueError) as exc:
        outcome = "validation"
        return ToolError("validation", f"{type(exc).__name__}: {exc}",
                          hint="Check the argument names and types against the tool schema."
                          ).to_dict()
    except Exception as exc:  # noqa: BLE001
        err = map_exception(exc)
        outcome = err.code
        return err.to_dict()
    finally:
        ctx.bump(f"tool.{spec.name}")
        if outcome != "ok":
            ctx.bump(f"err.{outcome}")
