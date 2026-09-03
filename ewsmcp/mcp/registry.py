"""The MCP's registry: the daemon packs' specs, re-homed. Local tools get the
Postgres-first handlers; everything else proxies to ewsd. Gates live in ewsd."""

from __future__ import annotations

import copy

from ..tools import calendar_people, mail_read, tasks, writes
from ..tools.base import CLASS_TIER, TIER_RANK, Context, ToolSpec
from . import local

LOCAL_TOOLS = frozenset(local.HANDLERS)


def _proxy(name: str):
    async def handler(ctx: Context, **kwargs):
        return await ctx.daemon.call_tool(name, kwargs)
    handler.__name__ = f"proxy_{name}"
    return handler


def build_mcp_registry(ctx: Context) -> dict[str, ToolSpec]:
    tier = ctx.settings.ews_capability_tier
    registry: dict[str, ToolSpec] = {}
    for spec in [*mail_read.TOOLS, *calendar_people.TOOLS, *tasks.TOOLS, *writes.TOOLS]:
        need = CLASS_TIER.get(spec.side_effect_class, "draft")
        if TIER_RANK[need] > TIER_RANK.get(tier, 2):
            continue
        schema = copy.deepcopy(spec.public_schema()["inputSchema"])
        registry[spec.name] = ToolSpec(
            name=spec.name, description=spec.description,
            side_effect_class=spec.side_effect_class, input_schema=schema,
            handler=local.HANDLERS.get(spec.name) or _proxy(spec.name),
            requires_ews=False, confirm=False, output_schema=spec.output_schema,
        )
    ctx.registry = registry
    return registry
