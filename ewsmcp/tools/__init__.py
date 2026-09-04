"""Tool registry: tools in four packs, tier-filtered. The count is asserted
by boot smokes and the generated docs — change it DELIBERATELY."""

from typing import Dict

from . import archive, calendar_people, mail_read, tasks, writes
from .base import CLASS_TIER, TIER_RANK, Context, ToolSpec


def build_registry(ctx: Context) -> Dict[str, ToolSpec]:
    specs = [*mail_read.TOOLS, *calendar_people.TOOLS, *tasks.TOOLS,
             *writes.TOOLS, *archive.TOOLS]
    tier = ctx.settings.ews_capability_tier
    registry: Dict[str, ToolSpec] = {}
    for spec in specs:
        need = CLASS_TIER.get(spec.side_effect_class, "draft")
        if TIER_RANK[need] <= TIER_RANK.get(tier, 2):
            registry[spec.name] = spec
    ctx.registry = registry
    return registry
