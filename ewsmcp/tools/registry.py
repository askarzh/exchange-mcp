"""The daemon's tool registry: the four packs, tier-filtered.

Separate from ``tools/__init__.py`` on purpose: importing the packs pulls in
exchangelib (writes.py), and the thin MCP imports ``tools.base`` /
``tools.cache_reads`` — which would run the package ``__init__`` and drag
the EWS machinery into a process that must not have it
(tests/test_mcp_import_boundary.py). The MCP builds its own registry from
``ewsmcp/mcp/registry.py``.

The tool count is asserted by boot smokes and the generated docs — change it
DELIBERATELY.
"""

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
