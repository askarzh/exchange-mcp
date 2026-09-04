"""MCP tool annotations, in a module with no heavy imports.

They live here rather than in ``ewsmcp/server.py`` (the daemon's wiring,
which imports the gateway and therefore exchangelib) because the thin MCP
process needs them and must not load exchangelib at all — see
tests/test_mcp_import_boundary.py.
"""

from mcp.types import ToolAnnotations

ANNOTATIONS = {
    "read": ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                            idempotentHint=True, openWorldHint=False),
    "write": ToolAnnotations(readOnlyHint=False, destructiveHint=False,
                             idempotentHint=False, openWorldHint=False),
    "destructive": ToolAnnotations(readOnlyHint=False, destructiveHint=True,
                                   idempotentHint=False, openWorldHint=False),
    "send": ToolAnnotations(readOnlyHint=False, destructiveHint=True,
                            idempotentHint=False, openWorldHint=True),
}
