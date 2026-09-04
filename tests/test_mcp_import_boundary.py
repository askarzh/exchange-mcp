"""The thin MCP process must not import exchangelib. At all.

`ewsmcp` (the MCP) proxies every EWS-touching call to `ewsd`; it holds no
Exchange credentials and opens no EWS connection, so loading exchangelib
there is pure cost and a lie about the architecture — and it is exactly how
the boundary rots: one convenience import in a shared module (the archive
package, the cache package, a tool pack) and the MCP is dragging the EWS
machinery, its lxml/tzdata tail and a slower cold start into a process that
never uses any of it.

Checked in a SUBPROCESS because `sys.modules` in the pytest process is
already polluted by every other test module in this run.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# Every module the MCP process actually loads at boot (ewsmcp/main.py picks
# stdio or http; both go through mcp.server).
MCP_MODULES = ("ewsmcp.mcp.server", "ewsmcp.mcp.local", "ewsmcp.mcp.http",
               "ewsmcp.mcp.registry", "ewsmcp.mcp.dispatch", "ewsmcp.mcp.client")

_PROBE = """
import sys
{imports}
leaked = sorted(m for m in sys.modules if m.split('.')[0] == 'exchangelib')
print('LEAKED:' + ','.join(leaked))
"""


def _probe(modules):
    code = _PROBE.format(imports="\n".join(f"import {m}" for m in modules))
    out = subprocess.run([sys.executable, "-c", code], cwd=REPO,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    line = [ln for ln in out.stdout.splitlines() if ln.startswith("LEAKED:")][0]
    return [m for m in line[len("LEAKED:"):].split(",") if m]


def test_the_mcp_process_never_imports_exchangelib():
    leaked = _probe(MCP_MODULES)
    assert leaked == [], (
        "the thin MCP imported exchangelib — find the offending module with "
        "`python -X importtime -c 'import ewsmcp.mcp.server'` and move the "
        "EWS-touching code behind a daemon-only module (see "
        "ewsmcp/tools/write_specs.py, ewsmcp/gateway/wellknown.py, "
        f"ewsmcp/annotations.py for the pattern). Loaded: {leaked}")


def test_the_daemon_still_does_import_exchangelib():
    """The mirror image: ewsd owns Exchange, and its imports are at module
    top (tests/test_no_lazy_imports.py). If THIS ever stops being true the
    boundary test above has become vacuous."""
    assert _probe(("ewsmcp.server",)) != []
