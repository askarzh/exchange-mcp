#!/usr/bin/env python3
"""v5 boot smoke: two real processes, no Exchange needed.

Boots `ewsd` (the daemon) against an unreachable Exchange and a throwaway
Postgres, then boots `ewsmcp` (the thin MCP, HTTP transport) pointed at that
daemon and database. Asserts:

- ewsd survives boot; `/livez` 200; `/readyz` 503 state=connecting
- ewsmcp survives boot; `/livez` 200; `/readyz` 200 with daemon status info
- `get_server_status` works cold via ewsd's REST shim
- an EWS-backed read (`search_messages`) fails fast with `upstream_unavailable`
  (no mirror yet, Exchange cold)
- `send_draft` refusal carries the `kill_switch` code (policy beats
  connectivity) and ewsd's `/openapi.json` publishes `confirm_token` for it
- ewsmcp's `/mcp` (Streamable HTTP, stateless) answers `initialize` and
  `tools/list`; tier=full lists 31 tools

Run from v5/:  python scripts/boot_smoke.py [draft|full]
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _pg import ThrowawayPostgres

EWSD_PORT = 8790
MCP_PORT = 8124
EWSD_BASE = f"http://127.0.0.1:{EWSD_PORT}"
MCP_BASE = f"http://127.0.0.1:{MCP_PORT}"
EWSD_KEY = "v5-smoke-ewsd-key"
MCP_KEY = "v5-smoke-mcp-key"


def _req(base, key, method, path, payload=None, timeout=8):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("Authorization", f"Bearer {key}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def _mcp_req(base, key, payload, timeout=10):
    """POST a JSON-RPC message to /mcp; the response may be plain JSON or a
    Streamable-HTTP SSE stream (one or more `data:` lines)."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(base + "/mcp", data=data, method="POST")
    req.add_header("Authorization", f"Bearer {key}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json, text/event-stream")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        ctype = r.headers.get("Content-Type", "")
        raw = r.read().decode()
    if "text/event-stream" in ctype:
        result = None
        for line in raw.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                result = json.loads(line[len("data:"):].strip())
        return result
    return json.loads(raw)


def _wait_livez(base, deadline_s, proc):
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        try:
            if urllib.request.urlopen(base + "/livez", timeout=2).status == 200:
                return True
        except Exception:  # noqa: BLE001 - startup race
            time.sleep(0.5)
    return False


def main() -> int:
    tier = sys.argv[1] if len(sys.argv) > 1 else "full"
    failures: list[str] = []

    with ThrowawayPostgres(name_suffix=f"smoke-{os.getpid()}") as dsn, \
         tempfile.TemporaryDirectory(prefix="v5-smoke-") as tmp:
        ewsd_env = dict(os.environ)
        ewsd_env.update({
            "EWS_SERVER_URL": "https://192.0.2.1/EWS/Exchange.asmx",  # TEST-NET-1
            "EWS_EMAIL": "smoke@example.invalid",
            "EWS_USERNAME": "smoke", "EWS_PASSWORD": "smoke",
            "DATABASE_URL": dsn,
            "EWSD_HOST": "127.0.0.1", "EWSD_PORT": str(EWSD_PORT),
            "EWSD_API_KEY": EWSD_KEY,
            "SEND_ENABLED": "false",
            "EWS_CAPABILITY_TIER": tier,
            "REQUEST_TIMEOUT": "3", "LOG_LEVEL": "WARNING",
            "DATA_DIR": os.path.join(tmp, "ewsd"),
        })
        mcp_env = dict(os.environ)
        mcp_env.update({
            "EWS_EMAIL": "smoke@example.invalid",
            "DATABASE_URL": dsn,
            "EWSD_URL": EWSD_BASE, "EWSD_API_KEY": EWSD_KEY,
            "MCP_TRANSPORT": "http", "MCP_HOST": "127.0.0.1", "MCP_PORT": str(MCP_PORT),
            "MCP_API_KEY": MCP_KEY,
            "EWS_CAPABILITY_TIER": tier,
            "LOG_LEVEL": "WARNING",
            "DATA_DIR": os.path.join(tmp, "ewsmcp"),
        })

        ewsd_proc = subprocess.Popen(
            [sys.executable, "-m", "ewsmcp.daemon"], env=ewsd_env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        mcp_proc = None
        try:
            if not _wait_livez(EWSD_BASE, 30, ewsd_proc):
                print(f"FAIL: ewsd /livez never came up (exit={ewsd_proc.poll()})")
                return 1

            mcp_proc = subprocess.Popen(
                [sys.executable, "-m", "ewsmcp.main"], env=mcp_env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if not _wait_livez(MCP_BASE, 30, mcp_proc):
                print(f"FAIL: ewsmcp /livez never came up (exit={mcp_proc.poll()})")
                return 1

            status, ready = _req(EWSD_BASE, EWSD_KEY, "GET", "/readyz")
            if status != 503 or ready.get("connection", {}).get("state") != "connecting":
                failures.append(f"ewsd /readyz {status} {ready}")

            status, mready = _req(MCP_BASE, MCP_KEY, "GET", "/readyz")
            if status != 200 or "daemon" not in mready:
                failures.append(f"ewsmcp /readyz {status} {mready}")

            status, st = _req(EWSD_BASE, EWSD_KEY, "POST", "/v1/tools/get_server_status", {})
            if status != 200 or st.get("connection", {}).get("state") != "connecting":
                failures.append(f"get_server_status cold {status} {st}")

            status, msg = _req(EWSD_BASE, EWSD_KEY, "POST",
                               "/v1/tools/search_messages", {"limit": 1})
            if status != 503 or msg.get("error", {}).get("code") != "upstream_unavailable":
                failures.append(f"search cold {status} {msg}")

            if tier == "full":
                status, send = _req(EWSD_BASE, EWSD_KEY, "POST",
                                    "/v1/tools/send_draft", {"draft_id": "d1"})
                if send.get("error", {}).get("code") != "kill_switch":
                    failures.append(f"send_draft expected kill_switch, got {send}")
                _, openapi = _req(EWSD_BASE, EWSD_KEY, "GET", "/openapi.json")
                schema = json.dumps(openapi.get("paths", {}).get("/api/tools/send_draft", {}))
                if "confirm_token" not in schema:
                    failures.append("confirm_token absent from send_draft OpenAPI")

            init_payload = {
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                          "clientInfo": {"name": "smoke", "version": "0"}},
            }
            init_resp = _mcp_req(MCP_BASE, MCP_KEY, init_payload)
            if not init_resp or "result" not in init_resp:
                failures.append(f"mcp initialize failed: {init_resp}")

            list_payload = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
            list_resp = _mcp_req(MCP_BASE, MCP_KEY, list_payload)
            tools = (list_resp or {}).get("result", {}).get("tools", [])
            if tier == "full" and len(tools) != 31:
                failures.append(f"expected 31 mcp tools at tier full, got {len(tools)}")

            if failures:
                print("FAILURES:")
                for f in failures:
                    print(" -", f)
                return 1
            print(f"boot smoke OK (tier={tier}, mcp_tools={len(tools)})")
            return 0
        finally:
            for proc in (mcp_proc, ewsd_proc):
                if proc is None:
                    continue
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    sys.exit(main())
