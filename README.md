# exchange-mcp — Exchange (EWS) as a safe, fast MCP tool surface

A Postgres-backed, two-process Exchange server: `ewsd` owns the Exchange
session, the sync mirror, uploads, the audit chain, and every safety
gate; `ewsmcp` is a thin MCP server that reads Postgres directly for
fast lookups and forwards everything else to `ewsd`. **31 tools**,
alias-only ids, token-lean DTOs, a Postgres mirror with full-text
search, and a two-phase confirm flow that makes autonomous sending
tamper-evident. This is a personal fork-off of
[`azizmazrou/ews-mcp`](https://github.com/azizmazrou/ews-mcp) 4.5,
released under the same MIT license.

> The release line is **5.0.x** (pre-release, `5.0.0a1`). Architecture:
> [DESIGN.md](DESIGN.md). Full API reference: [docs/API.md](docs/API.md).

## Quick start

```bash
git clone https://github.com/askarzh/exchange-mcp && cd exchange-mcp
```

**1. Postgres.** Either point at one you already run, or bring up the
dev stack's `postgres` service:

```bash
cp .env.example .env   # then edit it — see below
docker compose up postgres
```

`DATABASE_URL` (`postgresql://user:pass@host:5432/ews`) is required by
both processes below.

**2. `ewsd`** — the daemon. It needs Exchange credentials and
`DATABASE_URL`; it owns the only Exchange session and runs the sync
engine, uploads, audit log and the entire gate chain:

```bash
pip install .
EWS_SERVER_URL="https://mail.example.com/EWS/Exchange.asmx" \
EWS_EMAIL="user@example.com" EWS_USERNAME="user" EWS_PASSWORD="…" \
DATABASE_URL="postgresql://ews:change-me@127.0.0.1:5432/ews" \
EWSD_API_KEY="generate-a-long-random-string" \
  ewsd
```

By default it listens on `127.0.0.1:8790` (`EWSD_HOST`/`EWSD_PORT`).

**3. `ewsmcp`** over stdio — for Claude Code, Claude Desktop, or any
other MCP client. It does **not** need Exchange credentials, only
`DATABASE_URL` (to read the mirror) and `EWSD_URL` + `EWSD_API_KEY` (to
reach the daemon for writes and `fresh=true` reads):

```bash
claude mcp add exchange \
  -e DATABASE_URL="postgresql://ews:change-me@127.0.0.1:5432/ews" \
  -e EWSD_URL="http://127.0.0.1:8790" \
  -e EWSD_API_KEY="generate-a-long-random-string" \
  -- /absolute/path/to/.venv/bin/ewsmcp
```

Claude Desktop and other clients: same command + env in a config
block. Prefer a file? Copy [`.env.example`](.env.example) to `.env` in
the directory you launch from — it auto-loads for both `ewsd` and
`ewsmcp`.

That is the whole setup. The defaults are safe: capability tier `draft`
(26 read + draft tools registered; nothing can send), `SEND_ENABLED=false`
until you flip it, and `ewsd`'s mail-at-rest (audit chain; local blob
storage in later phases) goes to `~/.ewsmcp`. `ewsd` **refuses
cloud-synced folders** for that data — if your home directory lives in
OneDrive/Dropbox/iCloud, set `DATA_DIR` to a plain local path.

**First things to try in a chat:**

- "What's in my inbox this morning?" → `get_mailbox_overview`
- "Find the last message from the finance team" → `search_messages`
- "Show me that whole conversation" → `get_thread`
- "Draft a short reply to m3 saying I'll confirm tomorrow" →
  `create_draft` (saved as a draft, never sent)
- "What's on my calendar this week?" → `list_events`

Ids like `m3` / `e1` are the server's short aliases — the assistant uses
them exactly as returned; raw Exchange ids never appear. (5.0 mints
fresh aliases on first sync; ids from a prior 4.5 mailbox do not carry
over — see DESIGN.md §Ids.)

**4. HTTP mode** — when `ewsmcp` runs where the client isn't (a home
server, claude.ai connector, etc.):

```bash
MCP_TRANSPORT=http MCP_PORT=8000 MCP_API_KEY=<long-random-string> ewsmcp
```

- MCP endpoint: `http://host:8000/mcp` (Streamable HTTP, the modern
  replacement for SSE). Clients that only speak stdio can bridge:
  `npx mcp-remote http://host:8000/mcp --header "Authorization: Bearer <key>"`.
- Health: `GET /livez`, `/readyz`, `/health`, `/version`.
- `ewsmcp` in HTTP mode still needs `DATABASE_URL` + `EWSD_URL` +
  `EWSD_API_KEY`; it never talks to Exchange itself.

`ewsd`'s own HTTP surface (`GET /v1/tools`, `POST /v1/tools/<name>`,
`GET /v1/status`, `/metrics`, `/openapi.json` behind `EWSD_API_KEY`;
`/livez`, `/readyz`, `/health`, `/version` always public; `PUT|POST
/upload/<token>` deliberately ahead of the bearer gate) is separate — it
is what `ewsmcp` calls for writes, not something a client talks to
directly. See DESIGN.md §Processes.

Full dev stack (Postgres + `ewsd` + `ewsmcp` in HTTP mode), see
[`docker-compose.yml`](docker-compose.yml):

```bash
cp .env.example .env   # fill in EWS_* creds, PGPASSWORD, EWSD_API_KEY, MCP_API_KEY
docker compose up
```

## Why it looks like this

- **Token economy.** One legacy detail call shipped 115 kB of duplicated
  raw HTML for a 150-char message. Here a search result is a ~60-token
  card, bodies are cleaned once at sync time (bilingual quoted-history +
  signature stripping), and raw HTML requires an explicit flag.
- **Ids the model can actually copy.** Raw EWS ids are ~150 chars of
  case-sensitive base64 that change when items move. Tools emit short
  aliases (`m12`, `e3`) that survive moves and restarts.
- **Safety by declaration, one process.** Handlers contain zero policy;
  ONE dispatcher chain, running only in `ewsd`, enforces kill-switch →
  tier → recipient guard → content-bound two-phase confirm → rate cap.
  `ewsmcp` forwards writes to it verbatim. Defaults are safe: sends
  disabled, draft tier.
- **Cache-first reads.** `ewsd`'s background delta-sync (native EWS
  `SyncFolderItems`) keeps a Postgres mirror (schema `ews`) warm;
  `ewsmcp` answers reads from it in milliseconds with
  `{"source": "cache", "as_of": …}` provenance and forwards to `ewsd`'s
  live route when a caller passes `fresh=true`. Search runs on a
  generated `tsvector` column (Postgres `simple` config); accent folding
  happens in Python before indexing, so "café" finds "cafe" and back.
- **Never-exit boot.** Transports bind before any Exchange contact;
  `/livez` is up immediately, `/readyz` reports the warmup honestly, and
  the connection manager owns recovery.

## Configuration (env)

`ewsd` needs the Exchange block; `ewsmcp` does not (it only reads
Postgres and calls `ewsd`). Both need `DATABASE_URL`. See
[`.env.example`](.env.example) for an annotated template and
[`docker-compose.yml`](docker-compose.yml) for the dev stack's wiring.

| Variable | Default | Meaning |
|---|---|---|
| `EWS_SERVER_URL` / `EWS_EMAIL` / `EWS_USERNAME` / `EWS_PASSWORD` | — | Exchange endpoint + credentials (auth auto-negotiation; never pinned). Required by `ewsd`; unused by `ewsmcp` |
| `DATABASE_URL` | — | `postgresql://user:pass@host:5432/ews` — required by both processes |
| `EWSD_HOST` / `EWSD_PORT` | `127.0.0.1` / `8790` | Where `ewsd` serves its HTTP API |
| `EWSD_API_KEY` | — | Bearer `ewsmcp` presents to `ewsd`; required off-loopback |
| `EWSD_URL` | `http://127.0.0.1:8790` | `ewsmcp`'s base URL for the daemon |
| `EXTERNAL_URL` | `` (empty) | Public base URL that reaches `ewsd` — used to build the absolute capability URL returned by `create_upload_link`; the proxy must route `/upload/*` to `ewsd`:8790. Empty → the tool returns a relative `/upload/<token>` |
| `EWS_CAPABILITY_TIER` | `draft` | `read` ⊂ `draft` ⊂ `full` — above-tier tools are unregistered in `ewsmcp` AND refused by `ewsd`'s gate chain |
| `SEND_ENABLED` | `false` | Global send kill-switch (blocks every send-class tool), enforced in `ewsd` |
| `EWS_RECIPIENT_ALLOWLIST` / `EWS_RECIPIENT_DENYLIST` | — | Glob lists enforced on argument-borne AND draft-resolved recipients |
| `EWS_MAX_SENDS_PER_HOUR` | `10` | Send rate cap |
| `SEND_CONFIRM_SECRET` | per-process | HMAC secret for confirm tokens (set it to survive `ewsd` restarts) |
| `CONFIRM_TTL_SECONDS` | `600` | Confirm token lifetime |
| `MCP_TRANSPORT` / `MCP_HOST` / `MCP_PORT` / `MCP_API_KEY` | stdio | `ewsmcp` HTTP serving + bearer auth (all unused in stdio mode) |
| `DATA_DIR` | `~/.ewsmcp` | `ewsd`'s local mail-at-rest (audit chain). Absolute; cloud-synced paths are refused (`DATA_DIR_ALLOW_SYNCED=true` to override) |
| `SHARED_DIR` | — | Optional shared-space root for saved attachments (see `add_attachment`/`get_attachment`) |
| `EWS_CACHE_FOLDERS` | `inbox,sent` | Delta-synced folders |
| `EWS_CACHE_SYNC_SECONDS` | `45` | Delta cadence |
| `EWS_CACHE_HIERARCHY_SECONDS` | `600` | Folder tree / calendar / tasks refresh cadence |
| `EWS_CACHE_WINDOW_DAYS` | `365` | Mirror backfill window |
| `EWS_TZ` | `Asia/Riyadh` | Server timezone for date grammar + display |

Removed from the 4.5 line and no longer read: `EWS_CACHE_ENABLED`,
`EWS_CACHE_PURGE_ON_BOOT`, `EWS_SEMANTIC_INDEX`, `EWS_SEMANTIC_PG_DSN`,
`EWS_SEMANTIC_OLLAMA_URL`, `EWS_SEMANTIC_MODEL` — the mirror is always
on (Postgres, not an opt-in SQLite file) and semantic search is Phase 2
work, not an optional tier.

## The send flow (two-phase, content-bound)

```text
create_draft(mode="reply", reply_to="m12", body="…")
  → {draft_id: "d1", preview, note: "saved as draft — NOT sent"}
send_draft(draft_id="d1")
  → phase 1: fetches the draft, returns its REAL recipients/subject/body
    snippet + confirm_token bound to that content (nothing sent)
send_draft(draft_id="d1", confirm_token="…")
  → phase 2: REFETCHES the draft, verifies the content still matches,
    sends once (tokens are single-use; editing the draft in between
    invalidates the token)
```

## Health & operations

`ewsmcp`: `GET /livez` (process up), `GET /readyz` (honest 503 while
warming), `GET /health` (tool count), `GET /version`. `ewsd`: `GET
/livez`, `GET /readyz` (Exchange connection state), `GET /health`,
`GET /version` — all always public, no `EWSD_API_KEY` needed; `GET
/v1/status` and `GET /metrics` (Prometheus) require it. `get_server_status`
tool (answered by `ewsmcp` — tier, alias stats, cache stats, plus
`ewsd`'s own status merged in, or `daemon.reachable: false` if `ewsd` is
down — works while cold and over stdio too). Audit chain:
`python scripts/verify_audit_chain.py $DATA_DIR/audit` (run against
`ewsd`'s `DATA_DIR`).

## Development

```bash
pip install -e .[dev]
python -m pytest tests -q          # the full suite (skips Postgres-only tests without Docker)
python -m ruff check .
python scripts/boot_smoke.py full  # boots both ewsd + ewsmcp against a dead Exchange endpoint
python scripts/dump_tool_table.py --check   # docs vs registry drift gate
```

## Example assistant skill

`examples/skills/exchange-assistant/` shows how a Claude skill composes
this tool surface (morning overview → triage → reply-draft with the
two-phase confirm). It is deliberately generic — judgment lives in the
calling assistant, the server stays a data plane.

## License

MIT — see [LICENSE](LICENSE).
