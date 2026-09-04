# exchange-mcp — Exchange (EWS) as a safe, fast MCP tool surface

A Postgres-backed, two-process Exchange server: `ewsd` owns the Exchange
session, the sync mirror, uploads, the audit chain, and every safety
gate; `ewsmcp` is a thin MCP server that reads Postgres directly for
fast lookups and forwards everything else to `ewsd`. **35 tools**,
alias-only ids, token-lean DTOs, a Postgres mirror with full-text and
semantic search, a durable mail archive with attachments, and a
two-phase confirm flow that makes autonomous sending (and deleting)
tamper-evident. This is a personal fork-off of
[`azizmazrou/ews-mcp`](https://github.com/azizmazrou/ews-mcp) 4.5,
released under the same MIT license.

> The release line is **5.1.x** (pre-release, `5.1.0a1`). Architecture:
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
(29 read + draft tools registered; nothing can send), `SEND_ENABLED=false`
until you flip it, `ARCHIVE_DELETE_ENABLED=false` until you flip that too
(the archive still captures, verifies and embeds mail with it off — only
the Exchange delete is gated), and `ewsd`'s mail-at-rest (audit chain,
archived MIME + attachment blobs) goes to `~/.ewsmcp`. `ewsd` **refuses
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
them exactly as returned; raw Exchange ids never appear.

**Archive and semantic search.** `ewsd` also runs a background archive
pipeline: old mail (default: inbox/sent older than 180 days) is captured
to local disk with its attachments, verified, embedded and — only once
you explicitly set `ARCHIVE_DELETE_ENABLED=true` — deleted from Exchange.
Archived mail stays fully readable and searchable through the same tools
(`search_messages`'s `archived` argument selects `any`/`only`/`exclude`;
`list_folders` rows carry an `archived` count; `get_attachment` serves
archived attachments straight from disk). Four tools cover it:
`archive_run` (dry-run by default; `dry_run=false` is two-phase
confirmed, same as `send_draft`), `archive_status`, `get_raw_message`
(a single-use download link for the original .eml) and `find_similar`
(meaning-based search). `search_messages(mode="semantic")` and
`find_similar` both need `GEMINI_API_KEY` set on `ewsd` — `ewsmcp` never
holds that key, so both are always forwarded to the daemon, and both
degrade to keyword results (`meta.degraded=true`) rather than fail when
no key is configured. **Rollout default:** capture, verify and embed run
continuously out of the box; deletion from Exchange stays off until you
flip `ARCHIVE_DELETE_ENABLED=true` — run `archive_status` first to see
what a real pass would touch.

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
`GET /v1/status`, `/metrics`, `/openapi.json`, `POST /v1/archive/run`
(alias of `POST /v1/tools/archive_run`) and `GET /v1/archive/runs/<id>`
behind `EWSD_API_KEY`; `/livez`, `/readyz`, `/health`, `/version` always
public; `PUT|POST /upload/<token>` and `GET /download/<token>`
deliberately ahead of the bearer gate, single-use, served only out of
`{DATA_DIR}/mime/` and `{DATA_DIR}/blobs/`) is separate — it is what
`ewsmcp` calls for writes, not something a client talks to directly. See
DESIGN.md §Processes.

Full dev stack (Postgres + `ewsd` + `ewsmcp` in HTTP mode), see
[`docker-compose.yml`](docker-compose.yml):

```bash
cp .env.example .env   # fill in EWS_* creds, PGPASSWORD, EWSD_API_KEY, MCP_API_KEY
docker compose up
```

## Why it looks like this

- **Token economy.** One legacy detail call shipped 115 kB of duplicated
  raw HTML for a 150-char message. Here a search result is a ~60-token
  card, bodies are cleaned once at sync time (quoted-history +
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
  happens in the database via an `unaccent`-backed wrapper, so "café"
  finds "cafe" and back.
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
| `EWS_MIRROR_EXCLUDE` | `drafts,junk,trash,outbox` | Well-known folders NOT mirrored; every other mail folder is mirrored in full. Exclusion is by well-known key only — sub-folders of an excluded folder are still mirrored. |
| `EWS_CACHE_SYNC_SECONDS` | `45` | Delta cadence |
| `EWS_CACHE_HIERARCHY_SECONDS` | `600` | Folder tree / calendar / tasks refresh cadence. The folder walk clears exchangelib's cached tree and re-fetches it this often (not every cycle), so a new or deleted folder and fresh unread counts show up within this window while mail keeps syncing every `EWS_CACHE_SYNC_SECONDS`. |
| `EWS_TZ` | `Asia/Riyadh` | Server timezone for date grammar + display |
| `GEMINI_API_KEY` | — | **Daemon-only** — `ewsmcp` never holds it; `find_similar` and `mode="semantic"` are always forwarded to `ewsd`. Semantic search stays keyword-only (`semantic_enabled()` is `False`, results carry `meta.degraded=true`) until this is set |
| `EMBED_DIMS` | `768` | Fixed by migration 003's `vector(768)` column; any other value fails to boot |
| `ARCHIVE_FOLDERS` | `inbox,sent` | Well-known folder keys the archive pipeline captures; never calendar/contacts/tasks. Fail-closed: set to empty and the pipeline captures NOTHING, not every folder |
| `ARCHIVE_AFTER_DAYS` | `180` | Age cutoff (days) before a message becomes eligible for archival |
| `ARCHIVE_EXCLUDE_CATEGORIES` | — | Comma-separated categories excluded from archival |
| `ARCHIVE_GRACE_DAYS` | `7` | Extra days past the cutoff a `verified` row must age before it is eligible for deletion; floored at 1 |
| `ARCHIVE_DELETE_ENABLED` | `false` | **The rollout switch.** Capture, verify and embed run regardless; this is rail 1 of 3 for the Exchange delete itself — OFF by default, deletion also needs tier `full` (plus a confirm token) and the per-run cap |
| `ARCHIVE_MAX_DELETE_PER_RUN` | `200` | Deletion rail 2 of 3 — per-run cap |
| `ARCHIVE_MIN_FREE_GB` | `2.0` | Minimum free disk space required before each archive batch |
| `ARCHIVE_CYCLE_SECONDS` | `300` | Archive pipeline run cadence |

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
