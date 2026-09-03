# Changelog

Earlier history (the 4.0–4.5 lines) lives in the upstream
[`azizmazrou/ews-mcp`](https://github.com/azizmazrou/ews-mcp) changelog;
this file starts from the point this repository was extracted.

## [5.0.0a1] - 2026-09-03 (pre-release, Phase 1.5 simplification)

Amends the Postgres port below, still within the `5.0.0a1` pre-release:
every mail folder is now mirrored (no time window; `EWS_MIRROR_EXCLUDE`
replaces `EWS_CACHE_FOLDERS`/`EWS_CACHE_WINDOW_DAYS`), `messages.folder`
became `messages.folder_id` (the folder's EWS id), full-text search moved
from the Python NFKD fold over `norm_text` to a database-side
`ews.immutable_unaccent()` wrapper over the Postgres `unaccent`
extension (`normalize.py` and `norm_text` are gone), and the learned
sender-signature tier (`sender_sigs`, `trailing_block`, `SIG_MIN_HITS`)
is removed — quote/signature stripping in `bodyclean` stays. The Exchange
live-search fallback in `search_messages` is gone: it now answers only
from the mirror. See `docs/superpowers/specs/2026-09-03-phase1.5-simplification-design.md`
for the full decision record.

## [5.0.0a1] - 2026-09-03 (pre-release)

This line moves off SQLite onto Postgres and splits into two processes:
`ewsd` (daemon — owns the Exchange session, the sync mirror, uploads,
the audit chain, and the entire safety gate chain) and `ewsmcp` (thin
MCP — reads Postgres directly for fast lookups, forwards everything
else to `ewsd` over HTTP). Full reference: `docs/API.md`; architecture:
`DESIGN.md`; design rationale:
`docs/superpowers/specs/2026-09-03-postgres-archive-daemon-design.md`.

### Added
- Postgres store (schema `ews`): messages, events, tasks, folders,
  aliases and sync state now live in one Postgres database instead of a
  per-mailbox SQLite file. Full-text search runs on a generated
  `tsvector` column (`simple` text-search config, GIN index) over
  `norm_text`.
- `ewsd`/`ewsmcp` process split: `ewsd` exposes an HTTP API on
  `EWSD_HOST:EWSD_PORT` — `GET /v1/tools`, `POST /v1/tools/<name>`,
  `GET /v1/status`, `/metrics`, `/openapi.json` behind `EWSD_API_KEY`;
  `GET /livez`, `/readyz`, `/health`, `/version` always public;
  `PUT|POST /upload/<token>` deliberately ahead of the bearer gate (the
  single-use token is the credential). Any number of `ewsmcp` instances
  (stdio or Streamable HTTP `/mcp`) can share it.
- Settings added: `DATABASE_URL`, `EWSD_HOST`, `EWSD_PORT`,
  `EWSD_API_KEY`, `EWSD_URL`.

### Changed
- Accent/diacritic folding moved from a bespoke Arabic normalizer to a
  generic Python NFKD fold (`ewsmcp/normalize.py`) applied before
  indexing; Postgres does no folding of its own (no `unaccent`
  extension).
- **Breaking:** alias ids are re-minted from an empty table on first
  sync — there is no data migration from the 4.5 SQLite mirror. Ids from
  a 4.5 deployment (`m12`-style) stop resolving once; a client hitting
  one gets the usual stale-alias hint and a fresh id from its next
  search.

### Removed
- Arabic-specific normalization (`normalize_ar()`, the bidi/tatweel/
  alef-hamza/teh-marbuta folding and its gate suite) — replaced by the
  generic accent fold above.
- The optional pgvector/Ollama semantic tier: `find_similar` is
  unregistered and `search_messages(mode="semantic")` is a validation
  error pending Phase 2 (Gemini-embedding-backed semantic search; see
  the design spec).
- Settings removed: `EWS_CACHE_ENABLED`, `EWS_CACHE_PURGE_ON_BOOT`,
  `EWS_SEMANTIC_INDEX`, `EWS_SEMANTIC_PG_DSN`, `EWS_SEMANTIC_OLLAMA_URL`,
  `EWS_SEMANTIC_MODEL`.
- `docker-compose.nas.yml` (NAS-specific compose stack) — the dev stack
  (`docker-compose.yml`) covers local/LAN Postgres deployments.
- Legacy helper scripts (`run.sh`, `scripts/build.sh`,
  `scripts/deploy.sh`, `scripts/setup.sh`,
  `scripts/setup-basic-auth.sh`) and redundant configuration examples
  (`.env.basic.example`, `.env.oauth2.example`, `.env.ai.example`).

### Documentation
- Repository-wide revamp: the root README is now a single front door
  (version guide, stdio-first quick start); `docs/README.md` maps all
  documentation; the ten 4.0-era documents moved to `docs/legacy/` with
  banners; `docs/API.md` gained a generated per-tool parameter
  reference (`dump_tool_table.py` now emits and drift-checks it).

Removed from the 4.5 line and no longer read: `EWS_CACHE_ENABLED`,
`EWS_CACHE_PURGE_ON_BOOT`, `EWS_SEMANTIC_INDEX`, `EWS_SEMANTIC_PG_DSN`,
`EWS_SEMANTIC_OLLAMA_URL`, `EWS_SEMANTIC_MODEL` — the mirror is always
on (Postgres, not an opt-in SQLite file) and semantic search is Phase 2
work, not an optional tier.

## Historical: the v3 → 5.0 tool surface

Kept here rather than in the API reference: it describes what the 67-tool
v3 surface became, not what the server does today.

| v3 (67-tool surface) | 5.0 |
|---|---|
| `read_emails` / `search_emails` / `advanced_search` | `search_messages` |
| `get_email_details` | `get_message` |
| `get_thread` / `search_by_conversation` | `get_thread` |
| `list_folders` / `get_folder_tree` | `list_folders` |
| `read_attachment` / `download_attachment` | `get_attachment` |
| `get_calendar` / `list_appointments` | `list_events` |
| `get_appointment_details` | `get_event` |
| `check_availability` / `find_meeting_slots` | `check_availability` |
| `find_person` / `search_gal` / `list_contacts` | `find_people` |
| `get_person_details` | `get_contact` |
| `get_oof_settings` / `oof_settings(action=get)` | `get_oof_settings` |
| `oof_settings(action=set)` | `set_oof` |
| `whoami` / `get_server_info` | `get_server_status` |
| `create_draft` / `create_reply_draft` / `create_forward_draft` | `create_draft` (modes) |
| `update_draft` | `update_draft` |
| `send_draft` | `send_draft` (content-bound two-phase) |
| `send_email` / `reply_email` / `forward_email` | **removed** — draft-first only |
| `update_email` / `mark_read` / `update_messages` | `update_messages` (bulk) |
| `move_email` / `move_messages` | `move_messages` (bulk) |
| `delete_email` / `delete_messages` | `delete_messages` (bulk) |
| `create_appointment` | `create_event` |
| `update_appointment` | `update_event` |
| `respond_to_meeting` | `respond_to_event` |
| `delete_appointment` | `cancel_event` |
| `get_tasks` | `list_tasks` |
| `update_task` / `complete_task` | `update_task` |
| — (new) | `get_mailbox_overview`, `waiting_on` |
| — (Phase 2, not yet built) | a similarity-search tool |

### Intentionally dropped vs v3

Removed deliberately — most belong to the calling assistant (skills), not
a data-plane server:

- **One-shot send tools** (`send_email`, `reply_email`, `forward_email`):
  the ONLY way mail leaves the mailbox is `create_draft` → `send_draft`.
- **Impersonation / delegated mailboxes** (`target_mailbox` everywhere):
  one server = one mailbox.
- **OAuth2/MSAL flows**: the target deployment is on-prem Exchange with
  auto-negotiated auth; a Graph/OAuth backend would be a different
  gateway, not a flag.
- **Contacts folder management** (create/update/delete contacts).
- **Folder management** (create/rename/delete folders).
- **MIME export** and raw-content endpoints.
- **The agent-secretary stack** (server-side classify/summarize/brief/
  voice/commitments/approval queue): the caller already IS an LLM;
  `examples/skills/exchange-assistant/` shows the skill-side pattern.
- **Inbox rules tools**: prefer real server-side Exchange rules
  (revisit after an exchangelib ≥5.2 bump).
