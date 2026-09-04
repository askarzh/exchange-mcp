# Changelog

Earlier history (the 4.0–4.5 lines) lives in the upstream
[`azizmazrou/ews-mcp`](https://github.com/azizmazrou/ews-mcp) changelog;
this file starts from the point this repository was extracted.

## [5.1.0a1] - 2026-09-04 (pre-release)

Phase 2: the mail archive. `ewsd` now moves old mail onto local disk with its
attachments, proves the copy is good, and — only when explicitly enabled —
removes it from Exchange. Archived mail stays searchable, readable and
attachable through the same tools. Semantic search arrives with it. Design:
`docs/superpowers/specs/2026-09-03-postgres-archive-daemon-design.md`.

### Added
- Schema v3 (`ewsmcp/migrations/003_archive.sql`, `SCHEMA_VERSION = 3`):
  `ews.attachments`, `ews.chunks` (`embedding vector(768)`, HNSW +
  `vector_cosine_ops`) and `ews.archive_runs`, plus
  `messages.captured_changekey`. `CREATE EXTENSION vector` is pinned
  `WITH SCHEMA public` — the `ews` role/schema collision that would
  otherwise land the type in the wrong schema.
- Archive pipeline in `ewsmcp/archive/`: capturer (raw MIME + attachment
  blobs, content-addressed, temp-then-rename, free-space guard, fail-closed
  on an empty `ARCHIVE_FOLDERS`), verifier (changekey + MIME hash + blob
  existence/size, resets to `live` on any mismatch), deleter (three
  independent rails, `captured_changekey` re-checked immediately before
  each delete, per-run cap, one audit record per deletion,
  `deleted_unrecorded` tracking, invariant
  `deleted + failed + remaining == eligible`) and an embed worker, driven
  by `ArchiveRunner` every `ARCHIVE_CYCLE_SECONDS`. `run_once` returns
  `blocked` rather than hanging when a cycle is already in progress.
- Semantic search: `ewsmcp/embeddings.py` (Gemini `gemini-embedding-2` at
  768 dims over plain httpx, batches of 100, backoff on 429/5xx) and
  `ewsmcp/semantic.py` (`SemanticIndex`, cosine search, hybrid RRF k=60).
- Tools: `archive_run` (destructive, tier `full`, `dry_run=false` is
  confirm-gated), `archive_status`, `get_raw_message` (single-use
  capability download URL), `find_similar`. **35 tools at tier full, 29
  at draft, 18 at read** (was 31 / 26 / 15).
- `search_messages` gains `archived` (`any` | `only` | `exclude`) and a
  real `mode="semantic"`; every card carries `archive_state` when it is
  not `live`. `list_folders` rows carry `archived` counts.
  `get_attachment` serves archived mail from the blob store without
  contacting Exchange. `get_raw_message` serves live mail by fetching its
  MIME on demand through Exchange (cached, without changing archive
  state) alongside archived mail from disk.
- `find_similar` and `search_messages(mode="semantic")` are forwarded
  from `ewsmcp` to `ewsd` unconditionally — the MCP process never holds
  `GEMINI_API_KEY`. Without an embedder they behave differently on
  purpose: `search_messages(mode="semantic")` degrades to keyword results
  with `meta.degraded`/`meta.reason` set, while `find_similar` returns a
  `validation` error naming the key (a keyword list is not a semantic
  answer). Both read a capped vector candidate set (`limit * 4` chunks,
  at most 400) before filtering, so a very selective `archived` filter can
  under-fill a page.
- Daemon routes: `POST /v1/archive/run` (alias of
  `POST /v1/tools/archive_run`, same two-phase confirm), `GET
  /v1/archive/runs/<id>`, and `GET /download/<token>` (single-use, ahead
  of the bearer gate like `/upload`, served only from `mime/`/`blobs/`).
  `archive_status` reports Postgres-only counts everywhere; disk figures
  (`blob_store_bytes`, `free_gb`) are `ewsd`-only.
- Settings: `GEMINI_API_KEY`, `EMBED_DIMS`, `ARCHIVE_FOLDERS`,
  `ARCHIVE_AFTER_DAYS`, `ARCHIVE_EXCLUDE_CATEGORIES`, `ARCHIVE_GRACE_DAYS`
  (floored at 1), `ARCHIVE_DELETE_ENABLED`, `ARCHIVE_DELETE_AUTO`,
  `ARCHIVE_MAX_DELETE_PER_RUN`, `ARCHIVE_MIN_FREE_GB`,
  `ARCHIVE_CYCLE_SECONDS`.

### Changed
- A server-side delete of a `captured`/`verified` row no longer drops it: the
  row is kept and marked `deleted`. Live rows are dropped as before.
- The thin MCP process no longer imports `exchangelib` at all: the write
  tools' metadata moved to `ewsmcp/tools/write_specs.py`, `WELL_KNOWN`/
  `paginate` to `ewsmcp/gateway/wellknown.py`, `ANNOTATIONS` to
  `ewsmcp/annotations.py`, the shared ASGI helpers to `ewsmcp/httputil.py`,
  `build_registry` to `ewsmcp/tools/registry.py`, and the `archive`/`cache`
  packages export their workers lazily. Enforced by
  `tests/test_mcp_import_boundary.py` in a subprocess.
- The Gemini API key travels in the `x-goog-api-key` header instead of a
  `?key=` query parameter, which would leak into proxy and access logs.
- Downloads set `content-disposition: attachment; filename="<ascii>";
  filename*=UTF-8''<percent-encoded>` (RFC 5987), so a non-ASCII
  attachment name survives instead of being reduced to its extension.
- Archive workers, `get_server_status` and `/metrics` do their blocking
  store queries and file hashing on worker threads rather than the event
  loop.

### Safety
- Deletion is off by default (`ARCHIVE_DELETE_ENABLED=false`) and stays off
  until an operator flips it deliberately. Capture, verify and embed run
  regardless — only the Exchange-side hard delete is gated.
- Deletion is also MANUAL by default: the background cycle skips the delete
  lane unless `ARCHIVE_DELETE_AUTO=true` on top of
  `ARCHIVE_DELETE_ENABLED`. Left at the defaults, mail leaves Exchange only
  through a confirmed `archive_run(kind="delete", dry_run=false)`.
- The deleter re-reads the archive copy FROM DISK immediately before each
  batch (MIME hash + every attachment blob's existence and size), since a
  `verified` row is at least `ARCHIVE_GRACE_DAYS` old by the time it is
  deleted. A row whose copy is missing or corrupt is never deleted and is
  demoted `verified → captured` for the verifier to re-check.
- `get_raw_message` is no longer cold-gated: archived mail is served from
  disk while Exchange is warming up (the live-mail branch still refuses
  with `upstream_unavailable`).

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
