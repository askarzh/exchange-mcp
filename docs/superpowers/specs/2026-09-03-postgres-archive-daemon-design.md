# ews-mcp 5.0: Postgres store, mail archive, daemon/MCP split

Date: 2026-09-03
Status: approved design, not yet planned

## Problem

The Exchange mailbox is near its quota. The weight is attachment bytes, not
message count (about 2,400 mail items, a 1.6 MB text mirror). The user wants to
move old mail off the server while keeping the ability to search, read, and
work with it through the MCP, including attachments and originals.

The 4.5 line already has most of the machinery: an incremental SyncFolderItems
engine, a SQLite mirror with FTS5, an optional pgvector index, and hybrid
RRF search. What it lacks is a durable, attachment-inclusive archive with a
safe path to deletion, and a process model that allows several MCP clients
against one Exchange session.

## Decisions taken

| Question | Decision |
|---|---|
| What happens on Exchange after archive | Hard delete, after verification and a grace period |
| Storage | Postgres becomes the only database. SQLite is removed. |
| Process model | Daemon owns Exchange and all workers; MCP is a thin reader of Postgres plus an HTTP client of the daemon (the Telegram MCP pattern) |
| Embeddings | gemini-embedding-2, 768 dims, remote API |
| Arabic support | Removed from the new line entirely |
| Environment | Postgres on the same host or LAN; plenty of local disk; no local embedder; not the NAS compose stack |
| Repo | Lands inside `v5/` as the 5.0 line, not a new repository |

## 1. Process architecture

Two processes, one Postgres.

**`ewsd`, the daemon.** Runs once, always on. Owns the only Exchange session:
the existing gateway, connection manager, and SyncEngine, plus three new
workers (capturer, verifier/deleter, embedder). Exposes an authenticated HTTP
API for the MCP. Keeps the hash-chained audit log, so every send, delete, and
archive deletion is recorded by one process.

**`ewsmcp`, the MCP server.** Reads Postgres directly for search, get, list,
and overview. Calls the daemon over HTTP for anything that changes Exchange or
must bypass the mirror. Runs as stdio (Claude Code, Claude Desktop) or HTTP
(claude.ai connector), any number of instances at once. Does not import
exchangelib.

**Shared package.** Settings, schema and migrations, DTOs, alias resolution,
and the body cleaner live in a module both processes import.

**Safety split.** The gate chain in `tools/base.py` (tier, kill switch,
recipient allow/deny lists, rate guard, confirm tokens) runs in the MCP. The
daemon independently re-checks tier and kill switch, so a direct call to the
daemon cannot bypass safety.

**Deployment.** One compose file with three services: `postgres`
(`pgvector/pgvector:pg16`), `ewsd`, `ewsmcp` in HTTP mode. Only `ewsd` holds
Exchange credentials. For stdio use, `ewsmcp` runs on the host with a Postgres
DSN and the daemon URL in its environment.

**Layout.**

```
v5/ewsmcp/shared/    settings, db (schema, migrations, upsert helpers), dto, ids, bodyclean
v5/ewsmcp/daemon/    gateway, connection, sync, archive (capture, verify, delete), embed, api
v5/ewsmcp/mcp/       tools (read: SQL; write: daemon client), server, http
console scripts:     ewsd, ewsmcp
```

## 2. Data model

Schema `ews`. Archive is a state on the message row, not a separate table, so
live and archived mail are searched by one query and aliases need no change.

**`messages`.** All existing mirror columns (`ews_id` PK, `changekey`,
`folder`, `conversation_id`, sender fields, `to_json`, `subject`, `date_ts`,
`date_iso`, `is_read`, `has_attachments`, `importance`, `categories_json`,
`body_clean`, `internet_message_id`), plus:

- `archive_state` text: `live`, `captured`, `verified`, `deleted`
- `archived_at`, `verified_at`, `deleted_at` timestamptz
- `mime_sha256` text, `mime_path` text
- `embedded_at` timestamptz
- `search_tsv` tsvector, stored generated column:
  `to_tsvector('simple', unaccent(lower(coalesce(subject,'') || ' ' || coalesce(sender_name,'') || ' ' || coalesce(sender_email,'') || ' ' || coalesce(body_clean,''))))`
  with a GIN index. Ranking uses `ts_rank_cd`; prefix queries use `to_tsquery` with `:*`.

Rows for deleted messages are never dropped; `ews_id` stays the stable key.

**`attachments`.** `id` serial, `message_ews_id` FK, `name`, `content_type`,
`size`, `sha256`, `is_inline`, `name_tsv` generated tsvector. Blobs live at
`{DATA_DIR}/blobs/<sha256[:2]>/<sha256>`, deduplicated across messages.

**Raw MIME.** Not a column. File at `{DATA_DIR}/mime/<sha256>.eml`, referenced
by `messages.mime_path`.

**`chunks`.** `message_ews_id` FK, `seq`, `source` (`body` or
`attachment:<sha256>`), `text`, `embedding vector(768)`. HNSW index, cosine
distance. Replaces the 4.5 `ews.chunks`.

**`events`, `tasks`, `folders`, `sync_state`, `sender_sigs`, `meta`.**
Straight ports of the SQLite tables.

**`aliases`.** The table from `ids.py`, ported. Same alias grammar
(`^[a-z]{1,2}[0-9]+$`).

**`archive_runs`.** `id`, `kind` (`capture`, `verify`, `delete`), `dry_run`,
`policy_json`, `started_at`, `finished_at`, `captured`, `verified`, `deleted`,
`failed`, `error`, `sample_json`. The human-readable history of what happened
to the mailbox.

**Migrations.** Numbered SQL files under `shared/db/migrations/`, applied by
the daemon at startup, version recorded in `meta`. The MCP refuses to start if
the schema version is older than it expects. No Alembic.

**Migration from 4.5.** None. The daemon starts empty, sync tokens reset, and
the mirror rebuilds from Exchange. Aliases are re-minted; old short ids stop
resolving once.

**Full-text search config.** Postgres `simple` with `unaccent`, no stemming,
no custom normaliser. `normalize.py` and `test_arabic_search.py` are removed.
`bodyclean.py` keeps quote and signature stripping; Arabic patterns and bidi
handling are removed.

## 3. Archive pipeline

Three idempotent workers in the daemon, each driven by row state.

**Capturer.** Selects `live` rows matching the policy: folder in
`ARCHIVE_FOLDERS`, `date_ts` before the cutoff, not flagged, category not in
`ARCHIVE_EXCLUDE_CATEGORIES`. For each, fetches `mime_content` and all
attachments in one call, writes the MIME file and blobs (temp name, rename
after hash check), fills `attachments`, sets `mime_sha256` and
`archive_state = captured`. Batches of 25 through the existing circuit
breaker. One failed item is logged and skipped.

**Verifier.** For `captured` rows on a later cycle: the item still exists with
the same changekey, the MIME file hashes to `mime_sha256`, every attachment
blob exists with the recorded size. All pass: `verified`. Any failure: reset
to `live` with the reason in `archive_runs`, so capture retries.

**Deleter.** Takes `verified` rows older than cutoff plus `ARCHIVE_GRACE_DAYS`
(default 7), hard-deletes from Exchange in batches, sets `deleted`, writes an
audit record. Off unless `ARCHIVE_DELETE_ENABLED=true`. At most
`ARCHIVE_MAX_DELETE_PER_RUN` (default 200) per run.

**Policy settings.** `ARCHIVE_FOLDERS` (default `inbox,sent`),
`ARCHIVE_AFTER_DAYS` (default 180), `ARCHIVE_EXCLUDE_CATEGORIES`,
`ARCHIVE_GRACE_DAYS`, `ARCHIVE_DELETE_ENABLED`, `ARCHIVE_MAX_DELETE_PER_RUN`,
`ARCHIVE_MIN_FREE_GB`. Calendar, contacts, and tasks are never archived.

**Sync interaction.** The SyncEngine keeps syncing archived folders. On a
server-side delete event: if the row is `deleted`, ignore; if `captured` or
`verified` (deleted by hand in Outlook), keep the row and mark `deleted`; if
`live`, drop the row as today.

**Embedder.** Runs over all messages with `embedded_at IS NULL`, live and
archived. Chunks `body_clean` into 1,500-character pieces, embeds with
gemini-embedding-2 at 768 dims in batches of 100, backoff on 429, sets
`embedded_at`. Attachment text embedding is a later phase; the `source` column
leaves room for it.

## 4. Tool surface and daemon API

**Read tools** keep names and envelopes; the MCP answers them from Postgres.

- `search_messages` gains `archived` = `any` | `only` | `exclude` (default
  `any`). `mode` keeps `keyword` and `semantic`. Every card carries
  `archive_state`.
- `get_message`, `get_thread` are unchanged for archived mail.
- `get_attachment` on an archived message serves from the blob store;
  `mode="save"` and shared-space publishing unchanged.
- `list_folders` adds an `archived` count per folder.
- `find_similar` is always registered.
- `fresh=true` on reads is forwarded to the daemon's live route.

**New tools.**

- `archive_run(dry_run=true, before=None, folders=None)`: tier `full`;
  `dry_run=false` is confirm-gated with a token from the dry run.
- `archive_status()`: counts per state, last runs, blob store size, embedding
  backlog. Read tier.
- `get_raw_message(id)`: returns a capability download URL for the MIME file.

**Write tools** keep names and schemas. The MCP runs the gate chain, then
`POST /v1/tools/<name>` on the daemon with resolved arguments and returns the
daemon's envelope. Confirm tokens for send are minted and checked in the MCP.

**Daemon API.** Bearer `EWSD_API_KEY`.

| Route | Purpose |
|---|---|
| `POST /v1/tools/<name>` | write tool execution |
| `POST /v1/archive/run` | start a capture, verify, or delete pass |
| `GET /v1/archive/runs/<id>` | run status |
| `GET /v1/live/<tool>` | `fresh=true` reads that bypass the mirror |
| `PUT /upload/<token>` | unchanged |
| `GET /download/<token>` | raw MIME and large attachments |
| `GET /livez`, `/readyz`, `/metrics` | unchanged |

**Settings removed.** `EWS_SEMANTIC_INDEX`, `EWS_SEMANTIC_PG_DSN`,
`EWS_SEMANTIC_OLLAMA_URL`, `EWS_SEMANTIC_MODEL`, `EWS_CACHE_ENABLED`.
**Settings added.** `DATABASE_URL`, `EWSD_URL`, `EWSD_API_KEY`,
`GEMINI_API_KEY`, `EMBED_DIMS` (default 768), the `ARCHIVE_*` set above.

## 5. Error handling, safety, testing

**Failure modes.**

- Postgres down: MCP read tools return `backend_unavailable`; write tools
  still work through the daemon. Daemon pauses workers and retries with
  backoff; nothing is buffered in memory.
- Daemon down: read tools keep working, stamped `source=cache` with `as_of`;
  write tools and `fresh=true` return `daemon_unavailable`.
- Exchange down: unchanged from 4.5 (warm-up loop, circuit breaker, mirror
  reads).
- Gemini down or rate limited: backlog grows, keyword search unaffected,
  semantic search returns keyword results with `meta.degraded=true`.
- Disk full: capturer checks free space against `ARCHIVE_MIN_FREE_GB` before
  each batch and stops the run with a clear error. Temp-then-rename writes
  mean a crash never leaves a wrong file under a right name.

**Deletion rails.** Three independent conditions (`ARCHIVE_DELETE_ENABLED`,
tier `full`, `verified` older than grace), a per-run cap, an audit record per
deletion with `ews_id`, `internet_message_id`, `mime_sha256`, run id, and a
confirm token on `archive_run(dry_run=false)`.

**Testing.**

- Shared and MCP layers: pytest against a real Postgres started by a fixture
  (`docker run pgvector/pgvector:pg16`, skipped without Docker). tsvector and
  pgvector queries are the thing under test.
- Daemon workers: fake gateway with scripted items. Cases: capture then verify
  passes; verify fails on hash mismatch and resets to live; deleter respects
  grace and cap; sync delete of a verified row keeps the row.
- Existing contract tests (`test_envelope_contract.py`,
  `test_docs_match_registry.py`, `test_surface_completion.py`) keep running
  and cover the three new tools.
- `scripts/live_smoke.py` gains an archive dry run. It never deletes.

**Rollout.** Phase 1: daemon, Postgres store, mirror running there, MCP
reading it. Phase 2: capture, verify, embed, delete disabled. Phase 3: enable
deletion with the cap after a week of using search on archived mail. Each
phase leaves a working system.

## Out of scope

Archiving calendar, contacts, or tasks. Attachment text extraction and
embedding. Migrating data from the 4.5 SQLite mirror. Any Arabic-specific
processing. A web UI.
