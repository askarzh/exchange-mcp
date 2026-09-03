# Changelog

Earlier history (the 4.0–4.5 lines) lives in the upstream
[`azizmazrou/ews-mcp`](https://github.com/azizmazrou/ews-mcp) changelog;
this file starts from the point this repository was extracted.

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
