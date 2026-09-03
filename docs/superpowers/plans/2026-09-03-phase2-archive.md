# Phase 2 — Mail Archive, Blob Store and Semantic Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `ewsd` a durable, attachment-inclusive mail archive (capture → verify → delete) with raw-MIME and blob storage on disk, Gemini-backed semantic search over Postgres/pgvector, and the three new tools plus the read-tool changes that make archived mail as usable as live mail.

**Architecture:** Three idempotent, row-state-driven workers live in a new `ewsmcp/archive/` package inside the daemon and are driven by an `ArchiveRunner` asyncio task started after Exchange warm-up (cadence `ARCHIVE_CYCLE_SECONDS`, default 300). Capture writes `{DATA_DIR}/mime/<sha256>.eml` and `{DATA_DIR}/blobs/<sha256[:2]>/<sha256>` temp-then-rename, records `ews.attachments`, and flips `messages.archive_state`. A fourth worker embeds message bodies through an injectable `Embedder` (Gemini REST via `httpx`) into `ews.chunks.embedding vector(768)`; `ewsmcp/semantic.py` turns that into vector and hybrid-RRF search. The MCP never holds the Gemini key: `find_similar` and `search_messages(mode="semantic")` are forwarded to the daemon; `archive_status` is answered locally from Postgres.

**Tech Stack:** Python 3.11+, psycopg 3 + psycopg_pool, Postgres 16 with `pgvector` and `unaccent` (`pgvector/pgvector:pg16`), exchangelib 5.0.3, httpx, pydantic-settings, pytest, ruff (E,F,W,I).

**Spec:** `docs/superpowers/specs/2026-09-03-postgres-archive-daemon-design.md` (§2 tables, §3 pipeline, §4 tools + daemon API + settings, §5 failure modes / rails / tests), with the addendum `docs/superpowers/specs/2026-09-03-phase1.5-simplification-design.md` ("End state Phase 2 builds on").

## Hand-off adjustments from the Phase 1.5 final review (binding, override task text where they differ)

1. **Extension schema trap.** Production connects as role `ews` and a schema `ews` exists, so a bare `CREATE EXTENSION vector` lands in schema `ews` (`"$user"` wins over `public`) and every later `vector(768)` / `<=>` reference breaks. Migration 003 must use `CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;` and reference the type as `public.vector(768)`; `tests/test_db.py` already contains a schema-colliding-role migration test — extend it to 003.
2. **Server-side deletes must not destroy archived rows.** `CacheStore.tombstone_messages` is a hard `DELETE` and `SyncEngine._sync_one_folder` calls it for every EWS `delete` change. The very first Phase 2 task that touches the store must replace it with `apply_server_deletes(ews_ids)`: rows with `archive_state = 'live'` are deleted; `captured`/`verified` rows are kept and set to `deleted` with `deleted_at = now()`; `deleted` rows untouched. `tools/writes.py` `_write_through(..., "tombstone_messages", ...)` keeps working by pointing `tombstone_messages` at the new method. Land this BEFORE the capture worker exists.
3. **Store/cache_reads signatures as they now exist**: `CacheStore.search_messages(*, folder_ids: list[str] | None, text, sender, subject, since_ts, until_ts, is_unread, has_attachments, archived="any", offset, limit)`; `folder_id_for_wk(wk)`, `delete_live_messages_in_folder(folder_id)`, `drop_sync_state(key)`; sync-state keys are `item:<folder ews_id>`, `folders`, `events`, `item:tasks`; `cache_reads.resolve_folder_id(ctx, ref)`, `folder_watermark(ctx, folder_id)`, `wk_watermark(ctx, wk)`. `messages.folder_id` holds the EWS folder id; folder filtering in archive policy resolves well-known names through `folder_id_for_wk`.
4. **Search already returns every archive state** (`archived="any"` hard-coded in `cache_reads.search_messages`). Phase 2's `archived` tool argument replaces that constant; rows with `archive_state='deleted'` remain searchable (they are the archive), and every card carries `archive_state` ONLY when it is not `live` (token economy — overrides ambiguity resolution 5 below).
5. **The hierarchy lane refreshes the folder tree every `EWS_CACHE_HIERARCHY_SECONDS`** and item-syncs mail folders (`folder_class == 'IPF.Note'`) every cycle in batches of 200 with a per-batch flush. Archive workers must not assume a folder set is final at boot.
6. **MCP dispatch validates arguments with jsonschema** (`mcp/dispatch.py`); new tools get validation for free but their `input_schema` must therefore be complete (`additionalProperties: false`, bounds on `limit`, enums).

## Global Constraints

- **This plan targets the code state AFTER Phase 1.5 lands.** Assume `messages.folder_id` (an EWS folder id, not a well-known key string), no `norm_text`, `search_tsv` generated via `ews.immutable_unaccent`, all mail folders mirrored with no time window, `SCHEMA_VERSION = 2`, no live search path, `search_messages` store-only with `folder` optional, `NullAudit` in `ewsmcp/audit.py`, one `FakeGateway` + `make_row` + `make_context` in `tests/conftest.py`, and `scripts/live_smoke.py` deleted. If any of that is not true when you start, stop and report — do not re-do Phase 1.5 here.
- Branch: `feat/phase2-archive`, cut from `main`.
- Migration file for this phase is `ewsmcp/migrations/003_archive.sql`; `ewsmcp.db.SCHEMA_VERSION` becomes `3`.
- Tests: `.venv/bin/python -m pytest tests -q`. They run against a REAL Postgres started by the `db` fixture in `tests/conftest.py` using `pgvector/pgvector:pg16`, so `CREATE EXTENSION vector` works in tests.
- Lint: `.venv/bin/python -m ruff check ewsmcp tests scripts` — ruff is pinned to `E,F,W,I` (line-length 100, target py311).
- **Every `exchangelib` import lives at module top.** `tests/test_no_lazy_imports.py` fails the build otherwise, with no exemptions.
- The MCP process must never import `exchangelib` and must never hold `GEMINI_API_KEY`.
- Embeddings: Gemini REST directly with `httpx`, `POST https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:batchEmbedContents?key=<GEMINI_API_KEY>`, body `{"requests":[{"model":"models/gemini-embedding-2","content":{"parts":[{"text":"…"}]},"outputDimensionality":768}, …]}`, response `{"embeddings":[{"values":[…]}, …]}`. Batches of at most 100 texts; exponential backoff on HTTP 429/5xx. Queries are embedded with the text prefixed by the task instruction `"Retrieve email messages relevant to the query: "` (gemini-embedding-2 takes task instructions in the prompt, not a parameter). The embedder is an injectable `Embedder` Protocol so tests use a deterministic fake and NEVER touch the network.
- Chunking: 1,500 characters per chunk over `subject + "\n" + body_clean`, `source='body'`. Attachment text embedding is out of scope; the `source` column exists for it.
- Vector storage: `chunks.embedding vector(768)`, HNSW index with `vector_cosine_ops`, similarity `ORDER BY embedding <=> %s::vector` taking `MIN` distance per message. Hybrid = Reciprocal Rank Fusion with k=60 over the tsvector ranking and the vector ranking. Any embedder or vector-query failure degrades to keyword results with `meta.degraded` set.
- Files: MIME at `{DATA_DIR}/mime/<sha256>.eml`, blobs at `{DATA_DIR}/blobs/<sha256[:2]>/<sha256>`; always written to a temp name in the SAME directory and renamed only after the hash of the written file matches. Free space is checked with `shutil.disk_usage` against `ARCHIVE_MIN_FREE_GB` (default 2) before each batch.
- Deletion rails (all three independent, all required): `ARCHIVE_DELETE_ENABLED=true`, tier `full`, row `verified` and older than cutoff + `ARCHIVE_GRACE_DAYS` (default 7). Per-run cap `ARCHIVE_MAX_DELETE_PER_RUN` (default 200). One audit record per deletion carrying `ews_id`, `internet_message_id`, `mime_sha256` and the run id. `archive_run(dry_run=false)` is confirm-gated.
- New tool counts after this plan (assert them exactly): **read tier 18, draft tier 29, full tier 35** (was 15 / 26 / 31). `archive_run` is class `destructive` (min tier full); `archive_status`, `get_raw_message` and `find_similar` are class `read`.
- Commit trailers on every commit:

```
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
```

## File Map

**New files**

| file | responsibility |
|---|---|
| `ewsmcp/migrations/003_archive.sql` | `attachments`, `chunks`, `archive_runs`, the `vector` extension, HNSW + backlog indexes |
| `ewsmcp/archive/__init__.py` | package marker, re-exports `ArchiveRunner` |
| `ewsmcp/archive/files.py` | MIME/blob paths, hashed temp-then-rename writes, free-space guard |
| `ewsmcp/archive/policy.py` | `ArchivePolicy` — settings → cutoffs, folder ids, overrides |
| `ewsmcp/archive/capture.py` | `Capturer` — fetch MIME + attachments, write files, mark `captured` |
| `ewsmcp/archive/verify.py` | `Verifier` — re-fetch, re-hash, mark `verified` or reset to `live` |
| `ewsmcp/archive/delete.py` | `Deleter` — the three rails, the cap, hard delete, per-deletion audit |
| `ewsmcp/archive/embed.py` | `EmbedWorker` — drains `embedded_at IS NULL` through the `SemanticIndex` |
| `ewsmcp/archive/runner.py` | `ArchiveRunner` — cycle task, one `archive_runs` row per pass |
| `ewsmcp/embeddings.py` | `Embedder` Protocol, `GeminiEmbedder`, `chunk_text`, `EmbeddingError` |
| `ewsmcp/semantic.py` | `SemanticIndex` — index messages, vector search, hybrid RRF |
| `ewsmcp/downloads.py` | single-use capability download tokens (mirrors `uploads.py`) |
| `ewsmcp/tools/archive.py` | `archive_run`, `archive_status`, `get_raw_message`, `find_similar` |

**Modified files**

| file | change |
|---|---|
| `ewsmcp/db.py` | `SCHEMA_VERSION = 3` |
| `ewsmcp/config.py` | `gemini_api_key`, `embed_dims`, the `ARCHIVE_*` set |
| `ewsmcp/cache/store.py` | archive-state queries, attachments, chunks + vector search, `archive_runs` |
| `ewsmcp/cache/sync.py` | server-side delete handling per spec §3 |
| `ewsmcp/tools/base.py` | `Context.archive`, `Context.semantic` |
| `ewsmcp/tools/cache_reads.py` | `archived` filter, per-folder archived counts, `archive_state` on cards |
| `ewsmcp/tools/mail_read.py` | `search_messages` `archived` + real `semantic`; `get_attachment` from the blob store; `list_folders` archived counts |
| `ewsmcp/tools/__init__.py` | register the `archive` pack |
| `ewsmcp/server.py` | build `SemanticIndex` + `ArchiveRunner`, start the runner on warm |
| `ewsmcp/http.py` | `GET /download/<token>`, `POST /v1/archive/run`, `GET /v1/archive/runs/<id>` |
| `ewsmcp/mcp/local.py` | local `archive_status`, forward `mode=semantic` |
| `ewsmcp/mcp/registry.py` | include the archive pack |
| `scripts/dump_tool_table.py` | the archive pack in `_packs()` |
| `scripts/boot_smoke.py` | 35 tools, `archive_status`, cold `archive_run(dry_run=true)` |
| `DESIGN.md`, `README.md`, `docs/API.md`, `CHANGELOG.md`, `pyproject.toml`, `ewsmcp/__init__.py` | docs + version `5.1.0a1` |
| `/home/askar/stack/compose/personal.yml` | `GEMINI_API_KEY` + `ARCHIVE_*` on `ewsd` |

**New test files:** `tests/test_archive_schema.py`, `tests/test_archive_files.py`, `tests/test_archive_store.py`, `tests/test_embeddings.py`, `tests/test_semantic.py`, `tests/test_archive_capture.py`, `tests/test_archive_verify_delete.py`, `tests/test_archive_runner.py`, `tests/test_downloads.py`, `tests/test_archive_tools.py`, `tests/test_mail_read_archive.py`.

---

### Task 1: Migration 003 — archive tables, pgvector, indexes

**Files:**
- Create: `ewsmcp/migrations/003_archive.sql`
- Modify: `ewsmcp/db.py` (`SCHEMA_VERSION`)
- Test: `tests/test_archive_schema.py`

**Interfaces:**
- Consumes: `ewsmcp.db.Database.migrate()`, `Database.schema_version()` (already present).
- Produces: tables `ews.attachments`, `ews.chunks`, `ews.archive_runs`; extension `vector`; `ewsmcp.db.SCHEMA_VERSION == 3`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_archive_schema.py`:

```python
"""Migration 003: the archive tables, pgvector, and the indexes search needs."""

from ewsmcp.db import SCHEMA_VERSION


def _cols(db, table):
    with db.conn() as c:
        rows = c.execute(
            "SELECT column_name, data_type, udt_name FROM information_schema.columns "
            "WHERE table_schema = 'ews' AND table_name = %s", (table,)).fetchall()
    return {r["column_name"]: (r["data_type"], r["udt_name"]) for r in rows}


def _indexes(db, table):
    with db.conn() as c:
        rows = c.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'ews' AND tablename = %s", (table,)).fetchall()
    return {r["indexname"]: r["indexdef"] for r in rows}


def test_schema_version_is_three(db):
    assert SCHEMA_VERSION == 3
    assert db.schema_version() == 3


def test_vector_extension_is_installed(db):
    with db.conn() as c:
        row = c.execute("SELECT 1 AS ok FROM pg_extension WHERE extname = 'vector'").fetchone()
    assert row is not None


def test_attachments_table_shape(db):
    cols = _cols(db, "attachments")
    assert set(cols) == {"id", "message_ews_id", "name", "content_type", "size",
                         "sha256", "is_inline", "name_tsv"}
    assert cols["name_tsv"][1] == "tsvector"
    assert "ix_att_name_tsv" in _indexes(db, "attachments")


def test_attachments_cascade_when_a_live_row_is_dropped(db, store_with_message):
    store, ews_id = store_with_message
    store.replace_attachments(ews_id, [
        {"name": "q3.pdf", "content_type": "application/pdf", "size": 12,
         "sha256": "a" * 64, "is_inline": 0},
    ])
    with db.conn() as c:
        c.execute("DELETE FROM ews.messages WHERE ews_id = %s", (ews_id,))
        left = c.execute("SELECT COUNT(*) AS n FROM ews.attachments").fetchone()["n"]
    assert left == 0


def test_chunks_embedding_is_a_768_vector_with_an_hnsw_index(db):
    cols = _cols(db, "chunks")
    assert cols["embedding"][1] == "vector"
    with db.conn() as c:
        dims = c.execute(
            "SELECT a.atttypmod AS m FROM pg_attribute a "
            "JOIN pg_class t ON t.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "WHERE n.nspname = 'ews' AND t.relname = 'chunks' "
            "AND a.attname = 'embedding'").fetchone()["m"]
    assert dims == 768
    defs = _indexes(db, "chunks")
    assert any("hnsw" in d and "vector_cosine_ops" in d for d in defs.values())


def test_archive_runs_table_shape(db):
    cols = _cols(db, "archive_runs")
    assert set(cols) == {"id", "kind", "dry_run", "policy_json", "started_at",
                         "finished_at", "captured", "verified", "deleted",
                         "failed", "error", "sample_json"}
    with db.conn() as c:
        c.execute("INSERT INTO ews.archive_runs (kind) VALUES ('capture')")
        row = c.execute("SELECT id, dry_run, captured FROM ews.archive_runs").fetchone()
    assert row["id"] > 0 and row["dry_run"] == 1 and row["captured"] == 0
```

Add this fixture to `tests/conftest.py` (it is reused by later tasks):

```python
@pytest.fixture
def store_with_message(db):
    """A CacheStore holding exactly one live inbox message, plus its ews_id."""
    from ewsmcp.cache.store import CacheStore
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 1, "unread": 0, "children": 0},
    ])
    store.upsert_messages([make_row("RAW-1")])
    return store, "RAW-1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_archive_schema.py -q`
Expected: FAIL — `assert 2 == 3` on `SCHEMA_VERSION`, and `UndefinedTable: relation "ews.attachments" does not exist`.

- [ ] **Step 3: Write the migration**

Create `ewsmcp/migrations/003_archive.sql`:

```sql
-- ews schema v3: the archive tables. Rows for archived mail are never
-- dropped from ews.messages, so these children hang off ews_id and cascade
-- only when a LIVE row is removed by the sync engine.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE ews.attachments (
    id             bigserial PRIMARY KEY,
    message_ews_id text NOT NULL REFERENCES ews.messages(ews_id) ON DELETE CASCADE,
    name           text,
    content_type   text,
    size           bigint,
    -- NULL for ItemAttachment (a nested message): it is captured inside the
    -- MIME only, so there is no standalone blob to hash.
    sha256         text,
    is_inline      smallint NOT NULL DEFAULT 0,
    name_tsv       tsvector GENERATED ALWAYS AS (
                       to_tsvector('simple',
                           ews.immutable_unaccent(lower(coalesce(name, ''))))
                   ) STORED
);
CREATE INDEX ix_att_message  ON ews.attachments (message_ews_id);
CREATE INDEX ix_att_sha      ON ews.attachments (sha256);
CREATE INDEX ix_att_name_tsv ON ews.attachments USING GIN (name_tsv);

CREATE TABLE ews.chunks (
    id             bigserial PRIMARY KEY,
    message_ews_id text NOT NULL REFERENCES ews.messages(ews_id) ON DELETE CASCADE,
    seq            integer NOT NULL,
    source         text NOT NULL DEFAULT 'body',
    text           text NOT NULL,
    embedding      vector(768)
);
CREATE UNIQUE INDEX ux_chunks_msg_seq ON ews.chunks (message_ews_id, source, seq);
CREATE INDEX ix_chunks_embedding ON ews.chunks
    USING hnsw (embedding vector_cosine_ops);

CREATE TABLE ews.archive_runs (
    id          bigserial PRIMARY KEY,
    kind        text NOT NULL
                CHECK (kind IN ('capture', 'verify', 'delete', 'embed', 'all')),
    dry_run     smallint NOT NULL DEFAULT 1,
    policy_json text,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    captured    integer NOT NULL DEFAULT 0,
    verified    integer NOT NULL DEFAULT 0,
    deleted     integer NOT NULL DEFAULT 0,
    failed      integer NOT NULL DEFAULT 0,
    error       text,
    sample_json text
);
CREATE INDEX ix_runs_started ON ews.archive_runs (started_at DESC);

-- Backlog scan for the embedder: only the unembedded rows are ever selected.
CREATE INDEX ix_msg_unembedded ON ews.messages (date_ts DESC)
    WHERE embedded_at IS NULL;
-- Capture candidate scan: live rows, oldest first, per folder.
CREATE INDEX ix_msg_live_date ON ews.messages (folder_id, date_ts)
    WHERE archive_state = 'live';
```

Then in `ewsmcp/db.py` change the constant:

```python
SCHEMA_VERSION = 3  # bump together with the newest migrations/NNN_*.sql
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_archive_schema.py -q`
Expected: PASS (6 passed). `test_attachments_cascade_when_a_live_row_is_dropped` will still fail with `AttributeError: 'CacheStore' object has no attribute 'replace_attachments'` — mark it `@pytest.mark.xfail(reason="replace_attachments lands in Task 4", strict=True)` for now and remove the marker in Task 4.

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m pytest tests -q`
Expected: PASS — everything else is untouched.

- [ ] **Step 6: Commit**

```bash
git checkout -b feat/phase2-archive
git add ewsmcp/migrations/003_archive.sql ewsmcp/db.py tests/test_archive_schema.py tests/conftest.py
git commit -m "$(cat <<'EOF'
feat(archive): migration 003 — attachments, chunks(vector 768), archive_runs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 2: Settings — Gemini and the ARCHIVE_* set

**Files:**
- Modify: `ewsmcp/config.py`
- Test: `tests/test_config_guards.py` (append)

**Interfaces:**
- Produces: `Settings.gemini_api_key: str | None`, `Settings.embed_dims: int = 768`, `Settings.archive_folders: str = "inbox,sent"`, `archive_after_days: int = 180`, `archive_exclude_categories: str = ""`, `archive_grace_days: int = 7`, `archive_delete_enabled: bool = False`, `archive_max_delete_per_run: int = 200`, `archive_min_free_gb: float = 2.0`, `archive_cycle_seconds: int = 300`, and `Settings.semantic_enabled() -> bool`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config_guards.py`:

```python
def test_archive_settings_default_safe(monkeypatch):
    from conftest import make_settings
    s = make_settings()
    assert s.archive_folders == "inbox,sent"
    assert s.archive_after_days == 180
    assert s.archive_grace_days == 7
    assert s.archive_delete_enabled is False       # deletion is OFF by default
    assert s.archive_max_delete_per_run == 200
    assert s.archive_min_free_gb == 2.0
    assert s.archive_cycle_seconds == 300
    assert s.embed_dims == 768
    assert s.gemini_api_key is None
    assert s.semantic_enabled() is False


def test_semantic_enabled_only_with_a_key():
    from conftest import make_settings
    assert make_settings(gemini_api_key="k").semantic_enabled() is True


def test_embed_dims_must_match_the_vector_column():
    import pytest
    from conftest import make_settings
    with pytest.raises(ValueError, match="EMBED_DIMS"):
        make_settings(embed_dims=1536)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_config_guards.py -q`
Expected: FAIL with `AttributeError: 'Settings' object has no attribute 'archive_folders'`.

- [ ] **Step 3: Write the implementation**

In `ewsmcp/config.py`, add after the `# --- Reliability` block:

```python
    # --- Embeddings (daemon only; the MCP must never hold this key) ----------
    gemini_api_key: str | None = None
    embed_dims: int = 768  # fixed by migration 003's vector(768) column

    # --- Archive pipeline (daemon only) --------------------------------------
    archive_folders: str = "inbox,sent"        # well-known keys, never calendar/contacts/tasks
    archive_after_days: int = 180
    archive_exclude_categories: str = ""
    archive_grace_days: int = 7
    archive_delete_enabled: bool = False       # rail 1 of 3: deletion is OFF by default
    archive_max_delete_per_run: int = 200
    archive_min_free_gb: float = 2.0
    archive_cycle_seconds: int = 300
```

and, next to `_resolve_data_dir`:

```python
    @model_validator(mode="after")
    def _check_embed_dims(self) -> "Settings":
        if self.embed_dims != 768:
            raise ValueError(
                f"EMBED_DIMS must be 768 (got {self.embed_dims}): migration 003 "
                "declares chunks.embedding as vector(768). Changing the width "
                "needs a new migration that rebuilds the column and its index."
            )
        return self

    def semantic_enabled(self) -> bool:
        """Semantic search needs a remote embedder; without a key we stay keyword-only."""
        return bool(self.gemini_api_key)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_config_guards.py -q`
Expected: PASS

- [ ] **Step 5: Lint and run the whole suite**

Run: `.venv/bin/python -m ruff check ewsmcp tests scripts && .venv/bin/python -m pytest tests -q`
Expected: no lint findings; all tests pass.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/config.py tests/test_config_guards.py
git commit -m "$(cat <<'EOF'
feat(config): GEMINI_API_KEY, EMBED_DIMS and the ARCHIVE_* settings

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 3: `archive/files.py` — hashed blob and MIME storage

**Files:**
- Create: `ewsmcp/archive/__init__.py`, `ewsmcp/archive/files.py`
- Test: `tests/test_archive_files.py`

**Interfaces:**
- Produces:
  - `class DiskFull(RuntimeError)`
  - `sha256_bytes(data: bytes) -> str`
  - `sha256_file(path: pathlib.Path) -> str`
  - `mime_path(data_dir: str, sha: str) -> pathlib.Path` → `{data_dir}/mime/<sha>.eml`
  - `blob_path(data_dir: str, sha: str) -> pathlib.Path` → `{data_dir}/blobs/<sha[:2]>/<sha>`
  - `store_mime(data_dir: str, data: bytes) -> tuple[str, pathlib.Path]`
  - `store_blob(data_dir: str, data: bytes) -> tuple[str, pathlib.Path]`
  - `free_gb(data_dir: str) -> float`
  - `ensure_free_space(data_dir: str, min_free_gb: float) -> None` (raises `DiskFull`)
  - `blob_store_bytes(data_dir: str) -> int`

- [ ] **Step 1: Write the failing test**

Create `tests/test_archive_files.py`:

```python
"""Blob/MIME storage: content-addressed, temp-then-rename, free-space guard."""

import hashlib

import pytest

from ewsmcp.archive import files


def test_store_mime_is_content_addressed(tmp_path):
    data = b"From: a@b\r\nSubject: hi\r\n\r\nbody"
    sha, path = files.store_mime(str(tmp_path), data)
    assert sha == hashlib.sha256(data).hexdigest()
    assert path == tmp_path / "mime" / f"{sha}.eml"
    assert path.read_bytes() == data


def test_store_blob_shards_on_the_first_two_hex_chars(tmp_path):
    sha, path = files.store_blob(str(tmp_path), b"pdf-bytes")
    assert path == tmp_path / "blobs" / sha[:2] / sha
    assert path.read_bytes() == b"pdf-bytes"
    assert files.sha256_file(path) == sha


def test_storing_the_same_bytes_twice_is_idempotent(tmp_path):
    sha1, path1 = files.store_blob(str(tmp_path), b"same")
    sha2, path2 = files.store_blob(str(tmp_path), b"same")
    assert (sha1, path1) == (sha2, path2)
    assert len(list((tmp_path / "blobs" / sha1[:2]).iterdir())) == 1


def test_no_temp_files_survive_a_successful_write(tmp_path):
    files.store_blob(str(tmp_path), b"x")
    assert not list(tmp_path.rglob("*.tmp-*"))


def test_a_corrupt_existing_file_under_the_right_name_is_rewritten(tmp_path):
    sha, path = files.store_blob(str(tmp_path), b"good")
    path.write_bytes(b"CORRUPT")
    sha2, path2 = files.store_blob(str(tmp_path), b"good")
    assert (sha2, path2) == (sha, path)
    assert path.read_bytes() == b"good"


def test_ensure_free_space_raises_when_below_the_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(files.shutil, "disk_usage",
                        lambda p: (100, 99, 1 * 1024 ** 3))  # 1 GiB free
    with pytest.raises(files.DiskFull, match="ARCHIVE_MIN_FREE_GB"):
        files.ensure_free_space(str(tmp_path), 2.0)


def test_ensure_free_space_passes_when_above_the_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(files.shutil, "disk_usage",
                        lambda p: (100, 1, 50 * 1024 ** 3))
    files.ensure_free_space(str(tmp_path), 2.0)  # no raise


def test_blob_store_bytes_sums_mime_and_blobs(tmp_path):
    files.store_mime(str(tmp_path), b"a" * 10)
    files.store_blob(str(tmp_path), b"b" * 25)
    assert files.blob_store_bytes(str(tmp_path)) == 35
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_archive_files.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'ewsmcp.archive'`.

- [ ] **Step 3: Write the implementation**

Create `ewsmcp/archive/__init__.py`:

```python
"""The archive pipeline: capture → verify → delete, plus the embedder.

Every worker is idempotent and driven by ``messages.archive_state``, so a
crashed or half-finished pass is simply repeated on the next cycle. Nothing
here is imported by the MCP process — the daemon owns Exchange, the blob
store and the Gemini key.
"""
```

Create `ewsmcp/archive/files.py`:

```python
"""Content-addressed storage for raw MIME and attachment blobs.

Two invariants, both scar tissue from the failure modes in spec §5:

1. **Temp-then-rename inside the same directory.** The bytes are written to
   ``<name>.tmp-<random>`` next to their final home, re-hashed FROM DISK, and
   only then ``os.replace``d. A crash therefore never leaves wrong bytes under
   a right (content-addressed) name, and the rename is atomic because source
   and destination share a filesystem.
2. **Free space is checked before a batch, not after a failure.** A full disk
   stops the run with a clear error instead of writing truncated files.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import shutil
from pathlib import Path

MIME_DIRNAME = "mime"
BLOB_DIRNAME = "blobs"
_READ_CHUNK = 1024 * 1024


class DiskFull(RuntimeError):
    """Free space fell below ARCHIVE_MIN_FREE_GB — the run must stop."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(_READ_CHUNK)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def mime_path(data_dir: str, sha: str) -> Path:
    return Path(data_dir) / MIME_DIRNAME / f"{sha}.eml"


def blob_path(data_dir: str, sha: str) -> Path:
    return Path(data_dir) / BLOB_DIRNAME / sha[:2] / sha


def _write_verified(dest: Path, data: bytes, sha: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and sha256_file(dest) == sha:
        return  # already stored, byte-identical — dedup across messages
    tmp = dest.parent / f"{dest.name}.tmp-{secrets.token_hex(8)}"
    try:
        tmp.write_bytes(data)
        if sha256_file(tmp) != sha:
            raise OSError(f"hash mismatch after writing {tmp}")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def store_mime(data_dir: str, data: bytes) -> tuple[str, Path]:
    sha = sha256_bytes(data)
    dest = mime_path(data_dir, sha)
    _write_verified(dest, data, sha)
    return sha, dest


def store_blob(data_dir: str, data: bytes) -> tuple[str, Path]:
    sha = sha256_bytes(data)
    dest = blob_path(data_dir, sha)
    _write_verified(dest, data, sha)
    return sha, dest


def free_gb(data_dir: str) -> float:
    root = Path(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(str(root))[2] / 1024 ** 3


def ensure_free_space(data_dir: str, min_free_gb: float) -> None:
    free = free_gb(data_dir)
    if free < float(min_free_gb):
        raise DiskFull(
            f"only {free:.2f} GB free under {data_dir}; ARCHIVE_MIN_FREE_GB is "
            f"{min_free_gb}. Capture stopped — free space or lower the floor."
        )


def blob_store_bytes(data_dir: str) -> int:
    total = 0
    for name in (MIME_DIRNAME, BLOB_DIRNAME):
        root = Path(data_dir) / name
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if path.is_file():
                total += path.stat().st_size
    return total
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_archive_files.py -q`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add ewsmcp/archive tests/test_archive_files.py
git commit -m "$(cat <<'EOF'
feat(archive): content-addressed MIME/blob store with a free-space guard

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 4: Store — archive state, attachments, run records

**Files:**
- Modify: `ewsmcp/cache/store.py`
- Test: `tests/test_archive_store.py`, `tests/test_archive_schema.py` (drop the xfail marker)

**Interfaces:**
- Consumes: `ewsmcp.db.Database` (existing), `CacheStore.__init__(db)` (existing).
- Produces, all on `CacheStore`:

```python
def archive_candidates(self, *, folder_ids: list[str] | None, before_ts: int,
                       exclude_categories: list[str], limit: int) -> list[dict]
def archive_candidate_count(self, *, folder_ids: list[str] | None, before_ts: int,
                            exclude_categories: list[str]) -> int
def folder_ids_for_wk(self, wk_keys: list[str]) -> list[str]
def mark_captured(self, ews_id: str, *, mime_sha256: str, mime_path: str) -> None
def replace_attachments(self, ews_id: str, rows: list[dict]) -> None
def attachments_for(self, ews_id: str) -> list[dict]
def captured_rows(self, limit: int) -> list[dict]
def mark_verified(self, ews_id: str) -> None
def reset_to_live(self, ews_id: str) -> None
def deletable_rows(self, *, before_ts: int, verified_before: int, limit: int) -> list[dict]
def mark_deleted(self, ews_ids: list[str]) -> int
def apply_server_deletes(self, ews_ids: list[str]) -> tuple[int, int]
def archive_state_counts(self) -> dict[str, int]
def archived_counts_by_folder(self) -> dict[str, int]
def messages_by_ids(self, ews_ids: list[str]) -> dict[str, dict]
def start_run(self, kind: str, *, dry_run: bool, policy: dict) -> int
def finish_run(self, run_id: int, *, captured: int = 0, verified: int = 0,
               deleted: int = 0, failed: int = 0, error: str | None = None,
               sample: list | None = None) -> None
def get_run(self, run_id: int) -> dict | None
def recent_runs(self, limit: int = 5) -> list[dict]
```

An attachment row is `{"name": str, "content_type": str | None, "size": int | None, "sha256": str | None, "is_inline": int}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_archive_store.py`:

```python
"""CacheStore's archive half: candidate selection, state transitions,
attachment rows, and the archive_runs ledger."""

import time

from conftest import make_row

NOW = int(time.time())
DAY = 86400


def _folders(store):
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0},
        {"ews_id": "FID-SENT", "name": "Sent", "path": "Sent", "wk": "f:sent",
         "total": 0, "unread": 0, "children": 0},
        {"ews_id": "FID-PROJ", "name": "Projects", "path": "Projects", "wk": None,
         "total": 0, "unread": 0, "children": 0},
    ])


def test_folder_ids_for_wk(store_with_message):
    store, _ = store_with_message
    _folders(store)
    assert sorted(store.folder_ids_for_wk(["f:inbox", "f:sent"])) == \
        ["FID-INBOX", "FID-SENT"]
    assert store.folder_ids_for_wk(["f:nope"]) == []


def test_candidates_respect_folder_age_and_categories(store_with_message):
    store, _ = store_with_message
    _folders(store)
    store.upsert_messages([
        make_row("OLD-IN", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY),
        make_row("NEW-IN", folder_id="FID-INBOX", date_ts=NOW - 5 * DAY),
        make_row("OLD-PROJ", folder_id="FID-PROJ", date_ts=NOW - 300 * DAY),
        make_row("OLD-KEEP", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY,
                 categories=["Keep"]),
    ])
    cutoff = NOW - 180 * DAY
    rows = store.archive_candidates(folder_ids=["FID-INBOX", "FID-SENT"],
                                    before_ts=cutoff, exclude_categories=["keep"],
                                    limit=25)
    assert [r["ews_id"] for r in rows] == ["OLD-IN"]
    assert store.archive_candidate_count(
        folder_ids=["FID-INBOX", "FID-SENT"], before_ts=cutoff,
        exclude_categories=["keep"]) == 1


def test_candidates_across_all_folders_when_folder_ids_is_none(store_with_message):
    store, _ = store_with_message
    _folders(store)
    store.upsert_messages([make_row("OLD-PROJ", folder_id="FID-PROJ",
                                    date_ts=NOW - 300 * DAY)])
    rows = store.archive_candidates(folder_ids=None, before_ts=NOW - 180 * DAY,
                                    exclude_categories=[], limit=25)
    assert "OLD-PROJ" in {r["ews_id"] for r in rows}


def test_capture_verify_delete_state_machine(store_with_message):
    store, ews_id = store_with_message
    store.mark_captured(ews_id, mime_sha256="b" * 64, mime_path="/data/mime/b.eml")
    row = store.get_message(ews_id)
    assert row["archive_state"] == "captured" and row["mime_sha256"] == "b" * 64
    assert row["archived_at"] is not None
    assert [r["ews_id"] for r in store.captured_rows(10)] == [ews_id]

    store.mark_verified(ews_id)
    assert store.get_message(ews_id)["archive_state"] == "verified"
    assert store.get_message(ews_id)["verified_at"] is not None

    assert store.mark_deleted([ews_id]) == 1
    row = store.get_message(ews_id)
    assert row["archive_state"] == "deleted" and row["deleted_at"] is not None
    # the row itself SURVIVES — that is the whole point of the archive
    assert row["subject"]


def test_reset_to_live_clears_the_capture_fields(store_with_message):
    store, ews_id = store_with_message
    store.mark_captured(ews_id, mime_sha256="c" * 64, mime_path="/x.eml")
    store.reset_to_live(ews_id)
    row = store.get_message(ews_id)
    assert row["archive_state"] == "live"
    assert row["mime_sha256"] is None and row["mime_path"] is None
    assert row["archived_at"] is None


def test_deletable_rows_need_verified_plus_grace(store_with_message):
    store, _ = store_with_message
    store.upsert_messages([
        make_row("V-OLD", date_ts=NOW - 300 * DAY),
        make_row("V-YOUNG", date_ts=NOW - 10 * DAY),
        make_row("CAPTURED-ONLY", date_ts=NOW - 300 * DAY),
    ])
    for i in ("V-OLD", "V-YOUNG", "CAPTURED-ONLY"):
        store.mark_captured(i, mime_sha256="d" * 64, mime_path="/x.eml")
    store.mark_verified("V-OLD")
    store.mark_verified("V-YOUNG")
    rows = store.deletable_rows(before_ts=NOW - 187 * DAY,
                                verified_before=NOW + DAY, limit=100)
    assert [r["ews_id"] for r in rows] == ["V-OLD"]
    # grace not yet elapsed: nothing verified before that instant
    assert store.deletable_rows(before_ts=NOW - 187 * DAY,
                                verified_before=NOW - DAY, limit=100) == []


def test_deletable_rows_honour_the_limit(store_with_message):
    store, _ = store_with_message
    store.upsert_messages([make_row(f"V{i}", date_ts=NOW - 300 * DAY)
                           for i in range(5)])
    for i in range(5):
        store.mark_captured(f"V{i}", mime_sha256="e" * 64, mime_path="/x.eml")
        store.mark_verified(f"V{i}")
    assert len(store.deletable_rows(before_ts=NOW, verified_before=NOW + DAY,
                                    limit=2)) == 2


def test_replace_attachments_is_idempotent(store_with_message):
    store, ews_id = store_with_message
    rows = [{"name": "q3.pdf", "content_type": "application/pdf", "size": 120,
             "sha256": "f" * 64, "is_inline": 0},
            {"name": "logo.png", "content_type": "image/png", "size": 40,
             "sha256": "0" * 64, "is_inline": 1}]
    store.replace_attachments(ews_id, rows)
    store.replace_attachments(ews_id, rows)
    got = store.attachments_for(ews_id)
    assert len(got) == 2
    assert {r["name"] for r in got} == {"q3.pdf", "logo.png"}
    assert got[0]["size"] == 120


def test_apply_server_deletes_follows_the_archive_state(store_with_message):
    store, _ = store_with_message
    store.upsert_messages([make_row("LIVE-1"), make_row("CAP-1"),
                           make_row("VER-1"), make_row("DEL-1")])
    store.mark_captured("CAP-1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_captured("VER-1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("VER-1")
    store.mark_captured("DEL-1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("DEL-1")
    store.mark_deleted(["DEL-1"])

    dropped, tombstoned = store.apply_server_deletes(
        ["LIVE-1", "CAP-1", "VER-1", "DEL-1"])
    assert (dropped, tombstoned) == (1, 2)
    assert store.get_message("LIVE-1") is None            # live rows go
    for i in ("CAP-1", "VER-1", "DEL-1"):                  # archived rows stay
        assert store.get_message(i)["archive_state"] == "deleted"


def test_state_counts_and_per_folder_archived_counts(store_with_message):
    store, ews_id = store_with_message
    _folders(store)
    store.upsert_messages([make_row("A1", folder_id="FID-INBOX"),
                           make_row("A2", folder_id="FID-SENT")])
    store.mark_captured("A1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_captured("A2", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("A2")
    counts = store.archive_state_counts()
    assert counts["captured"] == 1 and counts["verified"] == 1
    assert counts["live"] >= 1
    assert store.archived_counts_by_folder() == {"FID-INBOX": 1, "FID-SENT": 1}


def test_messages_by_ids_returns_a_lookup(store_with_message):
    store, ews_id = store_with_message
    store.upsert_messages([make_row("B1"), make_row("B2")])
    got = store.messages_by_ids(["B1", "B2", "MISSING"])
    assert set(got) == {"B1", "B2"}
    assert got["B1"]["ews_id"] == "B1"


def test_run_ledger_round_trip(store_with_message):
    store, _ = store_with_message
    run_id = store.start_run("capture", dry_run=True,
                             policy={"folders": ["f:inbox"], "after_days": 180})
    assert isinstance(run_id, int) and run_id > 0
    open_row = store.get_run(run_id)
    assert open_row["finished_at"] is None and open_row["dry_run"] == 1
    store.finish_run(run_id, captured=3, failed=1, sample=[{"id": "RAW-1"}])
    row = store.get_run(run_id)
    assert row["captured"] == 3 and row["failed"] == 1
    assert row["finished_at"] is not None
    assert '"RAW-1"' in row["sample_json"]
    assert '"after_days": 180' in row["policy_json"]
    assert [r["id"] for r in store.recent_runs(5)] == [run_id]


def test_get_run_of_an_unknown_id_is_none(store_with_message):
    store, _ = store_with_message
    assert store.get_run(999999) is None
```

`make_row` in `tests/conftest.py` must accept `categories` (it already produces `categories_json`); if it does not, extend it:

```python
def make_row(ews_id, *, folder_id="FID-INBOX", subject="Budget review",
             sender_email="a@corp.example", sender_name="Ahmed",
             body="please review the numbers", date_ts=None, is_read=1,
             has_attachments=0, conv="CONV-1", imid=None, to=None,
             categories=None):
    ...
    "categories_json": json.dumps(categories or []),
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_archive_store.py -q`
Expected: FAIL — `AttributeError: 'CacheStore' object has no attribute 'folder_ids_for_wk'`.

- [ ] **Step 3: Write the implementation**

Append to `ewsmcp/cache/store.py` (inside `class CacheStore`), and add `from typing import Any` / `import json` if not already imported:

```python
    # --------------------------------------------------------- archive state

    def folder_ids_for_wk(self, wk_keys: list[str]) -> list[str]:
        """Well-known keys (f:inbox, …) → EWS folder ids, via ews.folders."""
        if not wk_keys:
            return []
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT ews_id FROM ews.folders WHERE wk = ANY(%s)",
                (list(wk_keys),)).fetchall()
        return [r["ews_id"] for r in rows]

    _CANDIDATE_WHERE = """
        m.archive_state = 'live'
        AND m.date_ts IS NOT NULL AND m.date_ts <= %(before_ts)s
        AND (%(folder_ids)s::text[] IS NULL OR m.folder_id = ANY(%(folder_ids)s))
        AND NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(
                COALESCE(NULLIF(m.categories_json, ''), '[]')::jsonb) AS cat
            WHERE lower(cat) = ANY(%(exclude_categories)s))
    """

    def _candidate_params(self, folder_ids, before_ts, exclude_categories):
        return {
            "before_ts": int(before_ts),
            "folder_ids": list(folder_ids) if folder_ids else None,
            "exclude_categories": [c.strip().lower()
                                   for c in (exclude_categories or []) if c.strip()],
        }

    def archive_candidates(self, *, folder_ids: list[str] | None, before_ts: int,
                           exclude_categories: list[str],
                           limit: int) -> list[dict[str, Any]]:
        params = self._candidate_params(folder_ids, before_ts, exclude_categories)
        params["limit"] = int(limit)
        with self.db.conn() as c:
            return c.execute(
                "SELECT m.ews_id, m.changekey, m.folder_id, m.subject, m.date_iso, "
                "m.date_ts, m.internet_message_id, m.has_attachments "
                f"FROM ews.messages m WHERE {self._CANDIDATE_WHERE} "
                "ORDER BY m.date_ts ASC LIMIT %(limit)s", params).fetchall()

    def archive_candidate_count(self, *, folder_ids: list[str] | None,
                                before_ts: int,
                                exclude_categories: list[str]) -> int:
        params = self._candidate_params(folder_ids, before_ts, exclude_categories)
        with self.db.conn() as c:
            return int(c.execute(
                "SELECT COUNT(*) AS n FROM ews.messages m "
                f"WHERE {self._CANDIDATE_WHERE}", params).fetchone()["n"])

    def mark_captured(self, ews_id: str, *, mime_sha256: str,
                      mime_path: str) -> None:
        with self.db.conn() as c:
            c.execute(
                "UPDATE ews.messages SET archive_state = 'captured', "
                "archived_at = now(), mime_sha256 = %s, mime_path = %s "
                "WHERE ews_id = %s", (mime_sha256, mime_path, ews_id))

    def captured_rows(self, limit: int) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.messages WHERE archive_state = 'captured' "
                "ORDER BY archived_at ASC LIMIT %s", (int(limit),)).fetchall()

    def mark_verified(self, ews_id: str) -> None:
        with self.db.conn() as c:
            c.execute("UPDATE ews.messages SET archive_state = 'verified', "
                      "verified_at = now() WHERE ews_id = %s", (ews_id,))

    def reset_to_live(self, ews_id: str) -> None:
        """Verification failed — forget the capture entirely so it is retried."""
        with self.db.conn() as c:
            c.execute(
                "UPDATE ews.messages SET archive_state = 'live', archived_at = NULL, "
                "verified_at = NULL, mime_sha256 = NULL, mime_path = NULL "
                "WHERE ews_id = %s", (ews_id,))
            c.execute("DELETE FROM ews.attachments WHERE message_ews_id = %s",
                      (ews_id,))

    def deletable_rows(self, *, before_ts: int, verified_before: int,
                       limit: int) -> list[dict[str, Any]]:
        """Rail 3: verified, older than the cutoff, and verified long enough ago."""
        with self.db.conn() as c:
            return c.execute(
                "SELECT ews_id, internet_message_id, mime_sha256, subject, date_iso "
                "FROM ews.messages WHERE archive_state = 'verified' "
                "AND date_ts IS NOT NULL AND date_ts <= %s "
                "AND verified_at IS NOT NULL AND verified_at <= to_timestamp(%s) "
                "ORDER BY date_ts ASC LIMIT %s",
                (int(before_ts), int(verified_before), int(limit))).fetchall()

    def mark_deleted(self, ews_ids: list[str]) -> int:
        if not ews_ids:
            return 0
        with self.db.conn() as c:
            cur = c.execute(
                "UPDATE ews.messages SET archive_state = 'deleted', "
                "deleted_at = now() WHERE ews_id = ANY(%s)", (list(ews_ids),))
        return cur.rowcount

    def apply_server_deletes(self, ews_ids: list[str]) -> tuple[int, int]:
        """A delete event arrived from Exchange (sync, or our own tool).

        Spec §3: an archived row (captured/verified) is KEPT and marked
        deleted — the mail is ours now; a live row is dropped as before; an
        already-deleted row is left alone. Returns (dropped, tombstoned)."""
        if not ews_ids:
            return 0, 0
        ids = list(ews_ids)
        with self.db.conn() as c:
            tombstoned = c.execute(
                "UPDATE ews.messages SET archive_state = 'deleted', "
                "deleted_at = now() WHERE ews_id = ANY(%s) "
                "AND archive_state IN ('captured', 'verified')", (ids,)).rowcount
            dropped = c.execute(
                "DELETE FROM ews.messages WHERE ews_id = ANY(%s) "
                "AND archive_state = 'live'", (ids,)).rowcount
        return dropped, tombstoned

    def archive_state_counts(self) -> dict[str, int]:
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT archive_state, COUNT(*) AS n FROM ews.messages "
                "GROUP BY archive_state").fetchall()
        counts = {"live": 0, "captured": 0, "verified": 0, "deleted": 0}
        counts.update({r["archive_state"]: int(r["n"]) for r in rows})
        return counts

    def archived_counts_by_folder(self) -> dict[str, int]:
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT folder_id, COUNT(*) AS n FROM ews.messages "
                "WHERE archive_state <> 'live' GROUP BY folder_id").fetchall()
        return {r["folder_id"]: int(r["n"]) for r in rows}

    def messages_by_ids(self, ews_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not ews_ids:
            return {}
        with self.db.conn() as c:
            rows = c.execute("SELECT * FROM ews.messages WHERE ews_id = ANY(%s)",
                             (list(ews_ids),)).fetchall()
        return {r["ews_id"]: r for r in rows}

    # ---------------------------------------------------------- attachments

    def replace_attachments(self, ews_id: str,
                            rows: list[dict[str, Any]]) -> None:
        """Idempotent: capture is re-runnable, so the inventory is rewritten."""
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.attachments WHERE message_ews_id = %s",
                      (ews_id,))
            if rows:
                c.cursor().executemany(
                    "INSERT INTO ews.attachments (message_ews_id, name, "
                    "content_type, size, sha256, is_inline) VALUES "
                    "(%(message_ews_id)s, %(name)s, %(content_type)s, %(size)s, "
                    "%(sha256)s, %(is_inline)s)",
                    [{"message_ews_id": ews_id, "name": r.get("name"),
                      "content_type": r.get("content_type"), "size": r.get("size"),
                      "sha256": r.get("sha256"),
                      "is_inline": int(r.get("is_inline") or 0)} for r in rows])

    def attachments_for(self, ews_id: str) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT id, name, content_type, size, sha256, is_inline "
                "FROM ews.attachments WHERE message_ews_id = %s ORDER BY id ASC",
                (ews_id,)).fetchall()

    # -------------------------------------------------------- archive_runs

    def start_run(self, kind: str, *, dry_run: bool, policy: dict[str, Any]) -> int:
        with self.db.conn() as c:
            row = c.execute(
                "INSERT INTO ews.archive_runs (kind, dry_run, policy_json) "
                "VALUES (%s, %s, %s) RETURNING id",
                (kind, 1 if dry_run else 0,
                 json.dumps(policy, sort_keys=True, default=str))).fetchone()
        return int(row["id"])

    def finish_run(self, run_id: int, *, captured: int = 0, verified: int = 0,
                   deleted: int = 0, failed: int = 0, error: str | None = None,
                   sample: list[Any] | None = None) -> None:
        with self.db.conn() as c:
            c.execute(
                "UPDATE ews.archive_runs SET finished_at = now(), captured = %s, "
                "verified = %s, deleted = %s, failed = %s, error = %s, "
                "sample_json = %s WHERE id = %s",
                (int(captured), int(verified), int(deleted), int(failed),
                 (error or None) and str(error)[:2000],
                 json.dumps(sample or [], ensure_ascii=False, default=str),
                 int(run_id)))

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self.db.conn() as c:
            return c.execute("SELECT * FROM ews.archive_runs WHERE id = %s",
                             (int(run_id),)).fetchone()

    def recent_runs(self, limit: int = 5) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT * FROM ews.archive_runs ORDER BY started_at DESC, id DESC "
                "LIMIT %s", (int(limit),)).fetchall()
```

Also point the existing write-through alias at the new rule:

```python
    tombstone_messages = apply_server_deletes
```

(replacing `tombstone_messages = delete_messages_by_id`; `delete_messages_by_id` stays for callers that genuinely want the row gone).

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_archive_store.py tests/test_archive_schema.py -q`
Expected: PASS. Remove the `xfail` marker added in Task 1 Step 4 and confirm the cascade test now passes for real.

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m pytest tests -q`
Expected: PASS. `tests/test_writes.py` asserts `delete_messages` write-through — the behaviour changed for archived rows only, so live-row deletion still removes the row.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/cache/store.py tests/test_archive_store.py tests/test_archive_schema.py tests/conftest.py
git commit -m "$(cat <<'EOF'
feat(archive): store — candidates, state machine, attachments, run ledger

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 5: `embeddings.py` — the Embedder interface and the Gemini client

**Files:**
- Create: `ewsmcp/embeddings.py`
- Modify: `tests/conftest.py` (deterministic `FakeEmbedder`)
- Test: `tests/test_embeddings.py`

**Interfaces:**
- Produces:

```python
QUERY_PREFIX = "Retrieve email messages relevant to the query: "
GEMINI_MODEL = "gemini-embedding-2"
MAX_BATCH = 100
CHUNK_CHARS = 1500

class EmbeddingError(RuntimeError): ...

class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...

def chunk_text(subject: str, body: str, chunk_chars: int = CHUNK_CHARS) -> list[str]

class GeminiEmbedder:
    def __init__(self, api_key: str, *, dims: int = 768, model: str = GEMINI_MODEL,
                 batch: int = MAX_BATCH, client: Any | None = None,
                 max_attempts: int = 5, base_delay: float = 1.0,
                 sleep: Callable[[float], None] = time.sleep) -> None
    def embed(self, texts: list[str]) -> list[list[float]]
    def close(self) -> None
```

- [ ] **Step 1: Write the failing test**

Add to `tests/conftest.py`:

```python
class FakeEmbedder:
    """Deterministic, offline stand-in for GeminiEmbedder.

    Hashes each whitespace token into one of `dims` buckets, so texts that
    share vocabulary land near each other under cosine distance and the same
    text always yields the same vector. No network, ever.
    """

    def __init__(self, dims: int = 768):
        self.dims = dims
        self.calls: list[list[str]] = []

    def embed(self, texts):
        import hashlib
        import math
        self.calls.append(list(texts))
        out = []
        for text in texts:
            vec = [0.0] * self.dims
            for token in (text or "").lower().split():
                h = int(hashlib.sha256(token.encode()).hexdigest()[:8], 16)
                vec[h % self.dims] += 1.0
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            out.append([v / norm for v in vec])
        return out
```

Create `tests/test_embeddings.py`:

```python
"""Embeddings: chunking, the Gemini request/response contract, and backoff.
Not one test touches the network — httpx is driven by a MockTransport."""

import json

import httpx
import pytest
from conftest import FakeEmbedder

from ewsmcp.embeddings import (
    GEMINI_MODEL,
    QUERY_PREFIX,
    EmbeddingError,
    GeminiEmbedder,
    chunk_text,
)


def test_chunk_text_joins_subject_and_body_and_splits_at_1500_chars():
    chunks = chunk_text("Budget", "x" * 3200)
    assert chunks[0].startswith("Budget\n")
    assert all(len(c) <= 1500 for c in chunks)
    assert len(chunks) == 3
    assert "".join(chunks) == "Budget\n" + "x" * 3200


def test_chunk_text_of_an_empty_body_is_the_subject_alone():
    assert chunk_text("Budget", "") == ["Budget"]
    assert chunk_text("", "") == []


def _transport(recorder, *responses):
    calls = iter(responses)

    def handler(request):
        recorder.append(json.loads(request.content))
        return next(calls)

    return httpx.MockTransport(handler)


def _ok(n, dims=768):
    return httpx.Response(200, json={"embeddings": [
        {"values": [0.1] * dims} for _ in range(n)]})


def test_request_shape_matches_the_gemini_batch_contract():
    seen = []
    client = httpx.Client(transport=_transport(seen, _ok(2)))
    emb = GeminiEmbedder("KEY", client=client)
    vectors = emb.embed(["alpha", "beta"])
    assert len(vectors) == 2 and len(vectors[0]) == 768
    body = seen[0]
    assert body["requests"][0] == {
        "model": f"models/{GEMINI_MODEL}",
        "content": {"parts": [{"text": "alpha"}]},
        "outputDimensionality": 768,
    }


def test_the_api_key_travels_in_the_query_string():
    emb = GeminiEmbedder("SECRET")
    assert emb.url.endswith("batchEmbedContents?key=SECRET")
    assert GEMINI_MODEL in emb.url
    emb.close()


def test_batches_are_capped_at_100():
    seen = []
    client = httpx.Client(transport=_transport(seen, _ok(100), _ok(20)))
    vectors = GeminiEmbedder("K", client=client).embed([f"t{i}" for i in range(120)])
    assert len(vectors) == 120
    assert [len(b["requests"]) for b in seen] == [100, 20]


def test_429_is_retried_with_exponential_backoff():
    seen, slept = [], []
    client = httpx.Client(transport=_transport(
        seen, httpx.Response(429, json={}), httpx.Response(503, json={}), _ok(1)))
    emb = GeminiEmbedder("K", client=client, base_delay=1.0, sleep=slept.append)
    assert len(emb.embed(["x"])) == 1
    assert slept == [1.0, 2.0]
    assert len(seen) == 3


def test_a_permanent_400_raises_without_retrying():
    seen = []
    client = httpx.Client(transport=_transport(
        seen, httpx.Response(400, json={"error": {"message": "bad"}})))
    with pytest.raises(EmbeddingError, match="400"):
        GeminiEmbedder("K", client=client, sleep=lambda s: None).embed(["x"])
    assert len(seen) == 1


def test_exhausted_retries_raise():
    seen = []
    client = httpx.Client(transport=_transport(seen, *[httpx.Response(429, json={})] * 3))
    emb = GeminiEmbedder("K", client=client, max_attempts=3, sleep=lambda s: None)
    with pytest.raises(EmbeddingError):
        emb.embed(["x"])


def test_a_short_or_wrong_width_response_is_an_error():
    seen = []
    client = httpx.Client(transport=_transport(seen, _ok(1, dims=3)))
    with pytest.raises(EmbeddingError, match="768"):
        GeminiEmbedder("K", client=client, sleep=lambda s: None).embed(["x"])


def test_query_prefix_is_the_documented_task_instruction():
    assert QUERY_PREFIX == "Retrieve email messages relevant to the query: "


def test_fake_embedder_is_deterministic_and_768_wide():
    fake = FakeEmbedder()
    a, b = fake.embed(["budget review", "budget review"])
    assert a == b and len(a) == 768
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_embeddings.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'ewsmcp.embeddings'`.

- [ ] **Step 3: Write the implementation**

Create `ewsmcp/embeddings.py`:

```python
"""Text → vector, behind one small interface.

``Embedder`` is a Protocol on purpose: the daemon injects ``GeminiEmbedder``
(remote, rate-limited, occasionally down), tests inject a deterministic local
fake, and neither the ``SemanticIndex`` nor the archive worker knows or cares
which it got. That is what keeps the whole embedding path testable offline.

gemini-embedding-2 takes its task instruction IN THE PROMPT, not as a
parameter, so query text is prefixed with ``QUERY_PREFIX`` while indexed
document text is embedded bare.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Protocol, runtime_checkable

import httpx

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-embedding-2"
GEMINI_ENDPOINT = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:batchEmbedContents"
)
QUERY_PREFIX = "Retrieve email messages relevant to the query: "
MAX_BATCH = 100
CHUNK_CHARS = 1500
_RETRY_STATUS = frozenset({408, 429, 500, 502, 503, 504})


class EmbeddingError(RuntimeError):
    """The embedder could not produce vectors. Callers degrade to keyword."""


@runtime_checkable
class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one vector per input text, in order."""


def chunk_text(subject: str, body: str,
               chunk_chars: int = CHUNK_CHARS) -> list[str]:
    """``subject + "\\n" + body`` split into fixed-width character chunks."""
    subject = (subject or "").strip()
    body = body or ""
    text = f"{subject}\n{body}" if subject and body else (subject or body)
    text = text.strip("\n") if not subject or not body else text
    if not text.strip():
        return []
    size = max(1, int(chunk_chars))
    return [text[i:i + size] for i in range(0, len(text), size)]


class GeminiEmbedder:
    """Gemini's batchEmbedContents over plain httpx — no vendor SDK."""

    def __init__(self, api_key: str, *, dims: int = 768,
                 model: str = GEMINI_MODEL, batch: int = MAX_BATCH,
                 client: Any | None = None, max_attempts: int = 5,
                 base_delay: float = 1.0,
                 sleep: Callable[[float], None] = time.sleep,
                 timeout: float = 60.0) -> None:
        if not api_key:
            raise ValueError("GeminiEmbedder needs an API key")
        self.model = model
        self.dims = int(dims)
        self.batch = max(1, min(int(batch), MAX_BATCH))
        self.max_attempts = max(1, int(max_attempts))
        self.base_delay = float(base_delay)
        self._sleep = sleep
        # The key is a query parameter. It is never logged: error messages
        # below quote the status and the response body, never the URL.
        self.url = f"{GEMINI_ENDPOINT.format(model=model)}?key={api_key}"
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for start in range(0, len(texts), self.batch):
            out.extend(self._embed_batch(texts[start:start + self.batch]))
        return out

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        payload = {"requests": [
            {"model": f"models/{self.model}",
             "content": {"parts": [{"text": t}]},
             "outputDimensionality": self.dims}
            for t in texts
        ]}
        delay = self.base_delay
        last = ""
        for attempt in range(self.max_attempts):
            try:
                resp = self._client.post(self.url, json=payload)
            except httpx.HTTPError as exc:
                last = f"{type(exc).__name__}: {exc}"
                if attempt == self.max_attempts - 1:
                    break
                self._sleep(delay)
                delay *= 2
                continue
            if resp.status_code in _RETRY_STATUS:
                last = f"HTTP {resp.status_code}"
                if attempt == self.max_attempts - 1:
                    break
                logger.warning("gemini embed %s — retrying in %.0fs",
                               last, delay)
                self._sleep(delay)
                delay *= 2
                continue
            if resp.status_code >= 400:
                raise EmbeddingError(
                    f"gemini embed failed: HTTP {resp.status_code} "
                    f"{resp.text[:200]}")
            return self._vectors(resp, len(texts))
        raise EmbeddingError(
            f"gemini embed failed after {self.max_attempts} attempts ({last})")

    def _vectors(self, resp: Any, expected: int) -> list[list[float]]:
        try:
            data = resp.json()
        except ValueError as exc:
            raise EmbeddingError("gemini embed returned non-JSON") from exc
        vectors = [e.get("values") for e in (data.get("embeddings") or [])]
        if len(vectors) != expected:
            raise EmbeddingError(
                f"gemini embed returned {len(vectors)} vectors for "
                f"{expected} texts")
        for vec in vectors:
            if not isinstance(vec, list) or len(vec) != self.dims:
                raise EmbeddingError(
                    f"gemini embed returned a vector of width "
                    f"{len(vec) if isinstance(vec, list) else '?'}, "
                    f"expected {self.dims}")
        return [[float(x) for x in vec] for vec in vectors]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_embeddings.py -q`
Expected: PASS (11 passed)

- [ ] **Step 5: Commit**

```bash
git add ewsmcp/embeddings.py tests/test_embeddings.py tests/conftest.py
git commit -m "$(cat <<'EOF'
feat(semantic): Embedder protocol, GeminiEmbedder (httpx + backoff), chunking

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 6: `semantic.py` — chunk storage, vector search, hybrid RRF

**Files:**
- Modify: `ewsmcp/cache/store.py` (chunk + vector queries)
- Create: `ewsmcp/semantic.py`
- Test: `tests/test_semantic.py`

**Interfaces:**
- Consumes: `CacheStore` (Task 4), `Embedder`, `chunk_text`, `QUERY_PREFIX`, `EmbeddingError` (Task 5), `CacheStore.search_messages(..., archived=…)` (already exists).
- Produces on `CacheStore`:

```python
def unembedded_messages(self, limit: int) -> list[dict]
def replace_chunks(self, ews_id: str, chunks: list[dict]) -> None   # {seq, source, text, embedding}
def mark_embedded(self, ews_ids: list[str]) -> None
def embedding_backlog(self) -> int
def embedded_count(self) -> int
def similar_message_ids(self, embedding: list[float], *, limit: int,
                        archived: str = "any",
                        exclude_ews_id: str | None = None) -> list[tuple[str, float]]
```

- Produces in `ewsmcp/semantic.py`:

```python
RRF_K = 60

class SemanticIndex:
    def __init__(self, store, embedder, *, chunk_chars: int = 1500,
                 batch: int = 100) -> None
    def index_messages(self, rows: list[dict]) -> int
    def vector_ids(self, text: str, *, limit: int, archived: str = "any",
                   exclude_ews_id: str | None = None) -> list[tuple[str, float]]
    def similar_to_message(self, ews_id: str, *, limit: int,
                           archived: str = "any") -> list[dict]
    def hybrid_search(self, query: str, *, limit: int, offset: int = 0,
                      archived: str = "any", **filters) -> tuple[list[dict], bool]
```

`hybrid_search` returns `(rows, degraded)`; `degraded=True` means the vector half failed and the result is pure keyword.

- [ ] **Step 1: Write the failing test**

Create `tests/test_semantic.py`:

```python
"""Vector + hybrid search over real pgvector, with a deterministic fake embedder."""

import pytest
from conftest import FakeEmbedder, make_row

from ewsmcp.cache.store import CacheStore
from ewsmcp.embeddings import QUERY_PREFIX, EmbeddingError
from ewsmcp.semantic import RRF_K, SemanticIndex


@pytest.fixture
def indexed(db):
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0}])
    store.upsert_messages([
        make_row("M-BUDGET", subject="Quarterly budget",
                 body="the budget forecast spreadsheet for finance"),
        make_row("M-LUNCH", subject="Lunch plans",
                 body="shawarma at noon near the office"),
        make_row("M-FORECAST", subject="Forecast update",
                 body="finance forecast numbers revised upward"),
    ])
    index = SemanticIndex(store, FakeEmbedder())
    index.index_messages(store.unembedded_messages(100))
    return store, index


def test_index_messages_writes_chunks_and_stamps_embedded_at(indexed):
    store, _index = indexed
    assert store.embedding_backlog() == 0
    assert store.embedded_count() == 3
    assert store.get_message("M-BUDGET")["embedded_at"] is not None
    with store.db.conn() as c:
        row = c.execute("SELECT source, seq, text, embedding IS NOT NULL AS has_vec "
                        "FROM ews.chunks WHERE message_ews_id = 'M-BUDGET'").fetchone()
    assert row["source"] == "body" and row["seq"] == 0 and row["has_vec"]
    assert "Quarterly budget" in row["text"]


def test_reindexing_replaces_chunks_instead_of_duplicating(indexed):
    store, index = indexed
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET embedded_at = NULL")
    index.index_messages(store.unembedded_messages(100))
    with store.db.conn() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM ews.chunks "
                      "WHERE message_ews_id = 'M-BUDGET'").fetchone()["n"]
    assert n == 1


def test_a_long_body_becomes_several_chunks(db):
    store = CacheStore(db)
    store.upsert_messages([make_row("M-LONG", subject="Long", body="word " * 900)])
    SemanticIndex(store, FakeEmbedder()).index_messages(store.unembedded_messages(10))
    with store.db.conn() as c:
        seqs = [r["seq"] for r in c.execute(
            "SELECT seq FROM ews.chunks WHERE message_ews_id = 'M-LONG' "
            "ORDER BY seq").fetchall()]
    assert seqs == [0, 1, 2]


def test_vector_search_ranks_the_related_message_first(indexed):
    _store, index = indexed
    ids = [i for i, _d in index.vector_ids("finance forecast", limit=3)]
    assert ids[0] in ("M-FORECAST", "M-BUDGET")
    assert ids[-1] == "M-LUNCH"


def test_query_text_is_embedded_with_the_task_instruction(indexed):
    _store, index = indexed
    index.embedder.calls.clear()
    index.vector_ids("budget", limit=2)
    assert index.embedder.calls[-1] == [QUERY_PREFIX + "budget"]


def test_similar_to_message_excludes_the_seed(indexed):
    _store, index = indexed
    ids = [r["ews_id"] for r in index.similar_to_message("M-BUDGET", limit=5)]
    assert "M-BUDGET" not in ids and ids


def test_archived_filter_applies_to_vector_search(indexed):
    store, index = indexed
    store.mark_captured("M-FORECAST", mime_sha256="a" * 64, mime_path="/x.eml")
    only = [i for i, _d in index.vector_ids("finance forecast", limit=5,
                                            archived="only")]
    assert only == ["M-FORECAST"]
    excl = [i for i, _d in index.vector_ids("finance forecast", limit=5,
                                            archived="exclude")]
    assert "M-FORECAST" not in excl


def test_hybrid_fuses_both_rankings_with_rrf(indexed):
    _store, index = indexed
    rows, degraded = index.hybrid_search("budget forecast", limit=3)
    assert degraded is False
    assert [r["ews_id"] for r in rows][:2] == ["M-BUDGET", "M-FORECAST"] or \
           [r["ews_id"] for r in rows][:2] == ["M-FORECAST", "M-BUDGET"]
    assert "M-LUNCH" not in [r["ews_id"] for r in rows][:1]


def test_hybrid_finds_a_message_only_one_engine_can_see(indexed):
    """RRF's whole point: keyword-only and vector-only hits both survive."""
    _store, index = indexed
    rows, _ = index.hybrid_search("shawarma", limit=3)
    assert "M-LUNCH" in [r["ews_id"] for r in rows]


def test_rrf_constant_is_sixty():
    assert RRF_K == 60


def test_hybrid_degrades_to_keyword_when_the_embedder_fails(indexed):
    store, _index = indexed

    class Broken:
        def embed(self, texts):
            raise EmbeddingError("gemini down")

    index = SemanticIndex(store, Broken())
    rows, degraded = index.hybrid_search("budget", limit=3)
    assert degraded is True
    assert [r["ews_id"] for r in rows] == ["M-BUDGET"]


def test_hybrid_passes_structured_filters_through(indexed):
    _store, index = indexed
    rows, _ = index.hybrid_search("budget", limit=5, sender="nobody@example.com")
    assert rows == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_semantic.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'ewsmcp.semantic'`.

- [ ] **Step 3a: Add the chunk and vector queries to the store**

Append to `class CacheStore` in `ewsmcp/cache/store.py`:

```python
    # -------------------------------------------------------------- chunks

    def unembedded_messages(self, limit: int) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            return c.execute(
                "SELECT ews_id, subject, body_clean FROM ews.messages "
                "WHERE embedded_at IS NULL ORDER BY date_ts DESC NULLS LAST "
                "LIMIT %s", (int(limit),)).fetchall()

    def replace_chunks(self, ews_id: str, chunks: list[dict[str, Any]]) -> None:
        """Rewrite one message's chunks. `embedding` is a list[float]."""
        with self.db.conn() as c:
            c.execute("DELETE FROM ews.chunks WHERE message_ews_id = %s", (ews_id,))
            if chunks:
                c.cursor().executemany(
                    "INSERT INTO ews.chunks (message_ews_id, seq, source, text, "
                    "embedding) VALUES (%(message_ews_id)s, %(seq)s, %(source)s, "
                    "%(text)s, %(embedding)s::vector)",
                    [{"message_ews_id": ews_id, "seq": int(ch["seq"]),
                      "source": ch.get("source", "body"), "text": ch["text"],
                      "embedding": _vector_literal(ch.get("embedding"))}
                     for ch in chunks])

    def mark_embedded(self, ews_ids: list[str]) -> None:
        if not ews_ids:
            return
        with self.db.conn() as c:
            c.execute("UPDATE ews.messages SET embedded_at = now() "
                      "WHERE ews_id = ANY(%s)", (list(ews_ids),))

    def embedding_backlog(self) -> int:
        with self.db.conn() as c:
            return int(c.execute("SELECT COUNT(*) AS n FROM ews.messages "
                                 "WHERE embedded_at IS NULL").fetchone()["n"])

    def embedded_count(self) -> int:
        with self.db.conn() as c:
            return int(c.execute("SELECT COUNT(*) AS n FROM ews.messages "
                                 "WHERE embedded_at IS NOT NULL").fetchone()["n"])

    def similar_message_ids(self, embedding: list[float], *, limit: int,
                            archived: str = "any",
                            exclude_ews_id: str | None = None
                            ) -> list[tuple[str, float]]:
        """Nearest messages by cosine distance, MIN over each message's chunks."""
        clause = _ARCHIVED.get(archived, "TRUE")
        params: list[Any] = [_vector_literal(embedding)]
        extra = ""
        if exclude_ews_id:
            extra = "AND c.message_ews_id <> %s "
            params.append(exclude_ews_id)
        params.append(int(limit))
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT c.message_ews_id AS ews_id, "
                "MIN(c.embedding <=> %s::vector) AS dist "
                "FROM ews.chunks c JOIN ews.messages m ON m.ews_id = c.message_ews_id "
                f"WHERE c.embedding IS NOT NULL AND {clause} {extra}"
                "GROUP BY c.message_ews_id ORDER BY dist ASC LIMIT %s",
                params).fetchall()
        return [(r["ews_id"], float(r["dist"])) for r in rows]
```

and this module-level helper next to `_ARCHIVED`:

```python
def _vector_literal(values: Any) -> str:
    """pgvector's text input format: '[0.1,0.2,…]'. psycopg casts it with ::vector."""
    if values is None:
        return "[]"
    return "[" + ",".join(f"{float(v):.7g}" for v in values) + "]"
```

- [ ] **Step 3b: Write `ewsmcp/semantic.py`**

```python
"""Semantic search: chunk → embed → pgvector, and the hybrid fusion on top.

Two rankings answer any query: the tsvector one (exact words, cheap, never
down) and the vector one (meaning, remote, sometimes down). Reciprocal Rank
Fusion combines them without needing their scores to be comparable — each
result scores ``sum(1 / (RRF_K + rank))`` over the lists it appears in — so a
message found by only one engine still surfaces.

When the embedder or the vector query fails, the caller gets keyword results
and ``degraded=True`` instead of an error: a mailbox search must not go dark
because a remote API is rate-limiting us.
"""

from __future__ import annotations

import logging
from typing import Any

from .cache.store import CacheStore
from .embeddings import (
    CHUNK_CHARS,
    MAX_BATCH,
    QUERY_PREFIX,
    Embedder,
    EmbeddingError,
    chunk_text,
)

logger = logging.getLogger(__name__)

RRF_K = 60
# How deep each engine is read before fusing. Fusing only `limit` rows per
# engine would make the union too shallow to re-rank meaningfully.
CANDIDATE_MULTIPLIER = 5
MAX_CANDIDATES = 100


class SemanticIndex:
    def __init__(self, store: CacheStore, embedder: Embedder, *,
                 chunk_chars: int = CHUNK_CHARS, batch: int = MAX_BATCH) -> None:
        self.store = store
        self.embedder = embedder
        self.chunk_chars = int(chunk_chars)
        self.batch = max(1, min(int(batch), MAX_BATCH))

    # ------------------------------------------------------------- indexing

    def index_messages(self, rows: list[dict[str, Any]]) -> int:
        """Chunk, embed and store `rows`; returns the number of messages done.

        Batching is by MESSAGE, never mid-message: ``replace_chunks`` is a full
        rewrite, so a message whose chunks straddled two API batches would have
        its first half deleted by its second half.
        """
        planned: list[tuple[str, list[str]]] = [
            (r["ews_id"], chunk_text(r.get("subject") or "",
                                     r.get("body_clean") or "", self.chunk_chars))
            for r in rows
        ]
        done: list[str] = []
        pending: list[tuple[str, list[str]]] = []
        pending_size = 0
        for ews_id, chunks in planned:
            if not chunks:                    # nothing to embed — still "done"
                self.store.replace_chunks(ews_id, [])
                done.append(ews_id)
                continue
            if pending and pending_size + len(chunks) > self.batch:
                done.extend(self._flush(pending))
                pending, pending_size = [], 0
            pending.append((ews_id, chunks))
            pending_size += len(chunks)
        if pending:
            done.extend(self._flush(pending))
        self.store.mark_embedded(done)
        return len(done)

    def _flush(self, pending: list[tuple[str, list[str]]]) -> list[str]:
        texts = [t for _i, chunks in pending for t in chunks]
        vectors = self.embedder.embed(texts)
        if len(vectors) != len(texts):
            raise EmbeddingError(
                f"embedder returned {len(vectors)} vectors for {len(texts)} chunks")
        cursor = 0
        for ews_id, chunks in pending:
            window = vectors[cursor:cursor + len(chunks)]
            cursor += len(chunks)
            self.store.replace_chunks(ews_id, [
                {"seq": seq, "source": "body", "text": text, "embedding": vec}
                for seq, (text, vec) in enumerate(zip(chunks, window))])
        return [i for i, _c in pending]

    # -------------------------------------------------------------- reading

    def vector_ids(self, text: str, *, limit: int, archived: str = "any",
                   exclude_ews_id: str | None = None) -> list[tuple[str, float]]:
        vector = self.embedder.embed([QUERY_PREFIX + (text or "")])[0]
        return self.store.similar_message_ids(
            vector, limit=limit, archived=archived, exclude_ews_id=exclude_ews_id)

    def similar_to_message(self, ews_id: str, *, limit: int,
                           archived: str = "any") -> list[dict[str, Any]]:
        seed = self.store.get_message(ews_id)
        if seed is None:
            return []
        text = f"{seed['subject'] or ''}\n{seed['body_clean'] or ''}"
        hits = self.vector_ids(text, limit=limit, archived=archived,
                               exclude_ews_id=seed["ews_id"])
        by_id = self.store.messages_by_ids([i for i, _d in hits])
        out = []
        for ews_id_hit, dist in hits:
            row = by_id.get(ews_id_hit)
            if row is not None:
                row = dict(row)
                row["similarity"] = round(1.0 - float(dist), 4)
                out.append(row)
        return out

    def hybrid_search(self, query: str, *, limit: int, offset: int = 0,
                      archived: str = "any",
                      **filters: Any) -> tuple[list[dict[str, Any]], bool]:
        depth = min(MAX_CANDIDATES, max(limit + offset, 1) * CANDIDATE_MULTIPLIER)
        keyword_rows, _total = self.store.search_messages(
            text=query, archived=archived, offset=0, limit=depth, **filters)
        keyword_ids = [r["ews_id"] for r in keyword_rows]
        degraded = False
        vector_ids: list[str] = []
        try:
            vector_ids = [i for i, _d in self.vector_ids(
                query, limit=depth, archived=archived)]
        except Exception as exc:  # noqa: BLE001 - degrade, never fail the search
            logger.warning("semantic half unavailable (%s) — keyword only", exc)
            degraded = True
        if degraded or not vector_ids:
            fused_ids = keyword_ids
        else:
            fused_ids = rrf([keyword_ids, vector_ids])
        # Structured filters live on the keyword side; a vector-only hit must
        # still satisfy them, so intersect with what the store would return.
        allowed = set(keyword_ids)
        if any(v is not None for v in filters.values()):
            fused_ids = [i for i in fused_ids if i in allowed]
        window = fused_ids[offset:offset + limit]
        by_id = self.store.messages_by_ids(window)
        return [by_id[i] for i in window if i in by_id], degraded


def rrf(rankings: list[list[str]], k: int = RRF_K) -> list[str]:
    """Reciprocal Rank Fusion over several ranked id lists (rank starts at 1)."""
    scores: dict[str, float] = {}
    first_seen: dict[str, int] = {}
    for ranking in rankings:
        for rank, ident in enumerate(ranking, start=1):
            scores[ident] = scores.get(ident, 0.0) + 1.0 / (k + rank)
            first_seen.setdefault(ident, rank)
    return sorted(scores, key=lambda i: (-scores[i], first_seen[i], i))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_semantic.py -q`
Expected: PASS (12 passed)

- [ ] **Step 5: Run the whole suite and lint**

Run: `.venv/bin/python -m ruff check ewsmcp tests && .venv/bin/python -m pytest tests -q`
Expected: clean, all pass.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/semantic.py ewsmcp/cache/store.py tests/test_semantic.py
git commit -m "$(cat <<'EOF'
feat(semantic): SemanticIndex — pgvector chunks, cosine search, hybrid RRF

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 7: `archive/policy.py` + `archive/capture.py`

**Files:**
- Create: `ewsmcp/archive/policy.py`, `ewsmcp/archive/capture.py`
- Modify: `tests/test_exchangelib_signatures.py` (pin the attachment API)
- Test: `tests/test_archive_capture.py`

**Interfaces:**
- Consumes: `CacheStore.archive_candidates / archive_candidate_count / folder_ids_for_wk / mark_captured / replace_attachments` (Task 4), `archive.files` (Task 3), `Settings` (Task 2), `EWSGateway.call(fn)` (existing).
- Produces:

```python
# policy.py
@dataclass(frozen=True)
class ArchivePolicy:
    folders: tuple[str, ...]            # normalised well-known keys: ("f:inbox", "f:sent")
    after_days: int
    exclude_categories: tuple[str, ...]
    grace_days: int
    delete_enabled: bool
    max_delete_per_run: int
    min_free_gb: float

    @classmethod
    def from_settings(cls, settings) -> "ArchivePolicy"
    def with_overrides(self, *, before: str | None = None,
                       folders: list[str] | None = None, tz: str = "UTC") -> "ArchivePolicy"
    def capture_cutoff_ts(self, now: float | None = None) -> int
    def delete_cutoff_ts(self, now: float | None = None) -> int
    def grace_instant_ts(self, now: float | None = None) -> int
    def folder_ids(self, store) -> list[str] | None
    def as_dict(self) -> dict

NEVER_ARCHIVED = ("f:calendar", "f:contacts", "f:tasks", "f:drafts", "f:outbox")

# capture.py
CAPTURE_FIELDS = ["mime_content", "changekey", "attachments", "has_attachments"]

class Capturer:
    def __init__(self, settings, gateway, store, policy) -> None
    async def run(self, *, limit: int = 25, dry_run: bool = True) -> dict
```

`Capturer.run` returns `{"candidates": int, "captured": int, "failed": int, "sample": list[dict], "stopped": str | None}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_archive_capture.py`:

```python
"""The capturer: policy selection, MIME + blob writing, attachment rows."""

import asyncio
import time
from types import SimpleNamespace

import pytest
from conftest import make_row, make_settings

from ewsmcp.archive import files
from ewsmcp.archive.capture import CAPTURE_FIELDS, Capturer
from ewsmcp.archive.policy import NEVER_ARCHIVED, ArchivePolicy
from ewsmcp.cache.store import CacheStore

NOW = int(time.time())
DAY = 86400


class FakeAttachment:
    """Stands in for exchangelib.FileAttachment."""

    def __init__(self, name, content, content_type="application/pdf",
                 is_inline=False):
        self.name = name
        self.content = content
        self.content_type = content_type
        self.size = len(content)
        self.is_inline = is_inline


class FakeItem:
    def __init__(self, raw_id, mime=b"MIME", attachments=(), changekey="CK"):
        self.id = raw_id
        self.changekey = changekey
        self.mime_content = mime
        self.attachments = list(attachments)
        self.has_attachments = bool(attachments)


class FakeAccount:
    def __init__(self, items):
        self.items = items                 # {raw_id: FakeItem | Exception}
        self.fetch_calls = []

    def fetch(self, ids=None, only_fields=None, **kw):
        self.fetch_calls.append((list(ids), list(only_fields or [])))
        return [self.items[i] for i, _ck in ids]


class FakeGatewayFor:
    def __init__(self, account):
        self.account = account

    async def call(self, fn):
        return fn(self.account)


@pytest.fixture
def seeded(db, tmp_path):
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0},
        {"ews_id": "FID-PROJ", "name": "Projects", "path": "Projects", "wk": None,
         "total": 0, "unread": 0, "children": 0}])
    store.upsert_messages([
        make_row("OLD-1", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY),
        make_row("OLD-2", folder_id="FID-INBOX", date_ts=NOW - 290 * DAY),
        make_row("NEW-1", folder_id="FID-INBOX", date_ts=NOW - 3 * DAY),
        make_row("OTHER", folder_id="FID-PROJ", date_ts=NOW - 300 * DAY),
    ])
    settings = make_settings(data_dir=str(tmp_path / "data"))
    return store, settings


def _policy(settings, **over):
    return ArchivePolicy.from_settings(settings).with_overrides(**over) \
        if over else ArchivePolicy.from_settings(settings)


# --- policy -------------------------------------------------------------------


def test_policy_normalises_folder_keys(seeded):
    _store, settings = seeded
    policy = ArchivePolicy.from_settings(
        make_settings(archive_folders="inbox, f:sent "))
    assert policy.folders == ("f:inbox", "f:sent")


def test_policy_never_archives_calendar_contacts_or_tasks():
    with pytest.raises(ValueError, match="never archived"):
        ArchivePolicy.from_settings(make_settings(archive_folders="inbox,calendar"))
    assert "f:calendar" in NEVER_ARCHIVED


def test_capture_cutoff_is_after_days_ago():
    policy = ArchivePolicy.from_settings(make_settings(archive_after_days=180))
    assert abs(policy.capture_cutoff_ts(now=NOW) - (NOW - 180 * DAY)) < 2


def test_delete_cutoff_adds_the_grace_period():
    policy = ArchivePolicy.from_settings(
        make_settings(archive_after_days=180, archive_grace_days=7))
    assert abs(policy.delete_cutoff_ts(now=NOW) - (NOW - 187 * DAY)) < 2
    assert abs(policy.grace_instant_ts(now=NOW) - (NOW - 7 * DAY)) < 2


def test_overrides_narrow_the_window_and_the_folders():
    policy = ArchivePolicy.from_settings(make_settings()).with_overrides(
        before="2026-01-01", folders=["sent"], tz="Asia/Riyadh")
    assert policy.folders == ("f:sent",)
    assert policy.capture_cutoff_ts(now=NOW) < NOW - 200 * DAY


def test_folder_ids_resolve_through_the_folders_table(seeded):
    store, settings = seeded
    assert _policy(settings).folder_ids(store) == ["FID-INBOX"]


# --- capture ------------------------------------------------------------------


def test_dry_run_touches_neither_exchange_nor_disk(seeded):
    store, settings = seeded
    account = FakeAccount({})
    cap = Capturer(settings, FakeGatewayFor(account), store, _policy(settings))
    result = asyncio.run(cap.run(limit=25, dry_run=True))
    assert result["candidates"] == 2 and result["captured"] == 0
    assert {s["ews_id"] for s in result["sample"]} == {"OLD-1", "OLD-2"}
    assert account.fetch_calls == []
    assert store.get_message("OLD-1")["archive_state"] == "live"


def test_capture_writes_mime_blobs_and_rows(seeded):
    store, settings = seeded
    pdf = b"%PDF-1.4 quarterly"
    account = FakeAccount({
        "OLD-1": FakeItem("OLD-1", mime=b"RAW-MIME-1",
                          attachments=[FakeAttachment("q3.pdf", pdf)]),
        "OLD-2": FakeItem("OLD-2", mime=b"RAW-MIME-2"),
    })
    cap = Capturer(settings, FakeGatewayFor(account), store, _policy(settings))
    result = asyncio.run(cap.run(limit=25, dry_run=False))
    assert result["captured"] == 2 and result["failed"] == 0

    row = store.get_message("OLD-1")
    assert row["archive_state"] == "captured"
    assert row["mime_sha256"] == files.sha256_bytes(b"RAW-MIME-1")
    assert files.mime_path(settings.data_dir, row["mime_sha256"]).read_bytes() \
        == b"RAW-MIME-1"

    atts = store.attachments_for("OLD-1")
    assert len(atts) == 1
    assert atts[0]["name"] == "q3.pdf" and atts[0]["size"] == len(pdf)
    assert files.blob_path(settings.data_dir, atts[0]["sha256"]).read_bytes() == pdf


def test_capture_projects_only_the_fields_it_needs(seeded):
    store, settings = seeded
    account = FakeAccount({"OLD-1": FakeItem("OLD-1"), "OLD-2": FakeItem("OLD-2")})
    asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                         _policy(settings)).run(dry_run=False))
    assert account.fetch_calls[0][1] == CAPTURE_FIELDS


def test_item_attachments_are_recorded_without_a_blob(seeded):
    store, settings = seeded
    nested = SimpleNamespace(name="Fwd: contract", is_inline=False, size=900)
    account = FakeAccount({
        "OLD-1": FakeItem("OLD-1", attachments=[nested]),
        "OLD-2": FakeItem("OLD-2"),
    })
    asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                         _policy(settings)).run(dry_run=False))
    att = store.attachments_for("OLD-1")[0]
    assert att["content_type"] == "message/rfc822"
    assert att["sha256"] is None
    assert att["name"] == "Fwd: contract"


def test_one_bad_item_is_skipped_not_fatal(seeded):
    store, settings = seeded
    account = FakeAccount({
        "OLD-1": ValueError("ErrorItemNotFound"),
        "OLD-2": FakeItem("OLD-2"),
    })
    result = asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                                  _policy(settings)).run(dry_run=False))
    assert result["captured"] == 1 and result["failed"] == 1
    assert store.get_message("OLD-1")["archive_state"] == "live"
    assert store.get_message("OLD-2")["archive_state"] == "captured"


def test_capture_is_idempotent(seeded):
    store, settings = seeded
    account = FakeAccount({
        "OLD-1": FakeItem("OLD-1", attachments=[FakeAttachment("a.pdf", b"AAA")]),
        "OLD-2": FakeItem("OLD-2"),
    })
    cap = Capturer(settings, FakeGatewayFor(account), store, _policy(settings))
    asyncio.run(cap.run(dry_run=False))
    store.reset_to_live("OLD-1")
    asyncio.run(cap.run(dry_run=False))
    assert len(store.attachments_for("OLD-1")) == 1


def test_low_disk_stops_the_run_before_fetching(seeded, monkeypatch):
    store, settings = seeded
    monkeypatch.setattr(files.shutil, "disk_usage", lambda p: (100, 99, 1))
    account = FakeAccount({})
    result = asyncio.run(Capturer(settings, FakeGatewayFor(account), store,
                                  _policy(settings)).run(dry_run=False))
    assert result["captured"] == 0
    assert "ARCHIVE_MIN_FREE_GB" in result["stopped"]
    assert account.fetch_calls == []
```

Also pin the attachment API in `tests/test_exchangelib_signatures.py`:

```python
def test_file_attachment_exposes_the_fields_capture_reads():
    from exchangelib.attachments import FileAttachment, ItemAttachment
    for field in ("name", "content_type", "size", "is_inline", "content"):
        assert hasattr(FileAttachment, field), field
    assert issubclass(ItemAttachment, object)


def test_message_exposes_mime_content():
    from exchangelib import Message
    assert "mime_content" in {f.name for f in Message.FIELDS}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_archive_capture.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'ewsmcp.archive.policy'`.

- [ ] **Step 3a: Write `ewsmcp/archive/policy.py`**

```python
"""What may be archived, and when.

The policy is a frozen value object: the runner builds one per pass from
settings (optionally narrowed by the tool's `before`/`folders` arguments) and
hands the SAME object to capturer, verifier and deleter, so a run cannot half
apply one policy and half another.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any

from ..dates import parse_when

# Spec §3: mail only. Drafts and the outbox are excluded too — a draft has no
# server copy worth keeping and an outbox item is mid-flight.
NEVER_ARCHIVED = ("f:calendar", "f:contacts", "f:tasks", "f:drafts", "f:outbox")
DAY = 86400


def _normalise(keys: Any) -> tuple[str, ...]:
    out: list[str] = []
    raw = keys.split(",") if isinstance(keys, str) else list(keys or [])
    for key in raw:
        key = str(key).strip().lower()
        if not key:
            continue
        if not key.startswith("f:"):
            key = f"f:{key}"
        if key in NEVER_ARCHIVED:
            raise ValueError(
                f"{key} is never archived (calendar, contacts, tasks, drafts "
                "and the outbox are out of scope by design)")
        if key not in out:
            out.append(key)
    return tuple(out)


@dataclass(frozen=True)
class ArchivePolicy:
    folders: tuple[str, ...]
    after_days: int
    exclude_categories: tuple[str, ...]
    grace_days: int
    delete_enabled: bool
    max_delete_per_run: int
    min_free_gb: float
    before_ts: int | None = None  # explicit cutoff from archive_run(before=…)

    @classmethod
    def from_settings(cls, settings: Any) -> "ArchivePolicy":
        return cls(
            folders=_normalise(settings.archive_folders),
            after_days=int(settings.archive_after_days),
            exclude_categories=tuple(
                c.strip().lower()
                for c in (settings.archive_exclude_categories or "").split(",")
                if c.strip()),
            grace_days=int(settings.archive_grace_days),
            delete_enabled=bool(settings.archive_delete_enabled),
            max_delete_per_run=int(settings.archive_max_delete_per_run),
            min_free_gb=float(settings.archive_min_free_gb),
        )

    def with_overrides(self, *, before: str | None = None,
                       folders: list[str] | None = None,
                       tz: str = "UTC") -> "ArchivePolicy":
        from dataclasses import replace
        changes: dict[str, Any] = {}
        if folders:
            changes["folders"] = _normalise(folders)
        if before:
            changes["before_ts"] = int(parse_when(before, "before", tz).timestamp())
        return replace(self, **changes) if changes else self

    # ------------------------------------------------------------- cutoffs

    def capture_cutoff_ts(self, now: float | None = None) -> int:
        if self.before_ts is not None:
            return int(self.before_ts)
        return int((now if now is not None else time.time())
                   - self.after_days * DAY)

    def delete_cutoff_ts(self, now: float | None = None) -> int:
        """Cutoff PLUS the grace period — rail 3, the date half."""
        return self.capture_cutoff_ts(now) - self.grace_days * DAY

    def grace_instant_ts(self, now: float | None = None) -> int:
        """A row must have been verified at least grace_days ago — rail 3, the
        verification half. Freshly verified mail is never deleted, however old."""
        return int((now if now is not None else time.time())
                   - self.grace_days * DAY)

    # ------------------------------------------------------------- folders

    def folder_ids(self, store: Any) -> list[str] | None:
        """None means 'every mirrored folder' (no ARCHIVE_FOLDERS configured)."""
        if not self.folders:
            return None
        return store.folder_ids_for_wk(list(self.folders))

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
```

- [ ] **Step 3b: Write `ewsmcp/archive/capture.py`**

```python
"""Capture: pull the raw MIME and every attachment of one batch of live mail.

The MIME is the durable artifact — it round-trips into any mail client and
carries nested ItemAttachments that have no standalone bytes. Attachment blobs
are stored SEPARATELY as well, deduplicated by hash, so `get_attachment` on
archived mail costs one file read instead of a MIME parse.

Failure posture: one bad item is logged and skipped (its row stays `live` and
is retried next cycle); a full disk stops the whole run before any fetch.
"""

from __future__ import annotations

import logging
from typing import Any

from exchangelib.attachments import FileAttachment

from . import files
from .policy import ArchivePolicy

logger = logging.getLogger(__name__)

# Never fetch the full item: without a projection exchangelib pulls every
# field, and `attachments` alone already carries the bytes we want.
CAPTURE_FIELDS = ["mime_content", "changekey", "attachments", "has_attachments"]
BATCH_SIZE = 25


class Capturer:
    def __init__(self, settings: Any, gateway: Any, store: Any,
                 policy: ArchivePolicy) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.policy = policy

    async def run(self, *, limit: int = BATCH_SIZE,
                  dry_run: bool = True) -> dict[str, Any]:
        folder_ids = self.policy.folder_ids(self.store)
        cutoff = self.policy.capture_cutoff_ts()
        selector = {"folder_ids": folder_ids, "before_ts": cutoff,
                    "exclude_categories": list(self.policy.exclude_categories)}
        total = self.store.archive_candidate_count(**selector)
        rows = self.store.archive_candidates(**selector, limit=int(limit))
        sample = [{"ews_id": r["ews_id"], "subject": r["subject"],
                   "date": r["date_iso"]} for r in rows[:10]]
        result: dict[str, Any] = {"candidates": total, "captured": 0, "failed": 0,
                                  "sample": sample, "stopped": None}
        if dry_run or not rows:
            return result
        try:
            files.ensure_free_space(self.settings.data_dir, self.policy.min_free_gb)
        except files.DiskFull as exc:
            logger.error("capture stopped: %s", exc)
            result["stopped"] = str(exc)
            return result
        ids = [r["ews_id"] for r in rows]
        captured, failed = await self.gateway.call(
            lambda account: self._capture_batch(account, ids))
        result["captured"], result["failed"] = captured, failed
        return result

    # Runs on the EWS pool (sync).
    def _capture_batch(self, account: Any, ids: list[str]) -> tuple[int, int]:
        captured = failed = 0
        fetched = account.fetch(ids=[(i, None) for i in ids],
                                only_fields=CAPTURE_FIELDS)
        for raw_id, item in zip(ids, fetched):
            try:
                if isinstance(item, Exception):
                    raise item
                self._capture_one(raw_id, item)
                captured += 1
            except Exception as exc:  # noqa: BLE001 - skip one, keep the batch
                failed += 1
                logger.warning("capture failed for %s: %s: %s", raw_id,
                               type(exc).__name__, exc)
        return captured, failed

    def _capture_one(self, raw_id: str, item: Any) -> None:
        mime = getattr(item, "mime_content", None)
        if not isinstance(mime, (bytes, bytearray)):
            raise ValueError(f"no mime_content for {raw_id}")
        data_dir = self.settings.data_dir
        sha, path = files.store_mime(data_dir, bytes(mime))
        rows: list[dict[str, Any]] = []
        for att in list(getattr(item, "attachments", None) or []):
            name = getattr(att, "name", None) or "attachment"
            inline = 1 if getattr(att, "is_inline", False) else 0
            if isinstance(att, FileAttachment):
                content = getattr(att, "content", None)
                if not isinstance(content, (bytes, bytearray)):
                    raise ValueError(f"attachment {name!r} of {raw_id} has no bytes")
                blob_sha, _blob_path = files.store_blob(data_dir, bytes(content))
                rows.append({"name": name,
                             "content_type": getattr(att, "content_type", None),
                             "size": len(content), "sha256": blob_sha,
                             "is_inline": inline})
            else:
                # ItemAttachment (a nested message): it lives inside the MIME
                # only, so there is no blob and nothing to hash.
                rows.append({"name": name, "content_type": "message/rfc822",
                             "size": getattr(att, "size", None), "sha256": None,
                             "is_inline": inline})
        self.store.replace_attachments(raw_id, rows)
        self.store.mark_captured(raw_id, mime_sha256=sha, mime_path=str(path))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_archive_capture.py tests/test_exchangelib_signatures.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ewsmcp/archive/policy.py ewsmcp/archive/capture.py tests/test_archive_capture.py tests/test_exchangelib_signatures.py
git commit -m "$(cat <<'EOF'
feat(archive): ArchivePolicy and the capturer (MIME + blobs + attachment rows)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 8: `archive/verify.py` + `archive/delete.py` — the rails

**Files:**
- Create: `ewsmcp/archive/verify.py`, `ewsmcp/archive/delete.py`
- Test: `tests/test_archive_verify_delete.py`

**Interfaces:**
- Consumes: `CacheStore.captured_rows / mark_verified / reset_to_live / attachments_for / deletable_rows / mark_deleted` (Task 4), `archive.files` (Task 3), `ArchivePolicy` (Task 7), `AuditLog.record(tool=…, side_effect_class=…, outcome=…, latency_ms=…, transport=…, detail=…)` (existing).
- Produces:

```python
# verify.py
VERIFY_FIELDS = ["changekey", "attachments"]

class Verifier:
    def __init__(self, settings, gateway, store) -> None
    async def run(self, *, limit: int = 25) -> dict
    # -> {"verified": int, "reset": int, "failed": int, "reasons": list[dict]}

# delete.py
class Deleter:
    def __init__(self, settings, gateway, store, policy, audit) -> None
    async def run(self, *, dry_run: bool = True, run_id: int | None = None) -> dict
    # -> {"eligible": int, "deleted": int, "failed": int, "blocked": str | None,
    #     "sample": list[dict]}
```

- [ ] **Step 1: Write the failing test**

Create `tests/test_archive_verify_delete.py`:

```python
"""Verifier and deleter: the reset-on-mismatch rule and the three delete rails."""

import asyncio
import time

import pytest
from conftest import make_row, make_settings
from test_archive_capture import FakeAccount, FakeAttachment, FakeGatewayFor, FakeItem

from ewsmcp.archive import files
from ewsmcp.archive.delete import Deleter
from ewsmcp.archive.policy import ArchivePolicy
from ewsmcp.archive.verify import VERIFY_FIELDS, Verifier
from ewsmcp.cache.store import CacheStore

NOW = int(time.time())
DAY = 86400


class RecordingAudit:
    def __init__(self):
        self.records = []

    def record(self, **kw):
        self.records.append(kw)


class DeletableItem(FakeItem):
    def __init__(self, raw_id, **kw):
        super().__init__(raw_id, **kw)
        self.deleted = False

    def delete(self):
        self.deleted = True


@pytest.fixture
def captured(db, tmp_path):
    """One captured message with one attachment, files present on disk."""
    settings = make_settings(data_dir=str(tmp_path / "data"))
    store = CacheStore(db)
    store.upsert_messages([make_row("CAP-1", date_ts=NOW - 300 * DAY)])
    sha, path = files.store_mime(settings.data_dir, b"RAW-MIME")
    blob_sha, _ = files.store_blob(settings.data_dir, b"PDFBYTES")
    store.replace_attachments("CAP-1", [
        {"name": "q3.pdf", "content_type": "application/pdf", "size": 8,
         "sha256": blob_sha, "is_inline": 0}])
    store.mark_captured("CAP-1", mime_sha256=sha, mime_path=str(path))
    return store, settings, sha, blob_sha


# --- verifier -----------------------------------------------------------------


def test_verify_promotes_a_matching_capture(captured):
    store, settings, _sha, _blob = captured
    account = FakeAccount({"CAP-1": FakeItem(
        "CAP-1", attachments=[FakeAttachment("q3.pdf", b"PDFBYTES")])})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result == {"verified": 1, "reset": 0, "failed": 0, "reasons": []}
    assert store.get_message("CAP-1")["archive_state"] == "verified"
    assert account.fetch_calls[0][1] == VERIFY_FIELDS


def test_verify_resets_when_the_changekey_moved(captured):
    store, settings, _sha, _blob = captured
    account = FakeAccount({"CAP-1": FakeItem("CAP-1", changekey="CK-DIFFERENT")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1 and result["verified"] == 0
    assert store.get_message("CAP-1")["archive_state"] == "live"
    assert "changekey" in result["reasons"][0]["reason"]


def test_verify_resets_when_the_mime_file_hash_is_wrong(captured):
    store, settings, sha, _blob = captured
    files.mime_path(settings.data_dir, sha).write_bytes(b"TAMPERED")
    account = FakeAccount({"CAP-1": FakeItem("CAP-1")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1
    assert "mime" in result["reasons"][0]["reason"]
    assert store.get_message("CAP-1")["archive_state"] == "live"


def test_verify_resets_when_a_blob_is_missing(captured):
    store, settings, _sha, blob_sha = captured
    files.blob_path(settings.data_dir, blob_sha).unlink()
    account = FakeAccount({"CAP-1": FakeItem("CAP-1")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1 and "blob" in result["reasons"][0]["reason"]


def test_verify_resets_when_a_blob_size_disagrees(captured):
    store, settings, _sha, blob_sha = captured
    with store.db.conn() as c:
        c.execute("UPDATE ews.attachments SET size = 999")
    account = FakeAccount({"CAP-1": FakeItem("CAP-1")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["reset"] == 1 and "size" in result["reasons"][0]["reason"]


def test_a_vanished_item_is_a_failure_not_a_promotion(captured):
    store, settings, _sha, _blob = captured
    account = FakeAccount({"CAP-1": ValueError("ErrorItemNotFound")})
    result = asyncio.run(Verifier(settings, FakeGatewayFor(account), store).run())
    assert result["failed"] == 1 and result["verified"] == 0
    assert store.get_message("CAP-1")["archive_state"] == "captured"


# --- deleter ------------------------------------------------------------------


def _verified(store, settings, n=3, verified_age_days=30):
    store.upsert_messages([make_row(f"V{i}", date_ts=NOW - 300 * DAY)
                           for i in range(n)])
    for i in range(n):
        store.mark_captured(f"V{i}", mime_sha256="a" * 64, mime_path="/x.eml")
        store.mark_verified(f"V{i}")
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET verified_at = now() - %s * interval '1 day' "
                  "WHERE archive_state = 'verified'", (verified_age_days,))
    return store


def _deleter(store, settings, account, audit=None, **over):
    policy = ArchivePolicy.from_settings(make_settings(**over))
    return Deleter(settings, FakeGatewayFor(account), store, policy,
                   audit or RecordingAudit())


def test_deletion_is_blocked_unless_explicitly_enabled(captured):
    store, settings, _s, _b = captured
    _verified(store, settings)
    account = FakeAccount({})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=False).run(dry_run=False))
    assert result["deleted"] == 0
    assert "ARCHIVE_DELETE_ENABLED" in result["blocked"]
    assert account.fetch_calls == []


def test_dry_run_reports_eligibility_without_deleting(captured):
    store, settings, _s, _b = captured
    _verified(store, settings)
    account = FakeAccount({})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True).run(dry_run=True))
    assert result["eligible"] == 3 and result["deleted"] == 0
    assert len(result["sample"]) == 3
    assert account.fetch_calls == []


def test_grace_period_protects_freshly_verified_mail(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, verified_age_days=1)
    account = FakeAccount({})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True,
                                  archive_grace_days=7).run(dry_run=True))
    assert result["eligible"] == 0


def test_the_per_run_cap_is_enforced(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, n=5)
    items = {f"V{i}": DeletableItem(f"V{i}") for i in range(5)}
    account = FakeAccount(items)
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True,
                                  archive_max_delete_per_run=2).run(dry_run=False))
    assert result["deleted"] == 2
    assert sum(1 for i in items.values() if i.deleted) == 2
    assert store.archive_state_counts()["deleted"] == 2


def test_delete_hard_deletes_marks_the_row_and_audits_each_one(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, n=2)
    items = {f"V{i}": DeletableItem(f"V{i}") for i in range(2)}
    audit = RecordingAudit()
    result = asyncio.run(_deleter(store, settings, FakeAccount(items), audit,
                                  archive_delete_enabled=True).run(
                                      dry_run=False, run_id=42))
    assert result["deleted"] == 2 and result["failed"] == 0
    assert all(i.deleted for i in items.values())
    assert store.get_message("V0")["archive_state"] == "deleted"
    assert len(audit.records) == 2
    detail = audit.records[0]["detail"]
    assert set(detail) >= {"ews_id", "internet_message_id", "mime_sha256", "run_id"}
    assert detail["run_id"] == 42
    assert audit.records[0]["side_effect_class"] == "destructive"


def test_one_failed_delete_does_not_mark_the_row(captured):
    store, settings, _s, _b = captured
    _verified(store, settings, n=2)

    class Stubborn(DeletableItem):
        def delete(self):
            raise RuntimeError("ErrorAccessDenied")

    account = FakeAccount({"V0": Stubborn("V0"), "V1": DeletableItem("V1")})
    result = asyncio.run(_deleter(store, settings, account,
                                  archive_delete_enabled=True).run(dry_run=False))
    assert result["deleted"] == 1 and result["failed"] == 1
    assert store.get_message("V0")["archive_state"] == "verified"
    assert store.get_message("V1")["archive_state"] == "deleted"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_archive_verify_delete.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'ewsmcp.archive.verify'`.

- [ ] **Step 3a: Write `ewsmcp/archive/verify.py`**

```python
"""Verify: prove the archive copy is good BEFORE anything is deleted upstream.

Four independent checks against a `captured` row — the item still exists with
the same changekey, the MIME file on disk still hashes to `mime_sha256`, every
recorded blob exists, and its size matches. Any failure resets the row to
`live` (dropping the capture entirely), so the next cycle re-captures it. The
row is only promoted to `verified` when all four pass.
"""

from __future__ import annotations

import logging
from typing import Any

from . import files

logger = logging.getLogger(__name__)

VERIFY_FIELDS = ["changekey", "attachments"]
BATCH_SIZE = 25


class Verifier:
    def __init__(self, settings: Any, gateway: Any, store: Any) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store

    async def run(self, *, limit: int = BATCH_SIZE) -> dict[str, Any]:
        rows = self.store.captured_rows(int(limit))
        result: dict[str, Any] = {"verified": 0, "reset": 0, "failed": 0,
                                  "reasons": []}
        if not rows:
            return result
        by_id = {r["ews_id"]: r for r in rows}
        fetched = await self.gateway.call(
            lambda account: account.fetch(
                ids=[(i, None) for i in by_id], only_fields=VERIFY_FIELDS))
        for ews_id, item in zip(list(by_id), fetched):
            row = by_id[ews_id]
            if isinstance(item, Exception):
                result["failed"] += 1
                logger.warning("verify could not re-fetch %s: %s", ews_id, item)
                continue
            reason = self._mismatch(row, item)
            if reason is None:
                self.store.mark_verified(ews_id)
                result["verified"] += 1
            else:
                self.store.reset_to_live(ews_id)
                result["reset"] += 1
                result["reasons"].append({"ews_id": ews_id, "reason": reason})
                logger.warning("verify reset %s to live: %s", ews_id, reason)
        return result

    def _mismatch(self, row: dict[str, Any], item: Any) -> str | None:
        live_ck = getattr(item, "changekey", None)
        if row["changekey"] and live_ck and live_ck != row["changekey"]:
            return (f"changekey changed since capture "
                    f"({row['changekey']} → {live_ck})")
        path = files.mime_path(self.settings.data_dir, row["mime_sha256"] or "")
        if not path.is_file():
            return f"mime file missing at {path}"
        if files.sha256_file(path) != row["mime_sha256"]:
            return f"mime hash mismatch at {path}"
        for att in self.store.attachments_for(row["ews_id"]):
            if not att["sha256"]:
                continue  # ItemAttachment — lives inside the verified MIME
            blob = files.blob_path(self.settings.data_dir, att["sha256"])
            if not blob.is_file():
                return f"blob missing for {att['name']!r} at {blob}"
            if att["size"] is not None and blob.stat().st_size != int(att["size"]):
                return (f"blob size mismatch for {att['name']!r}: "
                        f"{blob.stat().st_size} on disk vs {att['size']} recorded")
        return None
```

- [ ] **Step 3b: Write `ewsmcp/archive/delete.py`**

```python
"""Delete: the only code in this repository that removes mail from Exchange
without a human naming the message.

Three INDEPENDENT rails must all hold (spec §5) — ARCHIVE_DELETE_ENABLED is
true, the caller reached here through the `full` tier and (for the tool path)
a confirm token, and the row is `verified`, older than the cutoff plus the
grace period, and verified at least a grace period ago. On top of that a
per-run cap, and one audit record per deletion carrying enough identity
(`ews_id`, `internet_message_id`, `mime_sha256`, run id) to reconstruct what
went and which archive copy replaced it.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .policy import ArchivePolicy

logger = logging.getLogger(__name__)


class Deleter:
    def __init__(self, settings: Any, gateway: Any, store: Any,
                 policy: ArchivePolicy, audit: Any) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.policy = policy
        self.audit = audit

    async def run(self, *, dry_run: bool = True,
                  run_id: int | None = None) -> dict[str, Any]:
        result: dict[str, Any] = {"eligible": 0, "deleted": 0, "failed": 0,
                                  "blocked": None, "sample": []}
        if not self.policy.delete_enabled:
            result["blocked"] = (
                "ARCHIVE_DELETE_ENABLED=false — nothing is deleted from Exchange. "
                "Flip it deliberately once you trust the archive.")
            return result
        rows = self.store.deletable_rows(
            before_ts=self.policy.delete_cutoff_ts(),
            verified_before=self.policy.grace_instant_ts(),
            limit=int(self.policy.max_delete_per_run))
        result["eligible"] = len(rows)
        result["sample"] = [{"ews_id": r["ews_id"], "subject": r["subject"],
                             "date": r["date_iso"]} for r in rows[:10]]
        if dry_run or not rows:
            return result
        by_id = {r["ews_id"]: r for r in rows}
        deleted = await self.gateway.call(
            lambda account: self._delete_batch(account, list(by_id)))
        if deleted:
            self.store.mark_deleted(deleted)
        for ews_id in deleted:
            row = by_id[ews_id]
            self.audit.record(
                tool="archive_delete", side_effect_class="destructive",
                outcome="ok", latency_ms=0, transport="archive",
                detail={"ews_id": ews_id,
                        "internet_message_id": row["internet_message_id"],
                        "mime_sha256": row["mime_sha256"],
                        "run_id": run_id})
        result["deleted"] = len(deleted)
        result["failed"] = len(rows) - len(deleted)
        return result

    # Runs on the EWS pool (sync).
    def _delete_batch(self, account: Any, ids: list[str]) -> list[str]:
        done: list[str] = []
        started = time.time()
        fetched = account.fetch(ids=[(i, None) for i in ids],
                                only_fields=["id", "changekey"])
        for raw_id, item in zip(ids, fetched):
            try:
                if isinstance(item, Exception):
                    raise item
                item.delete()  # exchangelib 5.0.3: Item.delete() IS HardDelete
                done.append(raw_id)
            except Exception as exc:  # noqa: BLE001 - one failure never stops the pass
                logger.warning("archive delete failed for %s: %s: %s",
                               raw_id, type(exc).__name__, exc)
        logger.info("archive deleted %d/%d items in %.1fs",
                    len(done), len(ids), time.time() - started)
        return done
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_archive_verify_delete.py -q`
Expected: PASS (13 passed)

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m pytest tests -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/archive/verify.py ewsmcp/archive/delete.py tests/test_archive_verify_delete.py
git commit -m "$(cat <<'EOF'
feat(archive): verifier (reset on mismatch) and deleter (three rails + cap + audit)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 9: `archive/embed.py` + `archive/runner.py` + daemon wiring

**Files:**
- Create: `ewsmcp/archive/embed.py`, `ewsmcp/archive/runner.py`
- Modify: `ewsmcp/archive/__init__.py` (re-export), `ewsmcp/tools/base.py` (`Context.archive`, `Context.semantic`), `ewsmcp/server.py` (build + start)
- Test: `tests/test_archive_runner.py`

**Interfaces:**
- Consumes: `Capturer` (Task 7), `Verifier`, `Deleter` (Task 8), `SemanticIndex` (Task 6), `CacheStore.start_run / finish_run / get_run / recent_runs / unembedded_messages / embedding_backlog` (Tasks 4, 6), `ArchivePolicy` (Task 7).
- Produces:

```python
# embed.py
class EmbedWorker:
    def __init__(self, store, index, *, page: int = 200) -> None
    async def run(self, *, limit: int | None = None) -> dict   # {"embedded": int, "backlog": int, "error": str | None}

# runner.py
KINDS = ("capture", "verify", "delete", "embed", "all")

class ArchiveRunner:
    def __init__(self, settings, gateway, store, audit, index=None) -> None
    async def run_once(self, *, kind: str = "all", dry_run: bool = True,
                       before: str | None = None,
                       folders: list[str] | None = None) -> dict
    async def start(self) -> None
    async def stop(self) -> None
    def status(self) -> dict
```

`run_once` returns `{"ok": True, "run_id": int, "kind": str, "dry_run": bool, "candidates": int, "captured": int, "verified": int, "reset": int, "deleted": int, "eligible": int, "embedded": int, "failed": int, "blocked": str | None, "stopped": str | None, "sample": list}`.

- Also produces on `Context` (dataclass fields, default `None`): `archive: Any = None`, `semantic: Any = None`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_archive_runner.py`:

```python
"""The runner: one archive_runs row per pass, the right workers per kind,
and a background cycle that degrades instead of dying."""

import asyncio
import time

from conftest import FakeEmbedder, make_row, make_settings
from test_archive_capture import FakeAccount, FakeGatewayFor, FakeItem
from test_archive_verify_delete import DeletableItem, RecordingAudit

from ewsmcp.archive.runner import KINDS, ArchiveRunner
from ewsmcp.cache.store import CacheStore
from ewsmcp.semantic import SemanticIndex

NOW = int(time.time())
DAY = 86400


def _store(db):
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0}])
    store.upsert_messages([
        make_row("OLD-1", folder_id="FID-INBOX", date_ts=NOW - 300 * DAY),
        make_row("NEW-1", folder_id="FID-INBOX", date_ts=NOW - 2 * DAY),
    ])
    return store


def _runner(db, tmp_path, account=None, index=None, **over):
    store = _store(db)
    settings = make_settings(data_dir=str(tmp_path / "data"), **over)
    account = account if account is not None else FakeAccount({})
    return ArchiveRunner(settings, FakeGatewayFor(account), store,
                         RecordingAudit(), index=index), store


def test_kinds_cover_the_spec(tmp_path, db):
    assert KINDS == ("capture", "verify", "delete", "embed", "all")


def test_dry_run_records_a_run_row_and_counts_candidates(tmp_path, db):
    runner, store = _runner(db, tmp_path)
    out = asyncio.run(runner.run_once(kind="capture", dry_run=True))
    assert out["ok"] and out["dry_run"] is True
    assert out["candidates"] == 1 and out["captured"] == 0
    row = store.get_run(out["run_id"])
    assert row["kind"] == "capture" and row["dry_run"] == 1
    assert row["finished_at"] is not None
    assert '"after_days": 180' in row["policy_json"]


def test_all_runs_capture_verify_and_embed_in_one_pass(tmp_path, db):
    account = FakeAccount({"OLD-1": FakeItem("OLD-1")})
    index_store = None
    runner, store = _runner(db, tmp_path, account=account)
    runner.index = SemanticIndex(store, FakeEmbedder())
    out = asyncio.run(runner.run_once(kind="all", dry_run=False))
    assert out["captured"] == 1
    # capture and verify both ran against the same item this pass
    assert out["verified"] == 1
    assert store.get_message("OLD-1")["archive_state"] == "verified"
    assert out["embedded"] == 2 and store.embedding_backlog() == 0
    assert index_store is None  # sanity: the fixture did not leak state


def test_delete_stays_blocked_unless_enabled(tmp_path, db):
    items = {"OLD-1": DeletableItem("OLD-1")}
    runner, store = _runner(db, tmp_path, account=FakeAccount(items))
    out = asyncio.run(runner.run_once(kind="delete", dry_run=False))
    assert out["deleted"] == 0 and "ARCHIVE_DELETE_ENABLED" in out["blocked"]
    assert not items["OLD-1"].deleted


def test_overrides_narrow_the_policy_recorded_on_the_run(tmp_path, db):
    runner, store = _runner(db, tmp_path)
    out = asyncio.run(runner.run_once(kind="capture", dry_run=True,
                                      before="2020-01-01", folders=["inbox"]))
    row = store.get_run(out["run_id"])
    assert '"f:inbox"' in row["policy_json"]
    assert out["candidates"] == 0          # nothing is older than 2020 here


def test_a_worker_exception_is_recorded_on_the_run_not_raised(tmp_path, db):
    class Boom:
        async def call(self, fn):
            raise RuntimeError("exchange exploded")

    store = _store(db)
    settings = make_settings(data_dir=str(tmp_path / "data"))
    runner = ArchiveRunner(settings, Boom(), store, RecordingAudit())
    out = asyncio.run(runner.run_once(kind="capture", dry_run=False))
    assert out["ok"] is False and "exchange exploded" in out["error"]
    assert "exchange exploded" in store.get_run(out["run_id"])["error"]


def test_embed_is_a_noop_without_a_semantic_index(tmp_path, db):
    runner, store = _runner(db, tmp_path)
    out = asyncio.run(runner.run_once(kind="embed", dry_run=False))
    assert out["embedded"] == 0
    assert store.embedding_backlog() == 2


def test_status_reports_the_cadence_and_the_last_cycle(tmp_path, db):
    runner, _store = _runner(db, tmp_path, archive_cycle_seconds=300)
    st = runner.status()
    assert st["cycle_seconds"] == 300 and st["cycles"] == 0
    assert st["running"] is False


def test_the_background_loop_runs_a_cycle_and_can_be_stopped(tmp_path, db):
    account = FakeAccount({"OLD-1": FakeItem("OLD-1")})
    runner, store = _runner(db, tmp_path, account=account,
                            archive_cycle_seconds=1)

    async def drive():
        await runner.start()
        for _ in range(100):
            if runner.cycles:
                break
            await asyncio.sleep(0.05)
        await runner.stop()

    asyncio.run(drive())
    assert runner.cycles >= 1
    assert store.get_message("OLD-1")["archive_state"] in ("captured", "verified")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_archive_runner.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'ewsmcp.archive.runner'`.

- [ ] **Step 3a: Write `ewsmcp/archive/embed.py`**

```python
"""Embed backlog drainer.

Runs over every message with ``embedded_at IS NULL`` — live and archived
alike, because semantic search must not have a hole where the archive starts.
Embedding is remote and blocking, so it runs on a worker thread; a failure
leaves the backlog exactly where it was and the next cycle retries.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

PAGE = 200


class EmbedWorker:
    def __init__(self, store: Any, index: Any, *, page: int = PAGE) -> None:
        self.store = store
        self.index = index
        self.page = max(1, int(page))

    async def run(self, *, limit: int | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {"embedded": 0, "backlog": 0, "error": None}
        if self.index is None:
            out["backlog"] = self.store.embedding_backlog()
            return out
        budget = self.page if limit is None else int(limit)
        try:
            while budget > 0:
                rows = await asyncio.to_thread(
                    self.store.unembedded_messages, min(self.page, budget))
                if not rows:
                    break
                done = await asyncio.to_thread(self.index.index_messages, rows)
                out["embedded"] += done
                budget -= len(rows)
                if done == 0:
                    break  # nothing progressed — do not spin
        except Exception as exc:  # noqa: BLE001 - backlog grows, search degrades
            out["error"] = f"{type(exc).__name__}: {exc}"
            logger.warning("embedding pass failed: %s", out["error"])
        out["backlog"] = self.store.embedding_backlog()
        return out
```

- [ ] **Step 3b: Write `ewsmcp/archive/runner.py`**

```python
"""The archive cycle: one asyncio task, one ledger row per pass.

Started by the daemon AFTER Exchange warms up (like the sync engine), then
every ``ARCHIVE_CYCLE_SECONDS`` it captures, verifies, embeds and — only when
the rails allow — deletes. Every pass writes an ``ews.archive_runs`` row so
``archive_status`` and ``GET /v1/archive/runs/<id>`` can say exactly what
happened to the mailbox and when.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .capture import Capturer
from .delete import Deleter
from .embed import EmbedWorker
from .policy import ArchivePolicy
from .verify import Verifier

logger = logging.getLogger(__name__)

KINDS = ("capture", "verify", "delete", "embed", "all")
MIN_CYCLE_SECONDS = 30


class ArchiveRunner:
    def __init__(self, settings: Any, gateway: Any, store: Any, audit: Any,
                 index: Any = None) -> None:
        self.settings = settings
        self.gateway = gateway
        self.store = store
        self.audit = audit
        self.index = index
        self.cycles = 0
        self.last_error: str | None = None
        self.last_cycle_ts: float | None = None
        self.last_run_id: int | None = None
        self._task: asyncio.Task | None = None
        self._stopped = False

    # ------------------------------------------------------------ one pass

    async def run_once(self, *, kind: str = "all", dry_run: bool = True,
                       before: str | None = None,
                       folders: list[str] | None = None) -> dict[str, Any]:
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {', '.join(KINDS)}")
        policy = ArchivePolicy.from_settings(self.settings).with_overrides(
            before=before, folders=folders, tz=self.settings.ews_tz)
        run_id = self.store.start_run(kind, dry_run=dry_run,
                                      policy=policy.as_dict())
        self.last_run_id = run_id
        out: dict[str, Any] = {
            "ok": True, "run_id": run_id, "kind": kind, "dry_run": dry_run,
            "candidates": 0, "captured": 0, "verified": 0, "reset": 0,
            "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
            "blocked": None, "stopped": None, "error": None, "sample": [],
        }
        try:
            if kind in ("capture", "all"):
                res = await Capturer(self.settings, self.gateway, self.store,
                                     policy).run(dry_run=dry_run)
                out["candidates"] = res["candidates"]
                out["captured"] = res["captured"]
                out["failed"] += res["failed"]
                out["stopped"] = res["stopped"]
                out["sample"] = res["sample"]
            if kind in ("verify", "all") and not dry_run:
                res = await Verifier(self.settings, self.gateway,
                                     self.store).run()
                out["verified"] = res["verified"]
                out["reset"] = res["reset"]
                out["failed"] += res["failed"]
            if kind in ("embed", "all") and not dry_run:
                res = await EmbedWorker(self.store, self.index).run()
                out["embedded"] = res["embedded"]
                if res["error"]:
                    out["error"] = res["error"]
            if kind in ("delete", "all"):
                res = await Deleter(self.settings, self.gateway, self.store,
                                    policy, self.audit).run(dry_run=dry_run,
                                                            run_id=run_id)
                out["eligible"] = res["eligible"]
                out["deleted"] = res["deleted"]
                out["failed"] += res["failed"]
                out["blocked"] = res["blocked"]
                if not out["sample"]:
                    out["sample"] = res["sample"]
        except Exception as exc:  # noqa: BLE001 - a pass never takes ewsd down
            out["ok"] = False
            out["error"] = f"{type(exc).__name__}: {exc}"
            logger.error("archive run %s (%s) failed: %s", run_id, kind,
                         out["error"])
        self.store.finish_run(
            run_id, captured=out["captured"], verified=out["verified"],
            deleted=out["deleted"], failed=out["failed"],
            error=out["error"] or out["stopped"], sample=out["sample"])
        return out

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="archive")
            logger.info("archive runner started (every %ss, delete_enabled=%s)",
                        self.settings.archive_cycle_seconds,
                        self.settings.archive_delete_enabled)

    async def stop(self) -> None:
        self._stopped = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001, S110
                pass

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                result = await self.run_once(kind="all", dry_run=False)
                self.last_error = result.get("error")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - degrade, never die
                self.last_error = f"{type(exc).__name__}: {exc}"[:500]
                logger.warning("archive cycle failed: %s", self.last_error)
            self.cycles += 1
            self.last_cycle_ts = time.time()
            await asyncio.sleep(max(MIN_CYCLE_SECONDS,
                                    int(self.settings.archive_cycle_seconds)))

    def status(self) -> dict[str, Any]:
        return {
            "running": self._task is not None and not self._task.done(),
            "cycles": self.cycles,
            "cycle_seconds": int(self.settings.archive_cycle_seconds),
            "last_cycle_age_s": (int(time.time() - self.last_cycle_ts)
                                 if self.last_cycle_ts else None),
            "last_run_id": self.last_run_id,
            "last_error": self.last_error,
            "delete_enabled": bool(self.settings.archive_delete_enabled),
        }
```

Append the re-export to `ewsmcp/archive/__init__.py`:

```python
from .runner import ArchiveRunner  # noqa: E402,F401
```

- [ ] **Step 3c: Wire it into the daemon**

In `ewsmcp/tools/base.py`, add two fields to `@dataclass class Context` (next to `sync`):

```python
    archive: Any = None  # ArchiveRunner | None (daemon only)
    semantic: Any = None  # SemanticIndex | None (daemon only — holds the Gemini key)
```

In `ewsmcp/server.py`, extend `build_context` just before `build_registry(ctx)`:

```python
    if settings.semantic_enabled():
        from .embeddings import GeminiEmbedder
        from .semantic import SemanticIndex
        ctx.semantic = SemanticIndex(
            ctx.cache, GeminiEmbedder(settings.gemini_api_key,
                                      dims=settings.embed_dims))
    else:
        logger.info("GEMINI_API_KEY unset — semantic search disabled, "
                    "keyword search unaffected")
    from .archive import ArchiveRunner
    ctx.archive = ArchiveRunner(settings, gateway, ctx.cache, audit,
                                index=ctx.semantic)
```

and start it in `start_connection_manager`'s `on_warm`, after the sync engine block:

```python
        if ctx.archive is not None:
            try:
                await ctx.archive.start()
            except Exception as exc:  # noqa: BLE001 - archive is best-effort
                logger.error("archive runner start failed (%s) — capture/verify "
                             "will not run until ewsd restarts", exc)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_archive_runner.py -q`
Expected: PASS (9 passed)

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m ruff check ewsmcp tests && .venv/bin/python -m pytest tests -q`
Expected: PASS. Note `tests/test_no_lazy_imports.py` still passes — the lazy imports added in `server.py` are `ewsmcp` modules, not exchangelib.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/archive ewsmcp/tools/base.py ewsmcp/server.py tests/test_archive_runner.py
git commit -m "$(cat <<'EOF'
feat(archive): embed worker, ArchiveRunner cycle, daemon wiring on warm-up

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 10: SyncEngine — server-side deletes must not eat the archive

**Files:**
- Modify: `ewsmcp/cache/sync.py`
- Test: `tests/test_sync_engine.py` (append)

**Interfaces:**
- Consumes: `CacheStore.apply_server_deletes(ews_ids) -> tuple[int, int]` (Task 4).
- Produces: `SyncEngine` counters `self.dropped_rows: int` and `self.tombstoned_rows: int`, surfaced through `SyncEngine.status()` as `"dropped"` / `"tombstoned"`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_sync_engine.py`:

```python
# --- archive interaction (spec §3) -------------------------------------------


def test_a_server_delete_of_a_live_row_still_drops_it(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1"))], "TOK-1")
    asyncio.run(engine._cycle())
    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())
    assert store.get_message("M1") is None


def test_a_server_delete_of_a_captured_row_keeps_it_and_marks_deleted(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1", subject="Contract"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M1", mime_sha256="a" * 64, mime_path="/x.eml")

    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())

    row = store.get_message("M1")
    assert row is not None                    # the archive copy is ours now
    assert row["archive_state"] == "deleted"
    assert row["deleted_at"] is not None
    assert row["subject"] == "Contract"       # still searchable


def test_a_server_delete_of_a_verified_row_keeps_it(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("M1")
    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())
    assert store.get_message("M1")["archive_state"] == "deleted"


def test_our_own_archive_deletion_is_ignored_when_it_echoes_back(db):
    """The deleter already marked the row; the sync event must not disturb it."""
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M1", mime_sha256="a" * 64, mime_path="/x.eml")
    store.mark_verified("M1")
    store.mark_deleted(["M1"])
    before = store.get_message("M1")["deleted_at"]

    account.inbox.queue([("delete", SimpleNamespace(id="M1"))], "TOK-2")
    asyncio.run(engine._cycle())

    row = store.get_message("M1")
    assert row["archive_state"] == "deleted" and row["deleted_at"] == before


def test_status_reports_what_the_deletes_did(db):
    account = _account()
    engine, store = _engine(db, account)
    account.inbox.queue([("create", _msg("M1")), ("create", _msg("M2"))], "TOK-1")
    asyncio.run(engine._cycle())
    store.mark_captured("M2", mime_sha256="a" * 64, mime_path="/x.eml")
    account.inbox.queue([("delete", SimpleNamespace(id="M1")),
                         ("delete", SimpleNamespace(id="M2"))], "TOK-2")
    asyncio.run(engine._cycle())
    st = engine.status()
    assert st["dropped"] == 1 and st["tombstoned"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_sync_engine.py -q`
Expected: FAIL — `test_a_server_delete_of_a_captured_row_keeps_it_and_marks_deleted` fails with `assert None is not None` (the row was deleted outright), and `status()` has no `dropped` key.

- [ ] **Step 3: Write the implementation**

In `ewsmcp/cache/sync.py`, add the counters to `SyncEngine.__init__`:

```python
        self.dropped_rows = 0
        self.tombstoned_rows = 0
```

replace the delete call in `_sync_mail_folders`:

```python
            self.store.upsert_messages(upserts)
            # Spec §3: an archived row that disappears upstream (our own
            # deleter, or a hand-delete in Outlook) is KEPT and marked
            # deleted — we hold the only copy now. Only live rows are dropped.
            dropped, tombstoned = self.store.apply_server_deletes(deletes)
            self.dropped_rows += dropped
            self.tombstoned_rows += tombstoned
```

and extend `status()`:

```python
    def status(self) -> dict[str, Any]:
        return {
            "cycles": self.cycles,
            "last_cycle_age_s": (int(time.time() - self.last_cycle_ts)
                                 if self.last_cycle_ts else None),
            "last_error": self.last_error,
            "dropped": self.dropped_rows,
            "tombstoned": self.tombstoned_rows,
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_sync_engine.py -q`
Expected: PASS

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m pytest tests -q`
Expected: PASS — `/metrics` reads `sync.status()` by key and tolerates extra keys.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/cache/sync.py tests/test_sync_engine.py
git commit -m "$(cat <<'EOF'
feat(sync): keep archived rows when Exchange reports a delete

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 11: `downloads.py` + `GET /download/<token>`

**Files:**
- Create: `ewsmcp/downloads.py`
- Modify: `ewsmcp/http.py`
- Test: `tests/test_downloads.py`, `tests/test_daemon_api.py` (append)

**Interfaces:**
- Consumes: nothing new; mirrors the `ewsmcp/uploads.py` capability-token pattern.
- Produces:

```python
TOKEN_BYTES = 32
DEFAULT_TTL_SECONDS = 15 * 60
MAX_TTL_SECONDS = 24 * 60 * 60

class DownloadRejected(Exception): ...

def mint(data_dir: str, *, path: str, name: str, content_type: str = "application/octet-stream",
         ttl_seconds: int = DEFAULT_TTL_SECONDS) -> dict   # {"token","name","content_type","expires_at"}
def redeem(data_dir: str, token: str) -> dict              # {"path","name","content_type"}
def sweep(data_dir: str) -> int
```

- [ ] **Step 1: Write the failing test**

Create `tests/test_downloads.py`:

```python
"""Capability-URL downloads: unguessable, single use, short-lived, contained."""

import json
import time
from pathlib import Path

import pytest

from ewsmcp import downloads


def _file(tmp_path, name="msg.eml", body=b"RAW"):
    data = Path(tmp_path) / "mime"
    data.mkdir(parents=True, exist_ok=True)
    path = data / name
    path.write_bytes(body)
    return path


def test_mint_returns_an_unguessable_token(tmp_path):
    path = _file(tmp_path)
    rec = downloads.mint(str(tmp_path), path=str(path), name="msg.eml",
                         content_type="message/rfc822")
    assert len(rec["token"]) == 64 and rec["name"] == "msg.eml"
    assert rec["content_type"] == "message/rfc822"
    assert rec["expires_at"] > time.time()


def test_redeem_returns_the_file_once(tmp_path):
    path = _file(tmp_path)
    token = downloads.mint(str(tmp_path), path=str(path), name="msg.eml")["token"]
    got = downloads.redeem(str(tmp_path), token)
    assert Path(got["path"]).read_bytes() == b"RAW"
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_expired_links_are_rejected_and_removed(tmp_path):
    path = _file(tmp_path)
    token = downloads.mint(str(tmp_path), path=str(path), name="msg.eml",
                           ttl_seconds=1)["token"]
    link = Path(tmp_path) / "download-links" / f"{token}.json"
    rec = json.loads(link.read_text())
    rec["expires_at"] = time.time() - 1
    link.write_text(json.dumps(rec))
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)
    assert not link.exists()


@pytest.mark.parametrize("token", ["../../etc/passwd", "", "ZZZZ", "a" * 200])
def test_malformed_tokens_never_reach_the_filesystem(tmp_path, token):
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_a_link_pointing_outside_data_dir_is_refused(tmp_path):
    outside = tmp_path.parent / "secret.txt"
    outside.write_bytes(b"nope")
    token = downloads.mint(str(tmp_path), path=str(outside), name="x")["token"]
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_a_vanished_file_is_refused(tmp_path):
    path = _file(tmp_path)
    token = downloads.mint(str(tmp_path), path=str(path), name="msg.eml")["token"]
    path.unlink()
    with pytest.raises(downloads.DownloadRejected):
        downloads.redeem(str(tmp_path), token)


def test_sweep_removes_used_and_expired_records(tmp_path):
    path = _file(tmp_path)
    used = downloads.mint(str(tmp_path), path=str(path), name="a")["token"]
    downloads.redeem(str(tmp_path), used)
    downloads.mint(str(tmp_path), path=str(path), name="b")
    assert downloads.sweep(str(tmp_path)) == 1
```

Append to `tests/test_daemon_api.py`:

```python
def test_download_route_serves_the_file_ahead_of_the_bearer_gate(db, tmp_path):
    from pathlib import Path

    from ewsmcp import downloads

    ctx = make_context(db, ewsd_api_key="k")
    mime = Path(ctx.settings.data_dir) / "mime"
    mime.mkdir(parents=True, exist_ok=True)
    (mime / "m.eml").write_bytes(b"RAW-MIME")
    token = downloads.mint(ctx.settings.data_dir, path=str(mime / "m.eml"),
                           name="m.eml", content_type="message/rfc822")["token"]
    app = build_daemon_app(ctx, ctx.settings)
    status, headers, body = _drive_raw(app, f"/download/{token}")   # NO bearer
    assert status == 200 and body == b"RAW-MIME"
    assert (b"content-type", b"message/rfc822") in headers
    assert any(b"attachment" in v for k, v in headers
               if k == b"content-disposition")
    # single use
    assert _drive_raw(app, f"/download/{token}")[0] == 404


def test_unknown_download_tokens_are_an_opaque_404(db):
    ctx = make_context(db, ewsd_api_key="k")
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive_raw(app, "/download/" + "0" * 64)[0] == 404
    assert _drive_raw(app, "/download/nonsense")[0] == 404
```

with this raw-bytes driver next to `_drive` in the same file:

```python
def _drive_raw(app, path, method="GET", headers=()):
    """Like _drive but returns (status, headers, raw bytes) — the download
    route answers with file bytes, not JSON."""
    scope = {"type": "http", "path": path, "method": method,
             "headers": [(k.encode(), v.encode()) for k, v in headers]}
    msgs = [{"type": "http.request", "body": b"", "more_body": False}]
    sent = []

    async def receive():
        return msgs.pop(0)

    async def send(m):
        sent.append(m)

    asyncio.run(app(scope, receive, send))
    start = next(m for m in sent if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in sent
                   if m["type"] == "http.response.body")
    return start["status"], [tuple(h) for h in start["headers"]], raw
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_downloads.py -q`
Expected: FAIL — `ImportError: cannot import name 'downloads' from 'ewsmcp'`.

- [ ] **Step 3a: Write `ewsmcp/downloads.py`**

```python
"""Capability-URL downloads — get a file OUT of DATA_DIR without a standing secret.

The mirror image of ``uploads.py``, and for the same reason: MCP has no file
channel, so a 4 MB .eml would otherwise travel as base64 through the model's
context. ``get_raw_message`` mints a link instead:

    get_raw_message(id)  ->  https://<host>/download/<token>
    curl -O -J "<url>"

The URL IS the credential, so the token is 256 bits, single use, short-lived,
bound to ONE path that must live under DATA_DIR, and every rejection renders
as an identical opaque 404 — probing must not distinguish expired from used
from never-existed.
"""

from __future__ import annotations

import json
import re
import secrets
import time
from pathlib import Path
from typing import Any

TOKEN_BYTES = 32
DEFAULT_TTL_SECONDS = 15 * 60
MAX_TTL_SECONDS = 24 * 60 * 60

_TOKEN_RE = re.compile(r"^[0-9a-f]{32,128}$")


class DownloadRejected(Exception):
    """Any redemption failure. Callers MUST render this as an opaque 404."""


def _links_dir(data_dir: str) -> Path:
    return Path(data_dir) / "download-links"


def mint(data_dir: str, *, path: str, name: str,
         content_type: str = "application/octet-stream",
         ttl_seconds: int = DEFAULT_TTL_SECONDS) -> dict[str, Any]:
    ttl = min(int(ttl_seconds), MAX_TTL_SECONDS)
    token = secrets.token_hex(TOKEN_BYTES)
    record = {"path": str(Path(path).resolve()),
              "name": Path(str(name or "download.bin")).name,
              "content_type": content_type,
              "expires_at": time.time() + ttl, "used": False}
    links = _links_dir(data_dir)
    links.mkdir(parents=True, exist_ok=True)
    (links / f"{token}.json").write_text(json.dumps(record))
    return {"token": token, "name": record["name"],
            "content_type": content_type, "expires_at": record["expires_at"]}


def redeem(data_dir: str, token: str) -> dict[str, Any]:
    """Consume a link exactly once and return what to serve."""
    # Shape first: the token is used as a filename, so `../x` must never
    # become a path lookup.
    if not isinstance(token, str) or not _TOKEN_RE.match(token):
        raise DownloadRejected("bad token")
    record_path = _links_dir(data_dir) / f"{token}.json"
    try:
        record = json.loads(record_path.read_text())
    except Exception as exc:                      # missing/corrupt — same answer
        raise DownloadRejected("no such link") from exc
    if record.get("used"):
        raise DownloadRejected("already used")
    if float(record.get("expires_at", 0)) < time.time():
        record_path.unlink(missing_ok=True)
        raise DownloadRejected("expired")

    target = Path(str(record.get("path", ""))).resolve()
    root = Path(data_dir).resolve()
    # Containment: a link may only ever hand out something inside DATA_DIR.
    if not target.is_relative_to(root) or not target.is_file():
        raise DownloadRejected("not servable")

    record["used"] = True
    record_path.write_text(json.dumps(record))
    return {"path": str(target), "name": record.get("name") or target.name,
            "content_type": record.get("content_type")
            or "application/octet-stream"}


def sweep(data_dir: str) -> int:
    """Delete expired/used link records. Best-effort housekeeping."""
    links = _links_dir(data_dir)
    if not links.is_dir():
        return 0
    now, removed = time.time(), 0
    for f in links.glob("*.json"):
        try:
            rec = json.loads(f.read_text())
            if rec.get("used") or float(rec.get("expires_at", 0)) < now:
                f.unlink(missing_ok=True)
                removed += 1
        except Exception:  # noqa: BLE001 - a corrupt record is worth removing
            f.unlink(missing_ok=True)
            removed += 1
    return removed
```

- [ ] **Step 3b: Add the route to `ewsmcp/http.py`**

Import it next to `uploads` (`from . import __version__, downloads, uploads`) and insert this block immediately AFTER the `/upload/` block and BEFORE the bearer gate:

```python
        # Capability-URL download: GET /download/<token>. Same model as
        # /upload — deliberately ahead of the bearer gate, single use, and
        # every failure is an identical opaque 404.
        if path.startswith("/download/") and method == "GET":
            token = path[len("/download/"):]
            try:
                rec = downloads.redeem(settings.data_dir, token)
                body = Path(rec["path"]).read_bytes()
            except (downloads.DownloadRejected, OSError):
                return await _send_json(send, 404, {"ok": False, "error": {
                    "code": "not_found", "message": "not found"}})
            disposition = f'attachment; filename="{rec["name"]}"'.encode()
            await send({"type": "http.response.start", "status": 200, "headers": [
                [b"content-type", rec["content_type"].encode()],
                [b"content-length", str(len(body)).encode()],
                [b"content-disposition", disposition],
            ]})
            return await send({"type": "http.response.body", "body": body})
```

Add `from pathlib import Path` to the imports at the top of `http.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_downloads.py tests/test_daemon_api.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ewsmcp/downloads.py ewsmcp/http.py tests/test_downloads.py tests/test_daemon_api.py
git commit -m "$(cat <<'EOF'
feat(archive): single-use capability downloads and GET /download/<token>

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 12: The archive tool pack — `archive_run`, `archive_status`, `get_raw_message`, `find_similar`

**Files:**
- Create: `ewsmcp/tools/archive.py`
- Modify: `ewsmcp/tools/__init__.py`, `ewsmcp/mcp/registry.py`, `scripts/dump_tool_table.py`
- Test: `tests/test_archive_tools.py`, `tests/test_surface_completion.py` (counts), `tests/test_mcp_thin.py` (counts)

**Interfaces:**
- Consumes: `ArchiveRunner.run_once` (Task 9), `CacheStore.archive_state_counts / recent_runs / embedding_backlog / embedded_count / get_message / attachments_for` (Tasks 4, 6), `archive.files.blob_store_bytes` (Task 3), `downloads.mint` (Task 11), `SemanticIndex.similar_to_message / vector_ids` (Task 6), `ArchivePolicy.from_settings` (Task 7), `dto.envelope`, `cache_reads._row_card` (existing).
- Produces `ewsmcp/tools/archive.py`:

```python
async def _archive_run(ctx, *, dry_run: bool = True, kind: str = "all",
                       before: str | None = None,
                       folders: list[str] | None = None) -> dict
async def _archive_status(ctx) -> dict
async def _get_raw_message(ctx, *, id: str, ttl_minutes: int = 15) -> dict
async def _find_similar(ctx, *, id: str | None = None, text: str | None = None,
                        limit: int = 10, archived: str = "any") -> dict
TOOLS: list[ToolSpec]   # archive_run (destructive), archive_status / get_raw_message / find_similar (read)
```

**Tier counts after this task (assert exactly):** read **18**, draft **29**, full **35**.

- [ ] **Step 1: Write the failing test**

Create `tests/test_archive_tools.py`:

```python
"""The four new tools: gates, envelopes, and what each one reads."""

import asyncio
import time

from conftest import FakeEmbedder, make_context, make_row

from ewsmcp.archive import files
from ewsmcp.semantic import SemanticIndex
from ewsmcp.tools.base import dispatch

NOW = int(time.time())
DAY = 86400


def _run(ctx, name, **kw):
    return asyncio.run(dispatch(ctx, ctx.registry[name], dict(kw)))


def _ctx(db, **over):
    over.setdefault("ews_capability_tier", "full")
    ctx = make_context(db, **over)
    ctx.cache.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0}])
    return ctx


# --- registry -----------------------------------------------------------------


def test_the_four_tools_are_registered_at_the_right_tiers(db):
    full = _ctx(db)
    assert len(full.registry) == 35
    assert full.registry["archive_run"].side_effect_class == "destructive"
    for name in ("archive_status", "get_raw_message", "find_similar"):
        assert full.registry[name].side_effect_class == "read"
    read = _ctx(db, ews_capability_tier="read")
    assert len(read.registry) == 18
    assert "archive_run" not in read.registry
    assert "find_similar" in read.registry     # always registered now
    assert len(_ctx(db, ews_capability_tier="draft").registry) == 29


# --- archive_status -----------------------------------------------------------


def test_archive_status_reports_states_runs_blobs_and_backlog(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.cache.upsert_messages([make_row("A1"), make_row("A2")])
    ctx.cache.mark_captured("A1", mime_sha256="a" * 64, mime_path="/x.eml")
    files.store_mime(ctx.settings.data_dir, b"x" * 100)
    run_id = ctx.cache.start_run("capture", dry_run=True, policy={})
    ctx.cache.finish_run(run_id, captured=1)

    res = _run(ctx, "archive_status")
    assert res["ok"] is True
    assert res["states"] == {"live": 1, "captured": 1, "verified": 0, "deleted": 0}
    assert res["blob_store_bytes"] == 100
    assert res["embedding"]["backlog"] == 2
    assert res["recent_runs"][0]["id"] == run_id
    assert res["policy"]["folders"] == ["f:inbox", "f:sent"]
    assert res["delete_enabled"] is False


def test_archive_status_needs_no_exchange(db):
    spec = _ctx(db).registry["archive_status"]
    assert spec.requires_ews is False


# --- archive_run --------------------------------------------------------------


def test_archive_run_dry_run_needs_no_confirmation(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.archive = _FakeRunner()
    res = _run(ctx, "archive_run", dry_run=True)
    assert res["ok"] and "confirm_token" not in res
    assert ctx.archive.calls == [{"kind": "all", "dry_run": True,
                                  "before": None, "folders": None}]


def test_archive_run_for_real_is_two_phase(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.archive = _FakeRunner()
    phase1 = _run(ctx, "archive_run", dry_run=False)
    assert phase1["requires_confirmation"] is True and phase1["confirm_token"]
    assert ctx.archive.calls == []                      # NOTHING executed
    phase2 = _run(ctx, "archive_run", dry_run=False,
                  confirm_token=phase1["confirm_token"])
    assert phase2["ok"] is True
    assert ctx.archive.calls[0]["dry_run"] is False


def test_archive_run_is_blocked_below_the_full_tier(db):
    ctx = _ctx(db, ews_capability_tier="draft")
    assert "archive_run" not in ctx.registry


def test_archive_run_without_a_runner_is_unavailable(db):
    ctx = _ctx(db)
    ctx.archive = None
    res = _run(ctx, "archive_run", dry_run=True)
    assert res["ok"] is False
    assert res["error"]["code"] == "upstream_unavailable"


def test_archive_run_rejects_an_unknown_kind(db):
    ctx = _ctx(db)
    ctx.archive = _FakeRunner()
    res = _run(ctx, "archive_run", dry_run=True, kind="nuke")
    assert res["ok"] is False and res["error"]["code"] == "validation"


class _FakeRunner:
    def __init__(self):
        self.calls = []

    async def run_once(self, *, kind, dry_run, before, folders):
        self.calls.append({"kind": kind, "dry_run": dry_run, "before": before,
                           "folders": folders})
        return {"ok": True, "run_id": 7, "kind": kind, "dry_run": dry_run,
                "candidates": 3, "captured": 0, "verified": 0, "reset": 0,
                "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
                "blocked": None, "stopped": None, "error": None, "sample": []}


# --- get_raw_message ----------------------------------------------------------


def test_get_raw_message_returns_a_capability_url(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"),
               external_url="https://ews.example.com")
    ctx.cache.upsert_messages([make_row("A1", subject="Contract")])
    sha, path = files.store_mime(ctx.settings.data_dir, b"RAW-MIME")
    ctx.cache.mark_captured("A1", mime_sha256=sha, mime_path=str(path))

    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is True
    assert res["download_url"].startswith("https://ews.example.com/download/")
    assert res["name"].endswith(".eml")
    assert res["size_bytes"] == len(b"RAW-MIME")
    assert res["expires_in_minutes"] == 15

    from ewsmcp import downloads
    token = res["download_url"].rsplit("/", 1)[1]
    assert downloads.redeem(ctx.settings.data_dir, token)["path"] == str(path)


def test_get_raw_message_without_external_url_is_relative(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.cache.upsert_messages([make_row("A1")])
    sha, path = files.store_mime(ctx.settings.data_dir, b"M")
    ctx.cache.mark_captured("A1", mime_sha256=sha, mime_path=str(path))
    assert _run(ctx, "get_raw_message", id="A1")["download_url"].startswith(
        "/download/")


def test_get_raw_message_on_live_mail_explains_itself(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.cache.upsert_messages([make_row("A1")])
    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is False and res["error"]["code"] == "not_found"
    assert "not archived" in res["error"]["message"]


def test_get_raw_message_when_the_file_vanished(db, tmp_path):
    ctx = _ctx(db, data_dir=str(tmp_path / "data"))
    ctx.cache.upsert_messages([make_row("A1")])
    ctx.cache.mark_captured("A1", mime_sha256="a" * 64,
                            mime_path=str(tmp_path / "gone.eml"))
    res = _run(ctx, "get_raw_message", id="A1")
    assert res["ok"] is False and res["error"]["code"] == "not_found"


# --- find_similar -------------------------------------------------------------


def _semantic(ctx):
    ctx.cache.upsert_messages([
        make_row("S-BUDGET", subject="Quarterly budget",
                 body="finance forecast spreadsheet"),
        make_row("S-FORECAST", subject="Forecast update",
                 body="finance forecast numbers"),
        make_row("S-LUNCH", subject="Lunch", body="shawarma at noon"),
    ])
    ctx.semantic = SemanticIndex(ctx.cache, FakeEmbedder())
    ctx.semantic.index_messages(ctx.cache.unembedded_messages(100))
    return ctx


def test_find_similar_by_message_id(db):
    ctx = _semantic(_ctx(db))
    res = _run(ctx, "find_similar", id="S-BUDGET", limit=2)
    assert res["ok"] is True and res["count"] >= 1
    assert all(item["id"] != "S-BUDGET" for item in res["items"])
    assert "similarity" in res["items"][0]


def test_find_similar_by_free_text(db):
    ctx = _semantic(_ctx(db))
    res = _run(ctx, "find_similar", text="finance forecast", limit=3)
    ids = [i["subject"] for i in res["items"]]
    assert "Lunch" not in ids[:1]


def test_find_similar_needs_exactly_one_of_id_or_text(db):
    ctx = _semantic(_ctx(db))
    assert _run(ctx, "find_similar")["error"]["code"] == "validation"
    assert _run(ctx, "find_similar", id="S-BUDGET",
                text="x")["error"]["code"] == "validation"


def test_find_similar_without_a_key_is_a_clear_error(db):
    ctx = _ctx(db)
    ctx.semantic = None
    res = _run(ctx, "find_similar", text="budget")
    assert res["ok"] is False and res["error"]["code"] == "validation"
    assert "GEMINI_API_KEY" in res["error"]["hint"]
```

Update the count assertions in `tests/test_surface_completion.py`:

```python
def test_registry_counts_per_tier_and_semantic(tmp_path, db):
    full = _ctx(tmp_path, db, ews_capability_tier="full")
    assert len(full.registry) == 35
    assert "find_similar" in full.registry  # the semantic tier is back, Gemini-backed
    draft = _ctx(tmp_path, db, ews_capability_tier="draft")
    assert len(draft.registry) == 29
    read = _ctx(tmp_path, db, ews_capability_tier="read")
    assert len(read.registry) == 18
```

and in `tests/test_mcp_thin.py`:

```python
def test_registry_matches_daemon_counts(db):
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="full").registry) == 35
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="draft").registry) == 29
    assert len(_mcp_ctx(db, DeadDaemon(), ews_capability_tier="read").registry) == 18
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_archive_tools.py -q`
Expected: FAIL — `KeyError: 'archive_run'` (the registry has 31 tools).

- [ ] **Step 3a: Write `ewsmcp/tools/archive.py`**

```python
"""Tool pack: archive + semantic search.

`archive_run` is the only tool here with teeth. It is class `destructive`
(minimum tier `full`) and `dry_run=false` is two-phase confirmed, so removing
mail from Exchange takes a deliberate second model decision on top of
ARCHIVE_DELETE_ENABLED and the verified-plus-grace rail inside the deleter.

`get_raw_message` never returns bytes: a .eml is exactly the kind of payload
that must not travel through the model's context, so it mints a single-use
capability URL the same way `create_upload_link` does in the other direction.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from .. import downloads
from ..archive import files
from ..archive.policy import ArchivePolicy
from ..dto import envelope
from ..errors import ToolError
from .base import Context, ToolSpec
from .cache_reads import _row_card

logger = logging.getLogger(__name__)

KINDS = ("capture", "verify", "delete", "embed", "all")
ARCHIVED_MODES = ("any", "only", "exclude")


def _require_cache(ctx: Context) -> Any:
    if ctx.cache is None:
        raise ToolError("backend_unavailable", "the Postgres mirror is not available",
                        hint="Check DATABASE_URL.", retry_after_s=15)
    return ctx.cache


# --------------------------------------------------------------------------
# archive_run
# --------------------------------------------------------------------------


async def _archive_run(ctx: Context, *, dry_run: bool = True, kind: str = "all",
                       before: str | None = None,
                       folders: list[str] | None = None) -> dict[str, Any]:
    if kind not in KINDS:
        raise ToolError("validation",
                        f"kind must be one of {', '.join(KINDS)} (got {kind!r})")
    if ctx.archive is None:
        raise ToolError("upstream_unavailable",
                        "the archive runner is not started on this server",
                        hint="Only ewsd runs the archive; check /readyz.",
                        retry_after_s=30)
    return await ctx.archive.run_once(kind=kind, dry_run=bool(dry_run),
                                      before=before, folders=folders)


# --------------------------------------------------------------------------
# archive_status
# --------------------------------------------------------------------------


async def _archive_status(ctx: Context) -> dict[str, Any]:
    cache = _require_cache(ctx)

    def read() -> dict[str, Any]:
        return {
            "states": cache.archive_state_counts(),
            "recent_runs": [
                {"id": r["id"], "kind": r["kind"], "dry_run": bool(r["dry_run"]),
                 "started_at": r["started_at"], "finished_at": r["finished_at"],
                 "captured": r["captured"], "verified": r["verified"],
                 "deleted": r["deleted"], "failed": r["failed"],
                 "error": r["error"]}
                for r in cache.recent_runs(5)
            ],
            "embedding": {"backlog": cache.embedding_backlog(),
                          "embedded": cache.embedded_count()},
        }

    out = await asyncio.to_thread(read)
    out["blob_store_bytes"] = await asyncio.to_thread(
        files.blob_store_bytes, ctx.settings.data_dir)
    out["free_gb"] = round(await asyncio.to_thread(
        files.free_gb, ctx.settings.data_dir), 2)
    policy = ArchivePolicy.from_settings(ctx.settings)
    out["policy"] = {"folders": list(policy.folders),
                     "after_days": policy.after_days,
                     "grace_days": policy.grace_days,
                     "exclude_categories": list(policy.exclude_categories),
                     "max_delete_per_run": policy.max_delete_per_run,
                     "min_free_gb": policy.min_free_gb}
    out["delete_enabled"] = policy.delete_enabled
    out["semantic_enabled"] = ctx.settings.semantic_enabled()
    if ctx.archive is not None:
        out["runner"] = ctx.archive.status()
    out["ok"] = True
    return out


# --------------------------------------------------------------------------
# get_raw_message
# --------------------------------------------------------------------------


async def _get_raw_message(ctx: Context, *, id: str,
                           ttl_minutes: int = 15) -> dict[str, Any]:
    cache = _require_cache(ctx)
    row = await asyncio.to_thread(cache.get_message, id)
    if row is None:
        raise ToolError("not_found", f"No mirrored message matches {id!r}.",
                        hint="Re-run search_messages for a fresh id.")
    if not row["mime_path"]:
        raise ToolError(
            "not_found",
            "That message is not archived yet, so there is no raw MIME to serve.",
            hint="Raw MIME exists only for captured/verified/deleted mail — "
                 "check archive_status, or use get_message for the text.")
    path = Path(row["mime_path"])
    if not path.is_file():
        raise ToolError(
            "not_found", f"The archived MIME file is missing at {path}.",
            hint="The next verify pass will reset this message to live and "
                 "re-capture it.")
    ttl = max(1, min(int(ttl_minutes), 1440))
    subject = (row["subject"] or "message").strip() or "message"
    name = f"{subject[:60]}.eml"
    rec = downloads.mint(ctx.settings.data_dir, path=str(path), name=name,
                         content_type="message/rfc822", ttl_seconds=ttl * 60)
    await asyncio.to_thread(downloads.sweep, ctx.settings.data_dir)
    base = (getattr(ctx.settings, "external_url", "") or "").rstrip("/")
    url = f"{base}/download/{rec['token']}" if base else f"/download/{rec['token']}"
    return {
        "ok": True,
        "download_url": url,
        "name": rec["name"],
        "content_type": "message/rfc822",
        "size_bytes": path.stat().st_size,
        "sha256": row["mime_sha256"],
        "expires_in_minutes": ttl,
        "curl": f'curl -o {rec["name"]!r} "{url}"',
        "note": "single use — the link is spent by the first successful GET",
    }


# --------------------------------------------------------------------------
# find_similar
# --------------------------------------------------------------------------


async def _find_similar(ctx: Context, *, id: str | None = None,
                        text: str | None = None, limit: int = 10,
                        archived: str = "any") -> dict[str, Any]:
    if bool(id) == bool(text):
        raise ToolError("validation",
                        "pass exactly one of `id` (find mail like this message) "
                        "or `text` (find mail like this description)")
    if archived not in ARCHIVED_MODES:
        raise ToolError("validation",
                        f"archived must be one of {', '.join(ARCHIVED_MODES)}")
    if ctx.semantic is None:
        raise ToolError(
            "validation", "semantic search is not configured on this server",
            hint="Set GEMINI_API_KEY on ewsd; keyword search "
                 "(search_messages) works regardless.")
    cache = _require_cache(ctx)
    limit = max(1, min(int(limit), 50))

    def query() -> list[dict[str, Any]]:
        if id:
            return ctx.semantic.similar_to_message(id, limit=limit,
                                                   archived=archived)
        hits = ctx.semantic.vector_ids(text or "", limit=limit,
                                       archived=archived)
        by_id = cache.messages_by_ids([i for i, _d in hits])
        out = []
        for ews_id, dist in hits:
            row = by_id.get(ews_id)
            if row is not None:
                row = dict(row)
                row["similarity"] = round(1.0 - float(dist), 4)
                out.append(row)
        return out

    try:
        rows = await asyncio.to_thread(query)
    except ToolError:
        raise
    except Exception as exc:  # noqa: BLE001 - a dead embedder is not a 502
        raise ToolError(
            "upstream_unavailable", f"the embedding service failed: {exc}",
            hint="Keyword search (search_messages) is unaffected.",
            retry_after_s=60) from exc
    cards = []
    for row in rows:
        card = _row_card(ctx, row)
        card["similarity"] = row["similarity"]
        cards.append(card)
    return envelope(cards, total_available=len(cards), offset=0)


# --------------------------------------------------------------------------
# Specs
# --------------------------------------------------------------------------


def _schema(properties: dict[str, Any],
            required: list[str] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "additionalProperties": False,
                              "properties": properties}
    if required:
        schema["required"] = required
    return schema


TOOLS: list[ToolSpec] = [
    ToolSpec(
        name="archive_run",
        description=(
            "Run one archive pass. dry_run=true (the default) only REPORTS: "
            "how many messages match the policy and which would go, touching "
            "neither Exchange nor disk — always start there. dry_run=false "
            "actually captures (raw MIME + attachment blobs to the server's "
            "data dir), verifies, embeds, and — only when "
            "ARCHIVE_DELETE_ENABLED=true and a message is verified, older than "
            "the cutoff and past the grace period — hard-deletes it from "
            "Exchange, capped per run. dry_run=false is two-phase confirmed. "
            "`before` and `folders` narrow this pass only."
        ),
        side_effect_class="destructive",
        requires_ews=True,
        input_schema=_schema({
            "dry_run": {
                "type": "boolean", "default": True,
                "description": "true reports without changing anything.",
            },
            "kind": {
                "type": "string", "enum": list(KINDS), "default": "all",
                "description": "Which workers to run in this pass.",
            },
            "before": {
                "type": "string",
                "description": "Override the age cutoff for this pass: "
                               "YYYY-MM-DD, an ISO datetime, or '-Nd'.",
            },
            "folders": {
                "type": "array", "items": {"type": "string"},
                "description": "Override ARCHIVE_FOLDERS for this pass "
                               "(well-known keys, e.g. ['inbox']). Calendar, "
                               "contacts, tasks, drafts and outbox are never "
                               "archived.",
            },
        }),
        handler=_archive_run,
        confirm=lambda kw: not kw.get("dry_run", True),
    ),
    ToolSpec(
        name="archive_status",
        description=(
            "Where the archive stands: message counts per state (live, "
            "captured, verified, deleted), the last five runs with their "
            "counts and errors, blob-store size and free disk, the embedding "
            "backlog, the active policy, and whether deletion is enabled. "
            "Answered from Postgres — it works while Exchange is down."
        ),
        side_effect_class="read",
        requires_ews=False,
        input_schema=_schema({}),
        handler=_archive_status,
    ),
    ToolSpec(
        name="get_raw_message",
        description=(
            "Get the original RFC822 message of an ARCHIVED mail as a "
            "single-use download URL (the bytes never travel through the "
            "conversation). Works for captured, verified and deleted "
            "messages; live mail has no stored MIME yet. The link expires and "
            "is spent by the first successful download."
        ),
        side_effect_class="read",
        requires_ews=False,
        input_schema=_schema({
            "id": {"type": "string", "description": "Message id (m-alias or raw)."},
            "ttl_minutes": {"type": "integer", "minimum": 1, "maximum": 1440,
                            "default": 15},
        }, required=["id"]),
        handler=_get_raw_message,
    ),
    ToolSpec(
        name="find_similar",
        description=(
            "Find mail that MEANS the same thing, not mail that shares words: "
            "pass `id` to find messages like that one, or `text` to describe "
            "what you are looking for. Ranked by embedding similarity over "
            "live and archived mail alike; each card carries `similarity` "
            "(1.0 is identical). Use search_messages for exact terms, names "
            "and dates."
        ),
        side_effect_class="read",
        requires_ews=False,
        input_schema=_schema({
            "id": {"type": "string",
                   "description": "Seed message id (m-alias or raw)."},
            "text": {"type": "string",
                     "description": "Free-text description of what to find."},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50,
                      "default": 10},
            "archived": {"type": "string", "enum": list(ARCHIVED_MODES),
                         "default": "any",
                         "description": "any (default) | only | exclude."},
        }),
        handler=_find_similar,
    ),
]
```

- [ ] **Step 3b: Register the pack**

`ewsmcp/tools/__init__.py`:

```python
from . import archive, calendar_people, mail_read, tasks, writes
...
    specs = [*mail_read.TOOLS, *calendar_people.TOOLS, *tasks.TOOLS,
             *writes.TOOLS, *archive.TOOLS]
```

`ewsmcp/mcp/registry.py`:

```python
from ..tools import archive, calendar_people, mail_read, tasks, writes
...
    for spec in [*mail_read.TOOLS, *calendar_people.TOOLS, *tasks.TOOLS,
                 *writes.TOOLS, *archive.TOOLS]:
```

`scripts/dump_tool_table.py`:

```python
from ewsmcp.tools import archive, calendar_people, mail_read, tasks, writes  # noqa: E402
...
def _packs():
    return [
        ("mail-read", mail_read.TOOLS),
        ("calendar / people / status", calendar_people.TOOLS),
        ("tasks / waiting-on", tasks.TOOLS),
        ("writes", writes.TOOLS),
        ("archive / semantic", archive.TOOLS),
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_archive_tools.py tests/test_surface_completion.py tests/test_mcp_thin.py -q`
Expected: PASS

- [ ] **Step 5: Regenerate the docs table and run everything**

Run: `.venv/bin/python scripts/dump_tool_table.py --write && .venv/bin/python -m ruff check ewsmcp tests scripts && .venv/bin/python -m pytest tests -q`
Expected: `updated .../docs/API.md`, no lint findings, all tests pass (including `test_docs_match_registry.py`).

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/tools/archive.py ewsmcp/tools/__init__.py ewsmcp/mcp/registry.py scripts/dump_tool_table.py docs/API.md tests/test_archive_tools.py tests/test_surface_completion.py tests/test_mcp_thin.py
git commit -m "$(cat <<'EOF'
feat(tools): archive_run, archive_status, get_raw_message, find_similar (18/29/35)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 13: Archived mail in the existing read tools

**Files:**
- Modify: `ewsmcp/tools/cache_reads.py`, `ewsmcp/tools/mail_read.py`
- Test: `tests/test_mail_read_archive.py`

**Interfaces:**
- Consumes: `CacheStore.search_messages(..., archived=…)` (existing), `CacheStore.archived_counts_by_folder`, `attachments_for` (Task 4), `SemanticIndex.hybrid_search` (Task 6), `archive.files.blob_path` (Task 3).
- Produces:
  - `search_messages` gains `archived: "any" | "only" | "exclude"` (default `any`); `mode="semantic"` runs the hybrid and stamps `meta.degraded` when the vector half failed.
  - every message card carries `archive_state`.
  - `list_folders` rows gain `archived`.
  - `get_attachment` on a non-live message serves from the blob store and stamps `source: "archive"`.
  - `cache_reads.search_messages(...)` gains keyword-only params `archived: str = "any"`, `mode: str = "keyword"`.
  - New: `async def cache_reads.attachment_from_archive(ctx, raw_id: str, attachment: str | None, mode: str) -> dict | None`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_mail_read_archive.py`:

```python
"""Archived mail through the ordinary read tools: filters, cards, folders,
attachments from the blob store, and the semantic mode."""

import asyncio
import time

from conftest import FakeEmbedder, make_context, make_row

from ewsmcp.archive import files
from ewsmcp.semantic import SemanticIndex
from ewsmcp.tools.base import dispatch

NOW = int(time.time())


def _run(ctx, name, **kw):
    return asyncio.run(dispatch(ctx, ctx.registry[name], dict(kw)))


def _ctx(db, tmp_path, **over):
    over.setdefault("ews_capability_tier", "full")
    over.setdefault("data_dir", str(tmp_path / "data"))
    ctx = make_context(db, **over)
    ctx.cache.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 3, "unread": 0, "children": 0},
        {"ews_id": "FID-SENT", "name": "Sent", "path": "Sent", "wk": "f:sent",
         "total": 1, "unread": 0, "children": 0}])
    ctx.cache.upsert_messages([
        make_row("LIVE-1", subject="Budget live", folder_id="FID-INBOX"),
        make_row("ARCH-1", subject="Budget archived", folder_id="FID-INBOX"),
        make_row("ARCH-2", subject="Budget sent", folder_id="FID-SENT"),
    ])
    ctx.cache.mark_captured("ARCH-1", mime_sha256="a" * 64, mime_path="/x.eml")
    ctx.cache.mark_captured("ARCH-2", mime_sha256="a" * 64, mime_path="/x.eml")
    ctx.cache.mark_verified("ARCH-2")
    ctx.cache.set_sync_state("item:FID-INBOX", "T", NOW)
    ctx.cache.set_sync_state("item:FID-SENT", "T", NOW)
    ctx.cache.set_sync_state("events", None, NOW)
    return ctx


def test_search_defaults_to_any_and_cards_carry_archive_state(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    res = _run(ctx, "search_messages", query="budget")
    assert res["count"] == 3
    states = {i["subject"]: i["archive_state"] for i in res["items"]}
    assert states["Budget live"] == "live"
    assert states["Budget archived"] == "captured"
    assert states["Budget sent"] == "verified"


def test_search_archived_only_and_exclude(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    only = _run(ctx, "search_messages", query="budget", archived="only")
    assert {i["subject"] for i in only["items"]} == {"Budget archived",
                                                    "Budget sent"}
    excl = _run(ctx, "search_messages", query="budget", archived="exclude")
    assert {i["subject"] for i in excl["items"]} == {"Budget live"}


def test_search_rejects_an_unknown_archived_value(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    res = _run(ctx, "search_messages", query="budget", archived="sometimes")
    assert res["ok"] is False and res["error"]["code"] == "validation"


def test_list_folders_reports_archived_counts(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    rows = {r["name"]: r for r in _run(ctx, "list_folders")["items"]}
    assert rows["Inbox"]["archived"] == 1
    assert rows["Sent"]["archived"] == 1


def test_get_attachment_of_archived_mail_reads_the_blob_store(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    sha, _path = files.store_blob(ctx.settings.data_dir, b"col1,col2\n1,2\n")
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "data.csv", "content_type": "text/csv", "size": 14,
         "sha256": sha, "is_inline": 0}])
    res = _run(ctx, "get_attachment", message_id="ARCH-1")
    assert res["ok"] is True and res["source"] == "archive"
    assert res["mode"] == "text" and "col1,col2" in res["text"]
    assert ctx.gateway.calls == 0          # Exchange was never touched


def test_get_attachment_save_mode_writes_and_publishes(db, tmp_path):
    ctx = _ctx(db, tmp_path, shared_dir=str(tmp_path / "shared"))
    (tmp_path / "shared").mkdir()
    sha, _p = files.store_blob(ctx.settings.data_dir, b"%PDF-1.4")
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "q3.pdf", "content_type": "application/pdf", "size": 8,
         "sha256": sha, "is_inline": 0}])
    res = _run(ctx, "get_attachment", message_id="ARCH-1", mode="save")
    from pathlib import Path
    assert Path(res["saved_path"]).read_bytes() == b"%PDF-1.4"
    assert res["shared_name"] == "q3.pdf"


def test_get_attachment_picks_by_name_among_several(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    a, _ = files.store_blob(ctx.settings.data_dir, b"AAA")
    b, _ = files.store_blob(ctx.settings.data_dir, b"BBB")
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "a.txt", "content_type": "text/plain", "size": 3, "sha256": a,
         "is_inline": 0},
        {"name": "b.txt", "content_type": "text/plain", "size": 3, "sha256": b,
         "is_inline": 0}])
    assert _run(ctx, "get_attachment", message_id="ARCH-1",
                attachment="b.txt")["text"] == "BBB"
    assert _run(ctx, "get_attachment", message_id="ARCH-1",
                attachment="1")["text"] == "BBB"


def test_an_item_attachment_of_archived_mail_points_at_the_raw_mime(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    ctx.cache.replace_attachments("ARCH-1", [
        {"name": "Fwd: contract", "content_type": "message/rfc822", "size": 900,
         "sha256": None, "is_inline": 0}])
    res = _run(ctx, "get_attachment", message_id="ARCH-1")
    assert res["mode"] == "info"
    assert "get_raw_message" in res["hint"]


def test_semantic_mode_runs_the_hybrid_and_is_not_degraded(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    ctx.semantic = SemanticIndex(ctx.cache, FakeEmbedder())
    ctx.semantic.index_messages(ctx.cache.unembedded_messages(100))
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is True and res["count"] == 3
    assert res.get("meta", {}).get("degraded") is not True


def test_semantic_mode_degrades_to_keyword_when_the_embedder_dies(db, tmp_path):
    ctx = _ctx(db, tmp_path)

    class Broken:
        def embed(self, texts):
            raise RuntimeError("gemini down")

    ctx.semantic = SemanticIndex(ctx.cache, Broken())
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is True and res["count"] == 3
    assert res["meta"]["degraded"] is True


def test_semantic_mode_without_a_key_degrades_rather_than_failing(db, tmp_path):
    ctx = _ctx(db, tmp_path)
    ctx.semantic = None
    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["ok"] is True and res["meta"]["degraded"] is True
    assert "GEMINI_API_KEY" in res["meta"]["reason"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_mail_read_archive.py -q`
Expected: FAIL — `KeyError: 'archive_state'` on the first test; `archived` is rejected by the tool schema.

- [ ] **Step 3a: `ewsmcp/tools/cache_reads.py`**

Stamp the state on every card, in `_row_card`, just before the return:

```python
    card["archive_state"] = row["archive_state"]
```

Add the archived count to `list_folders`, inside the row loop:

```python
    archived = ctx.cache.archived_counts_by_folder()
    ...
        row["archived"] = archived.get(r["ews_id"], 0)
```

Extend `search_messages` with the two new keyword-only parameters and the semantic branch:

```python
async def search_messages(ctx: Context, *, folder: str | None,
                          query: str | None, sender: str | None,
                          subject: str | None, since: str | None,
                          until: str | None, is_unread: bool | None,
                          has_attachments: bool | None, offset: int,
                          limit: int, archived: str = "any",
                          mode: str = "keyword") -> dict[str, Any] | None:
    if archived not in ("any", "only", "exclude"):
        raise ToolError("validation",
                        "archived must be 'any', 'only' or 'exclude'")
    ...
        filters = dict(sender=sender, subject=subject, since_ts=since_ts,
                       until_ts=until_ts, is_unread=is_unread,
                       has_attachments=has_attachments,
                       folders=[folder_id] if folder_id else None)
        meta: dict[str, Any] | None = None
        if mode == "semantic":
            rows, total, meta = await asyncio.to_thread(
                _semantic_rows, ctx, query or "", archived, offset, limit, filters)
        else:
            rows, total = await asyncio.to_thread(
                ctx.cache.search_messages, text=query, archived=archived,
                offset=offset, limit=limit, **filters)
        cards = await asyncio.to_thread(lambda: [_row_card(ctx, r) for r in rows])
        out = _stamp(envelope(cards, total_available=total, offset=offset),
                     "cache", as_of)
        if meta:
            out["meta"] = meta
        return out
```

with this helper above it:

```python
def _semantic_rows(ctx: Context, query: str, archived: str, offset: int,
                   limit: int, filters: dict[str, Any]):
    """Hybrid when an index exists, keyword otherwise — never an error.

    A missing key or a dead Gemini must degrade the ANSWER, not remove the
    tool: the model asked for meaning and gets words, clearly labelled."""
    if ctx.semantic is None:
        rows, total = ctx.cache.search_messages(
            text=query, archived=archived, offset=offset, limit=limit, **filters)
        return rows, total, {"degraded": True,
                             "reason": "semantic search is not configured "
                                       "(GEMINI_API_KEY unset) — keyword results"}
    rows, degraded = ctx.semantic.hybrid_search(
        query, limit=limit, offset=offset, archived=archived, **filters)
    meta = {"degraded": True,
            "reason": "the embedding service failed — keyword results"} \
        if degraded else {"degraded": False, "mode": "hybrid_rrf"}
    return rows, len(rows) + offset, meta
```

Add the blob-store attachment reader (it needs `from pathlib import Path`, `from .. import shared`, `from ..archive import files`):

```python
_TEXTY_TYPES = ("text/",)
_TEXTY_SUFFIXES = (".txt", ".csv", ".md", ".log", ".json")


def _pick_row(rows: list[dict[str, Any]], selector: str | None) -> dict[str, Any]:
    if selector is None:
        if len(rows) != 1:
            raise ToolError(
                "validation",
                f"this message has {len(rows)} attachments — pass `attachment` "
                "with a name or a zero-based index as a string",
                hint="Names: " + ", ".join(str(r["name"]) for r in rows[:10]))
        return rows[0]
    if selector.isdigit() and int(selector) < len(rows):
        return rows[int(selector)]
    for row in rows:
        if (row["name"] or "").lower() == selector.lower():
            return row
    raise ToolError("not_found", f"No attachment named {selector!r} on this message.",
                    hint="Names: " + ", ".join(str(r["name"]) for r in rows[:10]))


async def attachment_from_archive(ctx: Context, raw_id: str,
                                  attachment: str | None,
                                  mode: str) -> dict[str, Any] | None:
    """Serve an attachment of ARCHIVED mail from the blob store, or None."""
    if ctx.cache is None:
        return None
    row = await asyncio.to_thread(ctx.cache.get_message, raw_id)
    if row is None or row["archive_state"] == "live":
        return None
    rows = await asyncio.to_thread(ctx.cache.attachments_for, row["ews_id"])
    if not rows:
        raise ToolError("not_found", "This archived message has no attachments.")
    att = _pick_row(rows, attachment)
    name = att["name"] or "attachment"
    out: dict[str, Any] = {"ok": True, "name": name, "size_bytes": att["size"],
                           "content_type": att["content_type"],
                           "source": "archive"}
    if not att["sha256"]:
        # A nested message: it exists only inside the raw MIME.
        out["mode"] = "info"
        out["hint"] = ("Nested message attachment — it lives inside the raw "
                       "MIME. Call get_raw_message for the original .eml.")
        return out
    path = files.blob_path(ctx.settings.data_dir, att["sha256"])
    if not path.is_file():
        raise ToolError("not_found", f"The archived blob is missing at {path}.",
                        hint="The next verify pass re-captures this message.")
    texty = (att["content_type"] or "").lower().startswith(_TEXTY_TYPES) or \
        name.lower().endswith(_TEXTY_SUFFIXES)
    chosen = mode
    if mode == "auto":
        chosen = "text" if texty else "info"
        if chosen == "info":
            out["hint"] = ("Binary attachment — metadata only. Call again with "
                           "mode='save' to write it to disk.")
    if chosen == "info":
        out["mode"] = "info"
        return out
    data = await asyncio.to_thread(path.read_bytes)
    if chosen == "text":
        text = data.decode("utf-8", errors="replace")
        out["mode"] = "text"
        out["text"] = text[:20_000]
        if len(text) > 20_000:
            out["truncated"] = True
        return out
    safe = shared.safe_name(name)
    dest = Path(ctx.settings.data_dir) / "attachments"
    dest.mkdir(parents=True, exist_ok=True)
    saved = dest / safe
    await asyncio.to_thread(saved.write_bytes, data)
    out["mode"] = "save"
    out["saved_path"] = str(saved)
    try:
        published = shared.publish(ctx.settings.shared_dir, saved)
    except OSError as exc:
        logger.warning("could not publish %s to the shared space: %s", safe, exc)
        published = None
    if published:
        out["shared_name"] = published
        out["shared_path"] = str(Path(ctx.settings.shared_dir) / published)
    return out
```

- [ ] **Step 3b: `ewsmcp/tools/mail_read.py`**

Pass the two new arguments through `_search_messages` (replacing the `mode == "semantic"` rejection):

```python
async def _search_messages(ctx: Context, query: str | None = None, ...,
                           archived: str = "any", mode: str = "keyword",
                           **kw) -> Dict[str, Any]:
    ...
    hit = await cache_reads.search_messages(
        ctx, folder=folder, query=query, sender=sender, subject=subject,
        since=since, until=until, is_unread=is_unread,
        has_attachments=has_attachments, offset=offset, limit=limit,
        archived=archived, mode=mode)
```

Try the archive first in `_get_attachment`, before the live closure:

```python
async def _get_attachment(ctx: Context, message_id: str,
                          attachment: Optional[str] = None,
                          mode: str = "auto") -> Dict[str, Any]:
    raw_id = message_id
    # Archived mail has no server copy to fetch — its bytes are ours, on disk.
    from_archive = await cache_reads.attachment_from_archive(
        ctx, raw_id, attachment, mode)
    if from_archive is not None:
        return from_archive
    ...
```

Extend the `search_messages` schema with:

```python
            "archived": {
                "type": "string", "enum": ["any", "only", "exclude"],
                "default": "any",
                "description": "any (default) searches live and archived mail; "
                               "only restricts to archived; exclude to live.",
            },
```

and replace the `mode` description:

```python
            "mode": {
                "type": "string", "enum": ["keyword", "semantic"],
                "default": "keyword",
                "description": "keyword = full-text over the mirror; semantic "
                               "= hybrid (full-text + embedding similarity, "
                               "RRF-fused). semantic falls back to keyword "
                               "with meta.degraded=true when embeddings are "
                               "unavailable.",
            },
```

Mention the new fields in the `list_folders` and `get_attachment` descriptions:

- `list_folders`: append to the description — `"Each row also carries archived: how many of that folder's messages now live only in the archive."`
- `get_attachment`: append — `"Attachments of archived mail are served from the server's blob store (stamped source='archive'); Exchange is not contacted."`

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_mail_read_archive.py -q`
Expected: PASS (12 passed)

- [ ] **Step 5: Regenerate the docs and run everything**

Run: `.venv/bin/python scripts/dump_tool_table.py --write && .venv/bin/python -m ruff check ewsmcp tests scripts && .venv/bin/python -m pytest tests -q`
Expected: clean, all pass. `tests/test_cache_reads.py` and `tests/test_envelope_contract.py` may need `archive_state` added to their expected card keys — update the expectation, never weaken the assertion.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/tools/cache_reads.py ewsmcp/tools/mail_read.py docs/API.md tests
git commit -m "$(cat <<'EOF'
feat(read): archived filter, archive_state on cards, blob-store attachments, semantic mode

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 14: Daemon archive routes

**Files:**
- Modify: `ewsmcp/http.py`
- Test: `tests/test_daemon_api.py` (append)

**Interfaces:**
- Consumes: `ctx.archive.run_once` (Task 9), `ctx.cache.get_run` (Task 4), `_read_json_body`, `_send_json` (existing).
- Produces: `POST /v1/archive/run` (body `{"kind","dry_run","before","folders"}`) and `GET /v1/archive/runs/<id>`, both behind the `EWSD_API_KEY` bearer.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_daemon_api.py`:

```python
class _Runner:
    def __init__(self):
        self.calls = []

    async def run_once(self, *, kind, dry_run, before, folders):
        self.calls.append((kind, dry_run, before, folders))
        return {"ok": True, "run_id": 11, "kind": kind, "dry_run": dry_run,
                "candidates": 5, "captured": 0, "verified": 0, "reset": 0,
                "deleted": 0, "eligible": 0, "embedded": 0, "failed": 0,
                "blocked": None, "stopped": None, "error": None, "sample": []}


def test_archive_run_route_needs_the_bearer(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    assert _drive(app, "/v1/archive/run", "POST", {})[0] == 401


def test_archive_run_route_defaults_to_a_dry_run(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {}, headers=AUTH)
    assert status == 200 and body["run_id"] == 11
    assert ctx.archive.calls == [("all", True, None, None)]


def test_archive_run_route_passes_the_arguments_through(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    _drive(app, "/v1/archive/run", "POST",
           {"kind": "capture", "dry_run": False, "before": "2026-01-01",
            "folders": ["inbox"]}, headers=AUTH)
    assert ctx.archive.calls == [("capture", False, "2026-01-01", ["inbox"])]


def test_archive_run_route_rejects_an_unknown_kind(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = _Runner()
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {"kind": "nuke"},
                          headers=AUTH)
    assert status == 400 and body["error"]["code"] == "validation"


def test_archive_run_route_without_a_runner_is_503(db):
    ctx = make_context(db, ewsd_api_key="k")
    ctx.archive = None
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, "/v1/archive/run", "POST", {}, headers=AUTH)
    assert status == 503 and body["error"]["code"] == "upstream_unavailable"


def test_archive_run_status_route(db):
    ctx = make_context(db, ewsd_api_key="k")
    run_id = ctx.cache.start_run("capture", dry_run=True, policy={})
    ctx.cache.finish_run(run_id, captured=2)
    app = build_daemon_app(ctx, ctx.settings)
    status, body = _drive(app, f"/v1/archive/runs/{run_id}", headers=AUTH)
    assert status == 200 and body["captured"] == 2 and body["kind"] == "capture"
    assert _drive(app, "/v1/archive/runs/999999", headers=AUTH)[0] == 404
    assert _drive(app, "/v1/archive/runs/abc", headers=AUTH)[0] == 400
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_daemon_api.py -q`
Expected: FAIL — `assert 404 == 200` (the route does not exist; the catch-all answers).

- [ ] **Step 3: Write the implementation**

In `ewsmcp/http.py`, insert after the `/v1/status` route and before the tools routes:

```python
        if path == "/v1/archive/run" and method == "POST":
            body = await _read_json_body(receive, send)
            if body is None:
                return
            if not isinstance(body, dict):
                return await _send_json(send, 400, {"ok": False, "error": {
                    "code": "validation",
                    "message": "request body must be a JSON object"}})
            kind = str(body.get("kind", "all"))
            if kind not in ARCHIVE_KINDS:
                return await _send_json(send, 400, {"ok": False, "error": {
                    "code": "validation",
                    "message": f"kind must be one of {', '.join(ARCHIVE_KINDS)}"}})
            if getattr(ctx, "archive", None) is None:
                return await _send_json(send, 503, {"ok": False, "error": {
                    "code": "upstream_unavailable",
                    "message": "the archive runner is not started"}})
            result = await ctx.archive.run_once(
                kind=kind, dry_run=bool(body.get("dry_run", True)),
                before=body.get("before"), folders=body.get("folders"))
            return await _send_json(send, 200, result)

        if path.startswith("/v1/archive/runs/") and method == "GET":
            raw = path.removeprefix("/v1/archive/runs/")
            if not raw.isdigit():
                return await _send_json(send, 400, {"ok": False, "error": {
                    "code": "validation", "message": "run id must be an integer"}})
            row = ctx.cache.get_run(int(raw)) if ctx.cache is not None else None
            if row is None:
                return await _send_json(send, 404, {"ok": False, "error": {
                    "code": "not_found", "message": f"no archive run {raw}"}})
            return await _send_json(send, 200, {"ok": True, **dict(row)})
```

and import the kinds at the top of `http.py`:

```python
from .archive.runner import KINDS as ARCHIVE_KINDS
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_daemon_api.py -q`
Expected: PASS

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m pytest tests -q`
Expected: PASS. `tests/test_no_lazy_imports.py` still passes — `archive.runner` imports `exchangelib` only transitively through `capture`, at module top.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/http.py tests/test_daemon_api.py
git commit -m "$(cat <<'EOF'
feat(daemon): POST /v1/archive/run and GET /v1/archive/runs/<id>

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 15: Thin MCP — local `archive_status`, forwarded semantic

**Files:**
- Modify: `ewsmcp/mcp/local.py`
- Test: `tests/test_mcp_thin.py` (append)

**Interfaces:**
- Consumes: `tools.archive._archive_status` (Task 12), `cache_reads.search_messages(..., archived=…)` (Task 13), `_cache_then_forward` (existing).
- Produces: `local.HANDLERS` gains `"archive_status"`, so `LOCAL_TOOLS` becomes
  `{list_folders, search_messages, get_message, get_thread, get_mailbox_overview, list_tasks, waiting_on, get_server_status, archive_status}`.

**Decision (spec §1 "safety split", made explicit here): the MCP must never hold the Gemini key.** `find_similar` and `search_messages(mode="semantic")` are therefore NOT local — they forward to `ewsd`, which owns `GEMINI_API_KEY` and the `SemanticIndex`. `archive_run` and `get_raw_message` also proxy (the runner and the blob store live in the daemon). `archive_status` is local because Postgres holds every number it reports.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_mcp_thin.py`:

```python
def test_archive_status_is_answered_locally_with_the_daemon_down(db):
    ctx = _mcp_ctx(db, DeadDaemon())
    _seed(ctx)
    ctx.cache.mark_captured("RAW-1", mime_sha256="a" * 64, mime_path="/x.eml")
    res = _run(ctx, "archive_status")
    assert res["ok"] is True
    assert res["states"]["captured"] == 1
    assert "archive_status" in LOCAL_TOOLS


def test_the_mcp_never_holds_the_gemini_key(db):
    """find_similar and mode=semantic are FORWARDED: only ewsd embeds."""
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon, ews_capability_tier="full")
    _seed(ctx)
    assert "find_similar" not in LOCAL_TOOLS

    res = _run(ctx, "find_similar", text="budget")
    assert res["proxied"] == "find_similar"

    res = _run(ctx, "search_messages", query="budget", mode="semantic")
    assert res["proxied"] == "search_messages"
    assert daemon.calls[-1][1]["mode"] == "semantic"


def test_keyword_search_stays_local_and_honours_archived(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon)
    _seed(ctx)
    ctx.cache.mark_captured("RAW-1", mime_sha256="a" * 64, mime_path="/x.eml")
    res = _run(ctx, "search_messages", query="budget", archived="only")
    assert res["source"] == "cache"
    assert [i["archive_state"] for i in res["items"]] == ["captured"]
    assert daemon.calls == []


def test_archive_run_and_get_raw_message_proxy_to_the_daemon(db):
    daemon = RecordingDaemon()
    ctx = _mcp_ctx(db, daemon, ews_capability_tier="full")
    assert _run(ctx, "archive_run", dry_run=True)["proxied"] == "archive_run"
    assert _run(ctx, "get_raw_message", id="RAW-1")["proxied"] == "get_raw_message"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_mcp_thin.py -q`
Expected: FAIL — `assert 'archive_status' in LOCAL_TOOLS` fails (it proxies today), and `search_messages(mode="semantic")` raises the old validation error instead of forwarding.

- [ ] **Step 3: Write the implementation**

In `ewsmcp/mcp/local.py`, replace the semantic rejection in `search_messages` and add the new handler:

```python
async def search_messages(ctx: Context, **kw) -> dict[str, Any]:
    cache_call = None
    # mode='semantic' needs an embedder, and the MCP deliberately does not
    # hold GEMINI_API_KEY — ewsd owns the key and the vector index, so the
    # call is forwarded verbatim.
    if not kw.get("fresh") and kw.get("mode", "keyword") != "semantic":
        sender = cache_reads.validate_search_args(
            kw.get("sender"), kw.get("from_"), kw.get("subject"), kw.get("since"),
            kw.get("until"), kw.get("is_unread"), kw.get("has_attachments"),
            kw.get("query"))
        cache_call = lambda: cache_reads.search_messages(
            ctx, folder=kw.get("folder"), query=kw.get("query"), sender=sender,
            subject=kw.get("subject"), since=kw.get("since"), until=kw.get("until"),
            is_unread=kw.get("is_unread"), has_attachments=kw.get("has_attachments"),
            offset=int(kw.get("offset", 0)), limit=int(kw.get("limit", 20)),
            archived=kw.get("archived", "any"))
    return await _cache_then_forward(ctx, "search_messages", kw, cache_call)


async def archive_status(ctx: Context, **kw) -> dict[str, Any]:
    """Every number archive_status reports lives in Postgres, so the MCP can
    answer it while ewsd is down — which is exactly when you want to ask."""
    from ..tools.archive import _archive_status
    try:
        return await _archive_status(ctx)
    except (psycopg.Error, RuntimeError) as exc:
        raise ToolError("backend_unavailable", f"Postgres unreachable ({exc})",
                         hint="Check DATABASE_URL.", retry_after_s=15) from exc
```

and extend the map:

```python
HANDLERS = {
    "list_folders": list_folders, "search_messages": search_messages,
    "get_message": get_message, "get_thread": get_thread,
    "get_mailbox_overview": get_mailbox_overview, "list_tasks": list_tasks,
    "waiting_on": waiting_on, "get_server_status": get_server_status,
    "archive_status": archive_status,
}
```

Note the `_archive_status` import is deliberately inside the function: `tools/archive.py` pulls in `ewsmcp.archive.files`, and keeping the MCP's module graph free of the archive package at import time keeps `test_no_lazy_imports` honest about what the MCP touches. (It imports no exchangelib either way; `files.py` has none.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_mcp_thin.py -q`
Expected: PASS

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m ruff check ewsmcp tests scripts && .venv/bin/python -m pytest tests -q`
Expected: clean, all pass.

- [ ] **Step 6: Commit**

```bash
git add ewsmcp/mcp/local.py tests/test_mcp_thin.py
git commit -m "$(cat <<'EOF'
feat(mcp): local archive_status; semantic search forwarded (the MCP holds no key)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 16: Boot smoke, docs, version 5.1.0a1

**Files:**
- Modify: `scripts/boot_smoke.py`, `DESIGN.md`, `README.md`, `CHANGELOG.md`, `docs/API.md`, `ewsmcp/__init__.py`, `pyproject.toml`, `tests/test_docs_match_registry.py`
- Test: `tests/test_docs_match_registry.py`

**Interfaces:**
- Consumes: the finished tool surface (Tasks 12–15).
- Produces: `ewsmcp.__version__ == "5.1.0a1"`, a regenerated `docs/API.md`, and a boot smoke that exercises the archive cold.

- [ ] **Step 1: Write the failing test**

In `tests/test_docs_match_registry.py`, change the version assertion:

```python
def test_version_is_51_line():
    spec = importlib.util.spec_from_file_location(
        "_v5_init", V5_ROOT / "ewsmcp" / "__init__.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.__version__.startswith("5.1."), mod.__version__
    pyproject = (V5_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert f'version = "{mod.__version__}"' in pyproject
```

and add a docs guard:

```python
def test_docs_describe_the_archive():
    design = (V5_ROOT / "DESIGN.md").read_text(encoding="utf-8")
    assert "## §Archive" in design
    assert "Phase 2 (not yet built)" not in design
    readme = (V5_ROOT / "README.md").read_text(encoding="utf-8")
    for key in ("ARCHIVE_DELETE_ENABLED", "ARCHIVE_AFTER_DAYS", "GEMINI_API_KEY",
                "ARCHIVE_GRACE_DAYS", "ARCHIVE_MAX_DELETE_PER_RUN",
                "ARCHIVE_MIN_FREE_GB", "ARCHIVE_CYCLE_SECONDS", "EMBED_DIMS"):
        assert key in readme, key
    changelog = (V5_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## [5.1.0a1]" in changelog
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_docs_match_registry.py -q`
Expected: FAIL — `assert '5.0.0a1'.startswith('5.1.')` and `assert '## §Archive' in design`.

- [ ] **Step 3a: Bump the version**

`ewsmcp/__init__.py` → `__version__ = "5.1.0a1"`; `pyproject.toml` → `version = "5.1.0a1"`.

- [ ] **Step 3b: `DESIGN.md`**

Replace the whole `## Phase 2 (not yet built)` section with:

```markdown
## §Archive — capture, verify, delete

The mailbox is near quota and the weight is attachment bytes, so `ewsd` moves
old mail onto local disk and then removes it from Exchange. Three idempotent
workers, each driven by `messages.archive_state`, run every
`ARCHIVE_CYCLE_SECONDS` (300) in an asyncio task started after warm-up:

- **Capture** takes `live` rows matching the policy (folder in
  `ARCHIVE_FOLDERS`, older than `ARCHIVE_AFTER_DAYS`, category not in
  `ARCHIVE_EXCLUDE_CATEGORIES`), fetches `mime_content` plus every attachment
  in ONE `Account.fetch`, writes `{DATA_DIR}/mime/<sha256>.eml` and
  `{DATA_DIR}/blobs/<sha[:2]>/<sha>`, fills `ews.attachments`, and sets
  `captured`. Batches of 25. Free space is checked against
  `ARCHIVE_MIN_FREE_GB` before each batch; writes are temp-then-rename, so a
  crash never leaves wrong bytes under a content-addressed name.
- **Verify** re-fetches each `captured` row (`changekey`, `attachments`),
  re-hashes the MIME file from disk and checks every blob's existence and
  size. All pass → `verified`. Any failure → back to `live`, capture retries.
- **Delete** hard-deletes `verified` rows through THREE independent rails —
  `ARCHIVE_DELETE_ENABLED=true`, capability tier `full` (plus a confirm token
  on `archive_run(dry_run=false)`), and verified older than the cutoff plus
  `ARCHIVE_GRACE_DAYS` — capped at `ARCHIVE_MAX_DELETE_PER_RUN` per pass, one
  audit record per deletion carrying `ews_id`, `internet_message_id`,
  `mime_sha256` and the run id. Off by default.

Rows are never dropped for archived mail: `ews_id` stays the stable key, so
search, `get_message` and `get_thread` read archived mail exactly like live
mail. When Exchange reports a delete for a `captured`/`verified` row (our own
deleter, or a hand-delete in Outlook) the SyncEngine keeps the row and marks
it `deleted`; only `live` rows are dropped. Every pass writes an
`ews.archive_runs` row — the human-readable history behind `archive_status`
and `GET /v1/archive/runs/<id>`.

Calendar, contacts, tasks, drafts and the outbox are never archived.

## §Semantic — embeddings and hybrid search

`ewsd` embeds `subject + body_clean` in 1,500-character chunks with Gemini's
`gemini-embedding-2` at 768 dimensions (plain HTTPS via httpx — no vendor
SDK), stores them in `ews.chunks.embedding vector(768)` behind an HNSW index
with `vector_cosine_ops`, and stamps `messages.embedded_at`. Backlog is
drained every cycle in batches of 100 with exponential backoff on 429/5xx.

`search_messages(mode="semantic")` fuses the tsvector ranking and the vector
ranking with Reciprocal Rank Fusion (k=60); `find_similar(id|text)` is pure
vector search. Both cover live and archived mail. If the embedder or the
vector query fails, the answer degrades to keyword results with
`meta.degraded=true` — a mailbox search never goes dark because a remote API
is rate-limiting us. The MCP process deliberately holds no `GEMINI_API_KEY`:
`find_similar` and `mode="semantic"` are forwarded to `ewsd`.
```

Also update `## §Store` to mention the new tables (`attachments`, `chunks`, `archive_runs`) and `## §Transports` to list `GET /download/<token>` (ahead of the bearer gate, like `/upload`), `POST /v1/archive/run` and `GET /v1/archive/runs/<id>`.

- [ ] **Step 3c: `README.md`**

In `## Configuration (env)`, add a table block:

```markdown
### Archive & semantic search (ewsd only)

| variable | default | what it does |
|---|---|---|
| `ARCHIVE_FOLDERS` | `inbox,sent` | Well-known folders eligible for archiving. Calendar, contacts, tasks, drafts and outbox are never archived. |
| `ARCHIVE_AFTER_DAYS` | `180` | Mail older than this is a capture candidate. |
| `ARCHIVE_EXCLUDE_CATEGORIES` | (empty) | Comma-separated categories that are never archived. |
| `ARCHIVE_GRACE_DAYS` | `7` | A verified message must sit verified this long before it can be deleted. |
| `ARCHIVE_DELETE_ENABLED` | `false` | **The deletion kill-switch.** While false nothing is ever removed from Exchange. |
| `ARCHIVE_MAX_DELETE_PER_RUN` | `200` | Hard cap on deletions per pass. |
| `ARCHIVE_MIN_FREE_GB` | `2` | Capture stops when free disk falls below this. |
| `ARCHIVE_CYCLE_SECONDS` | `300` | How often the archive pass runs. |
| `GEMINI_API_KEY` | (unset) | Enables semantic search. Unset → keyword only; `find_similar` says so. |
| `EMBED_DIMS` | `768` | Fixed by the `vector(768)` column; changing it needs a migration. |

Storage: raw MIME at `{DATA_DIR}/mime/<sha256>.eml`, attachment blobs at
`{DATA_DIR}/blobs/<sha256[:2]>/<sha256>`, deduplicated by content hash.
```

In the tools section, add the four new tools and note that `search_messages`
gained `archived` and a working `mode="semantic"`, `list_folders` gained
`archived` counts, and `get_attachment` serves archived mail from disk.

- [ ] **Step 3d: `CHANGELOG.md`**

Insert above `## [5.0.0a1]`:

```markdown
## [5.1.0a1] - 2026-09-03 (pre-release)

Phase 2: the mail archive. `ewsd` now moves old mail onto local disk with its
attachments, proves the copy is good, and — only when explicitly enabled —
removes it from Exchange. Archived mail stays searchable, readable and
attachable through the same tools. Semantic search arrives with it. Design:
`docs/superpowers/specs/2026-09-03-postgres-archive-daemon-design.md`.

### Added
- Schema v3 (`migrations/003_archive.sql`): `ews.attachments`, `ews.chunks`
  (`embedding vector(768)`, HNSW + `vector_cosine_ops`) and `ews.archive_runs`.
- Archive pipeline in `ewsmcp/archive/`: capturer (raw MIME + attachment
  blobs, content-addressed, temp-then-rename, free-space guard), verifier
  (changekey + MIME hash + blob existence/size, resets to `live` on any
  mismatch), deleter (three independent rails, per-run cap, one audit record
  per deletion) and an embed worker, driven by `ArchiveRunner` every
  `ARCHIVE_CYCLE_SECONDS`.
- Semantic search: `ewsmcp/embeddings.py` (Gemini `gemini-embedding-2` at 768
  dims over plain httpx, batches of 100, backoff on 429/5xx) and
  `ewsmcp/semantic.py` (`SemanticIndex`, cosine search, hybrid RRF k=60).
- Tools: `archive_run` (destructive, tier `full`, `dry_run=false` is
  confirm-gated), `archive_status`, `get_raw_message` (single-use capability
  download URL), `find_similar`. **35 tools at tier full, 29 at draft, 18 at
  read** (was 31 / 26 / 15).
- `search_messages` gains `archived` (`any` | `only` | `exclude`) and a real
  `mode="semantic"`; every card carries `archive_state`. `list_folders` rows
  carry `archived` counts. `get_attachment` serves archived mail from the
  blob store without contacting Exchange.
- Daemon routes: `POST /v1/archive/run`, `GET /v1/archive/runs/<id>`, and
  `GET /download/<token>` (ahead of the bearer gate, like `/upload`).
- Settings: `GEMINI_API_KEY`, `EMBED_DIMS`, `ARCHIVE_FOLDERS`,
  `ARCHIVE_AFTER_DAYS`, `ARCHIVE_EXCLUDE_CATEGORIES`, `ARCHIVE_GRACE_DAYS`,
  `ARCHIVE_DELETE_ENABLED`, `ARCHIVE_MAX_DELETE_PER_RUN`,
  `ARCHIVE_MIN_FREE_GB`, `ARCHIVE_CYCLE_SECONDS`.

### Changed
- A server-side delete of a `captured`/`verified` row no longer drops it: the
  row is kept and marked `deleted`. Live rows are dropped as before.

### Safety
- Deletion is off by default (`ARCHIVE_DELETE_ENABLED=false`) and stays off
  until an operator flips it deliberately.
```

- [ ] **Step 3e: `scripts/boot_smoke.py`**

Change the tool-count assertions (`31` → `35`, and the docstring's "tier=full lists 31 tools"), and add these checks after the `search cold` block:

```python
            status, arch = _req(EWSD_BASE, EWSD_KEY, "POST",
                                "/v1/tools/archive_status", {})
            if status != 200 or arch.get("states", {}).get("live") != 0:
                failures.append(f"archive_status cold {status} {arch}")
            if arch.get("delete_enabled") is not False:
                failures.append("archive_status must report delete_enabled=false")

            if tier == "full":
                # A dry run needs Exchange (the capturer projects live items),
                # so cold it must fail FAST with upstream_unavailable rather
                # than hanging or half-running.
                status, run = _req(EWSD_BASE, EWSD_KEY, "POST",
                                   "/v1/tools/archive_run", {"dry_run": True})
                if run.get("error", {}).get("code") != "upstream_unavailable":
                    failures.append(
                        f"archive_run dry-run cold expected upstream_unavailable, "
                        f"got {status} {run}")
```

and extend the module docstring's assertion list with those two lines.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python scripts/dump_tool_table.py --write && .venv/bin/python -m pytest tests/test_docs_match_registry.py -q`
Expected: PASS

- [ ] **Step 5: Run everything, including the boot smoke**

Run: `.venv/bin/python -m ruff check ewsmcp tests scripts && .venv/bin/python -m pytest tests -q && .venv/bin/python scripts/boot_smoke.py full`
Expected: all tests pass; `boot smoke OK (tier=full, mcp_tools=35)`.

- [ ] **Step 6: Commit**

```bash
git add DESIGN.md README.md CHANGELOG.md docs/API.md ewsmcp/__init__.py pyproject.toml scripts/boot_smoke.py tests/test_docs_match_registry.py
git commit -m "$(cat <<'EOF'
docs: DESIGN §Archive/§Semantic, README settings, CHANGELOG 5.1.0a1, boot smoke

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

---

### Task 17: Deploy to the lab stack (deletion still off)

**Files:**
- Modify: `/home/askar/stack/compose/personal.yml`
- No test file — this task's verification is the running stack.

**Interfaces:**
- Consumes: the released image built from `../src/exchange-mcp` (the `ewsd` and `ews-mcp` services already exist and already share `postgres-ews`, which already runs `pgvector/pgvector:pg16`).
- Produces: an `ewsd` container with the archive pipeline configured and `ARCHIVE_DELETE_ENABLED=false`.

- [ ] **Step 1: Confirm the Gemini key's variable name**

```bash
grep -n GEMINI /home/askar/stack/.env | cut -d= -f1
```

Expected: `37:GEMINI_API_KEY` (already present for gemini-mcp — reuse it, do not add a second key).

- [ ] **Step 2: Merge the branch and tag the image**

```bash
cd /home/askar/src/exchange-mcp
git checkout main && git merge --no-ff feat/phase2-archive
git rev-parse --short HEAD   # note this; it becomes EWS_MCP_TAG
```

- [ ] **Step 3: Edit the `ewsd` service environment**

In `/home/askar/stack/compose/personal.yml`, inside `services.ewsd.environment`,
remove the now-dead `EWS_CACHE_WINDOW_DAYS` line (Phase 1.5 dropped that
setting) and add:

```yaml
      # --- Phase 2 archive: capture + verify run from day one; DELETION IS OFF
      # until the archive has been trusted for a week (see DESIGN.md §Archive).
      ARCHIVE_FOLDERS: ${EWS_ARCHIVE_FOLDERS:-inbox,sent}
      ARCHIVE_AFTER_DAYS: ${EWS_ARCHIVE_AFTER_DAYS:-180}
      ARCHIVE_EXCLUDE_CATEGORIES: ${EWS_ARCHIVE_EXCLUDE_CATEGORIES:-}
      ARCHIVE_GRACE_DAYS: ${EWS_ARCHIVE_GRACE_DAYS:-7}
      ARCHIVE_DELETE_ENABLED: ${EWS_ARCHIVE_DELETE_ENABLED:-false}
      ARCHIVE_MAX_DELETE_PER_RUN: ${EWS_ARCHIVE_MAX_DELETE_PER_RUN:-200}
      ARCHIVE_MIN_FREE_GB: ${EWS_ARCHIVE_MIN_FREE_GB:-2}
      ARCHIVE_CYCLE_SECONDS: ${EWS_ARCHIVE_CYCLE_SECONDS:-300}
      # Semantic search. The MCP container deliberately does NOT get this key.
      GEMINI_API_KEY: ${GEMINI_API_KEY:?GEMINI_API_KEY must be set in .env}
      EMBED_DIMS: "768"
```

Leave `services.ews-mcp.environment` alone — no `GEMINI_API_KEY`, no
`ARCHIVE_*` there.

- [ ] **Step 4: Rebuild and restart**

```bash
cd /home/askar/stack/compose
EWS_MCP_TAG=<short-sha> docker compose -f personal.yml build ewsd
EWS_MCP_TAG=<short-sha> docker compose -f personal.yml up -d ewsd ews-mcp
docker compose -f personal.yml logs -f ewsd | head -60
```

Expected in the log: `applying migration 003_archive.sql`, then
`archive runner started (every 300s, delete_enabled=False)` once Exchange
warms up.

- [ ] **Step 5: Verify from outside**

```bash
curl -s -H "Authorization: Bearer $EWSD_API_KEY" \
  -X POST http://127.0.0.1:8790/v1/tools/archive_status -d '{}' \
  -H 'Content-Type: application/json' | python3 -m json.tool
```

Expected: `"delete_enabled": false`, `"semantic_enabled": true`, `states` with
a growing `captured`/`verified` count, `embedding.backlog` shrinking, and
`runner.cycles` increasing between calls. Then confirm the pgvector column is
live:

```bash
docker exec postgres-ews psql -U ews -d ews -c \
  "SELECT COUNT(*) FROM ews.chunks WHERE embedding IS NOT NULL"
```

- [ ] **Step 6: Commit the stack change**

```bash
cd /home/askar/stack
git add compose/personal.yml
git commit -m "$(cat <<'EOF'
ewsd: enable the Phase 2 archive (capture+verify+embed); deletion stays off

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01B4TssxWRa9m4hMLpVFyndx
EOF
)"
```

> **Phase 3 (not part of this plan):** after a week of using search on
> archived mail, flip `EWS_ARCHIVE_DELETE_ENABLED=true` in `/home/askar/stack/.env`
> and watch the first capped run through `archive_status`.

---

## Self-Review

**1. Spec coverage**

| spec requirement | task |
|---|---|
| §2 `attachments` table (name, content_type, size, sha256, is_inline, `name_tsv`) | 1 |
| §2 `chunks` table (message, seq, source, text, `vector(768)`, HNSW cosine) | 1 |
| §2 `archive_runs` ledger | 1, 4 |
| §2 blobs at `{DATA_DIR}/blobs/<sha[:2]>/<sha>`, MIME at `{DATA_DIR}/mime/<sha>.eml` | 3 |
| §2 migration numbered + version in `meta` (`SCHEMA_VERSION = 3`) | 1 |
| §3 capturer (policy, one fetch, batches of 25, skip one failure) | 7 |
| §3 verifier (changekey + hash + blob size, reset to live with a reason) | 8 |
| §3 deleter (grace, `ARCHIVE_DELETE_ENABLED`, per-run cap, audit) | 8 |
| §3 policy settings | 2, 7 |
| §3 sync interaction (deleted ignored, captured/verified kept, live dropped) | 10 |
| §3 embedder (all rows, 1500-char chunks, 768 dims, batches of 100, backoff) | 5, 6, 9 |
| §4 `search_messages` gains `archived`; cards carry `archive_state` | 13 |
| §4 `get_attachment` from the blob store for archived mail | 13 |
| §4 `list_folders` archived count | 13 |
| §4 `find_similar` always registered | 12 |
| §4 `mode="semantic"` hybrid RRF | 6, 13 |
| §4 `archive_run` / `archive_status` / `get_raw_message` | 12 |
| §4 daemon routes `/v1/archive/run`, `/v1/archive/runs/<id>`, `/download/<token>` | 11, 14 |
| §4 settings `GEMINI_API_KEY`, `EMBED_DIMS`, `ARCHIVE_*` | 2 |
| §5 Gemini down → keyword + `meta.degraded` | 6, 13 |
| §5 disk full → stop with a clear error; temp-then-rename | 3, 7 |
| §5 deletion rails, cap, audit record, confirm token | 8, 12 |
| §5 tests: capture→verify, hash mismatch resets, grace + cap, sync keeps verified | 7, 8, 10 |
| §5 contract tests cover the three new tools | 12 |
| §5 `scripts/live_smoke.py` archive dry run | 16 — Phase 1.5 deleted `live_smoke.py`, so this moved to `boot_smoke.py` (a cold `archive_run(dry_run=true)` that never deletes) |

Gaps: none found. Deliberately out of scope per the spec: attachment text extraction/embedding, archiving calendar/contacts/tasks, migrating 4.5 SQLite data.

**2. Placeholder scan**

No "TBD", no "add error handling", no "similar to Task N", no test step without executable test code. Every code step is a complete block. The one prose-only step is Task 17 Step 3 (a YAML environment edit shown in full).

**3. Type consistency**

- `CacheStore` methods are declared once in Task 4/6 interface blocks and used with the same names and keyword-only signatures afterwards: `mark_captured(ews_id, *, mime_sha256, mime_path)`, `deletable_rows(*, before_ts, verified_before, limit)`, `apply_server_deletes(ids) -> (dropped, tombstoned)`, `similar_message_ids(embedding, *, limit, archived, exclude_ews_id)`.
- `ArchivePolicy` exposes `capture_cutoff_ts`, `delete_cutoff_ts`, `grace_instant_ts`, `folder_ids(store)`, `as_dict()` — all three cutoff names are used exactly as defined by the deleter (Task 8) and the runner (Task 9).
- Worker return keys are fixed: capturer `{candidates, captured, failed, sample, stopped}`, verifier `{verified, reset, failed, reasons}`, deleter `{eligible, deleted, failed, blocked, sample}`, embed `{embedded, backlog, error}` — the runner reads exactly these.
- `SemanticIndex.hybrid_search` returns `(rows, degraded)` in Task 6 and is unpacked that way in Task 13.
- `Embedder.embed(texts) -> list[list[float]]` is the only method any consumer calls; `FakeEmbedder` (conftest) and `GeminiEmbedder` both implement it.
- `downloads.mint(data_dir, *, path, name, content_type, ttl_seconds)` / `redeem(data_dir, token) -> {"path","name","content_type"}` match between Task 11's module, its route, and Task 12's `get_raw_message`.
- Tool counts are stated identically in the Global Constraints, Task 12's tests, `boot_smoke.py` (Task 16) and the CHANGELOG: **18 / 29 / 35**.

Fixes applied during review: `SemanticIndex.index_messages` originally batched by chunk, which would let `replace_chunks` delete the first half of a message that straddled two API batches — it now batches by message and the `EmbeddingError` import was added to `semantic.py`.
