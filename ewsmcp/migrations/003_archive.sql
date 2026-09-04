-- ews schema v3: the archive tables. Rows for archived mail are never
-- dropped from ews.messages, so these children hang off ews_id and cascade
-- only when a LIVE row is removed by the sync engine.
--
-- WITH SCHEMA public is load-bearing, same trap as unaccent in migration 002:
-- production connects as role `ews` against a database that also has an
-- `ews` schema, so the default search_path's `"$user"` element resolves and
-- a bare `CREATE EXTENSION vector` lands in schema `ews` — after which every
-- `vector(768)` / `<=>` reference below fails with "type does not exist".
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

-- changekey observed by the capture fetch; the verifier compares it, sync
-- never overwrites it.
ALTER TABLE ews.messages ADD COLUMN captured_changekey text;

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
    embedding      public.vector(768)
);
CREATE UNIQUE INDEX ux_chunks_msg_seq ON ews.chunks (message_ews_id, source, seq);
CREATE INDEX ix_chunks_embedding ON ews.chunks
    USING hnsw (embedding public.vector_cosine_ops);

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
