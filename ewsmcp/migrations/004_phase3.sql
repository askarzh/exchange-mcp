-- ews schema v4 (Phase 3): item class + attachment inventory in the mirror,
-- boilerplate detector tables, and a one-time re-queue of short replies so
-- their chunk 0 gains thread context (see the Phase 3 design, §3).
ALTER TABLE ews.messages ADD COLUMN IF NOT EXISTS item_class text;
ALTER TABLE ews.messages ADD COLUMN IF NOT EXISTS attachments_json text;

CREATE TABLE IF NOT EXISTS ews.boilerplate_refs (
    id          bigserial PRIMARY KEY,
    label       text NOT NULL UNIQUE,
    text        text NOT NULL,
    embedding   public.vector(768) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ews.boilerplate_hits (
    id              bigserial PRIMARY KEY,
    message_ews_id  text NOT NULL,
    detector        text NOT NULL CHECK (detector IN ('embedding', 'llm')),
    paragraph       text NOT NULL,
    similarity      real,
    ref_label       text,
    dropped         smallint NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_hits_created ON ews.boilerplate_hits (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_hits_message ON ews.boilerplate_hits (message_ews_id);

CREATE INDEX IF NOT EXISTS ix_msg_conv_date ON ews.messages (conversation_id, date_ts);

-- A later task adds a GC lane to the archive runner; widen the kind check
-- now so archive_runs rows of kind='gc' can be inserted once that lands.
ALTER TABLE ews.archive_runs DROP CONSTRAINT IF EXISTS archive_runs_kind_check;
ALTER TABLE ews.archive_runs ADD CONSTRAINT archive_runs_kind_check
    CHECK (kind IN ('capture', 'verify', 'delete', 'embed', 'gc', 'all'));

-- Short replies get thread context in chunk 0 from this version on; re-queue
-- them once so the existing index catches up. ~900 rows on the owner's
-- mailbox = 4-5 embed cycles.
UPDATE ews.messages SET embedded_at = NULL
    WHERE length(coalesce(body_clean, '')) < 600 AND conversation_id IS NOT NULL;
