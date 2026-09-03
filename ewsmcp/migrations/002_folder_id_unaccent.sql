-- ews schema v2 (Phase 1.5): folder identity is the folder's EWS id, full-text
-- search is generated straight from the message columns through an immutable
-- unaccent wrapper, and learned signatures are gone.
--
-- The mirror is rebuildable and nothing has been archived yet (archive_state is
-- 'live' everywhere), so the cheapest correct migration is to empty the mirror
-- and let ewsd's sync engine refill it from scratch. sync_state goes with it:
-- its keys change from item:<well-known key> to item:<folder ews id>.
TRUNCATE ews.messages, ews.sync_state;

DROP TABLE IF EXISTS ews.sender_sigs;

-- search_tsv is generated from norm_text; drop it first (this also drops
-- ix_msg_tsv), then the shadow column it read.
ALTER TABLE ews.messages DROP COLUMN search_tsv;
ALTER TABLE ews.messages DROP COLUMN norm_text;

-- RENAME COLUMN rewrites ix_msg_folder_date's definition automatically.
ALTER TABLE ews.messages RENAME COLUMN folder TO folder_id;

-- unaccent's own unaccent(text) is only STABLE (it resolves the default
-- dictionary at run time), so it cannot appear in a generated column. Naming
-- the dictionary explicitly makes the call deterministic, and this wrapper
-- declares that fact to the planner.
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE FUNCTION ews.immutable_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$SELECT public.unaccent('public.unaccent', $1)$$;

ALTER TABLE ews.messages ADD COLUMN search_tsv tsvector
    GENERATED ALWAYS AS (
        to_tsvector('simple', ews.immutable_unaccent(lower(
            coalesce(subject, '') || ' ' ||
            coalesce(sender_name, '') || ' ' ||
            coalesce(sender_email, '') || ' ' ||
            coalesce(body_clean, ''))))
    ) STORED;

CREATE INDEX ix_msg_tsv ON ews.messages USING GIN (search_tsv);
