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
-- WITH SCHEMA public is load-bearing. Without it the extension is created in
-- the first schema of the CALLER's search_path, and production connects as
-- role `ews` against a database that also has an `ews` schema — so the default
-- search_path's `"$user"` element resolves, unaccent lands in `ews`, and every
-- `public.unaccent(...)` reference below fails with "function does not exist".
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;

-- The body is fully qualified, but `unaccent('public.unaccent', $1)` still
-- casts its first argument to `regdictionary`, and regdictionary input is
-- resolved against the search_path of whatever session evaluates it (any
-- session that INSERTs, since search_tsv is a generated column). Pinning
-- search_path on the function makes that resolution independent of the caller.
CREATE FUNCTION ews.immutable_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path = pg_catalog, public AS
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
