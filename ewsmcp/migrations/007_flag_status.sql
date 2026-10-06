-- Outlook follow-up flag (MAPI PidTagFlagStatus, tag 0x1090), mirrored so the
-- store-only search_messages can filter on it. 2 = flagged and still open,
-- 1 = marked complete; NULL = not flagged (Exchange reports a cleared flag as
-- 0, and sync stores that as NULL too). Rows mirrored before this column
-- existed get it from SyncEngine's one-time flag backfill, since nothing
-- changed upstream for the delta to carry it.
ALTER TABLE ews.messages ADD COLUMN IF NOT EXISTS flag_status smallint;
