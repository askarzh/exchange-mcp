-- ews.messages records when a mail was sent. The bridge contract needs the
-- order the store learned of them, because a folder sync discovers old mail
-- today and a cursor over the send date would step straight past it. This
-- table is that order, and it is the only state the bridge owns.
CREATE TABLE IF NOT EXISTS ews.bridge_arrival (
  ews_id      text PRIMARY KEY REFERENCES ews.messages(ews_id) ON DELETE CASCADE,
  seq         bigint NOT NULL,
  changekey   text,
  first_seen  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_bridge_arrival_seq ON ews.bridge_arrival (seq);
CREATE SEQUENCE IF NOT EXISTS ews.bridge_arrival_seq;

-- The generation. Rebuilding the ledger — a restore, a resync, a truncate —
-- must invalidate every cursor Mindet holds rather than let it resume in the
-- middle of a renumbered stream, so the generation is bumped and old cursors
-- are refused with 400.
CREATE TABLE IF NOT EXISTS ews.bridge_meta (
  id         int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  generation bigint NOT NULL DEFAULT 1
);
INSERT INTO ews.bridge_meta (id, generation) VALUES (1, 1) ON CONFLICT DO NOTHING;
