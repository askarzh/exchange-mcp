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

-- Spec §3.2: on a bridge's first migration the arrival time of pre-existing
-- rows is backfilled from their send time. Without it the whole store — years
-- of mail — would be stamped as having arrived at the moment of the first
-- poll, and a consumer bootstrapping to the edge of a fourteen-day window
-- would find nothing before that edge, take a cursor at the live head, and
-- never be offered a single mail again.
--
-- The sequence is assigned here explicitly, by row_number() over send time,
-- rather than left to nextval() inside an INSERT ... ORDER BY: nextval is
-- evaluated wherever the planner happens to put it, so the ORDER BY of an
-- arbitrary plan is a hint, not a guarantee. It has to be a guarantee, because
-- the consumer's bootstrap keeps the last sequence a bounded page returned,
-- and anything in the window sitting below that number is lost for good.
--
-- Guarded on an empty ledger so this is a first-migration backfill and nothing
-- else; once rows exist, sweep() owns every later arrival.
INSERT INTO ews.bridge_arrival (ews_id, seq, changekey, first_seen)
SELECT m.ews_id,
       row_number() OVER (ORDER BY m.date_ts NULLS FIRST, m.ews_id),
       m.changekey,
       coalesce(to_timestamp(m.date_ts), now())
  FROM ews.messages m
 WHERE m.deleted_at IS NULL
   AND NOT EXISTS (SELECT 1 FROM ews.bridge_arrival);

-- Whatever the backfill consumed, nextval() must continue past it.
SELECT setval('ews.bridge_arrival_seq',
              coalesce((SELECT max(seq) FROM ews.bridge_arrival), 0) + 1, false);

-- The generation. Rebuilding the ledger — a restore, a resync, a truncate —
-- must invalidate every cursor Mindet holds rather than let it resume in the
-- middle of a renumbered stream, so the generation is bumped and old cursors
-- are refused with 400.
--
-- It is drawn at random when this table is first created rather than fixed at
-- 1, which is the whole point: a rebuilt volume comes back with seq restarting
-- at 1, and a constant generation would make Mindet's stored cursor
-- `v1:1:2417` *valid* against a ledger that has 40 rows in it. Every poll
-- would return an empty page, the 400-and-re-bootstrap path would never fire,
-- and mail would stop for ever with no error anywhere. A random generation is
-- stable across restarts of this store (the row survives) and differs for a
-- store that was rebuilt.
CREATE TABLE IF NOT EXISTS ews.bridge_meta (
  id         int PRIMARY KEY CHECK (id = 1),
  generation bigint NOT NULL
);
INSERT INTO ews.bridge_meta (id, generation)
VALUES (1, ('x' || substr(md5(random()::text || clock_timestamp()::text),
                          1, 15))::bit(60)::bigint)
ON CONFLICT DO NOTHING;
