-- NOTE FOR ANYONE DEPLOYING THIS: a store that ran an early build of migration
-- 005 — the one that ordered `date_ts NULLS FIRST` — holds a sequence that no
-- migration can repair, because the numbers were handed out in the wrong order
-- and every consumer cursor already refers to them. Such a store must be
-- re-bootstrapped instead: drop `ews.bridge_meta` so the generation changes,
-- and the next poll is refused with 400 and the consumer starts again. No live
-- store is in that state (the owner's is at schema version 4), so this is a
-- note rather than a procedure.

-- Every bounded page a bootstrap asks for filters on `first_seen` and orders
-- by `seq`: `WHERE a.first_seen <= $1 ORDER BY a.seq LIMIT $2`. Migration 005
-- indexed `seq` alone, so that filter was a heap lookup per candidate row —
-- fine at 2,400 mails, and a scan the owner would eventually feel on a store
-- that has grown a year of mail. The pair covers the filter and hands back the
-- rows already in sequence order.
CREATE INDEX IF NOT EXISTS ix_bridge_arrival_first_seen
  ON ews.bridge_arrival (first_seen, seq);

-- `first_seen` answers "when did this version of the mail arrive": it moves
-- every time the mail is amended, because that is what orders the stream and
-- what `until` bounds. `first_arrival` answers a different question — "when did
-- we first meet this mail at all" — and is written once and never again.
--
-- They were one column, and conflating them made a timestamp wander. A mail
-- with no parseable send date reports `sent_at` from the ledger, since there is
-- nothing else to report; reading the column that moves meant an undated draft
-- appeared to have been sent a little later every time the owner flagged it in
-- Outlook. It reads `first_arrival` now, which stands still.
ALTER TABLE ews.bridge_arrival
  ADD COLUMN IF NOT EXISTS first_arrival timestamptz NOT NULL DEFAULT now();
-- Rows that predate the column took the default, now(), while their `first_seen`
-- carries the arrival 005 reconstructed from the send date. `first_arrival` can
-- never legitimately be later than `first_seen`, so this both backfills at
-- add-time and is a no-op on any re-application.
UPDATE ews.bridge_arrival SET first_arrival = first_seen
 WHERE first_arrival > first_seen;

-- Which chat a mail belongs to, pinned at first sight and never moved.
--
-- Exchange does not always know a mail's conversation when it first writes it —
-- a draft, or an item not yet indexed — and fills it in later. Recomputing
-- `coalesce(conversation_id, ews_id)` on every read meant the same mail was
-- handed to the consumer under one chat and then, after an amendment, under
-- another. The consumer keys an item on (venue, native_id), so the second
-- delivery misses the first and inserts a *second* item: one mail, two
-- directives in the owner's morning list. Noise he has to dismiss is worse
-- than the silence this ledger exists to replace — it teaches him to skim.
--
-- The honest consequence: a mail that arrived before Exchange knew its
-- conversation stays in a chat of its own for good, separate from the thread it
-- belongs to. That is the better trade. A stable wrong grouping is something
-- the owner can fix by hand; a moving one is not.
ALTER TABLE ews.bridge_arrival ADD COLUMN IF NOT EXISTS chat_id text;
UPDATE ews.bridge_arrival a
   SET chat_id = coalesce(m.conversation_id, m.ews_id)
  FROM ews.messages m
 WHERE m.ews_id = a.ews_id AND a.chat_id IS NULL;
