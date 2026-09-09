-- Every bounded page a bootstrap asks for filters on `first_seen` and orders
-- by `seq`: `WHERE a.first_seen <= $1 ORDER BY a.seq LIMIT $2`. Migration 005
-- indexed `seq` alone, so that filter was a heap lookup per candidate row —
-- fine at 2,400 mails, and a scan the owner would eventually feel on a store
-- that has grown a year of mail. The pair covers the filter and hands back the
-- rows already in sequence order.
--
-- A separate migration rather than an edit to 005, because 005 may already
-- have been applied.
CREATE INDEX IF NOT EXISTS ix_bridge_arrival_first_seen
  ON ews.bridge_arrival (first_seen, seq);
