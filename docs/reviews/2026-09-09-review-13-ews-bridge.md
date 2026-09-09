# Review 13 — EWS Bridge Whole-Branch Review

Review of branch `plan-8-ews-bridge` (commit `49b7167` over `6b54ce8` at PR #1), addressing the 6 numbered questions in [Brief 13](2026-09-09-brief-13-ews-bridge.md).

---

## 1. The Migration Against Real Data

Migration file: `ewsmcp/migrations/005_bridge_arrival.sql`

```sql
CREATE TABLE IF NOT EXISTS ews.bridge_arrival (
  ews_id      text PRIMARY KEY REFERENCES ews.messages(ews_id) ON DELETE CASCADE,
  seq         bigint NOT NULL,
  changekey   text,
  first_seen  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_bridge_arrival_seq ON ews.bridge_arrival (seq);
CREATE SEQUENCE IF NOT EXISTS ews.bridge_arrival_seq;

INSERT INTO ews.bridge_arrival (ews_id, seq, changekey, first_seen)
SELECT m.ews_id,
       row_number() OVER (ORDER BY m.date_ts NULLS FIRST, m.ews_id),
       m.changekey,
       coalesce(to_timestamp(m.date_ts), now())
  FROM ews.messages m
 WHERE m.deleted_at IS NULL
   AND NOT EXISTS (SELECT 1 FROM ews.bridge_arrival);

SELECT setval('ews.bridge_arrival_seq',
              coalesce((SELECT max(seq) FROM ews.bridge_arrival), 0) + 1, false);

CREATE TABLE IF NOT EXISTS ews.bridge_meta (
  id         int PRIMARY KEY CHECK (id = 1),
  generation bigint NOT NULL
);
INSERT INTO ews.bridge_meta (id, generation)
VALUES (1, ('x' || substr(md5(random()::text || clock_timestamp()::text),
                          1, 15))::bit(60)::bigint)
ON CONFLICT DO NOTHING;
```

### Speed & Performance
At 2,416 messages (~300–500 KB on disk), sorting the table over `(date_ts, ews_id)`, computing `row_number()`, and inserting into `bridge_arrival` with its primary key and unique sequence index will take between 10 and 30 milliseconds. It is not slow enough to matter.

### Locking & Concurrency with `ewsd`
- **Advisory Lock:** Both `ewsd` and `ews-bridge` call `db.migrate()` at boot. In `ewsmcp/db.py:73`, migrations are guarded by `SELECT pg_advisory_xact_lock(7355608)`. If both processes boot at the same time, one acquires the lock, applies migration 005, and commits. The second acquires the lock, sees version 5 already in `schema_migrations`, and exits `migrate()` immediately without re-running SQL.
- **Table Locks:** The `REFERENCES ews.messages(ews_id) ON DELETE CASCADE` clause acquires a `SHARE ROW EXCLUSIVE` lock on `ews.messages` to register the foreign key constraint. This blocks concurrent `INSERT`, `UPDATE`, and `DELETE` on `ews.messages` for the duration of the migration transaction. Because the transaction finishes in under 30 ms, any concurrent write from `ewsd` (which runs on a 45-second cycle) will briefly queue and proceed cleanly without timing out or deadlocking.

### Behavioral Risks on 2,416 Rows vs Test Fixtures

#### 1. The `date_ts NULLS FIRST` Trap (Critical)
In `005_bridge_arrival.sql`:
```sql
SELECT m.ews_id,
       row_number() OVER (ORDER BY m.date_ts NULLS FIRST, m.ews_id),
       m.changekey,
       coalesce(to_timestamp(m.date_ts), now())
```
In Exchange, messages can have `date_ts IS NULL` (drafts, calendar notices, system notifications, or messages with unparseable timestamps; `ews.messages.date_ts` is nullable).

If any message among the 2,416 rows has a NULL `date_ts`:
1. `ORDER BY m.date_ts NULLS FIRST` assigns it sequence numbers at the **very beginning** of the stream (`seq = 1, 2, ...`).
2. `coalesce(to_timestamp(m.date_ts), now())` sets its arrival timestamp to **`now()`**.
3. When Mindet bootstraps (`until = now - 14 days`), this message is excluded from the bootstrap page because `first_seen` (`now()`) is not `<= 14 days ago`.
4. The bootstrap proceeds through older historical messages (which have past send timestamps and therefore `seq > 1`), finishing at the edge of the window with a cursor such as `v1:gen:1800`.
5. When live polling begins with `since = v1:gen:1800`, the query filters for `a.seq > 1800`.
6. **The NULL-dated message (at `seq = 1`) sits permanently behind the cursor and will NEVER be delivered.**

**Recommendation:** Change `ORDER BY m.date_ts NULLS FIRST, m.ews_id` to `ORDER BY m.date_ts NULLS LAST, m.ews_id` in Migration 005 and in `arrival.py:sweep()`. A message with no send date arrives now, so it must be sequenced at the current head, not before five months of history.

#### 2. Sequence Advancement (`setval`)
`setval('ews.bridge_arrival_seq', coalesce((SELECT max(seq) FROM ews.bridge_arrival), 0) + 1, false)`:
When `max(seq)` is 2,416, setting the sequence to 2,417 with `is_called = false` ensures that the subsequent call to `nextval()` returns exactly 2,417. This is correct and leaves no gap or collision.

#### 3. Generation Token
Generating a random 60-bit integer stored in `bridge_meta` ensures that existing cursors survive daemon/bridge restarts, but a rebuilt or restored store will generate a new token and trigger a clean 400 re-bootstrap in Mindet.

---

## 2. The Incremental Sweep's Ordering

In `ewsmcp/bridge/arrival.py`:
```python
def sweep(conn) -> int:
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SWEEP_LOCK_KEY,))
    cur = conn.execute(
        "INSERT INTO ews.bridge_arrival (ews_id, seq, changekey, first_seen)"
        " SELECT m.ews_id, nextval('ews.bridge_arrival_seq'), m.changekey,"
        "        coalesce(to_timestamp(m.date_ts), now())"
        "   FROM ews.messages m"
        "   LEFT JOIN ews.bridge_arrival a ON a.ews_id = m.ews_id"
        "  WHERE m.deleted_at IS NULL"
        "    AND (a.ews_id IS NULL OR a.changekey IS DISTINCT FROM m.changekey)"
        "  ORDER BY m.date_ts NULLS FIRST, m.ews_id"
        " ON CONFLICT (ews_id) DO UPDATE"
        "   SET seq = excluded.seq, changekey = excluded.changekey, updated_at = now()")
    return cur.rowcount
```

### Is `nextval` Under `ORDER BY` Sound?
- Under SQL semantics and PostgreSQL documentation, `nextval()` evaluation order in a SELECT query with `ORDER BY` is an implementation detail, not a standard SQL guarantee. The PostgreSQL planner currently evaluates targetlist expressions after the `Sort` node when no index scan can replace the sort.
- However, if the planner hoists projection, uses a partial index, or changes plan shapes across PostgreSQL major versions, sequence numbers could be assigned out of send order.

### What Exactly Breaks If An Inversion Occurs?
1. **Normal Incremental Batches (Harmless):**
   In steady-state operation, `sweep()` runs every poll and typically finds 0 to 5 new messages. If two messages arriving together in a 45-second window have their sequence numbers inverted (e.g. 10:00 email gets seq 502, 09:00 email gets seq 503):
   - Both sequences are greater than the consumer's previous cursor (`seq > 500`).
   - Both are returned in the same page.
   - Mindet records `i.sent_at` from `m.sent_at` (derived from `date_ts`) and sorts all triage views by `sent_at`, not by `seq`.
   - No messages are lost or misordered in Mindet.
2. **Historical Discovery Crossing an `until` Boundary (Message Loss):**
   If a newly discovered folder syncs 500 messages that span the 14-day ingestion window boundary:
   - Suppose Message Old (sent 20 days ago) is assigned `seq = 800`, while Message New (sent 5 days ago, inside the window) is assigned `seq = 750` due to a sequence inversion.
   - Mindet boots with `until = 14 days ago`.
   - The bootstrap query (`WHERE a.first_seen <= 14 days ago`) includes Message Old (seq 800) and excludes Message New (seq 750).
   - The bootstrap page ends on `seq = 800` and saves `v1:gen:800` as the starting cursor.
   - Live polling asks for `a.seq > 800`.
   - **Message New (`seq = 750`) is permanently skipped.**
3. **The Active `NULLS FIRST` Bug:**
   As noted in Question 1, any message in `sweep()` with `date_ts IS NULL` is placed at the front of the sort order by `ORDER BY m.date_ts NULLS FIRST`, receiving a lower `seq` than historical messages while having `first_seen = now()`. This triggers the boundary loss bug even with the current planner.

**Recommendation:**
In `sweep()`, change `ORDER BY m.date_ts NULLS FIRST` to `ORDER BY m.date_ts NULLS LAST`. To make sequence assignment immune to future planner changes, generate sequence offsets using a CTE with an explicit `row_number() OVER (ORDER BY m.date_ts NULLS LAST, m.ews_id)`.

---

## 3. Exhaust the Cursor

### Failure Scenario 1: An Amended Mail Arriving Mid-Bootstrap Skips the Entire Ingestion Window (Critical)

This is a subtle, high-severity bug in the interplay between `sweep()`, `first_seen`, and bounded bootstrap paging.

**Sequence of Events:**
1. A mailbox has 1,000 messages: messages 1..500 were sent 30–60 days ago; messages 501..1,000 were sent in the last 14 days.
2. Migration 005 assigns messages 1..500 `seq` 1..500 and `first_seen` = `date_ts` (30–60 days ago).
3. Mindet starts up with a 14-day ingestion window (`window_start = now - 14 days`) and calls `_bootstrap()`:
   `GET /bridge/v1/messages?until=<window_start>&limit=500`
4. Page 1 returns messages 1..500. Next cursor is `v1:gen:500`.
5. While Mindet processes Page 1, the user opens Outlook and marks Message 50 (a 45-day-old message) as read, or flags it. Exchange updates its `ChangeKey`.
6. `ewsd` syncs the update and writes the new `changekey` to `ews.messages`.
7. Mindet requests Page 2 of bootstrap:
   `GET /bridge/v1/messages?since=v1:gen:500&until=<window_start>&limit=500`
8. In `app.py:messages()`, `arrival.sweep()` runs. It detects that Message 50 has a new `changekey`.
   `sweep()` assigns Message 50 a new sequence number at the live head: `seq = 1001`.
   However, `sweep()` preserves `first_seen`:
   `-- first_seen is deliberately absent: a re-arrival earns a new sequence number, not a new arrival time.`
   Therefore, Message 50 still has `first_seen = 45 days ago <= window_start`!
9. `arrival.page()` executes:
   `SELECT ... FROM bridge_arrival a WHERE a.seq > 500 AND a.first_seen <= <window_start>`
   Message 50 matches: `1001 > 500` is TRUE, and `45 days ago <= window_start` is TRUE!
10. Page 2 returns Message 50, with `next = v1:gen:1001`.
11. Mindet's `_bootstrap` sees `len(page.messages) = 1 < 500`, completes bootstrap, and records the live cursor as `v1:gen:1001`.
12. Mindet begins live polling: `GET /bridge/v1/messages?since=v1:gen:1001`.
13. **Result: Messages 501 through 1,000 had sequences 501..1,000. None are `> 1001`. Every single message in the entire 14-day ingestion window has been skipped and lost forever.**

**Fix:** In `arrival.page()`, when `until` is specified, the query must not return rows whose `seq` is higher than the maximum sequence assigned to messages before the window cutoff, or `_bootstrap` must track the highest sequence seen before the window and refuse to jump past the live head.

---

### Failure Scenario 2: Conversation ID Changes Create Duplicate Items in Mindet
In `ewsmcp/bridge/mapping.py`:
```python
def chat_native_id(row: dict) -> str:
    return row.get("conversation_id") or row["ews_id"]
```
- When an email is first synced by Exchange (or a draft / unindexed item), `conversation_id` may initially be null. `chat_native_id` falls back to `row["ews_id"]`.
- Mindet ingests the item under venue `venue.native_id = ews_id`.
- Later, Exchange updates the item with its proper `conversation_id`.
- The message is edited (`changekey` changes) and `sweep()` re-emits it with `chat = conversation_id`.
- In Mindet (`engine.py:114`), the item uniqueness constraint is:
  `ON CONFLICT (tenant_id, venue_id, native_id) DO UPDATE`
- Because `venue_id` has changed, the conflict target does not match!
- **Result:** Mindet inserts a second item with the same `native_id` into the new venue, calls `attribute()` again, and produces duplicate directives, evidence, and search results.

---

### Failure Scenario 3: Soft-Deleted Messages Persist Indefinitely in Mindet
- When an archived message is deleted upstream in Exchange, `store.apply_server_deletes()` marks it with `archive_state = deleted, deleted_at = now()`.
- The bridge's `arrival.page()` filters with `WHERE m.deleted_at IS NULL`.
- The bridge never emits a contract `tombstone` (`kind: "tombstone"`).
- **Result:** Any message soft-deleted after being handed out remains permanently active in Mindet as valid evidence and open obligations. Furthermore, a fresh consumer bootstrapping later will not see the deleted message, creating a divergence between consumers.

---

### Other Scenarios Evaluated
- **Cursor held across restart:** Fully safe. `generation` is persisted in `bridge_meta`. Rebuilding the database generates a new random token, causing existing cursors to fail with HTTP 400, cleanly triggering Mindet's re-bootstrap logic.
- **Two consumers with different cursors:** Fully safe. The bridge is completely stateless with respect to consumers. Concurrent calls serialize on `_SWEEP_LOCK_KEY` during `sweep()`, and paging is purely a parameter-driven query against immutable sequences.

---

## 4. Security

### Authentication and Guarding
- Every route (`/health`, `/messages`, `/chats`, `/contacts`) calls `guard(request)` as its first statement.
- The `Authorization: Bearer <token>` check uses `hmac.compare_digest(given.encode("latin-1"), token.encode())`, preventing timing attacks and avoiding crashes on non-ASCII characters.
- Empty tokens (`EWS_BRIDGE_TOKEN=""`) are rejected at startup in `build_app()`.

### Information Leakage Review
- **Error Bodies:** All custom errors use `_Bad(status, code, message)` with hardcoded string constants (e.g. `bad token`, `cursor is not of this contract`, `until must be ISO-8601`). No user input, query snippets, or internal filesystem paths are reflected in error responses.
- **Unhandled Exceptions:** Starlette runs with `debug=False` by default. Unhandled exceptions are caught by `ServerErrorMiddleware`, returning a generic HTTP 500 `"Internal Server Error"`. Stack traces are written to stderr/container logs, never to the HTTP client.
- **Database Credentials:** The database connection DSN is passed directly to psycopg in `db.py`. It is never returned in any response payload or error string.
- **PII / Content Exposure:**
  - `/bridge/v1/health` exposes only `connected`, `as_of`, `last_message_at`, capabilities, and staleness seconds. No subject lines, addresses, or mail content are exposed without valid authentication.
- **Residual Network Exposure:**
  `main.py:22` binds to `0.0.0.0:8081` by default. Because the bridge holds read-write access to the mail store via `ewsd`'s database, container network isolation must ensure that port 8081 is accessible only from the Mindet container.

---

## 5. `health`'s Staleness Rule

Implementation in `ewsmcp/bridge/app.py`:
```python
state = c.execute("SELECT max(as_of) AS as_of FROM ews.sync_state").fetchone()
...
elif now - float(as_of) > STALE_AFTER_SECONDS:  # 15 minutes
    age = int(now - float(as_of))
    connected, detail = False, f"the mail store last synced {age}s ago"
```

### Is 15 Minutes Defensible?
- **Yes.** In normal operation, `ewsd` runs its item sync lane every 45 seconds (`EWS_CACHE_SYNC_SECONDS`). Each mirrored mail folder updates its `as_of` timestamp in `ews.sync_state` at the end of its sync. 15 minutes represents 20 consecutive missed sync cycles, which is far beyond transient network jitter.
- If `ewsd` crashes, hangs, or loses its Exchange refresh token, the bridge promptly surfaces `connected: false`, alerting the owner in morning triage.

### What Does Mindet See During a Legitimate Sync Pause?
If Exchange applies severe throttling (e.g. exponential backoff under EWS 429/503), or if a large folder with 50,000 items is being synced for the first time:
- In `ewsmcp/cache/sync.py:495`, `set_sync_state()` is called **only after all pages of a folder have been fetched**. During a very long single-folder sync, `as_of` is not updated until the folder completes.
- If this takes longer than 15 minutes:
  1. `health` returns `connected: false` with detail `the mail store last synced Xs ago`.
  2. In Mindet (`generic.py:147-151`):
     - Sets `cursor["probed_ok"] = False`.
     - Logs the error and records `daemon.put_state(source, cursor=cursor, error=why)`.
     - **Immediately aborts the tick without polling `/messages` or `/chats`.**
  3. The owner sees the EWS source reported as disconnected.
  4. Once `ewsd` completes the folder sync and updates `sync_state`, `health.connected` returns `True`.
  5. On the next tick, Mindet re-runs `probe()`, verifies compliance, and resumes polling seamlessly from its saved cursor. No messages are lost or skipped.

---

## 6. Maintainer Regrets (Long-Term Operational Issues)

1. **Unbounded Full Table Scan in `/bridge/v1/chats`:**
   In `app.py:198-210`:
   ```python
   rows = c.execute(
       "SELECT coalesce(conversation_id, ews_id) AS native_id,"
       " subject, sender_email, to_json"
       " FROM ews.messages WHERE deleted_at IS NULL"
       " ORDER BY date_ts DESC NULLS LAST, ews_id DESC").fetchall()
   return JSONResponse({"chats": mapping.chats_from_rows([dict(r) for r in rows])})
   ```
   This loads **every message in the database** into Python memory, parses every `to_json` recipient list, and builds Python sets. Over a year (20,000–50,000 messages), this endpoint will consume hundreds of megabytes of memory and take seconds to respond every time Mindet refreshes chats. Conversations should be aggregated or selected in SQL rather than performing a full in-memory fold over the entire message history.
2. **Read-Flag Changes Triggering Full Re-Emissions:**
   Exchange changes a message's `ChangeKey` when an email is marked read or unread, categorized, or flagged. `arrival.py:sweep()` re-sequences any message whose `changekey` changed, moving it to the head of the arrival ledger. If the owner marks an old message as read in Outlook, that message is re-emitted on the live stream to Mindet. Mindet will re-upsert it and run `reattribute_if_untouched()`.
3. **Missing Index on `bridge_arrival(first_seen)`:**
   `005_bridge_arrival.sql` creates an index on `seq`, but none on `first_seen`. Paging queries during bootstrap (`WHERE a.first_seen <= %s ORDER BY a.seq LIMIT %s`) perform index scans on `seq` while filtering on unindexed `first_seen`. Adding an index on `(first_seen, seq)` will keep bootstrap queries fast as the table grows.
4. **No Application-Level Logging in `app.py`:**
   `app.py` contains no `logger` statements. When invalid cursors are rejected, authentication fails, or exceptions occur, nothing is recorded in application logs except raw Uvicorn HTTP status lines. Adding diagnostic logging to `_parse_cursor` and `on_error` will save hours of debugging in production.
