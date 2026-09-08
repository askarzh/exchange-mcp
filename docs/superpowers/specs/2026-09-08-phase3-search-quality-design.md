# Phase 3: search quality, mirror completeness, archive hygiene

Date: 2026-09-08
Status: draft for owner review (addendum to `2026-09-03-postgres-archive-daemon-design.md`)

## Why

Phase 2 put the archive and semantic search into production on 2026-09-04.
Verification over the following days (three full tool-checklist passes against
the live mailbox) showed the pipeline works and surfaced what limits it:

- Trailing legal disclaimers survive cleaning (73 messages, the BCC Invest
  footer in RU/KZ/EN) and create false similarity between unrelated mail.
- One-line replies embed as almost nothing: "RE: Прогноз - инвестиции в ДО"
  with two numeric lines scores 0.697 on a topical query, below unrelated
  background at 0.698, while its parent scores 0.747.
- Calendar auto-responses ("Accepted: …", "Declined: …") rank alongside real
  mail in subject search and are 372 of 2,375 rows.
- `get_message` from the mirror cannot list attachments; it points the model
  at a live call.
- Four hygiene items were parked during Phase 2 review: orphan blobs after a
  verification reset, no per-item size cap, no audit line for a delete a rail
  skipped, and a 4-connection pool shared by every lane.

Decisions below were confirmed by the owner on 2026-09-08.

## Decisions

| Area | Decision |
|---|---|
| Scope | Everything parked, one plan, ordered so search quality ships first. |
| Thread context | Chunk 0 of a short reply carries the parent's subject and first 300 characters. No thread-level vectors. |
| Boilerplate guardrail | Two detectors run log-only side by side — embedding similarity (§2) and an LLM boundary pass (§2b) — with `ARCHIVE_BOILERPLATE_DROP=off`; after two weeks the owner picks the winner and the loser is removed. |
| Third-party cleaners | `mail-parser-reply` is evaluated in a spike (§0) against the mirror before any dependency is added; `talon`, `unstructured`, DOM-level HTML cleaning are rejected (see Out of scope). |
| Delete rails | Unchanged (on, 120 days, 1-day grace). Phase 3 adds the audit line for skipped deletes and nothing else. |

## 0. Spike: `mail-parser-reply` against the mirror

Before §1 is written, a throwaway script runs `mail-parser-reply` (pure
Python, maintained) over every `body_clean` in the mirror and diffs its output
against ours: paragraphs it removes that we keep, and vice versa, grouped by
language. Output is a short report in the plan's SDD workspace. If it removes
real boilerplate we miss in more than a handful of cases, its rules for those
cases are ported into `bodyclean` (as regexes with golden tests); the library
itself is not added as a dependency unless its Russian/Kazakh coverage turns
out to exist and beat ours. Budget: half a day.

## 1. Trailing disclaimers (deterministic)

`bodyclean.clean_body` gains a tail cut after quoted-history and header-line
stripping and before signature stripping:

- Split the text into paragraphs (blank-line separated).
- Consider only the **tail window**: the last 8 paragraphs, and only those
  whose start lies in the last 40 % of the text by character offset. A
  message shorter than 400 characters has no tail window.
- The first tail paragraph matching a **disclaimer anchor** cuts that
  paragraph and everything after it.
- Never cut the first non-empty paragraph of the message.

Anchors (case-insensitive substrings, one regex alternation in
`_DISCLAIMER_ANCHOR_RE`):

- RU: `не является предложением`, `предназначено только для получател`,
  `является конфиденциальн`, `если вы не являетесь адресатом`,
  `получили это сообщение по ошибке`
- KZ: `тек хабарламада көрсетілген алушыларға`, `құпия ақпарат`
- EN: `intended solely for`, `intended only for the`, `confidentiality notice`,
  `if you are not the intended recipient`, `received this (e-?mail|message) in error`,
  `privileged and confidential`

`clean_body` reports `disclaimer_cut: bool` in its result dict. Existing rows
are repaired with `scripts/backfill_bodies.py --all` (bodies that change are
re-queued for embedding by `update_bodies`, bodies that do not are untouched).

## 2. Semantic boilerplate guardrail

A safety net for gateway text the anchors do not know. It runs inside
`SemanticIndex.index_messages`, before `chunk_text`, on the daemon only.

**Reference vectors.** New table `ews.boilerplate_refs(id, label, text,
embedding vector(768), created_at)`. Seeded by `scripts/seed_boilerplate.py`
from a small text file of real footers and banners taken from this mailbox
(RU, KZ, EN variants; 6–10 rows). The seed script embeds through the same
`Embedder` and is idempotent on `label`. `SemanticIndex` loads the refs once
per process and re-reads when the table's `max(created_at)` changes.

**Check.** For each message, take the paragraphs of the tail window (same
definition as §1), embed them in the same API batch as the chunks, and compute
cosine similarity against every ref. A paragraph with
`similarity >= EMBED_BOILERPLATE_THRESHOLD` (default 0.80) is a **hit**.

**Log.** Every hit is written to `ews.boilerplate_hits(id, message_ews_id,
detector, paragraph, similarity, ref_label, dropped, created_at)` with
`detector = 'embedding'`. `archive_status` reports
`boilerplate: {embedding: {hits_7d, dropped_7d}, llm: {hits_7d, dropped_7d,
errors_7d}, drop_detector, threshold}`.

**Drop.** Only when `ARCHIVE_BOILERPLATE_DROP` names this detector
(`embedding`): the hit paragraph and everything after it are removed from the
text that is chunked (the stored `body_clean` is NOT modified — the guardrail
affects the index only, so a threshold mistake is reversible by re-embedding).
The first non-empty paragraph of a message is never dropped.

**Cost.** Tail paragraphs add roughly 3–6 short texts per message to the
embedding batch; at gemini-embedding-2 pricing this is negligible.

### 2b. LLM boundary detector (second detector, same harness)

Runs alongside the embedding check on the same tail paragraphs, so the two can
be compared on real data before either is allowed to drop anything.

**Model.** Gemini Flash-Lite (`GEMINI_CLEAN_MODEL`, default
`gemini-2.5-flash-lite`) through the same API key as the embedder, from the
daemon only. `temperature = 0`, JSON response schema.

**Prompt.** The model receives the tail paragraphs numbered `0..n-1` plus the
first paragraph of the message for context, and must answer
`{"drop_from": <index or null>, "reason": <short string>}` — the index of the
first paragraph that begins signature/disclaimer/gateway boilerplate. It never
returns text. The daemon keeps the original bytes; the model only points.

**Validation.** An answer is accepted only if `drop_from` is a valid index into
the tail window and does not remove the first non-empty paragraph of the
message. Anything else (invalid index, malformed JSON, HTTP error, timeout >
10 s) is logged as `errors_7d` and treated as "no hit". One call per message,
no retries beyond the embedder's existing 429 backoff.

**Log.** Hits are written to the same `ews.boilerplate_hits` table with
`detector = 'llm'`, `similarity = NULL`, `ref_label = <reason>`.

**Drop.** Only when `ARCHIVE_BOILERPLATE_DROP=llm`. The flag is an enum:
`off` (default) | `embedding` | `llm` | `both` (drop when either detector hits).

**Tests.** Unit tests use a fake model with scripted answers (hit, miss,
invalid index, garbage JSON, timeout). One recorded-fixture test runs the real
model on six stored emails and asserts the drop set; it is marked
`live_llm` and skipped without the API key.

**Cost.** Roughly 1,000 input tokens per message: about 2.5 M tokens for the
whole mailbox once, then a few thousand per day.

### 2c. Choosing the winner

After two weeks of log-only running, `scripts/boilerplate_report.py` prints,
per detector: hits, hits on messages the other detector missed, and a sample
of 20 paragraphs each for eyeballing. The owner sets
`ARCHIVE_BOILERPLATE_DROP` to the winner (or `both`), re-queues embeddings for
messages with hits, and the losing detector is removed in a follow-up commit.

## 3. Thread context for short replies

`chunk_text(subject, body, chunk_chars)` becomes
`chunk_text(subject, body, chunk_chars, context=None)`. When `context` is
given, chunk 0 is:

```
<subject>
<body>

In reply to: <parent subject>
<parent body_clean, first 300 characters>
```

Later chunks are unchanged. `context` is supplied only when **all** hold:

- `len(body_clean) < 600`
- the message has a `conversation_id`
- a parent exists: the latest message in the same conversation with
  `date_ts < own date_ts` (any archive state, any folder)

The store gains `parent_in_thread(ews_id) -> row | None`. Chunk rows keep
`source='body'`; the stored `text` includes the context so it is visible when
debugging a ranking. Existing short replies are re-queued once by the
migration (§6) — `UPDATE ews.messages SET embedded_at = NULL WHERE
length(body_clean) < 600 AND conversation_id IS NOT NULL` — which on this
mailbox is roughly 900 rows, four to five embed cycles.

## 4. Item class in the mirror

Migration 004 adds `messages.item_class text`. `HYDRATE_FIELDS` gains
`item_class` (GetItem returns it; SyncFolderItems does not); `row_from_message`
stores it; the backfill fills it for existing rows.

`is_calendar_item(item_class)` is true for `IPM.Schedule.Meeting.*` (requests,
responses, cancellations) and `IPM.Appointment`. Behaviour:

- `search_messages` gains `include_calendar_items: bool = false`. When false,
  keyword and semantic candidates exclude calendar items; `total_available`
  reflects the exclusion.
- `find_similar` excludes them always (a meeting acceptance is never "similar
  mail" in the sense the tool promises).
- `get_thread` and `get_message` are unchanged: an accepted invitation is part
  of its thread.
- The archive policy is unchanged: calendar items in Inbox/Sent are archived
  and deleted like any other message (they are small, and the mirror keeps
  them).

## 5. Attachment inventory in the mirror

Migration 004 adds `messages.attachments_json text` (JSON list of
`{name, size, content_type, inline}`; `[]` when none). `HYDRATE_FIELDS` gains
`attachments`. GetItem returns attachment metadata without content; the
hydrator reads `name`, `size`, `content_type`, `is_inline` from each
`FileAttachment` and MUST NOT touch `.content` (a lazy GetAttachment call).
`ItemAttachment` entries are recorded as `{name, size: null, content_type:
"message/rfc822", inline: false}`.

`get_message` from the mirror returns `attachments` from this column and drops
`attachments_hint`. `get_attachment` on live mail keeps its live fetch (bytes
are never in the mirror). The archive's `ews.attachments` table stays the
source for archived mail; capture does not read `attachments_json`.

## 6. Migration 004

```
ALTER TABLE ews.messages ADD COLUMN item_class text;
ALTER TABLE ews.messages ADD COLUMN attachments_json text;
CREATE TABLE ews.boilerplate_refs (…);          -- §2
CREATE TABLE ews.boilerplate_hits (…);          -- §2
CREATE INDEX ix_hits_created ON ews.boilerplate_hits (created_at DESC);
CREATE INDEX ix_msg_conv_date ON ews.messages (conversation_id, date_ts);
UPDATE ews.messages SET embedded_at = NULL
    WHERE length(body_clean) < 600 AND conversation_id IS NOT NULL;  -- §3
```

`SCHEMA_VERSION = 4`. No data is dropped; the migration is safe to apply
before the new image starts (the old code ignores the new columns).

## 7. Archive hygiene

**Orphan blob GC.** New `GcWorker` lane, runs once per
`ARCHIVE_GC_INTERVAL_HOURS` (default 168). It lists files under
`{DATA_DIR}/mime` and `{DATA_DIR}/blobs`, subtracts every path referenced by
`messages.mime_path` and every sha256 in `ews.attachments`, and removes
unreferenced files older than 24 hours (a capture in flight is never older
than that). Counts go to `archive_runs` as `kind='gc'` and to
`archive_status.gc: {last_run, removed_files, removed_bytes}`. `archive_run`
accepts `kind='gc'` with `dry_run`.

**Per-item size cap.** `ARCHIVE_MAX_ITEM_MB` (default 50). The capturer reads
`item.size` from the candidate query's fetch and skips larger items with
`failed` reason `too_large`; they stay `live` and are listed in the run's
`sample`. The status counts them under `states.skipped_too_large`.

**Audit line for skipped deletes.** For every candidate the deleter skips —
changekey changed, disk re-check failed, rail blocked mid-batch — one audit
record `archive_delete_skipped {ews_id, reason}` is appended in the same
persist step that records successful deletes. The audit chain then explains
every gap between `eligible` and `deleted`.

**Pool sizing.** `Database(max_size=…)` is driven by a new setting
`DB_POOL_MAX` (default 8). The daemon logs at startup the number of concurrent
consumers it will run (`ews_max_concurrency` + archive lanes + HTTP) and warns
if it exceeds the pool. The MCP process keeps `max_size=4`.

## 8. Configuration

| Setting | Default | Section |
|---|---|---|
| `EMBED_BOILERPLATE_THRESHOLD` | `0.80` | §2 |
| `ARCHIVE_BOILERPLATE_DROP` | `off` (`off` \| `embedding` \| `llm` \| `both`) | §2, §2b |
| `GEMINI_CLEAN_MODEL` | `gemini-2.5-flash-lite` | §2b |
| `ARCHIVE_BOILERPLATE_LLM` | `true` (run the LLM detector at all) | §2b |
| `ARCHIVE_GC_INTERVAL_HOURS` | `168` | §7 |
| `ARCHIVE_MAX_ITEM_MB` | `50` | §7 |
| `DB_POOL_MAX` | `8` | §7 |

## 9. Rollout

1. Deploy the image; migration 004 applies on boot and re-queues short replies.
2. `docker exec -i ewsd python - < scripts/seed_boilerplate.py` (refs).
3. `docker exec -i ewsd python - --all < scripts/backfill_bodies.py`: fills
   `item_class` and `attachments_json` on every row, applies the disclaimer
   cut, re-queues bodies that changed.
4. After two weeks, run `scripts/boilerplate_report.py` (§2c), set
   `ARCHIVE_BOILERPLATE_DROP` to the winning detector, re-queue embeddings for
   messages with hits, and remove the losing detector.

## 10. Testing

- bodyclean: golden tests for the tail cut in RU/KZ/EN, the 40 % window, the
  first-paragraph protection, and a mid-body sentence containing an anchor
  that must NOT cut.
- semantic: FakeEmbedder returns scripted vectors so a paragraph hits or
  misses deterministically; assert hits are logged, dropped only under the
  flag, and the first paragraph survives; assert chunk 0 carries thread
  context only for short replies with a parent.
- llm detector: a fake model scripted per test (hit, miss, invalid index,
  malformed JSON, timeout); assert only validated answers become hits, errors
  are counted not raised, and the drop honours the enum flag; one
  `live_llm`-marked fixture test against the real model, skipped without a key.
- sync: hydration fills `item_class` and `attachments_json`; an
  `ItemAttachment` produces the documented row; `.content` is never accessed
  (a property that raises in the fake).
- tools: `search_messages` excludes calendar items by default and includes
  them with the flag; `find_similar` always excludes; `get_message` from the
  mirror lists attachments.
- archive: GC removes only unreferenced files older than 24 h and reports
  bytes; capture skips a too-large item with the right reason; a skipped
  delete produces an audit record that `verify_audit_chain.py` accepts.
- migration 004 applies on a database at version 3 with data and re-queues
  exactly the short replies.

## Out of scope

Thread-level vectors; DOM-level HTML cleaning (the mirror indexes Exchange's
plain-text rendering, and this gateway's banner is plain text there, not a
classed element); `talon` (unmaintained, Python 2-era scientific
dependencies) and `unstructured` (built for raw `.eml` ingestion, heavy
dependency tree); a local small model (no GPU in this deployment — the LLM
detector uses Gemini instead); any change to the delete rails or grace;
per-user ranking tuning.
