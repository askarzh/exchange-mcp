# Design — ews-mcp v5 (release line 5.1.x)

The architecture the code enforces. Module docstrings cite the sections
below (§Tools, §Safety, §Ids, §DTOs, §Errors, §Transports, §Audit, §Store,
§Processes).

## The law

1. **MCP = data plane only.** Fast, deterministic Exchange access plus
   safety gates. Judgment — summaries, briefings, prioritization,
   commitments, voice — belongs to the CALLING assistant. The server never
   makes an LLM call and never ships a "judgment tool". Tool count stays
   lean and is generated into the docs, never hand-counted.
2. **Storage = Postgres in core.** One `ews` schema holds the mirror,
   aliases and the archive (§Archive). Full-text search is a generated,
   stored `tsvector` over subject + sender + cleaned body, indexed GIN and
   queried with Postgres' `simple` text-search config (no stemming, no
   stopwords). Accent folding happens in the database, on both the index
   and the query side, through `ews.immutable_unaccent()` — an IMMUTABLE
   wrapper over the `unaccent` extension's dictionary.
3. **Reads are cache-first with provenance** (`source`, `as_of`,
   `fresh:true` escape hatch); **writes go straight to EWS** and then
   write-through to the mirror.
4. **Safety gates live ONLY in `ewsd`** (`tools/base.py`'s dispatcher,
   used exclusively by the daemon process): kill-switch, tier, circuit,
   recipient guard, two-phase confirm and the send rate cap all run there.
   `ewsmcp`'s dispatcher (`mcp/dispatch.py`) does alias resolution for its
   local reads and otherwise forwards verbatim — it applies no gates of
   its own beyond filtering its tool registry down to the configured tier
   (an above-tier tool is simply absent, not refused). Handlers declare
   `side_effect_class` and `confirm`; they contain no policy.
5. **Never pin `auth_type`.** Only exchangelib auto-negotiation works
   against the target Exchange (verified live). All exchangelib imports
   are module-top; every kwarg-bearing call has a signature pin.
6. **No footprints.** No personal names, real addresses, employer
   identifiers, personal skill names, mailbox content, or tokens in any
   tracked file, comment, commit message, or doc. Fixtures use
   example.com and neutral wording.

## §Processes — ewsd + ewsmcp

Two processes share one Postgres database.

- **`ewsd`, the daemon.** One instance, always on. Owns the only Exchange
  session (gateway, connection manager), the `SyncEngine` that keeps the
  mirror warm, capability-URL uploads, the hash-chained audit log, and the
  entire gate chain (§Safety). Serves HTTP on `EWSD_HOST:EWSD_PORT`
  (default `127.0.0.1:8790`): `GET /v1/tools`, `POST /v1/tools/<name>`,
  `GET /v1/status`, `/metrics`, `/openapi.json` are behind `EWSD_API_KEY`;
  `GET /livez`, `/readyz`, `/health`, `/version` are always public; `PUT|POST
  /upload/<token>` and `GET /download/<token>` are deliberately ahead of the
  bearer gate too — the unguessable single-use token IS the credential. Also
  owns the archive pipeline (§Archive) and holds the only `GEMINI_API_KEY`
  in the stack.
- **`ewsmcp`, the thin MCP.** Any number of instances. Reads Postgres
  directly for `list_folders`, `search_messages(mode="keyword")`,
  `get_message`, `get_thread`, `get_mailbox_overview`, `list_tasks`,
  `waiting_on`, `archive_status` and `get_server_status`. Every other tool
  — including `find_similar` and `search_messages(mode="semantic")`, since
  `ewsmcp` never holds `GEMINI_API_KEY` — and any read called with
  `fresh=true`, is forwarded to `ewsd` verbatim (`confirm_token` included)
  over `EWSD_URL`. Serves stdio or Streamable HTTP `/mcp` plus `/livez`,
  `/readyz`, `/health`, `/version`, behind `MCP_API_KEY` in HTTP mode.
  Does not import exchangelib.
- **Why the gate chain lives in one process.** Kill-switch, tier,
  recipient guard, confirm tokens and the send rate cap all depend on
  state that must be process-local and singular to mean anything: the
  in-memory rate-cap window, the HMAC confirm-token secret and its
  single-use bookkeeping, and the audit chain's hash head all need exactly
  one writer. Running the chain in `ewsd` — the one process that also
  owns the Exchange session — means there is one rate window, one set of
  live confirm tokens, and one unbroken audit chain, no matter how many
  `ewsmcp` instances are talking to it.

## §Tools — the surface

35 tools in the default (`full`-tier) registry; 29 register at `draft`
tier and 18 at `read` tier (see the generated table in `docs/API.md`).
Five packs:

- **mail-read** (6): `list_folders`, `search_messages`, `get_message`,
  `get_thread`, `get_attachment`, `get_mailbox_overview`.
- **calendar / people / status** (7): `list_events`, `get_event`,
  `check_availability`, `find_people`, `get_contact`, `get_oof_settings`,
  `get_server_status`.
- **tasks / waiting-on** (3): `list_tasks`, `update_task`, `waiting_on`.
- **writes** (15): draft lifecycle (`create_draft`, `update_draft`,
  `delete_draft`, `send_draft`), attachments (`create_upload_link`,
  `add_attachment`, `delete_attachment`), bulk ops (`update_messages`,
  `move_messages`, `delete_messages`), calendar writes (`create_event`,
  `update_event`, `respond_to_event`, `cancel_event`), `set_oof`.
- **archive / semantic** (4): `archive_status`, `get_raw_message` and
  `find_similar` are `read`-class and register at every tier;
  `archive_run` is `destructive`-class (min tier `full`) and
  `dry_run=false` is two-phase confirmed like `send_draft`.

`search_messages` gained an `archived` argument (`any` | `only` |
`exclude`, default `any`) and a working `mode="semantic"` — both are §Archive
work now, not a reserved placeholder. `mode="semantic"` and `find_similar`
are forwarded from `ewsmcp` to `ewsd` unconditionally (the MCP process
never holds `GEMINI_API_KEY`) and degrade to keyword ranking with
`meta.degraded=true`/`meta.reason` set when no embedder is configured, so a
mailbox search never goes dark because a remote API is unavailable or
rate-limiting. Every card carries `archive_state` — but ONLY when it is not
`live`, to keep the common case (all-live results) at its old token cost.

Every list-shaped result ships exactly the canonical envelope
`{items, count, total_available, next_offset}` (contract-tested).

## §Safety — one gate chain, one process

Dispatch order (policy precedes connectivity; nothing irreversible
without two model decisions):

    kill-switch → tier → circuit → cold gate → recipient guard →
    two-phase confirm → send rate cap → alias resolution → handler → audit

This entire chain runs inside `ewsd` and only `ewsd` — every write call
(`create_draft`, `send_draft`, `delete_messages`, …) that `ewsmcp`
receives is forwarded to `ewsd`'s `POST /v1/tools/<name>` with the
arguments (and `confirm_token`, when present) passed through unchanged;
`ewsmcp` itself makes no policy decision beyond which tools it exposes at
its configured tier. `ewsmcp` never mints or verifies a confirm token —
`send_draft`'s phase-1 preview and phase-2 verification both happen in
`ewsd`.

- **Kill-switch** `SEND_ENABLED=false` (default) refuses every send-class
  call before anything else.
- **Tiers** `EWS_CAPABILITY_TIER=read|draft|full` (default `draft`)
  remove above-tier tools from the registry AND refuse them at dispatch.
- **Recipient guard** (allow/denylist globs) fires on every tool whose
  arguments carry recipients (drafts, events) and on the draft's RESOLVED
  recipients inside `send_draft`'s confirm gate.
- **Two-phase confirm**: phase 1 returns a preview + HMAC token; phase 2
  must echo it. For `send_draft` the token binds the draft's CONTENT
  (subject + sorted recipients + full body, refetched and re-verified at
  phase 2), so editing the draft between preview and confirm kills the
  token. Tokens are single-use; idempotent replays (same
  `idempotency_key` + draft) return the cached receipt without a fresh
  token — that is what makes retry-after-timeout safe (Stripe semantics).
- **Send rate cap** `EWS_MAX_SENDS_PER_HOUR`.
- The one documented handler-side check: `create_event`/`update_event`
  are write-class for tier purposes, but invitations leave the org, so
  they re-check the kill-switch when (and only when) they would notify.

## §Ids — aliases only

The model never sees a raw EWS id: outputs carry short aliases (`m12`,
`e3`, `d1`, `t4`, `p2`, `k1`, `f7`), inputs accept aliases or raw ids.
The Postgres-backed aliaser (`ews.aliases`) survives restarts, rebinds on
moves, and keeps `internet_message_id` as a secondary key. Stale alias →
clean re-search hint, never an upstream error. Page-sized mints batch
into one transaction. See `CHANGELOG.md` for the alias re-mint note on
upgrade from a prior release line.

## §DTOs — token economy

`MsgCard` (~60 tokens): id, from, subject, date, 200-char snippet, flags.
`MsgFull`: card + recipients + CLEANED body (quoted-history +
signature stripping) + attachment inventory. Raw HTML only on explicit
`include_html=true`. The measured pathology this kills: one legacy detail
call shipped 115,457 chars for a ~150-char message.

## §Store — the mirror (Postgres, schema `ews`)

- `db.py` / `migrations/`: one Postgres database, schema `ews`, numbered
  SQL migrations applied by `ewsd` at startup and version-checked by
  `ewsmcp` (refuses to start against an older schema than it expects).
  `messages`, `events`, `tasks`, `folders`, `sync_state`, `aliases` —
  the durable mirror both processes read. Migration 003 (`SCHEMA_VERSION
  = 3`) adds the archive: `attachments` (one row per captured attachment,
  content-hashed, name search over a generated `tsvector`), `chunks`
  (`embedding vector(768)`, HNSW index, `vector_cosine_ops` — see
  §Archive/§Semantic below) and `archive_runs` (one row per pass, the
  history behind `archive_status`), plus `messages.captured_changekey`
  (the changekey observed at capture time, checked again before delete).
  `CREATE EXTENSION vector` is pinned `WITH SCHEMA public` — the
  production role and schema are both named `ews`, so a bare `CREATE
  EXTENSION` would otherwise land the type in schema `ews` via
  `"$user"` and break every later `vector(768)`/`<=>` reference.
- Full-text search: `search_tsv` is generated as
  `to_tsvector('simple', ews.immutable_unaccent(lower(subject || sender ||
  body_clean)))` with a GIN index (`ix_msg_tsv`). `search_messages` builds
  the prefix expression (`tok:* & tok2:*`) in Python and unaccents it in
  SQL, so index and query always agree.
- **The whole mailbox is mirrored.** `ewsd`'s `SyncEngine` runs resumable
  `SyncFolderItems` deltas (`EWS_CACHE_SYNC_SECONDS`, 45) for every mail
  folder except the well-known ones `EWS_MIRROR_EXCLUDE` names
  (drafts, junk, trash, outbox) — exclusion is by well-known key only, so
  sub-folders of an excluded folder are still mirrored — with one token
  per folder keyed `item:<folder ews id>`. A folder is "mail" when its EWS
  `folder_class` is `IPF.Note`; Contacts/Calendar/Tasks folders and their
  children (Recipient Cache, GAL Contacts, Sharing, Quick Step Settings, …)
  are listed by `list_folders` but never item-synced.
- **Hierarchy refresh cadence.** exchangelib caches the whole folder tree on
  the account root for the life of the process, so the hierarchy lane clears
  that cache and re-walks it — discovering new folders, dropping vanished
  ones, refreshing `total`/`unread` — every `EWS_CACHE_HIERARCHY_SECONDS`
  (600) and on the first cycle, not on every cycle; the item lane keeps
  running at the 45s cadence in between, and the walk runs before item sync
  so a newly discovered folder is mirrored in the same cycle. `list_folders`
  stamps its `as_of` from that lane's `folders` watermark. A slower lane on
  the same interval refreshes the calendar window and the tasks folder. A
  folder that disappears loses its token and its `live` rows; archived rows
  stay. Failures degrade — `ewsmcp` reads fall back to `ewsd`'s live route
  (`fresh=true`), the server never gates on the mirror.
- Provenance contract: every read is stamped `source: cache|live` (+
  `as_of` for cache); `fresh=true` is forwarded by `ewsmcp` to `ewsd`
  verbatim, bypassing the mirror.

## §Errors — a taxonomy, not tracebacks

`validation | auth_failed | tier_blocked | kill_switch |
recipient_blocked | confirm_invalid | not_found | throttled | rate_capped
| upstream_unavailable | upstream_error | daemon_unavailable |
backend_unavailable | internal` — each with an LLM-directed `hint` and
`retry_after_s` where meaningful. Handler `TypeError`/`ValueError` map to
`validation`, never 502.

## §Transports

`ewsmcp`: stdio (default) or Streamable HTTP `/mcp`, plus public health
`/livez`, `/readyz`, `/health`, `/version` (`MCP_API_KEY` guards `/mcp`
in HTTP mode). `ewsd`: HTTP only — `GET /v1/tools`,
`POST /v1/tools/<name>` (jsonschema-validated against the public tool
schema, 1 MiB body cap), `GET /v1/status`, `/metrics` (Prometheus) and
`/openapi.json` are behind `EWSD_API_KEY`; `GET /livez`, `/readyz`,
`/health`, `/version` are always public; `PUT|POST /upload/<token>` and
`GET /download/<token>` are deliberately ahead of the bearer gate — the
unguessable single-use token IS the credential. Both `/upload/<token>` and
`/download/<token>` are served ONLY by `ewsd` (port 8790 by default) and
ONLY out of `{DATA_DIR}/mime/` or `{DATA_DIR}/blobs/`: `create_upload_link`
and `get_raw_message` both build the capability URL from `EXTERNAL_URL`,
so any reverse proxy in front of the stack must route `/upload/*` and
`/download/*` through to `ewsd`, not to `ewsmcp`. A download link is
single-use — the first successful `GET` spends it, and every rejection
(expired, already used, never existed) renders as an identical opaque 404.
`POST /v1/archive/run` is a REST alias of `POST /v1/tools/archive_run`
(same two-phase confirm applies to `dry_run=false`); `GET
/v1/archive/runs/<id>` reads one `archive_runs` row.
**Never-exit boot** (both processes): tools/routes register and
transports bind before any Exchange contact; in `ewsd` a background
warmup loop owns connection recovery (exponential backoff + jitter,
protocol-cache eviction every 3 failures, heartbeat re-probe with a REAL
network round trip).

## §Audit

Hash-chained JSONL per tool call (no bodies; recipients/subject only for
send/destructive). The chain head persists across restarts
(`audit/chain.state`); `scripts/verify_audit_chain.py` re-derives every
link and catches edits, deletions and truncation.

## Structural guards (scar tissue, encoded)

- `test_exchangelib_signatures.py`: signature pins for every
  kwarg-bearing exchangelib call + behavior contracts for the three lies
  that caused the v5 criticals (string `conversation_id` raises; stored
  `total_count` is not a probe; the protocol cache must be evictable).
- AST sentinel: no exchangelib imports inside function bodies, no
  exemptions.
- Envelope contract test; north-star budget test (≤2 calls, <2k tokens);
  `test_docs_match_registry.py` fails CI when `docs/API.md` drifts from
  the registry.

## §Archive — capture, verify, delete

The mailbox is near quota and the weight is attachment bytes, so `ewsd`
moves old mail onto local disk and then removes it from Exchange. Three
idempotent workers, each driven by `messages.archive_state`, run every
`ARCHIVE_CYCLE_SECONDS` (300) in an asyncio task started after warm-up:

- **Capture** takes `live` rows matching the policy (folder in
  `ARCHIVE_FOLDERS`, older than `ARCHIVE_AFTER_DAYS`, category not in
  `ARCHIVE_EXCLUDE_CATEGORIES`), fetches `mime_content` plus every
  attachment in ONE `Account.fetch`, writes `{DATA_DIR}/mime/<sha256>.eml`
  and `{DATA_DIR}/blobs/<sha[:2]>/<sha>`, fills `ews.attachments`, and sets
  `captured`. Batches of 25. Free space is checked against
  `ARCHIVE_MIN_FREE_GB` before each batch; writes are temp-then-rename, so
  a crash never leaves wrong bytes under a content-addressed name. An
  EMPTY `ARCHIVE_FOLDERS` is fail-closed: it archives nothing, never every
  folder.
- **Verify** re-fetches each `captured` row (`changekey`, `attachments`),
  re-hashes the MIME file from disk and checks every blob's existence and
  size. All pass → `verified`. Any failure → back to `live`, capture
  retries.
- **Delete** hard-deletes `verified` rows through THREE independent
  rails — `ARCHIVE_DELETE_ENABLED=true`, capability tier `full` (plus a
  confirm token on `archive_run(dry_run=false)`), and verified older than
  the cutoff plus `ARCHIVE_GRACE_DAYS` (floored at 1 day, so a
  misconfigured 0 can never make freshly-verified mail immediately
  deletable) — capped at `ARCHIVE_MAX_DELETE_PER_RUN` per pass. The
  `captured_changekey` snapshot taken at capture time is verified twice —
  once by the verifier, and again immediately before each item's delete
  call — so mail that changed after verification is skipped, not deleted
  on stale trust. One audit record per deletion carries `ews_id`,
  `internet_message_id`, `mime_sha256` and the run id; deletes that
  succeeded against Exchange but could not be recorded afterward (a DB
  hiccup right after the hard-delete) surface in `deleted_unrecorded`
  rather than vanishing — the deleter's result always satisfies
  `deleted + failed + remaining == eligible`. Off by default.

`ArchiveRunner.run_once` and the background cycle share one lock, so a
manual `archive_run` call and a scheduled pass never run concurrently
against the same mailbox; a caller that can't get the lock within a few
seconds gets back `blocked` instead of hanging. `POST /v1/archive/run` is
a REST alias of `POST /v1/tools/archive_run` — the same two-phase confirm
applies to `dry_run=false`.

Rows are never dropped for archived mail: `ews_id` stays the stable key,
so search, `get_message` and `get_thread` read archived mail exactly like
live mail. `search_messages` takes an `archived` argument
(`any`|`only`|`exclude`, default `any`); a card carries `archive_state`
only when it is not `live`. When Exchange reports a delete for a
`captured`/`verified` row (our own deleter, or a hand-delete in Outlook)
the SyncEngine keeps the row and marks it `deleted`; only `live` rows are
dropped. `get_attachment` serves archived mail straight from the blob
store, without contacting Exchange. `get_raw_message` mints a single-use
`GET /download/<token>` capability URL for the original RFC822 bytes —
served from disk for captured/verified/deleted mail, fetched fresh
through Exchange (and cached, without changing archive state) for live
mail — because a .eml is exactly the kind of payload that must not
travel through the model's context. Every pass writes an
`ews.archive_runs` row — the human-readable history behind
`archive_status` and `GET /v1/archive/runs/<id>`. `archive_status` itself
splits cleanly by process: the Postgres-only body (state counts, recent
runs, embedding backlog, policy, `delete_enabled`) is computed by whichever
process answers the call, so it works from `ewsmcp` with no filesystem
access; blob-store size and free disk are ewsd-only figures, since only
`ewsd` owns `DATA_DIR`.

Calendar, contacts, tasks, drafts and the outbox are never archived.

## §Semantic — embeddings and hybrid search

`ewsd` embeds `subject + body_clean` in 1,500-character chunks with
Gemini's `gemini-embedding-2` at 768 dimensions (plain HTTPS via
`httpx` — no vendor SDK), stores them in `ews.chunks.embedding
vector(768)` behind an HNSW index with `vector_cosine_ops`, and stamps
`messages.embedded_at`. Backlog is drained every cycle in batches of 100
with exponential backoff on 429/5xx.

`search_messages(mode="semantic")` fuses the tsvector ranking and the
vector ranking with Reciprocal Rank Fusion (k=60); `find_similar(id|text)`
is pure vector search. Both cover live and archived mail. If the embedder
or the vector query fails, the answer degrades to keyword results with
`meta.degraded=true`/`meta.reason` set — a mailbox search never goes dark
because a remote API is rate-limiting us. The MCP process deliberately
holds no `GEMINI_API_KEY`: `find_similar` and `mode="semantic"` are
forwarded to `ewsd` unconditionally, whether or not an embedder is
actually configured there.
