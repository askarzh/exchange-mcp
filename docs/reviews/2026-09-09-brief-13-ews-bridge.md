# Review brief 13 — the EWS bridge, whole branch, before it runs on real mail

## What this is

`exchange-mcp` already syncs Exchange into `postgres-ews`. This branch adds a thin
adapter over that store speaking `/bridge/v1`, the contract Mindet (a personal ledger
of obligations) uses for every source. Its own process, own port, own token, beside
`ewsd` rather than inside it. Read-only in effect: it declares `messages`, `chats` and
`contacts`, and deliberately not `send`, `login` or `media`.

PR https://github.com/askarzh/exchange-mcp/pull/1, branch `plan-8-ews-bridge`, 17
commits over `main` at `6b54ce8`. Read it with
`git -C /home/askar/src/exchange-mcp/.worktrees/plan-8-ews-bridge diff main...HEAD`.
The contract is `/home/askar/src/mindet/docs/superpowers/specs/2026-09-07-platform-design.md`
§3, §4 and §12; what Mindet actually parses with is
`/home/askar/src/mindet/mindet/contract/{models,client,probe,fake}.py`.

The one hard part: `ews.messages` records when a mail was *sent*, and the contract needs
the order the store *learned* of it, because a folder sync discovers three-week-old mail
today. Migration `005` adds an arrival ledger and every cursor walks that.

## What has already been found, so do not spend the review re-finding it

Nine task-level reviews and one whole-branch review ran on this. The whole-branch review
found that, read as one piece, the branch **delivered zero mail forever with no error**,
from two independent causes: arrival time was never backfilled from send time (the spec
requires it), and a bounded page jumped its cursor to the live head (a misreading of the
spec, since `until` returns old history rather than excluding it). Both fixed, with a
round-trip test proven to fail on each independently. Also already fixed: a hardcoded
cursor generation that would silently accept a restored store forever; `/contacts`
minting keys the message path refuses; a `/chats` cap that could drop a message while
advancing the cursor past it; `health` claiming connected while the store was frozen; a
500 from the auth guard on a non-ASCII header; a sequence assignment safe only because
one uvicorn worker serialises requests; a page bounded by send time rather than arrival;
`recipients()` expecting objects where the column holds bare address strings; keys minted
from Exchange legacy distinguished names; a null send date becoming 1 Jan 1970; the
owner's own mail never marked as his; an empty token making `compare_digest` true against
a request with no header; and `max(to_json)` choosing a thread's membership
alphabetically.

## What I want from you

Assume the reviews above were competent and look where they did not.

1. **The migration, against real data.** `005` will run against the owner's live store —
   2,416 messages, five months, currently at schema version 4. It backfills `first_seen`
   from `date_ts` and assigns `seq` by `row_number()` over `(date_ts NULLS FIRST,
   ews_id)`, then `setval`s the sequence. Is there any way that is slow enough to matter,
   locks something `ewsd` needs while it runs, or behaves differently at 2,416 rows than
   in tests? `ewsd` and the bridge share the image and both call `migrate()` at boot.
2. **The incremental sweep's ordering.** Pre-existing rows get an explicit `row_number()`.
   Later batches take `nextval` under an `ORDER BY`, which is a planner hint rather than a
   guarantee. A prior reviewer ran `EXPLAIN` and found the projection above the sort, with
   zero inversions over 3,000 shuffled rows. Do you agree that is sound, and if a future
   planner hoisted it, what exactly breaks and would anything notice?
3. **Exhaust the cursor.** Find a sequence of legitimate consumer calls that loses a
   message or delivers one twice. Consider a cursor held across a restart, an amended mail
   arriving mid-bootstrap, a message soft-deleted after being handed out, a conversation
   id that changes, and two consumers with different cursors.
4. **Security.** The bridge is reachable by another container and holds `ewsd`'s
   read-write credential for the mail store. Auth, error bodies, logs, anything a caller
   influences. Can any path leak a DSN, an internal path, a subject line or an address?
5. **`health`'s staleness rule.** It reads `max(as_of)` from `ews.sync_state` and reports
   `connected: false` past fifteen minutes. Is that threshold defensible against how
   `ewsd` actually writes that table, and what does a consumer see during a long
   legitimate sync pause?
6. Anything a person who has to keep this running for a year would regret.

House rule: no real names, addresses, message content or tokens in anything you write to
a file. Roles, not names.
