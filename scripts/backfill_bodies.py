"""One-off: fill empty mirror bodies with GetItem and queue them for re-embedding.

Until 2026-09-04 the sync engine took `text_body` from SyncFolderItems, which
Exchange never populates, so every mirrored row had an empty `body_clean`
(full-text search and embeddings ran on subject + sender only). The sync path
now fetches bodies; this script repairs the rows written before that fix.

Run inside the ewsd container (it needs the Exchange credentials and
DATABASE_URL of the daemon). The image ships only the `ewsmcp` package, so
pipe the script in:

    docker exec -i ewsd python - < scripts/backfill_bodies.py               # all rows
    docker exec -i ewsd python - --limit 200 < scripts/backfill_bodies.py
    docker exec -i ewsd python - --all < scripts/backfill_bodies.py   # re-clean all

Idempotent: it only touches rows whose body is still empty and that are not
in the `deleted` archive state. Rows Exchange no longer returns are left as
they are and reported. The archive's own verifier is unaffected: it compares
the MIME on disk, not `body_clean`.
"""

from __future__ import annotations

import argparse
import logging
import sys

from ewsmcp.bodyclean import clean_body
from ewsmcp.cache.store import CacheStore
from ewsmcp.cache.sync import (
    BODY_CLEAN_MAX,
    attachments_json,
    hydrate_bodies,
    recipients_json,
)
from ewsmcp.config import Settings
from ewsmcp.db import Database
from ewsmcp.gateway.client import EWSGateway

log = logging.getLogger("backfill_bodies")


class _Ref:
    """The (id, changekey) pair `Account.fetch` wants, with `text_body`,
    `to_recipients`, `item_class` and `attachments` slots for
    `hydrate_bodies` to fill."""

    __slots__ = ("id", "changekey", "text_body", "to_recipients", "item_class",
                "attachments")

    def __init__(self, ews_id: str, changekey: str | None):
        self.id, self.changekey = ews_id, changekey
        self.text_body, self.to_recipients = None, None
        self.item_class, self.attachments = None, None


_ALL_ROWS: list[dict] | None = None


def _all_rows(store: CacheStore) -> list[dict]:
    """Every row GetItem can still reach, newest first (fetched once)."""
    global _ALL_ROWS
    if _ALL_ROWS is None:
        with store.db.conn() as c:
            _ALL_ROWS = c.execute(
                "SELECT ews_id, changekey FROM ews.messages "
                "WHERE archive_state <> 'deleted' "
                "ORDER BY date_ts DESC NULLS LAST").fetchall()
    return _ALL_ROWS


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after this many rows (default: all)")
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--all", action="store_true",
                    help="re-fetch and re-clean EVERY row still on Exchange (e.g. after "
                         "a bodyclean change); rows whose cleaned body is unchanged keep "
                         "their embedding")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    settings = Settings()
    db = Database(settings.database_url)
    db.migrate()
    store = CacheStore(db)
    account = EWSGateway(settings).account

    done = filled = missing = 0
    seen: set[str] = set()
    while True:
        want = args.batch if args.limit is None else min(args.batch, args.limit - done)
        if want <= 0:
            break
        if args.all:
            rows = [r for r in _all_rows(store) if r["ews_id"] not in seen][:want]
        else:
            rows = [r for r in store.messages_missing_body(want + len(seen))
                    if r["ews_id"] not in seen][:want]
        if not rows:
            break
        refs = [_Ref(r["ews_id"], r["changekey"]) for r in rows]
        hydrate_bodies(account, refs)
        bodies: dict[str, str] = {}
        tos: dict[str, str] = {}
        extra: dict[str, dict] = {}
        for ref in refs:
            seen.add(ref.id)
            if ref.to_recipients is not None:
                tos[ref.id] = recipients_json(ref)
            extra[ref.id] = {
                "item_class": ref.item_class,
                "attachments_json": attachments_json(ref) if ref.attachments is not None
                else None,
            }
            if ref.text_body is None:
                # No text on Exchange (a body-less meeting response, say).
                # `update_bodies` still applies the recipients/item_class/
                # attachments it DID return, leaving body_clean untouched.
                missing += 1
                continue
            try:
                bodies[ref.id] = clean_body(ref.text_body, max_chars=BODY_CLEAN_MAX)["text"]
            except Exception:  # noqa: BLE001 - mirror the sync engine's fallback
                bodies[ref.id] = ref.text_body[:BODY_CLEAN_MAX]
        # A genuinely empty body (Exchange returned "" or whitespace) still
        # counts as fetched and is written as "": it keeps the row out of the
        # next pass (it is selected by body_clean = '') only for this run, via
        # `seen`; across runs it is simply re-fetched, which is cheap and
        # correct. An unchanged body keeps its embedding.
        filled += store.update_bodies(bodies, tos, extra)
        done += len(rows)
        log.info("backfilled %d/%d rows so far (%d with no text body on Exchange)",
                 filled, done, missing)
    log.info("done: %d rows fetched, %d bodies written, %d with no text body on Exchange; "
             "embed worker will re-chunk them (backlog=%d)",
             done, filled, missing, store.embedding_backlog())
    return 0


if __name__ == "__main__":
    sys.exit(main())
