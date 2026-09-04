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
from ewsmcp.cache.sync import BODY_CLEAN_MAX, hydrate_bodies, recipients_json
from ewsmcp.config import Settings
from ewsmcp.db import Database
from ewsmcp.gateway.client import EWSGateway

log = logging.getLogger("backfill_bodies")


class _Ref:
    """The (id, changekey) pair `Account.fetch` wants, with `text_body` and
    `to_recipients` slots for `hydrate_bodies` to fill."""

    __slots__ = ("id", "changekey", "text_body", "to_recipients")

    def __init__(self, ews_id: str, changekey: str | None):
        self.id, self.changekey = ews_id, changekey
        self.text_body, self.to_recipients = None, None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after this many rows (default: all)")
    ap.add_argument("--batch", type=int, default=200)
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
        rows = [r for r in store.messages_missing_body(want + len(seen))
                if r["ews_id"] not in seen][:want]
        if not rows:
            break
        refs = [_Ref(r["ews_id"], r["changekey"]) for r in rows]
        hydrate_bodies(account, refs)
        bodies: dict[str, str] = {}
        tos: dict[str, str] = {}
        for ref in refs:
            seen.add(ref.id)
            if ref.to_recipients is not None:
                tos[ref.id] = recipients_json(ref)
            if ref.text_body is None:
                missing += 1
                if ref.id in tos:
                    bodies[ref.id] = ""   # recipients-only repair; body stays empty
                continue
            try:
                bodies[ref.id] = clean_body(ref.text_body, max_chars=BODY_CLEAN_MAX)["text"]
            except Exception:  # noqa: BLE001 - mirror the sync engine's fallback
                bodies[ref.id] = ref.text_body[:BODY_CLEAN_MAX]
        # A genuinely empty body still counts as fetched: writing "" keeps the
        # row out of the next pass (it is selected by body_clean = '') only
        # for this run, via `seen`; across runs it is simply re-fetched, which
        # is cheap and correct. An unchanged body keeps its embedding.
        filled += store.update_bodies(bodies, tos)
        done += len(rows)
        log.info("backfilled %d/%d rows so far (%d with no text body on Exchange)",
                 filled, done, missing)
    log.info("done: %d rows fetched, %d bodies written, %d with no text body on Exchange; "
             "embed worker will re-chunk them (backlog=%d)",
             done, filled, missing, store.embedding_backlog())
    return 0


if __name__ == "__main__":
    sys.exit(main())
