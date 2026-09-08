"""Read-only: what the two boilerplate detectors have been doing lately.

The image ships only the `ewsmcp` package, so pipe the script in:

    docker exec -i ewsd python - < scripts/boilerplate_report.py

Prints, over the last 14 days: hits/dropped/errors per detector, how many
messages one detector flagged and the other did not (the disagreement that
tells you whether a `drop` change is safe), and 20 random flagged paragraphs
per detector so a human can eyeball what would be cut. Pure SQL — nothing
here embeds, calls Gemini, or writes.
"""

from __future__ import annotations

import sys

from ewsmcp.cache.store import CacheStore
from ewsmcp.config import Settings
from ewsmcp.db import Database

DAYS = 14
WINDOW = "created_at > now() - make_interval(days => %s)"


def main() -> int:
    settings = Settings()
    db = Database(settings.database_url)
    store = CacheStore(db)
    with store.db.conn() as c:
        totals = c.execute(
            "SELECT detector, COUNT(*) AS hits, "
            "  COUNT(*) FILTER (WHERE dropped = 1) AS dropped, "
            "  COUNT(*) FILTER (WHERE ref_label LIKE 'error:%%') AS errors, "
            "  COUNT(DISTINCT message_ews_id) AS messages "
            f"FROM ews.boilerplate_hits WHERE {WINDOW} "
            "GROUP BY detector ORDER BY detector", (DAYS,)).fetchall()
        only = c.execute(
            "WITH per AS ("
            "  SELECT message_ews_id,"
            "    bool_or(detector = 'embedding') AS emb,"
            "    bool_or(detector = 'llm' AND real_hit) AS llm"
            "  FROM (SELECT message_ews_id, detector,"
            "          ref_label NOT LIKE 'error:%%' AS real_hit"
            f"        FROM ews.boilerplate_hits WHERE {WINDOW}) h"
            "  GROUP BY message_ews_id)"
            " SELECT COUNT(*) FILTER (WHERE emb AND NOT llm) AS only_embedding,"
            "        COUNT(*) FILTER (WHERE llm AND NOT emb) AS only_llm,"
            "        COUNT(*) FILTER (WHERE emb AND llm) AS both,"
            "        COUNT(*) AS messages"
            " FROM per", (DAYS,)).fetchone()

        print(f"boilerplate hits, last {DAYS} days")
        print(f"{'detector':<12}{'hits':>8}{'dropped':>9}{'errors':>8}{'messages':>10}")
        for r in totals:
            print(f"{r['detector']:<12}{r['hits']:>8}{r['dropped']:>9}"
                  f"{r['errors']:>8}{r['messages']:>10}")
        if not totals:
            print("  (no hits logged)")
        print()
        print(f"messages flagged: embedding only {only['only_embedding']}, "
              f"llm only {only['only_llm']}, both {only['both']} "
              f"(of {only['messages']} flagged messages)")

        for detector in ("embedding", "llm"):
            print()
            print(f"--- 20 random {detector} paragraphs "
                  f"------------------------------")
            rows = c.execute(
                "SELECT message_ews_id, similarity, ref_label, dropped, paragraph "
                f"FROM ews.boilerplate_hits WHERE {WINDOW} AND detector = %s "
                "ORDER BY random() LIMIT 20", (DAYS, detector)).fetchall()
            if not rows:
                print("  (none)")
            for r in rows:
                sim = f"{r['similarity']:.3f}" if r["similarity"] is not None else "-"
                para = " ".join((r["paragraph"] or "").split())[:160]
                print(f"[{r['message_ews_id'][:12]}] sim={sim} "
                      f"dropped={bool(r['dropped'])} label={r['ref_label']!r}")
                print(f"    {para}")
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
