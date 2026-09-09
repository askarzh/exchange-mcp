"""The one property test in this suite, and why it is here.

Every cursor this bridge hands out rests on a single invariant: **`seq` is
monotonic in `first_seen`**. `until` filters a page on `first_seen` while the
page is ordered and cut by `seq`, so the moment the two disagree, `until` stops
being a prefix of the sequence stream — a bounded walk ends on a cursor with
live mail sitting below it, and that mail is never delivered, with no error
anywhere.

Three separate Criticals on this branch were that one invariant breaking:
the bounded page's cursor jumping to the live head; an amended mail keeping its
old arrival time while taking a new sequence; and a discovery taking an arrival
time reconstructed from its send date. Each was found by a different round-trip
test written *after* the fact. Four examples arguing for a property is the case
for checking the property.

So this test builds a store by a randomised sequence of the operations that
really happen to one — mail arriving, mail arriving out of order, mail with no
send date, mail with a send date in the future, an old mail amended in Outlook,
a sweep landing at an arbitrary moment between them — and after every sweep
asserts the ledger read by `seq` is the ledger read by `first_seen`. The seed
is fixed so a failure reproduces, and the operation log is printed with it so
whoever meets this in a year knows what built the store.

It is the only property test in the suite deliberately: this is the only
invariant here that every other guarantee is derived from.
"""
import datetime as dt
import random

from ewsmcp.bridge import arrival

SEED = 20260909          # fixed: a failure here must reproduce exactly
OPERATIONS = 60


def _insert(conn, ews_id, sent):
    conn.execute(
        "INSERT INTO ews.messages (ews_id, changekey, folder_id, conversation_id,"
        " sender_email, subject, date_ts, body_clean)"
        " VALUES (%s,'ck1','inbox',%s,'a@example.test','s',%s,'b')",
        (ews_id, "conv-" + ews_id, None if sent is None else int(sent.timestamp())))


def _ledger(conn):
    return [(r["seq"], r["first_seen"], r["ews_id"]) for r in conn.execute(
        "SELECT seq, first_seen, ews_id FROM ews.bridge_arrival ORDER BY seq")]


def test_the_sequence_and_the_arrival_time_never_disagree(db):
    rng = random.Random(SEED)
    now = dt.datetime.now(dt.timezone.utc)
    log: list[str] = []
    live: list[str] = []
    oldest = now - dt.timedelta(days=30)
    counter = 0

    # The store exists before the ledger does — the ordinary case, and the only
    # one in which `first_seen` is anything but a sweep's clock.
    with db.conn() as conn:
        for i in range(6):
            ews_id = f"seed-{i}"
            # one of them carries a send date in the future — a sender whose
            # clock is wrong, which the real store holds
            sent = (now + dt.timedelta(days=200) if i == 5
                    else now - dt.timedelta(days=rng.randint(20, 400)))
            oldest = min(oldest, sent)
            _insert(conn, ews_id, sent)
            live.append(ews_id)
            log.append(f"pre-existing {ews_id} sent {sent.date()}")
        conn.execute("TRUNCATE ews.bridge_arrival")
    db.reapply_migration_for_tests(5)
    db.reapply_migration_for_tests(6)
    log.append("migration 005/006 backfilled the store that was already there")

    def check(where: str) -> None:
        with db.conn() as conn:
            rows = _ledger(conn)
        by_arrival = sorted(rows, key=lambda r: (r[1], r[0]))
        if rows != by_arrival:
            bad = next(i for i, (a, b) in enumerate(zip(rows, by_arrival)) if a != b)
            raise AssertionError(
                f"seq and first_seen disagree after {where}.\n"
                f"first divergence at position {bad}: by seq {rows[bad]}, "
                f"by arrival {by_arrival[bad]}\n"
                f"seed={SEED}\noperations:\n  " + "\n  ".join(log))

    for _ in range(OPERATIONS):
        choice = rng.choice(["dated", "undated", "older", "future", "amend",
                             "sweep", "sweep", "sweep"])
        with db.conn() as conn:
            if choice == "sweep":
                n = arrival.sweep(conn)
                log.append(f"sweep ({n} row(s))")
            elif choice == "amend" and live:
                ews_id = rng.choice(live)
                conn.execute("UPDATE ews.messages SET changekey = %s WHERE ews_id = %s",
                             (f"ck{rng.randint(2, 10_000)}", ews_id))
                log.append(f"amended {ews_id} (a read flag in Outlook)")
            else:
                counter += 1
                ews_id = f"m{counter}"
                if choice == "undated":
                    sent = None                      # a draft or a calendar notice
                elif choice == "older":
                    # a folder sync reaching further back than anything so far
                    oldest = sent = oldest - dt.timedelta(days=rng.randint(1, 90))
                elif choice == "future":
                    # a sender whose clock is wrong: the store really holds these
                    sent = now + dt.timedelta(days=rng.randint(1, 400))
                else:
                    sent = now - dt.timedelta(days=rng.randint(0, 30))
                _insert(conn, ews_id, sent)
                live.append(ews_id)
                log.append(f"{choice} {ews_id} sent "
                           f"{sent.date() if sent else 'never (no date_ts)'}")
        if choice == "sweep":
            check(log[-1])

    with db.conn() as conn:
        arrival.sweep(conn)
    check("the final sweep")
    # and the store this built was worth checking
    with db.conn() as conn:
        assert len(_ledger(conn)) >= 15
