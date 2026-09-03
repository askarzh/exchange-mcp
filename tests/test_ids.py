"""Tests for the short-alias layer over raw EWS item ids (ewsmcp/ids.py).

All tests hit a real Postgres (via the `db` fixture: fresh `ews` schema per
test) and no network. The raw-id fixtures deliberately contain ``=`` /
uppercase so they can never be mistaken for an alias by the
``^[a-z]{1,2}[0-9]+$`` shape check.
"""

from __future__ import annotations

import re
import threading

import pytest

from ewsmcp.ids import IdAliaser, NullAliaser, kind_for_key  # noqa: F401

RAW_A = "AAMkAGI2NGVhZTVlLTI3ZjMtNDlmMS1iZjk4LWRlMDUxYmQ5NzU5AAA="
RAW_B = "AAMkAGI2NGVhZTVlLTI3ZjMtNDlmMS1iZjk4LU1PVkVEX0FGVEVSAAB="


@pytest.fixture
def aliaser(db) -> IdAliaser:
    return IdAliaser(db)


# --- Minting -----------------------------------------------------------------


def test_mint_and_idempotent_realias(aliaser):
    alias = aliaser.alias_for(RAW_A)
    assert alias == "m1"
    # Same raw id again -> same alias, no second row.
    assert aliaser.alias_for(RAW_A) == "m1"
    assert aliaser.stats() == {"m": 1}


def test_per_kind_counters_are_independent(aliaser):
    assert aliaser.alias_for("ID-M-ONE=", kind="m") == "m1"
    assert aliaser.alias_for("ID-E-ONE=", kind="e") == "e1"
    assert aliaser.alias_for("ID-M-TWO=", kind="m") == "m2"
    assert aliaser.alias_for("ID-E-TWO=", kind="e") == "e2"
    assert aliaser.alias_for("ID-D-ONE=", kind="d") == "d1"
    assert aliaser.stats() == {"m": 2, "e": 2, "d": 1}


# --- Resolution ----------------------------------------------------------------


def test_resolve_roundtrip(aliaser):
    alias = aliaser.alias_for(RAW_A)
    assert aliaser.resolve(alias) == RAW_A


def test_resolve_passes_through_non_alias_values(aliaser):
    # Raw EWS ids, arbitrary strings, and near-misses of the alias shape
    # must come back untouched.
    for value in (
        RAW_A,                      # raw EWS id
        "Inbox/Subfolder",          # arbitrary string
        "hello world",
        "M1",                       # uppercase: not alias-shaped
        "m",                        # no counter digits
        "abc1",                     # three letters: not alias-shaped
        "",                         # empty string
    ):
        assert aliaser.resolve(value) == value


def test_resolve_unknown_alias_raises_helpful_keyerror(aliaser):
    with pytest.raises(KeyError) as excinfo:
        aliaser.resolve("m42")
    message = str(excinfo.value)
    assert "stale" in message
    assert "re-run" in message.lower()


# --- Rebinding -----------------------------------------------------------------


def test_rebind_keeps_alias_and_resolves_to_new_id(aliaser):
    alias = aliaser.alias_for(RAW_A)
    assert aliaser.rebind(RAW_A, RAW_B, changekey="CK-NEW") == alias
    assert aliaser.resolve(alias) == RAW_B
    # The old raw id is no longer registered; a fresh alias_for would mint.
    assert aliaser.stats() == {"m": 1}


def test_rebind_unknown_old_id_returns_none(aliaser):
    assert aliaser.rebind("NEVER-SEEN=", RAW_B) is None


def test_rebind_onto_id_already_aliased_elsewhere(aliaser):
    first = aliaser.alias_for("ID-OLD=")    # m1
    second = aliaser.alias_for("ID-NEW=")   # m2 — same item, seen post-move
    assert aliaser.rebind("ID-OLD=", "ID-NEW=") == first
    # The surviving handle is the one the model already holds.
    assert aliaser.resolve(first) == "ID-NEW="
    with pytest.raises(KeyError):
        aliaser.resolve(second)
    assert aliaser.stats() == {"m": 1}


# --- Internet-Message-Id ---------------------------------------------------------


def test_imid_storage_and_retrieval(aliaser):
    alias = aliaser.alias_for(RAW_A, internet_message_id="<msg-1@example.com>")
    assert aliaser.imid_for(alias) == "<msg-1@example.com>"
    assert aliaser.imid_for(RAW_A) == "<msg-1@example.com>"
    assert aliaser.imid_for("m999") is None
    assert aliaser.imid_for("UNKNOWN-RAW=") is None
    # Backfill: imid supplied on a later sighting sticks to the same alias.
    late = aliaser.alias_for("ID-LATE=")
    assert aliaser.imid_for(late) is None
    aliaser.alias_for("ID-LATE=", internet_message_id="<late@example.com>")
    assert aliaser.imid_for(late) == "<late@example.com>"


# --- Persistence -----------------------------------------------------------------


def test_persistence_across_instances_on_same_db(db):
    first = IdAliaser(db)
    alias = first.alias_for(RAW_A, kind="m")
    second = IdAliaser(db)
    assert second.resolve(alias) == RAW_A
    assert second.alias_for("ID-NEW=", kind="m") == "m2"  # counter continues


# --- Thread safety ----------------------------------------------------------------


def test_thread_safety_smoke(aliaser):
    n_threads, n_per_thread = 8, 50
    results: list[list[str]] = [[] for _ in range(n_threads)]

    def worker(i: int) -> None:
        for j in range(n_per_thread):
            results[i].append(aliaser.alias_for(f"ID-THREAD-{i}-{j}="))

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    flat = [alias for chunk in results for alias in chunk]
    assert len(flat) == n_threads * n_per_thread
    # Every distinct raw id got a distinct, well-formed alias.
    assert len(set(flat)) == n_threads * n_per_thread
    assert all(re.fullmatch(r"m[0-9]+", alias) for alias in flat)
    assert aliaser.stats() == {"m": n_threads * n_per_thread}


# --- Bulk minting ------------------------------------------------------------------


def test_alias_many_mints_in_one_transaction(aliaser):
    out = aliaser.alias_many([("A=", "m", None, None), ("B=", "e", "CK", "<b@x>"),
                              ("", "m", None, None)])
    assert out == {"A=": "m1", "B=": "e1"}
    assert aliaser.imid_for("e1") == "<b@x>"


# --- Defensive behaviour -----------------------------------------------------------


def test_alias_for_and_rebind_swallow_storage_errors(db, monkeypatch):
    aliaser = IdAliaser(db)
    aliaser.alias_for(RAW_A)
    db.close()  # every later query fails
    assert aliaser.alias_for("NEW=") == "NEW="  # fail open: raw id back
    assert aliaser.rebind(RAW_A, RAW_B) is None
    assert aliaser.stats() == {}
    assert aliaser.resolve("m1") == "m1"  # lookup failed → pass through


def test_concurrent_mint_of_same_id_yields_one_alias(db):
    import threading
    aliaser = IdAliaser(db)
    results = []

    def mint():
        results.append(aliaser.alias_for("SAME=", kind="m"))

    threads = [threading.Thread(target=mint) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(results) == {"m1"}
    assert aliaser.stats() == {"m": 1}


# --- Kind inference ------------------------------------------------------------------


def test_kind_for_key_mapping():
    assert kind_for_key("message_id") == "m"
    assert kind_for_key("email_id") == "m"
    assert kind_for_key("draft_id") == "d"
    assert kind_for_key("event_id") == "e"
    assert kind_for_key("appointment_id") == "e"
    assert kind_for_key("task_id") == "k"
    assert kind_for_key("contact_id") == "c"
    assert kind_for_key("attachment_id") == "a"
    assert kind_for_key("conversation_id") == "t"
    assert kind_for_key("thread_id") == "t"
    assert kind_for_key("folder_id") == "f"
    assert kind_for_key("MESSAGE_ID") == "m"  # case-insensitive
    assert kind_for_key("something_else") == "x"
    assert kind_for_key("") == "x"
