"""Short-alias layer over raw EWS item identifiers.

Raw EWS ``ItemId`` values are ~100-150 characters of case-sensitive
base64 — expensive for an LLM to copy verbatim and silently remapped
whenever an item moves folders. This module mints short, stable aliases
(``m1``, ``e3``, ``a12`` …) that a central dispatcher attaches to tool
outputs and transparently resolves back to raw ids on tool inputs.

Design goals
------------
1. **Tiny, model-friendly handles.** Aliases match ``^[a-z]{1,2}[0-9]+$``
   — one or two lowercase kind letters plus a per-kind counter. Anything
   that does not look like an alias passes through :meth:`IdAliaser.resolve`
   unchanged, so raw EWS ids keep working everywhere.
2. **Stable across moves.** When a move/copy remaps an ``ItemId`` the
   dispatcher calls :meth:`IdAliaser.rebind` and the *same* alias starts
   pointing at the new raw id — the model never has to re-learn handles.
3. **SQL safety.** Every query uses parameterised placeholders; never
   f-string values into SQL.
4. **Persistent and shared.** State lives in Postgres (``ews.aliases`` /
   ``ews.alias_counters``), shared by every process on the same database
   — the daemon and all MCPs mint from one counter sequence, and aliases
   survive restarts and stay consistent across sessions.
5. **Thread safety.** Minting is a single UPSERT + INSERT under Postgres'
   own row-level locking; a concurrent minter loses the UNIQUE(ews_id)
   race and re-reads the winner's row.
6. **Fail open, not loud.** Aliasing is sugar — a database hiccup must
   never break a tool call. :meth:`IdAliaser.alias_for` falls back to the
   raw id and :meth:`IdAliaser.rebind` to ``None`` on storage errors. The
   one deliberate exception: resolving an alias-shaped string that is
   *unknown* raises ``KeyError`` so the model learns the handle is stale
   instead of EWS rejecting a garbage id downstream.
"""

import logging
import re
import time

import psycopg

from .db import Database

_LOG = logging.getLogger(__name__)


# --- Configuration --------------------------------------------------------

# What an alias looks like: one or two lowercase kind letters + counter.
_ALIAS_RE = re.compile(r"^[a-z]{1,2}[0-9]+$")
# Kinds must be alias-prefix shaped, otherwise minted handles would not
# round-trip through resolve().
_KIND_RE = re.compile(r"^[a-z]{1,2}$")

# Tool-argument key -> kind letter. Anything unrecognised maps to "x".
_KIND_BY_KEY = {
    "message_id": "m",
    "email_id": "m",
    "draft_id": "d",
    "event_id": "e",
    "appointment_id": "e",
    "task_id": "k",
    "contact_id": "c",
    "attachment_id": "a",
    "conversation_id": "t",
    "thread_id": "t",
    "folder_id": "f",
}


def kind_for_key(key: str) -> str:
    """Infer the alias kind letter from a tool-argument key name."""
    return _KIND_BY_KEY.get((key or "").lower(), "x")


# --- Aliaser implementation ------------------------------------------------

_MINT = """
INSERT INTO ews.alias_counters (kind, n) VALUES (%s, 1)
ON CONFLICT (kind) DO UPDATE SET n = ews.alias_counters.n + 1
RETURNING n
"""


class IdAliaser:
    """Postgres-backed bidirectional map between short aliases and EWS ids.
    Shared by every process on the same database: the daemon and all MCPs
    mint from one counter sequence."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def _find(self, c, ews_id: str):
        return c.execute(
            "SELECT alias, changekey, internet_message_id FROM ews.aliases "
            "WHERE ews_id = %s", (ews_id,)).fetchone()

    def _mint_locked(self, c, ews_id: str, kind: str, changekey, imid) -> str:
        """Inside a transaction: return the existing alias (refreshing metadata)
        or mint a new one. A concurrent minter loses on UNIQUE(ews_id) and
        re-reads the winner's row."""
        now = time.time()
        row = self._find(c, ews_id)
        if row:
            c.execute(
                "UPDATE ews.aliases SET last_seen = %s, changekey = COALESCE(%s, changekey), "
                "internet_message_id = COALESCE(%s, internet_message_id) WHERE alias = %s",
                (now, changekey, imid, row["alias"]))
            return row["alias"]
        n = c.execute(_MINT, (kind,)).fetchone()["n"]
        alias = f"{kind}{n}"
        try:
            with c.transaction():  # savepoint: a lost race must not poison the txn
                c.execute(
                    "INSERT INTO ews.aliases (alias, kind, ews_id, changekey, "
                    "internet_message_id, first_seen, last_seen) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (alias, kind, ews_id, changekey, imid, now, now))
        except psycopg.errors.UniqueViolation:
            row = self._find(c, ews_id)
            return row["alias"] if row else ews_id
        return alias

    def alias_for(self, ews_id: str, kind: str = "m", changekey: str | None = None,
                  internet_message_id: str | None = None) -> str:
        """Return the (existing or freshly minted) alias for ``ews_id``.

        Idempotent: the same raw id always maps to the same alias, whose
        ``last_seen`` is refreshed and whose changekey / Internet-Message-Id
        are backfilled when newly supplied. New aliases are ``<kind><n>``
        with a per-kind counter bumped atomically. Never raises on storage
        errors — falls back to returning the raw id.
        """
        if not ews_id:
            return ews_id
        if not _KIND_RE.match(kind):
            _LOG.warning("id_alias: invalid kind %r; using 'x'", kind)
            kind = "x"
        try:
            with self.db.conn() as c:
                row = self._find(c, ews_id)
                if row is not None and (
                    changekey is None or row["changekey"] == changekey
                ) and (
                    internet_message_id is None
                    or row["internet_message_id"] == internet_message_id
                ):
                    return row["alias"]
                return self._mint_locked(c, ews_id, kind, changekey, internet_message_id)
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: alias_for failed (%s); returning raw id", exc)
            return ews_id

    def alias_many(self, entries: "list[tuple[str, str, str | None, str | None]]") -> dict:
        """Bulk ``alias_for``: one transaction for a whole result page.

        ``entries`` is ``[(ews_id, kind, changekey, internet_message_id)]``.
        Returns ``{ews_id: alias}``. Never raises on storage errors —
        missing entries simply fall back to per-id alias_for behavior.
        """
        out: dict = {}
        if not entries:
            return out
        try:
            with self.db.conn() as c:
                for ews_id, kind, changekey, imid in entries:
                    if not ews_id:
                        continue
                    if not _KIND_RE.match(kind):
                        kind = "x"
                    out[ews_id] = self._mint_locked(c, ews_id, kind, changekey, imid)
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: alias_many failed (%s); falling back", exc)
        return out

    def resolve(self, value: str) -> str:
        """Translate an alias back to its raw EWS id.

        Non-alias-shaped input (raw EWS id, folder name, arbitrary string)
        passes through unchanged. An alias-shaped but *unknown* value raises
        ``KeyError`` — deliberately, so the model re-fetches fresh ids
        instead of EWS failing on a phantom handle.
        """
        if not isinstance(value, str) or not _ALIAS_RE.match(value):
            return value
        try:
            with self.db.conn() as c:
                row = c.execute("SELECT ews_id FROM ews.aliases WHERE alias = %s",
                                (value,)).fetchone()
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: resolve lookup failed for %r (%s); passing through",
                         value, exc)
            return value
        if row is None:
            raise KeyError(
                f"Unknown alias {value!r}: it is stale or from a previous session. "
                "EWS ids change when items move; re-run the search/list tool to get "
                "fresh ids, then retry.")
        return row["ews_id"]

    def rebind(self, old_ews_id: str, new_ews_id: str,
               changekey: str | None = None) -> str | None:
        """Repoint the alias of ``old_ews_id`` at ``new_ews_id`` after a move.

        Keeps the alias itself stable. If ``new_ews_id`` was already
        registered under a *different* alias, that duplicate row is dropped
        first (the surviving handle is the one the model already holds).
        Returns the alias, or ``None`` when the old id was never aliased.
        Never raises on storage errors.
        """
        try:
            with self.db.conn() as c:
                row = c.execute("SELECT alias FROM ews.aliases WHERE ews_id = %s FOR UPDATE",
                                (old_ews_id,)).fetchone()
                if not row:
                    return None
                alias = row["alias"]
                c.execute("DELETE FROM ews.aliases WHERE ews_id = %s AND alias <> %s",
                          (new_ews_id, alias))
                c.execute(
                    "UPDATE ews.aliases SET ews_id = %s, changekey = COALESCE(%s, changekey), "
                    "last_seen = %s WHERE alias = %s",
                    (new_ews_id, changekey, time.time(), alias))
                return alias
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: rebind failed (%s); returning None", exc)
            return None

    def imid_for(self, alias_or_id: str) -> str | None:
        """Return the stored Internet-Message-Id for an alias or raw id."""
        try:
            with self.db.conn() as c:
                row = c.execute(
                    "SELECT internet_message_id FROM ews.aliases "
                    "WHERE alias = %s OR ews_id = %s", (alias_or_id, alias_or_id)).fetchone()
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: imid_for failed (%s)", exc)
            return None
        return row["internet_message_id"] if row else None

    def stats(self) -> dict:
        """Alias count per kind, e.g. ``{"m": 12, "e": 3}``."""
        try:
            with self.db.conn() as c:
                rows = c.execute(
                    "SELECT kind, COUNT(*) AS c FROM ews.aliases GROUP BY kind").fetchall()
        except (psycopg.Error, RuntimeError) as exc:
            _LOG.warning("id_alias: stats failed (%s)", exc)
            return {}
        return {r["kind"]: r["c"] for r in rows}
