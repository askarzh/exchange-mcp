"""What may be archived, and when.

The policy is a frozen value object: the runner builds one per pass from
settings (optionally narrowed by the tool's `before`/`folders` arguments) and
hands the SAME object to capturer, verifier and deleter, so a run cannot half
apply one policy and half another.
"""

from __future__ import annotations

import os
import time
from dataclasses import asdict, dataclass, replace
from typing import Any

from ..dates import parse_when

# Spec §3: mail only. Drafts and the outbox are excluded too — a draft has no
# server copy worth keeping and an outbox item is mid-flight.
NEVER_ARCHIVED = ("f:calendar", "f:contacts", "f:tasks", "f:drafts", "f:outbox")
DAY = 86400


def _normalise(keys: Any) -> tuple[str, ...]:
    out: list[str] = []
    raw = keys.split(",") if isinstance(keys, str) else list(keys or [])
    for key in raw:
        key = str(key).strip().lower()
        if not key:
            continue
        if not key.startswith("f:"):
            key = f"f:{key}"
        if key in NEVER_ARCHIVED:
            raise ValueError(
                f"{key} is never archived (calendar, contacts, tasks, drafts "
                "and the outbox are out of scope by design)")
        if key not in out:
            out.append(key)
    return tuple(out)


@dataclass(frozen=True)
class ArchivePolicy:
    folders: tuple[str, ...]
    after_days: int
    exclude_categories: tuple[str, ...]
    grace_days: int
    delete_enabled: bool
    max_delete_per_run: int
    min_free_gb: float
    before_ts: int | None = None  # explicit cutoff from archive_run(before=…)
    capture_after_days: int | None = None  # None: capture at after_days
    backup_stamp: str = ""                 # empty: rail 4 is off
    backup_max_age_hours: int = 48
    backup_hold_max_days: int = 10

    @classmethod
    def from_settings(cls, settings: Any) -> "ArchivePolicy":
        return cls(
            folders=_normalise(settings.archive_folders),
            after_days=int(settings.archive_after_days),
            exclude_categories=tuple(
                c.strip().lower()
                for c in (settings.archive_exclude_categories or "").split(",")
                if c.strip()),
            # Floored at 1: a grace period of 0 (or negative, via a stray
            # override) would let something just marked verified become
            # immediately deletable — the whole point of the grace period is
            # a minimum cooling-off window.
            grace_days=max(1, int(settings.archive_grace_days)),
            delete_enabled=bool(settings.archive_delete_enabled),
            max_delete_per_run=int(settings.archive_max_delete_per_run),
            min_free_gb=float(settings.archive_min_free_gb),
            capture_after_days=settings.archive_capture_after_days,
            backup_stamp=settings.archive_backup_stamp,
            backup_max_age_hours=int(settings.archive_backup_max_age_hours),
            backup_hold_max_days=int(settings.archive_backup_hold_max_days),
        )

    def with_overrides(self, *, before: str | None = None,
                       folders: list[str] | None = None,
                       tz: str = "UTC") -> "ArchivePolicy":
        changes: dict[str, Any] = {}
        if folders:
            changes["folders"] = _normalise(folders)
        if before:
            changes["before_ts"] = int(parse_when(before, "before", tz).timestamp())
        return replace(self, **changes) if changes else self

    # ------------------------------------------------------------- cutoffs

    def _age_cutoff_ts(self, days: int, now: float | None) -> int:
        if self.before_ts is not None:
            return int(self.before_ts)
        return int((now if now is not None else time.time()) - days * DAY)

    def capture_cutoff_ts(self, now: float | None = None) -> int:
        """Never later than after_days: mail that is not captured cannot be
        deleted, and a mailbox with a quota fills up behind it."""
        days = self.after_days if self.capture_after_days is None else (
            min(max(0, int(self.capture_after_days)), self.after_days))
        return self._age_cutoff_ts(days, now)

    def delete_cutoff_ts(self, now: float | None = None) -> int:
        """after_days PLUS the grace period — rail 3, the date half. Measured
        from after_days, not from the capture age: capturing mail early says
        nothing about when it may leave the mailbox."""
        return self._age_cutoff_ts(self.after_days, now) - self.grace_days * DAY

    def grace_instant_ts(self, now: float | None = None) -> int:
        """A row must have been verified at least grace_days ago — rail 3, the
        verification half. Freshly verified mail is never deleted, however old."""
        return int((now if now is not None else time.time())
                   - self.grace_days * DAY)

    # -------------------------------------------------------------- backup

    def backup_state(self, data_dir: str, now: float | None = None) -> dict[str, Any]:
        """Rail 4. `state` is off, fresh, stale (holds deletion), expired (the
        hold ran out: deletion resumes, loudly) or missing (holds for good)."""
        if not self.backup_stamp:
            return {"state": "off", "age_hours": None}
        try:
            stamped = os.stat(os.path.join(data_dir, self.backup_stamp)).st_mtime
        except OSError:
            return {"state": "missing", "age_hours": None}
        age = ((now if now is not None else time.time()) - stamped) / 3600
        if age <= self.backup_max_age_hours:
            state = "fresh"
        elif age <= self.backup_hold_max_days * 24:
            state = "stale"
        else:
            state = "expired"
        return {"state": state, "age_hours": round(age, 1)}

    # ------------------------------------------------------------- folders

    def folder_ids(self, store: Any) -> list[str]:
        """Fail-closed: an EMPTY ``folders`` (ARCHIVE_FOLDERS explicitly set to
        "", or overridden to an empty list) selects NOTHING — never every
        folder. `CacheStore.archive_candidates` distinguishes `[]` ("no
        folder") from `None` ("every folder"); the policy never returns
        `None` here, so a misconfigured/blanked-out ARCHIVE_FOLDERS cannot
        silently widen a run to the whole mailbox."""
        return store.folder_ids_for_wk(list(self.folders)) if self.folders else []

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
