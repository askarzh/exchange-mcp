"""What may be archived, and when.

The policy is a frozen value object: the runner builds one per pass from
settings (optionally narrowed by the tool's `before`/`folders` arguments) and
hands the SAME object to capturer, verifier and deleter, so a run cannot half
apply one policy and half another.
"""

from __future__ import annotations

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

    @classmethod
    def from_settings(cls, settings: Any) -> "ArchivePolicy":
        return cls(
            folders=_normalise(settings.archive_folders),
            after_days=int(settings.archive_after_days),
            exclude_categories=tuple(
                c.strip().lower()
                for c in (settings.archive_exclude_categories or "").split(",")
                if c.strip()),
            grace_days=int(settings.archive_grace_days),
            delete_enabled=bool(settings.archive_delete_enabled),
            max_delete_per_run=int(settings.archive_max_delete_per_run),
            min_free_gb=float(settings.archive_min_free_gb),
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

    def capture_cutoff_ts(self, now: float | None = None) -> int:
        if self.before_ts is not None:
            return int(self.before_ts)
        return int((now if now is not None else time.time())
                   - self.after_days * DAY)

    def delete_cutoff_ts(self, now: float | None = None) -> int:
        """Cutoff PLUS the grace period — rail 3, the date half."""
        return self.capture_cutoff_ts(now) - self.grace_days * DAY

    def grace_instant_ts(self, now: float | None = None) -> int:
        """A row must have been verified at least grace_days ago — rail 3, the
        verification half. Freshly verified mail is never deleted, however old."""
        return int((now if now is not None else time.time())
                   - self.grace_days * DAY)

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
