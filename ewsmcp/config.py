"""Environment-driven configuration (12-factor; every knob defaults safe)."""

from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Path fragments that identify cloud-synced folders. The data dir holds
# mail-at-rest (alias DB, audit chain, cache mirror) — it must never ride
# a sync client onto other machines or a vendor cloud.
_SYNCED_MARKERS = ("onedrive", "dropbox", "google drive", "googledrive", "icloud")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # --- Exchange upstream (the daemon needs these; the MCP does not) --------
    ews_server_url: str | None = None
    ews_email: str = ""  # ewsd requires this (see require_exchange); ewsmcp does not
    ews_username: str | None = None
    ews_password: str | None = None
    # NEVER pin auth_type against this Exchange: the front door only works
    # via exchangelib auto-negotiation (verified live 2026-06-12; pinning
    # BASIC/NTLM both fail). Escape hatch for a *different* server only.
    ews_auth_type_force: Literal["basic", "ntlm", "digest"] | None = None
    ews_insecure_skip_verify: bool = False
    ews_tz: str = "Asia/Riyadh"
    request_timeout: int = 30

    # --- Storage: Postgres (both processes) -----------------------------------
    database_url: str  # postgresql://user:pass@host:5432/ews

    # --- Daemon HTTP API (ewsd serves; ewsmcp calls) ---------------------------
    ewsd_host: str = "127.0.0.1"
    ewsd_port: int = 8790
    ewsd_api_key: str | None = None  # bearer the MCP presents; required off-loopback
    ewsd_url: str = "http://127.0.0.1:8790"

    # --- Mirror sync (daemon) ---------------------------------------------------
    # Every mail folder is mirrored in full. These well-known keys are not:
    # drafts churn, junk/trash are noise, outbox is transient.
    ews_mirror_exclude: str = "drafts,junk,trash,outbox"
    ews_cache_sync_seconds: int = 45
    ews_cache_hierarchy_seconds: int = 600

    # --- Reliability --------------------------------------------------------
    ews_warmup_max_backoff_seconds: int = 300
    ews_heartbeat_seconds: int = 600
    ews_retry_max_wait_seconds: int = 300
    ews_max_concurrency: int = 4
    circuit_failure_threshold: int = 5
    circuit_open_seconds: int = 60

    # --- Safety -------------------------------------------------------------
    ews_capability_tier: Literal["read", "draft", "full"] = "draft"
    send_enabled: bool = False  # kill-switch: v5 defaults SAFE (off)
    send_confirm_secret: str | None = None
    confirm_ttl_seconds: int = 600  # ONE default everywhere (== confirm.DEFAULT_TTL_SECONDS)
    ews_recipient_allowlist: str = ""
    ews_recipient_denylist: str = ""
    ews_max_sends_per_hour: int = 10

    # --- Serving ------------------------------------------------------------
    mcp_transport: Literal["stdio", "http"] = "stdio"
    mcp_host: str = "127.0.0.1"
    mcp_port: int = 8000
    mcp_api_key: str | None = None
    log_level: str = "INFO"
    # Public base URL of this server (e.g. https://ews.example.com), used to build
    # the absolute capability URL returned by create_upload_link. Empty → the tool
    # returns a relative /upload/<token> and the caller supplies its own origin.
    external_url: str = ""

    # --- Storage (NEVER a synced folder) -------------------------------------
    data_dir: str = ""  # empty → ~/.ewsmcp; always resolved to an absolute path
    # Shared space other services can read (files-mcp lists its root, gemini-mcp
    # reads from it). Saved attachments are copied there; nothing else in
    # DATA_DIR — cache, audit chain, alias map — ever leaves. Empty disables it.
    shared_dir: str = ""
    data_dir_allow_synced: bool = False  # explicit opt-out of the synced-path guard

    # --- Response economy ----------------------------------------------------
    default_page_size: int = Field(default=20, le=50)
    body_max_chars: int = 4000

    # --- Embeddings (daemon only; the MCP must never hold this key) ----------
    gemini_api_key: str | None = None
    embed_dims: int = 768  # fixed by migration 003's vector(768) column

    # --- Archive pipeline (daemon only) --------------------------------------
    archive_folders: str = "inbox,sent"        # well-known keys, never calendar/contacts/tasks
    archive_after_days: int = 180
    archive_exclude_categories: str = ""
    archive_grace_days: int = 7
    archive_delete_enabled: bool = False       # rail 1 of 3: deletion is OFF by default
    # Even with ARCHIVE_DELETE_ENABLED=true, the BACKGROUND cycle does not
    # delete unless this is also true: deletion stays a deliberate,
    # confirm-gated `archive_run(kind="delete", dry_run=false)` call until an
    # operator opts the loop in.
    archive_delete_auto: bool = False
    archive_max_delete_per_run: int = 200
    archive_min_free_gb: float = 2.0
    archive_cycle_seconds: int = 300

    # --- Phase 3: boilerplate detectors (daemon only) ------------------------
    embed_boilerplate_threshold: float = 0.80
    archive_boilerplate_drop: str = "off"      # off | embedding | llm | both
    archive_boilerplate_llm: bool = True       # run the LLM detector at all
    # Ceiling on Gemini boundary calls per embed pass. Each call is serial
    # and blocking while the runner lock is held, so a whole page of
    # messages calling out would hold that lock for page * timeout seconds.
    archive_boilerplate_llm_per_cycle: int = 40
    gemini_clean_model: str = "gemini-2.5-flash-lite"
    # --- Phase 3: hygiene ----------------------------------------------------
    archive_gc_interval_hours: int = 168
    archive_max_item_mb: int = 50
    db_pool_max: int = 8

    @model_validator(mode="after")
    def _check_boilerplate_drop(self) -> "Settings":
        if self.archive_boilerplate_drop not in ("off", "embedding", "llm", "both"):
            raise ValueError("ARCHIVE_BOILERPLATE_DROP must be off|embedding|llm|both")
        return self

    @model_validator(mode="after")
    def _check_embed_dims(self) -> "Settings":
        if self.embed_dims != 768:
            raise ValueError(
                f"EMBED_DIMS must be 768 (got {self.embed_dims}): migration 003 "
                "declares chunks.embedding as vector(768). Changing the width "
                "needs a new migration that rebuilds the column and its index."
            )
        return self

    def semantic_enabled(self) -> bool:
        """Semantic search needs a remote embedder; without a key we stay keyword-only."""
        return bool(self.gemini_api_key)

    @model_validator(mode="after")
    def _resolve_data_dir(self) -> "Settings":
        raw = self.data_dir or str(Path.home() / ".ewsmcp")
        resolved = Path(raw).expanduser().resolve()
        if not self.data_dir_allow_synced:
            lowered = str(resolved).lower()
            marker = next((m for m in _SYNCED_MARKERS if m in lowered), None)
            if marker is not None:
                raise ValueError(
                    f"DATA_DIR {resolved} appears to be inside a cloud-synced "
                    f"folder ({marker!r}). It stores mail-at-rest (aliases, "
                    "audit chain, cache) and must stay local — point DATA_DIR "
                    "at a local path, or set DATA_DIR_ALLOW_SYNCED=true to "
                    "accept the risk deliberately."
                )
        self.data_dir = str(resolved)
        return self

    def require_exchange(self) -> None:
        """ewsd boot guard: the daemon cannot run without an Exchange endpoint."""
        missing = [n for n in ("ews_server_url", "ews_email", "ews_password")
                   if not getattr(self, n)]
        if missing:
            raise ValueError("ewsd needs " + ", ".join(m.upper() for m in missing))


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
