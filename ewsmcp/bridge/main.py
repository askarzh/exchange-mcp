"""Serve the mail bridge. Its own process and its own port: `ewsd` holds a live
Exchange connection and must not fall over because Mindet asked for a page."""
from __future__ import annotations

import logging
import os
import sys

import uvicorn

from ..config import get_settings
from ..db import Database
from .app import build_app


def main() -> None:
    settings = get_settings()
    # Same shape as ewsd's entry point. Without it the bridge's own log lines
    # fall through to logging.lastResort, which prints them bare and drops
    # everything below WARNING — including the one INFO line that says a
    # consumer is holding a cursor from a store that no longer exists.
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        stream=sys.stderr,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    db = Database(settings.database_url)
    db.migrate()
    # build_app raises ValueError on an empty token — deliberately left
    # uncaught, so a missing EWS_BRIDGE_TOKEN kills the process at startup
    # rather than serving an open bridge.
    app = build_app(db, token=os.environ["EWS_BRIDGE_TOKEN"], owner_email=settings.ews_email)
    uvicorn.run(app, host=os.environ.get("EWS_BRIDGE_HOST", "0.0.0.0"),
                port=int(os.environ.get("EWS_BRIDGE_PORT", "8081")))


if __name__ == "__main__":
    main()
