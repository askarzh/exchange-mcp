"""Serve the mail bridge. Its own process and its own port: `ewsd` holds a live
Exchange connection and must not fall over because Mindet asked for a page."""
from __future__ import annotations

import os

import uvicorn

from ..config import get_settings
from ..db import Database
from .app import build_app


def main() -> None:
    settings = get_settings()
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
