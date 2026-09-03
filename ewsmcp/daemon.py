"""ewsd — the Exchange daemon: sync engine, uploads, audit, and the /v1 API
the thin MCP calls. Runs once per mailbox. No MCP transport here."""

import asyncio
import logging
import sys

from .config import Settings, get_settings
from .http import build_app
from .server import build_context

logger = logging.getLogger(__name__)


def build_daemon_app(ctx, settings: Settings):
    return build_app(ctx, settings, None, mount_mcp=False, tools_prefix="/v1/tools",
                     api_key=settings.ewsd_api_key or "")


async def serve(settings: Settings) -> None:
    import uvicorn
    settings.require_exchange()
    if settings.ewsd_host not in ("127.0.0.1", "localhost", "::1") and not settings.ewsd_api_key:
        raise SystemExit("refusing to bind ewsd on a non-loopback address without EWSD_API_KEY")
    ctx = build_context(settings)
    app = build_daemon_app(ctx, settings)
    logger.info("ewsd starting on %s:%s", settings.ewsd_host, settings.ewsd_port)
    config = uvicorn.Config(app, host=settings.ewsd_host, port=settings.ewsd_port,
                            log_level=settings.log_level.lower(), http="h11")
    await uvicorn.Server(config).serve()


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO),
                        stream=sys.stderr,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        asyncio.run(serve(settings))
    except KeyboardInterrupt:
        print("ewsd shutting down", file=sys.stderr)


if __name__ == "__main__":
    main()
