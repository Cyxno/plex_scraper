"""Eén rol-proces. Wordt door de supervisor gestart:
    python -m plex_scraper.roles.proc <resolver|scraper|web|vfs>
"""
from __future__ import annotations

import logging
import os
import sys

from ..common.config import Settings
from ..common.log import setup_logging


def main() -> int:
    role = sys.argv[1] if len(sys.argv) > 1 else ""
    settings = Settings.from_env()
    setup_logging(os.environ.get("LOG_LEVEL", "info"))
    log = logging.getLogger(f"proc.{role}")

    if role == "resolver":
        import uvicorn
        from ..resolver.api.app import create_app
        from ..cli import build_resolver

        app = create_app(build_resolver(settings), settings)
        host, _, port = settings.resolver_bind.rpartition(":")
        uvicorn.run(app, host=host or "0.0.0.0", port=int(port or 8282),
                    log_level=os.environ.get("LOG_LEVEL", "info"), access_log=False)
        return 0

    if role == "scraper":
        import uvicorn
        from ..scraper.service import create_scraper_app


        app = create_scraper_app(settings)
        bind = os.environ.get("SCRAPER_BIND", "0.0.0.0:8283")
        host, _, port = bind.rpartition(":")
        uvicorn.run(app, host=host or "0.0.0.0", port=int(port or 8283),
                    log_level=os.environ.get("LOG_LEVEL", "info"), access_log=False)
        return 0

    if role == "web":
        import uvicorn
        from ..web.app import create_web_app

        app = create_web_app(settings)
        bind = os.environ.get("WEB_BIND", "0.0.0.0:8285")
        host, _, port = bind.rpartition(":")
        uvicorn.run(app, host=host or "0.0.0.0", port=int(port or 8285),
                    log_level=os.environ.get("LOG_LEVEL", "info"), access_log=False)
        return 0

    if role == "vfs":
        from ..vfs.fs import mount_main
        os.makedirs(settings.vfs_mountpoint, exist_ok=True)
        try:
            mount_main(settings.vfs_mountpoint, settings.resolver_url)
        except RuntimeError as exc:
            log.error("fuse init failed: %r", exc)
            return 1
        return 0

    log.error("onbekende rol: %s", role)
    return 2


if __name__ == "__main__":
    sys.exit(main())
