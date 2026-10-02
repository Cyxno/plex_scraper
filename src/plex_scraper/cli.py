"""plex_scraper role dispatcher — one image, four process-roles.

  resolver  — FastAPI: state machine, scoring, providers, stream proxy
  vfs       — pyfuse3 FUSE mount (stable Plex path)
  scraper   — standalone HTTP service exposing scraper search/score (optional
              diagnostics role; the resolver normally uses scrapers in-process)
  web       — read-only diagnostics GUI (playback troubleshooting)
  register  — CLI: register logical items from YAML
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import httpx
import uvicorn

from .common.config import Settings
from .common.log import setup_logging


def build_resolver(settings: Settings):
    from .resolver.caches import CacheSet
    from .resolver.engine import Resolver
    from .resolver.store import Store
    from .common.scoring.engine import Scorer
    from .scraper.providers.demo_seed import seeded_provider, seeded_scrapers
    from .scraper.providers.torbox import TorboxProvider
    from .scraper.scrapers.torrentio import TorrentioScraper

    os.makedirs(os.path.dirname(settings.db_path) or ".", exist_ok=True)
    store = Store(settings.db_path)
    scorer = Scorer.from_yaml(_preferences_path(settings))
    caches = CacheSet(
        candidates_ttl=settings.cache_candidates_ttl,
        checkcached_ttl=settings.cache_checkcached_ttl,
        link_ttl=settings.cache_link_ttl,
    )
    if settings.torbox_api_token:
        provider = TorboxProvider(settings)
        scrapers = [TorrentioScraper(settings.scraper_torrentio_base)]
    else:
        # PoC/demo convenience: no token -> seeded mocks (synthetic bytes)
        logging.getLogger("resolver").warning(
            "TORBOX_API_TOKEN not set - using seeded mock provider/scraper")
        provider = seeded_provider()
        scrapers = seeded_scrapers()
    return Resolver(settings, store, provider, scrapers, scorer, caches)


def _preferences_path(settings: Settings) -> str:
    for candidate in ("preferences.yaml", "preferences.example.yaml"):
        path = os.path.join(settings.config_dir, candidate)
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"no preferences file found in {settings.config_dir}")


def _uvicorn(app, bind: str):
    host, _, port = bind.rpartition(":")
    uvicorn.run(app, host=host or "0.0.0.0", port=int(port or 8282),
                log_level=os.environ.get("LOG_LEVEL", "info"), access_log=False)


def cmd_resolver(settings: Settings) -> int:
    from .api_compat import resolver_app
    _uvicorn(resolver_app(settings), settings.resolver_bind)
    return 0


def cmd_scraper(settings: Settings) -> int:
    """Standalone scraper role: HTTP service for candidate search + scoring."""
    from .scraper.service import create_scraper_app
    app = create_scraper_app(settings)
    bind = os.environ.get("SCRAPER_BIND", "0.0.0.0:8283")
    _uvicorn(app, bind)
    return 0


def cmd_web(settings: Settings) -> int:
    """Diagnostics GUI role."""
    from .web.app import create_web_app
    app = create_web_app(settings)
    bind = os.environ.get("WEB_BIND", "0.0.0.0:8285")
    _uvicorn(app, bind)
    return 0


def cmd_vfs(settings: Settings) -> int:
    from .vfs.fs import mount_main
    os.makedirs(settings.vfs_mountpoint, exist_ok=True)
    try:
        mount_main(settings.vfs_mountpoint, settings.resolver_url)
    except RuntimeError as exc:
        logging.getLogger("vfs").error("fuse init failed: %r (device/privileges?)", exc)
        return 1
    return 0


async def cmd_register(settings: Settings, yaml_path: str, url: str | None) -> int:
    import yaml

    from .common.log import event

    with open(yaml_path, "r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    base = url or settings.resolver_url
    created = failed = 0
    async with httpx.AsyncClient(base_url=base, timeout=300.0) as client:
        for item in payload.get("items") or []:
            try:
                resp = await client.post("/media", json=item)
                if resp.status_code == 201:
                    body = resp.json()
                    print(f"registered {item.get('plex_path')} -> {body['id']} "
                          f"status={body['status']} generation={body['generation']}")
                    event("cli_registered", plex_path=item.get("plex_path"))
                    created += 1
                else:
                    print(f"FAILED {item.get('plex_path')}: HTTP {resp.status_code} "
                          f"{resp.text[:200]}", file=sys.stderr)
                    failed += 1
            except httpx.HTTPError as exc:
                print(f"FAILED {item.get('plex_path')}: {exc!r}", file=sys.stderr)
                failed += 1
    print(f"done: {created} registered, {failed} failed")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="plex-scraper", description="one image, four roles")
    parser.add_argument("--url", help="resolver URL for register")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("resolver", help="run the resolver HTTP API (production role)")
    sub.add_parser("scraper", help="run the standalone scraper service (optional role)")
    sub.add_parser("vfs", help="run the FUSE mount (production role)")
    sub.add_parser("web", help="run the diagnostics GUI (read-only)")
    reg = sub.add_parser("register", help="register logical items from YAML")
    reg.add_argument("yaml", help="path to items YAML")

    args = parser.parse_args(argv)
    settings = Settings.from_env()
    setup_logging(settings.log_level)

    if args.command == "resolver":
        return cmd_resolver(settings)
    if args.command == "scraper":
        return cmd_scraper(settings)
    if args.command == "vfs":
        return cmd_vfs(settings)
    if args.command == "web":
        return cmd_web(settings)
    if args.command == "register":
        return asyncio.run(cmd_register(settings, args.yaml, args.url))
    return 2


if __name__ == "__main__":
    sys.exit(main())
