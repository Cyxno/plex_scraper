"""CLI: `resolver`, `vfs`, `register` (FASE 1/18 entrypoints)."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import httpx
import uvicorn

from .api.app import create_app
from .config import Settings
from .log import setup_logging
from .providers.mock import MockProvider
from .providers.torbox import TorboxProvider
from .resolver.caches import CacheSet
from .resolver.engine import Resolver
from .resolver.store import Store
from .scoring.engine import Scorer
from .scrapers.mock import MockScraper
from .scrapers.torrentio import TorrentioScraper


def build_resolver(settings: Settings) -> Resolver:
    store = Store(settings.db_path)
    os.makedirs(os.path.dirname(settings.db_path) or ".", exist_ok=True)
    scorer = Scorer.from_yaml(_preferences_path(settings))
    caches = CacheSet(
        candidates_ttl=settings.cache_candidates_ttl,
        checkcached_ttl=settings.cache_checkcached_ttl,
        link_ttl=settings.cache_link_ttl,
    )
    if settings.torbox_api_token or os.environ.get("TORBOX_API_TOKEN"):
        provider = TorboxProvider(settings)
    else:
        # PoC convenience: run without a token -> seeded mocks (demo/test only)
        from .providers.demo_seed import seeded_provider, seeded_scrapers
        logging.getLogger("resolver").warning(
            "TORBOX_API_TOKEN not set — using seeded mock provider/scraper "
            "(offline demo, synthetic bytes, no real content)")
        provider = seeded_provider()
        return Resolver(settings, store, provider, seeded_scrapers(), scorer, caches)
    scrapers: list = [TorrentioScraper(settings.scraper_torrentio_base)]
    return Resolver(settings, store, provider, scrapers, scorer, caches)


def _preferences_path(settings: Settings) -> str:
    for candidate in ("preferences.yaml", "preferences.example.yaml"):
        path = os.path.join(settings.config_dir, candidate)
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"no preferences file found in {settings.config_dir}")


def cmd_resolver(settings: Settings) -> int:
    app = create_app(build_resolver(settings), settings)
    host, _, port = settings.resolver_bind.rpartition(":")
    uvicorn.run(app, host=host or "0.0.0.0", port=int(port or 8282),
                log_level=settings.log_level, access_log=False)
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
                    created += 1
                else:
                    print(f"FAILED {item.get('plex_path')}: HTTP {resp.status_code} {resp.text[:200]}",
                          file=sys.stderr)
                    failed += 1
            except httpx.HTTPError as exc:
                print(f"FAILED {item.get('plex_path')}: {exc!r}", file=sys.stderr)
                failed += 1
    print(f"done: {created} registered, {failed} failed")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="plex-scraper")
    parser.add_argument("--url", help="resolver URL for register (default $RESOLVER_URL or localhost)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("resolver", help="run the resolver HTTP API")
    sub.add_parser("vfs", help="run the FUSE mount")
    reg = sub.add_parser("register", help="register logical items from YAML")
    reg.add_argument("yaml", help="path to items YAML (e.g. config/testset.example.yaml)")

    args = parser.parse_args(argv)
    settings = Settings.from_env()
    setup_logging(settings.log_level)

    if args.command == "resolver":
        return cmd_resolver(settings)
    if args.command == "vfs":
        return cmd_vfs(settings)
    if args.command == "register":
        return asyncio.run(cmd_register(settings, args.yaml, args.url))
    return 2


if __name__ == "__main__":
    sys.exit(main())
