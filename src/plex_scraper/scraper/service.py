"""Standalone scraper role (optional): HTTP service exposing candidate search
and scoring for diagnostics. The resolver normally uses the scrapers
in-process; this role exists so the `scraper` process-role can run detached.
Read-only: never mutates resolver state."""
from __future__ import annotations

from fastapi import FastAPI, Query

from ..common.config import Settings
from ..common.log import setup_logging
from ..common.scoring.engine import Scorer
from ..scraper.providers.torbox import TorboxProvider
from ..scraper.scrapers.torrentio import TorrentioScraper


def create_scraper_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="plex_scraper scraper", docs_url="/docs")
    scorer = Scorer.from_yaml(_prefs(settings))
    scraper = TorrentioScraper(settings.scraper_torrentio_base)
    provider = TorboxProvider(settings) if settings.torbox_api_token else None

    @app.get("/health")
    async def health():
        return {"status": "ok", "role": "scraper"}

    @app.get("/search")
    async def search(imdb: str = Query(...), season: int | None = None,
                     episode: int | None = None, limit: int = 20):
        kind = "episode" if season is not None else "movie"
        key = {"kind": kind, "imdb_id": imdb}
        if season is not None:
            key.update({"season": season, "episode": episode})
        cands = await scraper.search(key)
        out = []
        for c in cands[:limit]:
            b = scorer.score(c.torrent_name, seeders=c.seeders, size=c.size)
            out.append({"name": c.torrent_name[:70], "hash": c.info_hash[:12],
                        "size_gb": round((c.size or 0)/(1<<30), 2),
                        "seeders": c.seeders, "score": b.total,
                        "rejects": b.rejects})
        return {"count": len(cands), "candidates": out}

    @app.get("/availability")
    async def availability(hashes: str):
        if not provider:
            return {"error": "no TORBOX_API_TOKEN"}
        hs = [h.strip() for h in hashes.split(",") if h.strip()][:100]
        return await provider.availability(hs)

    return app


def _prefs(settings: Settings) -> str:
    import os
    for candidate in ("preferences.yaml", "preferences.example.yaml"):
        path = os.path.join(settings.config_dir, candidate)
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"no preferences file found in {settings.config_dir}")
