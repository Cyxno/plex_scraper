"""Offline demo seed: wires MockProvider/MockScraper with deterministic
synthetic content for the example test set. Used when no TORBOX_API_TOKEN is
configured (DEMO_SEED=true) — proves the full stack without any provider."""
from __future__ import annotations

import os

from plex_scraper.scraper.scrapers.base import TorrentCandidate
from .mock import MockProvider

# info_hash -> provider spec (sizes differ so generation switches are visible)
DEMO_SPECS = {
    "demo2160dv": {"cached": True, "size": 32 << 20},
    "demo1080": {"cached": True, "size": 16 << 20},
    "demo720": {"cached": True, "size": 8 << 20},
}

_ITEM_TEMPLATES = [
    ("episode:tt0903747:1:1", "Breaking.Bad.S01E01.{q}.{ext}-GRP"),
    ("episode:tt0944947:1:1", "Game.of.Thrones.S01E01.{q}.{ext}-GRP"),
    ("movie:tt0133093", "Fight.Club.1999.{q}.{ext}-GRP"),
    ("movie:tt0137523", "The.Matrix.1999.{q}.{ext}-GRP"),
    ("movie:tt15239678", "Dune.Part.Two.2024.{q}.{ext}-GRP"),
]

_QUALITIES = [
    ("2160p.DOLBY.VISION.WEB-DL.DDP5.1.Atmos.HEVC", "demo2160dv", 40_000_000),
    ("1080p.WEB-DL.DDP5.1.H.264", "demo1080", 4_000_000),
    ("720p.HDTV.x264", "demo720", 1_000_000),
]


def _fixture_path() -> str:
    return os.environ.get("DEMO_FIXTURE", "/data/demo-fixture.mkv")


def seeded_provider() -> MockProvider:
    specs = {**DEMO_SPECS, "demo2160dv": {**DEMO_SPECS["demo2160dv"]}}
    fixture = _fixture_path()
    if os.path.exists(fixture):
        specs["demoreal"] = {"cached": True, "file_path": fixture,
                             "file_name": "real.media.mkv"}
    return MockProvider(specs)


def seeded_scrapers() -> list:
    from ..scrapers.mock import MockScraper

    fixture = _fixture_path()
    real_size = os.path.getsize(fixture) if os.path.exists(fixture) else 0
    results: dict[str, list[TorrentCandidate]] = {}
    for key, template in _ITEM_TEMPLATES:
        cands = []
        if real_size:
            cands.append(TorrentCandidate(
                info_hash="demoreal",
                torrent_name=template.format(
                    q="2160p.DOLBY.VISION.WEB-DL.TrueHD.7.1.Atmos.HEVC", ext="MKV"),
                size=real_size, seeders=99,
                file_name="real.media.mkv", file_index=0,
            ))
        for i, (quality, info_hash, size) in enumerate(_QUALITIES):
            cands.append(TorrentCandidate(
                info_hash=info_hash,
                torrent_name=template.format(q=quality, ext="MKV"),
                size=size, seeders=50,
                file_name=template.format(q=quality, ext="mkv"),
                file_index=0,
            ))
        results[key] = cands
    return [MockScraper(results)]
