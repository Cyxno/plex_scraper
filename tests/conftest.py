"""Shared fixtures: mock provider + mock scraper + in-memory-ish engine."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from plex_scraper.common.config import Settings  # noqa: E402
from plex_scraper.scraper.providers.mock import MockProvider  # noqa: E402
from plex_scraper.resolver.caches import CacheSet  # noqa: E402
from plex_scraper.resolver.engine import Resolver  # noqa: E402
from plex_scraper.resolver.store import Store  # noqa: E402
from plex_scraper.common.scoring.engine import Scorer  # noqa: E402
from plex_scraper.scraper.scrapers.base import TorrentCandidate  # noqa: E402
from plex_scraper.scraper.scrapers.mock import MockScraper  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
PREFS = REPO_ROOT / "config" / "preferences.example.yaml"


@pytest.fixture
def settings(tmp_path):
    s = Settings(db_path=str(tmp_path / "state.db"), debug=True,
                 cache_bad_ttl=0.05,          # tests control bad_until explicitly
                 cache_bad_ttl_max=0.5,
                 validation_probe_bytes=1024,
                 min_media_movie_mb=0,        # mock fixtures use tiny content
                 min_media_episode_mb=0)
    return s


@pytest.fixture
def scorer():
    return Scorer.from_yaml(str(PREFS))


def make_engine(settings, provider_specs: dict, results: dict, scorer: Scorer | None = None):
    provider = MockProvider(provider_specs)
    scraper = MockScraper(results)
    caches = CacheSet(candidates_ttl=settings.cache_candidates_ttl,
                      checkcached_ttl=settings.cache_checkcached_ttl,
                      link_ttl=settings.cache_link_ttl)
    store = Store(settings.db_path)
    return Resolver(settings, store, provider, [scraper], scorer or Scorer.from_yaml(str(PREFS)), caches), provider, scraper


def cand(info_hash: str, name: str, *, size: int | None = None, seeders: int | None = 50,
         file_name: str | None = None, file_index: int | None = 0) -> TorrentCandidate:
    return TorrentCandidate(info_hash=info_hash, torrent_name=name, size=size,
                            seeders=seeders, file_name=file_name, file_index=file_index)


@pytest.fixture
def got_item():
    """Game of Thrones S01E01 logical item (test set #2)."""
    return {
        "kind": "episode", "title": "Winter Is Coming", "series": "Game of Thrones",
        "season": 1, "episode": 1, "imdb_id": "tt0944947",
        "plex_path": "TV/Game of Thrones/Season 01/Game of Thrones - S01E01.mkv",
    }


def got_results() -> list[TorrentCandidate]:
    return [
        cand("got2160dv", "Game.of.Thrones.S01E01.2160p.DOLBY.VISION.REMUX.TrueHD.7.1.Atmos.HEVC-GRP",
             size=8_000_000_000),
        cand("got1080", "Game.of.Thrones.S01E01.1080p.WEB-DL.DDP5.1.H.264-GRP", size=3_000_000_000),
        cand("got720", "Game.of.Thrones.S01E01.720p.HDTV.x264-GRP", size=900_000_000),
        cand("got3d", "Game.of.Thrones.S01E01.3D.1080p.BluRay.REMUX.AVC-GRP", size=9_000_000_000),
        cand("gotcam", "Game.of.Thrones.S01E01.CAM.x264-GRP", size=700_000_000),
        cand("gotsd", "Game.of.Thrones.S01E01.SDR.WEB-DL.DDP2.0.x264-GRP.GERMAN", size=800_000_000),
    ]


def got_key() -> str:
    return "episode:tt0944947:1:1"
