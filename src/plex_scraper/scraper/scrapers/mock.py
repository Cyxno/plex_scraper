"""Scriptable scraper for tests / offline demo (FASE 5)."""
from __future__ import annotations

from .base import Scraper, TorrentCandidate


class MockScraper(Scraper):
    name = "mock"

    def __init__(self, results: dict[str, list[TorrentCandidate]]):
        # key: "movie:tt0133093" or "episode:tt0903747:1:1"
        self.results = results

    @staticmethod
    def key_for(item_key: dict) -> str:
        if item_key.get("kind") == "episode":
            return f"episode:{item_key['imdb_id']}:{item_key['season']}:{item_key['episode']}"
        return f"movie:{item_key['imdb_id']}"

    async def search(self, item_key: dict) -> list[TorrentCandidate]:
        return list(self.results.get(self.key_for(item_key)) or [])
