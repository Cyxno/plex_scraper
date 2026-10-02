"""Scraper interface (FASE 5). One working adapter (Torrentio) ships, but the
resolver depends only on this contract, so more scrapers can be added."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TorrentCandidate:
    info_hash: str
    torrent_name: str                 # release name (scoring input)
    size: int | None = None
    file_name: str | None = None      # episode file hint inside the torrent
    file_index: int | None = None     # scraper-provided file index, may be None
    seeders: int | None = None
    scraper: str = "?"


class Scraper(ABC):
    name: str = "abstract"

    @abstractmethod
    async def search(self, item_key: dict) -> list[TorrentCandidate]:
        """item_key: {'kind': 'movie'|'episode', 'imdb_id': ..., ...}"""
