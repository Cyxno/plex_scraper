"""Debrid provider interface (FASE 4). TorBox-only in v0.1, but every caller
depends on this interface only, so other backends can slot in later."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


class ProviderError(Exception):
    pass


class NotReadyError(ProviderError):
    """Torrent known but not downloadable yet."""


class LinkExpiredError(ProviderError):
    """Stream link invalid -> refresh and retry within the same generation."""


@dataclass
class ProviderTorrent:
    provider: str
    torrent_id: int | str
    info_hash: str
    name: str
    cached: bool
    ready: bool                      # downloadable (cached or finished)
    files: dict[int, dict] = field(default_factory=dict)   # id -> {name, size}


class DebridProvider(ABC):
    name: str = "abstract"

    @abstractmethod
    async def availability(self, info_hashes: list[str]) -> dict[str, list[dict]]:
        """Batch check which hashes are cached on the provider's servers.
        Returns {hash: [file dicts with id/name/size]} for cached hashes only."""

    @abstractmethod
    async def ensure_torrent(self, info_hash: str, torrent_name: str) -> ProviderTorrent:
        """Make sure the torrent exists on the account and is ready.
        Bounded: adding + polling with timeouts; raises NotReadyError otherwise."""

    @abstractmethod
    async def get_stream_url(self, torrent_id: int, file_id: int) -> str:
        """A (possibly short-lived) URL serving the file's bytes with Range."""

    @abstractmethod
    async def read_range(self, url: str, start: int, length: int) -> bytes:
        """HTTP Range read. Raises LinkExpiredError on 403/410."""
