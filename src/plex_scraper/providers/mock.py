"""In-memory provider used by tests and offline demos.

Deterministic synthetic bytes per info_hash so range/seek correctness is
verifiable. Failure modes are injectable per hash:

  MockProvider({
      "abc123": {"cached": True, "size": 1_000_000},
      "bad": {"validate_fails": True},
  })
"""
from __future__ import annotations

import hashlib

from .base import DebridProvider, LinkExpiredError, NotReadyError, ProviderError, ProviderTorrent


def synthetic_bytes(info_hash: str, size: int) -> bytes:
    """Deterministic pseudo-random content for a hash (repeatable in tests)."""
    out = bytearray()
    seed = info_hash.encode()
    while len(out) < size:
        seed = hashlib.sha256(seed).digest()
        out.extend(seed)
    return bytes(out[:size])


class MockProvider(DebridProvider):
    name = "mock"

    def __init__(self, specs: dict[str, dict] | None = None, default_size: int = 1 << 20):
        self.specs = specs or {}
        self.default_size = default_size
        self.stream_urls: dict[tuple[int, int], str] = {}
        self.expired_urls: set[str] = set()
        self._tid_to_hash: dict[str, str] = {}

    # ---------------------------------------------------------------- helpers
    def _spec(self, info_hash: str) -> dict:
        return self.specs.setdefault(info_hash, {"cached": False, "size": self.default_size})

    def content(self, info_hash: str) -> bytes:
        return synthetic_bytes(info_hash, self._spec(info_hash).get("size", self.default_size))

    # ------------------------------------------------------- DebridProvider
    async def availability(self, info_hashes: list[str]) -> dict[str, list[dict]]:
        out = {}
        for h in info_hashes:
            spec = self.specs.get(h)
            if spec and spec.get("cached"):
                out[h] = [{"id": 0, "name": spec.get("file_name") or f"{h}.mkv",
                           "size": spec.get("size", self.default_size)}]
        return out

    async def ensure_torrent(self, info_hash: str, torrent_name: str) -> ProviderTorrent:
        spec = self._spec(info_hash)
        if spec.get("validate_fails"):
            raise NotReadyError(f"mock: torrent {info_hash} fails validation")
        if spec.get("validate_failures_left", 0) > 0:
            spec["validate_failures_left"] -= 1
            raise NotReadyError(f"mock: torrent {info_hash} fails (transient)")
        file_id = 0
        files = {file_id: {"name": spec.get("file_name") or f"{torrent_name}.mkv",
                           "size": spec.get("size", self.default_size)}}
        torrent_id = f"t-{abs(hash(info_hash)) % (10 ** 8)}"
        self._tid_to_hash[torrent_id] = info_hash
        return ProviderTorrent(
            provider=self.name, torrent_id=torrent_id,
            info_hash=info_hash, name=torrent_name,
            cached=bool(spec.get("cached")), ready=True, files=files,
        )

    async def get_stream_url(self, torrent_id: int, file_id: int) -> str:
        url = f"mock-stream://{torrent_id}/{file_id}"
        self.stream_urls[(torrent_id, file_id)] = url
        return url

    async def read_range(self, url: str, start: int, length: int) -> bytes:
        if url in self.expired_urls:
            raise LinkExpiredError(url)
        if not url.startswith("mock-stream://"):
            raise ProviderError(f"mock: unknown url {url}")
        _, _, tid, _fid = url.split("/")
        info_hash = self._tid_to_hash.get(tid)
        if info_hash is None:
            raise ProviderError(f"mock: no torrent for url {url}")
        data = self.content(info_hash)
        return data[start:start + length]

    # ------------------------------------------------------- test controls
    def expire_url(self, url: str) -> None:
        self.expired_urls.add(url)
