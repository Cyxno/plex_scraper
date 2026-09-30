"""Metadata caches with TTL eviction (FASE 13). No media byte cache in v0.1."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class _Entry:
    value: object
    expires_at: float


@dataclass
class TTLCache:
    name: str
    ttl: float
    _data: dict = field(default_factory=dict, init=False)

    def get(self, key: str):
        entry = self._data.get(key)
        if entry is None:
            return None
        if entry.expires_at < time.monotonic():
            self._data.pop(key, None)
            return None
        return entry.value

    def put(self, key: str, value) -> None:
        self._data[key] = _Entry(value, time.monotonic() + self.ttl)
        if len(self._data) > 2048:                       # hard bound, evict oldest
            for k in list(self._data)[:256]:
                self._data.pop(k, None)

    def invalidate(self, key: str) -> None:
        self._data.pop(key, None)

    def size(self) -> int:
        return len(self._data)


class CacheSet:
    def __init__(self, candidates_ttl: float, checkcached_ttl: float, link_ttl: float):
        self.candidates = TTLCache("candidates", candidates_ttl)
        self.checkcached = TTLCache("checkcached", checkcached_ttl)
        self.links = TTLCache("links", link_ttl)

    def stats(self) -> dict:
        return {c.name: {"entries": c.size(), "ttl_s": c.ttl}
                for c in (self.candidates, self.checkcached, self.links)}
