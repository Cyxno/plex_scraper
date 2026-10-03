"""Domain model (FASE 2).

The load-bearing distinction of the whole project:

  * MediaItem   — permanent logical identity (stable plex_path)
  * Source      — disposable backing (torrent/file on a debrid provider)
  * Session     — a pinned (item, source-generation) pair for one open handle

Torrent hash is never a media identity.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()


class ItemStatus(str, Enum):
    NO_SOURCE = "NO_SOURCE"
    RESOLVING = "RESOLVING"
    CANDIDATE_VALIDATION = "CANDIDATE_VALIDATION"
    READY = "READY"
    SOURCE_FAILED = "SOURCE_FAILED"


class SourceState(str, Enum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    FAILED = "failed"
    RETIRED = "retired"


class SessionState(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class MediaItem:
    id: str
    kind: str                    # "movie" | "episode"
    title: str
    plex_path: str               # relative to VFS mount; NEVER changes
    series: str | None = None
    season: int | None = None
    episode: int | None = None
    year: int | None = None
    imdb_id: str | None = None
    tmdb_id: int | None = None
    tvdb_id: int | None = None
    status: str = ItemStatus.NO_SOURCE.value
    generation: int = 0
    desired: dict = field(default_factory=dict)   # quality intent (FASE 15)
    duration_s: float | None = None               # mediaduurtijd (Plex/derive)
    media_bitrate_mbit: float | None = None       # totale container-bitrate
    created_at: float = field(default_factory=now)
    updated_at: float = field(default_factory=now)

    def search_key(self) -> dict:
        """What scrapers need to find this item."""
        if self.kind == "episode":
            return {
                "kind": "episode",
                "imdb_id": self.imdb_id,
                "series": self.series,
                "season": self.season,
                "episode": self.episode,
            }
        return {"kind": "movie", "imdb_id": self.imdb_id, "title": self.title, "year": self.year}


@dataclass
class Source:
    id: str
    media_item_id: str
    generation: int                       # item.generation at activation
    provider: str                         # "torbox"
    info_hash: str
    torrent_name: str
    file_id: int | None = None
    file_name: str | None = None
    size: int = 0
    resolution: str | None = None
    codec: str | None = None
    hdr: str | None = None
    audio: str | None = None
    language: str | None = None
    release_type: str | None = None
    seeders: int | None = None
    cached: bool = False
    score: float | None = None
    score_json: dict = field(default_factory=dict)   # transparent breakdown
    state: str = SourceState.CANDIDATE.value
    failure_count: int = 0
    bad_until: float = 0.0                # temporary bad-until (TTL backoff)
    last_verified: float = 0.0
    created_at: float = field(default_factory=now)

    def is_bad(self, at: float | None = None) -> bool:
        return self.bad_until > (at or now())


@dataclass
class Session:
    handle: str
    media_item_id: str
    source_id: str
    generation: int
    size: int
    state: str = SessionState.OPEN.value
    opened_at: float = field(default_factory=now)
    closed_at: float | None = None
    read_count: int = 0


@dataclass
class ScoreLine:
    label: str
    points: float


@dataclass
class ScoreBreakdown:
    """Transparent scoring (FASE 3): a number is never enough."""
    total: float = 0.0
    lines: list[ScoreLine] = field(default_factory=list)
    rejects: list[str] = field(default_factory=list)   # exclusion reasons

    @property
    def rejected(self) -> bool:
        return bool(self.rejects)
