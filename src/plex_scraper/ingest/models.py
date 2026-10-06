"""Ingest-domeinmodellen (Phases 10-11): persistente wanted-queue.

Eén job = één arr wanted/missing-item (episode óf film). Dedupe via
`dedupe_key` (UNIQUE): TV = show-identiteit + S:E, movie = IMDb/TMDb.
Herhaalde arr-events (webhook én reconcile) landen dus altijd op dezelfde
rij — nooit dubbele resolver-items of dubbele provider-searches.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


def new_job_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()


class JobState(str, Enum):
    QUEUED = "QUEUED"
    IDENTITY_VERIFYING = "IDENTITY_VERIFYING"
    REGISTERING = "REGISTERING"
    RESOLVING = "RESOLVING"
    PROVIDER_WAIT = "PROVIDER_WAIT"
    READY = "READY"
    DELIVERING = "DELIVERING"
    PLEX_REFRESH = "PLEX_REFRESH"
    COMPLETED = "COMPLETED"
    BLOCKED_IDENTITY = "BLOCKED_IDENTITY"
    BLOCKED_NO_SOURCE = "BLOCKED_NO_SOURCE"
    BLOCKED_MAPPING = "BLOCKED_MAPPING"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_FINAL = "FAILED_FINAL"


TERMINAL_STATES = frozenset({
    JobState.COMPLETED.value,
    JobState.BLOCKED_IDENTITY.value,
    JobState.BLOCKED_NO_SOURCE.value,
    JobState.BLOCKED_MAPPING.value,
    JobState.FAILED_FINAL.value,
})

# states waar de worker weer uit kan (retrybaar)
ACTIVE_STATES = frozenset({
    JobState.QUEUED.value,
    JobState.IDENTITY_VERIFYING.value,
    JobState.REGISTERING.value,
    JobState.RESOLVING.value,
    JobState.PROVIDER_WAIT.value,
    JobState.READY.value,
    JobState.DELIVERING.value,
    JobState.PLEX_REFRESH.value,
    JobState.FAILED_RETRYABLE.value,
})


@dataclass
class IngestJob:
    source: str                       # "sonarr" | "radarr"
    arr_item_id: str                  # sonarr: seriesId:episodeId — radarr: movieId
    kind: str                         # "episode" | "movie"
    dedupe_key: str                   # tv: tv:<imdb|tvdb>:S:E — movie: movie:<imdb|tmdb>
    title: str = ""
    series: str | None = None
    season: int | None = None
    episode: int | None = None
    year: int | None = None
    show_imdb_id: str | None = None
    show_tvdb_id: str | None = None
    show_tmdb_id: str | None = None
    imdb_id: str | None = None
    tmdb_id: str | None = None
    monitored: bool = True
    wanted: bool = True
    arr_path: str | None = None       # sonarr-seriespath /media/Lanterns (voor symlink)
    air_date_utc: str | None = None   # prioriteitsvolgorde (recent eerst)
    enqueue_reason: str = "reconcile"  # webhook | reconcile | manual
    status: str = JobState.QUEUED.value
    attempts: int = 0
    next_attempt_at: float = 0.0
    provider_block: str | None = None
    last_error: str | None = None
    resolver_item_id: str | None = None
    delivered_symlink: str | None = None
    id: str = field(default_factory=new_job_id)
    created_at: float = field(default_factory=now)
    updated_at: float = field(default_factory=now)
    completed_at: float | None = None

    @staticmethod
    def tv_dedupe_key(show_imdb: str | None, show_tvdb: str | None,
                      season: int, episode: int) -> str:
        """Sterke TV-key: voorkeur IMDb (cross-bron), fallback TVDb."""
        if show_imdb:
            return f"tv:imdb:{show_imdb}:{season}:{episode}"
        return f"tv:tvdb:{show_tvdb}:{season}:{episode}"

    @staticmethod
    def movie_dedupe_key(imdb: str | None, tmdb: str | None) -> str:
        if imdb:
            return f"movie:imdb:{imdb}"
        return f"movie:tmdb:{tmdb}"
