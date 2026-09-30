"""Wire-format helpers for API responses."""
from __future__ import annotations

from ..domain import models as m


def item_out(item: m.MediaItem) -> dict:
    return {
        "id": item.id, "kind": item.kind, "title": item.title,
        "series": item.series, "season": item.season, "episode": item.episode,
        "year": item.year, "imdb_id": item.imdb_id, "tmdb_id": item.tmdb_id,
        "tvdb_id": item.tvdb_id, "plex_path": item.plex_path,
        "status": item.status, "generation": item.generation,
        "desired": item.desired,
    }


def source_out(src: m.Source | None) -> dict | None:
    if src is None:
        return None
    return {
        "id": src.id, "generation": src.generation, "provider": src.provider,
        "info_hash": src.info_hash, "torrent_name": src.torrent_name,
        "file_id": src.file_id, "file_name": src.file_name, "size": src.size,
        "resolution": src.resolution, "codec": src.codec, "hdr": src.hdr,
        "audio": src.audio, "language": src.language, "release_type": src.release_type,
        "seeders": src.seeders, "cached": src.cached, "score": src.score,
        "score_breakdown": src.score_json, "state": src.state,
        "failure_count": src.failure_count,
        "bad_until": src.bad_until or None, "last_verified": src.last_verified or None,
    }
