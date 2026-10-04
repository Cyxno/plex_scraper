"""Metadata-only TV show-ID backfill (FASE A1-A7, A14).

Field ownership: schrijft UITSLUITEND show_imdb_id/show_tmdb_id/show_tvdb_id.
Geen status/generation/source/runtime mutatie. Idempotent: items met
show_imdb_id worden overgeslagen. Per-show cache: één Plex-lookup per show.
"""
from __future__ import annotations
import time
from collections import defaultdict


def parse_show_guids(guid_list: list[str]) -> dict:
    out = {"show_imdb_id": None, "show_tmdb_id": None, "show_tvdb_id": None}
    for g in guid_list or []:
        if g.startswith("imdb://"):
            out["show_imdb_id"] = g[7:]
        elif g.startswith("tmdb://"):
            out["show_tmdb_id"] = g[7:]
        elif g.startswith("tvdb://"):
            out["show_tvdb_id"] = g[7:]
    return out


def group_by_show(episodes: list[dict]) -> dict[str, list[dict]]:
    """FASE A2: per-show groepering op Plex show-ratingKey (stabiele key)."""
    groups = defaultdict(list)
    for ep in episodes:
        groups[ep["show_rating_key"]].append(ep)
    return dict(groups)


def apply_show_ids(episodes: list[dict], ids: dict) -> int:
    """Metadata-only toepassing; return aantal werkelijk gewijzigde items."""
    changed = 0
    for ep in episodes:
        new = {k: ids[k] for k in ("show_imdb_id", "show_tmdb_id", "show_tvdb_id")
               if ids.get(k) and ep.get(k) != ids[k]}
        if new:
            ep.update(new)
            changed += 1
    return changed
