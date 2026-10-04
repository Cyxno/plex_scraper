"""Play-triggered PLEX_ORPHAN detectie + generieke path-repair helpers.

State-model: PATH_EXISTS ≠ REGISTERED ≠ PLAYABLE. Een broken Plex part
(defecte symlink, legacy target) is PLEX_ORPHAN en mag nooit stil naar
00:00-spinner leiden: repair registreert een nieuw resolver-item (identity
uit Plex-metadata), resolveert een nieuwe TorBox-source en swapt de
symlink atomisch naar de nieuwe .ids-route — Plex pathname ongewijzigd.
"""
from __future__ import annotations
import os, uuid

RESOLVER_PREFIX = "/mnt/remote/nzbdav/"
LEGACY_PREFIXES = ("/mnt/debrid/", "/mnt/infinidysk/", "/mnt/user/debrid/")

REASONS = ("BROKEN_SYMLINK", "MISSING_TARGET", "LEGACY_PATH",
           "UNKNOWN_RESOLVER_MAPPING", "STALE_PART", "AMBIGUOUS")


def classify_part(file_path: str, target: str | None) -> dict:
    """FASE 2: PLEX_ORPHAN-reden bepalen uit pad + symlink-target.
    target=None betekent: geen symlink / defect (readlink faalde)."""
    if target is None:
        return {"orphan": True, "reason": "BROKEN_SYMLINK"}
    if target.startswith(LEGACY_PREFIXES):
        return {"orphan": True, "reason": "LEGACY_PATH",
                "legacy": True}
    if target.startswith(RESOLVER_PREFIX):
        rel = target[len(RESOLVER_PREFIX):]
        return {"orphan": False, "reason": "OK", "resolver_rel": rel}
    return {"orphan": True, "reason": "UNKNOWN_RESOLVER_MAPPING"}


def new_resolver_path() -> str:
    """Nieuwe .ids-route in de huidige architectuur (geen legacy target)."""
    u = str(uuid.uuid4())
    return f".ids/{u[0]}/{u[1]}/{u[2]}/{u[3]}/{u[4]}/{u}"


def episode_identity(grandparent: str | None, season, episode) -> dict | None:
    """FASE 5/6: series-identity; season/episode zijn hard verplicht."""
    if not grandparent or season is None or episode is None:
        return None
    return {"kind": "episode", "series": grandparent,
            "season": int(season), "episode": int(episode)}


def movie_identity(title: str | None, year) -> dict | None:
    if not title:
        return None
    return {"kind": "movie", "title": title, "year": int(year) if year else None}


def registration_payload(identity: dict, duration_s: float | None) -> dict:
    """FASE 4: resolver-registratie-payload uit Plex-identity."""
    payload = dict(identity)
    payload["plex_path"] = new_resolver_path()
    if duration_s:
        payload["duration_s"] = duration_s / 1000.0
    return payload


def atomic_symlink_swap(link_path: str, new_target: str) -> None:
    """FASE 13: atomische swap — Plex pathname blijft gelijk, geen window
    waarin er nergens gewezen wordt. Crash-safe: tmp+rename."""
    tmp = link_path + ".repair-tmp"
    if os.path.lexists(tmp):
        os.remove(tmp)
    os.symlink(new_target, tmp)
    os.replace(tmp, link_path)


def episode_match_ok(cand_name: str, season: int, episode: int) -> bool:
    """FASE 6/27: hard SxxEyy-gate voor repair-kandidaten."""
    import re
    m = re.search(r"[sS](\d{1,2})[eE](\d{1,3})", cand_name or "")
    if not m:
        m2 = re.search(r"(\d{1,2})x(\d{1,3})", cand_name or "")
        if not m2:
            return False
        return int(m2.group(1)) == season and int(m2.group(2)) == episode
    return int(m.group(1)) == season and int(m.group(2)) == episode


def extract_episode_identity(ep_row: dict, season_row: dict,
                             show_row: dict) -> dict | None:
    """FASE 1: Plex hierarchy → identity. episode → parent(season) →
    parent(show). Geen filename-afleiding."""
    if not (ep_row and season_row and show_row):
        return None
    season_no = season_row.get("index")
    ep_no = ep_row.get("index")
    if season_no is None or ep_no is None or not show_row.get("title"):
        return None
    return {"kind": "episode", "series": show_row["title"],
            "season": int(season_no), "episode": int(ep_no),
            "episode_title": ep_row.get("title"),
            "year": show_row.get("year")}


def parse_guids(guids: list[str]) -> dict:
    """FASE 2: imdb://tt… / tmdb://… / tvdb://… normaliseren."""
    out = {"imdb_id": None, "tmdb_id": None, "tvdb_id": None}
    for g in guids or []:
        if g.startswith("imdb://"):
            out["imdb_id"] = g[7:]
        elif g.startswith("tmdb://"):
            out["tmdb_id"] = g[7:]
        elif g.startswith("tvdb://"):
            out["tvdb_id"] = g[7:]
    return out
