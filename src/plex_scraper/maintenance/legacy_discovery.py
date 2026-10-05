"""Deterministic discovery-engine voor legacy links zonder basename-match.

Normalization is DISCOVERY-ONLY: mapping is EXACT pas bij unieke
genormaliseerde titel + jaar + aanwezige Plex-GUIDs (P6), daarna
identity_guard. AMBIGUOUS/NO_MATCH worden nooit automatisch gemigreerd.
"""
from __future__ import annotations

import re

_RELEASE_TAGS = re.compile(
    r"\b(2160p|1080p|720p|4k|uhd|bluray|blu-ray|brremux|remux|web[- ]?dl|webrip|"
    r"web|hdtv|dvdrip|hdr10?(\+)?|dolby.?vision|dv|hdr|hevc|h\.?26[45]|x26[45]|"
    r"avc|truehd|atmos|ddp?5?\.?[01]?|dts(-hd)?( ma| x)?|aac[0-9.]*|ac3|flac|"
    r"mult[i|i]|sub(s)?|dual|audio|ita|eng|nld|spa|fre|ger|proper|repack|extended|"
    r"unrated|remastered|imax|hybrid|northma|_group)\b", re.I)
_YEAR = re.compile(r"(19|20)\d{2}")
_SE = re.compile(r"\bS(\d{1,2})E(\d{1,3})\b", re.I)


def norm_title(title: str) -> str:
    """PHASE 13: discovery-normalisatie (nooit authority)."""
    t = (title or "").casefold()
    t = t.replace(":", " ").replace("-", " ").replace("'", "").replace("_", " ")
    t = t.replace(".", " ")
    t = _YEAR.sub(" ", t)
    t = _RELEASE_TAGS.sub(" ", t)
    t = re.sub(r"\[.*?\]|\(.*?\)", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def extract_year(text: str) -> int | None:
    m = _YEAR.search(text or "")
    return int(m.group(0)) if m else None


def extract_se(basename: str) -> tuple[int, int] | None:
    m = _SE.search(basename or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _prefix_hit(nt: str, t: str) -> bool:
    """Plex-titel (genormaliseerd) is een prefix van de release op woordgrens
    (release-namen zijn <canonieke titel> + technische suffixen). Minimale
    lengte 4 tegen vals-positieven als 'it'/'up'."""
    t = norm_title(t)
    return len(t) >= 4 and (nt == t or nt.startswith(t + " "))


def match_movie(norm_plex: dict[str, tuple], title: str, year: int | None):
    """Retourneer (confidence, key_of_match, candidates).

    EXACT: unieke Plex-titel die als woordgrens-prefix van de genormaliseerde
    release staat (+ jaar-overeenkomst indien bekend). Meerdere -> AMBIGUOUS;
    geen -> NO_MATCH.
    """
    nt = norm_title(title)
    hits = [k for k, (t, _y, _g) in norm_plex.items() if _prefix_hit(nt, t)]
    if year is not None and len(hits) > 1:
        year_hits = [k for k in hits if norm_plex[k][1] in (None, year)]
        if year_hits:
            hits = year_hits
    if len(hits) == 1:
        return "EXACT", hits[0], hits
    if len(hits) > 1:
        return "AMBIGUOUS", None, hits
    return "NO_MATCH", None, []


def match_episode(plex_shows: dict[str, tuple], series: str, season: int, episode: int):
    """TV: exacte show (genormaliseerd) + S/E. Unieke show -> EXACT."""
    ns = norm_title(series)
    hits = [k for k, (t, _y, _g) in plex_shows.items() if norm_title(t) == ns]
    if len(hits) == 1:
        return "EXACT", hits[0], hits
    if len(hits) > 1:
        return "AMBIGUOUS", None, hits
    return "NO_MATCH", None, []
