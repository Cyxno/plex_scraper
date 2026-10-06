"""Legacy Arr-intent-classificatie (Phases 34-35, ingest-hardening 2026-10-06).

Vraag per legacy-item (dead óf working): wat WIL de arr ermee?

  dead + arr monitored + arr hasFile   → MONITORED_PRESENT_RUNTIME_DEAD
  dead + arr monitored + arr !hasFile  → MONITORED_MISSING
  dead + arr unmonitored               → UNMONITORED
  geen arr-match                       → NO_ARR_MATCH
  working-equivalenten                 → MONITORED_WORKING_LEGACY /
                                         UNMONITORED_WORKING_LEGACY /
                                         NO_ARR_MATCH

NEOIT automatisch opnieuw downloaden: dit is recovery-prioritering, niet
actie. Output: /data/legacy-arr-intent.json + samenvatting.

Run (in de core-container):
  python -m plex_scraper.cli ... # nee — direct:
  docker exec plex-scraper-core python -m maintenance_runner  # zie runner()
Dit module is ook te importeren voor de pure classificatie (getest).
"""
from __future__ import annotations

import json
import re
import time

OUTPUT_PATH = "/data/legacy-arr-intent.json"


def classify_intent(dead: bool, arr_has_file: bool | None,
                    arr_monitored: bool | None) -> str:
    """Pure classificatie (Phase 34/35). arr_monitored=None = geen arr-match."""
    if arr_monitored is None:
        return "NO_ARR_MATCH"
    if dead:
        if arr_has_file:
            return "MONITORED_PRESENT_RUNTIME_DEAD"
        return "MONITORED_MISSING" if arr_monitored else "UNMONITORED"
    if arr_monitored:
        return "MONITORED_WORKING_LEGACY"
    return "UNMONITORED_WORKING_LEGACY"


def recovery_priority(cls: str) -> int:
    """Prioriteit: runtime-dead waar de arr het wil → eerst; daarna echte
    missing; onmonitored/no-match als laatste."""
    return {
        "MONITORED_PRESENT_RUNTIME_DEAD": 0,
        "MONITORED_MISSING": 1,
        "BLOCKED_NO_SOURCE": 2,
        "MONITORED_WORKING_LEGACY": 3,
        "UNMONITORED": 4,
        "UNMONITORED_WORKING_LEGACY": 5,
        "NO_ARR_MATCH": 6,
    }.get(cls, 9)


def norm_title(t: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (t or "").lower())


def match_series(series_rows: list[dict], show_tvdb: int | None,
                 show_title: str | None) -> dict | None:
    if show_tvdb:
        for s in series_rows:
            if int(s.get("tvdbId") or 0) == int(show_tvdb):
                return s
    want = norm_title(show_title)
    if want:
        for s in series_rows:
            if norm_title(s.get("title")) == want:
                return s
    return None


def match_movie(movie_rows: list[dict], imdb: str | None,
                tmdb: str | None, title: str | None,
                year: int | None = None) -> dict | None:
    if imdb:
        for mv in movie_rows:
            if (mv.get("imdbId") or "").lower() == imdb.lower():
                return mv
    if tmdb:
        for mv in movie_rows:
            if str(mv.get("tmdbId") or "") == str(tmdb):
                return mv
    want = norm_title(title)
    if want:
        cands = [mv for mv in movie_rows if norm_title(mv.get("title")) == want]
        if len(cands) == 1:
            return cands[0]
        if year:
            exact = [mv for mv in cands if int(mv.get("year") or 0) == int(year)]
            if exact:
                return exact[0]
    return None


PLEX_DUMP_SCRIPT = r"""
import json, re, sys, urllib.request
tok = re.search(r'PlexOnlineToken="([^"]+)"',
    open('/config/Plex Media Server/Preferences.xml').read()).group(1)
def get(url):
    req = urllib.request.Request(url, headers={
        'Accept': 'application/json', 'X-Plex-Token': tok})
    return json.loads(urllib.request.urlopen(req, timeout=120).read())['MediaContainer'].get('Metadata') or []
out = {"tv": [], "movies": []}
shows = get('http://127.0.0.1:32400/library/sections/2/all')
for s in shows:
    guid = {}
    for g in s.get('Guid') or []:
        p = g['id'].split('://')
        guid[p[0]] = p[1]
    try:
        leaves = get(f"http://127.0.0.1:32400/library/metadata/{s['ratingKey']}/allLeaves")
    except Exception:
        continue
    for ep in leaves:
        files = []
        for med in ep.get('Media') or []:
            for pt in med.get('Part') or []:
                files.append(pt.get('file'))
        out["tv"].append({"show": s.get('title'), "tvdb": guid.get('tvdb'),
                          "imdb": guid.get('imdb'),
                          "season": ep.get('parentIndex'),
                          "episode": ep.get('index'),
                          "title": ep.get('title'), "files": files})
movies = get('http://127.0.0.1:32400/library/sections/1/all')
for mv in movies:
    guid = {}
    for g in mv.get('Guid') or []:
        p = g['id'].split('://')
        guid[p[0]] = p[1]
    files = []
    for med in mv.get('Media') or []:
        for pt in med.get('Part') or []:
            files.append(pt.get('file'))
    out["movies"].append({"title": mv.get('title'), "year": mv.get('year'),
                          "imdb": guid.get('imdb'), "tmdb": guid.get('tmdb'),
                          "files": files})
print(json.dumps(out))
"""


def read_probe_script(path: str) -> str:
    return (
        "import json,sys,os\n"
        "p=sys.argv[1]\n"
        "try:\n"
        "    with open(p,'rb') as fh: d=fh.read(65536)\n"
        "    print(json.dumps({'ok': bool(d)}))\n"
        "except OSError as e:\n"
        "    print(json.dumps({'ok': False, 'err': repr(e)[:80]}))\n")


class IntentRunner:
    """Verzamelt Plex-populatie + arr-intent en classificeert."""

    def __init__(self, plex_client, sonarr=None, radarr=None,
                 output_path: str = OUTPUT_PATH):
        self.plex = plex_client
        self.sonarr = sonarr
        self.radarr = radarr
        self.output_path = output_path

    async def run(self, limit_probes: int | None = None) -> dict:
        dump = await self.plex._exec(PLEX_DUMP_SCRIPT)
        if "tv" not in dump:
            raise RuntimeError(f"plex dump failed: {str(dump)[:160]}")
        sonarr_series = await self.sonarr.series_all() if self.sonarr else []
        sonarr_eps = {}
        if self.sonarr:
            async for rec, s in self.sonarr.wanted_missing():
                pass  # wanted/missing alleen voor prioriteit; hasFile via series
        radarr_movies = await self.radarr.movies_all() if self.radarr else []
        # sonarr episode-hasFile-map (via per-series episodefetch is duur;
        # gebruik de wanted/missing-lijst + file-count heuristiek:
        # episodefile-aanwezigheid vragen we per match op met episodefetch
        # alleen voor dead-episodes (begrensd).
        results = {"generated_at": time.time(), "tv": [], "movies": [],
                   "summary": {}}
        dead_tv = 0
        for ep in dump["tv"]:
            files = [f for f in (ep.get("files") or []) if f]
            dead = not files
            if not dead and limit_probes is None:
                # dead-check via leesprobe op het eerste bestand
                probe = await self.plex._exec(read_probe_script(files[0]))
                dead = not probe.get("ok")
            s = match_series(sonarr_series, _int_or_none(ep.get("tvdb")),
                             ep.get("show"))
            arr_monitored = None
            arr_has_file = None
            if s is not None:
                arr_monitored = bool(s.get("monitored"))
                arr_has_file = False        # exact per-episode hieronder
                if not dead and self.sonarr is not None:
                    arr_has_file = True     # leesbaar bestand = present genoeg
                elif self.sonarr is not None:
                    arr_has_file = await self._sonarr_has_file(s, ep)
            cls = classify_intent(dead, arr_has_file, arr_monitored)
            if dead:
                dead_tv += 1
            results["tv"].append({
                "show": ep.get("show"), "season": ep.get("season"),
                "episode": ep.get("episode"), "tvdb": ep.get("tvdb"),
                "dead": dead, "arr": (s or {}).get("title"),
                "arr_monitored": arr_monitored, "arr_has_file": arr_has_file,
                "classification": cls,
                "priority": recovery_priority(cls),
                "file": (files[0][-100:] if files else None)})
        for mv in dump["movies"]:
            files = [f for f in (mv.get("files") or []) if f]
            dead = not files
            if not dead:
                probe = await self.plex._exec(read_probe_script(files[0]))
                dead = not probe.get("ok")
            rec = match_movie(radarr_movies, mv.get("imdb"),
                              str(mv.get("tmdb") or ""), mv.get("title"),
                              mv.get("year"))
            arr_monitored = None if rec is None else bool(rec.get("monitored"))
            arr_has_file = None if rec is None else bool(rec.get("hasFile"))
            cls = classify_intent(dead, arr_has_file, arr_monitored)
            results["movies"].append({
                "title": mv.get("title"), "year": mv.get("year"),
                "imdb": mv.get("imdb"), "dead": dead,
                "arr": (rec or {}).get("title"),
                "arr_monitored": arr_monitored, "arr_has_file": arr_has_file,
                "classification": cls,
                "priority": recovery_priority(cls),
                "file": (files[0][-100:] if files else None)})
        results["summary"] = _summary(results)
        with open(self.output_path, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=1)
        return results

    async def _sonarr_has_file(self, series: dict, ep: dict) -> bool:
        """Exacte episode-hasFile via episodes-fetch van de serie (begrensd:
        alleen voor dead-items zodat een gezonde library geen 1000 calls doet)."""
        try:
            eps = await self.sonarr._get(
                f"/api/v3/episode?seriesId={series['id']}")
            for e in eps:
                if (e.get("seasonNumber") == ep.get("season")
                        and e.get("episodeNumber") == ep.get("episode")):
                    return bool(e.get("hasFile"))
        except Exception:                            # noqa: BLE001
            return False
        return False


def _int_or_none(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _summary(results: dict) -> dict:
    out = {}
    for key in ("tv", "movies"):
        counts: dict[str, int] = {}
        for it in results[key]:
            counts[it["classification"]] = counts.get(it["classification"], 0) + 1
        out[key] = counts
    out["priority_order"] = [
        {"show": it.get("show") or it.get("title"), **{
            k: it.get(k) for k in ("season", "episode", "dead", "classification")}}
        for it in sorted(results["tv"] + results["movies"],
                         key=lambda x: (x["priority"], x.get("show") or ""))
        if it["dead"]][:25]
    return out
