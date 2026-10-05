"""Legacy discovery runner: classificeert alle dead-unmanaged links met de
discovery-engine en migreert EXACT-match items via de normale resolver-flow.

Gebruik (in plex-container, heeft Plex + resolver-API):
  python3 run_legacy_discovery.py < classified.json   # dead-links subset
Output: één JSON per regel naar stdout:
  {"link","kind","confidence","state","item_id"?,"plex_path"?,"message"}

States: MIGRATED | BLOCKED_NO_SOURCE | BLOCKED_AMBIGUOUS |
BLOCKED_PLEX_MAPPING | FAILED
"""
import json
import os
import re
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, "/tmp")

from legacy_discovery import (  # noqa: E402
    extract_se, match_episode, match_movie, norm_title)

RES = os.environ.get("RESOLVER_URL", "http://192.168.1.2:18282")
PLEX_BASE = os.environ.get("PLEX_URL", "http://127.0.0.1:32400")


def _token():
    m = re.search(r'PlexOnlineToken="([^"]+)"',
                  open("/config/Plex Media Server/Preferences.xml").read())
    return m.group(1)


def plex(path, tok):
    req = urllib.request.Request(
        f"{PLEX_BASE}{path}",
        headers={"Accept": "application/json", "X-Plex-Token": tok})
    return json.loads(urllib.request.urlopen(req, timeout=90).read())["MediaContainer"]["Metadata"]


def api(method, path, body=None, timeout=300):
    req = urllib.request.Request(f"{RES}{path}", method=method,
                                 data=json.dumps(body).encode() if body else b"",
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def main() -> int:
    import re
    tok = _token()
    data = json.load(sys.stdin)
    dead = [r for r in data if not r.get("readable")]

    movies = plex("/library/sections/1/all?includeGuids=1", tok)
    shows = plex("/library/sections/2/all?includeGuids=1", tok)
    movie_index = {norm_title(md.get("title") or ""):
                   (md.get("title"), md.get("year"), md) for md in movies
                   if norm_title(md.get("title") or "")}
    show_index = {norm_title(md.get("title") or ""):
                  (md.get("title"), md.get("year"), md) for md in shows
                  if norm_title(md.get("title") or "")}

    done_links = set()
    if os.path.exists("/tmp/legacy-done-links.txt"):
        done_links = {l.strip() for l in open("/tmp/legacy-done-links.txt")}

    for r in dead:
        link = r["link"].replace("/mnt/vm_storage/symlinks", "/symlinks", 1)
        if r["link"] in done_links:
            print(json.dumps({"link": r["link"], "state": "ALREADY_DONE"}))
            continue
        dirname = os.path.basename(os.path.dirname(link))
        base = os.path.basename(link)
        ym = re.search(r"(19|20)\d{2}", base)
        year = int(ym.group(0)) if ym else None
        try:
            if "/TV Shows/" in link:
                se = extract_se(base)
                if se is None:
                    print(json.dumps({"link": r["link"],
                                      "state": "BLOCKED_PLEX_MAPPING",
                                      "message": "no SxxEyy in basename"}))
                    continue
                conf, key, _hits = match_episode(show_index, dirname, *se)
                if conf != "EXACT":
                    print(json.dumps({"link": r["link"],
                                      "state": f"BLOCKED_{conf or 'NO_MATCH'}",
                                      "series": dirname, "season": se[0],
                                      "episode": se[1]}))
                    continue
                show = show_index[key][2]
                g = {y["id"].split("://")[0]: y["id"].split("://")[1]
                     for y in show.get("Guid", []) or []}
                eps = plex(f"/library/metadata/{show['ratingKey']}/children", tok)
                season = [s for s in eps if s.get("index") == se[0]][0]
                epm = plex(f"/library/metadata/{season['ratingKey']}/children", tok)
                ep = [e for e in epm if e.get("index") == se[1]][0]
                body = {"kind": "episode", "series": show.get("title"),
                        "title": ep.get("title"), "season": se[0],
                        "episode": se[1],
                        "show_imdb_id": g.get("imdb"),
                        "show_tmdb_id": g.get("tmdb"),
                        "show_tvdb_id": g.get("tvdb")}
            else:
                conf, key, _hits = match_movie(movie_index, dirname, year)
                if conf != "EXACT":
                    print(json.dumps({"link": r["link"],
                                      "state": f"BLOCKED_{conf or 'NO_MATCH'}",
                                      "dirname": dirname}))
                    continue
                md = movie_index[key][2]
                g = {y["id"].split("://")[0]: y["id"].split("://")[1]
                     for y in md.get("Guid", []) or []}
                body = {"kind": "movie", "title": md.get("title"),
                        "year": md.get("year"), "imdb_id": g.get("imdb"),
                        "tmdb_id": g.get("tmdb"), "tvdb_id": g.get("tvdb")}
            nu = str(__import__("uuid").uuid4())
            body["plex_path"] = (f".ids/{nu[0]}/{nu[1]}/{nu[2]}/{nu[3]}/{nu[4]}/{nu}")
            out = api("POST", "/media", body, timeout=30)
            time.sleep(3)
            res = api("POST", f"/media/{out['id']}/resolve", None, timeout=300)
            print(json.dumps({"link": r["link"], "state": res["item"]["status"],
                              "item_id": out["id"],
                              "plex_path": body["plex_path"],
                              "title": body.get("title") or body.get("series")}))
        except Exception as exc:                        # noqa: BLE001
            print(json.dumps({"link": r["link"], "state": "FAILED",
                              "error": repr(exc)[:120]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
