"""Driver: IntentRunner tegen productie (in de core-container).

  docker exec plex-scraper-core python /app/maintenance/run_arr_intent.py

Dead-detectie zonder D-state-hangs:
  1. readlink-map van ALLE /symlinks-bestanden in 1 exec (geen traversal);
  2. targets onder /mnt/remote/nzbdav/.ids/ = managed (coverage-verified ok);
  3. alleen legacy-targets worden gelezen, in kleine chunks met SIGALRM.
"""
import asyncio
import json
import sys

sys.path.insert(0, "/app/src")
sys.path.insert(0, "/app")

from plex_scraper.ingest.arr import RadarrClient, SonarrClient  # noqa: E402
from plex_scraper.ingest.plex_client import PlexExecClient  # noqa: E402
from maintenance.legacy_arr_intent import (  # noqa: E402
    PLEX_DUMP_SCRIPT,
    IntentRunner,
    read_probe_script,
)

MANAGED_PREFIX = "/mnt/remote/nzbdav/.ids/"

def managed_of(files):
    """files[0] is canonical-.ids-link? (gebruikt readlink-map van de dump)
    — wordt in main() geïnjecteerd via closure over _LINKS."""
    return _LINKS.get((files or [""])[0], "").startswith(MANAGED_PREFIX)

_LINKS: dict = {}
READLINK_SCRIPT = (
    "import json,sys,os\n"
    "paths=json.loads(sys.argv[1])\n"
    "out={}\n"
    "for p in paths:\n"
    "    try:\n"
    "        out[p]=os.readlink(p)\n"
    "    except OSError:\n"
    "        out[p]=''\n"
    "print(json.dumps(out))\n")


async def readlink_map(plex, paths, chunk=500) -> dict:
    out = {}
    for i in range(0, len(paths), chunk):
        res = await plex._exec(READLINK_SCRIPT, json.dumps(paths[i:i+chunk]))
        out.update({k: v for k, v in res.items() if isinstance(v, str)})
    return out


async def probe_map(plex, paths, chunk=8) -> dict:
    """Leesprobe per klein chunk; hangt een chunk (D-state FUSE), dan sterft
    alleen die exec — daarna per-één forbij de deadline."""
    out: dict[str, bool] = {}
    script = read_probe_script()
    i = 0
    while i < len(paths):
        group = paths[i:i+chunk]
        try:
            res = await plex._exec(script, json.dumps(group))
        except (TimeoutError, OSError):
            if len(group) == 1:
                out[group[0]] = False          # hard hangend doel = dead
                i += 1
                continue
            i += 0                              # zelfde groep, fijner verdelen
            chunk = 1
            continue
        out.update({k: bool(v) for k, v in res.items()
                    if isinstance(v, bool)})
        i += len(group)
    return out


async def main() -> None:
    plex = PlexExecClient("plex", section_tv=2, section_movies=1,
                          sock_timeout=90.0)
    skey = open("/run/secrets/sonarr_key", encoding="utf-8").read().strip()
    rkey = open("/run/secrets/radarr_key", encoding="utf-8").read().strip()
    sonarr = SonarrClient("sonarr", "http://192.168.1.2:7854", skey)
    radarr = RadarrClient("radarr", "http://192.168.1.2:7878", rkey)

    dump = await plex._exec(PLEX_DUMP_SCRIPT)
    files = []
    for ep in dump["tv"]:
        files += [f for f in (ep.get("files") or []) if f]
    n_tv = len(files)
    for mv in dump["movies"]:
        files += [f for f in (mv.get("files") or []) if f]
    files = list(dict.fromkeys(files))
    print(f"{len(files)} unieke bestanden ({n_tv} tv)...", flush=True)

    links = await readlink_map(plex, files)
    legacy = [p for p in files if not links.get(p, "").startswith(
        MANAGED_PREFIX)]
    print(f"managed(.ids): {len(files)-len(legacy)}, legacy: {len(legacy)}",
          flush=True)

    results = {p: True for p in files if p not in legacy}
    if legacy:
        pm = await probe_map(plex, legacy)
        results.update(pm)
        print(f"legacy probe: {sum(1 for p in legacy if pm.get(p))} ok / "
              f"{len(legacy)}", flush=True)

    runner = IntentRunner(plex, sonarr, radarr,
                          output_path="/data/legacy-arr-intent.json")
    _LINKS.update(links)
    res = await runner.run(probe_results=results, link_is_managed=managed_of)
    print(json.dumps(res["summary"]["tv"], indent=1))
    print(json.dumps(res["summary"]["movies"], indent=1))
    await sonarr.close()
    await radarr.close()


if __name__ == "__main__":
    asyncio.run(main())
