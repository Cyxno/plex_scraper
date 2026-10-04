"""Productie-runner: TV show-ID backfill (metadata-only).

Per unieke serie: één Plex API show-lookup (includeGuids=1) → show IMDb/
TMDb/TVDb → persist op alle episodes van die serie. Geen provider-search,
geen status/generation/source mutatie. Idempotent: episodes met
show_imdb_id worden overgeslagen.

Gebruik (in plex-container):
  python3 run_show_backfill.py --dry-run [--limit-shows N] [--series TITEL]
  python3 run_show_backfill.py --apply   [--limit-shows N] [--series TITEL]
"""
import argparse, json, re, sqlite3, sys, time
import urllib.parse, urllib.request

PLEX_DB = "/config/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db"
PLEX_API = "http://127.0.0.1:32400"
RESOLVER = "http://192.168.1.2:18282"
GUID_RE = re.compile(r"tt\d{7,10}")


def plex_token():
    m = re.search(r'PlexOnlineToken="([^"]+)"',
                  open("/config/Plex Media Server/Preferences.xml").read())
    return m.group(1) if m else None


def api_show_guids(rating_key, token):
    req = urllib.request.Request(
        f"{PLEX_API}/library/metadata/{rating_key}?includeGuids=1",
        headers={"Accept": "application/json", "X-Plex-Token": token})
    d = json.loads(urllib.request.urlopen(req, timeout=15).read())
    md = d["MediaContainer"]["Metadata"][0]
    out = {"show_imdb_id": None, "show_tmdb_id": None, "show_tvdb_id": None}
    for g in md.get("Guid", []) or []:
        i = g["id"]
        if i.startswith("imdb://"):
            out["show_imdb_id"] = i[7:]
        elif i.startswith("tmdb://"):
            out["show_tmdb_id"] = i[7:]
        elif i.startswith("tvdb://"):
            out["show_tvdb_id"] = i[7:]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit-shows", type=int, default=0)
    ap.add_argument("--series", default=None)
    args = ap.parse_args()
    if args.apply == args.dry_run:
        print("kies --apply OF --dry-run"); sys.exit(2)

    token = plex_token()
    if not token:
        print("FATAAL: geen PlexOnlineToken"); sys.exit(1)

    db = sqlite3.connect("file:" + PLEX_DB + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    shows = {}
    for r in db.execute("""SELECT mi.id episode_rk, mi.parent_id season_id,
                                  md.parent_id show_id, mi.title etitle
                           FROM metadata_items md
                           JOIN media_items mi ON mi.metadata_item_id = md.id
                           WHERE md.episode_index IS NOT NULL"""):
        sid = r["show_id"]
        if sid:
            shows.setdefault(sid, {"rk": sid, "episodes": []})["episodes"].append(r)
    print(f"unieke shows in Plex: {len(shows)}")

    # resolver episodes zonder show_imdb_id groeperen op series-titel
    core = sqlite3.connect("/data/state.db"); core.row_factory = sqlite3.Row
    todo = {}
    for r in core.execute("""SELECT id, series, season, episode, show_imdb_id
                             FROM media_items WHERE kind='episode'
                             AND show_imdb_id IS NULL AND series IS NOT NULL"""):
        todo.setdefault(r["series"], []).append(r)
    print(f"resolver episodes zonder show_imdb_id: {sum(len(v) for v in todo.values())} "
          f"in {len(todo)} series")

    updated = noop = unavailable = conflict = errors = 0
    processed = 0
    for series, eps in sorted(todo.items()):
        if args.limit_shows and processed >= args.limit_shows:
            break
        if args.series and args.series.lower() not in series.lower():
            continue
        processed += 1
        srow = db.execute(
            "SELECT id, title, year FROM metadata_items "
            "WHERE parent_id IS NULL AND title=?", (series,)).fetchone()
        if srow is None:
            unavailable += 1
            print(f"  SHOW_GUID_UNAVAILABLE (geen Plex show-row): {series[:40]}")
            continue
        try:
            guids = api_show_guids(srow["id"], token)
        except Exception as e:
            errors += 1
            print(f"  ERROR {series[:40]}: {e!r}"); continue
        if not any(guids.values()):
            unavailable += 1
            print(f"  SHOW_GUID_UNAVAILABLE: {series[:40]}"); continue
        if args.dry_run:
            print(f"  DRY {series[:40]}: {guids} → {len(eps)} episodes")
            continue
        # metadata-only PATCH per episode (update_item persisteert show-cols)
        n = 0
        for ep in eps:
            body = json.dumps({k: guids[k] for k in guids if guids[k]}).encode()
            req = urllib.request.Request(
                f"{RESOLVER}/media/{ep['id']}", data=body,
                headers={"Content-Type": "application/json"}, method="PATCH")
            try:
                urllib.request.urlopen(req, timeout=30)
                n += 1
            except Exception as e:
                errors += 1
                print(f"  PATCH-ERR {ep['id'][:8]}: {e!r}"); break
        print(f"  enriched {series[:40]}: {n}/{len(eps)} episodes ({guids['show_imdb_id']})")
    print(f"\nsamenvatting: processed={processed} updated_eps={updated} "
          f"noop={noop} unavailable={unavailable} errors={errors}")


if __name__ == "__main__":
    main()
