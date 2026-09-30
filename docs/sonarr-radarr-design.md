# Sonarr / Radarr integration design (FASE 15 — designed, not implemented in v0.1)

## Division of responsibility

| Concern | Owner |
|---|---|
| which media is *wanted*, monitored state, air/release dates, quality **intent**, custom formats & policy | Sonarr / Radarr (stay in place) |
| what is *playable right now* (scrape → score → validate → serve) | plex_scraper resolver |
| user requests | Overseerr (stays above Sonarr/Radarr) |

**Principle: a Sonarr release is never a permanent media identity.** Sonarr
expresses *desire*; the resolver independently decides *which live source
serves the bytes at playback time*. This is what makes sources disposable
without touching the Plex library.

## Contract (already implemented endpoints in v0.1; webhooks come later)

### Register a logical item

```
POST /media
{
  "kind": "episode",            # episode | movie
  "title": "Pilot",
  "series": "Breaking Bad", "season": 1, "episode": 1,
  "imdb_id": "tt0903747", "tmdb_id": 1396, "tvdb_id": 81189,
  "plex_path": "TV/Breaking Bad/Season 01/Breaking Bad - S01E01.mkv",
  "desired": {"resolution": "1080p"}     # optional quality intent
}
→ 201 {id, status: READY, generation, plex_path}
```

Bootstrap resolves one source immediately so Plex can analyze real bytes.

### Update desired quality (FASE 3 hook)

```
PATCH /media/{id}  {"desired": {"resolution": "2160p", "video": "dolby_vision"}}
```

v0.1 stores intent on the item; v0.2 applies it as a scorer overlay (boost
matching candidates) and may trigger re-resolution if the active source no
longer matches.

### Remove

```
DELETE /media/{id}
```

Drops sessions, sources, and the VFS path. The Plex library entry is left to
Plex's normal "file missing" behaviour (documented in plex-behaviour.md).

### Force re-resolution (optional, used by tests/debug)

```
POST /media/{id}/resolve
```

## How Sonarr/Radarr would drive it (v0.2+)

1. **Webhook path (preferred):** Sonarr/Radarr `Test`/`Grab`/`Import` webhooks
   hit a small adapter that maps the event to `POST /media` / `PATCH /media`.
   - `Grab` → register item (bootstrap source resolved from the grabbed
     release's info hash when available — faster than scraping).
   - `Rename/move` → update `plex_path` once, explicitly.
2. **Sync path (fallback):** a periodic `/api/v3/series`+`episodefile` poll
   reconciles desired state (bounded, e.g. every 15 min, cheap diff) — this is
   the only scheduled loop the design allows, and it is optional.
3. **Quality policy:** custom formats stay in Sonarr; they are collapsed into
   the item's `desired` blob. The resolver's own YAML scoring stays the
   single source of truth for *ranking what is actually available now*.

## What deliberately stays out

- Import/rename/move logic (Sonarr's job)
- Episode monitoring/calendar (Sonarr's job)
- Any long-lived coupling between a grabbed release and the item identity
