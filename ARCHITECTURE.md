# ARCHITECTURE

Technical architecture of the plex_scraper PoC. Rationale for *why* lives in
[ARCHITECTURE_DECISION.md](ARCHITECTURE_DECISION.md); this file describes *how*
it works.

## Components

```
┌────────────────────────────┐        ┌─────────────────────────────────────────┐
│ plex-scraper-vfs           │  HTTP  │ plex-scraper-resolver (FastAPI)         │
│                            │◀──────▶│                                         │
│ pyfuse3 mount              │        │  ┌────────────┐  ┌───────────────────┐  │      ┌──────────┐
│  /mnt/plex-scraper         │        │  │ SQLite     │  │ Resolution engine │  │◀────▶│ TorBox   │
│  - getattr → resolver      │        │  │ items      │  │  state machine    │  │      └──────────┘
│  - readdir → resolver      │        │  │ sources    │  │  scoring/ranking  │  │      ┌──────────┐
│  - open → POST /open       │        │  │ sessions   │  │  validation       │  │◀────▶│ Torrentio│
│  - read → GET /stream      │        │  │ events     │  │  generations      │  │      └──────────┘
│  - release → DELETE /open  │        │  └────────────┘  └───────┬───────────┘  │
│                            │        │  ┌──────────────────────▼───────────┐  │
│ per-handle read-ahead      │        │  │ Range stream proxy               │──┼──▶ TorBox CDN
│ buffer per session         │        │  │ (bounded buffer, HTTP 206)       │  │
└────────────────────────────┘        │  └──────────────────────────────────┘  │
                                      │  caches: candidates / links / bads TTL │
                                      └─────────────────────────────────────────┘
```

Both processes ship in one image and are deployed as two compose services so
that only `vfs` needs `CAP_SYS_ADMIN` + `/dev/fuse`. The resolver's HTTP API is
the single source of truth; the VFS layer is thin and stateless.

## Data model (SQLite, WAL mode)

```
media_items                    sources                          sessions
────────────                   ───────                          ────────
id TEXT PK                     id TEXT PK                       handle TEXT PK
kind movie|episode             media_item_id FK                 media_item_id FK
title                          generation INT                   source_id FK (pinned)
series                         provider (torbox)                opened_at
season, episode                info_hash                        state open|closed
imdb_id, tmdb_id, tvdb_id      torrent_name, file_id, file_name
plex_path  (STABLE)            size, resolution, codec, hdr,
status  NO_SOURCE|RESOLVING|    audio, language, release_type
       CANDIDATE_VALIDATION|   score REAL, score_json (breakdown)
       READY|SOURCE_FAILED     state candidate|active|failed|retired
generation INT                 failure_count INT
desired JSON (quality intent)  bad_until TS (temporary bad)
created_at, updated_at         last_verified TS
                               created_at
```

Identity rule: `media_items.plex_path` never changes; `sources.generation`
changes as often as needed. A source is identified by
`(provider, info_hash, file_id)` but is *never* used as identity of the item.

events (append-only JSON log of resolutions/switches/failures) feeds
`GET /status` and the structured logs.

## Resolution state machine (per media item)

```
NO_SOURCE ──open/register──▶ RESOLVING ──candidates found──▶ CANDIDATE_VALIDATION
                                   │                              │
                                   │ no candidates                │ probe read ok
                                   ▼                              ▼
                              NO_SOURCE ◀──all rejected      READY (gen N)
                                                                  │
                                    probe failed for candidate ──▶ mark bad (TTL)
                                                                  │ try next candidate
                                                                  ▼
                              RESOLVING ◀──failure detected──── READY
                                   │      (read EIO / debug fail /
                                   │       link refresh failed)
                                   ▼
                             SOURCE_FAILED (gen N dead)
                                   │
                                   └──▶ RESOLVING ──▶ CANDIDATE_VALIDATION
                                                          │
                                                          ▼
                                                   READY (gen N+1)
```

Rules:
- One broken torrent can never produce `MEDIA_ITEM_BROKEN` while other
  candidates exist; candidates are tried in score order until one validates.
- Failed candidates get `bad_until = now + ttl` (default 30 min) and
  `failure_count += 1`; they are skipped while `bad_until` is in the future.
- Each successful switch increments `generation` and writes an event with the
  `reason_for_source_switch`.
- Resolution is synchronous-on-demand with a bounded budget (per-candidate
  validation timeout); it happens on bootstrap, on `open` when unresolved, and
  on failure.

## Session pinning

```
Plex            VFS (handle h)            Resolver                          TorBox
 │ open(path) ─▶ │                          │                                │
 │               │ POST /media/{id}/open ──▶│ resolve if needed              │
 │               │                          │ pick active source (gen N)     │
 │               │ ◀── {handle, size, gen} ─│ create session (pins source)   │
 │ read(fd,off) ▶│ GET /stream/{h}?off&len ▶│ Range GET from pinned CDN link │──▶ 206 bytes
 │ ◀── bytes ────│◀── bytes (buffered) ─────│◀── bytes                       │
 │ seek/read     │  (in-buffer ▶ serve; outside ▶ re-Range same source)      │
 │ close(fd) ───▶│ DELETE /open/{h} ───────▶│ close session                  │
```

- Concurrent Plex operations (transcoder + analyzer + player) get independent
  handles → independent pinned sources; state never mixes.
- Upstream error mid-session → `EIO` on that handle only. Other sessions are
  untouched. Next open → fresh resolution, generation+1.

## Range stream proxy

- One upstream HTTP connection per active range; bounded read-ahead buffer
  (default 8 MiB) per session.
- Sequential reads are served from the buffer; a seek outside the buffer
  aborts the current GET and issues a new `Range:` request against the same
  pinned source (same file layout, offsets stay valid).
- Upstream concurrency bounded by a global semaphore; every request has
  connect/read timeouts.

## VFS semantics (what Plex actually touches)

| Plex activity | VFS behaviour |
|---|---|
| Library scan | `readdir`/`getattr` from item registry — stable tree, no provider calls |
| Media analysis / thumbnails | sequential `read()` at small offsets — served like any read; requires READY item (bootstrap guarantees this) |
| Direct Play | open → range reads, mostly sequential with MKV cue jumps |
| Transcode | ffmpeg child opens the same path (its own handle/pin) |
| Seek | offset outside buffer → new ranged GET, same source |
| Source dies mid-play | `read()` returns `EIO`; Plex shows playback error; next playback uses new generation, same path |

Mount tree (example):

```
/mnt/plex-scraper/
  TV/MobLand/Season 02/MobLand - S02E01.mkv
  Movies/Fight Club (1999)/Fight Club (1999).mkv
```

Paths come from `media_items.plex_path` (set at registration) — nothing else
can create paths, which removes path-traversal surface by construction.

## Caching (v0.1)

| Cache | Scope | TTL (default) | Notes |
|---|---|---|---|
| candidates | per item | 30 min (config 15–60) | scraper results, normalized |
| checkcached batch | per hash set | 10 min | avoids TorBox quota burn |
| download links | per source | 2.5 h | requestdl validity ≈ 3 h; refresh on 403/410 keeps generation |
| failed candidates | per source | 30 min | `bad_until`, backoff ×2 per repeat failure (cap 6 h) |
| successful source | per item | until failure | READY state itself |

No media byte cache in v0.1 (bounded cache reserved for later; unbounded disk
growth is a non-goal).

## Security posture

- `TORBOX_API_TOKEN` only via env/`.env`; redacted in logs (never printed).
- Resolver binds `0.0.0.0` inside the compose network only; host publishing is
  `127.0.0.1` by default, opt-in LAN.
- Debug/failure-injection endpoints answer 404 unless `DEBUG=true`.
- VFS paths are registry-derived; no client-supplied path components.
- All outbound calls: bounded timeouts, bounded retries, bounded concurrency.
- SSRF surface is minimal: stream proxy only talks to URLs obtained from the
  TorBox `requestdl` response (scheme/host allow-checked).

## Interface for Sonarr/Radarr (designed, not implemented)

`POST /media` (register), `PATCH /media/{id}` (desired quality), `DELETE
/media/{id}`, `POST /media/{id}/resolve` (force re-resolution). Sonarr/Radarr
webhooks or periodic *PlantTheVine*-style sync can call these; see
[docs/sonarr-radarr-design.md](docs/sonarr-radarr-design.md).
