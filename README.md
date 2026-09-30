# plex_scraper — Plex-first dynamic media resolver (PoC)

**Logical media identity is permanent. The backing torrent is disposable.**

Traditional Plex/debrid stacks tie a Plex library item to one backing source
for too long: when that torrent dies, the item breaks and the whole
remove/re-add/repair dance starts. This PoC proves the opposite model:

```
Sonarr / Radarr
      ↓
logical media item  (permanent: /media/TV/MobLand/Season 02/MobLand - S02E01.mkv)
      ↓
resolver  (scrape → score → validate → pin)
      ↓
ranked current sources  (disposable, generation-numbered)
      ↓
stable VFS mount
      ↓
Plex  (sees the same path forever)
```

When a source dies:

```
source A dead → resolver scrapes & validates source B → same Plex item keeps playing
```

Design rationale: [ARCHITECTURE_DECISION.md](ARCHITECTURE_DECISION.md) ·
Technical architecture: [ARCHITECTURE.md](ARCHITECTURE.md)

## Status

Proof of Concept — TorBox-only, no DUMB / Real-Debrid / Usenet. See
[POC_REPORT.md](POC_REPORT.md) for measured results and the viability verdict.

## Components

| Service | Role |
|---|---|
| `plex-scraper-resolver` | FastAPI: domain model, scoring, TorBox adapter, scraper interface, resolution state machine, session pinning, range-stream proxy, caches, debug/failure-injection |
| `plex-scraper-vfs` | pyfuse3 mount at `/mnt/plex-scraper` — stable paths, honest sizes, real POSIX read/seek semantics for Plex |

## Quick start (Unraid / Docker Compose)

```bash
cp .env.example .env               # fill in TORBOX_API_TOKEN
cp config/preferences.example.yaml config/preferences.yaml
docker compose up -d --build
```

One-time host prep (verified on Unraid): the bind source must live under a
shared mount (Unraid marks `/mnt/cache` as `shared`):

```bash
mkdir -p /mnt/cache/appdata/plex-scraper/vfs
```

Plex then gets the mount via a read-only bind of that host path — see the
commented `plex` example in `docker-compose.yml`.

Register the test set:

```bash
docker compose exec resolver python -m plex_scraper.cli register config/testset.example.yaml
curl -s localhost:8282/media | jq
ls "/mnt/cache/appdata/plex-scraper/vfs/TV/Breaking Bad/Season 01/"
```

Failure injection (the core PoC test, `DEBUG=true`):

```bash
curl -s -X POST localhost:8282/debug/media/<item-id>/fail-current
# next playback resolves the next working source — same path, new generation
```

## Configuration

- `.env` — secrets and endpoints (`TORBOX_API_TOKEN`, bind address, flags)
- `config/preferences.yaml` — scoring preferences (resolution, video, audio,
  language, release type, exclusions, size limits); every candidate gets a
  transparent score breakdown, every reject a reason.

## Docs

- [docs/poc-test-plan.md](docs/poc-test-plan.md) — test set, layers, gates
- [docs/plex-behaviour.md](docs/plex-behaviour.md) — Plex semantics, generation changes, bootstrap
- [docs/sonarr-radarr-design.md](docs/sonarr-radarr-design.md) — integration design (not implemented in v0.1)

## Hard non-goals v0.1

Sonarr/Radarr/Overseerr replacement · Usenet/NZBDAV/Real-Debrid ·
library-wide repair · DUMB compatibility · web UI · local media storage ·
24/7 scraping.

## License

MIT
