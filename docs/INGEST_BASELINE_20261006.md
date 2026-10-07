# Ingest-hardening baseline — 2026-10-06 (Phase 0)

Vastgelegd vóór mutaties. **Publieke kopie: site-specifieke hostnamen, LAN-adressen en backup-locaties zijn geanonimiseerd.**

## Topologie

- Unraid-host `<unraid-host>` (`<LAN-IP>`).
- Code: `/workspace/plex_scraper` (git main @ 90fca3d, clean), image `plex-scraper:local`.
- `plex-scraper-core`: supervisor-roles resolver (8282→18282), scraper (8283→18283), web/cockpit (8285).
  - Env: SWEEPER_ENABLED=true, SWEEPER_SHADOW_MODE=false, SWEEPER_ITEMS_PER_HOUR=200, JIT_ENABLED=true.
  - Mounts: /config, /db.sqlite, torbox secret, /data, docker.sock. **Geen** symlink-tree en **geen** /mnt/remote/nzbdav.
- `plex-scraper-vfs`: twee vfs-roles: FUSE `/mnt/cache/appdata/plex-scraper/vfs` (primair) en `/mnt/remote/nzbdav` (rehydrate), beide RESOLVER_URL=http://<LAN-IP>:18282.
- Plex: `/symlinks` (RO) + `/mnt/remote/nzbdav` (RO) + `/media/plex-scraper` (RO). Plex-namespace is autoritatief (sentinel via docker exec, python3 aanwezig).
- Sonarr: host-poort **7854** (container 8989), rootfolder `/media` = host `/mnt/vm_storage/symlinks/TV Shows` (RW).
- Radarr: 7878, rootfolder idem `/media-movies` → `/mnt/vm_storage/symlinks/Movies`.
- Symlink-conventie: `/mnt/vm_storage/symlinks/TV Shows/<Show>/Season N/<release-name>.mkv` → `/mnt/remote/nzbdav/.ids/<h>/<h>/<h>/<h>/<h>/<uuid>` (eerste 5 uuid-hexen als dirs).
- Bazarr 0.13% CPU (geen storm), Seerr 5055, Tautulli 8181: beide levend.

## Resolver-tellingen (17:52, uptime ~21,75h)

- items 1908: READY 1894, NO_SOURCE 14 (waaronder Lanterns S01E08 `f74db00b93864581b3b1b1ca8b08d39a`).
- Coverage latest.json: logical_total 2022, managed 1955, canonical_plexns_verified 1875, legacy_working 72, legacy_dead 67, coverage 96,7%, blocked 10.
- sessions_open 22; runtime-failover searches 10721 (24h-venster van proces) — belangrijke Torrentio-belastingsbron.
- scraper_errors laatste 24h: **122, vrijwel allemaal Torrentio 429**.

## 429→NO_SOURCE root cause (Phase 1, bewezen via events)

Lanterns S01E08 "Dirt and Stars" (Sonarr episodeId 2526, seriesId 58, IMDb tt26545992, TVDb 376098):

1. 17:18:05 item_registered (bootstrap-resolve).
2. `TorrentioScraper.search` → `resp.raise_for_status()` → httpx 429 (`torrentio.py:55`).
3. `Resolver._gather_candidates` vangt generic `Exception` → event `scraper_error` → **leeg resultaat wordt gecachet** (`engine.py:391`, TTL 1800s).
4. candidate_count=0 → `resolution_failed` + `rejects {"no_candidates": 1}` → `item.status = NO_SOURCE` (`engine.py:309`).
5. Forced re-resolve 17:18:09 → identiek (429 live) → blijft NO_SOURCE.

Semantische fout: 429 = provider onbeschikbaar, NIET "geen bron". Daarnaast: lege resultaten door provider-fout mogen nooit de negative cache in.

## Arr-baseline

- Sonarr health: alleen AllowedHosts-warning. RSS-sync actief (165 reports). Queue 7 items, status `downloadClientUnavailable` (decypharr-imports falen op pad).
- **Backlog: 106 monitored missing episodes** (nieuwste eerst: Lanterns S01E08, gelucht 2026-10-05).
- Radarr: **27 monitored missing movies** (o.a. Worldbreaker). Health-errors: InfiniDysk DC faalt (nog ENABLED in Radarr), decypharr remote-path-mapping `/mnt/debrid/decypharr_downloads` mist in container.
- Prowlarr: 5 enabled indexers (LimeTorrents, NZBGeek, Spotweb, TPB, YTS); health alleen AllowedHosts-warning.
- Download clients: Sonarr decypharr qBit (8282, cat `sonarr:Default`) + InfiniDysk SAB (disabled). Radarr decypharr qBit (cat `radarr:Default`) + **InfiniDysk SAB ENABLED**.
- Remote path mapping: alleen Sonarr (`<LAN-IP>: /mnt/debrid/decypharr_downloads/ → /mnt/debrid/decypharr/`). Radarr: geen.
- decypharr-layout: `/mnt/cache/appdata/decypharr/mnt/debrid/decypharr/{__all__,__bad__,nzbs,torbox,torrents}` — arr-imports verwachten `sonarr:Default`-paden die niet bestaan → E2E-import NIET bewezen.
- Sonarr series 58 path = `/media/Lanterns` (zonder jaar) — bridge moet de arr-seriepad volgen, niet gokken.

## Conclusies / ontwerpbesluiten

1. Fix in scraper-laag (typed provider-exceptions) + circuit + engine PROVIDER_WAIT; negative-cache nooit bij providerfout.
2. Bridge draait als asyncio-task in het resolver-proces (sweeper-patroon): deelt Store/Resolver/circuit, geen dubbele caches. Webhook-endpoints op resolver-API.
3. Symlink-delivery: core-container krijgt RW-mount van `/mnt/remote/nzbdav` (probe) en `/mnt/vm_storage/symlinks` (atomische symlink). Plex-verificatie via docker-exec-in-plex (RO, autoritatief).
4. Arr post-delivery: RescanSeries/RescanMovie-command + hasFile-poll (ondersteunde API's, geen DB-writes).
5. Queue persisteert in state.db (WAL, /data) — overleeft restart.
