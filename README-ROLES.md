# plex_scraper — rollen (één image, vier process-roles)

Eén repo, één image: `plex-scraper:local`. Rollen via subcommand:

| Container/rol | Command | Poort (host) | Functie |
|---|---|---|---|
| `plex-scraper-resolver` | `resolver` | 18282 | state machine, scoring, TorBox, stream proxy |
| `plex-scraper-vfs` | `vfs` | — | FUSE-mount (test-set) /mnt/plex-scraper |
| `plex-scraper-vfs-legacy` | `vfs` | — | FUSE-mount (legacy nzbdav) /mnt/remote/nzbdav |
| `plex-scraper-scraper` | `scraper` | 18283 | optionele standalone scraper-API (search/score/availability) |
| `plex-scraper-web` | `web` | 8285 | read-only diagnostics GUI |

Elke rol: eigen healthcheck, logs, restart-policy, env/config.
Deployment op deze Unraid: DockerMan-containers (niet compose).
Compose-bestand heeft ook scraper/web-services voor nieuwe installs.

## Diagnostics GUI

- Summary: health, resolver totals, queue, mounts, latencies (auto-refresh 15s)
- Failures-tabel met trace-link per item
- Trace per rk: resolver record → status → source → symlink → VFS target →
  byte-read → resolver stream (groen/rood per stap)
- Acties: retry resolve, retry read-test (geen deletes)

URL: http://<host>:8285/

## Host-storage: stateful mounts direct via /mnt/cache (shfs/FUSE-bypass)

Op Unraid loopt `/mnt/user/appdata/...` via shfs/FUSE. Na een shfs-wedge zijn
alle I/O-/stategevoelige plex_scraper-mounts (state db, config, secrets,
migration-state) daarom bewust direct via `/mnt/cache/appdata/plex-scraper/...`
gemount (appdata-share staat fysiek uitsluitend op de cachepool, dus het is
dezelfde data — alleen het I/O-pad verschilt: XFS direct i.p.v. fuse.shfs).

Dit geldt voor: resolver (/data, /config, secret), scraper (/config, secret),
web (/db.sqlite snapshot van migration-state) én de migration-worker state/logs.
RO media-binds (/mnt/debrid, /mnt/remote/nzbdav, /mnt/vm_storage) zijn ongewijzigd.
Bij recreatie altijd de cache-pad-variant gebruiken; DockerMan-templates en
docker-compose.yml zijn hierop gezet.
