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
