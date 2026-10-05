# Runbook: VFS/FUSE ↔ Plex consumer-chain (les: PLEX_NAMESPACE_STALE_FUSE)

## Symptoom
Plex: "Controleer of het bestand bestaat..." / mass "unavailable" / oneindig bufferen.
Check: `docker exec plex ls /mnt/remote/nzbdav` →
`Transport endpoint is not connected` = Plex namespace hangt aan een DODE
FUSE-instance (VFS is gerecreated, Plex niet).

## Waarom
Plex bindt `/mnt/remote/nzbdav:ro` (default = rprivate). Een nieuwe FUSE-mount
op de host propageert NIET in de draaiende container.

## Herstel
`docker restart plex` (bindt de huidige gezonde mount). Daarna sentinel:
`docker exec plex dd if="< MobLand-symlink >" of=/dev/null bs=256K count=1`.

## Automatisch
De physical-health monitor in plex-scraper-core (PHYSICAL_HEALTH_ENABLED=true)
checkt elke 60s een sentinel in de Plex-namespace, classificeert de faalwijze
en herstelt bij exact `PLEX_NAMESPACE_STALE_FUSE` + gezonde VFS-container één
keer automatisch (cooldown 30 min, geen restart-loop; mislukt het → escalate).
Status: cockpit Overview + `GET :8285/ops/physical`.

## Deploy
ALTIJD via `scripts/deploy-production.sh` — recreates VFS, wacht op
mount-sentinel-healthy, herstart Plex, verifieert de MobLand-tree vóór PASS.
Handmatige recreates zijn verboden-drift.

## Optionele definitieve fix (Option B, nog niet toegepast)
Plex-bind wijzigen naar `/mnt/remote/nzbdav:/mnt/remote/nzbdav:ro,rslave`
(host-mount is `shared:135`) zodat nieuwe mounts propageren zonder restart.
Vereist Plex-recreate in de Unraid-template; test eerst in een wegwerp-container.

## Residuen (apart van het incident)
37 Plex part-rows zonder symlink-bestand (legacy/multi-part nalatenschap) —
individueel op te ruimen, nooit bulk.
