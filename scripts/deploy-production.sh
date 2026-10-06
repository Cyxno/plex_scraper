#!/bin/bash
# Canonieke productie-deploy (F33/F35): vervangt handmatige docker-run drift.
# Volledige runbook incl. de Plex-herstart die NA elke VFS-recreate verplicht
# is (Plex bindt de FUSE-mount bij containerstart — zie incident 2026-10-05).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== build =="
docker compose build resolver

echo "== secrets (arr api-keys voor ingest-bridge) =="
APPDATA=/mnt/cache/appdata/plex-scraper
mkdir -p "$APPDATA/secret"
chmod 700 "$APPDATA/secret"
for a in sonarr radarr; do
  key=$(grep -oE "<ApiKey>[^<]*" /mnt/cache/appdata/$a/config.xml 2>/dev/null | head -1 | cut -d'>' -f2)
  if [ -n "$key" ]; then printf '%s' "$key" > "$APPDATA/secret/${a}_key"; chmod 600 "$APPDATA/secret/${a}_key"; fi
done

echo "== stop/remove =="
docker stop plex-scraper-vfs plex-scraper-core >/dev/null
docker rm plex-scraper-vfs plex-scraper-core >/dev/null
umount -l /mnt/cache/appdata/plex-scraper/vfs 2>/dev/null || true
umount -l /mnt/remote/nzbdav 2>/dev/null || true
sleep 3   # mount-propagatie laten settlen vóór recreate (voorkomt 'file exists'-race)

CORE_ENV='[{"role":"resolver","env":{"RESOLVER_BIND":"0.0.0.0:8282","DB_PATH":"/data/state.db","CONFIG_DIR":"/config","TORBOX_API_TOKEN_FILE":"/run/secrets/torbox_key","SWEEPER_ENABLED":"true","SWEEPER_AUTOSTART":"true","SWEEPER_ITEMS_PER_HOUR":"200","SWEEPER_SHADOW_MODE":"false","SWEEPER_FAIL_STRIKES":"2","SWEEPER_PLAYBACK_PAUSE":"true","STREAM_TWO_WAY_ENABLED":"true","ADAPTIVE_TWO_WAY_MIN_MBIT":"40","JIT_ENABLED":"true","JIT_PREFLIGHT_MIN_MBIT":"40","JIT_MAX_WAIT_S":"12","JIT_RESCUE_MARGIN":"1.2","JIT_STARTUP_FIRST_BYTE_S":"8","INGEST_ENABLED":"true","SONARR_ENABLED":"true","SONARR_URL":"http://192.168.1.2:7854","SONARR_API_KEY_FILE":"/run/secrets/sonarr_key","RADARR_ENABLED":"true","RADARR_URL":"http://192.168.1.2:7878","RADARR_API_KEY_FILE":"/run/secrets/radarr_key","INGEST_BATCH_SIZE":"1","INGEST_RECONCILE_INTERVAL_S":"1200"}},{"role":"scraper","env":{"SCRAPER_BIND":"0.0.0.0:8283","CONFIG_DIR":"/config","TORBOX_API_TOKEN_FILE":"/run/secrets/torbox_key"}},{"role":"web","env":{"WEB_BIND":"0.0.0.0:8285","RESOLVER_URL":"http://127.0.0.1:8282","MIG_DB":"/db.sqlite"}}]'
VFS_ENV='[{"role":"vfs","env":{"VFS_MOUNTPOINT":"/mnt/cache/appdata/plex-scraper/vfs","RESOLVER_URL":"http://192.168.1.2:18282"}},{"role":"vfs","env":{"VFS_MOUNTPOINT":"/mnt/remote/nzbdav","RESOLVER_URL":"http://192.168.1.2:18282"}}]'

echo "== core =="
docker run -d --name plex-scraper-core --restart unless-stopped --entrypoint python \
  -p 18282:8282 -p 18283:8283 -p 8285:8285 \
  -v /mnt/cache/appdata/plex-scraper/config:/config \
  -v /mnt/cache/appdata/plex-scraper/migration/migration-state.sqlite:/db.sqlite \
  -v /mnt/cache/appdata/plex-scraper/secret/torbox_key:/run/secrets/torbox_key:ro \
  -v /mnt/cache/appdata/plex-scraper/secret/sonarr_key:/run/secrets/sonarr_key:ro \
  -v /mnt/cache/appdata/plex-scraper/secret/radarr_key:/run/secrets/radarr_key:ro \
  -v /mnt/cache/appdata/plex-scraper/data:/data \
  -v /mnt/vm_storage/symlinks:/mnt/vm_storage/symlinks \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --env ROLE_SPECS="$CORE_ENV" --env HEALTH_ROLES='["resolver","scraper","web"]' --env PHYSICAL_HEALTH_ENABLED=true \
  plex-scraper:local -m plex_scraper.roles.supervisor

echo "== vfs =="
docker run -d --name plex-scraper-vfs --restart unless-stopped --entrypoint python \
  --cap-add CAP_SYS_ADMIN --device /dev/fuse \
  -v /mnt/cache/appdata/plex-scraper/vfs:/mnt/cache/appdata/plex-scraper/vfs:rshared \
  -v /mnt/remote/nzbdav:/mnt/remote/nzbdav:rshared \
  --env ROLE_SPECS="$VFS_ENV" \
  plex-scraper:local -m plex_scraper.roles.supervisor

echo "== wacht tot vfs healthy is (healthcheck = FUSE-sentinel) =="
for i in $(seq 1 20); do
  st=$(docker inspect plex-scraper-vfs --format '{{.State.Health.Status}}' 2>/dev/null || echo starting)
  [ "$st" = healthy ] && break; sleep 5
done
[ "${st:-}" = healthy ] || { echo "VFS niet healthy — ABORT vóór Plex-herstart"; exit 1; }

echo "== plex-herstart (verplicht: plex bindt de FUSE-mount bij start) =="
docker restart plex
sleep 30
docker exec plex sh -c 'ls /mnt/remote/nzbdav/ >/dev/null 2>&1' \
  || { echo "Plex ziet de mount niet — HANDMATIG INGRIJPEN"; exit 1; }

echo "== arr-herstart (zelfde reden als plex: oude FUSE-bind = 'Socket not connected') =="
docker restart sonarr radarr 2>/dev/null || true
sleep 20
docker exec sonarr sh -c 'ls /mnt/remote/nzbdav/.ids/ >/dev/null 2>&1' \
  || { echo "Sonarr ziet de mount niet — HANDMATIG INGRIJPEN"; exit 1; }
docker exec radarr sh -c 'ls /mnt/remote/nzbdav/.ids/ >/dev/null 2>&1' \
  || { echo "Radarr ziet de mount niet — HANDMATIG INGRIJPEN"; exit 1; }

echo "== sentinels =="
docker exec plex sh -c 'test -e "/symlinks/TV Shows/MobLand (2025)/Season 2"' \
  && echo "OK: MobLand-tree bereikbaar in Plex-namespace"
docker ps --filter name=plex-scraper --format '{{.Names}} {{.Status}}'
echo "deploy klaar"
