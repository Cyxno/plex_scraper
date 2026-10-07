#!/bin/bash
# Soak-monitor (Phase 37): elke 10 min een JSONL-snapshot van stack + queue.
# Run: setsid nohup bash scripts/soak_monitor.sh >> .../soak/soak.log 2>&1 &
# Rapporteer: scripts/soak_report.py /mnt/cache/appdata/plex-scraper/soak/ingest-soak.jsonl
set -u
OUT=${SOAK_OUT:-/mnt/cache/appdata/plex-scraper/soak/ingest-soak.jsonl}
mkdir -p "$(dirname "$OUT")"
while true; do
  TS=$(date -u +%FT%TZ)
  CORE=$(curl -s -m 5 --unix-socket /var/run/docker.sock http://x/containers/plex-scraper-core/json | grep -o '"RestartCount":[0-9]*' | cut -d: -f2 | tr -d '
'; echo -n ' '; curl -s -m 5 --unix-socket /var/run/docker.sock http://x/containers/plex-scraper-core/json | grep -o '"Status":"healthy"\|"Status":"running"' | head -1 | cut -d'"' -f4)
  VFS=$(curl -s -m 5 --unix-socket /var/run/docker.sock http://x/containers/plex-scraper-vfs/json | grep -o '"RestartCount":[0-9]*' | cut -d: -f2 | tr -d '
'; echo -n ' '; curl -s -m 5 --unix-socket /var/run/docker.sock http://x/containers/plex-scraper-vfs/json | grep -o '"Status":"healthy"\|"Status":"running"' | head -1 | cut -d'"' -f4)
  STATUS=$(curl -s -m 5 http://127.0.0.1:18282/api/ingest/status)
  QUEUE=$(echo "$STATUS" | grep -o '"counts_by_state":{[^}]*}')
  PROV=$(echo "$STATUS" | grep -o '"state":"[A-Z_]*","retry_in_s"' | sed 's/"state":"//; s/","retry_in_s"//')
  CANARY=$(curl -s -m 15 http://127.0.0.1:18282/api/ingest/canary | grep -o '"ok":true' | head -1 | grep -c true)
  PHYS=$(curl -s -m 5 http://127.0.0.1:18282/api/physical | grep -o '"status":"[A-Z]*"' | sed 's/"//g')
  STATS=$(curl -s -m 5 --unix-socket /var/run/docker.sock "http://x/stats?stream=false" > /dev/null; docker stats --no-stream --format '{{.Name}}={{.CPUPerc}};{{.MemUsage}}' plex-scraper-core plex-scraper-vfs bazarr 2>/dev/null | tr '\n' ' ')
  DBSZ=$(stat -c%s /mnt/cache/appdata/plex-scraper/data/state.db 2>/dev/null || echo 0)
  WALSZ=$(stat -c%s /mnt/cache/appdata/plex-scraper/data/state.db-wal 2>/dev/null || echo 0)
  echo "{\"ts\":\"$TS\",\"core\":\"$CORE\",\"vfs\":\"$VFS\",${QUEUE:-\"counts_by_state\":{}},\"provider\":\"$PROV\",\"canary_ok\":$CANARY,\"physical\":\"$PHYS\",\"resources\":\"$STATS\",\"db_bytes\":$DBSZ,\"wal_bytes\":$WALSZ}" >> "$OUT"
  sleep 600
done
