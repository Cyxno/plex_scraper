# Runbooks — plex-scraper productie

Korte, concrete procedures per storing. Elke runbook: symptoom → diagnose → actie → valideren.

---

## A. Provider 429 (Torrentio rate limit)

**Symptoom**: cockpit Providers toont `RATE_LIMITED` + retry-afstand; jobs in `PROVIDER_WAIT`.

**Diagnose**:
```
curl -s http://127.0.0.1:18282/api/ingest/status | grep -o '"provider":{[^}]*}'
docker logs plex-scraper-core --since 30m | grep -c 'PROVIDER_RATE_LIMITED'
```

**Actie**: GEEN. Het circuit pauzeert de queue veilig en probeert half-open.
Alleen bij >2h continue RATE_LIMITED: controleer of een ander proces op het
zelfde IP Torrentio raakt (Stremio/crosswatch). **Nooit** handmatig hammeren.

**Valideer**: circuit gaat HEALTHY binnen de backoff-cap (30 min) zodra
Torrentio weer antwoordt; PROVIDER_WAIT-jobs hervatten automatisch.

---

## B. Sonarr/Radarr indexers unavailable

**Symptoom**: arr-health errors "Indexers unavailable"; RSS-sync leeg.

**Diagnose**:
```
curl -s http://127.0.0.1:9696/api/v3/health -H "X-Api-Key: $(cat /mnt/cache/appdata/plex-scraper/secret/radarr_key)"
curl -s http://127.0.0.1:7854/api/v3/health -H "X-Api-Key: $(cat /mnt/cache/appdata/plex-scraper/secret/sonarr_key)"
```

**Actie**: Prowlarr app-sync-URL's controleren (moeten host-gateway-adressen
zijn, géén localhost — zie incident 2026-10-05). Indexer-statistieken in
Prowlarr bekijken; individuelle indexers tijdelijk disablen bij hard falen.

**Valideer**: arr-health binnen 15 min groen; RSS-sync-taak krijgt
`lastExecution` vers (System→Tasks).

---

## C. Queue stuck (geen voortgang terwijl provider HEALTHY)

**Symptoom**: soak-rapport "STILVAL" of queue-teller beweegt 30+ min niet.

**Diagnose**:
```
curl -s http://127.0.0.1:18282/api/ingest/status
docker logs plex-scraper-core --since 30m | grep ingest_worker_error | tail
curl -s http://127.0.0.1:18282/api/ingest/canary
```

**Actie**: worker-fouten bekijken; één job handmatig:
```
curl -s -X POST http://127.0.0.1:18282/api/ingest/jobs/<job_id>/retry
```
Blijft hangen op één item: annuleer (wordt FAILED_FINAL mét reden):
```
curl -s -X DELETE http://127.0.0.1:18282/api/ingest/jobs/<job_id>
```

**Valideer**: `ingest_job_completed`-events komen weer voor; soak-rapport
stopt met STILVAL.

---

## D. Item NO_SOURCE (echt geen bron)

**Symptoom**: job `BLOCKED_NO_SOURCE` of item-status NO_SOURCE.

**Diagnose**: events van het item — `resolution_failed` met
`rejects: {"no_candidates": 1}` én géén provider-fout ernaast. Zo ja, dan is
het een echte no-match (429/5xx zijn nooit NO_SOURCE).

**Actie**: later opnieuw proberen mag altijd:
```
curl -s -X POST http://127.0.0.1:18282/api/media/<item_id>/actions/retry
```
Nieuwe releases: Gewoon wachten — releases verschijnen vaak binnen dagen.

---

## E. Plex EIO / onleesbare media

**Symptoom**: afspeelfout in Plex; `physical` niet HEALTHY; runtime-dead.

**Diagnose**:
```
curl -s http://127.0.0.1:18282/api/physical
docker exec plex python3 -c "print(open('<plex-pad>','rb').read(64))"
```

**Actie**: de resolver repareert automatisch (failover naar alternatieve
bron, path-repair). Handmatig forceren:
```
curl -s -X POST http://127.0.0.1:18282/api/media/<item_id>/actions/find-alternative
```
Blijft EIO op VFS-niveau: zie runbook G.

**Valideer**: `runtime_delivery_healthy`-events; Plex-playback start.

---

## F. Bazarr hoge CPU

**Symptoom**: Bazarr CPU >20% aanhoudend; error-storm in logs.

**Diagnose**:
```
docker stats --no-stream bazarr
docker logs bazarr --since 30m 2>&1 | grep -ciE 'error|exception'
```

**Actie**: bijna altijd veroorzaakt door dode paden in de library — de
onderliggende oorzaak is dan runbook E. Bazarr zelf heeft native backoff;
niets in plex-scraper stuurt Bazarr aan.

**Valideer**: CPU <5% na het oplossen van de oorzaak.

---

## G. VFS unhealthy

**Symptoom**: `plex-scraper-vfs` health niet healthy; Plex ziet mount niet.

**Diagnose**:
```
docker inspect plex-scraper-vfs --format '{{.State.Health.Status}}'
docker exec plex sh -c 'ls /mnt/remote/nzbdav/.ids/'
```

**Actie**: volledige deploy (`scripts/deploy-production.sh`) — herstart core +
vfs + plex + arrs in de juiste volgorde met health-gates. Losse vfs-restart
is NIET genoeg (Plex bindt de FUSE-mount alleen bij containerstart).

**Valideer**: sentinel-checks in de deploy-output ("MobLand-tree bereikbaar").

---

## H. Arr rapporteert missing na delivery

**Symptoom**: job `BLOCKED_MAPPING`/FAILED_RETRYABLE "hasFile=false na rescan".

**Diagnose**:
```
curl -s http://127.0.0.1:18282/api/ingest/jobs?status=FAILED_RETRYABLE
# bestaat de symlink in de arr-container?
docker exec sonarr ls -la "/media/<Show>/Season N/"
docker exec sonarr sh -c 'head -c 16 "$(readlink -f /media/<Show>/Season N/<file>)" >/dev/null && echo READ_OK || echo READ_FAIL'
```

**Actie**: READ_FAIL met "Socket not connected" → mount stale → arr-herstart
(`docker restart sonarr radarr`). Anders: één rescan forceren:
```
curl -s -X POST http://127.0.0.1:7854/api/v3/command -H "X-Api-Key: $(cat /mnt/cache/appdata/plex-scraper/secret/sonarr_key)" -H 'Content-Type: application/json' -d '{"name":"RescanSeries","seriesId":<id>}'
```

**Valideer**: `hasFile: true` op de episode/movie; job COMPLETED.

---

## Rollback (deploy faalt of regressie)

1. Vorige image staat nog op de host: `docker images plex-scraper:local` —
   tag/rollback via `docker tag` + `scripts/deploy-production.sh SKIP_TESTS=1`
   na checkout van de vorige commit (`git checkout <sha>` eerst).
2. De persistente queue staat in `/mnt/cache/appdata/plex-scraper/data/state.db`
   (WAL) en is forward/backward-compatibel binnen 1.x — rollen tussen 1.x-versies
   verliest geen jobs.
3. Na rollback: `curl http://127.0.0.1:18282/api/ingest/canary` moet groen zijn.
