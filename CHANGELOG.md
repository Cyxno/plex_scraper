# Changelog

## 1.0.1 — 2026-10-07 (healthcheck-fix + cockpit KPI-audit)

### Healthcheck (na VFS-unhealthy-events)
- Dockerfile HEALTHCHECK: interval 30s→60s, timeout 10s→60s. De 10s-timeout
  was korter dan een koude FUSE-stat op het nzbdav-mount en gaf onterecht
  `container_unhealthy`. Sinds de recreate met 60s/60s: geen unhealthy-events
  meer, alle probes exit 0 (`vfs`+`nzbdav` beide "mounted").

### Cockpit-KPI's (audit 2026-10-07)
- ops.py: coverage/latest.json (puntmoment-snapshot) degradeert de live
  health niet meer; hij gaat uitsluitend gelabeld (incl. `age_s`) mee in de
  payload. Einde van de foutieve DEGRADED/HEALTHY_WITH_LEGACY_GAPS-path.
- cockpit.html: primaire kaarten Ready / Issues / Pending, rekenkundig
  sluitend (Ready + Issues + Pending = Total). Coverage-kaart gemuteerd als
  "Coverage snapshot" (STALE >48u, "not live health") plus "Legacy audit:
  outdated"; de onterechte "123 dead legacy · 0 working"-regel is verwijderd.

## 1.0.0 — 2026-10-07 (arr-authoritative ingest — eerste productie-release)

### Arr-authoritative ingest (nieuw)
- Persistente wanted-queue (SQLite ingest_jobs, WAL): Sonarr/Radarr
  webhook-events én 20-min reconciliatie landen idempotent op één rij per
  logisch item; queue overleeft restarts (tussenstanden → FAILED_RETRYABLE).
- Volledige pipeline: identity-guard → resolver-registratie (dubbel-preventie
  van de resolver leidend) → bounded resolve → canonical `.ids` → atomische
  symlink in het arr-seriepad → leesprobe in de plex-container (authoritatieve
  namespace) → Plex section-scan → RescanSeries/RescanMovie + hasFile-poll →
  COMPLETED.
- Ownership-guards: arr `hasFile` → COMPLETED zonder dubbele delivery; gezonde
  arr-grabs worden gedeferd; stuck grabs worden door de bridge overgenomen.
- COMPLETED-jobs heractiveren automatisch wanneer het item wéér wanted raakt.
- Age-aware prioriteit: nieuwe releases (<48u) gaan vóór recente backlog
  (<14d) en achtergrond-catch-up; binnen een tier is next_attempt de
  round-robin-fairness.

### Provider-semantiek (correct)
- HTTP 429 is nooit NO_SOURCE: getypeerde fouten (ProviderRateLimited /
  ProviderBackendUnavailable), item-status PROVIDER_WAIT, negatieve
  candidate-cache alleen bij succes.
- Circuit breaker per scraper (HEALTHY/RATE_LIMITED/DEGRADED/UNAVAILABLE),
  half-open probe, bounded exponentiële backoff (Retry-After wordt eerlijk
  gehonoreerd), 3s zelf-pacing tegen post-recovery-vloeden.
- Torrentio: kale stream-URL (de providers=-addon-variant werd per-IP
  ge-429'd), provider-budget ~1 req/30-60s wordt gerespecteerd.

### Runtime recovery
- Runtime-dead media: detectie via physical health/sweeper → failover naar
  alternatieve bron of her-resolve via de ingest-queue — het logische
  Plex-pad blijft stabiel. 123 dode legacy-delen zijn op deze manier
  geclassificeerd tegen arr-intent en (voor monitored items) opnieuw
  verworven.

### Companion-app realignment
- decypharr: OBSOLETE voor nieuwe ingest (imports symlinken naar een
  container-lokale FUSE) — download-clients uitgeschakeld in beide arrs;
  InfiniDysk (dead) uitgezet; verkeerde remote-path-mapping verwijderd.
- Bazarr: paths aligneren native; geen storm (0,1% CPU).
- Seerr → arr's alleen (correcte poorten/rootfolders); Tautulli puur
  observatie.

### Beveiliging
- Webhook-token (INGEST_WEBHOOK_TOKEN/FILE) op de ingest-endpoints.
- Webhook-payload is alléén trigger: identiteit/monitored wordt via de
  arr-API gevalideerd vóór enqueue (payload-identiteit wordt niet vertrouwd).

### Observabiliteit & operator
- Cockpit INGEST-tab: arr-health, RSS-cadans, queue-states, provider-pauze
  (correcte RATE_LIMITED-semantiek, nooit "no source"), circuit-panels.
- `/api/ingest/canary`: synthetische ketencheck (arr-API's, queue-schrijftest,
  resolver, provider-state, plex-exec) — zonder downloads.
- `/api/ingest/jobs/{id}` DELETE: veilige annulering met leesbare reden +
  audit-event. Elke niet-verse job heeft een menselijk leesbare reden.
- Startup-config-validatie: keys/URL's/symlink-root/docker.sock/webhook-token
  worden bij start gecontroleerd en in status().config_issues gerapporteerd.

### Tooling
- `maintenance/queue_invariants.py` — 7 harde queue-invariants.
- `maintenance/symlink_audit.py` — symlink-invariants over de hele tree.
- `maintenance/legacy_arr_intent.py` + `run_arr_intent.py` — legacy-classificatie.
- `scripts/soak_monitor.sh` + `soak_report.py` — doorlopende soak met
  anomalie-regels (herstarts, stilval, 429-burst, geheugen-trend, canary).
- `scripts/deploy-production.sh`: preflight-testsuite, health-gates,
  arr-herstarts na FUSE-recreate, ingest-health + canary-gate.

### Tests
- 300+ pytest-tests, o.a. queue-persistentie bij restart, reconcilie-idempotency,
  circuit-recovery, dead-grab-guard, Radarr wanted-shape, webhook-revalidatie,
  prioriteits-tiering, canary, invariant-audit.

## 0.1.x — eerdere PoC-geschiedenis
Zie git-log; de PoC-fase leverde de resolver, VFS, failover/rescue,
canonical-migratie en het cockpit-dashboardsysteem.
