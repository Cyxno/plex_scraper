# Changelog

## 1.0.4 — 2026-10-08 (public-release hardening)

### Public/privacy hardening
- Removed site-specific LAN addresses and hostnames from committed defaults, deploy tooling and operator docs.
- Disabled Sonarr/Radarr URL defaults are now empty; deployments must supply their own endpoints.
- Added `secrets/`, private-key formats and local backup artifacts to `.gitignore`.
- Added `SECURITY.md` with secret-handling and network-exposure guidance.

### Release/CI hygiene
- GitHub-hosted CI now runs the portable suite and explicitly excludes the privileged VFS integration module; that suite requires real FUSE mount semantics and remains part of local/production preflight.
- README updated from the old PoC framing to the current 1.0.x architecture and capabilities.
- Added an explicit MIT `LICENSE` file.

## 1.0.3 — 2026-10-07 (JIT failover file-selectie-fix)

### JIT-probe file-selectie (root cause van de valse failover-leegte)
- jit.py `_probe_by_hash`: gebruikte torrentio's `fileIdx` rechtstreeks als
  TorBox-file-id. Bij multi-file torrents is TorBox id 0 vaak een NFO-sidecar
  (Lanterns S01E08 FLUX/Kitsune: id 0 = 1.467 B NFO, id 2 = 6,55 GB video) —
  de probe vroeg `cand.size // 2` (torrent-totaal) aan die sidecar en kreeg
  HTTP 416; 3/3 same-class kandidaten vielen weg → vals
  `jit_no_equivalent_source {rejected_quality: 0}` tijdens playback-stalls.
- Nu: file-selectie via provider.pick_file (S/E-hint, video-extensie +
  minimumgrootte, grootste videofile als fallback) — identiek aan de
  resolve/validatie-flow; scraper-fileIdx wordt nooit meer als provider-id
  gebruikt; sidecar-only torrents worden geweigerd (`jit_probe_file_unusable`).
- Probe-offsets op de GEKOZEN file berekend en geclampt binnen de
  file-grenzen; één falende sample degradeert de probe i.p.v. haar weg te
  gooien; nieuwe events: `jit_probe_file_selected` (file-id/-naam/-grootte,
  offsets), `jit_probe_file_unusable`, `jit_probe_sample_degraded`, en
  verrijkte `jit_candidate_probe`/`jit_candidate_probe_error`.

### Tests
- test_jit_probe_file_selection.py: incident-reproductie (NFO op id 0),
  fileIdx-mismatch, sidecar-only, single-file, offset-clamp, partial-failure,
  en de Lanterns-top-3 end-to-end (geen vals `no_equivalent_source` meer);
  structuur-guard dat `_probe_by_hash` geen `file_index` meer raakt.

## 1.0.2 — 2026-10-07 (cockpit UI/UX-overhaul + job-progress-fix)

### Data-bugs (root causes gefixt)
- store.job_progress(): de `progress_current`-kwarg bestond niet — de sweeper
  crashte onzichtbaar (TypeError, stil ingeslikt) en schreef nooit tussentijds
  progress. Active Now bleef daardoor eeuwig "0 / 50 · 0%" terwijl de run
  wél liep. De kwarg bestaat nu en wordt correct gebonden.
- ops.py: `now.jobs_running` bevat nu ook `started_at`/`processed`/
  `recovered`; `library.pending` wordt server-side als complement berekend
  (Ready + Issues + Pending = Total, één denominator).
- ops.py: coverage-age via centrale parser (epoch-s én -ms én ISO); ongeldig
  → `age_s: null` i.p.v. een absurd getal; coverage/ingest-provider-state
  overschrijft de 24h provider-stats niet meer.
- Nieuw `common/timefmt.py` + gemarkeerd UI-FMT-blok in cockpit.html: één
  centrale relatieve-tijd-formatter (s/ms/ISO/duur), 'ago' exact één keer,
  absurde leeftijden → "timestamp unavailable". Einde van "20733d ago ago".

### Cockpit-UI (rustiger, minder blokkerig)
- Compacte statusregel i.p.v. de brede ATTENTION-banner (volle banner alleen
  bij echte storingen), Library health als één cluster (percentage,
  gesegmenteerde balk, klikbare Ready/Issues/Pending), Automation als
  stat-strip, Physical/Coverage/Legacy audit als rustige Diagnostics-lijst.
- Active now is een live job-component: RUNNING met progressbar, verwerkt/
  percentage en live elapsed-ticker; idle → compacte regel, nooit een stale
  0/50-kaart. Run history: duur als "25m 9s", live elapsed bij RUNNING,
  compacte rijen en kleinere badges. Subtielere sidebar-active-state.
- Responsive: health-cluster en stat-strip stacken; tabellen scrollen zonder
  layoutbreuk (getest op 1920/1366/tablet/390px).

### Tests
- pytest: timefmt (s/ms/ISO/invalid), ops UI-semantiek (active-job volgt
  store, idle → leeg, Ready+Issues+Pending=Total, stale coverage/legacy
  audit degraderen health niet, SUCCESS/DEFERRED/INTERRUPTED + startup-
  reconcile, sweep-serialisatie), en node-gedreven asserts op het
  UI-FMT-blok uit de template zelf (Dockerfile.test: +nodejs).

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
