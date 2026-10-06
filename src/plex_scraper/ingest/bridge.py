"""Persistent ingest-bridge (Phases 10-20): arr wanted-state → resolver →
canonical `.ids` → stabiele symlink → Plex → arr-reconcile.

Ontwerp:

* draait als asyncio-task IN het resolver-proces (sweeper-patroon) en deelt
  Store/Resolver/circuit — geen dubbele caches, geen HTTP-hop;
* queue is persistent (state.db ingest_jobs, WAL) — overleeft restart;
* webhook-events en periodieke reconciliatie landen idempotent op dezelfde
  job-rij (dedupe_key UNIQUE);
* provider-respect: het provider-circuit bepaalt of er gezocht mag worden.
  Een 429 zet de job naar PROVIDER_WAIT met next_attempt_at op de
  circuit-cooldown — nooit een storm over 106 backlog-items;
* delivery pas ná READY; arr herkent het resultaat via RescanSeries/
  RescanMovie-command (ondersteunde API, geen DB-writes).

Job-states (Phase 11): QUEUED, IDENTITY_VERIFYING, REGISTERING, RESOLVING,
PROVIDER_WAIT, READY, DELIVERING, PLEX_REFRESH, COMPLETED, BLOCKED_IDENTITY,
BLOCKED_NO_SOURCE, BLOCKED_MAPPING, FAILED_RETRYABLE, FAILED_FINAL.
"""
from __future__ import annotations

import asyncio
import json
import os
import time

from plex_scraper.common.domain import models as m
from plex_scraper.common.log import event
from plex_scraper.ingest import delivery
from plex_scraper.ingest.arr import (
    ArrUnavailable,
    SonarrClient,
    RadarrClient,
    build_clients_from_env,
    wait_command_done,
)
from plex_scraper.ingest.models import (
    ACTIVE_STATES,
    TERMINAL_STATES,
    IngestJob,
    JobState,
)
from plex_scraper.ingest.plex_client import PlexExecClient

# broadcast-fix: brandwijzigingen die de resolver-log niet aanbiedt


def _job_event(bridge, ev: str, job: IngestJob, **fields) -> None:
    """Persistente event-regel per job-overgang (observability Phase 8/42)."""
    fields.update({
        "ingest_job": job.id, "source": job.source,
        "arr_item_id": job.arr_item_id, "dedupe_key": job.dedupe_key,
        "title": job.title, "state": job.status,
    })
    bridge.store_event(ev, **fields)


class IngestBridge:
    def __init__(self, resolver, settings):
        self.resolver = resolver
        self.s = settings
        self.store = resolver.store
        self.circuit = resolver.circuit
        self.sonarr, self.radarr = build_clients_from_env(settings)
        self.plex = PlexExecClient(
            container=getattr(settings, "plex_container", "plex"),
            section_tv=getattr(settings, "plex_section_tv", 2),
            section_movies=getattr(settings, "plex_section_movies", 1))
        self.plex_tv_section = int(getattr(settings, "plex_section_tv", 2))
        self.plex_movie_section = int(getattr(settings, "plex_section_movies", 1))
        self.sonarr_root_map = delivery.parse_root_map(
            getattr(settings, "sonarr_root_map", "/media=TV Shows"))
        self.radarr_root_map = delivery.parse_root_map(
            getattr(settings, "radarr_root_map", "/media-movies=Movies"))
        self._running = False
        self._task: asyncio.Task | None = None
        self._reconcile_task: asyncio.Task | None = None
        self._circuit_drain_task: asyncio.Task | None = None
        self._last_reconcile_at = 0.0
        self._reconciling = asyncio.Lock()
        self.metrics = {
            "enqueued": 0, "completed": 0, "failed_final": 0,
            "provider_waits": 0, "reconciles": 0,
            "last_completed_at": 0.0,
            "last_completed": "",
            "symlink_created": 0, "plex_verifications": 0,
            "arr_reconciles_ok": 0, "arr_reconciles_failed": 0,
        }

    # ------------------------------------------------------------- events
    def store_event(self, kind: str, **payload) -> None:
        """Synchrone event-bridge naar de async store (fire-and-forget)."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.store.add_event(kind, **payload))
            event(kind, **payload)
        except RuntimeError:
            pass

    # ----------------------------------------------------------- lifecycle
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        loop = asyncio.get_event_loop()
        loop.create_task(self._recover_on_start())
        self._task = loop.create_task(self.run())
        if getattr(self.s, "arr_reconcile_enabled", True):
            self._reconcile_task = loop.create_task(self._reconcile_loop())
        self._circuit_drain_task = loop.create_task(self._circuit_drain())

    def stop(self) -> None:
        self._running = False
        for t in (self._task, self._reconcile_task, self._circuit_drain_task):
            if t is not None:
                t.cancel()

    async def _recover_on_start(self) -> None:
        """Herstart-veiligheid: tussenstanden → FAILED_RETRYABLE (Phase 10)."""
        try:
            n = await self.store.reset_stale_running_jobs()
            if n:
                self.store_event("ingest_queue_recovered_jobs", count=n)
        except Exception as exc:                       # noqa: BLE001
            event("ingest_recover_failed", error=repr(exc)[:120])

    async def run(self) -> None:
        """Worker-loop: verwerk due jobs, batch-begrensd (Phase 25)."""
        while self._running:
            try:
                await self.process_due()
            except asyncio.CancelledError:
                return
            except Exception as exc:                   # noqa: BLE001
                event("ingest_worker_error", error=repr(exc)[:160])
            await asyncio.sleep(getattr(self.s, "ingest_worker_interval_s", 20.0))

    async def _circuit_drain(self) -> None:
        """Circuit-lifecycle-events persisteren (provider_circuit_open etc.)."""
        while self._running:
            try:
                for rec in self.circuit.take_change_events():
                    await self.store.add_event(rec["event"], **{
                        k: v for k, v in rec.items() if k != "event"})
            except asyncio.CancelledError:
                return
            except Exception:                          # noqa: BLE001
                pass
            await asyncio.sleep(5.0)

    # ------------------------------------------------------------ enqueue
    async def enqueue(self, job: IngestJob, reason: str) -> tuple[str, bool]:
        """Idempotente enqueue: bestaat de dedupe_key al, dan géén nieuwe
        job en géén re-queue van een PROVIDER_WAIT/actieve job (Phase 14/16) —
        next_attempt_at van de bestaande rij blijft leidend."""
        job.enqueue_reason = reason
        job_id, created = await self.store.upsert_job(job)
        if created:
            self.metrics["enqueued"] += 1
            self.store_event("ingest_job_enqueued",
                             job_id=job_id, reason=reason,
                             dedupe_key=job.dedupe_key, source=job.source,
                             title=job.title, arr_item_id=job.arr_item_id)
        else:
            self.store_event("ingest_job_deduped", job_id=job_id,
                             reason=reason, dedupe_key=job.dedupe_key)
        return job_id, created

    def _sort_value(self, job: IngestJob) -> float:
        """Catch-up-volgorde (Phase 26): recent gelucht eerst."""
        try:
            return -time.mktime(time.strptime(
                (job.air_date_utc or "")[:19], "%Y-%m-%dT%H:%M:%S"))
        except (ValueError, TypeError):
            return 0.0

    async def process_due(self) -> int:
        """Verwerk max ingest_batch_size due jobs per cyclus."""
        batch = int(getattr(self.s, "ingest_batch_size", 1))
        jobs = await self.store.due_jobs(time.time(), limit=batch)
        jobs.sort(key=self._sort_value)
        done = 0
        for job in jobs:
            try:
                await self.process_job(job)
            except ArrUnavailable as exc:
                # arr weg → retryable, niets kapot classificeren
                await self._set_state(job, JobState.FAILED_RETRYABLE,
                                      error=f"arr unavailable: {exc}",
                                      retry_in=300.0)
            except Exception as exc:                   # noqa: BLE001
                await self._set_state(job, JobState.FAILED_RETRYABLE,
                                      error=repr(exc)[:200], retry_in=600.0)
            done += 1
        return done

    # ------------------------------------------------------------- pipeline
    async def _arr_has_file(self, job: IngestJob) -> bool | None:
        """Phase 17/22: heeft de arr het item al als file? None = niet te
        achterhalen (client weg / onbekend item) — dan niet blokkeren."""
        client = self.sonarr if job.source == "sonarr" else self.radarr
        if client is None:
            return None
        try:
            if job.source == "sonarr":
                ep = await client.episode(int(job.arr_item_id.split(":")[1]))
                return bool(ep.get("hasFile"))
            mv = await client.movie(int(job.arr_item_id.split(":")[0]))
            return bool(mv.get("hasFile"))
        except (ArrUnavailable, ValueError, IndexError):
            return None

    _DEAD_GRAB_STATUSES = frozenset(
        {"downloadclientunavailable", "failed"})

    @staticmethod
    def _grab_record_active(r: dict) -> bool:
        """Eén queue-record = actieve grab? Dode grabs (client weg, failed)
        en stuck imports (completed+warning: onleesbaar decypharr-FUSE-pad)
        claimen NIET — daar neemt de bridge het over."""
        status = (r.get("status") or "").lower()
        tds = (r.get("trackedDownloadStatus") or "ok").lower()
        if status in IngestBridge._DEAD_GRAB_STATUSES:
            return False
        return tds not in ("error", "warning")

    async def _arr_grab_active(self, job: IngestJob) -> bool:
        """Phase 22 (dual-ingest-ownership): is de arr dit item actief aan het
        grijpen/downloaden via een legacy download-client? Alleen een GEZONDE
        grab (trackedDownloadStatus ok) claimt ownership — stuck/warning/error
        grabs (decypharr-imports die op een onleesbaar FUSE-pad hangen) worden
        bewust NIET als actief gezien; de bridge neemt die over."""
        client = self.sonarr if job.source == "sonarr" else self.radarr
        if client is None:
            return False
        try:
            if job.source == "sonarr":
                recs = await client.queue_for_episode(
                    int(job.arr_item_id.split(":")[1]))
            else:
                recs = await client.queue_for_movie(
                    int(job.arr_item_id.split(":")[0]))
        except (ArrUnavailable, ValueError, IndexError):
            return False
        return any(self._grab_record_active(r) for r in recs)

    async def process_job(self, job: IngestJob) -> None:
        job.attempts += 1
        await self.store.update_job(job, {"attempts", "updated_at"})

        # 0) ownership-guards (Phase 17/22): arr heeft al een file → klaar;
        #    arr grijpt actief → defer (geen dubbele delivery).
        if job.status not in TERMINAL_STATES:
            has_file = await self._arr_has_file(job)
            if has_file:
                await self._complete(job)
                await self.store.add_event(
                    "ingest_job_completed_by_arr", ingest_job=job.id,
                    source=job.source, arr_item_id=job.arr_item_id,
                    title=job.title,
                    note="arr heeft file — bridge niet nodig (hasFile=true)")
                return
            if job.status in (JobState.QUEUED.value, JobState.RESOLVING.value,
                              JobState.FAILED_RETRYABLE.value) and \
                    await self._arr_grab_active(job):
                await self._set_state(
                    job, JobState.FAILED_RETRYABLE,
                    provider_block="arr grab actief (legacy download-client)",
                    retry_in=900.0)
                return

        # 1) circuit-gate: bij open provider nul HTTP-kosten, wél ordelijke
        #    PROVIDER_WAIT-state (Phases 6/16). PROVIDER_WAIT-jobs worden
        #    expliciet mee-genomen zodat de cooldown netjes verlengt.
        blocked = self.circuit.blocked()
        if blocked and job.status in (JobState.QUEUED.value,
                                      JobState.RESOLVING.value,
                                      JobState.FAILED_RETRYABLE.value,
                                      JobState.IDENTITY_VERIFYING.value,
                                      JobState.REGISTERING.value,
                                      JobState.PROVIDER_WAIT.value):
            retry_in = max(
                (self.circuit.snapshot().get("scrapers", {})
                 .get(name, {}).get("retry_in_s", 0) for name in blocked),
                default=60.0)
            await self._set_state(
                job, JobState.PROVIDER_WAIT,
                provider_block=f"circuit open: {','.join(blocked)}",
                retry_in=max(retry_in, 30.0))
            return

        # 2) circuit weer gezond → deferred jobs keren terug in de pipeline
        #    (anders zijn PROVIDER_WAIT/FAILED_RETRYABLE doodlopende states);
        #    circuit-churn telt NIET als poging — alleen echt pipeline-werk.
        if job.status in (JobState.PROVIDER_WAIT.value,
                          JobState.FAILED_RETRYABLE.value):
            if job.provider_block and "circuit open" in job.provider_block:
                job.attempts = 0
            await self._set_state(job, JobState.QUEUED)

        if job.status == JobState.QUEUED.value:
            await self._identity_and_register(job)

        job = await self.store.get_job(job.id) or job

        if job.status == JobState.RESOLVING.value:
            await self._resolve(job)
            job = await self.store.get_job(job.id) or job

        if job.status == JobState.PROVIDER_WAIT.value:
            return                                     # cooldown loopt nog
        if job.status in (JobState.READY.value, JobState.DELIVERING.value,
                          JobState.PLEX_REFRESH.value):
            await self._deliver(job)

    async def _identity_and_register(self, job: IngestJob) -> None:
        """IDENTITY_VERIFYING → REGISTERING: dedupe op resolver-items, dan
        normale registratie (identiteits-guard + conflict-regels van de
        resolver blijven leidend). De bootstrap-resolve van register_item
        levert direct een eindoordeel — géén tweede search in dezelfde cyclus
        (Phase 14: geen onnodige provider-searches)."""
        await self._set_state(job, JobState.IDENTITY_VERIFYING)
        item = await self._find_existing_item(job)
        if item is None:
            await self._set_state(job, JobState.REGISTERING)
            item = await self._register_item(job)
        if item is None:
            # conflict/identity-guard heeft geblokkeerd
            await self._set_state(job, JobState.BLOCKED_IDENTITY,
                                  error="resolver registratie blokkeerde "
                                        "(identity-conflict of invalide payload)")
            return
        job.resolver_item_id = item.id
        await self.store.update_job(job, {"resolver_item_id", "updated_at"})
        if item.status == m.ItemStatus.READY.value:
            await self._set_state(job, JobState.READY)
        elif item.status == m.ItemStatus.PROVIDER_WAIT.value:
            retry_in = 0.0
            snap = self.circuit.snapshot().get("scrapers", {})
            for name in self.circuit.blocked():
                retry_in = max(retry_in, snap.get(name, {}).get("retry_in_s", 0))
            self.metrics["provider_waits"] += 1
            await self._set_state(
                job, JobState.PROVIDER_WAIT,
                provider_block="bootstrap deferred (provider unavailable)",
                retry_in=max(retry_in, 60.0))
        elif item.status == m.ItemStatus.NO_SOURCE.value:
            # bestaand item met oude/stale NO_SOURCE — laat _resolve hem
            # opnieuw zoeken (candidate-cache weg, circuit bepaalt); pas na
            # max_attempts échte no-matches volgt BLOCKED_NO_SOURCE (Phase 27)
            await self._set_state(job, JobState.RESOLVING)
        else:
            # bestaand item in tussenstand (RESOLVING/SOURCE_FAILED/...):
            # worker doet in de volgende stap een gewone resolve
            await self._set_state(job, JobState.RESOLVING)

    def _job_backoff(self, job: IngestJob) -> float:
        return min(
            getattr(self.s, "ingest_job_backoff_base_s", 300.0)
            * (2 ** max(job.attempts - 1, 0)),
            getattr(self.s, "ingest_job_backoff_max_s", 86400.0))

    async def _find_existing_item(self, job: IngestJob) -> m.MediaItem | None:
        items = await self.store.list_items()
        if job.kind == "episode":
            for it in items:
                if it.kind == "episode" and it.season == job.season \
                        and it.episode == job.episode and (
                            (job.show_imdb_id and it.show_imdb_id == job.show_imdb_id)
                            or (not it.show_imdb_id and job.show_tvdb_id
                                and it.tvdb_id == job.show_tvdb_id)):
                    return it
            return None
        for it in items:
            if it.kind != "movie":
                continue
            if job.imdb_id and it.imdb_id == job.imdb_id:
                return it
            if job.tmdb_id and it.tmdb_id == job.tmdb_id:
                return it
        return None

    def _registration_payload(self, job: IngestJob) -> dict:
        """Canonical `.ids`-plex_path (zelfde conventie als migratie)."""
        uid = job.dedupe_key  # deterministisch uuid uit de key
        import hashlib
        hexdigest = hashlib.md5(uid.encode()).hexdigest()  # noqa: S324
        uuid_like = f"{hexdigest[:8]}-{hexdigest[8:12]}-{hexdigest[12:16]}-" \
                    f"{hexdigest[16:20]}-{hexdigest[20:32]}"
        plex_path = f".ids/{'/'.join(uuid_like[:5])}/{uuid_like}"
        payload = {
            "plex_path": plex_path,
            "kind": job.kind,
            "title": job.title or f"{job.series} S{job.season:02d}E{job.episode:02d}",
            "desired": {},
        }
        if job.kind == "episode":
            payload.update({
                "series": job.series, "season": job.season,
                "episode": job.episode,
                "show_imdb_id": job.show_imdb_id,
                "show_tmdb_id": int(job.show_tmdb_id) if job.show_tmdb_id else None,
                "show_tvdb_id": int(job.show_tvdb_id) if job.show_tvdb_id else None,
            })
        else:
            payload.update({
                "year": job.year,
                "imdb_id": job.imdb_id,
                "tmdb_id": int(job.tmdb_id) if job.tmdb_id else None,
            })
        return payload

    async def _register_item(self, job: IngestJob) -> m.MediaItem | None:
        """Registratie via de normale resolver-pad (identiteits-guard, events,
        bootstrap-resolve). Bootstrap defer vanwege circuit is OK."""
        payload = self._registration_payload(job)
        try:
            item = await self.resolver.register_item(payload)
        except Exception as exc:                       # noqa: BLE001
            await self.store.add_event(
                "ingest_register_failed", error=repr(exc)[:160],
                dedupe_key=job.dedupe_key)
            return None
        return item

    async def _resolve(self, job: IngestJob) -> None:
        """RESOLVING: bounded resolve via de normale engine (scorering,
        identity-gates, validatie). PROVIDER_WAIT komt uit de engine terug."""
        item = await self.store.get_item(job.resolver_item_id or "")
        if item is None:
            await self._set_state(job, JobState.FAILED_RETRYABLE,
                                  error=f"resolver item {job.resolver_item_id} verdwenen",
                                  retry_in=600.0)
            return
        await self._set_state(job, JobState.RESOLVING)
        self.resolver.caches.candidates.invalidate(
            f"{item.kind}:{item.imdb_id}:{item.season}:{item.episode}")
        source = await self.resolver.resolve_item(item, reason="ingest")
        fresh = await self.store.get_item(item.id)
        status = fresh.status if fresh is not None else item.status
        if status == m.ItemStatus.READY.value and source is not None:
            await self._set_state(job, JobState.READY)
        elif status == m.ItemStatus.PROVIDER_WAIT.value:
            retry_in = 0.0
            snap = self.circuit.snapshot().get("scrapers", {})
            for name in self.circuit.blocked():
                retry_in = max(retry_in, snap.get(name, {}).get("retry_in_s", 0))
            self.metrics["provider_waits"] += 1
            await self._set_state(
                job, JobState.PROVIDER_WAIT,
                provider_block="provider unavailable during resolve",
                retry_in=max(retry_in, 60.0))
        elif status == m.ItemStatus.NO_SOURCE.value:
            # ECHT nul resultaten (zoekacties normaal afgerond) — bewijs staat
            # in de events (resolution_failed + reject_summary no_candidates).
            if job.attempts >= int(getattr(self.s, "ingest_job_max_attempts", 8)):
                await self._set_state(
                    job, JobState.BLOCKED_NO_SOURCE,
                    error=f"no usable candidates after {job.attempts} attempts")
            else:
                await self._set_state(
                    job, JobState.FAILED_RETRYABLE,
                    error="true no-source (candidates=0, provider ok)",
                    retry_in=self._job_backoff(job))
        else:
            await self._set_state(job, JobState.FAILED_RETRYABLE,
                                  error=f"resolver status {status}",
                                  retry_in=self._job_backoff(job))

    async def _deliver(self, job: IngestJob) -> None:
        """DELIVERING: canonical-verificatie (resolver-side), symlink in de
        arr-structuur, plex-namespace head+seek probe, scan-trigger,
        part-verificatie; daarna arr-reconcile (Phase 20)."""
        item = await self.store.get_item(job.resolver_item_id or "")
        if item is None or item.status != m.ItemStatus.READY.value:
            await self._set_state(job, JobState.RESOLVING,
                                  error="delivery: item niet READY (terug naar resolve)")
            return
        source = await self.resolver._active_source(item.id)
        ok, why = delivery.verify_canonical_source_ready(item, source)
        if not ok:
            await self._set_state(job, JobState.RESOLVING, error=f"delivery precheck: {why}")
            return
        await self._set_state(job, JobState.DELIVERING)

        link, target = delivery.build_symlink_target(
            item.kind, job.arr_path or "", item.season, item.plex_path,
            source, getattr(self.s, "symlink_root", "/mnt/vm_storage/symlinks"),
            self.sonarr_root_map if job.source == "sonarr" else self.radarr_root_map,
            canonical_root=getattr(self.s, "canonical_root",
                                   delivery.CANONICAL_ROOT))
        # resolver-container schrijft de symlink rechtstreeks (rw-mount)
        delivery.atomic_symlink(link, os.path.join(
            getattr(self.s, "canonical_root", "/mnt/remote/nzbdav"),
            item.plex_path))
        job.delivered_symlink = link
        self.metrics["symlink_created"] += 1
        await self.store.update_job(job, {"delivered_symlink", "updated_at"})
        await self.store.add_event(
            "ingest_symlink_created", item_id=item.id, link=link,
            target=target[:160])

        # canonical+symlink leesprobes in de PLEX-container (autoritatief).
        # De plex-container mount de symlink-tree onder een ANDERE prefix
        # (/symlinks i.p.v. /mnt/vm_storage/symlinks) — probe in plex-ns.
        # Verse .ids-nodes materialiseren bovendien binnen enkele sec; de
        # begrensde retry vangt de overgang.
        link_plex = delivery.to_plex_ns(
            link, getattr(self.s, "symlink_root", ""),
            getattr(self.s, "plex_symlink_root", "/symlinks"))
        await self._set_state(job, JobState.PLEX_REFRESH)
        probe: dict = {}
        probe_tries = max(int(getattr(self.s, "ingest_plex_probe_retries", 6)), 1)
        probe_wait = float(getattr(self.s, "ingest_plex_probe_wait_s", 5.0))
        for attempt in range(probe_tries):
            probe = await self.plex.read_probe(link_plex)
            if probe.get("ok"):
                break
            await asyncio.sleep(probe_wait)
        if not probe.get("ok"):
            # materiaalisatie van de verse .ids-node in de rehydrate-FUSE kan
            # enkele minuten duren — korteriek retryen i.p.v. 300s
            await self._set_state(
                job, JobState.FAILED_RETRYABLE,
                error=f"plex-namespace probe failed: {probe.get('error')}",
                retry_in=90.0)
            return

        # Plex-scan trigger (bounded) en part-verificatie
        section = self.plex_tv_section if item.kind == "episode" \
            else self.plex_movie_section
        scan_dir = os.path.dirname(link_plex)   # plex-namespace-pad!
        try:
            await self.plex.scan_section(section, scan_dir)
        except Exception as exc:                       # noqa: BLE001
            await self.store.add_event("ingest_plex_scan_failed",
                                       item_id=item.id, error=repr(exc)[:160])
        verified = False
        suffix = os.path.basename(link)
        wait = float(getattr(self.s, "ingest_delivery_probe_wait_s", 20.0))
        for attempt in range(int(getattr(self.s, "ingest_delivery_probe_retries", 3))):
            await asyncio.sleep(wait if attempt or True else 0)
            try:
                if item.kind == "episode":
                    res = await self.plex.find_episode(
                        job.series or item.series or "", item.season or 0,
                        item.episode or 0, guid_imdb=item.show_imdb_id,
                        file_suffix=suffix)
                    verified = bool(res.get("present")) and \
                        bool(res.get("file_match"))
                else:
                    res = await self.plex.read_probe(link_plex)
                    verified = bool(res.get("ok"))
                if verified:
                    break
            except Exception as exc:                   # noqa: BLE001
                await self.store.add_event("ingest_plex_verify_error",
                                           item_id=item.id,
                                           error=repr(exc)[:160])
        self.metrics["plex_verifications"] += 1
        if not verified:
            await self._set_state(
                job, JobState.FAILED_RETRYABLE,
                error="plex part-verificatie bleef uit (scan nog niet verwerkt?)",
                retry_in=600.0)
            return
        await self.store.add_event("ingest_plex_verified", item_id=item.id,
                                   link=link)

        # Phase 20: arr laat de deliver herkennen via supported commands
        await self._arr_reconcile(job)

    async def _arr_reconcile(self, job: IngestJob) -> None:
        client = self.sonarr if job.source == "sonarr" else self.radarr
        if client is None:
            await self._set_state(job, JobState.COMPLETED)
            return
        try:
            if job.source == "sonarr":
                series_id = int(job.arr_item_id.split(":")[0])
                cmd = await client.rescan_series(series_id)
                done = await wait_command_done(client, cmd, timeout_s=180.0)
                if not done:
                    await asyncio.sleep(15.0)
                ep = await client.episode(int(job.arr_item_id.split(":")[1]))
                if ep.get("hasFile"):
                    self.metrics["arr_reconciles_ok"] += 1
                    await self._complete(job)
                else:
                    # arr ziet de symlink-structuur (nog) niet als episodefile:
                    # eerst begrensd retryen (scan-cadans), daarna pas blocked
                    self.metrics["arr_reconciles_failed"] += 1
                    if job.attempts >= int(getattr(
                            self.s, "ingest_job_max_attempts", 8)):
                        await self._set_state(
                            job, JobState.BLOCKED_MAPPING,
                            error=f"sonarr hasFile=false na rescan "
                                  f"(episode {ep.get('id')}, bestand bestaat "
                                  f"wel in Plex-namespace)")
                    else:
                        await self._set_state(
                            job, JobState.FAILED_RETRYABLE,
                            error="sonarr hasFile=false na rescan (retry: "
                                  "scan-cadans kan trager zijn dan rescan)",
                            retry_in=600.0)
            else:
                movie_id = int(job.arr_item_id.split(":")[0])
                cmd = await client.rescan_movie(movie_id)
                await wait_command_done(client, cmd, timeout_s=180.0)
                mv = await client.movie(movie_id)
                if mv.get("hasFile") or mv.get("movieFile"):
                    self.metrics["arr_reconciles_ok"] += 1
                    await self._complete(job)
                else:
                    self.metrics["arr_reconciles_failed"] += 1
                    if job.attempts >= int(getattr(
                            self.s, "ingest_job_max_attempts", 8)):
                        await self._set_state(
                            job, JobState.BLOCKED_MAPPING,
                            error="radarr hasFile=false na rescan")
                    else:
                        await self._set_state(
                            job, JobState.FAILED_RETRYABLE,
                            error="radarr hasFile=false na rescan (retry)",
                            retry_in=600.0)
        except ArrUnavailable as exc:
            await self._set_state(job, JobState.FAILED_RETRYABLE,
                                  error=f"arr reconcile: {exc}", retry_in=300.0)

    async def _complete(self, job: IngestJob) -> None:
        job.status = JobState.COMPLETED.value
        job.completed_at = m.now()
        job.last_error = None
        job.provider_block = None
        self.metrics["completed"] += 1
        self.metrics["last_completed_at"] = m.now()
        self.metrics["last_completed"] = job.title
        await self.store.update_job(job, {"status", "completed_at", "last_error",
                                          "provider_block", "updated_at"})
        _job_event(self, "ingest_job_completed", job)

    async def _set_state(self, job: IngestJob, state: JobState, *,
                         error: str | None = None, retry_in: float = 0.0,
                         provider_block: str | None = None) -> None:
        job.status = state.value
        job.updated_at = m.now()
        job.next_attempt_at = m.now() + max(retry_in, 0.0)
        if error is not None:
            job.last_error = error[:400]
        if provider_block is not None:
            job.provider_block = provider_block[:200]
        fields = {"status", "updated_at", "next_attempt_at"}
        if error is not None:
            fields.add("last_error")
        if provider_block is not None:
            fields.add("provider_block")
        await self.store.update_job(job, fields)
        _job_event(self, "ingest_job_state", job, error=job.last_error,
                   retry_in_s=round(retry_in, 0))

    # -------------------------------------------------------- reconciliatie
    async def _reconcile_loop(self) -> None:
        interval = float(getattr(self.s, "ingest_reconcile_interval_s", 1200.0))
        while self._running:
            try:
                await asyncio.sleep(interval)
                if not self._running:
                    return
                await self.reconcile()
            except asyncio.CancelledError:
                return
            except Exception as exc:                   # noqa: BLE001
                event("ingest_reconcile_error", error=repr(exc)[:160])

    async def reconcile(self) -> dict:
        """Phase 15/16: monitored wanted/missing uit beide arr's → idempotente
        enqueue. PROVIDER_WAIT-jobs worden NOOIT opnieuw geënqueue'd: de
        dedupe_key-rij bestaat al en next_attempt_at wordt gerespecteerd."""
        async with self._reconciling:
            self._last_reconcile_at = time.time()
            self.metrics["reconciles"] += 1
            out = {"sonarr": {"seen": 0, "enqueued": 0},
                   "radarr": {"seen": 0, "enqueued": 0}}
            if self.sonarr is not None:
                try:
                    out["sonarr"] = await self._reconcile_sonarr()
                except ArrUnavailable as exc:
                    out["sonarr"] = {"error": str(exc)[:160]}
            if self.radarr is not None:
                try:
                    out["radarr"] = await self._reconcile_radarr()
                except ArrUnavailable as exc:
                    out["radarr"] = {"error": str(exc)[:160]}
            await self.store.add_event("ingest_reconcile", **{
                k: v for k, v in out.items()})
            return out

    async def _reconcile_sonarr(self) -> dict:
        seen = enqueued = 0
        async for rec, series in self.sonarr.wanted_missing():
            if not rec.get("monitored"):
                continue
            seen += 1
            s = series or {}
            job = IngestJob(
                source="sonarr",
                arr_item_id=f"{rec.get('seriesId')}:{rec.get('id')}",
                kind="episode",
                dedupe_key=IngestJob.tv_dedupe_key(
                    s.get("imdbId"), s.get("tvdbId"),
                    rec.get("seasonNumber") or 0, rec.get("episodeNumber") or 0),
                title=rec.get("title") or "",
                series=s.get("title"),
                season=rec.get("seasonNumber"),
                episode=rec.get("episodeNumber"),
                year=s.get("year"),
                show_imdb_id=s.get("imdbId"),
                show_tvdb_id=str(s.get("tvdbId")) if s.get("tvdbId") else None,
                show_tmdb_id=str(s.get("tmdbId")) if s.get("tmdbId") else None,
                arr_path=s.get("path"),
                air_date_utc=rec.get("airDateUtc"),
            )
            _, created = await self.enqueue(job, "reconcile")
            if created:
                enqueued += 1
        return {"seen": seen, "enqueued": enqueued}

    async def _reconcile_radarr(self) -> dict:
        seen = enqueued = 0
        async for rec, movie in self.radarr.wanted_missing():
            if not rec.get("monitored"):
                continue
            seen += 1
            mv = movie or {}
            job = IngestJob(
                source="radarr",
                # Radarr wanted-records ZIJN de movie: rec["id"] is de movie-id
                arr_item_id=f"{mv.get('id') or rec.get('id')}",
                kind="movie",
                dedupe_key=IngestJob.movie_dedupe_key(
                    mv.get("imdbId") or rec.get("imdbId"),
                    str(mv.get("tmdbId") or rec.get("tmdbId") or "")),                title=mv.get("title") or rec.get("title") or "",
                year=mv.get("year") or rec.get("year"),
                imdb_id=mv.get("imdbId") or rec.get("imdbId"),
                tmdb_id=str(mv.get("tmdbId") or rec.get("tmdbId") or ""),
                arr_path=mv.get("path"),
            )
            _, created = await self.enqueue(job, "reconcile")
            if created:
                enqueued += 1
        return {"seen": seen, "enqueued": enqueued}

    # ------------------------------------------------------------ webhooks
    _GRAB_EVENTS = frozenset({"Grab", "Download", "Test"})

    async def handle_webhook(self, source: str, payload: dict) -> dict:
        """Phase 12/13: webhook-event → idempotente enqueue.

        Grab/Download-events worden bewust NIET geënqueue'd zolang er nog een
        legacy download-client (decypharr) actief kan grijpen — dat is de
        dual-ingest-hazard (Phase 22). De reconcile-loop is de authority voor
        wanted-state; webhooks versnellen identity-bearing events. Zodra de
        legacy client uit de ingest-pad is, kan dit omgezet worden.
        """
        ev = (payload.get("eventType") or "").strip()
        if not ev:
            return {"ignored": "no eventType"}
        if ev in self._GRAB_EVENTS:
            return {"event": ev, "enqueued": 0,
                    "ignored": "grab/download event — single-ingest policy "
                               "(reconcile is authority)"}
        out: dict = {"event": ev, "enqueued": 0, "ignored": None}
        if source == "sonarr":
            series = payload.get("series") or {}
            episodes = payload.get("episodes") or [{}]
            for ep in episodes:
                if not ep:
                    continue
                job = IngestJob(
                    source="sonarr",
                    arr_item_id=f"{series.get('id')}:{ep.get('id')}",
                    kind="episode",
                    dedupe_key=IngestJob.tv_dedupe_key(
                        series.get("imdbId"), series.get("tvdbId"),
                        ep.get("seasonNumber") or 0,
                        ep.get("episodeNumber") or 0),
                    title=ep.get("title") or "",
                    series=series.get("title"),
                    season=ep.get("seasonNumber"),
                    episode=ep.get("episodeNumber"),
                    year=series.get("year"),
                    show_imdb_id=series.get("imdbId"),
                    show_tvdb_id=str(series.get("tvdbId")) if series.get("tvdbId") else None,
                    show_tmdb_id=str(series.get("tmdbId")) if series.get("tmdbId") else None,
                    arr_path=series.get("path"),
                )
                if ":None" not in job.arr_item_id and job.show_imdb_id:
                    _, created = await self.enqueue(job, "webhook")
                    if created:
                        out["enqueued"] += 1
                else:
                    out["ignored"] = "incomplete identity in webhook payload"
        elif source == "radarr":
            movie = payload.get("movie") or payload.get("remoteMovie") or {}
            if movie.get("id") or movie.get("tmdbId"):
                job = IngestJob(
                    source="radarr",
                    arr_item_id=f"{movie.get('id')}",
                    kind="movie",
                    dedupe_key=IngestJob.movie_dedupe_key(
                        movie.get("imdbId"),
                        str(movie.get("tmdbId") or "")),
                    title=movie.get("title") or "",
                    year=movie.get("year"),
                    imdb_id=movie.get("imdbId"),
                    tmdb_id=str(movie.get("tmdbId") or ""),
                    arr_path=movie.get("path"),
                )
                if job.imdb_id or job.tmdb_id:
                    _, created = await self.enqueue(job, "webhook")
                    if created:
                        out["enqueued"] += 1
                else:
                    out["ignored"] = "incomplete identity in webhook payload"
        else:
            return {"ignored": f"unknown source {source}"}
        return out

    # ------------------------------------------------------------ status
    async def status(self) -> dict:
        counts = await self.store.ingest_job_counts()
        blocked = self.circuit.blocked()
        snap = self.circuit.snapshot()
        next_retry = 0.0
        for name in blocked:
            next_retry = max(next_retry, snap.get("scrapers", {})
                             .get(name, {}).get("retry_in_s", 0.0))
        return {
            "enabled": True,
            "sonarr": {"enabled": self.sonarr is not None,
                       "url": getattr(self.s, "sonarr_url", "")},
            "radarr": {"enabled": self.radarr is not None,
                       "url": getattr(self.s, "radarr_url", "")},
            "counts_by_state": counts,
            "batch_size": getattr(self.s, "ingest_batch_size", 1),
            "reconcile_interval_s": getattr(
                self.s, "ingest_reconcile_interval_s", 1200.0),
            "last_reconcile_at": self._last_reconcile_at,
            "provider": {"blocked": blocked,
                         "state": snap.get("overall"),
                         "retry_in_s": round(next_retry, 1)},
            "metrics": dict(self.metrics),
        }
