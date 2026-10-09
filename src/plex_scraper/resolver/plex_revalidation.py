"""Gerichte Plex-metadata-revalidatie na bron-generatiewissel.

Bewezen probleem (Dark Matter S02E03, 2026-10-09): een stabiel Plex-pad kan
na een source/JIT-generatiewissel naar nieuwe bytes wijzen terwijl Plex de
media_streams van de oude generatie behoudt. PMS bouwt daaruit een ongeldige
transcoder-mapping (`-codec:0 eac3_eae -codec:1 h264` terwijl de echte
stream 0 hevc-video is) → "Invalid decoder type" → geen eerste segment.

Bewezen kleinste supported repair (fase-1-audit): een item-gerichte
Plex-analyse (`PUT /library/metadata/{ratingKey}/analyze`) bouwt media_parts,
media_streams, codec/resolutie/volgorde en duur/bitrate volledig herbouwd,
scope = exact dit item, geen section-scan. Metadata-refresh (`/refresh`)
en section-scan zijn óf geen-opera óf breder; directe DB-mutatie is niet
nodig zolang dit endpoint bestaat.

Contract:
  * trigger alléén bij material change tussen oude en nieuwe bron
    (size/duur/resolutie/audio/HDR/stream-signatuur);
  * één revalidatie-job per logisch item, laatste generatie wint
    (gen1→gen2→gen3 binnen het cooldown-venster → één job op gen3);
  * toestanden: queued → running → succeeded / failed (met bounded retry);
  * playback tijdens het venster: open_handle wacht bounded op coherentie
    (analyze leest via dezelfde VFS, dus coherent) en valt daarna door
    zonder de sessie te saboteren;
  * het stabiele pad verandert nooit; geen full-library scan.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field

from plex_scraper.common.domain import models as m
from plex_scraper.common.log import event

# toestanden van de revalidatie (observerbaar in trace/cockpit)
QUEUED = "plex_metadata_revalidation_queued"
RUNNING = "plex_metadata_revalidation_running"
SUCCEEDED = "plex_metadata_revalidation_succeeded"
FAILED = "plex_metadata_revalidation_failed"
# meta-status voor observability: coherent = laatste verify OK en geen swap erna
COHERENT = "COHERENT"
STALE = "STALE"
REVALIDATING = "REVALIDATING"


def material_change(old: m.Source | None, new: m.Source,
                    old_duration_s: float | None = None,
                    new_duration_s: float | None = None,
                    size_rel_tol: float = 0.02,
                    min_size_delta_mb: float = 50.0,
                    duration_rel_tol: float = 0.05) -> list[str]:
    """Waarom de nieuwe bron bestaande Plex-media-metadata kan invalidaten.

    Gebruikt alléén metadata die al in de resolver beschikbaar is (release-
    parse + provider size + geleerde duur); downloadt nooit bytes.
    Lege/onzekere velden aan weerszijden tellen niet als wijziging —
    een wissel zonder bewijs is geen trigger (geen onnodige zware analyze).
    """
    reasons: list[str] = []
    if old is None:
        return reasons
    if old.size and new.size:
        delta = abs(new.size - old.size)
        if delta >= min_size_delta_mb * 10**6 and delta > size_rel_tol * max(
                old.size, new.size):
            reasons.append(
                f"size {old.size}->{new.size}")
    if old_duration_s and new_duration_s and old_duration_s > 0:
        if abs(new_duration_s - old_duration_s) > duration_rel_tol * old_duration_s:
            reasons.append(
                f"duration {old_duration_s:.0f}s->{new_duration_s:.0f}s")
    for fieldname in ("resolution", "audio", "hdr"):
        a, b = getattr(old, fieldname), getattr(new, fieldname)
        if a and b and a != b:
            reasons.append(f"{fieldname} {a}->{b}")
    return reasons


@dataclass
class RevalJob:
    item_id: str
    target_generation: int
    reasons: list[str]
    state: str = QUEUED
    attempts: int = 0
    queued_at: float = field(default_factory=time.time)
    started_at: float = 0.0
    finished_at: float = 0.0
    last_result: dict = field(default_factory=dict)


class PlexRevalidator:
    """Per-item state machine rond source swaps (fase 3 + 4 + 5)."""

    def __init__(self, resolver, plex, settings):
        self.resolver = resolver
        self.plex = plex
        self.s = settings
        self._jobs: dict[str, RevalJob] = {}
        self._last_done: dict[str, float] = {}     # item_id -> ts laatste OK
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self.metrics = {"queued": 0, "succeeded": 0, "failed": 0,
                        "deduped": 0, "cooldown_skipped": 0}

    # ------------------------------------------------------------- trigger
    def on_swap(self, item: m.MediaItem, old: m.Source | None,
                new: m.Source) -> list[str]:
        """Hook na _activate: queue revalidatie bij material change.

        Synchronisch en faalveilig aangeroepen vanuit Resolver._activate —
        een crash hier mag de swap nooit terugdraaien.
        """
        if not getattr(self.s, "plex_revalidation_enabled", True):
            return []
        # nieuwe duur is pre-swap onbekend; duur-afwijking wordt in _verify
        # tegen Plex' herbouwde metadata gecontroleerd (fail → retry/flag)
        reasons = material_change(
            old, new, old_duration_s=None, new_duration_s=None,
            size_rel_tol=getattr(self.s, "plex_revalidation_size_rel_tol", 0.02),
            min_size_delta_mb=getattr(
                self.s, "plex_revalidation_min_size_delta_mb", 50.0),
            duration_rel_tol=getattr(
                self.s, "plex_revalidation_duration_rel_tol", 0.05))
        if not reasons:
            return []
        self.queue(item, new.generation, reasons)
        return reasons

    def queue(self, item: m.MediaItem, target_generation: int,
              reasons: list[str]) -> RevalJob:
        """Één job per item; latest generation wins; cooldown-respect."""
        job = self._jobs.get(item.id)
        now = time.time()
        cooldown = getattr(self.s, "plex_revalidation_cooldown_s", 120.0)
        if job is not None and job.state in (QUEUED, RUNNING):
            # coalesce: zelfde job schuift mee naar de nieuwste generatie
            job.target_generation = max(job.target_generation, target_generation)
            job.reasons = reasons
            self.metrics["deduped"] += 1
            event("plex_revalidation_deduped", item_id=item.id,
                  target_generation=job.target_generation)
            return job
        if now - self._last_done.get(item.id, 0.0) < cooldown:
            self.metrics["cooldown_skipped"] += 1
            event("plex_revalidation_cooldown_skip", item_id=item.id,
                  generation=target_generation)
            return RevalJob(item.id, target_generation, reasons,
                            state=SUCCEEDED,
                            last_result={"skipped": "cooldown"})
        job = RevalJob(item.id, target_generation, reasons)
        self._jobs[item.id] = job
        self.metrics["queued"] += 1
        event(QUEUED, item_id=item.id, generation=target_generation,
              reasons=";".join(reasons))
        return job

    # ------------------------------------------------------------ playback
    async def wait_for_coherent(self, item_id: str, timeout: float) -> dict:
        """Playback-guard: wacht bounded tot een lopende revalidatie klaar is.

        De analyze leest via dezelfde VFS als playback, dus bytes zijn
        coherent; alleen Plex' metadata-cache kan kort stale zijn. Time-out →
        gewoon doorgaan (Direct Play blijft veilig; enige risico is een
        transcode-start in het korte venster, daarna repareert een retry zich
        omdat de metadata dan coherent is).
        """
        job = self._jobs.get(item_id)
        if job is None or job.state in (SUCCEEDED, FAILED):
            return {"waited": False}
        deadline = time.monotonic() + max(0.0, timeout)
        while time.monotonic() < deadline:
            job = self._jobs.get(item_id)
            if job is None or job.state in (SUCCEEDED, FAILED):
                break
            await asyncio.sleep(0.25)
        job = self._jobs.get(item_id) or job
        return {"waited": True, "state": job.state if job else None}

    # --------------------------------------------------------------- loop
    def start(self) -> None:
        self._task = asyncio.get_event_loop().create_task(self._run())

    def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.sleep(1.0)
                for job in list(self._jobs.values()):
                    if job.state == QUEUED:
                        await self._process(job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:            # noqa: BLE001 — worker blijft leven
                event("plex_revalidation_worker_error", error=repr(exc)[:160])

    async def _process(self, job: RevalJob) -> None:
        item = await self.resolver.store.get_item(job.item_id)
        if item is None:
            job.state = FAILED
            job.last_result = {"error": "item vanished"}
            return
        if item.generation != job.target_generation:
            # nieuwere generatie actief → oude job is achterhaald; herschrijf
            job.target_generation = item.generation
        job.state = RUNNING
        job.attempts += 1
        job.started_at = time.time()
        event(RUNNING, item_id=job.item_id, attempt=job.attempts)
        try:
            result = await self._revalidate(item)
            job.state = SUCCEEDED if result.get("coherent") else FAILED
            job.last_result = result
            if job.state == SUCCEEDED:
                self.metrics["succeeded"] += 1
                self._last_done[job.item_id] = time.time()
                job.finished_at = time.time()
            else:
                self.metrics["failed"] += 1
                if job.attempts >= getattr(
                        self.s, "plex_revalidation_attempts", 3):
                    job.finished_at = time.time()
                    event("plex_metadata_revalidation_failed", item_id=job.item_id,
                          result=json.dumps(result)[:200])
                else:
                    job.state = QUEUED          # bounded retry, exponential
                    await asyncio.sleep(min(30.0, 2.0 ** job.attempts))
        except Exception as exc:                # noqa: BLE001
            job.state = QUEUED if job.attempts < getattr(
                self.s, "plex_revalidation_attempts", 3) else FAILED
            job.last_result = {"error": repr(exc)[:160]}
            if job.state == FAILED:
                job.finished_at = time.time()

    async def _revalidate(self, item: m.MediaItem) -> dict:
        """Analyze + verificatie tegen de actuele bron (fase 5)."""
        source = None
        for src in await self.resolver.store.list_sources(item.id):
            if src.state == m.SourceState.ACTIVE.value:
                source = src
                break
        if source is None:
            return {"coherent": False, "error": "no active source"}
        rk = await self.plex.find_rating_key_by_path(item.plex_path)
        if not rk:
            return {"coherent": False, "error": "rating_key_not_found"}
        ana = await self.plex.analyze_item(rk)
        if ana.get("error"):
            return {"coherent": False, "error": ana["error"][:120]}
        # analyze is async aan Plex-zijde: kort rusten en daarna echt checken
        info: dict = {"ok": False}
        for _ in range(10):
            await asyncio.sleep(1.0)
            info = await self.plex.get_media_info(rk)
            if info.get("ok"):
                break
        if not info.get("ok"):
            return {"coherent": False, "error": "media_info_unavailable",
                    "rating_key": rk}
        mismatches = self._verify(item, source, info)
        return {"coherent": not mismatches, "rating_key": rk,
                "plex": {k: info.get(k) for k in (
                    "container", "video_codec", "width", "height",
                    "duration_s", "audio_codec", "size", "streams")},
                "mismatches": mismatches}

    def _verify(self, item: m.MediaItem, source: m.Source, info: dict) -> list[str]:
        """Vergelijk Plex-media-metadata met de bekende bronkarakteristieken.

        Alléén harde feiten uit de bron-vergelijking; onbekende Plex-waarden
        (None) zijn geen mismatch — een geen-uitspraak mag nooit als faal
        tellen, een vals succes wél worden voorkomen door harde velden.
        """
        mm: list[str] = []
        if source.size and info.get("size"):
            if abs(info["size"] - source.size) > max(
                    10**6, 0.05 * source.size):
                mm.append(f"size {source.size}->{info['size']}")
        exp_res = (source.resolution or "").lower()
        if exp_res and info.get("height"):
            want = {"2160p": 2160, "1080p": 1080, "720p": 720}.get(exp_res)
            if want and abs(info["height"] - want) > 40:
                mm.append(f"resolution {exp_res}->{info['height']}p")
        if item.duration_s and info.get("duration_s"):
            if abs(info["duration_s"] - item.duration_s) > 0.1 * item.duration_s:
                mm.append(f"duration {item.duration_s:.0f}->{info['duration_s']:.0f}")
        return mm

    # ------------------------------------------------------ observability
    def snapshot(self, item_id: str) -> dict:
        """Cockpit/trace-status: COHERENT / REVALIDATING / STALE / FAILED."""
        job = self._jobs.get(item_id)
        if job is None:
            return {"state": COHERENT, "job": None}
        if job.state in (QUEUED, RUNNING):
            label = REVALIDATING
        elif job.state == SUCCEEDED:
            label = COHERENT
        else:
            label = STALE
        return {"state": label, "job": {
            "state": job.state,
            "target_generation": job.target_generation,
            "reasons": job.reasons,
            "attempts": job.attempts,
            "queued_at": job.queued_at,
            "finished_at": job.finished_at or None,
            "last_result": job.last_result,
        }}
