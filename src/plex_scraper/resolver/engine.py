"""Resolution engine (FASE 6 + 7): state machine, generations, pinning.

NO_SOURCE → RESOLVING → CANDIDATE_VALIDATION → READY
READY --failure--> SOURCE_FAILED --> RESOLVING --> ... --> READY (generation N+1)

One broken torrent can never break the item while alternatives exist: the
engine walks ranked candidates, marks failures bad for a TTL, and activates
the first candidate whose bytes validate.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from collections import defaultdict
import time
from dataclasses import dataclass, field

from plex_scraper.common.config import Settings
from plex_scraper.common.domain import models as m
from plex_scraper.common.log import event
from plex_scraper.scraper.providers.base import DebridProvider, NotReadyError, ProviderError
from plex_scraper.common.scoring.release_parser import parse_release
from plex_scraper.scraper.scrapers.base import Scraper, TorrentCandidate
from plex_scraper.resolver.selfheal import identity_gate
from plex_scraper.resolver import media as m_prof
from plex_scraper.resolver.jit import JitConfig, JitController
from plex_scraper.resolver.runtime import DeliveryMonitor, DEGRADED as RT_DEGRADED
from plex_scraper.resolver.stream import AdaptiveRangeReader
from .caches import CacheSet
from .store import Store


@dataclass
class SessionContext:
    session: m.Session
    source: m.Source
    reader: AdaptiveRangeReader
    opened_at: float = field(default_factory=time.monotonic)
    last_read_at: float = 0.0     # wall-clock; 0 = nog niet gelezen
    required_mbit: float = 0.0    # media-aware vereiste (FASE 2)
    bitrate_confidence: str = "floor"
    jit_decision: object = None   # JitDecision (FASE 3)
    monitor: object = None        # DeliveryMonitor (runtime delivery)


GB = 10**9


class UnresolvedError(Exception):
    pass


class Resolver:
    def __init__(self, settings: Settings, store: Store, provider: DebridProvider,
                 scrapers: list[Scraper], scorer, caches: CacheSet):
        self.s = settings
        self.store = store
        self.provider = provider
        self.scrapers = scrapers
        self.scorer = scorer
        self.caches = caches
        self.sessions: dict[str, SessionContext] = {}
        self._resolve_locks: dict[str, asyncio.Lock] = {}
        self._sem = asyncio.Semaphore(settings.upstream_concurrency)
        self.metrics = {
            "resolutions": 0, "fallbacks": 0, "generation_switches": 0,
            "reads": 0, "read_bytes": 0, "resolve_latency_sum": 0.0,
            "request_latency_sum": 0.0, "request_count": 0,
            # adaptive read-ahead (FASE 8/12/20)
            "prefetch_bytes": 0, "prefetch_cancelled_bytes": 0,
            "prefetch_hits": 0, "prefetch_errors": 0,
            "adaptive_fallbacks": 0, "two_way_sessions": 0,
        }
        # FASE 3/9/12: JIT playback preflight + quality-preserving failover
        self.jit = JitController(self, JitConfig(
            enabled=getattr(settings, "jit_enabled", True),
            preflight_min_mbit=getattr(settings, "jit_preflight_min_mbit", 40.0),
            fast_ratio=getattr(settings, "jit_fast_ratio", 1.2),
            degraded_ratio=getattr(settings, "jit_degraded_ratio", 0.8),
            ttfb_max_s=getattr(settings, "jit_ttfb_max_s", 5.0),
            max_wait_s=getattr(settings, "jit_max_wait_s", 12.0),
            probe_candidates=getattr(settings, "jit_probe_candidates", 3),
            min_gain=getattr(settings, "jit_min_gain", 1.5),
            allow_minor_deviation=getattr(settings, "jit_allow_minor_deviation", True),
            allow_quality_downgrade=getattr(settings, "jit_allow_quality_downgrade", False),
            delivery_bad_ttl_s=getattr(settings, "jit_delivery_bad_ttl_s", 3600.0),
            probe_parallel=getattr(settings, "jit_probe_parallel", 2),
            rescue_margin=getattr(settings, "jit_rescue_margin", 1.2),
            confirm_cached_fast=getattr(settings, "jit_confirm_cached_fast", True),
        ))
        # FASE 10/19: runtime-failover administratie (switch-budget per item)
        self._failover_budget: dict[str, tuple[int, float]] = {}
        self.runtime_metrics = defaultdict(int)

    # ------------------------------------------------------------ lifecycle
    async def _evt(self, event_kind: str, item: m.MediaItem | None = None,
                   item_id: str | None = None, generation: int | None = None, **fields) -> None:
        """Persist an event AND emit it as a structured log line (FASE 16)."""
        if item_id is None and item is not None:
            item_id = item.id
        if generation is None and item is not None:
            generation = item.generation
        await self.store.add_event(event_kind, item_id, generation, **fields)
        event(event_kind, item_id=item_id, generation=generation, **fields)

    async def close(self) -> None:
        client = getattr(self.provider, "_client", None)
        if client is not None:
            await client.aclose()

    # ------------------------------------------------------------- item mgmt
    async def register_item(self, payload: dict) -> m.MediaItem:
        plex_path = self._sanitize_plex_path(payload["plex_path"])
        item = m.MediaItem(
            id=m.new_id(), kind=payload.get("kind", "movie"),
            title=payload.get("title") or plex_path, plex_path=plex_path,
            series=payload.get("series"), season=payload.get("season"),
            episode=payload.get("episode"), year=payload.get("year"),
            imdb_id=payload.get("imdb_id"), tmdb_id=payload.get("tmdb_id"),
            tvdb_id=payload.get("tvdb_id"), desired=payload.get("desired") or {},
        )
        await self.store.create_item(item)
        await self._evt("item_registered", item_id=item.id, plex_path=plex_path, kind=item.kind)
        # bootstrap: one source resolved immediately so Plex can analyze real bytes
        await self.resolve_item(item, reason="bootstrap")
        return item

    @staticmethod
    def _sanitize_plex_path(path: str) -> str:
        parts = [p for p in path.replace("\\", "/").split("/") if p not in ("", ".")]
        if not parts or any(p == ".." for p in parts):
            raise ValueError(f"unsafe plex_path: {path!r}")
        return "/".join(parts)

    async def delete_item(self, item_id: str) -> bool:
        item = await self.store.get_item(item_id)
        if item is None:
            return False
        for handle in [h for h, ctx in self.sessions.items()
                       if ctx.session.media_item_id == item_id]:
            await self.release(handle)
        await self.store.delete_item(item_id)
        await self._evt("item_deleted", item_id=item_id)
        return True

    async def update_desired(self, item_id: str, desired: dict) -> m.MediaItem | None:
        item = await self.store.get_item(item_id)
        if item is None:
            return None
        item.desired = desired
        await self.store.update_runtime(item)
        return item

    # ------------------------------------------------------ state machine
    async def resolve_item(self, item: m.MediaItem, reason: str) -> m.Source | None:
        lock = self._resolve_locks.setdefault(item.id, asyncio.Lock())
        async with lock:
            return await self._resolve_locked(item, reason)

    async def _resolve_locked(self, item: m.MediaItem, reason: str) -> m.Source | None:
        started = time.monotonic()
        self.metrics["resolutions"] += 1
        previous = await self._active_source(item.id)
        item.status = m.ItemStatus.RESOLVING.value
        await self.store.update_runtime(item)
        try:
            return await self._resolve_attempt(item, reason, previous, started)
        except Exception as exc:
            # crash-safe (DOEL 3): een exception mag de item-status nooit
            # vastlaten in een tussenstaat — reconcilieer naar de vorige
            # bruikbare state en rapporteer
            await self._reconcile_after_crash(item, reason, exc)
            raise

    async def _resolve_attempt(self, item: m.MediaItem, reason: str,
                               previous: m.Source | None,
                               started: float) -> m.Source | None:
        candidates = await self._gather_candidates(item)
        ranked = await self._rank_candidates(item, candidates)
        item.status = m.ItemStatus.CANDIDATE_VALIDATION.value
        await self.store.update_runtime(item)

        await self._evt("resolution_started", item=item, reason=reason,
              candidate_count=len(candidates), ranked_count=len(ranked),
              current_generation=item.generation)

        # cached-first ordering: cached candidates are instantly verifiable;
        # uncached ones need a provider add (createtorrent, 60/h account cap)
        # plus download wait, so they run afterwards and budget-limited.
        ranked = self._cached_first(ranked)

        # candidates marked temporary-bad are skipped BEFORE the provider-add
        # budget is considered, so dead releases never block fresh ones
        sources = await self.store.list_sources(item.id)
        bad_hashes = {s.info_hash for s in sources if s.is_bad()}
        # FASE 13: JIT delivery-bad bronnen tijdelijk uitgesloten (inhoudelijk
        # geldig, alleen op dit moment te traag) — TTL laat ze terugkomen
        delivery_bad = {s.info_hash for s in sources if s.is_delivery_bad()}

        fallback_count = 0
        provider_adds = 0
        for cand, _score in ranked:
            # DOEL 2: de huidige actieve bron is grandfathered — hij is
            # historisch al identiteits-geverifieerd en mag bij re-resolve
            # opnieuw gevalideerd/herkozen worden, ook als zijn release-naam
            # (legacy season-pack) niet aan de identity-regels voldoet.
            # NIEUWE candidates blijven door de harde gate.
            grandfathered = (previous is not None
                             and cand.info_hash == previous.info_hash)
            if not grandfathered:
                ok, why = identity_gate(item.title, item.series,
                                        item.season, item.episode,
                                        cand.torrent_name, item.year)
                if not ok:
                    await self._evt("candidate_identity_rejected", item=item,
                                    hash=cand.info_hash, name=cand.torrent_name,
                                    reason=why)
                    continue
                if cand.info_hash in bad_hashes or cand.info_hash in delivery_bad:
                    continue
            elif cand.info_hash in bad_hashes or cand.info_hash in delivery_bad:
                # grandfathered probeert ondanks bad-marking opnieuw: de
                # validatie-probe beslist (transiënte 400's zijn geen bewijs)
                await self._evt("candidate_grandfathered_retry", item=item,
                                hash=cand.info_hash, name=cand.torrent_name)
            if cand.info_hash not in self._cached_hashes():
                if provider_adds >= self.s.max_provider_adds_per_resolve:
                    await self._evt("resolution_skip_uncached", item=item,
                                    hash=cand.info_hash,
                                    reason="provider_add_budget_exhausted")
                    continue
                provider_adds += 1
            t0 = time.monotonic()
            source = await self._validate_candidate(item, cand)
            validation_latency = time.monotonic() - t0
            if source is None:
                fallback_count += 1
                continue
            await self._activate(item, source, previous, reason)
            self.metrics["resolve_latency_sum"] += time.monotonic() - started
            await self._evt("resolution_succeeded", item=item, reason=reason,
                  selected_candidate={"hash": source.info_hash, "file": source.file_name,
                                      "score": source.score},
                  candidate_count=len(candidates), fallback_count=fallback_count,
                  validation_latency=round(validation_latency, 3),
                  resolution_latency=round(time.monotonic() - started, 3),
                  generation=source.generation,
                  reason_for_source_switch=(
                      "first_resolution" if previous is None
                      else f"{reason}:previous_source_replaced"))
            return source

        # DOEL 1: een gefaalde repair mag een nog-leesbare actieve bron nooit
        # omzetten naar NO_SOURCE. Probe de vorige bron; leest die nog, dan
        # behouden we hem en blijft het item READY (alleen event/history).
        if previous is not None and await self._probe_readable(previous):
            item.status = m.ItemStatus.READY.value
            await self.store.update_runtime(item)
            self.metrics["resolve_latency_sum"] += time.monotonic() - started
            await self._evt("repair_kept_current", item=item, reason=reason,
                  hash=previous.info_hash,
                  candidate_count=len(candidates), fallback_count=fallback_count,
                  resolution_latency=round(time.monotonic() - started, 3),
                  reason_for_source_switch=f"{reason}:kept_current_source")
            return previous

        item.status = m.ItemStatus.NO_SOURCE.value
        await self.store.update_runtime(item)
        await self._evt("resolution_failed", item=item, reason=reason,
              candidate_count=len(candidates), fallback_count=fallback_count,
              resolution_latency=round(time.monotonic() - started, 3))
        return None

    async def _probe_readable(self, src: m.Source) -> bool:
        """Twee kleine leesprobes op een bestaande bron (offset 0 en midden).

        Alleen als BEIDE falen geldt de bron als onbruikbaar; één transient
        dat faalt is geen bewijs (de 400-retry zit al in read_range).
        """
        size = max(int(src.size or 0), 1)
        for offset in (0, size // 2):
            try:
                torrent = await self.provider.ensure_torrent(
                    src.info_hash, src.torrent_name)
                url = await self._link_for(src, torrent)
                data = await self.provider.read_range(
                    url, min(offset, max(0, size - 65536)), 65536)
                if data:
                    return True
            except Exception as exc:                     # noqa: BLE001
                await self._evt("keep_current_probe_failed",
                                item_id=src.media_item_id, hash=src.info_hash,
                                offset=offset, error=repr(exc)[:120])
        return False

    async def _reconcile_after_crash(self, item: m.MediaItem,
                                     reason: str, exc: Exception) -> None:
        """Reconcilieer een gecrashte resolve: actieve bron → READY, anders
        NO_SOURCE. Nooit permanent in RESOLVING/CANDIDATE_VALIDATION blijven
        hangen (de sweeper slaat tussenstanden over)."""
        try:
            previous = await self._active_source(item.id)
            item.status = (m.ItemStatus.READY.value if previous is not None
                           else m.ItemStatus.NO_SOURCE.value)
            await self.store.update_runtime(item)
            await self._evt("resolution_crashed", item=item, reason=reason,
                  error=repr(exc)[:160], reconciled_status=item.status)
        except Exception as inner:                       # noqa: BLE001
            log.error("reconcile_after_crash faalde voor %s: %r",
                      item.plex_path, inner)

    def _cached_hashes(self) -> set:
        hashes = set()
        for value in self.caches.checkcached._data.values():
            hashes.update((value.value or {}).keys())
        return hashes

    def _cached_first(self, ranked):
        cached, uncached = [], []
        for entry in ranked:
            (cached if entry[0].info_hash in self._cached_hashes() else uncached).append(entry)
        return cached + uncached

    async def _gather_candidates(self, item: m.MediaItem) -> list[TorrentCandidate]:
        key = f"{item.kind}:{item.imdb_id}:{item.season}:{item.episode}"
        cached = self.caches.candidates.get(key)
        if cached is not None:
            return cached
        results: list[TorrentCandidate] = []
        for scraper in self.scrapers:
            try:
                results.extend(await scraper.search(item.search_key()))
            except Exception as exc:                    # scraper failure is not fatal
                await self._evt("scraper_error", item=item, scraper=scraper.name, error=repr(exc))
        # dedupe on (hash, filename-hint) — same hash often appears with indexes
        seen: set[tuple[str, str]] = set()
        unique = []
        for cand in results:
            k = (cand.info_hash, cand.file_name or "")
            if k not in seen:
                seen.add(k)
                unique.append(cand)
        self.caches.candidates.put(key, unique)
        return unique

    async def _rank_candidates(self, item: m.MediaItem,
                               candidates: list[TorrentCandidate]) -> list[tuple[TorrentCandidate, float]]:
        if not candidates:
            return []
        hashes = [c.info_hash for c in candidates]
        cached_map = self.caches.checkcached.get("batch:" + ",".join(sorted(hashes)))
        if cached_map is None:
            try:
                cached_map = await self.provider.availability(hashes)
            except ProviderError as exc:
                await self._evt("availability_error", item=item, error=repr(exc))
                cached_map = {}
            self.caches.checkcached.put("batch:" + ",".join(sorted(hashes)), cached_map)

        scored: list[tuple[TorrentCandidate, float]] = []
        for cand in candidates:
            breakdown = self.scorer.score(
                cand.torrent_name, cached=cand.info_hash in cached_map,
                seeders=cand.seeders, size=cand.size)
            if breakdown.rejected:
                await self._evt("candidate_rejected", item=item, hash=cand.info_hash,
                                name=cand.torrent_name, rejects=breakdown.rejects)
                continue
            scored.append((cand, breakdown.total))
        scored.sort(key=lambda t: t[1], reverse=True)
        return scored

    async def _validate_candidate(self, item: m.MediaItem,
                                  cand: TorrentCandidate) -> m.Source | None:
        """Real validation: provider knows it AND bytes come back (206 probe)."""
        src = m.Source(
            id=m.new_id(), media_item_id=item.id, generation=item.generation,
            provider=self.provider.name, info_hash=cand.info_hash,
            torrent_name=cand.torrent_name, file_id=cand.file_index or 0,
            file_name=cand.file_name, size=cand.size or 0,
            seeders=cand.seeders,
        )
        existing = await self._similar_source(item, src)
        if existing is not None:
            src = existing
        if src.is_bad():
            return None                                  # temporary bad TTL

        breakdown = self.scorer.score(
            cand.torrent_name, cached=src.cached, seeders=cand.seeders, size=cand.size)
        src.score, src.score_json = breakdown.total, self._breakdown_json(breakdown)
        parsed = parse_release(cand.torrent_name)
        src.resolution = parsed.resolution
        src.audio, src.release_type, src.language = parsed.audio, parsed.release_type, parsed.language
        src.hdr = parsed.video if parsed.video in ("dolby_vision", "hdr10") else None

        try:
            torrent = await self.provider.ensure_torrent(cand.info_hash, cand.torrent_name)
            choice = None
            if cand.file_index is not None and cand.file_index in torrent.files:
                choice = (cand.file_index, torrent.files[cand.file_index])
            elif torrent.files:
                hint = cand.file_name
                choice = self.provider.pick_file(torrent, hint) \
                    if hasattr(self.provider, "pick_file") else next(iter(torrent.files.items()))
            if choice is None:
                raise NotReadyError("torrent has no files")
            src.file_id, meta = choice
            src.file_name, known_size = meta["name"], int(meta["size"])
            # release-size sanity: guards against pack .nfo picks and
            # mislabeled sample/segment releases (torrentio noise)
            min_bytes = self.s.min_media_movie_mb if item.kind == "movie" \
                else self.s.min_media_episode_mb
            min_media = min_bytes << 20
            if known_size and known_size < min_media:
                raise NotReadyError(
                    f"release too small for {item.kind} "
                    f"({known_size / (1 << 20):.0f}MB) - likely mislabeled")
            src.size = known_size or src.size
            url = await self._link_for(src, torrent)
            probe = self.s.validation_probe_bytes
            head = await self.provider.read_range(url, 0, probe)
            middle = await self.provider.read_range(
                url, max(0, src.size // 2), min(probe, max(1, src.size - src.size // 2)))
            if not head or not middle:
                raise ProviderError("probe read returned no bytes")
        except (NotReadyError, ProviderError) as exc:
            src.failure_count += 1
            src.bad_until = self._bad_until(src.failure_count)
            src.state = m.SourceState.FAILED.value
            await self.store.upsert_source(src)
            await self._evt("candidate_failed", item=item, hash=cand.info_hash,
                  name=cand.torrent_name, error=str(exc),
                  bad_until=src.bad_until, failure_count=src.failure_count)
            return None

        src.cached = bool(torrent.cached)
        src.state = m.SourceState.CANDIDATE.value
        src.last_verified = m.now()
        # final breakdown includes the cached bonus so the persisted score
        # matches the one the ranking used
        breakdown = self.scorer.score(
            cand.torrent_name, cached=src.cached, seeders=cand.seeders, size=src.size)
        src.score, src.score_json = breakdown.total, self._breakdown_json(breakdown)
        src = await self.store.upsert_source(src)
        return src

    def _bad_until(self, failure_count: int) -> float:
        ttl = self.s.cache_bad_ttl * (2 ** (failure_count - 1))
        return m.now() + min(ttl, self.s.cache_bad_ttl_max)

    @staticmethod
    def _breakdown_json(breakdown) -> dict:
        return {"total": breakdown.total,
                "lines": [[l.label, l.points] for l in breakdown.lines],
                "rejects": breakdown.rejects}

    async def _similar_source(self, item: m.MediaItem, src: m.Source) -> m.Source | None:
        for existing in await self.store.list_sources(item.id):
            if existing.info_hash == src.info_hash:
                return existing
        return None

    async def _activate(self, item: m.MediaItem, source: m.Source,
                        previous: m.Source | None, reason: str) -> None:
        switched = previous is None or previous.id != source.id
        if switched:
            item.generation += 1
            self.metrics["generation_switches"] += 1
        source.generation = item.generation
        source.state = m.SourceState.ACTIVE.value
        await self.store.retire_active(item.id, except_source_id=source.id)
        await self.store.update_source(source)
        item.status = m.ItemStatus.READY.value
        await self.store.update_runtime(item)

    async def _active_source(self, item_id: str) -> m.Source | None:
        for src in await self.store.list_sources(item_id):
            if src.state == m.SourceState.ACTIVE.value:
                return src
        return None

    # ------------------------------------------------------------ sessions
    def media_profile(self, item: m.MediaItem, size: int) -> m_prof.MediaProfile:
        """FASE 2/3: per-media throughput-profiel (bitrate + confidence)."""
        return m_prof.build_profile(
            size_bytes=size or None,
            media_bitrate_mbit=item.media_bitrate_mbit,
            duration_s=item.duration_s,
            floor_mbit=self.s.playback_min_mbit)

    def two_way_for(self, item: m.MediaItem, required_mbit: float) -> bool:
        """FASE 7/12: 2-way read-ahead alleen als deze media er belang bij
        heeft (required boven drempel) óf al gedegradeerd is gemeten.
        Lage-bitrate content blijft gewoon single-stream."""
        if not getattr(self.s, "stream_two_way_enabled", True):
            return False
        if required_mbit >= self.s.adaptive_two_way_min_mbit:
            return True
        sweeper = getattr(self, "_sweeper", None)
        if sweeper is not None and item.plex_path in getattr(
                sweeper, "degraded_throughput", set()):
            return True
        return False

    async def open_handle(self, item_id: str, two_way: bool | None = None) -> SessionContext:
        item = await self.store.get_item(item_id)
        if item is None:
            raise KeyError(f"unknown media item {item_id}")
        source = await self._active_source(item_id)
        if item.status != m.ItemStatus.READY.value or source is None or source.is_bad():
            item.status = (m.ItemStatus.SOURCE_FAILED.value
                           if source is not None else item.status)
            if item.status == m.ItemStatus.SOURCE_FAILED.value:
                await self.store.update_runtime(item)
            source = await self.resolve_item(
                item, reason="open_needs_source")
        if source is None:
            raise UnresolvedError(f"no working source for {item.plex_path}")
        profile = self.media_profile(item, source.size)
        required = profile.required_mbit(self.s.sweeper_throughput_margin)
        # FASE 1/3: JIT playback preflight — alleen op het echte play-signaal
        # (open_handle zónder two_way=0); background-checks (two_way=0) slaan
        # dit over. Bounded: FAST/MARGINAL direct, DEGRADED ≤ jit_max_wait_s.
        jit_decision = None
        jit_note = ""
        item._jit_risk = profile.risk
        if two_way != 0 and required > 0:
            jit_decision = await self.jit.preflight_async(item, source, required)
            jit_note = jit_decision.note
        is_playback = two_way is not 0               # None/True = echte play
        if two_way is None:
            two_way = self.two_way_for(item, required)
        reader = AdaptiveRangeReader(self, source.id, source.size,
                                     self.s.stream_readahead_bytes,
                                     two_way=two_way,
                                     fallback_after_errors=self.s.adaptive_fallback_errors)
        if two_way:
            self.metrics["two_way_sessions"] += 1
            # FASE 18: playback-scoped hot spare (link-prewarm, geen benchmark)
            if self.jit.cfg.hot_spare:
                asyncio.create_task(self.jit.warm_spare(item, source))
            # metadata-learning: duration bij play (playback-scoped) →
            # media-bitrate wordt 'derived' i.p.v. floor-schatting
            if (getattr(self.s, "tautulli_url", "") and getattr(self.s, "tautulli_apikey", "")
                    and item.media_bitrate_mbit is None and item.duration_s is None
                    and item.kind == "movie" and (source.size or 0) > 20 * GB):
                asyncio.create_task(self._learn_duration(item))
        session = m.Session(handle=m.new_id(), media_item_id=item_id,
                            source_id=source.id, generation=source.generation,
                            size=source.size)
        await self.store.create_session(session)
        ctx = SessionContext(session=session, source=source, reader=reader)
        ctx.required_mbit = round(required, 1)
        ctx.bitrate_confidence = profile.confidence
        ctx.jit_decision = jit_decision
        # FASE 5: runtime delivery-monitor op echte playback (niet background)
        if is_playback:
            ctx.monitor = DeliveryMonitor(
                item.plex_path,
                media_bitrate=profile.bitrate_mbit,
                target_mbit=required,
                degraded_ratio=self.s.jit_degraded_ratio,
                stall_s=self.s.jit_stall_s)
            self.metrics.setdefault("active_delivery_monitors", 0)
            self.metrics["active_delivery_monitors"] += 1
        self.sessions[session.handle] = ctx
        # DEEL B: startup-watchdog — 00:00-hang mag niet eeuwig duren
        ctx.startup = None
        if is_playback:
            ctx.startup = {"state": "BUFFERING_STARTUP",
                           "opened_at": time.time(), "first_byte_at": None,
                           "bytes": 0,
                           "deadline_s": self.s.jit_startup_first_byte_s}
            asyncio.create_task(self._startup_watchdog(ctx, item, required))
        await self._evt("session_opened", item_id=item_id, handle=session.handle,
              generation=source.generation, size=source.size,
              read_mode="2way" if two_way else "single",
              required_mbit=round(required, 1))
        return ctx

    async def _learn_duration(self, item: m.MediaItem) -> None:
        """Playback-scoped metadata-learning: filmduur uit de Tautulli-
        historie → media-bitrate 'derived' (betere drempels, geen floor)."""
        try:
            import httpx
            title = (item.title or "").strip()
            if not title:
                return
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as c:
                resp = await c.get(
                    f"{self.s.tautulli_url.rstrip('/')}/api/v2",
                    params={"apikey": self.s.tautulli_apikey, "cmd": "get_history",
                            "search": title, "length": 5})
                rows = (resp.json().get("response", {}).get("data", {}) or {}).get("data") or []
            for row in rows:
                dur_ms = row.get("duration")
                if dur_ms and (not item.year or str(item.year) in str(row.get("year") or "")):
                    item.duration_s = dur_ms / 1000.0
                    item.media_bitrate_mbit = None
                    await self.store.update_runtime(item)
                    await self._evt("media_duration_learned", item=item,
                                    duration_s=item.duration_s)
                    return
        except Exception as exc:                        # noqa: BLE001
            log.info("duration-learning overgeslagen: %r", exc)

    def get_session(self, handle: str) -> SessionContext:
        ctx = self.sessions.get(handle)
        if ctx is None or ctx.session.state != m.SessionState.OPEN.value:
            raise KeyError("unknown or closed handle")
        return ctx

    async def read(self, handle: str, offset: int, length: int) -> bytes:
        ctx = self.get_session(handle)
        t0 = time.monotonic()
        data = await ctx.reader.read(offset, length)
        ctx.last_read_at = time.time()
        if getattr(ctx, "startup", None) and data:
            su = ctx.startup
            if su.get("first_byte_at") is None:
                su["first_byte_at"] = time.time()
                self.runtime_metrics["startup_time_to_first_byte_max"] = round(
                    su["first_byte_at"] - su["opened_at"], 2)
                asyncio.create_task(self._evt("startup_first_byte",
                                              item_id=ctx.session.media_item_id,
                                              ttfb_s=su["first_byte_at"] - su["opened_at"]))
            su["bytes"] += len(data)
            if su["bytes"] >= 4 * 1048576 and su["state"] != "STARTED":
                su["state"] = "STARTED"
                asyncio.create_task(self._evt("startup_started",
                                              item_id=ctx.session.media_item_id))
        self.metrics["reads"] += 1
        self.metrics["read_bytes"] += len(data)
        self.metrics["request_count"] += 1
        self.metrics["request_latency_sum"] += time.monotonic() - t0
        ctx.session.read_count += 1
        # FASE 5/9: runtime delivery-monitor — gebruikt de bytes die toch al
        # voor Plex worden gelezen; geen extra provider-load
        if ctx.monitor is not None and data:
            # FASE 8: seek (grote offset-sprong) telt niet als stall
            seek = (ctx.monitor.last_offset >= 0
                    and abs(offset - ctx.monitor.last_offset) > 50 * 1048576)
            if time.time() < ctx.monitor._pause_until:
                seek = True
            ctx.monitor.feed(len(data), time.monotonic() - t0, seek=seek)
            ctx.monitor.last_offset = offset
            snap = ctx.monitor.evaluate()
            if snap["state"] != ctx.monitor.last_state_reported:
                ctx.monitor.last_state_reported = snap["state"]
                await self._evt("runtime_delivery_" + snap["state"].lower(),
                                item_id=ctx.session.media_item_id,
                                handle=handle, mbit=snap.get("rolling_mbit"),
                                min_realtime=snap.get("min_realtime"),
                                target=snap.get("target"))
                self.runtime_metrics["runtime_delivery_" + snap["state"].lower()] += 1
            if (snap["state"] == RT_DEGRADED
                    and not ctx.monitor.failed_over
                    and self._failover_budget_ok(ctx.session.media_item_id)):
                ctx.monitor.failed_over = True
                self.runtime_metrics["runtime_failover_searches"] += 1
                t_detect = snap["elapsed_s"]
                self.runtime_metrics["time_to_detect_degraded_max"] = max(
                    self.runtime_metrics.get("time_to_detect_degraded_max", 0.0),
                    t_detect)
                asyncio.create_task(self._runtime_failover(ctx, snap))
        return data

    async def _startup_watchdog(self, ctx, item: m.MediaItem, required: float) -> None:
        """DEEL B: 00:00-hang — geen first byte / geen progress binnen de
        deadline → STARTUP_FAILED → rescue-search + reconnect."""
        try:
            while True:
                await asyncio.sleep(2.0)
                su = getattr(ctx, "startup", None)
                if su is None or su["state"] in ("STARTED", "STARTUP_FAILED"):
                    return
                elapsed = time.time() - su["opened_at"]
                has_first = su.get("first_byte_at") is not None
                if not has_first and elapsed >= su["deadline_s"]:
                    reason = "NO_FIRST_BYTE"
                elif (has_first and elapsed >= su["opened_at"] + su["deadline_s"] * 2
                      and su["bytes"] < 1048576):
                    reason = "ZERO_PROGRESS"
                else:
                    continue
                su["state"] = "STARTUP_FAILED"
                self.runtime_metrics["startup_failures"] += 1
                await self._evt("startup_failed", item=item, reason=reason,
                                elapsed_s=round(elapsed, 1))
                if not self._failover_budget_ok(item.id):
                    return
                self.runtime_metrics["runtime_failover_searches"] += 1
                decision = SimpleNamespace(
                    measured_mbit=0.0, band="DEGRADED",
                    severity="STARTUP_FAILED", rejected_quality=0,
                    switched=False, switched_to=None, note="")
                try:
                    source = await self._active_source(item.id)
                    ok = await self.jit._search_and_switch(
                        item, source, required, decision, background=False)
                    if ok:
                        self.runtime_metrics["startup_failover_success"] += 1
                        await self._evt("startup_reconnect", item=item)
                        if getattr(self.s, "jit_reconnect_on_failover", True):
                            for hdl, other in list(self.sessions.items()):
                                if other.session.media_item_id == item.id:
                                    await self.release(hdl)
                except Exception as exc:                # noqa: BLE001
                    self.runtime_metrics["runtime_failover_failed"] += 1
                    await self._evt("startup_failover_failed", item=item,
                                    error=repr(exc)[:120])
                return
        except asyncio.CancelledError:
            pass

    def _failover_budget_ok(self, item_id: str) -> bool:
        """FASE 19: max N runtime-failovers per item per budgetvenster."""
        count, window_start = self._failover_budget.get(item_id, (0, 0.0))
        now = time.time()
        if now - window_start > 1800:
            count, window_start = 0, now
        if count >= getattr(self.s, "jit_max_failovers_per_session", 2):
            return False
        self._failover_budget[item_id] = (count + 1, window_start)
        return True

    async def _runtime_failover(self, ctx: SessionContext, snap: dict) -> None:
        """FASE 10/14: runtime DEGRADED → same-class search + switch;
        daarna gecontroleerde reconnect van de item-sessies (het FUSE
        handle is source-bound — bewezen; mid-byte rebind tussen
        verschillende releases is onveilig en wordt niet gedaan)."""
        item = await self.store.get_item(ctx.session.media_item_id)
        if item is None:
            return
        source = await self._active_source(item.id)
        if source is None:
            return
        decision = SimpleNamespace(
            measured_mbit=snap.get("rolling_mbit", 0.0), band="DEGRADED",
            severity="SEVERELY_DEGRADED", rejected_quality=0,
            switched=False, switched_to=None, note="")
        try:
            ok = await self.jit._search_and_switch(
                item, source, ctx.required_mbit or 0.0, decision,
                background=False)
        except Exception as exc:                        # noqa: BLE001
            self.runtime_metrics["runtime_failover_failed"] += 1
            await self._evt("runtime_failover_failed", item=item,
                            error=repr(exc)[:120])
            return
        if ok:
            self.runtime_metrics["runtime_failover_success"] += 1
            await self._evt("runtime_reconnect_requested", item=item,
                            reason="delivery degraded, new source active")
            # gecontroleerde reconnect: sessies van dit item sluiten →
            # FUSE read → EIO → Plex heropent (zelfde ratingKey/timeline)
            if getattr(self.s, "jit_reconnect_on_failover", True):
                for hdl, other in list(self.sessions.items()):
                    if other.session.media_item_id == item.id:
                        try:
                            await self.release(hdl)
                        except Exception:           # noqa: BLE001
                            pass

    async def release(self, handle: str) -> None:
        ctx = self.sessions.pop(handle, None)
        if ctx is None:
            return
        ctx.reader.close()               # FASE 18: prefetch cancelen
        ctx.session.state = m.SessionState.CLOSED.value
        ctx.session.closed_at = m.now()
        await self.store.close_session(handle)
        await self._evt("session_closed", item_id=ctx.session.media_item_id, handle=handle,
              read_count=ctx.session.read_count)

    # ------------------------------------------------------------ byte path
    async def upstream_read(self, source_id: str, offset: int, length: int) -> bytes:
        async with self._sem:
            source = await self.store.get_source(source_id)
            if source is None:
                raise ProviderError(f"source {source_id} vanished")
            url = await self._link_for(source)
            try:
                return await self.provider.read_range(url, offset, length)
            except Exception as exc:
                await self._evt("upstream_read_failed", source_id=source_id, offset=offset,
                                length=length, error=str(exc))
                raise

    async def _link_for(self, source: m.Source, torrent=None) -> str:
        cached = self.caches.links.get(source.id)
        if cached:
            return cached
        if torrent is None:
            torrent = await self.provider.ensure_torrent(source.info_hash, source.torrent_name)
        url = await self.provider.get_stream_url(torrent.torrent_id, source.file_id or 0)
        self.caches.links.put(source.id, url)
        return url

    # ----------------------------------------------------- failure injection
    async def fail_source(self, source_id: str, reason: str = "debug_injection") -> bool:
        src = await self.store.get_source(source_id)
        if src is None:
            return False
        return await self._fail(src, reason)

    async def fail_current(self, item_id: str, reason: str = "debug_injection") -> bool:
        src = await self._active_source(item_id)
        if src is None:
            return False
        return await self._fail(src, reason)

    async def _fail(self, src: m.Source, reason: str) -> bool:
        src.state = m.SourceState.FAILED.value
        src.failure_count += 1
        bad_until = self._bad_until(src.failure_count)
        if reason.startswith("debug"):
            # operator-injected failure: treat as a long-lived verdict so the
            # next playback demonstrably picks a DIFFERENT release
            bad_until = max(bad_until, m.now() + 3600.0)
        src.bad_until = bad_until
        await self.store.update_source(src)
        item = await self.store.get_item(src.media_item_id)
        if item is not None:
            item.status = m.ItemStatus.SOURCE_FAILED.value
            await self.store.update_runtime(item)
            await self._evt("source_failed", item=item, source_id=src.id,
                  generation=src.generation, reason=reason,
                  reason_for_source_switch=reason, failure_count=src.failure_count)
        return True
