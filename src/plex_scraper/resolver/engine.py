"""Resolution engine (FASE 6 + 7): state machine, generations, pinning.

NO_SOURCE → RESOLVING → CANDIDATE_VALIDATION → READY
READY --failure--> SOURCE_FAILED --> RESOLVING --> ... --> READY (generation N+1)

One broken torrent can never break the item while alternatives exist: the
engine walks ranked candidates, marks failures bad for a TTL, and activates
the first candidate whose bytes validate.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from ..config import Settings
from ..domain import models as m
from ..log import event
from ..providers.base import DebridProvider, NotReadyError, ProviderError
from ..scoring.release_parser import parse_release
from ..scrapers.base import Scraper, TorrentCandidate
from .caches import CacheSet
from .store import Store
from .stream import RangeReader


@dataclass
class SessionContext:
    session: m.Session
    source: m.Source
    reader: RangeReader
    opened_at: float = field(default_factory=time.monotonic)


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
        }

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
        await self.store.update_item(item)
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
        await self.store.update_item(item)

        candidates = await self._gather_candidates(item)
        ranked = await self._rank_candidates(item, candidates)
        item.status = m.ItemStatus.CANDIDATE_VALIDATION.value
        await self.store.update_item(item)

        await self._evt("resolution_started", item=item, reason=reason,
              candidate_count=len(candidates), ranked_count=len(ranked),
              current_generation=item.generation)

        fallback_count = 0
        for cand, _score in ranked:
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

        item.status = m.ItemStatus.NO_SOURCE.value
        await self.store.update_item(item)
        await self._evt("resolution_failed", item=item, reason=reason,
              candidate_count=len(candidates), fallback_count=fallback_count,
              resolution_latency=round(time.monotonic() - started, 3))
        return None

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
        await self.store.update_item(item)

    async def _active_source(self, item_id: str) -> m.Source | None:
        for src in await self.store.list_sources(item_id):
            if src.state == m.SourceState.ACTIVE.value:
                return src
        return None

    # ------------------------------------------------------------ sessions
    async def open_handle(self, item_id: str) -> SessionContext:
        item = await self.store.get_item(item_id)
        if item is None:
            raise KeyError(f"unknown media item {item_id}")
        source = await self._active_source(item_id)
        if item.status != m.ItemStatus.READY.value or source is None or source.is_bad():
            item.status = (m.ItemStatus.SOURCE_FAILED.value
                           if source is not None else item.status)
            if item.status == m.ItemStatus.SOURCE_FAILED.value:
                await self.store.update_item(item)
            source = await self.resolve_item(
                item, reason="open_needs_source")
        if source is None:
            raise UnresolvedError(f"no working source for {item.plex_path}")

        reader = RangeReader(self, source.id, source.size, self.s.stream_readahead_bytes)
        session = m.Session(handle=m.new_id(), media_item_id=item_id,
                            source_id=source.id, generation=source.generation,
                            size=source.size)
        await self.store.create_session(session)
        ctx = SessionContext(session=session, source=source, reader=reader)
        self.sessions[session.handle] = ctx
        await self._evt("session_opened", item_id=item_id, handle=session.handle,
              generation=source.generation, size=source.size)
        return ctx

    def get_session(self, handle: str) -> SessionContext:
        ctx = self.sessions.get(handle)
        if ctx is None or ctx.session.state != m.SessionState.OPEN.value:
            raise KeyError("unknown or closed handle")
        return ctx

    async def read(self, handle: str, offset: int, length: int) -> bytes:
        ctx = self.get_session(handle)
        t0 = time.monotonic()
        data = await ctx.reader.read(offset, length)
        self.metrics["reads"] += 1
        self.metrics["read_bytes"] += len(data)
        self.metrics["request_count"] += 1
        self.metrics["request_latency_sum"] += time.monotonic() - t0
        ctx.session.read_count += 1
        return data

    async def release(self, handle: str) -> None:
        ctx = self.sessions.pop(handle, None)
        if ctx is None:
            return
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
        src.bad_until = self._bad_until(src.failure_count)
        await self.store.update_source(src)
        item = await self.store.get_item(src.media_item_id)
        if item is not None:
            item.status = m.ItemStatus.SOURCE_FAILED.value
            await self.store.update_item(item)
            await self._evt("source_failed", item=item, source_id=src.id,
                  generation=src.generation, reason=reason,
                  reason_for_source_switch=reason, failure_count=src.failure_count)
        return True
