"""Just-in-Time playback preflight + quality-preserving source failover.

Principes:
- Delivery-performance wordt gemeten OP HET MOMENT van Play (open_handle),
  nooit via library-wide scans.
- Fast path: lage-bitrate media (required < drempel) krijgt géén preflight.
- Quality class (resolution, release-type, HDR, lossless-audio) gaat vóór
  delivery: een source wordt alleen vervangen door een gelijkwaardige of
  equivalent-acceptabele bron; stille downgrades zijn default verboden.
- Een perfecte release kan delivery-DEGRADED zijn zonder een "slechte
  source" te zijn → aparte `delivery_bad_until`-state met TTL, niet mengen
  met permanent-bad/identity/read-failure.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
import logging
import time
from dataclasses import dataclass, field

from plex_scraper.common.scoring.release_parser import parse_release
from plex_scraper.resolver.selfheal import identity_gate

log = logging.getLogger("jit")

FAST, MARGINAL, DEGRADED = "FAST", "MARGINAL", "DEGRADED"

_RES_PTS = {"2160p": 3, "1080p": 2, "720p": 1}
_TYPE_PTS = {"remux": 4, "bluray": 3, "web-dl": 2, "webrip": 1}
_LOSSLESS = ("truehd", "dts-hd", "dts_hd_ma", "flac", "pcm")


def quality_tier(torrent_name: str) -> dict:
    """FASE 6: expliciete quality class uit de release-naam."""
    p = parse_release(torrent_name)
    res = _RES_PTS.get((p.resolution or "").lower(), 0)
    typ = _TYPE_PTS.get((p.release_type or "").lower(), 0)
    hdr = 1 if (p.video or "") in ("dolby_vision", "hdr10") else 0
    lossless = 1 if (p.audio or "") in _LOSSLESS else 0
    return {"resolution": p.resolution, "release_type": p.release_type,
            "hdr": p.video, "audio": p.audio,
            "res_pts": res, "type_pts": typ, "hdr_pts": hdr,
            "lossless": lossless,
            "tier": (res, typ, hdr, lossless)}


def quality_label(tier: dict) -> str:
    parts = [tier["resolution"] or "?"]
    if tier["release_type"]:
        parts.append(tier["release_type"].upper())
    if tier["hdr_pts"]:
        parts.append("DV/HDR")
    parts.append("lossless" if tier["lossless"] else "lossy")
    return " ".join(parts)


def quality_relation(cur: tuple, cand: tuple) -> str:
    """FASE 7: same / minor / lower / higher tussen quality tiers."""
    if cand == cur:
        return "same"
    if cand > cur:
        return "higher"
    cr, ct, ch, cl = cur
    kr, kt, kh, kl = cand
    if kr == cr:
        if kt == ct:
            # zelfde res+type, alleen audio- of hdr-variant verschilt
            return "minor"
        if abs(kt - ct) == 1:
            # zelfde resolutie, één type-stap verschil (REMUX↔BluRay-encode)
            return "minor"
        return "lower" if cand < cur else "higher"
    return "lower" if cand < cur else "higher"


def delivery_band(mbit: float, ttfb_s: float, required: float, cfg) -> str:
    """FASE 4: FAST ≥ 1,2×required · MARGINAL 0,8–1,2× · DEGRADED < 0,8× of
    ernstige TTFB/stall. Alles configureerbaar."""
    if ttfb_s > cfg.ttfb_max_s:
        return DEGRADED
    if mbit >= required * cfg.fast_ratio:
        return FAST
    if mbit >= required * cfg.degraded_ratio:
        return MARGINAL
    return DEGRADED


def current_severity(mbit: float, required: float, cfg) -> str:
    """FASE-policy: FAST ≥ required · MARGINAL 0,8–1×required ·
    SEVERELY_DEGRADED < 0,8×required (bepaalt hoe pragmatisch de JIT mag zijn)."""
    if mbit >= required:
        return "FAST"
    if mbit >= required * cfg.degraded_ratio:
        return "MARGINAL"
    return "SEVERELY_DEGRADED"


@dataclass
class JitDecision:
    band: str                       # FAST | MARGINAL | DEGRADED
    measured_mbit: float
    ttfb_s: float
    required_mbit: float
    severity: str = ""              # FAST | MARGINAL | SEVERELY_DEGRADED
    switched: bool = False
    switched_to: dict | None = None
    searched: bool = False
    rejected_quality: int = 0
    note: str = ""


@dataclass
class JitConfig:
    enabled: bool = True
    preflight_min_mbit: float = 40.0     # FASE 2: required onder dit → fast path
    fast_ratio: float = 1.2
    degraded_ratio: float = 0.8
    ttfb_max_s: float = 5.0
    sample_bytes: int = 1048576
    max_wait_s: float = 12.0             # FASE 15: bounded blokkeren bij DEGRADED
    probe_candidates: int = 3
    min_gain: float = 1.5
    allow_minor_deviation: bool = True
    allow_quality_downgrade: bool = False   # FASE 8: geen stille downgrade
    fast_cache_s: float = 900.0
    degraded_cache_s: float = 600.0
    delivery_bad_ttl_s: float = 3600.0      # FASE 13


class JitController:
    """FASE 3/9/12: preflight → (zo nodig) same-class search → probe →
    atomic switch. Aangeroepen vanuit open_handle op het play-moment."""

    def __init__(self, resolver, cfg: JitConfig):
        self.resolver = resolver
        self.cfg = cfg
        self._cache: dict[str, tuple[float, JitDecision]] = {}   # plex_path → (vervaltijd, decision)
        self._inflight: set[str] = set()
        self.metrics = defaultdict(int)

    # ------------------------------------------------------------- helpers
    def _cached(self, plex_path: str) -> JitDecision | None:
        hit = self._cache.get(plex_path)
        if hit and hit[0] > time.time():
            return hit[1]
        if hit:
            del self._cache[plex_path]
        return None

    def _store_cache(self, plex_path: str, decision: JitDecision) -> None:
        ttl = (self.cfg.fast_cache_s if decision.band in (FAST, MARGINAL)
               else self.cfg.degraded_cache_s)
        self._cache[plex_path] = (time.time() + ttl, decision)

    def _probe(self, source, sample_bytes: int | None = None) -> dict:
        """FASE 3/9: bounded doorvoer-probe op een bron (2 samples: 0 + midden)."""
        cfg = self.cfg
        n = sample_bytes or cfg.sample_bytes

        async def _run() -> dict:
            torrent = await self.resolver.provider.ensure_torrent(
                src_info_hash(source), source.torrent_name)
            url = await self.resolver.provider.get_stream_url(
                torrent.torrent_id, source.file_id or 0)
            samples = []
            errors = 0
            for off in {0, max(0, int(source.size or n) // 2)}:
                t0 = time.monotonic()
                try:
                    data = await self.resolver.provider.read_range(url, off, n)
                except Exception:                      # noqa: BLE001
                    errors += 1
                    continue
                dt = max(time.monotonic() - t0, 1e-6)
                samples.append({"ttfb_s": round(dt, 2),
                                "mbit": round(len(data) * 8 / 1e6 / dt, 1),
                                "short": len(data) < n})
            if not samples:
                return {"mbit": 0.0, "ttfb_s": cfg.ttfb_max_s + 1,
                        "short": True, "errors": errors + 1}
            worst = min(samples, key=lambda s: s["mbit"])
            return {"mbit": worst["mbit"],
                    "ttfb_s": max(s["ttfb_s"] for s in samples),
                    "short": any(s["short"] for s in samples),
                    "errors": errors}

        return _run()

    # -------------------------------------------------------------- entry
    def on_play(self, item, source, required_mbit: float) -> JitDecision:
        """FASE 1-5: play-trigger. NB: open_handle is async — gebruik
        preflight_async(); deze wrapper bestaat alleen voor documentatie."""
        raise NotImplementedError("use preflight_async from async context")

    async def preflight_async(self, item, source, required_mbit: float) -> JitDecision:
        cfg = self.cfg
        self.metrics["jit_preflights_total"] += 1
        if not cfg.enabled:
            return JitDecision(FAST, 0, 0, required_mbit, note="jit disabled")
        if required_mbit < cfg.preflight_min_mbit:
            # FASE 2: lage-bitrate → direct fast path, geen preflight
            self.metrics["jit_low_bitrate_fastpath"] += 1
            return JitDecision(FAST, 0, 0, required_mbit, note="low-bitrate fast path")
        cached = self._cached(item.plex_path)
        if cached is not None:
            return cached
        decision = await self._preflight(item, source, required_mbit)
        self._store_cache(item.plex_path, decision)
        return decision

    async def _preflight(self, item, source, required_mbit: float) -> JitDecision:
        cfg = self.cfg
        self.metrics["jit_preflight_started"] += 1
        await self.resolver._evt("jit_preflight_started", item=item,
                                 hash=src_info_hash(source), required_mbit=round(required_mbit, 1))
        probe = await self._probe(source)
        band = delivery_band(probe["mbit"], probe["ttfb_s"], required_mbit, cfg)
        decision = JitDecision(band, probe["mbit"], probe["ttfb_s"], required_mbit,
                               severity=current_severity(probe["mbit"], required_mbit, cfg))
        if band == FAST:
            self.metrics["jit_fast_pass"] += 1
            await self.resolver._evt("jit_preflight_pass", item=item,
                                     hash=src_info_hash(source), mbit=probe["mbit"],
                                     ttfb_s=probe["ttfb_s"], required_mbit=round(required_mbit, 1))
            return decision
        if band == MARGINAL:
            # FASE 5: playback start gewoon; background-search alleen rapporteren
            self.metrics["jit_marginal"] += 1
            await self.resolver._evt("jit_marginal", item=item,
                                     hash=src_info_hash(source), mbit=probe["mbit"],
                                     required_mbit=round(required_mbit, 1))
            asyncio.create_task(
                self._search_and_switch(item, source, required_mbit,
                                        decision, background=True))
            return decision
        # DEGRADED
        self.metrics["jit_degraded"] += 1
        await self.resolver._evt("jit_delivery_degraded", item=item,
                                 hash=src_info_hash(source), mbit=probe["mbit"],
                                 ttfb_s=probe["ttfb_s"], required_mbit=round(required_mbit, 1))
        try:
            switched = await asyncio.wait_for(
                self._search_and_switch(item, source, required_mbit, decision,
                                        background=False),
                timeout=cfg.max_wait_s)
        except asyncio.TimeoutError:
            decision.note = "search timeout; playback op huidige bron"
            self.metrics["jit_search_timeout"] += 1
        except Exception as exc:                        # noqa: BLE001
            decision.note = f"search error: {exc!r}"[:80]
        return decision

    # ----------------------------------------------------- search+switch
    async def _search_and_switch(self, item, current, required_mbit: float,
                                 decision: JitDecision, background: bool) -> bool:
        """FASE 7/9/10/12: same-class kandidaten zoeken, top-N proben,
        winner atomisch activeren. Retourneert of er geswitcht is."""
        if item.plex_path in self._inflight:
            return False
        self._inflight.add(item.plex_path)
        try:
            self.metrics["jit_searches"] += 1
            await self.resolver._evt("jit_candidate_search", item=item,
                                     required_mbit=round(required_mbit, 1),
                                     current_mbit=decision.measured_mbit)
            cur_tier = quality_tier(current.torrent_name)["tier"]
            candidates = await self.resolver._gather_candidates(item)
            ranked = await self.resolver._rank_candidates(item, candidates)

            sources = {s.info_hash: s for s in
                       await self.resolver.store.list_sources(item.id)}
            probed: list[tuple[float, object, dict]] = []
            rejected_quality = 0
            tried = 0
            for cand, _score in ranked:
                if tried >= self.cfg.probe_candidates:
                    break
                if src_info_hash(cand) == src_info_hash(current):
                    continue
                if cand.info_hash in sources and sources[cand.info_hash].is_delivery_bad():
                    continue
                if sources.get(cand.info_hash) is None and not cand.info_hash in _cached_hashes(self.resolver):
                    continue                       # alleen cached: geen add-druk tijdens Play
                ok, _why = identity_gate(item.title, item.series, item.season,
                                         item.episode, cand.torrent_name, item.year)
                if not ok:
                    continue
                relation = quality_relation(cur_tier, quality_tier(cand.torrent_name)["tier"])
                if relation == "same" or relation == "higher":
                    pass                                   # gelijkwaardig of beter
                elif relation == "minor" and self.cfg.allow_minor_deviation:
                    pass
                elif relation == "lower" and self.cfg.allow_quality_downgrade:
                    pass
                else:
                    if relation == "lower":
                        rejected_quality += 1
                        self.metrics["quality_downgrade_blocked"] += 1
                        await self.resolver._evt("jit_candidate_rejected_quality",
                                                 item=item, hash=cand.info_hash,
                                                 name=cand.torrent_name,
                                                 relation=relation)
                    continue
                tried += 1
                self.metrics["jit_candidate_probe"] += 1
                probe = await self._probe_by_hash(item, cand)
                if probe and probe.get("mbit", 0) > 0:
                    probed.append((probe["mbit"], cand, probe))
                    await self.resolver._evt("jit_candidate_probe", item=item,
                                             hash=cand.info_hash, name=cand.torrent_name,
                                             mbit=probe["mbit"], ttfb_s=probe.get("ttfb_s"),
                                             relation=relation)
                    # POLICY-PASS early-exit: bij SEVERELY_DEGRADED current is
                    # de eerste geverifieerde same/minor-class candidate die
                    # ≥ required én duidelijk beter is direct de winnaar
                    if (decision.severity == "SEVERELY_DEGRADED"
                            and self._candidate_sufficient(probe["mbit"],
                                                           decision.measured_mbit,
                                                           required_mbit)):
                        break
            decision.rejected_quality = rejected_quality

            if not probed:
                self.metrics["jit_no_equivalent_source"] += 1
                await self.resolver._evt("jit_no_equivalent_source", item=item,
                                         rejected_quality=rejected_quality)
                decision.note = "no equally-good faster source found"
                return False

            # POLICY-PASS: `required = bitrate × playback_margin` is DE
            # playback-drempel en bevat al de headroom — daar komt géén
            # tweede candidate-marge (geen required × fast_ratio) bovenop.
            probed.sort(key=lambda t: t[0], reverse=True)
            best_mbit, best_cand, best_probe = probed[0]
            if not self._candidate_sufficient(best_mbit, decision.measured_mbit,
                                              required_mbit):
                self.metrics["jit_no_equivalent_source"] += 1
                await self.resolver._evt("jit_no_equivalent_source", item=item,
                                         best_mbit=best_mbit,
                                         required_mbit=round(required_mbit, 1))
                decision.note = "best alternative insufficient"
                return False

            switched = await self._activate(item, current, best_cand, best_mbit)
            if switched:
                decision.switched = True
                decision.switched_to = {"hash": src_info_hash(best_cand),
                                        "name": best_cand.torrent_name[:90],
                                        "mbit": best_mbit,
                                        "quality": quality_label(quality_tier(best_cand.torrent_name))}
                self.metrics["jit_switches"] += 1
                await self.resolver._evt("jit_switch", item=item,
                                         old_hash=src_info_hash(current),
                                         new_hash=src_info_hash(best_cand),
                                         old_mbit=decision.measured_mbit,
                                         new_mbit=best_mbit,
                                         quality=decision.switched_to["quality"])
            return switched
        finally:
            self._inflight.discard(item.plex_path)

    async def _activate(self, item, current, cand, cand_mbit: float) -> bool:
        """FASE 12: atomic switch — B geverifieerd, dan A→B in één stap;
        A krijgt delivery_bad_until (niet permanent bad)."""
        from plex_scraper.common.domain import models as m
        src = (await self.resolver.store.list_sources(item.id)
               and next((s for s in await self.resolver.store.list_sources(item.id)
                         if s.info_hash == src_info_hash(cand)), None))
        if src is None:
            # bouw source-row via de normale validatie (probe + pick_file)
            validated = await self.resolver._validate_candidate(item, cand)
            if validated is None:
                return False
            src = validated
        previous = await self.resolver._active_source(item.id)
        if previous is not None:
            previous.delivery_bad_until = m.now() + self.cfg.delivery_bad_ttl_s
            await self.resolver.store.update_source(previous)
            self.metrics["delivery_bad_ttl_count"] += 1
        await self.resolver._activate(item, src, previous, reason="jit_failover")
        await self.resolver._evt("jit_completed", item=item,
                                 hash=src_info_hash(src), mbit=cand_mbit)
        return True

    def _candidate_sufficient(self, cand_mbit: float, current_mbit: float,
                              required: float) -> bool:
        """POLICY-PASS basisregel: candidate ≥ required (dé playback-drempel)
        én duidelijke verbetering t.o.v. current (× min_gain, hysteresis).
        Geen tweede veiligheidsmarge boven required."""
        return (cand_mbit >= required
                and cand_mbit / max(current_mbit, 0.1) >= self.cfg.min_gain)

    async def _probe_by_hash(self, item, cand) -> dict | None:
        """FASE 9-probe op een kandidaat zonder hem te activeren."""
        try:
            torrent = await self.resolver.provider.ensure_torrent(
                src_info_hash(cand), cand.torrent_name)
            choice = None
            if cand.file_index is not None and cand.file_index in torrent.files:
                choice = (cand.file_index, torrent.files[cand.file_index])
            elif torrent.files:
                choice = self.resolver.provider.pick_file(torrent, cand.file_name) \
                    if hasattr(self.resolver.provider, "pick_file") \
                    else next(iter(torrent.files.items()))
            if choice is None:
                return None
            fid = choice[0]
            url = await self.resolver.provider.get_stream_url(torrent.torrent_id, fid)
            out = []
            for off in (0, max(0, int(cand.size or MB) // 2)):
                t0 = time.monotonic()
                data = await self.resolver.provider.read_range(url, off, MB)
                dt = max(time.monotonic() - t0, 1e-6)
                out.append(len(data) * 8 / 1e6 / dt)
            return {"mbit": round(min(out), 1), "ttfb_s": None}
        except Exception as exc:                        # noqa: BLE001
            await self.resolver._evt("jit_candidate_probe_error", item=item,
                                     hash=src_info_hash(cand), error=repr(exc)[:100])
            return None


MB = 1048576


def src_info_hash(obj):
    return getattr(obj, "info_hash", None) or getattr(obj, "info_hash", "")


def _cached_hashes(resolver) -> set:
    hashes = set()
    for value in resolver.caches.checkcached._data.values():
        hashes.update((value.value or {}).keys())
    return hashes
