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


def qualify_candidate(samples: list[float], ttfb_s: float | None,
                      required: float, current_mbit: float, cfg,
                      rescue: float, rescue_mode: bool) -> tuple[str, dict, str]:
    """Switch-hardening (incident Pirates 2026-10-09): een candidate wordt
    gekwalificeerd op DUURZAME doorvoer, niet op één piek.

    Bewezen faalpatroon: samples 9,1 / 97,9 / 10,0 mbit bij required 37,5 —
    de piek (97,9) werd geaccepteerd, de bron hield 37,5 niet vol en de
    client bleef iedere ~30 s bufferen.

    Retourneert (verdict, stats, reason) met verdict:
      IDEAL | ACCEPTABLE_RESCUE | UNSTABLE | INSUFFICIENT
    """
    vals = sorted(s for s in (samples or []) if s and s > 0)
    stats = {"samples": [round(v, 1) for v in vals], "median": 0.0,
             "p25": 0.0, "cv": None, "ttfb_s": ttfb_s}
    if not vals:
        return "INSUFFICIENT", stats, "geen geldige samples"
    median = vals[len(vals) // 2]
    p25 = vals[max(0, int(round((len(vals) - 1) * 0.25)))]
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    cv = (var ** 0.5) / median if median > 0 else None
    vmax = vals[-1]
    stats.update({"median": round(median, 1), "p25": round(p25, 1),
                  "cv": round(cv, 2) if cv is not None else None,
                  "max": round(vmax, 1)})

    # drempel: normaal required (= bitrate × playback_margin, bevat al
    # headroom); in rescue (SEVERELY_DEGRADED/STARTUP_FAILED) geldt de
    # rescue-drempel — géén gestapelde marges, consistent met FASE-policy
    threshold = rescue if rescue_mode else required

    # 1) first-byte-consistentie: een trage ttfb is sowieso geen verbetering
    if ttfb_s is not None and ttfb_s > cfg.ttfb_max_s:
        return "UNSTABLE", stats, f"ttfb {ttfb_s:.1f}s > max {cfg.ttfb_max_s:.1f}s"
    # 2) duurzaamheid: de median moet de drempel × headroom volhouden
    #    (rescue: alleen boven de rescue-drempel — geen dubbele marge)
    headroom = cfg.candidate_headroom if not rescue_mode \
        else cfg.candidate_headroom_rescue
    if median < threshold * headroom:
        return ("UNSTABLE", stats,
                f"median {median:.1f} < threshold {threshold:.1f} "
                f"× headroom {headroom}")
    # 3) floor: zelfs de slechtste kwartiel moet realtime kunnen suspporten
    if p25 < threshold * cfg.candidate_p25_ratio:
        return ("UNSTABLE", stats,
                f"p25 {p25:.1f} < threshold {threshold:.1f}")
    # 4) variance/spike-guard: één uitschieter maakt het gemiddelde leugenachtig
    if cv is not None and cv > cfg.candidate_max_cv:
        return "UNSTABLE", stats, f"cv {cv:.2f} > max {cfg.candidate_max_cv}"
    if len(vals) >= 3 and vmax > median * cfg.candidate_max_spike:
        return ("UNSTABLE", stats,
                f"spike {vmax:.1f} > median {median:.1f} "
                f"× {cfg.candidate_max_spike}")

    # 5) relatief: aantoonbaar beter dan de CURRENT source (geen churn)
    if median / max(current_mbit, 0.1) < cfg.min_gain:
        return ("INSUFFICIENT", stats,
                f"median {median:.1f} < current {current_mbit:.1f} "
                f"× min_gain {cfg.min_gain}")
    if rescue_mode and median < required:
        return "ACCEPTABLE_RESCUE", stats, "OK"
    return "IDEAL", stats, "OK"


@dataclass
class JitDecision:
    band: str                       # FAST | MARGINAL | DEGRADED
    measured_mbit: float
    ttfb_s: float
    required_mbit: float
    severity: str = ""              # FAST | MARGINAL | SEVERELY_DEGRADED
    rescue_mbit: float = 0.0        # DEEL A: media_bitrate × rescue_margin
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
    fast_cache_s: float = 180.0          # FASE 3: kort — FAST is optimalisatie, geen garantie
    degraded_cache_s: float = 600.0
    probe_parallel: int = 2              # FASE 16: bounded parallel top-2
    rescue_margin: float = 1.2           # DEEL A: rescue-drempel
    confirm_cached_fast: bool = True
    hot_spare: bool = True               # FASE 18: playback-scoped link-prewarm
    delivery_bad_ttl_s: float = 3600.0      # FASE 13
    # --- switch-hardening (incident Pirates 2026-10-09) ---
    # Een source-switch mag nooit werkende playback slechter maken.
    candidate_samples: int = 3           # meerdere throughput-samples per candidate
    candidate_headroom: float = 1.4      # median >= required × headroom
    candidate_headroom_rescue: float = 1.0   # rescue: median >= rescue-drempel
    candidate_p25_ratio: float = 1.0     # p25 >= required × ratio (floor)
    candidate_max_cv: float = 0.5        # stdev/median plafond (variance-guard)
    candidate_max_spike: float = 4.0     # max > median × spike → UNSTABLE
    require_stable_candidate: bool = True


class JitController:
    """FASE 3/9/12: preflight → (zo nodig) same-class search → probe →
    atomic switch. Aangeroepen vanuit open_handle op het play-moment."""

    def __init__(self, resolver, cfg: JitConfig):
        self.resolver = resolver
        self.cfg = cfg
        self._cache: dict[str, tuple[float, JitDecision]] = {}   # plex_path → (vervaltijd, decision)
        self._inflight: set[str] = set()
        self._hot_spares: dict[str, str] = {}                    # item_id → hash (FASE 18)
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
            # current-source probe hergebruikt de link-cache (geen requestdl
            # op het play-moment); deadline per sample houdt de preflight
            # bounded — een sample die het budget overschrijdt is sowieso
            # geen FAST
            try:
                url = await self.resolver._link_for(source)
            except Exception:                          # noqa: BLE001
                torrent = await self.resolver.provider.ensure_torrent(
                    src_info_hash(source), source.torrent_name)
                url = await self.resolver.provider.get_stream_url(
                    torrent.torrent_id, source.file_id or 0)
            samples = []
            errors = 0
            deadline = cfg.ttfb_max_s + 1.0
            for off in {0, max(0, int(source.size or n) // 2)}:
                t0 = time.monotonic()
                try:
                    data = await asyncio.wait_for(
                        self.resolver.provider.read_range(url, off, n),
                        timeout=deadline)
                except asyncio.TimeoutError:
                    errors += 1
                    samples.append({"ttfb_s": round(deadline, 2), "mbit": 0.0,
                                    "short": True})
                    continue
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
        # provider-blackout (2026-10-08): geen probes, geen candidate-search —
        # playback start gewoon op de huidige bron en de runtime-monitor
        # bewaakt delivery. NOOIT een sessie afbreken om provider-state rood.
        avail = getattr(self.resolver, "availability", None)
        if avail is not None and avail.blocked():
            self.metrics["jit_deferred_provider_unavailable"] += 1
            await self.resolver._evt(
                "jit_deferred_provider_unavailable", item=item,
                hash=src_info_hash(source), provider=avail.provider,
                state=avail.state, cooldown_until=avail.cooldown_until)
            return JitDecision(FAST, 0, 0, required_mbit,
                               note="provider blackout — jit deferred")
        risk = getattr(item, "_jit_risk", None)
        if required_mbit < cfg.preflight_min_mbit and risk != "HIGH_RISK":
            # FASE 2: lage-bitrate → direct fast path, geen preflight —
            # BEHALVE HIGH_RISK (unknown metadata + zware file, FASE 10)
            self.metrics["jit_low_bitrate_fastpath"] += 1
            return JitDecision(FAST, 0, 0, required_mbit, note="low-bitrate fast path")
        cached = self._cached(item.plex_path)
        if cached is not None:
            if cached.band != FAST or not cfg.confirm_cached_fast:
                # DEGRADED-cache (kort) of confirmatie uit: cache volstaat
                return cached
            # FASE 3: FAST is een optimalisatie, geen garantie — een oude
            # FAST-cache bij volatiele routes vraagt om lichte live-
            # confirmatie (1 sample); zakt de bron door → verse beoordeling
            probe = await self._probe(source, sample_bytes=cfg.sample_bytes)
            if delivery_band(probe["mbit"], probe["ttfb_s"], required_mbit,
                             cfg) == FAST:
                self.metrics["jit_fast_cache_confirmed"] += 1
                self._store_cache(item.plex_path, cached)
                return cached
            self.metrics["fast_cache_false_positive"] += 1
            del self._cache[item.plex_path]
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
        # provider-blackout: alle candidates lopen uiteindelijk tegen
        # dezelfde geblokkeerde TorBox-API aan — defer i.p.v. zinloze probes
        # met een misleidend jit_no_equivalent_source als gevolg.
        avail = getattr(self.resolver, "availability", None)
        if avail is not None and avail.blocked():
            self.metrics["jit_deferred_provider_unavailable"] += 1
            await self.resolver._evt(
                "jit_deferred_provider_unavailable", item=item,
                hash=src_info_hash(current), provider=avail.provider,
                state=avail.state, cooldown_until=avail.cooldown_until,
                context="search_and_switch")
            decision.note = "provider blackout — failover deferred"
            return False
        self._inflight.add(item.plex_path)
        try:
            self.metrics["jit_searches"] += 1
            await self.resolver._evt("jit_candidate_search", item=item,
                                     required_mbit=round(required_mbit, 1),
                                     current_mbit=decision.measured_mbit)
            ideal_mbit, self._rescue_mbit = self._thresholds(
                item, current, required_mbit)
            self._rescue_mode = decision.severity in (
                "SEVERELY_DEGRADED", "STARTUP_FAILED")
            decision.required_mbit = round(required_mbit, 1)
            decision.rescue_mbit = round(self._rescue_mbit, 1)
            cur_tier = quality_tier(current.torrent_name)["tier"]
            candidates = await self.resolver._gather_candidates(item)
            ranked = await self.resolver._rank_candidates(item, candidates)

            sources = {s.info_hash: s for s in
                       await self.resolver.store.list_sources(item.id)}
            probed: list[tuple[float, object, dict]] = []
            selected: list[tuple[str, object]] = []
            rejected_quality = 0
            tried = 0
            spare_hash = self._hot_spares.get(item.id)
            if spare_hash:
                ranked.sort(key=lambda t: 0 if src_info_hash(t[0]) == spare_hash else 1)
            for cand, _score in ranked:
                if tried >= self.cfg.probe_candidates:
                    break
                if src_info_hash(cand) == src_info_hash(current):
                    continue
                if cand.info_hash in sources and sources[cand.info_hash].is_delivery_bad():
                    continue
                if sources.get(cand.info_hash) is None and not cand.info_hash in _cached_hashes(self.resolver):
                    continue                       # alleen cached: geen add-druk tijdens Play
                ok, _why, _sub = identity_gate(item.title, item.series, item.season,
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
                selected.append((relation, cand))
                if tried >= self.cfg.probe_candidates:
                    break
            decision.rejected_quality = rejected_quality

            # FASE 16: bounded parallel probes (top-2 tegelijk) — halveert
            # time-to-good-candidate; concurrency blijft klein en gated door
            # de globale upstream-semaphore
            if (selected and self.cfg.probe_parallel > 1 and len(selected) > 1
                    and decision.severity != "SEVERELY_DEGRADED"):
                results = list(await asyncio.gather(
                    *[self._probe_by_hash(item, c) for _, c in selected]))
            else:
                # sequentieel met early-exit: bij SEVERELY_DEGRADED geen
                # probes verspillen nadat een kandidaat al slaagt
                results = []
                for _rel, _c in selected:
                    _r = await self._probe_by_hash(item, _c)
                    results.append(_r)
                    if (_r and _r.get("mbit", 0) > 0
                            and self._candidate_sufficient(_r["mbit"],
                                                           decision.measured_mbit,
                                                           required_mbit)):
                        break
            for (relation, cand), probe in zip(selected, results):
                if not (probe and probe.get("mbit", 0) > 0):
                    continue
                probed.append((probe["mbit"], cand, probe))
                await self.resolver._evt("jit_candidate_probe", item=item,
                                         hash=cand.info_hash, name=cand.torrent_name,
                                         mbit=probe["mbit"], ttfb_s=probe.get("ttfb_s"),
                                         relation=relation, file_id=probe.get("file_id"),
                                         file_size=probe.get("file_size"),
                                         samples=probe.get("samples"))
                # POLICY-PASS early-exit alleen bij een AANTOONBAAR stabiele
                # candidate (duurzame throughput boven required) — een piek
                # volstaat niet meer (switch-hardening 2026-10-09)
                if (decision.severity == "SEVERELY_DEGRADED"
                        and self._qualify(probe, decision.measured_mbit,
                                          required_mbit)[0] in ("IDEAL",
                                                                "ACCEPTABLE_RESCUE")):
                    break

            if not probed:
                self.metrics["jit_no_equivalent_source"] += 1
                await self.resolver._evt("jit_no_equivalent_source", item=item,
                                         rejected_quality=rejected_quality)
                decision.note = "no equally-good faster source found"
                return False

            # switch-hardening: kies de beste candidate die zowel stabiel als
            # aantoonbaar beter is; een onstabiele/snellere-op-piek candidate
            # wordt geweigerd (event) en de current source behouden
            probed.sort(key=lambda t: t[0], reverse=True)
            chosen = None
            for best_mbit, best_cand, best_probe in probed:
                verdict, stats, why = self._qualify(
                    best_probe, decision.measured_mbit, required_mbit)
                if verdict in ("IDEAL", "ACCEPTABLE_RESCUE"):
                    if verdict == "ACCEPTABLE_RESCUE":
                        await self.resolver._evt(
                            "candidate_rescue_acceptable", item=item,
                            mbit=best_mbit, rescue_mbit=round(self._rescue_mbit, 1),
                            ideal_mbit=round(ideal_mbit, 1))
                    chosen = (best_cand, best_mbit, best_probe, verdict, stats)
                    break
                self.metrics[f"jit_candidate_{verdict.lower()}"] += 1
                await self.resolver._evt(
                    "jit_candidate_rejected_unstable" if verdict == "UNSTABLE"
                    else "jit_candidate_rejected_insufficient",
                    item=item, hash=src_info_hash(best_cand),
                    name=getattr(best_cand, "torrent_name", "")[:90],
                    reason=why, **{k: v for k, v in stats.items()})
            if chosen is None:
                self.metrics["jit_no_equivalent_source"] += 1
                await self.resolver._evt("jit_no_equivalent_source", item=item,
                                         best_mbit=probed[0][0],
                                         required_mbit=round(required_mbit, 1),
                                         reason="geen stabiele candidate")
                decision.note = "no stable faster source found; current retained"
                return False
            best_cand, best_mbit, best_probe, verdict, stats = chosen

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
        """FASE 12 + switch-hardening (2026-10-09): veilige volgorde —

        1. source-row verifiëren/bouwen (candidate volledig geverifieerd);
        2. PRE-COMMIT: Plex ratingKey betrouwbaar resolven — lukt dat niet
           (rating_key_not_found) dan wordt de switch GEABORTEERD en blijft
           de oude bron + actieve sessie intact (geen generation bump, geen
           delivery_bad, geen dead-handle window);
        3. COMMIT: atomisch A→B activeren, daarna pas de oude bron
           delivery_bad markeren;
        4. POST-COMMIT: gerichte metadata-revalidatie queued (faalveilig).
        """
        from plex_scraper.common.domain import models as m
        src = (await self.resolver.store.list_sources(item.id)
               and next((s for s in await self.resolver.store.list_sources(item.id)
                         if s.info_hash == src_info_hash(cand)), None))
        if src is None:
            # bouw source-row via de normale validatie (probe + pick_file)
            validated = await self.resolver._validate_candidate(item, cand)
            if validated is None:
                await self.resolver._evt("jit_switch_aborted", item=item,
                                         hash=src_info_hash(cand),
                                         reason="candidate_validatie_mislukt")
                return False
            src = validated

        # PRE-COMMIT ratingKey-check: kan Plex het item (nog) niet resolven,
        # dan mislukt de post-commit revalidatie sowieso en blijft de client
        # op een dode metadata-link hangen (bewezen bij Pirates) — abort.
        # Resolutie volgt de revalidator-voorkeursvolgorde (bekende rk →
        # exact part-pad → suffix → bounded retry) om valse aborts te voorkomen.
        reval = getattr(self.resolver, "_revalidator", None)
        if reval is not None and hasattr(reval, "resolve_rating_key"):
            try:
                rk, _how = await reval.resolve_rating_key(item)
            except Exception as exc:                # noqa: BLE001
                rk = None
                await self.resolver._evt("jit_switch_precheck_error", item=item,
                                         error=repr(exc)[:120])
            if not rk:
                self.metrics["jit_switch_aborted_ratingkey"] += 1
                await self.resolver._evt("jit_switch_aborted", item=item,
                                         old_hash=src_info_hash(current),
                                         hash=src_info_hash(cand),
                                         reason="rating_key_unresolved",
                                         action="old_source_retained")
                return False
        elif getattr(self.resolver, "plex", None) is not None:
            # fallback zonder revalidator-hulp: directe lookup
            try:
                rk = await self.resolver.plex.find_rating_key_by_path(
                    item.plex_path)
            except Exception as exc:                # noqa: BLE001
                rk = None
                await self.resolver._evt("jit_switch_precheck_error", item=item,
                                         error=repr(exc)[:120])
            if not rk:
                self.metrics["jit_switch_aborted_ratingkey"] += 1
                await self.resolver._evt("jit_switch_aborted", item=item,
                                         old_hash=src_info_hash(current),
                                         hash=src_info_hash(cand),
                                         reason="rating_key_unresolved",
                                         action="old_source_retained")
                return False

        previous = await self.resolver._active_source(item.id)
        try:
            await self.resolver._activate(item, src, previous, reason="jit_failover")
        except Exception as exc:                    # noqa: BLE001
            # commit mislukt → oude bron blijft actief, GEEN generation bump
            self.metrics["jit_switch_commit_failed"] += 1
            await self.resolver._evt("jit_switch_aborted", item=item,
                                     old_hash=src_info_hash(current),
                                     hash=src_info_hash(cand),
                                     reason=f"commit_failed: {exc!r}"[:120],
                                     action="old_source_retained")
            return False
        if previous is not None:
            # pas ná een geslaagde commit: de oude bron mag nooit als bad
            # staan terwijl hij nog de actieve bron is
            previous.delivery_bad_until = m.now() + self.cfg.delivery_bad_ttl_s
            await self.resolver.store.update_source(previous)
            self.metrics["delivery_bad_ttl_count"] += 1
        await self.resolver._evt("jit_completed", item=item,
                                 hash=src_info_hash(src), mbit=cand_mbit)
        return True

    def _qualify(self, probe: dict, current_mbit: float,
                 required_mbit: float) -> tuple[str, dict, str]:
        """Stabiliteits-guard rond qualify_candidate (switch-hardening).

        Legacy-probes (zonder samples-lijst, bijv. tests/anders) worden
        behandeld als twee consistente samples van de gemeten waarde —
        productie-probes leveren altijd meerdere samples."""
        samples = probe.get("samples")
        if not samples:
            samples = [probe.get("mbit", 0.0), probe.get("mbit", 0.0)]
        return qualify_candidate(samples, probe.get("ttfb_s"), required_mbit,
                                 current_mbit, self.cfg, self._rescue_mbit,
                                 getattr(self, "_rescue_mode", False))

    def _thresholds(self, item, current, required: float) -> tuple[float, float]:
        """(DEEL A) ideal_target = required (= bitrate × playback_margin);
        rescue_threshold = media_bitrate × rescue_margin (1,2×)."""
        mp = getattr(self.resolver, "media_profile", None)
        if mp is not None:
            profile = mp(item, int(getattr(current, "size", 0) or 0))
            rescue = profile.bitrate_mbit * self.cfg.rescue_margin
        else:
            rescue = required * (self.cfg.rescue_margin / 1.5)
        return required, rescue

    def _candidate_verdict(self, cand_mbit: float, current_mbit: float,
                           ideal: float, rescue: float,
                           rescue_ok: bool = False) -> str:
        """IDEAL / ACCEPTABLE_RESCUE / INSUFFICIENT. De rescue-drempel geldt
        ALLÉÉN bij echte rescue-situaties (SEVERELY_DEGRADED/STARTUP_FAILED);
        bij MARGINAL/gezonde current blijft de ideal-target leidend."""
        gain_ok = cand_mbit / max(current_mbit, 0.1) >= self.cfg.min_gain
        if cand_mbit >= ideal and gain_ok:
            return "IDEAL"
        if rescue_ok and cand_mbit >= rescue and gain_ok:
            return "ACCEPTABLE_RESCUE"
        return "INSUFFICIENT"

    def _candidate_sufficient(self, cand_mbit: float, current_mbit: float,
                              required: float) -> bool:
        """Rescue-bewust: bij SEVERELY_DEGRADED/STARTUP_FAILED current geldt
        de rescue-drempel (media_bitrate × 1,2) i.p.v. de ideal-target —
        geen gestapelde marges; hysteresis (× min_gain) blijft."""
        threshold = self._rescue_mbit if getattr(self, "_rescue_mode", False) \
            else required
        return (cand_mbit >= threshold
                and cand_mbit / max(current_mbit, 0.1) >= self.cfg.min_gain)

    async def warm_spare(self, item, current) -> None:
        """FASE 18: playback-scoped hot spare — bij zware playback in de
        achtergrond 1 same-class cached candidate linken (geen throughput-
        benchmark als current gezond is). Failover wordt dan sneller
        probe-ready. Geen library-scanning."""
        try:
            avail = getattr(self.resolver, "availability", None)
            if avail is not None and avail.blocked():
                # geen prewarm-verkeer tijdens een provider-blackout
                return
            cur_tier = quality_tier(current.torrent_name)["tier"]
            candidates = await self.resolver._gather_candidates(item)
            ranked = await self.resolver._rank_candidates(item, candidates)
            sources = {s.info_hash: s for s in
                       await self.resolver.store.list_sources(item.id)}
            for cand, _s in ranked:
                h = src_info_hash(cand)
                if h == src_info_hash(current):
                    continue
                if h in sources and sources[h].is_delivery_bad():
                    continue
                if sources.get(h) is None and h not in _cached_hashes(self.resolver):
                    continue
                if quality_relation(cur_tier, quality_tier(cand.torrent_name)["tier"])                         not in ("same", "higher", "minor"):
                    continue
                ok, _, _sub = identity_gate(item.title, item.series, item.season,
                                      item.episode, cand.torrent_name, item.year)
                if not ok:
                    continue
                torrent = await self.resolver.provider.ensure_torrent(h, cand.torrent_name)
                await self.resolver.provider.get_stream_url(torrent.torrent_id,
                                                            cand.file_index or 0)
                self._hot_spares[item.id] = h
                await self.resolver._evt("hot_spare_ready", item=item, hash=h,
                                         name=cand.torrent_name[:80])
                return
        except Exception as exc:                            # noqa: BLE001
            await self.resolver._evt("hot_spare_failed", item=item,
                                     error=repr(exc)[:100])

    async def _probe_by_hash(self, item, cand) -> dict | None:
        """FASE 9-probe op een kandidaat zonder hem te activeren.

        File-selectie volgt de resolve/validatie-flow (provider.pick_file:
        S/E-hint, video-extensie + minimumgrootte, grootste videofile als
        fallback). Een scraper-fileIdx is een index in de eigen telling van
        de scraper en wordt NOOIT 1-op-1 als provider-file-id gebruikt —
        bij TorBox is id 0 vaak een NFO-sidecar (incident 2026-10-07,
        Lanterns S01E08: 3/3 same-class kandidaten vielen weg op HTTP 416
        omdat de probe-range uit de torrent-totaalgrootte tegen die sidecar
        werd gevraagd). Probe-offsets worden daarom op de GEKOZEN file
        berekend en geclampt binnen de file-grenzen."""
        try:
            torrent = await self.resolver.provider.ensure_torrent(
                src_info_hash(cand), cand.torrent_name)
            chosen = None
            if torrent.files:
                if hasattr(self.resolver.provider, "pick_file"):
                    chosen = self.resolver.provider.pick_file(
                        torrent, cand.file_name or cand.torrent_name)
                if chosen is None:
                    # providers zonder pick_file: grootste bestand als beste
                    # media-kandidaat (never-smaller dan het video-idee)
                    chosen = max(torrent.files.items(),
                                 key=lambda kv: int((kv[1] or {}).get("size") or 0))
            if chosen is None:
                await self.resolver._evt("jit_probe_file_unusable", item=item,
                                         hash=src_info_hash(cand),
                                         reason="torrent zonder bestanden")
                return None
            fid, meta = chosen
            fname = str((meta or {}).get("name") or "")
            fsize = int((meta or {}).get("size") or 0)
            if not _looks_like_media_file(fname):
                # sidecars (NFO/SRR/jpg/...) zijn nooit een geldige mediafile
                await self.resolver._evt("jit_probe_file_unusable", item=item,
                                         hash=src_info_hash(cand), file_id=fid,
                                         file_name=fname[:90],
                                         reason="gekozen file is geen videofile")
                return None
            url = await self.resolver.provider.get_stream_url(torrent.torrent_id, fid)
            # samples: meerdere offsets over de GEKOZEN file (0, kwart, midden)
            # — een switch-beslissing mag nooit op één sample vallen (switch-
            # hardening 2026-10-09); clamp binnen de file-grenzen zodat een
            # range nooit voorbij EOF kan
            span = max(1, self.cfg.candidate_samples)
            offsets = sorted({min(fsize - MB if fsize > MB else 0,
                                  int(fsize * f) // 1)
                              for f in (0.0, 0.25, 0.5)[:span]})
            offsets = [max(0, o) for o in offsets]
            await self.resolver._evt("jit_probe_file_selected", item=item,
                                     hash=src_info_hash(cand), file_id=fid,
                                     file_name=fname[:90], file_size=fsize,
                                     offsets=offsets)
            samples: list[float] = []
            ttfbs: list[float] = []
            failures: list[str] = []
            for off in offsets:
                t0 = time.monotonic()
                try:
                    data = await self.resolver.provider.read_range(url, off, MB)
                except Exception as exc:                # noqa: BLE001
                    failures.append(f"offset {off}: {repr(exc)[:80]}")
                    continue
                dt = max(time.monotonic() - t0, 1e-6)
                samples.append(len(data) * 8 / 1e6 / dt)
                ttfbs.append(dt)
            if not samples:
                await self.resolver._evt("jit_candidate_probe_error", item=item,
                                         hash=src_info_hash(cand), file_id=fid,
                                         file_name=fname[:90],
                                         error="; ".join(failures)[:160])
                return None
            if failures:
                await self.resolver._evt("jit_probe_sample_degraded", item=item,
                                         hash=src_info_hash(cand),
                                         failures="; ".join(failures)[:160])
            vals = sorted(samples)
            median = vals[len(vals) // 2]
            return {"mbit": round(median, 1),
                    "samples": [round(v, 1) for v in samples],
                    "ttfb_s": round(max(ttfbs), 2) if ttfbs else None,
                    "file_id": fid, "file_size": fsize}
        except Exception as exc:                        # noqa: BLE001
            await self.resolver._evt("jit_candidate_probe_error", item=item,
                                     hash=src_info_hash(cand), error=repr(exc)[:100])
            return None


MB = 1048576

# Spiegel van TorboxProvider.VIDEO_EXTS — jit is provider-vrij; deze lokale
# kopie is de backstop zodat een probe nooit tegen een sidecar plaatsvindt.
_MEDIA_EXTS = (".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov", ".mpg", ".webm")


def _looks_like_media_file(name) -> bool:
    return str(name or "").lower().endswith(_MEDIA_EXTS)


def src_info_hash(obj):
    return getattr(obj, "info_hash", None) or getattr(obj, "info_hash", "")


def _cached_hashes(resolver) -> set:
    hashes = set()
    for value in resolver.caches.checkcached._data.values():
        hashes.update((value.value or {}).keys())
    return hashes
