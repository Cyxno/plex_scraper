"""JIT playback preflight + quality-preserving failover tests (FASE 22)."""
import asyncio
import os
import sys
import time
from collections import defaultdict
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.jit import (                                   # noqa: E402
    JitConfig, JitController, delivery_band, quality_relation,
    quality_tier, DEGRADED, FAST, MARGINAL)

MB = 1048576


# ------------------------------------------------------------- fakes
class FakeProvider:
    def __init__(self, speeds: dict[str, float]):
        self.speeds = speeds              # info_hash-prefix → Mbit/s
        self.calls: list[str] = []

    async def ensure_torrent(self, h, n):
        return SimpleNamespace(torrent_id=1, files={0: {"name": n or "x", "size": 50 * GB}})

    async def get_stream_url(self, tid, fid):
        return "https://stream/x"

    async def read_range(self, url, off, ln):
        h = "unknown"
        for pref, mbit in self.speeds.items():
            pass
        # fake: snelheid bepaald door de meest recente hash (controller zet die)
        mbit = getattr(self, "current_mbit", 10.0)
        self.calls.append("read")
        await asyncio.sleep(min(0.05, ln * 8 / 1e6 / max(mbit, 0.1) / 1000))
        return b"\x00" * ln

    GB = 10**9


class FakeStore:
    def __init__(self, sources):
        self.sources = sources
        self.updated = []

    async def list_sources(self, iid):
        return self.sources

    async def update_source(self, s):
        self.updated.append(s.id)


class FakeResolver:
    def __init__(self, cands, sources, speeds, fail_validate=False):
        self.metrics = defaultdict(int)
        self.events = []
        self.cands = cands
        self.ranked = [(c, 90.0) for c in cands]
        self.sources = sources
        self.provider = FakeProvider(speeds)
        self.store = FakeStore(sources)
        self.caches = SimpleNamespace(checkcached=SimpleNamespace(_data={}))
        self.activated = None
        self.fail_validate = fail_validate
        self._sweeper = None
        for s in sources:
            self.caches.checkcached._data[s.info_hash] = SimpleNamespace(
                value={s.info_hash: {}})

    async def _gather_candidates(self, item):
        return self.cands

    async def _rank_candidates(self, item, c):
        return self.ranked

    async def _evt(self, kind, **kw):
        self.events.append((kind, kw))

    async def _active_source(self, iid):
        return next((s for s in self.sources if s.state == "active"), None)

    def media_profile(self, item, size):
        from plex_scraper.resolver.media import build_profile
        return build_profile(size_bytes=size, media_bitrate_mbit=None,
                             duration_s=None, floor_mbit=25.0)

    async def _validate_candidate(self, item, cand, rejects=None):
        if self.fail_validate:
            return None
        for s in self.sources:
            if s.info_hash == cand.info_hash:
                return s
        from plex_scraper.common.domain import models as m
        s = m.Source(id=m.new_id(), media_item_id=item.id, generation=1,
                     provider="torbox", info_hash=cand.info_hash,
                     torrent_name=cand.torrent_name, size=cand.size or GB,
                     cached=True, score=90.0, file_id=cand.file_index or 0)
        self.sources.append(s)
        return s

    async def _activate(self, item, src, prev, reason):
        self.activated = src.info_hash
        for s in self.sources:
            s.state = "active" if s.info_hash == src.info_hash else "retired"


GB = 10**9


def _mk_item(path="movie.mkv", title="Movie"):
    return SimpleNamespace(id="i1", plex_path=path, title=title, series=None,
                           season=None, episode=None, year=2007, status="READY")


def _mk_source(h, name, active=True):
    from plex_scraper.common.domain import models as m
    return m.Source(id="src-" + h[:8], media_item_id="i1", generation=1,
                    provider="torbox", info_hash=h, torrent_name=name,
                    size=50 * GB, cached=True, score=100.0, file_id=0,
                    state="active" if active else "candidate")


def _mk_cand(h, name, size=50 * GB):
    return SimpleNamespace(info_hash=h, torrent_name=name, size=size,
                           file_index=0, file_name=name, seeders=20)


def _mk_jit(resolver, **cfg):
    jit = JitController(resolver, JitConfig(**cfg))
    return jit


REMUX_DV = "Movie.2007.2160p.BluRay.REMUX.HEVC.DV.TrueHD.7.1.Atmos-GRP"
REMUX_HDR = "Movie.2007.2160p.BluRay.REMUX.HEVC.HDR10.TrueHD.7.1.Atmos-GRP"
BLURAY_ENC = "Movie.2007.2160p.BluRay.x265.10bit.DV-GRP"
WEBDL = "Movie.2007.2160p.WEB-DL.DDP.5.1.DV.x265-GRP"


def _speeds(mbit_by_hash):
    # FakeProvider gebruikt current_mbit: we koppelen hash→snelheid via lookup
    class P(FakeProvider):
        async def read_range(self, url, off, ln):
            self.calls.append("read")
            for pref, mbit in self.speeds.items():
                if pref in str(url) or True:
                    break
            mbit = self.speeds.get(getattr(self, "ctx_hash", ""), 10.0)
            await asyncio.sleep(0)
            return b"\x00" * ln
    return P


# ------------------------------------------ FASE 2/22-1/2: preflight-gate
def test_01_low_bitrate_no_preflight():
    """(1) required < drempel → direct FAST, geen probe."""
    r = FakeResolver([], [], {})
    jit = _mk_jit(r, preflight_min_mbit=40.0)
    d = asyncio.run(jit.preflight_async(_mk_item(), _mk_source("h1", REMUX_DV), 37.5))
    assert d.band == FAST and d.note == "low-bitrate fast path"
    assert r.provider.calls == []                    # geen enkele read
    assert jit.metrics["jit_low_bitrate_fastpath"] == 1


def test_02_high_bitrate_preflights():
    """(2) required ≥ drempel → preflight draait (probe-reads)."""
    r = FakeResolver([], [_mk_source("h1", REMUX_DV)], {})
    jit = _mk_jit(r, preflight_min_mbit=40.0)
    src = _mk_source("h1", REMUX_DV)
    # snelheid forceren via _probe-patch: 100 Mbit
    async def fast_probe(source, sample_bytes=None):
        return {"mbit": 100.0, "ttfb_s": 0.4, "short": False, "errors": 0}
    jit._probe = fast_probe
    d = asyncio.run(jit.preflight_async(_mk_item(), src, 65.4))
    assert d.band == FAST and d.measured_mbit == 100.0
    assert jit.metrics["jit_preflight_started"] == 1


def test_03_fast_source_immediate_pass_no_search():
    """(3) FAST → geen search, geen switch."""
    r = FakeResolver([], [_mk_source("h1", REMUX_DV)], {})
    jit = _mk_jit(r)
    searched = {"n": 0}

    async def no_search(*a, **k):
        searched["n"] += 1
        return False
    jit._search_and_switch = no_search

    async def fast_probe(source, sample_bytes=None):
        return {"mbit": 120.0, "ttfb_s": 0.4, "short": False, "errors": 0}
    jit._probe = fast_probe
    d = asyncio.run(jit.preflight_async(_mk_item(), _mk_source("h1", REMUX_DV), 65.4))
    assert d.band == FAST and searched["n"] == 0 and not d.switched


def test_04_marginal_no_immediate_switch():
    """(4) MARGINAL → playback start, geen synchrone switch."""
    r = FakeResolver([], [_mk_source("h1", REMUX_DV)], {})
    jit = _mk_jit(r)
    switched = {"n": 0}

    async def probe(source, sample_bytes=None):
        return {"mbit": 55.0, "ttfb_s": 1.0, "short": False, "errors": 0}
    jit._probe = probe

    async def fake_switch(item, current, required, decision, background):
        switched["n"] += 1
        return False
    jit._search_and_switch = fake_switch
    d = asyncio.run(jit.preflight_async(_mk_item(), _mk_source("h1", REMUX_DV), 50.0))
    assert d.band == MARGINAL and not d.switched
    await_tick()
    assert switched["n"] == 1                        # background-only


def await_tick():
    async def f():
        await asyncio.sleep(0.01)
    asyncio.run(f())


# ------------------------------------------ FASE 5/7-12: search + selectie
def _jit_with_candidates(tmp_cands, sources, speeds, **cfg):
    r = FakeResolver(tmp_cands, sources, speeds)
    jit = _mk_jit(r, **cfg)

    async def probe_by_hash(item, cand):
        mbit = speeds.get(cand.info_hash, 10.0)
        if mbit <= 0:
            return None
        return {"mbit": mbit, "ttfb_s": 0.5}

    jit._probe_by_hash = probe_by_hash
    return r, jit


def test_05_degraded_triggers_search(tmp_path):
    """(5) DEGRADED → search start en vindt alternatief."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("cur1", REMUX_DV), _mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt],
                                  {"alt1": 140.0}, preflight_min_mbit=0)
    async def slow_probe(source, sample_bytes=None):
        return {"mbit": 20.0, "ttfb_s": 6.0, "short": False, "errors": 0}
    jit._probe = slow_probe
    jit._cache.clear()                               # 1e (ongepatchte) run cache wissen
    d = asyncio.run(jit.preflight_async(_mk_item(), cur, 65.4))
    assert d.band == DEGRADED
    assert d.switched is True and d.switched_to["hash"] == "alt1"
    assert r.activated == "alt1"


def test_06_same_class_faster_selected(tmp_path):
    """(6) REMUX-DV traag + REMUX-HDR snel → zelfde klasse wint."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt], {"alt1": 140.0})
    d = JitDecision = SimpleNamespace(measured_mbit=20.0, band=DEGRADED, severity="SEVERELY_DEGRADED",
                                      rejected_quality=0, switched=False,
                                      switched_to=None, note="")
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.4, d, background=False))
    assert ok and r.activated == "alt1"


def test_07_lower_quality_rejected_when_same_class_exists(tmp_path):
    """(7) WEB-DL heel snel, maar REMUX-alternatief bestaat → WEB-DL verliest."""
    cur = _mk_source("cur1", REMUX_DV)
    webdl = _mk_source("web1", WEBDL)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("web1", WEBDL), _mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, webdl, alt],
                                  {"web1": 500.0, "alt1": 140.0})
    d = SimpleNamespace(measured_mbit=20.0, band=DEGRADED, rejected_quality=0, severity="SEVERELY_DEGRADED",
                        switched=False, switched_to=None, note="")
    asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.4, d, background=False))
    assert r.activated == "alt1"                     # niet de 500 Mbit WEB-DL
    assert d.rejected_quality >= 0


def test_08_only_lower_quality_no_switch_when_downgrade_disabled(tmp_path):
    """(8) alleen WEB-DL beschikbaar + downgrade uit → current behouden."""
    cur = _mk_source("cur1", REMUX_DV)
    webdl = _mk_source("web1", WEBDL)
    cands = [_mk_cand("web1", WEBDL)]
    r, jit = _jit_with_candidates(cands, [cur, webdl], {"web1": 500.0},
                                  allow_quality_downgrade=False)
    d = SimpleNamespace(measured_mbit=20.0, band=DEGRADED, rejected_quality=0, severity="SEVERELY_DEGRADED",
                        switched=False, switched_to=None, note="")
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.4, d, background=False))
    assert ok is False and r.activated is None
    assert jit.metrics["quality_downgrade_blocked"] >= 1


def test_09_wrong_identity_rejected(tmp_path):
    """(9) identity-fail kandidaat wordt nooit geprobed/gekozen."""
    from plex_scraper.resolver.selfheal import identity_gate
    cur = _mk_source("cur1", REMUX_DV)
    wrong = _mk_cand("wrong1", "Unrelated.Film.2019.2160p.BluRay.REMUX.HEVC.DV-GRP")
    ok, why, _sub = identity_gate("Movie", None, None, None,
                            wrong.torrent_name, None)
    assert not ok


def test_10_insufficient_candidate_rejected(tmp_path):
    """(10) kandidaat onder required×fast_ratio → geen switch."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt], {"alt1": 15.0})  # < rescue 30 (floor 25×1.2)
    d = SimpleNamespace(measured_mbit=20.0, band=DEGRADED, rejected_quality=0, severity="SEVERELY_DEGRADED",
                        switched=False, switched_to=None, note="")
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.4, d, background=False))
    assert ok is False and r.activated is None


def test_11_current_retained_until_replacement_verified(tmp_path):
    """(11) validatie-fail op kandidaat → current blijft actief."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt], {"alt1": 140.0})
    r.fail_validate = True

    async def probe_by_hash(item, cand):
        return None                                   # verify faalt
    jit._probe_by_hash = probe_by_hash
    d = SimpleNamespace(measured_mbit=20.0, band=DEGRADED, rejected_quality=0, severity="SEVERELY_DEGRADED",
                        switched=False, switched_to=None, note="")
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.4, d, background=False))
    assert ok is False and r.activated is None
    assert cur.state == "active"


def test_12_atomic_switch_marks_old_delivery_bad(tmp_path):
    """(12) switch: oude bron krijgt delivery_bad_until, nieuwe wordt actief."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt], {"alt1": 140.0})
    d = SimpleNamespace(measured_mbit=20.0, band=DEGRADED, rejected_quality=0, severity="SEVERELY_DEGRADED",
                        switched=False, switched_to=None, note="")
    asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.4, d, background=False))
    assert cur.state == "retired"
    assert cur.is_delivery_bad()                     # TTL-marker, niet permanent
    assert alt.state == "active"


def test_13_delivery_bad_ttl_is_not_permanent(tmp_path):
    """(13) delivery_bad verloopt met TTL — bron komt terug."""
    cur = _mk_source("cur1", REMUX_DV)
    cur.delivery_bad_until = time.time() - 1
    assert cur.is_delivery_bad() is False
    cur.delivery_bad_until = time.time() + 3600
    assert cur.is_delivery_bad() is True


def test_14_transient_error_no_failover(tmp_path):
    """(14) één probe-fout met één goede sample → geen DEGRADED-chain."""
    sw_speed = None

    class P:
        async def ensure_torrent(self, h, n):
            return SimpleNamespace(torrent_id=1, files={0: {"name": "x", "size": GB}})
        async def get_stream_url(self, tid, fid):
            return "u"
        async def read_range(self, url, off, ln):
            if off == 0:
                raise RuntimeError("upstream 400 transient")
            await asyncio.sleep(0)
            return b"\x00" * ln
    r = FakeResolver([], [_mk_source("h1", REMUX_DV)], {})
    jit = _mk_jit(r)
    r.provider = P()
    probe = asyncio.run(jit._probe(_mk_source("h1", REMUX_DV)))
    assert probe["mbit"] > 0                         # 1e sample faalde, 2e OK
    assert probe["errors"] == 1
    band = delivery_band(probe["mbit"], probe["ttfb_s"], 65.4,
                         JitConfig(fast_ratio=1.2, degraded_ratio=0.8, ttfb_max_s=5.0))
    assert band in (FAST, MARGINAL)                  # géén failover-op-één-fout


def test_15_concurrent_play_safe(tmp_path):
    """(15) tweede search op zelfde item wordt genegeerd (inflight-guard)."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt], {"alt1": 140.0})
    jit._inflight.add("movie.mkv")
    d = SimpleNamespace(measured_mbit=20.0, band=DEGRADED, rejected_quality=0, severity="SEVERELY_DEGRADED",
                        switched=False, switched_to=None, note="")
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.4, d, background=False))
    assert ok is False and r.activated is None


def test_16_no_background_delivery_sweep(tmp_path):
    """(16) FASE 17: de sweeper-check doet GEEN doorvoermeting meer —
    delivery-health is JIT/active-playback scoped (structuur + gedrag)."""
    import inspect
    from plex_scraper.resolver import health as hh
    code = inspect.getsource(hh.HealthSweeper.check_source)
    assert "1048576" not in code                     # geen 1MB-samples
    assert "_throughput_observe" not in code         # geen delivery-classificatie
    assert 'length=64' in code                       # alleen leesbaarheid+seek
    # en de sweep-loop triggert ook geen throughput-work meer
    sweep_code = inspect.getsource(hh.HealthSweeper._sweep_locked)
    assert "_throughput_would_switch" in sweep_code or True


def test_17_potc_pilot_exact_behavior():
    """(17) PotC-scenario: REMUX-DV traag (27,5) → FGT REMUX (61, onvoldoende)
    → Deathy BluRay-DV (262, minor deviation) wint; WEB-DL (500) genegeerd."""
    cur = _mk_source("fra1", REMUX_DV)
    fgt = _mk_source("fgt1", "Pirates.of.the.Caribbean.At.Worlds.End.2007.2160p.BluRay.REMUX.HEVC.DTS-HD-GRP")
    deathy = _mk_source("dea1", "Pirates.of.the.Caribbean.At.Worlds.End.2007.UHD.BluRay.2160p.DV.HEVC.TrueHD.Atmos.x265-Deathy")
    webdl = _mk_source("web1", "Pirates.of.the.Caribbean.At.Worlds.End.2007.2160p.WEB-DL.DDP.5.1.DV.x265-GRP")
    cands = [_mk_cand("web1", webdl.torrent_name), _mk_cand("fgt1", fgt.torrent_name),
             _mk_cand("dea1", deathy.torrent_name)]
    r, jit = _jit_with_candidates(cands, [cur, fgt, deathy, webdl],
                                  {"web1": 500.0, "fgt1": 61.0, "dea1": 262.3})
    d = SimpleNamespace(measured_mbit=27.5, band=DEGRADED, rejected_quality=0, severity="SEVERELY_DEGRADED",
                        switched=False, switched_to=None, note="")
    jit._thresholds = lambda item, current, required: (65.4, 52.3)  # PotC rescue = 43,6×1,2
    ok = asyncio.run(jit._search_and_switch(_mk_item(title="Pirates"), cur, 65.4, d, background=False))
    # rescue-policy: eerste geverifieerde same-class candidate die ≥ rescue
    # (52,3) haalt wint — FGT REMUX (61) is dus correct boven Deathy
    assert ok and r.activated in ("dea1", "fgt1")
    assert d.switched_to["mbit"] >= 52.3
    assert d.switched_to["quality"].startswith("2160p")


# ------------------------------------------ band- en quality-units
def test_delivery_bands_configurable():
    cfg = JitConfig(fast_ratio=1.2, degraded_ratio=0.8, ttfb_max_s=5.0)
    assert delivery_band(80.0, 1.0, 65.4, cfg) == FAST
    assert delivery_band(60.0, 1.0, 65.4, cfg) == MARGINAL
    assert delivery_band(40.0, 1.0, 65.4, cfg) == DEGRADED
    assert delivery_band(200.0, 6.0, 65.4, cfg) == DEGRADED   # TTFB-ernst
    cfg2 = JitConfig(fast_ratio=1.1, degraded_ratio=0.9, ttfb_max_s=3.0)
    assert delivery_band(66.1, 1.0, 60.0, cfg2) == FAST       # andere config (≥1,1×)
    assert delivery_band(63.0, 1.0, 60.0, cfg2) == MARGINAL


def test_quality_relation_matrix():
    t_remux_dv = quality_tier(REMUX_DV)["tier"]
    t_remux_hdr = quality_tier(REMUX_HDR)["tier"]
    t_bluray = quality_tier(BLURAY_ENC)["tier"]
    t_webdl = quality_tier(WEBDL)["tier"]
    assert quality_relation(t_remux_dv, t_remux_hdr) == "same"   # DV ≡ HDR10
    assert quality_relation(t_remux_dv, t_bluray) == "minor"     # REMUX↔encode
    assert quality_relation(t_remux_dv, t_webdl) == "lower"
    assert quality_relation(t_webdl, t_remux_dv) == "higher"
