"""JIT policy-pass regressietests: geen dubbele veiligheidsmarge, severity-
bewuste switching, hysteresis, early-exit (FASE policy-pass)."""
import asyncio
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.jit import (                                    # noqa: E402
    JitConfig, current_severity, delivery_band, quality_relation,
    quality_tier, DEGRADED, FAST, MARGINAL)

from test_jit import (                                                    # noqa: E402
    _jit_with_candidates, _mk_source, _mk_cand, _mk_item,
    REMUX_DV, REMUX_HDR, WEBDL)


def _media_profile(item, size):
    from plex_scraper.resolver.media import build_profile
    return build_profile(size_bytes=size, media_bitrate_mbit=None,
                         duration_s=None, floor_mbit=25.0)

GB = 10**9


def _decision(mbit, required=65.4):
    cfg = JitConfig()
    return SimpleNamespace(measured_mbit=mbit, band=DEGRADED, rejected_quality=0,
                           switched=False, switched_to=None, note="",
                           severity=current_severity(mbit, required, cfg))


def test_01_no_double_safety_margin():
    """(1) de switch-formule stapelt geen extra marge op required:
    candidate-drempel == required zelf (geen required × fast_ratio)."""
    import inspect
    from plex_scraper.resolver import jit as jit_mod
    src = inspect.getsource(jit_mod.JitController._candidate_sufficient)
    assert "fast_ratio" not in src
    assert "required" in src and "min_gain" in src


def test_02_required_formula():
    """(2) 43,6 × 1,5 = required 65,4."""
    from plex_scraper.resolver.media import build_profile
    prof = build_profile(size_bytes=55_111_675_036, media_bitrate_mbit=None,
                         duration_s=10108.288, floor_mbit=25.0)
    assert round(prof.required_mbit(1.5), 1) == 65.4


def _potc_setup(speeds, extra_cands=()):
    cur = _mk_source("fra1", REMUX_DV)
    cands = [_mk_cand(h, n) for h, n in extra_cands]
    r, jit = _jit_with_candidates(
        cands, [cur] + [_mk_source(h, n) for h, n in extra_cands], speeds)
    return r, jit, cur


def test_03_potc_switch_77_7_over_7_2():
    """(3) current 7,2 / candidate 77,7 / required 65,4 → SWITCH
    (het exacte live-pilot geval dat vóór de policy-pass faalde)."""
    alt = ("ccafeb96ea200c8e8cca9d7e2ce0d2d93ae2dfd7",
           "Pirates.of.the.Caribbean.At.Worlds.End.2007.UHD.BluRay.2160p.DV.HEVC.TrueHD.Atmos.x265-Deathy")
    r, jit, cur = _potc_setup({alt[0]: 77.7}, extra_cands=[alt])
    d = _decision(7.2)
    ok = asyncio.run(jit._search_and_switch(_mk_item(title="Pirates"), cur, 65.4, d, background=False))
    assert ok is True and r.activated == alt[0]
    assert d.switched is True


def test_04_marginal_hysteresis_keep():
    """(4) current 62 / required 65 / candidate 68 → KEEP (churn-bescherming)."""
    alt = ("alt1", REMUX_HDR)
    r, jit, cur = _potc_setup({"alt1": 68.0}, extra_cands=[alt])
    d = _decision(62.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.0, d, background=False))
    assert ok is False and r.activated is None
    assert cur.state == "active"


def test_05_severe_switch_70_over_30():
    """(5) current 30 / required 65 / candidate 70 → SWITCH (70 ≥ required,
    2,3× verbetering)."""
    alt = ("alt1", REMUX_HDR)
    r, jit, cur = _potc_setup({"alt1": 70.0}, extra_cands=[alt])
    d = _decision(30.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.0, d, background=False))
    assert ok is True and r.activated == "alt1"


def test_06_candidate_below_required_no_switch():
    """(6) candidate 60 / required 65 → NO SWITCH (ook bij severe current)."""
    alt = ("alt1", REMUX_HDR)
    r, jit, cur = _potc_setup({"alt1": 40.0}, extra_cands=[alt])
    jit._thresholds = lambda item, current, required: (required, 52.3)
    d = _decision(10.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(title="Pirates"), cur, 65.0, d, background=False))
    assert ok is False and r.activated is None


def test_07_same_class_70_beats_lower_quality_150():
    """(7) same-class 70 wint van lower-quality 150."""
    cur = _mk_source("fra1", REMUX_DV)
    webdl = _mk_source("web1", WEBDL)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("web1", WEBDL), _mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, webdl, alt],
                                  {"web1": 150.0, "alt1": 70.0})
    jit._thresholds = lambda item, current, required: (required, 52.3)
    d = _decision(30.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.0, d, background=False))
    assert ok is True and r.activated == "alt1"


def test_08_only_lower_quality_no_switch():
    """(8) alleen lower-quality 150 + downgrade disabled → geen switch."""
    cur = _mk_source("fra1", REMUX_DV)
    webdl = _mk_source("web1", WEBDL)
    cands = [_mk_cand("web1", WEBDL)]
    r, jit = _jit_with_candidates(cands, [cur, webdl], {"web1": 150.0},
                                  allow_quality_downgrade=False)
    d = _decision(30.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.0, d, background=False))
    assert ok is False and r.activated is None
    assert jit.metrics["quality_downgrade_blocked"] >= 1


def test_09_early_exit_first_good_candidate():
    """(9) SEVERELY_DEGRADED: early-exit na de eerste ≥required candidate —
    geen probes verspild aan de rest."""
    alt1 = _mk_source("alt1", REMUX_HDR)
    alt2 = _mk_source("alt2", REMUX_HDR.replace("HDR10", "HDR10-B"))
    cands = [_mk_cand("alt1", REMUX_HDR), _mk_cand("alt2", alt2.torrent_name)]
    r, jit = _jit_with_candidates(cands, [_mk_source("fra1", REMUX_DV), alt1, alt2],
                                  {"alt1": 90.0, "alt2": 120.0})
    probes = {"n": 0}
    orig = jit._probe_by_hash

    async def counting(item, cand):
        probes["n"] += 1
        return await orig(item, cand)
    jit._probe_by_hash = counting
    d = _decision(10.0)                              # SEVERELY_DEGRADED
    ok = asyncio.run(jit._search_and_switch(_mk_item(), _mk_source("fra1", REMUX_DV),
                                            65.0, d, background=False))
    assert ok is True and r.activated == "alt1"
    assert probes["n"] == 1                          # early-exit na #1


def test_10_fast_no_search():
    """(10) FAST source triggert geen search."""
    r, jit, cur = _potc_setup({})
    jit._probe = None

    async def fast_probe(source, sample_bytes=None):
        return {"mbit": 130.0, "ttfb_s": 0.4, "short": False, "errors": 0}
    jit._probe = fast_probe
    searched = {"n": 0}

    async def no_search(*a, **k):
        searched["n"] += 1
        return False
    jit._search_and_switch = no_search
    d = asyncio.run(jit.preflight_async(_mk_item(), cur, 65.0))
    assert d.band == FAST and searched["n"] == 0


def test_11_marginal_no_aggressive_blocking_switch():
    """(11) MARGINAL → playback-start zonder blocking switch (background)."""
    alt = ("alt1", REMUX_HDR)
    r, jit, cur = _potc_setup({"alt1": 140.0}, extra_cands=[alt])

    async def probe(source, sample_bytes=None):
        return {"mbit": 58.0, "ttfb_s": 1.2, "short": False, "errors": 0}
    jit._probe = probe
    switched = {"n": 0}

    async def fake_switch(item, current, required, decision, background):
        switched["n"] += 1
        assert background is True                    # nooit blocking
        return False
    jit._search_and_switch = fake_switch
    d = asyncio.run(jit.preflight_async(_mk_item(), cur, 55.0))  # 58 ≥ 0,8×55 → MARGINAL
    assert d.band == MARGINAL and not d.switched
    asyncio.run(asyncio.sleep(0.01))
    assert switched["n"] == 1                        # alleen background


def test_12_delivery_bad_ttl_temporary():
    """(12) delivery_bad_until blijft tijdelijk en verloopt."""
    from plex_scraper.common.domain.models import Source
    s = Source(id="x", media_item_id="i", generation=1, provider="torbox",
               info_hash="h", torrent_name="n")
    assert s.is_delivery_bad() is False
    s.delivery_bad_until = time.time() + 1800
    assert s.is_delivery_bad() is True
    s.delivery_bad_until = time.time() - 1
    assert s.is_delivery_bad() is False
    assert s.is_bad() is False                       # niet gemengd met bad_until


def test_13_identity_gate_still_hard():
    """(13) identity gate blijft hard in de JIT-search."""
    cur = _mk_source("fra1", REMUX_DV)
    wrong = _mk_cand("wrong1", "Unrelated.Film.2019.2160p.BluRay.REMUX.HEVC.DV-GRP")
    cands = [_mk_cand("wrong1", wrong.torrent_name)]
    r, jit = _jit_with_candidates(cands, [cur, _mk_source("wrong1", wrong.torrent_name)],
                                  {"wrong1": 300.0})
    d = _decision(10.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.0, d, background=False))
    assert ok is False and r.activated is None


def test_14_atomic_switch_invariants():
    """(14) atomic switch: oude bron retired+delivery_bad, nieuwe active,
    generation-gedrag via engine onaangetast."""
    alt_tuple = ("alt1", REMUX_HDR)
    r, jit, cur = _potc_setup({"alt1": 90.0}, extra_cands=[alt_tuple])
    alt_src = next(s for s in r.sources if s.info_hash == "alt1")
    d = _decision(10.0)
    asyncio.run(jit._search_and_switch(_mk_item(), cur, 65.0, d, background=False))
    assert cur.state == "retired" and alt_src.state == "active"
    assert cur.is_delivery_bad() and not alt_src.is_delivery_bad()
    assert d.switched is True and d.switched_to["mbit"] == 90.0


def test_severity_bands():
    """Severity: FAST ≥ required · MARGINAL 0,8–1× · SEVERELY_DEGRADED < 0,8×."""
    cfg = JitConfig(degraded_ratio=0.8)
    assert current_severity(70.0, 65.4, cfg) == "FAST"
    assert current_severity(60.0, 65.4, cfg) == "MARGINAL"
    assert current_severity(7.2, 65.4, cfg) == "SEVERELY_DEGRADED"



def test_high_risk_unknown_heavy_remux_forces_preflight(tmp_path):
    """(22) unknown metadata + 65 GB 2160p REMUX = HIGH_RISK → géén
    low-bitrate fast path ondanks lage floor-required."""
    from plex_scraper.resolver.media import build_profile
    prof = build_profile(size_bytes=65 * GB, media_bitrate_mbit=None,
                         duration_s=None, floor_mbit=25.0)
    assert prof.risk == "HIGH_RISK" and prof.confidence == "floor"
    item = _mk_item()
    item._jit_risk = prof.risk
    r, jit = _jit_with_candidates([], [_mk_source("h1", REMUX_DV)], {})
    jit._probe = None
    probed = {"n": 0}

    async def probe(source, sample_bytes=None):
        probed["n"] += 1
        return {"mbit": 20.0, "ttfb_s": 1.0, "short": False, "errors": 0}
    jit._probe = probe
    d = asyncio.run(jit.preflight_async(item, _mk_source("h1", REMUX_DV), 37.5))
    assert probed["n"] == 1                            # wél preflight gedraaid
    assert d.band == DEGRADED                          # 20 < 0,8×37,5


def test_low_bitrate_normal_risk_still_fast_path():
    from plex_scraper.resolver.media import build_profile
    prof = build_profile(size_bytes=2 * GB, media_bitrate_mbit=None,
                         duration_s=None, floor_mbit=25.0)
    assert prof.risk == "normal"
    item = _mk_item()
    item._jit_risk = prof.risk
    r, jit = _jit_with_candidates([], [_mk_source("h1", REMUX_DV)], {})
    jit._probe = None
    probed = {"n": 0}

    async def probe(source, sample_bytes=None):
        probed["n"] += 1
        return {"mbit": 30.0, "ttfb_s": 1.0, "short": False, "errors": 0}
    jit._probe = probe
    d = asyncio.run(jit.preflight_async(item, _mk_source("h1", REMUX_DV), 37.5))
    assert probed["n"] == 0 and d.band == FAST


def test_rescue_threshold_math():
    """(32-1/2) ideal = bitrate×1.5 · rescue = bitrate×1.2."""
    from plex_scraper.resolver.media import build_profile
    prof = build_profile(size_bytes=None, media_bitrate_mbit=69.7,
                         duration_s=None, floor_mbit=25.0)
    assert round(prof.required_mbit(1.5), 1) == 104.6
    assert round(prof.bitrate_mbit * 1.2, 1) == 83.6


def test_ts4_rescue_switch():
    """(32-7) current 57-67+stalls / candidate 91,8 / rescue 83,6 → SWITCH."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt], {"alt1": 91.8})
    jit._thresholds = lambda item, current, required: (required, 83.6)
    jit._rescue_mbit = 83.6
    d = SimpleNamespace(measured_mbit=60.0, band=DEGRADED, rejected_quality=0,
                        severity="SEVERELY_DEGRADED", switched=False,
                        switched_to=None, note="")
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 104.6, d, background=False))
    assert ok is True and r.activated == "alt1"
    assert d.switched_to["mbit"] == 91.8


def test_healthy_current_no_rescue_switch():
    """(32-5) gezonde current + alleen rescue-waardige kandidaat → geen switch."""
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r, jit = _jit_with_candidates(cands, [cur, alt], {"alt1": 91.8})
    d = SimpleNamespace(measured_mbit=60.0, band=DEGRADED, rejected_quality=0,
                        severity="MARGINAL", switched=False, switched_to=None, note="")
    jit._thresholds = lambda item, current, required: (required, 83.6)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 104.6, d, background=False))
    assert ok is False and r.activated is None
