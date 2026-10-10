"""Switch-hardening regressietests (incident Pirates 2011, 2026-10-09).

Bewezen keten: current degradeert → JIT kiest candidate op één piek-probe
(97,9 mbit na 9,1/10,0) → actieve session gesloten → plex_metadata_revalida-
tion faalt (rating_key_not_found) → client freezt → alleen handmatig reopen
herstelde playback.

Policy nu: een switch mag werkende playback nóóit slechter maken.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.jit import (                                    # noqa: E402
    JitConfig, JitController, qualify_candidate, DEGRADED, MARGINAL)
from plex_scraper.resolver.engine import Resolver                          # noqa: E402

from test_jit import (                                                    # noqa: E402
    _jit_with_candidates, _mk_source, _mk_cand, _mk_item,
    FakeResolver, REMUX_DV, REMUX_HDR, BLURAY_ENC)

GB = 10 ** 9


def _decision(mbit, severity="SEVERELY_DEGRADED", required=37.5):
    return SimpleNamespace(measured_mbit=mbit, band=DEGRADED,
                           rejected_quality=0, switched=False,
                           switched_to=None, note="", severity=severity)


def _probes(speeds_by_hash):
    """probe_by_hash met meerdere samples per hash (productie-vorm)."""
    async def probe(item, cand):
        samples = speeds_by_hash.get(cand.info_hash)
        if not samples:
            return None
        return {"mbit": sorted(samples)[len(samples) // 2],
                "samples": list(samples), "ttfb_s": 0.4}
    return probe


def _potc(jit_cfg=None, speeds=None, extra=()):
    """Pirates-achtige opzet: current REMUX-DV + same-class candidates."""
    cur = _mk_source("cur1", REMUX_DV)
    cands = [_mk_cand(h, n) for h, n in extra]
    r, jit = _jit_with_candidates(cands, [cur] + [_mk_source(h, n)
                                                  for h, n in extra],
                                  {}, **(jit_cfg or {}))
    jit._probe_by_hash = _probes(speeds or {})
    return r, jit, cur


# ------------------------------------------- qualify_candidate (eenheid)
def test_01_spike_plus_lage_samples_geweigerd():
    """(1) 9,1 / 97,9 / 10,0 bij required 37,5 → UNSTABLE (het Pirates-
    faalpatroon: de piek mag de beslissing nooit dragen)."""
    verdict, stats, why = qualify_candidate(
        [9.1, 97.9, 10.0], 0.4, 37.5, 20.0, JitConfig(), 45.0, False)
    assert verdict == "UNSTABLE"
    assert "spike" in why or "p25" in why or "median" in why


def test_02_hoog_gemiddelde_hoge_variance_geweigerd():
    """(2) hoog gemiddelde, te hoge spreiding (cv > 0,5) → UNSTABLE."""
    samples = [40.0, 45.0, 90.0, 200.0]           # media 90, cv ~0,7
    verdict, stats, why = qualify_candidate(
        samples, 0.4, 37.5, 20.0, JitConfig(), 45.0, False)
    assert verdict == "UNSTABLE" and "cv" in why


def test_03_stabiele_1_5x_geaccepteerd():
    """(3) stabiele samples ruim boven required → IDEAL."""
    samples = [95.0, 98.0, 100.0, 97.0]           # median 98 ≥ 37,5×1,4
    verdict, stats, why = qualify_candidate(
        samples, 0.4, 37.5, 20.0, JitConfig(), 45.0, False)
    assert verdict == "IDEAL", why
    assert stats["p25"] >= 37.5


def test_04_rescue_drempel_geen_dubbele_marge():
    """Rescue (SEVERELY_DEGRADED): mediaan boven rescue-drempel volstaat —
    géén required × headroom gestapeld bovenop de rescue-marge."""
    cfg = JitConfig()
    verdict, _, why = qualify_candidate(
        [50.0, 52.0, 51.0], 0.4, 90.0, 5.0, cfg, 45.0, True)
    assert verdict == "ACCEPTABLE_RESCUE", why


def test_05_traag_ttfb_geweigerd():
    """First-byte-consistentie: ttfb boven ttfb_max → UNSTABLE."""
    verdict, _, why = qualify_candidate(
        [90.0, 95.0, 99.0], 8.0, 37.5, 20.0, JitConfig(), 45.0, False)
    assert verdict == "UNSTABLE" and "ttfb" in why


# ------------------------------------- search_and_switch-integratie
def test_10_onstabiele_candidate_geen_switch():
    """Pirates-exact: enige candidate toont 9,1/97,9/10,0 → GEEN switch,
    current behouden, expliciete unstable-event."""
    alt = ("alt1", REMUX_HDR)
    r, jit, cur = _potc(speeds={"alt1": [9.1, 97.9, 10.0]}, extra=[alt])
    d = _decision(20.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 37.5, d,
                                            background=False))
    assert ok is False and r.activated is None
    assert d.switched is False
    kinds = [k for k, _e in r.events]
    assert "jit_candidate_rejected_unstable" in kinds
    assert cur.state == "active"


def test_11_candidate_niet_duidelijk_beter_geen_switch():
    """(4) candidate maar marginaal beter dan current → geen switch (churn)."""
    alt = ("alt1", REMUX_HDR)
    r, jit, cur = _potc(speeds={"alt1": [60.0, 61.0, 60.5]}, extra=[alt])
    d = _decision(55.0, severity="MARGINAL")
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 37.5, d,
                                            background=False))
    assert ok is False and r.activated is None     # 60/55 < min_gain 1,5


def test_12_stabiele_betre_source_wel_switch():
    """Oude source degraded + nieuwe aantoonbaar stabiel beter → veilige
    switch met generation-bump ná commit."""
    alt = ("alt1", REMUX_HDR)
    r, jit, cur = _potc(speeds={"alt1": [95.0, 98.0, 97.0]}, extra=[alt])
    d = _decision(20.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 37.5, d,
                                            background=False))
    assert ok is True and r.activated == "alt1"
    assert d.switched is True
    kinds = [k for k, _e in r.events]
    assert "jit_switch_aborted" not in kinds
    assert "jit_completed" in kinds                # commit bepaalt de bump


def test_13_meerdere_candidates_stabiele_wint():
    """Piekkandidaat geweigerd, stabiele kandidaat wint toch de switch."""
    spike = ("sp1", REMUX_HDR)
    stable = ("st1", BLURAY_ENC)
    r, jit, cur = _potc(
        speeds={"sp1": [9.0, 120.0, 9.5], "st1": [80.0, 82.0, 81.0]},
        extra=[spike, stable])
    d = _decision(20.0)
    ok = asyncio.run(jit._search_and_switch(_mk_item(), cur, 37.5, d,
                                            background=False))
    assert ok is True and r.activated == "st1"


# ------------------------------------------- veilige _activate-volgorde
class _Reval:
    def __init__(self, rk):
        self.rk = rk

    def on_swap(self, *a, **k):
        pass


def _mk_r(rk):
    class P:
        async def find_rating_key_by_path(self, path):
            return rk

    class Reval:
        def on_swap(self, *a, **k):
            pass
    return P(), Reval()


def _setup_with_precheck(rk, fail_activate=False):
    cur = _mk_source("cur1", REMUX_DV)
    alt = _mk_source("alt1", REMUX_HDR)
    cands = [_mk_cand("alt1", REMUX_HDR)]
    r = FakeResolver(cands, [cur, alt], {})
    jit = JitController(r, JitConfig())
    jit._probe_by_hash = _probes({"alt1": [95.0, 98.0, 97.0]})
    plex, reval = _mk_r(rk)
    r.plex = plex
    r._revalidator = reval
    if fail_activate:
        async def boom(item, src, prev, reason):
            raise RuntimeError("store on fire")
        r._activate = boom
    return r, jit, cur


def test_20_ratingkey_not_found_abort_retient_old():
    """(4) rating_key_not_found vóór commit → switch geaboorteerd, oude
    bron behouden, géén delivery_bad, géén generation bump."""
    item = _mk_item()
    item.generation = 2
    r, jit, cur = _setup_with_precheck(rk=None)
    d = _decision(20.0)
    ok = asyncio.run(jit._search_and_switch(item, cur, 37.5, d,
                                            background=False))
    assert ok is False and r.activated is None
    assert cur.state == "active"
    assert cur.delivery_bad_until in (0, None)
    assert item.generation == 2                    # geen bump bij mislukte switch
    kinds = [k for k, _e in r.events]
    assert "jit_switch_aborted" in kinds
    abort = next(e for k, e in r.events if k == "jit_switch_aborted")
    assert abort["reason"] == "rating_key_unresolved"
    assert abort["action"] == "old_source_retained"


def test_21_commit_failure_retient_old_no_bad_mark():
    """(5) commit-fout (store-faal) → oude bron blijft actief én wordt niet
    achteraf als delivery_bad gemarkeerd; geen generation bump."""
    item = _mk_item()
    item.generation = 1
    r, jit, cur = _setup_with_precheck(rk=123, fail_activate=True)
    d = _decision(20.0)
    ok = asyncio.run(jit._search_and_switch(item, cur, 37.5, d,
                                            background=False))
    assert ok is False and r.activated is None
    assert cur.state == "active"
    assert cur.delivery_bad_until in (0, None)
    assert item.generation == 1
    assert jit.metrics.get("jit_switch_commit_failed") == 1


def test_22_delivery_bad_pas_na_commit():
    """delivery_bad-markering van de oude bron gebeurt pas NÁ een geslaagde
    commit (volgorde-guard in de bron)."""
    import inspect
    src = inspect.getsource(JitController._activate)
    commit = src.index("reason=\"jit_failover\"")
    bad = src.index("delivery_bad_until")
    assert bad > commit


# ------------------------------------------- engine: runtime-failover
class _FakeStore:
    def __init__(self, item, source):
        self._item, self._source = item, source

    async def get_item(self, iid):
        return self._item

    async def get_source(self, sid):
        return self._source


def _engine_failover(monkeypatch_search, ok):
    """Minimale engine-fake voor _runtime_failover; levert events + sessies."""
    item = SimpleNamespace(id="i1", plex_path="p.mkv", generation=1)
    source = _mk_source("cur1", REMUX_DV)
    eng = SimpleNamespace(
        store=_FakeStore(item, source),
        jit=SimpleNamespace(_search_and_switch=monkeypatch_search),
        sessions={"h1": SimpleNamespace(
            session=SimpleNamespace(media_item_id="i1", state="open"),
            monitor=None)},
        released=[],
        runtime_metrics={},
        metrics={},
        s=SimpleNamespace(jit_reconnect_on_failover=True),
        _evt=None,
    )

    async def active_source(iid):
        return source
    eng._active_source = active_source
    events = []

    async def evt(kind, **kw):
        events.append((kind, kw))
    eng._evt = evt

    async def release(hdl):
        eng.released.append(hdl)
    eng.release = release
    ctx = SimpleNamespace(session=SimpleNamespace(media_item_id="i1"),
                          required_mbit=37.5)
    snap = {"rolling_mbit": 22.0, "elapsed_s": 30.0}
    return eng, events, ctx, snap


def test_30_runtime_switch_declined_geen_reconnect():
    """(6) runtime DEGRADED + geen stabiele candidate → current blijft,
    sessions blijven OPEN (geen dead-handle), expliciet declined-event."""
    async def no_switch(item, source, required, decision, background):
        decision.note = "no stable faster source found; current retained"
        return False
    eng, events, ctx, snap = _engine_failover(no_switch, ok=False)
    asyncio.run(Resolver._runtime_failover(eng, ctx, snap))
    kinds = [k for k, _e in events]
    assert "runtime_switch_declined" in kinds
    assert "runtime_reconnect_requested" not in kinds
    assert eng.released == []                      # geen sessie gedood
    assert eng.runtime_metrics["runtime_switch_declined"] == 1


def test_31_runtime_switch_success_reconnect():
    """Geslaagde stabiele switch → gecontroleerde reconnect (enkel dan)."""
    async def do_switch(item, source, required, decision, background):
        return True
    eng, events, ctx, snap = _engine_failover(do_switch, ok=True)
    asyncio.run(Resolver._runtime_failover(eng, ctx, snap))
    kinds = [k for k, _e in events]
    assert "runtime_reconnect_requested" in kinds
    assert eng.released == ["h1"]


def test_32_buffering_cadence_geen_switch_storm():
    """Herhaald bufferen (~30s) zonder betere candidate → iedere keer
    eval uitgevoerd (declined), maar het failover-budget caps de storm."""
    # budget-logica: max 2 per 30-min venster (FASE 19) — blijft actief
    import inspect
    src = inspect.getsource(Resolver._failover_budget_ok)
    assert "jit_max_failovers_per_session" in src
    # en de declined-path is een apart event, geen extra sessie-kill
    src2 = inspect.getsource(Resolver._runtime_failover)
    assert "runtime_switch_declined" in src2
    assert src2.index("runtime_switch_declined") < src2.index("runtime_reconnect_requested")
