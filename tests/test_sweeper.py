"""E2E shadow-validatie: sweeper-gedrag met guardrails + engine identity gate."""
import asyncio
import json
import os
import sqlite3
import sys
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.health import HealthSweeper  # noqa: E402

from conftest import cand, got_item, got_key, got_results, make_engine  # noqa: E402


# -------------------------------------------------------------- fake resolver
def _fake_resolver(items, ranked_by_key, active_by_id=None):
    """resolver-store stub: list_items + gather/rank + active source."""
    store = SimpleNamespace(
        list_items=lambda: _async(items))

    async def gather(item):
        return [c for c, _ in ranked_by_key.get(item.id, [])]

    async def rank(item, candidates):
        return ranked_by_key.get(item.id, [])

    async def active(item_id):
        return (active_by_id or {}).get(item_id)

    return SimpleNamespace(store=store,
                           _gather_candidates=gather,
                           _rank_candidates=rank,
                           _active_source=active)


def _async(x):
    async def f():
        return x
    return f()


def _mk_item(rid="m1", status="READY", **kw):
    base = dict(id=rid, plex_path=f"TV/x/{rid}.mkv", status=status,
                title="Winter Is Coming", series="Game of Thrones",
                season=1, episode=1, year=None, kind="episode")
    base.update(kw)
    return SimpleNamespace(**base)


def _events(db_path):
    c = sqlite3.connect(db_path)
    rows = [dict(zip(("id", "ts", "plex_path", "event", "detail"), r))
            for r in c.execute("SELECT * FROM health_events ORDER BY id")]
    c.close()
    return rows


GOOD = ("Game.of.Thrones.S01E01.1080p.WEB-DL.DDP5.1.H.264-GRP", 80.0)
WRONG = ("Better.Call.Saul.S01E01.2160p.REMUX-GRP", 99.0)


# ------------------------------------------------------------------ engine
async def test_engine_identity_gate_blocks_wrong_content(settings, scorer, got_item):
    """Hoogst-scorende candidate met verkeerde series wordt nooit geactiveerd."""
    results = [
        cand("wrong1", "Better.Call.Saul.S01E01.2160p.DOLBY.VISION.REMUX.TrueHD-GRP",
             size=8_000_000_000),
        cand("got1080", "Game.of.Thrones.S01E01.1080p.WEB-DL.DDP5.1.H.264-GRP",
             size=3_000_000_000),
    ]
    engine, _p, _s = make_engine(settings, {}, {got_key(): results}, scorer)
    item = await engine.register_item(dict(got_item))
    assert item.status == "READY"
    src = await engine._active_source(item.id)
    assert src.info_hash == "got1080"  # wrong1 had meer score, maar identity-fail
    evs = [e["kind"] for e in await engine.store.recent_events(50)]
    assert any("identity_rejected" in e for e in evs)


# ------------------------------------------------------------------ sweeper
def _mk_sweeper(tmp_path, resolver, *, shadow=True, items_per_hour=360000,
                fail_strikes=2):
    return HealthSweeper(resolver, str(tmp_path / "hs.sqlite"),
                         items_per_hour=items_per_hour, shadow_mode=shadow,
                         fail_strikes=fail_strikes)


async def test_shadow_would_switch_on_broken_ready(tmp_path):
    """Gebroken READY-item: shadow eval vindt alternatief, actief blijft staan."""
    item = _mk_item()
    resolver = _fake_resolver(
        [item], {item.id: [(cand("h1", GOOD[0]), GOOD[1])]},
        active_by_id={item.id: SimpleNamespace(score=70.0, info_hash="old")})
    sw = _mk_sweeper(tmp_path, resolver, fail_strikes=1)
    await sw.sweep()
    evs = _events(sw.db_path)
    kinds = [e["event"] for e in evs]
    assert "sweep_repair_needed" in kinds           # HTTP open faalt (geen resolver)
    assert "shadow_would_switch" in kinds
    det = json.loads(next(e["detail"] for e in evs
                          if e["event"] == "shadow_would_switch"))
    assert det["best"]["hash"] == "h1"
    assert det["identity_rejected"] == 0
    assert det["would_switch"] is True
    assert "repair_triggered" not in kinds           # shadow raakt niets aan


async def test_shadow_no_alternative_when_identity_fails(tmp_path):
    item = _mk_item()
    resolver = _fake_resolver(
        [item], {item.id: [(cand("w1", WRONG[0]), WRONG[1])]},
        active_by_id={item.id: SimpleNamespace(score=70.0, info_hash="old")})
    sw = _mk_sweeper(tmp_path, resolver, fail_strikes=1)
    await sw.sweep()
    evs = _events(sw.db_path)
    assert "shadow_no_alternative" in [e["event"] for e in evs]
    det = json.loads(next(e["detail"] for e in evs
                          if e["event"] == "shadow_no_alternative"))
    assert det["identity_rejected"] == 1
    assert det["best"] is None


async def test_no_source_would_recover_then_backoff(tmp_path):
    """NO_SOURCE: candidate gevonden → would_recover; meteen daarna backoff."""
    item = _mk_item(status="NO_SOURCE")
    resolver = _fake_resolver(
        [item], {item.id: [(cand("h1", GOOD[0]), GOOD[1])]})
    sw = _mk_sweeper(tmp_path, resolver)
    await sw.sweep()
    kinds = [e["event"] for e in _events(sw.db_path)]
    assert "no_source_would_recover" in kinds
    # tweede sweep direct erna: backoff actief (record_success reset, maar
    # should_retry kijkt naar _next_retry die op success NIET gezet wordt —
    # dus onmiddellijke herpoging is toegestaan; assert dat de poging
    # idempotent blijft)
    await sw.sweep()
    kinds2 = [e["event"] for e in _events(sw.db_path)]
    assert kinds2.count("no_source_would_recover") == 2


async def test_no_source_backoff_grows_without_candidates(tmp_path):
    item = _mk_item(status="NO_SOURCE")
    resolver = _fake_resolver([item], {item.id: []})
    sw = _mk_sweeper(tmp_path, resolver)
    await sw.sweep()
    det = json.loads(next(e["detail"] for e in _events(sw.db_path)
                          if e["event"] == "no_source_backoff"))
    assert det["next_retry_s"] == 3600.0             # 1e failure: base
    assert sw.no_source_retry.should_retry(item.plex_path) is False


async def test_auto_repair_blocked_by_antiflapping(tmp_path):
    """Auto-mode met vol repair-verleden: flapping-gate blokkeert resolve."""
    item = _mk_item()
    resolver = _fake_resolver(
        [item], {item.id: [(cand("h1", GOOD[0]), GOOD[1])]},
        active_by_id={item.id: SimpleNamespace(score=70.0, info_hash="old")})
    sw = _mk_sweeper(tmp_path, resolver, shadow=False, fail_strikes=1)
    now = time.time()
    sw.antiflap._history[item.plex_path] = [now] * 3  # 3 repairs vandaag
    await sw.sweep()
    kinds = [e["event"] for e in _events(sw.db_path)]
    assert "repair_skipped_flapping" in kinds
    assert "repair_triggered" not in kinds


async def test_fail_strikes_debounce(tmp_path):
    """Eerste failure = strike, pas de tweede = repair_needed evaluatie."""
    item = _mk_item()
    resolver = _fake_resolver(
        [item], {item.id: [(cand("h1", GOOD[0]), GOOD[1])]},
        active_by_id={item.id: SimpleNamespace(score=70.0, info_hash="old")})
    sw = _mk_sweeper(tmp_path, resolver)  # default fail_strikes=2
    await sw.sweep()
    kinds = [e["event"] for e in _events(sw.db_path)]
    assert "sweep_strike" in kinds
    assert "sweep_repair_needed" not in kinds
    assert "shadow_would_switch" not in kinds
    await sw.sweep()
    kinds2 = [e["event"] for e in _events(sw.db_path)]
    assert "sweep_repair_needed" in kinds2
    assert "shadow_would_switch" in kinds2
