"""Stabilisatiepass-tests: DOEL 1-7 regressie-bescherming."""
import asyncio
import json
import os
import sqlite3
import sys
import time
from types import SimpleNamespace

import pytest

import httpx  # noqa: F401  (type hints in handlers)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

import plex_scraper.scraper.providers.torbox as torbox_mod          # noqa: E402
from plex_scraper.scraper.providers.base import ProviderError       # noqa: E402
from plex_scraper.scraper.providers.torbox import TorboxProvider    # noqa: E402
from plex_scraper.resolver.health import HealthSweeper              # noqa: E402

from conftest import cand, got_item, got_key, got_results, make_engine  # noqa: E402


# ------------------------------------------------------- DOEL 4: mylist-bug
def test_extract_torrent_id_rejects_empty():
    assert TorboxProvider._extract_torrent_id({"torrent_id": ""}) is None
    assert TorboxProvider._extract_torrent_id({"id": None}) is None
    assert TorboxProvider._extract_torrent_id({}) is None
    assert TorboxProvider._extract_torrent_id({"torrent_id": "123"}) == 123
    assert TorboxProvider._extract_torrent_id({"id": 456}) == 456


def _mk_provider(handler) -> TorboxProvider:
    import httpx
    settings = SimpleNamespace(
        torbox_base_url="https://api.torbox.dev", torbox_api_token="t",
        torbox_timeout_read=5.0, torbox_timeout_connect=2.0,
        torbox_max_retries=2, torrent_ready_max_polls=1,
        torrent_ready_poll_interval=0.01)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return TorboxProvider(settings, client=client)


def test_add_magnet_duplicate_empty_id_falls_back_to_mylist():
    """Regressie: createtorrent duplicate-add geeft torrent_id='' → de oude
    code pollde mylist met id='' → HTTP 422 int_parsing."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.path == "/torrents/createtorrent":
            return httpx.Response(200, json={"success": True, "data": {"torrent_id": ""}})
        if request.url.path == "/torrents/mylist":
            return httpx.Response(200, json={"success": True, "data": [
                {"hash": "ABC", "id": 77, "download_finished": True}]})
        return httpx.Response(404)

    p = _mk_provider(handler)
    torrent = asyncio.run(p._add_magnet("abc", "name"))
    assert torrent["id"] == 77
    assert not any("id=&" in u or u.endswith("id=") for u in calls)


def test_read_range_400_is_transient_retried():
    """DOEL 5: eerste 400 → retry binnen dezelfde call; tweede poging lukt."""
    import httpx
    state = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["n"] += 1
        if state["n"] == 1:
            return httpx.Response(400, json={"error": "transient"})
        return httpx.Response(206, content=b"x" * 1000)

    p = _mk_provider(handler)
    data = asyncio.run(p.read_range("https://stream/x", 0, 1000))
    assert len(data) == 1000
    assert state["n"] == 2


def test_read_range_400_persistent_raises_with_body():
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, content=b'{"error":"BAD"}')

    p = _mk_provider(handler)
    with pytest.raises(ProviderError) as exc:
        asyncio.run(p.read_range("https://stream/x", 0, 1000))
    assert "400" in str(exc.value) and "BAD" in str(exc.value)


# ------------------------------------------- DOEL 1/2/3: engine-gedrag
def _src(info_hash, name, size=1_000_000_000):
    from plex_scraper.common.domain import models as m
    return m.Source(id=m.new_id(), media_item_id="x", generation=1,
                    provider="torbox", info_hash=info_hash,
                    torrent_name=name, size=size)


async def _register(engine, item):
    return await engine.register_item(dict(item))


async def test_failed_repair_keeps_readable_current_source(settings, scorer, got_item, monkeypatch):
    """DOEL 1: alle nieuwe kandidaten falen, maar de actieve bron leest nog →
    item blijft READY met dezelfde bron (repair_kept_current), NIET NO_SOURCE."""
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await _register(engine, got_item)
    first = await engine._active_source(item.id)

    async def fail_all(item_, cand):
        return None
    monkeypatch.setattr(engine, "_validate_candidate", fail_all)

    async def probe_ok(src):
        return True
    monkeypatch.setattr(engine, "_probe_readable", probe_ok)

    got = await engine.resolve_item(item, reason="forced")
    assert got is not None and got.info_hash == first.info_hash
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "READY"
    evs = [e["kind"] for e in await engine.store.recent_events(20)]
    assert "repair_kept_current" in evs


async def test_failed_repair_demotes_when_current_unreadable(settings, scorer, got_item, monkeypatch):
    """Als de actieve bron ook niet meer leest: terecht NO_SOURCE."""
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await _register(engine, got_item)

    async def fail_all(item_, cand):
        return None
    monkeypatch.setattr(engine, "_validate_candidate", fail_all)

    async def probe_dead(src):
        return False
    monkeypatch.setattr(engine, "_probe_readable", probe_dead)

    got = await engine.resolve_item(item, reason="forced")
    assert got is None
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "NO_SOURCE"


async def test_grandfathered_hash_bypasses_identity_gate(settings, scorer, got_item):
    """DOEL 2: de historisch actieve pack-hash (naam zonder SxxEyy) mag bij
    re-resolve opnieuw geactiveerd worden; een verkeerde NIEUWE kandidaat niet."""
    from plex_scraper.common.domain import models as m
    engine, _p, _s = make_engine(
        settings, {"wrong1": {"cached": True, "validate_fails": True},
                   "legacy_pack": {"cached": True}},
        {got_key(): got_results()}, scorer)
    item = await _register(engine, got_item)

    # simuleer legacy-staat: het actieve item is een pack zonder SxxEyy
    pack = m.Source(id=m.new_id(), media_item_id=item.id, generation=1,
                    provider="torbox", info_hash="legacy_pack",
                    torrent_name="Game.of.Thrones.S01.COMPLETE.1080p.BluRay-GRP",
                    size=50_000_000_000, cached=True, score=70.0, file_id=0)
    await engine.store.upsert_source(pack)
    await engine._activate(item, pack, await engine._active_source(item.id),
                           "legacy_migration")

    # re-resolve: alleen de verkeerde nieuwe kandidaat + het legacy-pack zijn
    # beschikbaar (geen andere identiteits-juiste release) — wrong1 faalt
    # validatie, legacy_pack is grandfathered en moet opnieuw passeren
    async def only_two(item_):
        return [cand("wrong1", "Better.Call.Saul.S01E01.2160p.REMUX-GRP", size=8_000_000_000),
                cand("legacy_pack", "Game.of.Thrones.S01.COMPLETE.1080p.BluRay-GRP",
                     size=50_000_000_000)]
    engine._gather_candidates = only_two
    orig_validate = engine._validate_candidate

    async def fail_wrong(item_, cand_):
        if cand_.info_hash == "wrong1":
            return None
        return await orig_validate(item_, cand_)
    engine._validate_candidate = fail_wrong

    got = await engine.resolve_item(item, reason="forced")
    assert got is not None and got.info_hash == "legacy_pack"
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "READY"
    # de verkeerde nieuwe kandidaat moet geweigerd zijn
    evs = await engine.store.recent_events(40)
    wrong_rejects = [e for e in evs if e["kind"] == "candidate_identity_rejected"
                     and e.get("hash") == "wrong1"]
    assert wrong_rejects


async def test_crash_reconciles_status(settings, scorer, got_item, monkeypatch):
    """DOEL 3: een exception tijdens resolve laat de status nooit vaststaan
    op RESOLVING/CANDIDATE_VALIDATION; met actieve bron → READY."""
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await _register(engine, got_item)

    async def boom(item_, cand):
        raise RuntimeError("probe ontplofte")
    monkeypatch.setattr(engine, "_validate_candidate", boom)

    with pytest.raises(RuntimeError):
        await engine.resolve_item(item, reason="forced")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "READY"


def test_store_reconcile_stale(settings, tmp_path):
    from plex_scraper.common.domain import models as m
    from plex_scraper.resolver.store import Store
    store = Store(str(tmp_path / "s.db"))

    async def seed():
        for i, status in (("a", "CANDIDATE_VALIDATION"), ("b", "RESOLVING")):
            item = m.MediaItem(id=i, kind="movie", title=i.upper(),
                               plex_path=f"{i}.mkv", status=status)
            await store.create_item(item)

        def backdate(c):
            c.execute("UPDATE media_items SET updated_at=? WHERE id IN ('a','b')",
                      (m.now() - 3600,))
        await store.run(backdate)
    asyncio.run(seed())

    fixed = asyncio.run(store.reconcile_stale(900.0))
    assert len(fixed) == 2
    assert all(f["now"] == "NO_SOURCE" for f in fixed)   # geen actieve bronnen


# ------------------------------------------- DOEL 6/7: sweeper
def _mk_sweeper(tmp_path, resolver, **kw):
    kw.setdefault("items_per_hour", 360000)
    return HealthSweeper(resolver, str(tmp_path / "hs.sqlite"), **kw)


def test_throughput_degraded_needs_three_strikes(tmp_path):
    sw = _mk_sweeper(tmp_path, SimpleNamespace())
    req = sw._required_mbit()
    for i in range(2):
        sw._throughput_observe("p", {"mbit": req / 4, "ttfb_s": 1.0,
                                     "required_mbit": req})
        assert "p" not in sw._degraded
    sw._throughput_observe("p", {"mbit": req / 4, "ttfb_s": 1.0,
                                 "required_mbit": req})
    assert "p" in sw._degraded
    evs = sqlite3.connect(sw.db_path).execute(
        "SELECT event, count(*) FROM health_events GROUP BY event").fetchall()
    kinds = dict(evs)
    assert kinds.get("throughput_strike") == 1   # alleen strike-1 wordt gelogd
    assert kinds.get("throughput_degraded") == 1


def test_throughput_recovers_on_good_observation(tmp_path):
    sw = _mk_sweeper(tmp_path, SimpleNamespace())
    req = sw._required_mbit()
    for _ in range(3):
        sw._throughput_observe("p", {"mbit": req / 4, "ttfb_s": 1.0,
                                     "required_mbit": req})
    assert "p" in sw._degraded
    sw._throughput_observe("p", {"mbit": req * 4, "ttfb_s": 0.3,
                                 "required_mbit": req})
    assert "p" in sw._degraded                       # FASE 6: 1 goede check cleart niet
    sw._throughput_observe("p", {"mbit": req * 4, "ttfb_s": 0.3,
                                 "required_mbit": req})
    assert "p" not in sw._degraded                   # pas na 2 goede checks
    kinds = dict(sqlite3.connect(sw.db_path).execute(
        "SELECT event, count(*) FROM health_events GROUP BY event").fetchall())
    assert kinds.get("throughput_recovered") == 1


async def test_sweep_pauses_on_active_playback(tmp_path):
    """DOEL 6: playback actief → sweep doet niets."""
    from test_sweeper import _fake_resolver, _mk_item
    item = _mk_item()
    resolver = _fake_resolver([item], {item.id: []})
    sw = _mk_sweeper(tmp_path, resolver)
    sw.playback_active_count = lambda: _async_result(2)
    await sw.sweep()
    kinds = _hs_events(sw.db_path)
    assert "sweep_paused_playback" in kinds
    assert "sweep_repair_needed" not in kinds


def _async_result(v):
    async def f():
        return v
    return f()


def _hs_events(db):
    c = sqlite3.connect(db)
    rows = [r[0] for r in c.execute("SELECT event FROM health_events")]
    c.close()
    return rows
