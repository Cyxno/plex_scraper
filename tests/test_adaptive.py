"""Adaptive throughput + read-ahead tests (FASE 22-nummering in docstrings)."""
import asyncio
import os
import sqlite3
import sys
from collections import defaultdict
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.media import build_profile                    # noqa: E402
from plex_scraper.resolver.stream import AdaptiveRangeReader             # noqa: E402
from plex_scraper.resolver.engine import Resolver                        # noqa: E402
from plex_scraper.resolver.health import HealthSweeper                   # noqa: E402
from plex_scraper.common.config import Settings                          # noqa: E402

MB = 1024 * 1024


# ---------------------------------------------------------------- helpers
class FakeEngine:
    def __init__(self, payload: bytes, err_offsets: set[int] | None = None,
                 delay: float = 0.0):
        self.payload = payload
        self.err_offsets = err_offsets or set()
        self.delay = delay
        self.metrics = defaultdict(int)
        self.upstream_calls: list[tuple[int, int]] = []

    async def upstream_read(self, source_id: str, offset: int, length: int) -> bytes:
        self.upstream_calls.append((offset, length))
        if self.delay:
            await asyncio.sleep(self.delay)
        if offset in self.err_offsets:
            raise RuntimeError("upstream 4xx/5xx")
        return self.payload[offset:offset + length]


def _mk_sweeper(tmp_path, resolver=None, **kw):
    kw.setdefault("items_per_hour", 360000)
    return HealthSweeper(resolver or SimpleNamespace(),
                         str(tmp_path / "hs.sqlite"), **kw)


def _async(x):
    async def f():
        return x
    return f()


def _events(db):
    c = sqlite3.connect(db)
    rows = [r[0] for r in c.execute("SELECT event FROM health_events")]
    c.close()
    return rows


# ------------------------------------- FASE 2: media-aware requirement
def test_01_low_bitrate_media_high_throughput_is_healthy(tmp_path):
    """(1) 10 Mbit media + 30 Mbit source → HEALTHY_FOR_MEDIA."""
    prof = build_profile(size_bytes=None, media_bitrate_mbit=10.0,
                         duration_s=None, floor_mbit=25.0)
    required = prof.required_mbit(1.5)
    assert required == 15.0
    sw = _mk_sweeper(tmp_path)
    for _ in range(3):
        sw._throughput_observe("ep", {"mbit": 30.0, "ttfb_s": 0.5,
                                      "required_mbit": required})
    assert sw.throughput_state("ep") == "HEALTHY_FOR_MEDIA"
    assert "ep" not in sw._degraded


def test_02_high_bitrate_media_low_throughput_degrades(tmp_path):
    """(2) 60 Mbit media + 30 Mbit source → DEGRADED_THROUGHPUT."""
    prof = build_profile(size_bytes=None, media_bitrate_mbit=60.0,
                         duration_s=None, floor_mbit=25.0)
    required = prof.required_mbit(1.5)
    assert required == 90.0
    sw = _mk_sweeper(tmp_path)
    for _ in range(3):
        sw._throughput_observe("remux", {"mbit": 30.0, "ttfb_s": 1.0,
                                         "required_mbit": required})
    assert sw.throughput_state("remux") == "DEGRADED_THROUGHPUT"
    assert "remux" in sw._degraded


def test_derived_bitrate_from_size_duration():
    """FASE 2 fallback: filesize/duration → bitrate (PotC: 55,1GB/168min)."""
    prof = build_profile(size_bytes=55_111_675_036, media_bitrate_mbit=None,
                         duration_s=10108.288, floor_mbit=25.0)
    assert prof.confidence == "derived"
    assert 42.0 < prof.bitrate_mbit < 45.0        # ≈ 43,7 Mbit/s


def test_floor_confidence_when_nothing_known():
    prof = build_profile(size_bytes=0, media_bitrate_mbit=None,
                         duration_s=None, floor_mbit=25.0)
    assert prof.confidence == "floor"
    assert prof.bitrate_mbit == 25.0


# ------------------------------------- FASE 6: strikes + hysteresis
def test_03_one_bad_sample_is_not_degraded(tmp_path):
    """(3) één slechte meting = observatie."""
    sw = _mk_sweeper(tmp_path)
    sw._throughput_observe("p", {"mbit": 1.0, "ttfb_s": 8.0,
                                 "required_mbit": 37.5})
    assert "p" not in sw._degraded
    assert sw.throughput_state("p") == "HEALTHY_FOR_MEDIA"


def test_04_three_bad_samples_degrade(tmp_path):
    """(4) drie slechte metingen = DEGRADED_THROUGHPUT."""
    sw = _mk_sweeper(tmp_path)
    for _ in range(3):
        sw._throughput_observe("p", {"mbit": 5.0, "ttfb_s": 6.0,
                                     "required_mbit": 37.5})
    assert "p" in sw._degraded
    assert sw.throughput_state("p") == "DEGRADED_THROUGHPUT"


def test_05_recovery_hysteresis(tmp_path):
    """(5) herstel pas na 2 goede checks."""
    sw = _mk_sweeper(tmp_path)
    for _ in range(3):
        sw._throughput_observe("p", {"mbit": 5.0, "ttfb_s": 6.0,
                                     "required_mbit": 37.5})
    sw._throughput_observe("p", {"mbit": 90.0, "ttfb_s": 0.4,
                                 "required_mbit": 37.5})
    assert sw.throughput_state("p") == "DEGRADED_THROUGHPUT"
    sw._throughput_observe("p", {"mbit": 90.0, "ttfb_s": 0.4,
                                 "required_mbit": 37.5})
    assert sw.throughput_state("p") == "HEALTHY_FOR_MEDIA"


def test_read_failure_marks_broken(tmp_path):
    sw = _mk_sweeper(tmp_path)
    sw._mark_broken("x")
    assert sw.throughput_state("x") == "BROKEN"


# ------------------------------------- FASE 7/12: adaptive read mode
def _fake_self(required_threshold=40.0, two_way=True, degraded=frozenset()):
    s = Settings(playback_min_mbit=25.0, sweeper_throughput_margin=1.5,
                 adaptive_two_way_min_mbit=required_threshold,
                 stream_two_way_enabled=two_way)
    return SimpleNamespace(s=s, _sweeper=SimpleNamespace(
        degraded_throughput=degraded))


def test_06_sufficient_single_stays_single():
    """(6) required 37,5 < drempel 40 en niet degraded → SINGLE."""
    fake = _fake_self()
    item = SimpleNamespace(plex_path="ep.mkv")
    assert Resolver.two_way_for(fake, item, 37.5) is False


def test_07_high_bitrate_enables_2way():
    """(7) required 65 (4K remux) ≥ drempel → 2-way."""
    fake = _fake_self()
    item = SimpleNamespace(plex_path="movie.mkv")
    assert Resolver.two_way_for(fake, item, 65.4) is True


def test_degraded_item_enables_2way_even_low_bitrate():
    fake = _fake_self(degraded=frozenset(["ep.mkv"]))
    item = SimpleNamespace(plex_path="ep.mkv")
    assert Resolver.two_way_for(fake, item, 20.0) is True


def test_two_way_disabled_by_config():
    fake = _fake_self(two_way=False)
    item = SimpleNamespace(plex_path="movie.mkv")
    assert Resolver.two_way_for(fake, item, 90.0) is False


# ------------------------------------- FASE 8: byte order / cancel-safety
def test_08_two_way_preserves_byte_order():
    """(8) 2-way read-ahead levert exact dezelfde bytes (sequentieel + seek)."""
    payload = bytes(range(256)) * 4096          # 1 MB
    eng = FakeEngine(payload)
    rd = AdaptiveRangeReader(eng, "s", len(payload), readahead=256 * 1024,
                             two_way=True)

    async def run():
        out = bytearray()
        off = 0
        for _ in range(12):                     # sequentieel, 48KB-stappen
            chunk = await rd.read(off, 48 * 1024)
            out += chunk
            off += len(chunk)
            await asyncio.sleep(0)              # laat prefetch lopen
        # seek naar het midden en verder lezen
        off = len(payload) // 2
        chunk = await rd.read(off, 100 * 1024)
        out += chunk
        return bytes(out)

    got = asyncio.run(run())
    assert got == payload[:12 * 48 * 1024] + payload[len(payload) // 2:len(payload) // 2 + 100 * 1024]


def test_09_seek_cancels_stale_prefetch():
    """(9) seek buiten het prefetch-traject cancelt de prefetch."""
    payload = bytes(2 * MB)
    eng = FakeEngine(payload)
    rd = AdaptiveRangeReader(eng, "s", len(payload), readahead=256 * 1024,
                             two_way=True)

    async def run():
        await rd.read(0, 48 * 1024)
        task = rd._prefetch_task
        assert task is not None and not task.done()
        # ver seek: ver voorbij het prefetch-window
        data = await rd.read(1500 * 1024, 48 * 1024)
        assert data == payload[1500 * 1024:1500 * 1024 + 48 * 1024]
        await asyncio.sleep(0)  # laat de loop de cancel verwerken
        assert task.cancelled() or task.done()   # gecanceld óf al klaar (geen leak)
        assert rd._prefetch_task is None or rd._prefetch_off != 256 * 1024

    asyncio.run(run())


def test_10_stop_cancels_prefetch_no_leak():
    """(10) close() cancelt de prefetch en laat geen task achter."""
    payload = bytes(MB)
    eng = FakeEngine(payload)
    rd = AdaptiveRangeReader(eng, "s", len(payload), readahead=256 * 1024,
                             two_way=True)

    async def run():
        await rd.read(0, 48 * 1024)
        assert rd._prefetch_task is not None
        rd.close()
        assert rd._prefetch_task is None
        assert rd._prefetch_off is None
        assert rd._buf == b""
    asyncio.run(run())


def test_11_429_burst_triggers_fallback_to_single():
    """(11) herhaalde provider-fouten tijdens 2-way → fallback single."""
    payload = bytes(4 * MB)
    eng = FakeEngine(payload, err_offsets={256 * 1024})   # prefetch-zone faalt
    rd = AdaptiveRangeReader(eng, "s", len(payload), readahead=256 * 1024,
                             two_way=True, fallback_after_errors=2)

    async def run():
        await rd.read(0, 48 * 1024)             # start prefetch die faalt
        try:
            await rd._prefetch_task
        except Exception:
            pass
        await rd.read(0, 48 * 1024)             # hit in buffer; nieuwe prefetch
        try:
            if rd._prefetch_task:
                await rd._prefetch_task
        except Exception:
            pass
        # tweede fout → fallback
        assert rd.two_way is False
        assert eng.metrics["adaptive_fallbacks"] == 1

    asyncio.run(run())


def test_12_5xx_does_not_corrupt_stream():
    """(12) fouten leveren nooit verkeerde bytes — read faalt of geeft het
    juiste window; herstel werkt daarna gewoon."""
    payload = bytes(range(256)) * (MB // 256)
    eng = FakeEngine(payload, err_offsets={256 * 1024})
    rd = AdaptiveRangeReader(eng, "s", len(payload), readahead=256 * 1024,
                             two_way=True)

    async def run():
        first = await rd.read(0, 48 * 1024)
        assert first == payload[:48 * 1024]
        # gefaalde zone: prefetch faalt → read zelf faalt (geen foute bytes)
        try:
            await rd.read(256 * 1024 + 1, 48 * 1024)
            assert False, "moet falen"
        except Exception:
            pass
        # herstel: geldige zone leest correct
        good = await rd.read(512 * 1024, 48 * 1024)
        assert good == payload[512 * 1024:512 * 1024 + 48 * 1024]

    asyncio.run(run())


# ------------------------------------- FASE 14: candidate policy
def test_13_candidate_throughput_policy(tmp_path):
    """(13) kandidaat alleen acceptabel bij ≥ required×1.2 én ≥ current×1.5."""
def _mk_sweeper_with_policy(tmp_path):
    sw = _mk_sweeper(tmp_path, SimpleNamespace())
    sw.candidate_headroom = 1.2
    sw.candidate_min_improvement = 1.5
    return sw


    sw = _mk_sweeper_with_policy(tmp_path)
    required = 65.4        # 4K-remux requirement
    current = 38.0         # gemeten current source
    assert sw._candidate_acceptable(45.0, current, required) is False   # < required×1.2 (78,5)
    assert sw._candidate_acceptable(78.5, current, required) is True    # ≥ headroom, 2,1×
    assert sw._candidate_acceptable(60.0, current, required) is False   # onder headroom
    assert sw._candidate_acceptable(120.0, current, required) is True   # 3,2×
    # huidige-bron-bescherming: kandidaat moet áánstoonlijk beter zijn
    assert sw._candidate_acceptable(55.0, 40.0, 37.5) is False          # 1,375× < 1,5×
    assert sw._candidate_acceptable(61.0, 40.0, 37.5) is True           # 1,525× en ≥ headroom




def test_14_no_throughput_auto_switch(tmp_path, monkeypatch):
    """(14) throughput-would-switch raakt NIET de actieve source aan."""
    sw = _mk_sweeper_with_policy(tmp_path)
    item = SimpleNamespace(id="i1", plex_path="p.mkv", title="T", series=None,
                           season=None, episode=None, year=None, status="READY")
    calls = {"resolve": 0}

    async def fake_shadow(item_):
        return {"best": {"hash": "h1", "score": 80.0}, "would_switch": True,
                "identity_rejected": 0}
    sw._shadow_evaluate = fake_shadow

    async def no_repair(item_):
        calls["resolve"] += 1
        return True
    sw._repair = no_repair

    src = SimpleNamespace(info_hash="h1", cached=True, size=1_000_000_000,
                          torrent_name="x", file_id=0)
    sw.resolver = SimpleNamespace(
        store=SimpleNamespace(
            list_sources=lambda iid: _async([src]),
            get_item=lambda iid: _async(item)),
        provider=SimpleNamespace(
            ensure_torrent=lambda h, n: _async(SimpleNamespace(torrent_id=1)),
            get_stream_url=lambda tid, fid: _async("http://stream/x"),
            read_range=lambda url, off, ln: _async(b"\xff" * ln)))

    async def run():
        await sw._throughput_would_switch(item, measured_mbit=30.0)
    asyncio.run(run())
    assert calls["resolve"] == 0                    # géén switch/repair


def test_15_playback_pauses_sweeper(tmp_path):
    """(15) actieve playback pauzeert de sweep nog steeds."""
    item = SimpleNamespace(id="i", plex_path="p.mkv", status="READY",
                           title="T", series=None, season=None, episode=None,
                           year=None, kind="movie")
    store = SimpleNamespace(list_items=lambda: _async([item]))
    resolver = SimpleNamespace(
        store=store,
        _gather_candidates=lambda i: _async([]),
        _rank_candidates=lambda i, c: _async([]),
        _active_source=lambda iid: _async(None))
    sw = _mk_sweeper(tmp_path, resolver)
    sw.playback_active_count = lambda: _async(1)
    asyncio.run(sw.sweep())
    assert "sweep_paused_playback" in _events(sw.db_path)
