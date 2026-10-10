"""Regressie: read-ahead-escalatie voor kleine sequentiële FUSE-reads.

Bewezen incident (Pirates 2011, 2026-10-10 16:47 CEST): Plex leest 32 KiB
via FUSE; required 37,5 Mbit lag net onder de two-way-drempel (40 Mbit),
zodat windows serial geketend werden → 9–14 Mbit effectief, 2 s play /
10 s buffer. De reader fetcht al 8 MiB-windows; het gat is de prefetch van
het VOLGENDE window. Fix: dynamische escalatie naar prefetch zodra kleine
sequentiële reads zich herhalen.
"""
import asyncio
import os
import sys
import time
from collections import defaultdict
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.stream import AdaptiveRangeReader              # noqa: E402

KIB = 1024
MIB = 1024 * 1024
WINDOW = 8 * MIB


class FakeEngine:
    """upstream_read met configureerbare latentie per window-fetch."""

    def __init__(self, latency_s: float = 0.0, fail_after: int | None = None):
        self.s = SimpleNamespace(stream_seq_escalate_after=6,
                                 stream_seq_escalate_max_len=128 * KIB)
        self.metrics = defaultdict(int)
        self.latency_s = latency_s
        self.requests: list[tuple[int, int]] = []
        self._fail_after = fail_after

    async def upstream_read(self, source_id, offset, length):
        self.requests.append((offset, length))
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        return b"\x01" * length


def _reader(engine, size=4 * WINDOW):
    return AdaptiveRangeReader(engine, "src1", size, WINDOW, two_way=False)


def test_01_32k_sequentieel_escalneert_en_levert_boven_realtime():
    """(1) 32 KiB sequentieel op 37,5-Mbit-media: na escalatie wordt het
    volgende window onderweg gehouden — geen serialisatie-gat meer."""
    # CDN-latency 1,0 s per 8 MiB-window ≈ 67 Mbit per verbinding
    eng = FakeEngine(latency_s=1.0)
    r = _reader(eng)

    async def go():
        off = 0
        delivered = 0
        t0 = time.monotonic()
        for _ in range(400):                      # 400 × 32 KiB
            data = await r.read(off, 32 * KIB)
            assert len(data) == 32 * KIB
            off += len(data)
            delivered += len(data)
            await asyncio.sleep(0.0005)           # consument-tempo
        return delivered, time.monotonic() - t0
    delivered, dt = asyncio.run(go())
    assert r.two_way is True                      # geëscaleerd
    assert eng.metrics["readahead_escalations"] == 1
    # doorlooptijd ≪ som van losse window-fetches (windows overlappen)
    assert dt < 6 * eng.latency_s                 # 400 reads, ~1 fetch-latency


def test_02_20mbit_media_geen_overfetch():
    """(2) zelfde gedrag bij trager medium: overfetch blijft gebonden aan
    geconsumeerde windows (requests ≈ geconsumeerde windows, max 1 outstanding)."""
    eng = FakeEngine(latency_s=2.0)               # traag CDN ≈ 33 Mbit/window
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(100):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
    asyncio.run(go())
    # 100 × 32 KiB = 3,2 MB → hooguit 2 window-fetches (8 MiB window)
    assert len(eng.requests) <= 2
    assert all(ln == WINDOW for _off, ln in eng.requests)


def test_03_seek_annuleert_stale_prefetch():
    """(3) seek tijdens prefetch → oude read-ahead gecanceld, geen storm."""
    eng = FakeEngine(latency_s=5.0)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(7):                        # escalatie uitlokken
            await r.read(off, 32 * KIB)
            off += 32 * KIB
        assert r.two_way is True
        assert r._prefetch_task is not None       # prefetch loopt
        before = eng.metrics["prefetch_cancelled_bytes"]
        await r.read(3 * WINDOW, 32 * KIB)        # ver seek tijdens prefetch
        assert eng.metrics["prefetch_cancelled_bytes"] > before
        assert r._prefetch_off in (None,)
    asyncio.run(go())


def test_04_random_reads_geen_overfetch():
    """(4) random reads: de fresh-open-escalatie vuurt één keer (ffmpeg-open
    is een probe), maar random seeks cancelen de prefetch telkens — geen
    runaway overfetch, requests gebonden aan reads."""
    eng = FakeEngine(latency_s=0.0)
    r = _reader(eng)

    async def go():
        offs = [0, 2 * WINDOW, WINDOW, 3 * WINDOW, 0, 2 * WINDOW + 5 * KIB,
                WINDOW + 7 * KIB, 3 * WINDOW + 3 * KIB]
        for o in offs:
            await r.read(o, 32 * KIB)
            await r.read(o + 32 * KIB, 32 * KIB)
    asyncio.run(go())
    assert eng.metrics.get("readahead_escalations", 0) == 1   # alleen fresh-open
    # gebonden: hooguit 1 fetch + 1 gecancelde prefetch per read
    assert len(eng.requests) <= 2 * 8 * 2   # 8 random seeks, gebonden
    assert eng.metrics["prefetch_cancelled_bytes"] > 0        # seeks cancelen


def test_05_hoge_cdn_latentie_geen_per_read_bottleneck():
    """(5) hoge latentie: na escalatie serveert de buffer/prefetch de reads —
    het aantal synchrone fetches blijft beperkt tot windows."""
    eng = FakeEngine(latency_s=1.0)
    r = _reader(eng)

    async def go():
        t0 = time.monotonic()
        off = 0
        for _ in range(256):                      # 8 MiB → precies 1 window
            await r.read(off, 32 * KIB)
            off += 32 * KIB
        return time.monotonic() - t0, len(eng.requests)
    dt, nreq = asyncio.run(go())
    assert nreq == 1                              # 1 remote range voor 8 MiB
    assert dt < 2 * eng.latency_s                 # niet per-read vertraagd


def test_06_oud_vs_nieuw_metrics():
    """(6) vergelijkings-metrics bestaan: requests (range-grootte), delivered
    bytes en amplification zijn aftrekbaar uit de reader-metrics."""
    eng = FakeEngine(latency_s=0.5)
    r = _reader(eng)

    async def go():
        off = 0
        delivered = 0
        for _ in range(200):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
            delivered += 32 * KIB
        fetched = sum(ln for _o, ln in eng.requests)
        return delivered, fetched, len(eng.requests)
    delivered, fetched, nreq = asyncio.run(go())
    amplification = fetched / delivered
    # tail-slack van het laatste 8 MiB-window is inherent en gebonden
    # (max 1 window + max 1 outstanding prefetch); remote ranges blijven
    # groot (8 MiB) — géén per-32KiB-ranges
    assert amplification <= 1.35
    assert all(ln == WINDOW for _o, ln in eng.requests)
    assert nreq <= delivered // WINDOW + 2
