"""BACKGROUND_FLOOD_FIX (2026-10-11) — regressietests.

Incident: élke fresh open activeerde SEEK_RECOVERY_PREFETCH → korte
sweeper/soak-sessies startten speculatief W2/W3, closeden snel, al het
speculatieve werk werd gecanceld: ~10.8k activaties, ~22k cancels,
~250 GB "waste", 148–215 Mbit achtergrondverkeer bij gezonde provider.

Contract na fix:
  * activatie alléén na bewezen niet-sequentiële jump;
  * expliciete background-callers (two_way=0 → allow_speculation=False)
    speculative nooit;
  * globaal speculatief budget system-wide (non-blocking, P0 blokkeert
    nooit, slots yielden aan demand);
  * waste-metrics: alléén echt speculatieve bytes; gedemandeerd werk
    telt nooit als speculative waste.
"""
import asyncio
import os
import sys
from collections import defaultdict
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.stream import AdaptiveRangeReader              # noqa: E402
from plex_scraper.resolver.engine import Resolver                         # noqa: E402

KIB = 1024
MIB = 1024 * 1024
RA = 8 * MIB


class FakeEngine:
    def __init__(self, size, latency_s=0.0, spec_max=None):
        self.s = SimpleNamespace(stream_seq_escalate_after=6,
                                 stream_seq_escalate_max_len=128 * KIB,
                                 stream_speculative_global_max=spec_max)
        self.metrics = defaultdict(int)
        self.size = size
        self.latency_s = latency_s
        self.requests: list[tuple[int, int]] = []
        self.active = 0
        self.peak = 0
        self._spec_slots = 0

    async def upstream_read(self, source_id, offset, length):
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            self.requests.append((offset, length))
            if self.latency_s:
                await asyncio.sleep(self.latency_s)
            return b"\x01" * length
        finally:
            self.active -= 1

    # globale slot-administratie (als Resolver)
    def spec_slot_acquire(self) -> bool:
        cap = getattr(self.s, "stream_speculative_global_max", 3)
        if cap is None:                           # unlimited (test-engines)
            return True
        cap = max(0, int(cap))
        if cap == 0 or self._spec_slots >= cap:
            self.metrics["speculative_slot_denied"] += 1
            return False
        self._spec_slots += 1
        return True

    def spec_slot_release(self) -> None:
        if self._spec_slots > 0:
            self._spec_slots -= 1


def _reader(eng, **kw):
    return AdaptiveRangeReader(eng, "src1", kw.pop("size", 512 * MIB),
                               kw.pop("readahead", RA), **kw)


async def _reads(r, off, n, step=32 * KIB):
    for _ in range(n):
        await r.read(off, step)
        off += step


# ------------------------------------------------ Phase 1: fresh open
def test_fresh_open_geen_recover_en_geen_speculatie():
    eng = FakeEngine(512 * MIB)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 50)                    # 1.6 MB sequentieel
    asyncio.run(go())
    assert r.recovery is False
    assert eng.metrics.get("seek_recovery_activated", 0) == 0
    # hooguit de normale single-prefetch (P1) — geen W2/W3-speculatie
    assert eng.metrics.get("speculative_bytes_requested", 0) <= RA
    assert eng.metrics.get("speculative_bytes_wasted", 0) == 0


def test_sequentiele_32k_reads_blijven_normaal():
    """Normale Plex-like sequentiële reads → single-prefetch-escalatie,
    nooit recovery-concurrency."""
    eng = FakeEngine(512 * MIB, latency_s=0.005)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(1000):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
    asyncio.run(go())
    assert r.two_way is True and r.recovery is False
    assert eng.peak <= 2                          # P0 + hooguit 1 prefetch


def test_eerste_read_is_geen_jump():
    """Ook een grote eerste read (index/header-pull) mag nooit recovery
    geven: _buf_off == -1 → geen jump-detectie mogelijk."""
    eng = FakeEngine(512 * MIB)
    r = _reader(eng)

    async def go():
        await r.read(30 * MIB, 256 * KIB)
    asyncio.run(go())
    assert r.recovery is False


# ------------------------------------------------ Phase 3: background
def test_background_caller_seek_geen_recover():
    """two_way=0 / allow_speculation=False: óók een echte jump geeft geen
    recovery en geen speculatieve windows."""
    eng = FakeEngine(512 * MIB)
    r = _reader(eng, allow_speculation=False)

    async def go():
        await _reads(r, 0, 50)
        await r.read(200 * MIB, 32 * KIB)         # echte jump
        await r.read(20 * MIB, 32 * KIB)          # nog een jump
    asyncio.run(go())
    assert r.recovery is False
    assert eng.metrics.get("seek_recovery_activated", 0) == 0
    assert eng.metrics.get("speculative_bytes_requested", 0) == 0
    assert eng.metrics.get("remote_window_cancelled", 0) == 0


def test_sweeper_style_korte_sessies_geen_storm():
    """Vele korte probe-sessies (sweeper/soak-patroon): open → korte
    burst → close. Geen recovery-activaties, geen speculatief verkeer,
    geen cancels."""
    eng = FakeEngine(512 * MIB, latency_s=0.01)
    total_waste = 0

    async def go():
        nonlocal total_waste
        for i in range(50):
            r = _reader(eng, allow_speculation=False)
            await _reads(r, i * KIB, 8)           # korte probe-burst
            r.close()
            await asyncio.sleep(0)
        total_waste = eng.metrics.get("speculative_bytes_wasted", 0)
    asyncio.run(go())
    assert eng.metrics.get("seek_recovery_activated", 0) == 0
    assert total_waste == 0
    assert eng.metrics.get("remote_window_cancelled", 0) == 0


def test_soak_style_herhaald_short_open_geen_accumulatie():
    """Soak: herhaaldelijke korte opens met read+seek-achtig patroon op
    dezelfde engine — speculatieve tellers blijven op nul."""
    eng = FakeEngine(512 * MIB, latency_s=0.0)

    async def go():
        for _ in range(30):
            r = _reader(eng, allow_speculation=False)
            await r.read(0, 32 * KIB)
            await r.read(100 * MIB, 32 * KIB)     # probe-spring (geen spec)
            r.close()
    asyncio.run(go())
    assert eng.metrics.get("speculative_bytes_requested", 0) == 0
    assert eng.metrics.get("speculative_bytes_used", 0) == 0
    assert eng.metrics.get("speculative_bytes_wasted", 0) == 0


# ------------------------------------------------ Phase 2: echte seek
def test_jump_activeert_en_concurrentie_werkt():
    """Echte jump → recovery met echte window-concurrentie (P0+P1[+P2]),
    cap 3."""
    eng = FakeEngine(512 * MIB, latency_s=0.05)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 50)
        assert r.recovery is False
        await r.read(200 * MIB, 32 * KIB)         # jump → recovery
        off = 200 * MIB + 32 * KIB
        for _ in range(400):                      # doorlopen in recovery
            await r.read(off, 32 * KIB)
            off += 32 * KIB
            await asyncio.sleep(0.001)
    asyncio.run(go())
    assert eng.metrics.get("seek_recovery_activated", 0) == 1
    assert eng.peak >= 2                          # concurrency werkt
    assert eng.peak <= 3
    assert eng.metrics.get("concurrent_windows_peak", 0) <= 3


def test_recovery_cap_max_3():
    eng = FakeEngine(512 * MIB, latency_s=0.02)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 20)
        await r.read(200 * MIB, 32 * KIB)
        off = 200 * MIB + 32 * KIB
        for _ in range(300):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
    asyncio.run(go())
    assert len(r._inflight) <= 3
    assert len(r._ready) <= 2


def test_deescalatie_en_reactivatie():
    eng = FakeEngine(512 * MIB, latency_s=0.0)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 20)
        await r.read(200 * MIB, 32 * KIB)
        off = 200 * MIB + 32 * KIB
        for _ in range(700):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
        assert r.recovery is False
        await r.read(50 * MIB, 32 * KIB)          # nieuwe seek
    asyncio.run(go())
    assert r.recovery is True
    assert eng.metrics["seek_recovery_activated"] == 2
    assert eng.metrics["seek_recovery_deescalated"] == 1


def test_close_cancelt_speculatief_en_telt_waste():
    eng = FakeEngine(512 * MIB, latency_s=0.2)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 50)
        await r.read(200 * MIB, 32 * KIB)         # recovery + prefetch
        await asyncio.sleep(0.01)
        assert r._inflight
        r.close()
        await asyncio.sleep(0.02)
    asyncio.run(go())
    assert eng.metrics.get("speculative_bytes_wasted", 0) > 0
    assert not r._inflight


# ------------------------------------------------ Phase 4: globaal budget
def test_globaal_budget_wordt_afgedwongen():
    """System-wide cap: vier gelijktijdige sessies met elk een prefetch →
    de vierde krijgt geen slot meer (non-blocking)."""
    eng = FakeEngine(512 * MIB, latency_s=0.5, spec_max=3)
    readers = [_reader(eng) for _ in range(4)]

    async def go():
        async def session(i):
            await _reads(readers[i], i * 100 * MIB, 9)   # escalatie → P1
        await asyncio.gather(*(session(i) for i in range(4)))
        await asyncio.sleep(0.05)                 # prefetches laten aflopen
    asyncio.run(go())
    assert eng.metrics.get("speculative_slot_denied", 0) >= 1
    assert eng._spec_slots == 0                   # geen leak


def test_demand_wordt_nooit_geblokkeerd_door_budget():
    """Budget uitgeput → gedemandeerde P0-read slaagt gewoon (synchroon
    miss-pad gaat niet via slots)."""
    eng = FakeEngine(512 * MIB, latency_s=0.05, spec_max=1)
    r1, r2 = _reader(eng), _reader(eng)

    async def go():
        # r1 vult het enige slot
        await _reads(r1, 0, 10)
        await asyncio.sleep(0.01)
        # r2: demanded reads moeten slagen ondanks vol budget
        data = await r2.read(300 * MIB, 32 * KIB)
        assert data == b"\x01" * 32 * KIB
        data = await r2.read(320 * MIB, 32 * KIB)  # jump-demandaard
        assert data == b"\x01" * 32 * KIB
    asyncio.run(go())


def test_slots_lekken_niet_bij_cancel():
    """Jump-cancel + close geven elk slot terug — budget herstelt volledig."""
    eng = FakeEngine(512 * MIB, latency_s=0.1, spec_max=3)
    r = _reader(eng)

    async def go():
        for _ in range(5):                        # jumps cancelen telkens
            await r.read(200 * MIB, 32 * KIB)
            await asyncio.sleep(0.01)
            await r.read(300 * MIB, 32 * KIB)
            await asyncio.sleep(0.01)
        r.close()
        await asyncio.sleep(0.02)
    asyncio.run(go())
    assert eng._spec_slots == 0


# ------------------------------------------------ Phase 5: waste-metrics
def test_gecancelde_demanded_bytes_niet_als_waste():
    """Een niet-speculatief in-flight window wordt bij cancel geboekt als
    demanded_bytes_cancelled, nooit als speculative_bytes_wasted."""
    eng = FakeEngine(512 * MIB, latency_s=0.3)
    r = _reader(eng, two_way=True)

    async def go():
        # simuleer een gedemandeerd in-flight window (niet via _schedule)
        r._inflight[RA] = asyncio.create_task(r._fetch_window(RA))
        await asyncio.sleep(0.01)
        await r.read(300 * MIB, 32 * KIB)         # jump: cancelt obsolete
        await asyncio.sleep(0.01)
    asyncio.run(go())
    assert eng.metrics.get("speculative_bytes_wasted", 0) == 0
    assert eng.metrics.get("demanded_bytes_cancelled", 0) == 1


def test_allleen_speculatieve_bytes_in_waste():
    """requested ≤ used + wasted-consistentie: waste telt alléen windows
    die ook als speculative requested zijn geboekt."""
    eng = FakeEngine(512 * MIB, latency_s=0.05)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 50)
        await r.read(200 * MIB, 32 * KIB)         # jump + cancels
        off = 200 * MIB + 32 * KIB
        for _ in range(300):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
        r.close()
    asyncio.run(go())
    req = eng.metrics.get("speculative_bytes_requested", 0)
    used = eng.metrics.get("speculative_bytes_used", 0)
    wasted = eng.metrics.get("speculative_bytes_wasted", 0)
    assert wasted > 0                             # close wist ready-windows
    assert req >= used + wasted - RA              # bookkeeping-consistentie
    # requested is jetzt inclusief P1 — elke waste-positie was requested
    assert wasted <= req


# ------------------------------------------------ engine-integratie
def test_resolver_slot_administratie_bestaat():
    """Resolver heeft de globale slot-administratie gekoppeld aan de
    config-cap (structurele guard)."""
    import inspect
    src = inspect.getsource(Resolver.spec_slot_acquire)
    assert "stream_speculative_global_max" in src
    src = inspect.getsource(Resolver.spec_slot_release)
    assert "_spec_slots_used" in src
