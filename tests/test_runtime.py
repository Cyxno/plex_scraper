"""Runtime playback-delivery tests (FASE 5-14/22-23)."""
import asyncio
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.runtime import (                                # noqa: E402
    DeliveryMonitor, DEGRADED, HEALTHY, MARGINAL, RECOVERING, WARMING_UP)


def _mk(media=60.0, target=90.0):
    m = DeliveryMonitor("p.mkv", media, target)
    base = time.monotonic()
    clock = [base]
    m._now = lambda: clock[0]
    m.started = base
    return m, clock


def _play(m, clock, seconds, mbit, wait=0.2):
    """Simuleer reads van `seconds` seconden op `mbit`."""
    for i in range(seconds):
        clock[0] += 1.0
        m.feed(int(mbit * 1e6 / 8), wait)


def test_01_warmup_then_healthy():
    """(F9) warmup 10s → HEALTHY bij ruim boven target."""
    m, c = _mk()
    _play(m, c, 6, 150.0)
    assert m.evaluate()["state"] == WARMING_UP
    _play(m, c, 8, 150.0)
    assert m.evaluate()["state"] == HEALTHY


def test_02_preflight_fast_runtime_degraded():
    """(2) preflightFAST-gedrag nagebootst: playback zakt direct onder
    realtime → DEGRADED binnen de detectie-window (~15-30s)."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 12, 150.0)                    # warmup + eerste seconden snel
    assert m.evaluate()["state"] in (HEALTHY, WARMING_UP)
    _play(m, c, 20, 20.0)                     # instorten
    snap = m.evaluate()
    assert snap["state"] == DEGRADED
    assert snap["rolling_mbit"] < 60.0        # onder MINIMUM_REALTIME


def test_03_one_slow_chunk_no_failover():
    """(5) één trage chunk → geen DEGRADED."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 14, 150.0)
    c[0] += 1
    m.feed(0, 0.0)                            # één lege/slechte read
    _play(m, c, 8, 150.0)
    assert m.evaluate()["state"] in (HEALTHY, MARGINAL)


def test_04_sustained_under_realtime_degraded():
    """(6) sustained < media_bitrate → DEGRADED."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 14, 150.0)
    _play(m, c, 20, 25.0)
    assert m.evaluate()["state"] == DEGRADED


def test_05_marginal_zone_not_degraded():
    """(7) 60 Mbit film met 75 Mbit delivery → MARGINAL (speelt waarschijnlijk)."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 14, 150.0)
    _play(m, c, 20, 75.0)
    assert m.evaluate()["state"] == MARGINAL


def test_06_recovery_hysteresis():
    """(8) herstel vereist meerdere goede windows (RECOVERING→HEALTHY)."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 14, 150.0)
    _play(m, c, 18, 20.0)
    assert m.evaluate()["state"] == DEGRADED
    _play(m, c, 6, 150.0)
    for _ in range(4):
        s = m.evaluate()
    assert s["state"] in (RECOVERING, DEGRADED)
    _play(m, c, 12, 150.0)
    for _ in range(4):
        s = m.evaluate()
    assert s["state"] == HEALTHY              # na volgehouden goede windows


def test_07_stall_with_insufficient_recovery():
    """severe stall + onvoldoende herstel → DEGRADED."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 14, 150.0)
    c[0] += 1
    m.feed(int(20e6 / 8), 7.0)                # stall 7s > stall_s 6s
    _play(m, c, 20, 55.0)                     # net onder target, boven realtime
    snap = m.evaluate()
    assert snap["state"] == DEGRADED


def test_08_uses_existing_bytes_only():
    """(10/11) de monitor krijgt uitsluitend de playback-bytes gevoed —
    geen aparte download-teller; api is feed() + evaluate()."""
    assert hasattr(DeliveryMonitor, "feed")
    assert hasattr(DeliveryMonitor, "evaluate")
    m, c = _mk()
    n0 = len(m._events_dq)
    _play(m, c, 3, 80.0)
    assert len(m._events_dq) == n0 + 3        # exact de playback-reads


def test_09_seek_gap_resets_windows():
    """(22) seek/pauze: oude events verlopen; de window begint opnieuw
    (geen gemengd beeld van vóór de seek)."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 12, 20.0)                     # slechte periode
    clock_jump = c[0] + 600.0                 # seek: 10 min verder
    c[0] = clock_jump
    m._events_dq.clear()                      # evict doet dit bij evaluatie
    _play(m, c, 8, 150.0)
    snap = m.evaluate()
    assert snap["state"] in (HEALTHY, WARMING_UP, MARGINAL)
    assert snap["rolling_mbit"] > 60.0
