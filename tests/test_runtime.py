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


def test_periodic_stalls_degraded_despite_high_throughput():
    """(F16) Toy Story-patroon: 150 Mbit avg + elke 30s een 5s-stall →
    STALL_DEGRADED terwijl throughput HEALTHY is."""
    m, c = _mk(media=60.0, target=90.0)
    t = 0
    for cycle in range(4):                       # 4 × 30 s
        for _ in range(25):                      # 25 s snelle reads
            t += 1; c[0] = t
            m.feed(int(150e6/8), 0.2)
        t += 5; c[0] = t                         # 5s stall
        m.feed(int(5e6/8), 5.0)
    s = m.evaluate()
    assert s["stall_state"] == "STALL_DEGRADED"
    assert s["state"] == "STALL_DEGRADED"        # combined = worst
    assert s["throughput_state"] in (HEALTHY, WARMING_UP)
    assert s["severe_stalls"] >= 2


def test_single_4s_stall_no_degrade():
    """(F17) één 4s-stall → hooguit STALL_WARNING, geen DEGRADED."""
    m, c = _mk(media=60.0, target=90.0)
    _play(m, c, 5, 150.0)
    t = c[0] + 1; c[0] = t
    m.feed(int(10e6/8), 4.0)
    _play(m, c, 10, 150.0)
    s = m.evaluate()
    assert s["stall_state"] in ("OK", "STALL_WARNING")
    assert s["stall_state"] != "STALL_DEGRADED"


def test_seek_and_pause_excluded_from_stalls():
    """(F8/20-21) seek/pause veroorzaken geen stall-score."""
    m, c = _mk(media=60.0, target=90.0)
    for _ in range(3):
        t = c[0] + 1; c[0] = t
        m.feed(int(150e6/8), 6.0, seek=True)     # 6s 'stall' maar seek-context
        t = c[0] + 1; c[0] = t
        m.feed(int(150e6/8), 6.0, seek=True)
    s = m.evaluate()
    assert s["stalls"] == 0 and s["stall_state"] == "OK"


def test_stall_window_expiry_recovery():
    """(F22) stall-history verloopt na het window → herstel naar OK."""
    m, c = _mk(media=60.0, target=90.0)
    t = 0
    for _ in range(2):
        t += 1; c[0] = t
        m.feed(int(5e6/8), 6.0)                  # 2 severe stalls
    assert m.evaluate()["stall_state"] == "STALL_DEGRADED"
    t += 200; c[0] = t                          # 200 s later: window verlopen
    for _ in range(5):
        t += 1; c[0] = t
        m.feed(int(150e6/8), 0.2)
    assert m.evaluate()["stall_state"] == "OK"


def test_combined_worst_state():
    """(F11) final state = worst van throughput en stall."""
    m, c = _mk(media=60.0, target=90.0)
    t = 0
    for _ in range(2):
        t += 1; c[0] = t
        m.feed(int(5e6/8), 6.0)                  # stalls
    t += 1; c[0] = t
    m.feed(int(20e6/8), 0.3)                     # throughput laag
    s = m.evaluate()
    assert s["state"] in ("STALL_DEGRADED", "DEGRADED")


def test_patch_persists_despite_stale_runtime_writer(tmp_path):
    """FASE 1-repro: stale runtime-writer mag external IDs/identity nooit
    overschrijven (generieke persistence-semantiek)."""
    from plex_scraper.common.domain.models import MediaItem
    from plex_scraper.resolver.store import Store
    store = Store(str(tmp_path / "s.db"))

    async def seq():
        it = MediaItem(id="i1", kind="movie", title="MobLand", plex_path="p.mkv",
                       series="MobLand", season=2, episode=2)
        await store.create_item(it)
        # stale copy A (vóór enrich)
        stale = await store.get_item("i1")
        # "PATCH": enrichment schrijft external IDs (full-row, verse copy)
        fresh = await store.get_item("i1")
        fresh.imdb_id = "tt43338257"
        await store.update_item(fresh)
        # stale runtime-writer (resolve/open-pad) schrijft daarna
        stale.status = "RESOLVING"
        await store.update_runtime(stale)
        back = await store.get_item("i1")
        return back
    it = asyncio.run(seq())
    assert it.imdb_id == "tt43338257"            # niet overschreven
    assert it.series == "MobLand"
    assert it.status == "RESOLVING"              # runtime-veld wél bijgewerkt


def test_patch_roundtrip_all_external_ids(tmp_path):
    from plex_scraper.common.domain.models import MediaItem
    from plex_scraper.resolver.store import Store
    store = Store(str(tmp_path / "s.db"))

    async def seq():
        it = MediaItem(id="i1", kind="movie", title="X", plex_path="x.mkv")
        await store.create_item(it)
        fresh = await store.get_item("i1")
        fresh.imdb_id, fresh.tmdb_id, fresh.tvdb_id = "tt1", "7492638", "11542639"
        await store.update_item(fresh)
        # tweede store-instantie = onafhankelijke read (geen in-memory echo)
        store2 = Store(str(tmp_path / "s.db"))
        return await store2.get_item("i1")
    it = asyncio.run(seq())
    assert (it.imdb_id, it.tmdb_id, it.tvdb_id) == ("tt1", "7492638", "11542639")
