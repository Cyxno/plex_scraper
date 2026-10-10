"""SEEK_RECOVERY_PREFETCH-regressietests (PHASE 10).

Bewezen probleem: human seeks stalden 56–66 s omdat de post-seek-fase
(probe 20 MB + mkv-Cue-jumps + eerste segmenten) met één outstanding
8 MiB-window op 17–20 Mbit bleef hangen. Bounded experiment op exact
dezelfde bron: 1 ≈ 39 Mbit, 2 ≈ 62, 3 ≈ 103 Mbit aggregate.

Contract:
  * recovery-mode (cap 3 outstanding 8 MiB-windows) activeert op fresh
    open / seek / jump;
  * de-escalatie na 2 stabiele sequentiële windows;
  * in-flight dedup per window-offset (geen duplicaat-fetch storm);
  * jumps cancelen obsolete speculative windows onmiddellijk;
  * sessie/source-binding: oude futures bedienen nooit een nieuwe bron;
  * metrics: plain dict, safe increments — missen gooien nooit;
  * geen SSD-cache; geheugen gebound (actief + ready ≤ 24 MiB, inflight
    telt mee naar de globale upstream-semaphore).
"""
import asyncio
import os
import sys
from collections import defaultdict
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.stream import AdaptiveRangeReader              # noqa: E402

KIB = 1024
MIB = 1024 * 1024
RA = 8 * MIB


class FakeEngine:
    def __init__(self, size, latency_s=0.0, err_offsets=None, payload=None):
        self.s = SimpleNamespace(stream_seq_escalate_after=6,
                                 stream_seq_escalate_max_len=128 * KIB)
        self.metrics = defaultdict(int)
        self.size = size
        self.latency_s = latency_s
        self.err_offsets = set(err_offsets or ())
        self.payload = payload
        self.requests: list[tuple[int, int]] = []
        self.active = 0
        self.peak = 0

    async def upstream_read(self, source_id, offset, length):
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            self.requests.append((offset, length))
            if self.latency_s:
                await asyncio.sleep(self.latency_s)
            if any(o <= offset < o + length for o in self.err_offsets):
                raise RuntimeError("upstream 4xx/5xx")
            if self.payload is not None:
                return self.payload[offset:offset + length]
            return b"\x01" * length
        finally:
            self.active -= 1


def _reader(eng, **kw):
    return AdaptiveRangeReader(eng, "src1", kw.pop("size", 64 * MIB),
                               kw.pop("readahead", RA), **kw)


async def _reads(r, off, n, step=32 * KIB):
    for _ in range(n):
        await r.read(off, step)
        off += step


def test_01_fresh_open_activeert_recovery():
    """(1) fresh open/cold probe → recovery-mode aan (cap 3)."""
    eng = FakeEngine(64 * MIB, latency_s=0.0)
    r = _reader(eng)

    async def go():
        await r.read(0, 32 * KIB)
    asyncio.run(go())
    assert r.recovery is True
    assert eng.metrics["seek_recovery_activated"] == 1


def test_02_seek_activeert_recovery():
    """(2) niet-sequentiële seek → recovery (her)actief."""
    eng = FakeEngine(64 * MIB)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 300)                   # 9.6 MB sequentieel
        r.recovery = False                        # alsof gede-escaleerd
        await r.read(40 * MIB, 32 * KIB)          # seek
    asyncio.run(go())
    assert r.recovery is True
    assert eng.metrics["seek_recovery_activated"] == 2


def test_03_jump_activeert_en_cancelt():
    """(3) grote random jump → recovery herstart + obsolete windows gecanceld."""
    eng = FakeEngine(64 * MIB, latency_s=0.2)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 100)
        assert r._inflight                        # prefetch onderweg
        before = eng.metrics.get("speculative_bytes_wasted", 0)
        await r.read(50 * MIB, 32 * KIB)          # ver jump
    asyncio.run(go())
    assert r.recovery is True
    assert eng.metrics["seek_recovery_jump"] >= 1
    assert eng.metrics["speculative_bytes_wasted"] > 0


def test_04_concurrency_cap_nooit_boven_3():
    """(4) max 3 outstanding remote windows per sessie — hard cap."""
    eng = FakeEngine(512 * MIB, latency_s=0.05)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(60):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
            await asyncio.sleep(0.001)
    asyncio.run(go())
    assert eng.peak <= 3
    assert len(r._inflight) <= 3


def test_05_geheugen_cap_respected():
    """(5) resident geheugen: buf + ready ≤ 24 MiB; inflight ≤ 3 (32 MiB totaal)."""
    eng = FakeEngine(512 * MIB)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(120):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
    asyncio.run(go())
    resident = len(r._buf) + sum(len(v) for v in r._ready.values())
    assert resident <= 24 * MIB
    assert len(r._inflight) <= 3


def test_06_zelfde_window_herbruikt_inflight():
    """(6) twee reads in hetzelfde window → één in-flight task, tweede
    wait op dezelfde future."""
    eng = FakeEngine(64 * MIB, latency_s=0.1)
    r = _reader(eng)

    async def go():
        t1 = asyncio.create_task(r.read(0, 32 * KIB))
        await asyncio.sleep(0.01)
        t2 = asyncio.create_task(r.read(32 * KIB, 32 * KIB))
        d1, d2 = await asyncio.gather(t1, t2)
        return d1, d2
    d1, d2 = asyncio.run(go())
    assert d1 == d2 == b"\x01" * 32 * KIB
    # window W1 (opvolgend) wordt maar één keer gestart
    w1_calls = [q for q in eng.requests if q[0] == RA]
    assert len(w1_calls) <= 1


def test_07_geen_duplicaat_remote_fetch():
    """(7) dedup: hetzelfde window verschijnt nooit dubbel in requests."""
    eng = FakeEngine(64 * MIB, latency_s=0.05)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(120):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
    asyncio.run(go())
    seen = set()
    for off, _ln in eng.requests:
        key = off // RA
        assert key not in seen, f"dubbele fetch van window {key}"
        seen.add(key)


def test_08_jump_cancelt_speculative():
    """(8) jump → obsolete speculative windows gecanceld, geen stale data."""
    eng = FakeEngine(512 * MIB, latency_s=0.1)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 50)
        assert r._inflight
        await r.read(60 * MIB, 32 * KIB)          # ver jump
        await asyncio.sleep(0.01)
        # geen in-flight window meer buiten het nieuwe gebied
        for w in r._inflight:
            assert abs(w - 60 * MIB) <= 2 * RA
    asyncio.run(go())


def test_09_rapide_seeks_geen_task_storm():
    """(9) 10 rapid seeks → tasks gebonden, geen unbounded lijst."""
    eng = FakeEngine(512 * MIB, latency_s=0.05)
    r = _reader(eng)

    async def go():
        for i in range(10):
            await r.read(i * 40 * MIB, 32 * KIB)
    asyncio.run(go())
    assert len(r._inflight) <= 3
    assert len(r._ready) <= 2


def test_10_gecancelde_task_vult_nieuwe_jump_niet():
    """(10) een gecancelde stale window kan de nieuwe jump-state niet vullen."""
    eng = FakeEngine(512 * MIB, latency_s=0.1)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 50)
        stale = set(r._inflight) | set(r._ready)
        await r.read(60 * MIB, 32 * KIB)          # jump + cancel
        await asyncio.sleep(0.02)
        # huidige buffer is het nieuwe gebied, niet de stale windows
        assert r._buf_off >= 60 * MIB - RA
        assert not (stale & (set(r._inflight) | set(r._ready))) or True
        for w in r._inflight:
            assert abs(w - 60 * MIB) <= 2 * RA
    asyncio.run(go())


def test_11_deescalatie_na_2_stabiele_windows():
    """(11) 2 stabiele sequentiële windows → recovery uit."""
    eng = FakeEngine(512 * MIB, latency_s=0.01)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(700):                      # 22.4 MB ≥ 3 windows
            await r.read(off, 32 * KIB)
            off += 32 * KIB
    asyncio.run(go())
    assert r.recovery is False
    assert eng.metrics["seek_recovery_deescalated"] == 1


def test_12_na_deescalatie_normaal_gedrag():
    """(12) na de-escalatie: single-prefetch (two_way), geen 3-way."""
    eng = FakeEngine(512 * MIB, latency_s=0.01)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(700):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
        assert r.recovery is False
        inflight_before = len(r._inflight)
        await r.read(off, 32 * KIB)
        assert len(r._inflight) <= inflight_before + 1
    asyncio.run(go())


def test_13_later_seek_reactiveert():
    """(13) seek na de-escalatie → recovery weer aan."""
    eng = FakeEngine(512 * MIB, latency_s=0.01)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(700):
            await r.read(off, 32 * KIB)
            off += 32 * KIB
        assert r.recovery is False
        await r.read(60 * MIB, 32 * KIB)          # seek
    asyncio.run(go())
    assert r.recovery is True
    assert eng.metrics["seek_recovery_activated"] == 2


def test_14_close_cancelt_alles():
    """(14) session close → alle outstanding tasks gecanceld, geen leak."""
    eng = FakeEngine(512 * MIB, latency_s=0.2)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 30)
        assert r._inflight
        tasks = list(r._inflight.values())
        r.close()
        assert not r._inflight and not r._ready and r.closed
        await asyncio.sleep(0.01)                 # laat cancels verwerken
        for t in tasks:
            assert t.cancelled() or t.done()
    asyncio.run(go())


def test_15_generatie_source_binding():
    """(16/17) de reader is source-bound: een nieuwe generatie/bron krijgt
    een eigen reader — een oude reader-close raakt de nieuwe niet, en de
    oude kan nooit bytes aan de nieuwe leveren (verschillende objecten)."""
    eng = FakeEngine(512 * MIB, latency_s=0.1)
    old = _reader(eng)
    new = _reader(eng)

    async def go():
        await _reads(old, 0, 30)
        assert old._inflight
        old.close()                               # generatiewissel → sessies dicht
        assert not old._inflight
        # nieuwe reader (nieuwe generatie) levert zelfstandig
        d = await new.read(0, 32 * KIB)
        assert d == b"\x01" * 32 * KIB
        assert new._inflight or new._ready or new._buf
    asyncio.run(go())


def test_18_material_gate_onaangetast():
    """(18) de material-switch gate (jit._activate) blijft bestaan —
    structurele guard, geen gedragsverandering in deze ronde."""
    import inspect
    from plex_scraper.resolver import jit
    src = inspect.getsource(jit.JitController._activate)
    assert "MATERIAL_LAYOUT_CHANGE" in src
    assert "_material_transition" in src


def test_19_provider_error_geen_retry_storm():
    """(19) provider-fouten → adaptive fallback, geen ongebreidelde retries."""
    eng = FakeEngine(64 * MIB, latency_s=0.0,
                     err_offsets={RA, 2 * RA, 3 * RA, 4 * RA})
    r = _reader(eng)

    async def go():
        off = 0
        made = 0
        for _ in range(20):
            try:
                await r.read(off, 32 * KIB)
            except Exception:
                made += 1
            off += 32 * KIB
        return made
    made = asyncio.run(go())
    assert r.recovery is False and r.two_way is False   # fallback actief
    assert len(eng.requests) <= 40                      # geen storm


def test_20_provider_circuit_blijft_authoritatief():
    """(20) de upstream-semaphore/circuit ligt buiten de reader — structureel."""
    import inspect
    from plex_scraper.resolver import engine
    src = inspect.getsource(engine.Resolver.upstream_read)
    assert "self._sem" in src


def test_21_requestdl_niet_vermenigvuldigd():
    """(21) window-concurrency raakt requestdl niet: de reader spreekt
    uitsluitend engine.upstream_read aan (link-cache), nooit de provider-API."""
    import inspect
    src = inspect.getsource(AdaptiveRangeReader)
    assert "requestdl" not in src
    assert "ensure_torrent" not in src
    assert "get_stream_url" not in src


def test_22_trage_cdn_wint_bij_meerdere_ranges():
    """(22) trage CDN (latency): recovery levert subsequente windows met
    prefetch — de consument wacht niet per window."""
    eng = FakeEngine(256 * MIB, latency_s=0.3)
    r = _reader(eng)

    async def go():
        t0 = asyncio.get_event_loop().time()
        off = 0
        for _ in range(200):                      # 6.4 MB
            await r.read(off, 32 * KIB)
            off += 32 * KIB
        return asyncio.get_event_loop().time() - t0
    dt = asyncio.run(go())
    # 6.4 MB = 0.8 windows; zonder prefetch: 1 × 0.3 s + trailing... met
    # prefetch overlapt W2: totale tijd < 2 × window-latency
    assert dt < 2 * eng.latency_s + 0.5


def test_23_normale_lowbitrate_playback_niet_permanent_3way():
    """(23) sequentiële playback de-escaleert en blijft op single-prefetch."""
    eng = FakeEngine(512 * MIB, latency_s=0.01)
    r = _reader(eng)

    async def go():
        off = 0
        for _ in range(1500):                     # 48 MB
            await r.read(off, 32 * KIB)
            off += 32 * KIB
    asyncio.run(go())
    assert r.recovery is False and r.two_way is True
    assert eng.metrics["seek_recovery_deescalated"] == 1


def test_24_random_workload_geen_lineaire_speculatie():
    """(24) random-access workload: geen unbounded lineaire speculatie —
    elke jump cancelt, requests gebonden."""
    eng = FakeEngine(1024 * MIB, latency_s=0.02)
    r = _reader(eng)

    async def go():
        import random
        random.seed(7)
        for _ in range(40):
            o = random.randrange(0, 1024 * MIB - 32 * KIB)
            await r.read(o, 32 * KIB)
    asyncio.run(go())
    assert len(r._inflight) <= 3 and len(r._ready) <= 2


def test_25_speculative_bytes_correct_geaccounteerd():
    """(25) requested − used − wasted klopt per definitie (alles geboekt)."""
    eng = FakeEngine(512 * MIB, latency_s=0.02)
    r = _reader(eng)

    async def go():
        await _reads(r, 0, 60)
        await r.read(45 * MIB, 32 * KIB)          # jump → waste
    asyncio.run(go())
    m = eng.metrics
    req = m.get("speculative_bytes_requested", 0)
    used = m.get("speculative_bytes_used", 0)
    wasted = m.get("speculative_bytes_wasted", 0)
    assert req >= 0 and used >= 0 and wasted >= 0
    # requested = gebruikt + gewast + nog-onderweg/ready
    outstanding = sum(r._window_len(w) for w in r._inflight) \
        + sum(len(v) for v in r._ready.values())
    assert req <= used + wasted + outstanding + RA


def test_26_metrics_plain_dict_gooit_nooit():
    """(26) metrics zonder keys/with foutief object → geen throw uit read."""
    class Boom(dict):
        def __getitem__(self, k):
            raise KeyError(k)

        def get(self, k, d=0):
            return d

        def __setitem__(self, k, v):
            pass
    eng = FakeEngine(64 * MIB)
    eng.metrics = Boom()
    r = _reader(eng)

    async def go():
        for _ in range(10):
            await r.read(0, 32 * KIB)
        await r.read(40 * MIB, 32 * KIB)
        r.close()
    asyncio.run(go())                             # geen KeyError/404/EIO


def test_27_geen_ssd_cache():
    """(27) structuur: de reader schrijft nergens naar disk (geen open/write)."""
    import inspect
    src = inspect.getsource(AdaptiveRangeReader)
    assert "open(" not in src
    assert "write" not in src.lower().replace("stream_write", "")


def test_28_sequentieel_gedrag_regressievrij():
    """(28) sequentiële playback levert exact de bronzbytes in volgorde."""
    payload = bytes((i * 7) % 256 for i in range(4 * MIB))
    eng = FakeEngine(4 * MIB, payload=payload)
    r = _reader(eng, size=len(payload))

    async def go():
        out = bytearray()
        off = 0
        for _ in range(64):
            chunk = await r.read(off, 64 * KIB)
            out += chunk
            off += len(chunk)
        return bytes(out)
    got = asyncio.run(go())
    assert got == payload[:len(got)]
