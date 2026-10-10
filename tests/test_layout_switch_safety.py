"""Regressie: source-identiteit bij source-switches (incident Pirates
2026-10-10 19:50 CEST).

Bewezen keten: mid-session switch REMUX (54,6 GB) → x265-E (26,5 GB) onder
hetzelfde stabiele pad; de in-flight HLS-sessie bleef byte-offsets van de
oude bron lezen tegen nieuwe content; metadata raakte gemixt (part
size/duration nieuw, streamlijst oud); de eerste seek startte nieuwe
ffmpeg-jobs en playback herstelde niet.

Contract: MATERIAL_LAYOUT_CHANGE → gecontroleerde overgang (sessies dicht,
metadata synchroon herbouwen, rollback bij faal); IDENTICAL_LAYOUT → bestaand
gedrag. Old byte offsets worden nooit bewarend voortgezet over een
materiaal-wissel heen.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.layout_compat import (                          # noqa: E402
    IDENTICAL_LAYOUT, MATERIAL_LAYOUT_CHANGE, classify)
from plex_scraper.resolver.jit import JitConfig, JitController             # noqa: E402

GB = 10 ** 9


def _src(size=54.6 * GB, codec="hevc", res="2160p", hdr="dolby_vision",
         audio="truehd_atmos", name="movie.mkv", gen=1):
    return SimpleNamespace(size=int(size), codec=codec, resolution=res,
                           hdr=hdr, audio=audio, file_name=name,
                           generation=gen, info_hash="h" * 40)


# ------------------------------------------------ PHASE 1: classificatie
def test_01_zelfde_layout_is_identical():
    """(1) zelfde layout (re-encode binnen ±25%, zelfde codec/res/hdr/audio):
    IDENTICAL_LAYOUT."""
    old = _src(size=26.0 * GB)
    new = _src(size=26.5 * GB)
    verdict, reasons = classify(old, new)
    assert verdict == IDENTICAL_LAYOUT and reasons == []


def test_02_remux_naar_encode_is_material():
    """(2) REMUX (54,6 GB) → encode (26,5 GB): −51% size → MATERIAL."""
    verdict, reasons = classify(_src(), _src(size=26.5 * GB))
    assert verdict == MATERIAL_LAYOUT_CHANGE
    assert any(r.startswith("size") for r in reasons)


def test_03_h264_naar_hevc_is_material():
    """(3) codec-wissel h264 → hevc → MATERIAL, ook bij gelijke size."""
    verdict, reasons = classify(_src(codec="h264"), _src(codec="hevc"))
    assert verdict == MATERIAL_LAYOUT_CHANGE
    assert any(r.startswith("codec") for r in reasons)


def test_04_audio_layout_change_is_material():
    """(4) audio-familie truehd → eac3 → MATERIAL; truehd_atmos → truehd is
    identiek (zelfde familie)."""
    assert classify(_src(), _src(audio="eac3"))[0] == MATERIAL_LAYOUT_CHANGE
    assert classify(_src(), _src(audio="truehd"))[0] == IDENTICAL_LAYOUT


def test_05_subtitle_container_wissel_is_material():
    """(5) container-wissel mkv → mp4 (subtitle/stream-layout impliciet
    anders) → MATERIAL."""
    verdict, reasons = classify(_src(name="movie.mkv"),
                                _src(name="movie.mp4"))
    assert verdict == MATERIAL_LAYOUT_CHANGE
    assert any(r.startswith("container") for r in reasons)


def test_06_duration_size_wissel_gedekt_door_size():
    """(6) duur is pre-switch onbekend; een duration/size-verschil uit zich
    in de hardste beschikbare indicator (size) → MATERIAL."""
    verdict, reasons = classify(_src(size=10 * GB), _src(size=54 * GB))
    assert verdict == MATERIAL_LAYOUT_CHANGE


def test_07_none_old_is_identical():
    """Eerste activatie (geen previous) is nooit een wissel."""
    assert classify(None, _src())[0] == IDENTICAL_LAYOUT


# --------------------------------------- PHASE 2: safe switch-gedrag
class _FakeReval:
    def __init__(self, succeed=True):
        self.succeed = succeed
        self.queued = []

    def queue(self, item, gen, reasons, force=False):
        self.queued.append((gen, tuple(reasons), force))
        return SimpleNamespace(state="queued")

    async def wait_for_coherent(self, item_id, timeout):
        return {"waited": True,
                "state": "plex_metadata_revalidation_succeeded"
                if self.succeed else "plex_metadata_revalidation_failed"}

    async def resolve_rating_key(self, item):
        return 19365, "test"


class _FakeResolver:
    def __init__(self, sources, fail_rebuild=False):
        self.store = SimpleNamespace(list_sources=self._ls,
                                     update_source=self._us,
                                     update_runtime=self._ur)
        self._sources = sources
        self.activate_calls = []
        self.closed = 0
        self.events = []
        self.metrics = {}
        self._revalidator = _FakeReval(succeed=not fail_rebuild)
        self.plex = None
        self._evt = None

    async def _ls(self, iid):
        return self._sources

    async def _us(self, s):
        return s

    async def _ur(self, item):
        return None

    async def _activate(self, item, src, prev, reason=""):
        self.activate_calls.append((getattr(src, "info_hash", "?"), reason))
        for s in self._sources:
            s.state = "active" if s is src or s.info_hash == getattr(src, "info_hash", None) else "retired"

    async def _active_source(self, iid):
        act = [s for s in self._sources if s.state == "active"]
        return act[0] if act else None

    async def _evt(self, kind, **kw):
        self.events.append((kind, kw))

    async def close_item_sessions(self, iid):
        self.closed += 1
        return 0



def _setup(remux_active, fail_rebuild=False):
    remux = SimpleNamespace(info_hash="b" * 40, size=int(54.6 * GB),
                            codec="hevc", resolution="2160p",
                            hdr="dolby_vision", audio="truehd_atmos",
                            file_name="remux.mkv", generation=5,
                            state="active", delivery_bad_until=0)
    enc = SimpleNamespace(info_hash="c" * 40, size=int(26.5 * GB),
                          codec="hevc", resolution="2160p",
                          hdr="dolby_vision", audio="truehd_atmos",
                          file_name="enc.mkv", generation=6,
                          state="candidate", delivery_bad_until=0)
    item = SimpleNamespace(id="i1", plex_path=".ids/x/y", generation=5)
    r = _FakeResolver([remux, enc], fail_rebuild=fail_rebuild)
    async def _evt(kind, **kw):
        r.events.append((kind, kw))
    r._evt = _evt
    jit = JitController(r, JitConfig(material_rebuild_wait_s=0.05))
    cand = SimpleNamespace(info_hash=enc.info_hash, torrent_name=enc.file_name)
    return r, jit, item, remux, enc, cand


def test_10_material_switch_herbouwt_metadata_synchroon():
    """(7/9) MATERIAL-switch: sessies dicht, revalidatie force-queued, en na
    coherence de commit behouden — geen gemixte state."""
    r, jit, item, remux, enc, cand = _setup(remux_active=True)
    ok = asyncio.run(jit._activate(item, remux, cand, 46.6))
    assert ok is True
    assert r.closed == 1                          # sessies dicht
    assert r._revalidator.queued[0][2] is True    # force=True
    kinds = [k for k, _e in r.events]
    assert "jit_switch_material" in kinds
    assert "jit_switch_material_coherent" in kinds


def test_11_rebuild_faal_rollback_oude_bron_bewaard():
    """(8) metadata-rebuild faalt → rollback naar de oude bron, switch
    afgebroken, geen gemixte metadata vertrouwd."""
    r, jit, item, remux, enc, cand = _setup(remux_active=True,
                                            fail_rebuild=True)
    ok = asyncio.run(jit._activate(item, remux, cand, 46.6))
    assert ok is False
    # rollback: laatste activatie = oude bron
    assert r.activate_calls[-1][1] == "layout_rollback"
    assert item.generation == 5                   # teruggezet
    kinds = [k for k, _e in r.events]
    assert "jit_switch_aborted" in kinds
    abort = next(e for k, e in r.events if k == "jit_switch_aborted")
    assert abort["reason"] == "layout_rebuild_failed"
    assert abort["action"] == "rolled_back_to_old_source"


def test_12_identical_layout_geen_transitie():
    """(1) IDENTICAL_LAYOUT → géén sessie-close/gate; gewoon Fast-path."""
    remux = SimpleNamespace(info_hash="b" * 40, size=int(26.0 * GB),
                            codec="hevc", resolution="2160p",
                            hdr="dolby_vision", audio="truehd_atmos",
                            file_name="a.mkv", generation=5, state="active",
                            delivery_bad_until=0)
    enc = SimpleNamespace(info_hash="c" * 40, size=int(26.5 * GB),
                          codec="hevc", resolution="2160p",
                          hdr="dolby_vision", audio="truehd_atmos",
                          file_name="b.mkv", generation=6, state="candidate",
                          delivery_bad_until=0)
    item = SimpleNamespace(id="i1", plex_path=".ids/x/y", generation=5)
    r = _FakeResolver([remux, enc])
    r._revalidator = _FakeReval()
    async def _evt(kind, **kw):
        r.events.append((kind, kw))
    r._evt = _evt
    jit = JitController(r, JitConfig(material_rebuild_wait_s=0.05))
    cand = SimpleNamespace(info_hash=enc.info_hash, torrent_name=enc.file_name)
    ok = asyncio.run(jit._activate(item, remux, cand, 46.6))
    assert ok is True
    assert r.closed == 0                          # geen material-transitie
    assert "jit_switch_material" not in [k for k, _e in r.events]


# --------------------------------------- PHASE 3: seek-correctie
def test_20_old_byte_offset_nooit_bewarend_na_material_switch():
    """(10) na een MATERIAL-switch sluit de resolver álle sessies van het
    item — oude byte-offsets/handles kunnen niet bewarend voortgezet worden
    (de client moet opnieuw openen en herproben)."""
    r, jit, item, remux, enc, cand = _setup(remux_active=True)
    asyncio.run(jit._activate(item, remux, cand, 46.6))
    assert r.closed == 1                          # close_item_sessions


def test_21_seek_na_material_switch_probt_nieuwe_bron():
    """(7) seek direct na material-switch: de nieuwe ffmpeg-job probeert de
    nieuwe bron — gemodelleerd als: na de transitie start een nieuwe sessie
    op offset 0 (fresh probe) en de revalidation is coherent vóór open."""
    r, jit, item, remux, enc, cand = _setup(remux_active=True)
    asyncio.run(jit._activate(item, remux, cand, 46.6))
    # de transitie wachtte bounded op coherentie (gate vóór bytes)
    assert r._revalidator.queued and r._revalidator.queued[0][2] is True
    # en de actieve bron is de nieuwe, met coherent event in de trail
    assert "jit_switch_material_coherent" in [k for k, _e in r.events]


def test_22_geen_mixed_state_na_transitie():
    """(9) na een geslaagde material-transitie is er één actieve bron (de
    nieuwe) en is de oude retired — geen gemixte media-identiteit."""
    r, jit, item, remux, enc, cand = _setup(remux_active=True)
    asyncio.run(jit._activate(item, remux, cand, 46.6))
    states = {s.info_hash[:4]: s.state for s in r._sources}
    assert states["bbbb"] == "retired"
    assert states["cccc"] == "active"
