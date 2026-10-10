"""Regressie: JIT-probe file-selectie (incident 2026-10-07, Lanterns S01E08).

Root cause: _probe_by_hash gebruikte torrentio's fileIdx rechtstreeks als
TorBox-file-id. Bij multi-file torrents is TorBox id 0 vaak een NFO-sidecar
(Lanterns S01E08 FLUX/Kitsune: id 0 = 1.467 B NFO, id 2 = 6,55 GB video) —
de probe vroeg een range uit de torrent-totaalgrootte aan de sidecar en kreeg
HTTP 416; 3/3 same-class kandidaten vielen weg → vals jit_no_equivalent_source.

De fix: file-selectie via provider.pick_file (S/E-hint, video-extensie,
minimumgrootte, grootste videofile als fallback), offsets op de GEKOZEN file
geclampt binnen de file-grenzen, expliciete probe-events.
"""
from __future__ import annotations

import inspect
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.common.config import Settings
from plex_scraper.resolver.caches import CacheSet
from plex_scraper.resolver.jit import (  # noqa: E402
    JitConfig, JitController, JitDecision, MARGINAL, _looks_like_media_file)
from plex_scraper.scraper.providers.torbox import TorboxProvider  # noqa: E402
from plex_scraper.scraper.scrapers.base import TorrentCandidate  # noqa: E402

MB = 1048576

# echte file-lijst uit de TorBox checkcached-API (incident 2026-10-07)
FLUX_HASH = "0ebb87c3d45a200598de00da015cd97ee5a68106"
HMAX_HASH = "e4fceaa5d185abf87e788b7f463815ea10cbde1d"
KITSUNE_HASH = "28557727f692acd7092fcf8ee79e23d4eb75bcf7"
UINDEX = "www.UIndex.org    -    "
LANTERNS_FILES = {  # hash → torbox file-id map (id 0 = NFO-sidecar!)
    FLUX_HASH: {0: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p ....nfo", "size": 1467},
                1: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p ....srr", "size": 127},
                2: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p AMZN WEB-DL DV HDR H 265-FLUX.mkv",
                    "size": 6550605678}},
    HMAX_HASH: {0: {"name": UINDEX + "Lanterns S01E08 2160p HMAX WEB-DL ....nfo", "size": 1405},
                1: {"name": UINDEX + "Lanterns S01E08 2160p HMAX WEB-DL ....srr", "size": 127},
                2: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p HMAX WEB-DL DDP5 1 Atmos DV HDR H 265-FLUX.mkv",
                    "size": 1562861562}},
    KITSUNE_HASH: {0: {"name": UINDEX + "Lanterns S01E08 4K....nfo", "size": 1524},
                   1: {"name": UINDEX + "Lanterns S01E08 4K....srr", "size": 127},
                   2: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p AMZN WEB-DL DDP5 1 Atmos DV HDR10Plus H 265-Kitsune.mkv",
                       "size": 6550635667}},
}
TORRENT_SIZES = {FLUX_HASH: 6551826126, HMAX_HASH: 1567663062, KITSUNE_HASH: 6551826126}


def _mk_cand(h: str, name: str, file_idx: int | None = 0) -> TorrentCandidate:
    """Lanterns-candidate met de torrentio-metadata van het incident:
    fileIdx=0 (torrentio-volgorde, daar de video) en cand.size = TORRENT-totaal."""
    return TorrentCandidate(info_hash=h, torrent_name=name, size=TORRENT_SIZES[h],
                            seeders=100, file_index=file_idx,
                            file_name="Lanterns.S01E08.Dirt.and.Stars.2160p.AMZN.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX.mkv")


class StubProvider:
    """Simuleert TorBox-streaming: read voorbij de file-grens → HTTP 416."""

    name = "stub"

    def __init__(self, files_by_hash: dict[str, dict]):
        self.files_by_hash = files_by_hash
        self.reads: list[tuple[str, int]] = []
        self.requested_file: int | None = None

    async def ensure_torrent(self, info_hash, torrent_name):
        files = self.files_by_hash[info_hash]
        return SimpleNamespace(torrent_id=hash(info_hash) % 10**6, files=files,
                               cached=True, ready=True, info_hash=info_hash,
                               name=torrent_name)

    async def get_stream_url(self, torrent_id, file_id):
        self.requested_file = file_id
        return f"stub://{torrent_id}/{file_id}"

    async def read_range(self, url, off, n):
        fid = int(url.rsplit("/", 1)[1])
        fsize = next(f["size"] for files in self.files_by_hash.values()
                     for fid_, f in files.items() if fid_ == fid)
        if off + n > fsize:
            raise RuntimeError(f"upstream HTTP 416 (range {off}-{off + n} beyond EOF)")
        self.reads.append((url, off))
        return b"\x00" * n

    def pick_file(self, torrent, file_name_hint=None):
        videos = [(fid, m) for fid, m in sorted(torrent.files.items())
                  if m["name"].lower().endswith((".mkv", ".mp4", ".ts", ".avi"))
                  and m["size"] >= 20 << 20]
        if not videos:
            return None
        if file_name_hint:
            import re
            se = re.search(r"S\d{1,2}E\d{1,3}", file_name_hint, re.I)
            if se:
                for fid, m in videos:
                    if se.group(0).lower() in m["name"].lower():
                        return fid, m
        return max(videos, key=lambda kv: kv[1]["size"])


class StubResolver:
    def __init__(self, provider, sources):
        self.provider = provider
        self.store = SimpleNamespace(list_sources=self._list_sources,
                                     update_source=self._update_source,
                                     update_runtime=self._update_source)
        self.caches = CacheSet(1800, 600, 900)
        self.events: list[dict] = []
        self._sources = sources
        self.activated = None
        self.candidates: list = []

    async def _list_sources(self, item_id):
        return self._sources

    async def _update_source(self, src):
        return None

    async def _active_source(self, item_id):
        return self._sources[0] if self._sources else None

    async def _evt(self, kind, item=None, **payload):
        self.events.append({"kind": kind, **payload})

    class _Reval:
        async def resolve_rating_key(self, item):
            return 123, "test"

        def queue(self, item, gen, reasons, force=False):
            return SimpleNamespace(state="queued")

        async def wait_for_coherent(self, item_id, timeout):
            return {"waited": True,
                    "state": "plex_metadata_revalidation_succeeded"}

    @property
    def _revalidator(self):
        return self._Reval()

    async def _gather_candidates(self, item):
        return list(self.candidates)

    async def _rank_candidates(self, item, candidates):
        return [(c, 100.0) for c in candidates]

    async def _validate_candidate(self, item, cand):
        return SimpleNamespace(id="newsrc", media_item_id=item.id,
                               info_hash=cand.info_hash, state="active",
                               size=6550605678, codec="hevc",
                               resolution="2160p", hdr="dolby_vision",
                               audio="eac3", file_name="x.mkv",
                               generation=1)

    async def _activate(self, item, src, previous, reason=""):
        self.activated = (item.id, src.info_hash)

    async def close_item_sessions(self, item_id):
        self.closed_sessions = getattr(self, "closed_sessions", 0) + 1
        return 0

    def event_kinds(self):
        return [e["kind"] for e in self.events]


def _mk_item():
    return SimpleNamespace(id="f74db00b93", plex_path="/x/Lanterns S01E08.mkv",
                           kind="episode", title="Dirt and Stars", series="Lanterns",
                           season=1, episode=8, year=None, generation=1)


def _mk_current():
    return SimpleNamespace(
        id="src-cur", info_hash="f56df75335ee7d00c1eea36e9574be5294e37ea3",
        torrent_name="Lanterns.S01E08.2160p.AMZN.WEB-DL.DV.HDR10+.DDP5.1.Atmos..H265.MP4-BTM",
        size=6965991009, delivery_bad_until=0.0,
        codec="hevc", resolution="2160p", hdr="hdr10", audio="eac3",
        file_name="Lanterns.S01E08.mkv", generation=1)


def _mk_jit(provider, resolver):
    return JitController(resolver, JitConfig())


# ---------------------------------------------------------------- direct probe
async def test_multi_file_nfo_at_id0_video_at_id2_probe_targets_video():
    """DE incident-casuïstiek: torrentio fileIdx=0, TorBox id 0 = NFO.
    De probe moet de videofile (id 2) kiezen en binnen diens grenzen lezen."""
    provider = StubProvider(LANTERNS_FILES)
    r = StubResolver(provider, [])
    jit = _mk_jit(provider, r)
    cand = _mk_cand(FLUX_HASH, "Lanterns S01E08 2160p AMZN WEB-DL DDP5 1 Atmos DV HDR H 265-FLUX")
    cand.file_name = "Lanterns.S01E08.Dirt.and.Stars.2160p.AMZN.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX.mkv"

    probe = await jit._probe_by_hash(_mk_item(), cand)

    assert probe is not None and probe["mbit"] > 0
    assert probe["file_id"] == 2                       # de video, niet de NFO
    vsize = LANTERNS_FILES[FLUX_HASH][2]["size"]
    offsets = next(e["offsets"] for e in r.events if e["kind"] == "jit_probe_file_selected")
    assert all(off + MB <= vsize for off in offsets)   # nooit voorbij de video
    assert offsets == [0, vsize // 4, vsize // 2]       # 3 samples over de GEKOZEN file
    # het oude foute offset (torrent-size//2) mag niet meer voorkomen
    assert TORRENT_SIZES[FLUX_HASH] // 2 not in [off for _, off in provider.reads]
    assert not any(e["kind"] == "jit_probe_file_unusable" for e in r.events)


async def test_torrentio_fileidx_never_used_as_provider_id():
    """fileIdx=0 mag nooit de TorBox-file-id worden — ook niet als die bestaat."""
    files = {0: {"name": "sample.nfo", "size": 900},
             5: {"name": "Lanterns.S01E08.Dirt.and.Stars.2160p.WEB-DL.mkv", "size": 3_000_000_000}}
    provider = StubProvider({FLUX_HASH: files})
    r = StubResolver(provider, [])
    jit = _mk_jit(provider, r)
    cand = _mk_cand(FLUX_HASH, "Lanterns S01E08 2160p WEB-DL", file_idx=0)

    probe = await jit._probe_by_hash(_mk_item(), cand)

    assert provider.requested_file == 5                # videofile via pick_file
    assert probe is not None and probe["file_id"] == 5


async def test_sidecar_only_torrent_is_unusable_no_request_possible():
    """Alleen sidecars in de torrent → nooit een media-probe (geen 416-mogelijk)."""
    provider = StubProvider({FLUX_HASH: {0: {"name": "release.nfo", "size": 1467},
                                         1: {"name": "release.srr", "size": 127}}})
    r = StubResolver(provider, [])
    jit = _mk_jit(provider, r)
    cand = _mk_cand(FLUX_HASH, "Lanterns S01E08", file_idx=0)

    probe = await jit._probe_by_hash(_mk_item(), cand)

    assert probe is None
    assert provider.reads == []                        # geen enkele range-request
    assert "jit_probe_file_unusable" in r.event_kinds()


async def test_single_file_torrent_still_probes():
    files = {0: {"name": "Lanterns.S01E08.1080p.WEB.mkv", "size": 800_000_000}}
    provider = StubProvider({FLUX_HASH: files})
    r = StubResolver(provider, [])
    jit = _mk_jit(provider, r)
    cand = _mk_cand(FLUX_HASH, "Lanterns S01E08 1080p WEB", file_idx=None)

    probe = await jit._probe_by_hash(_mk_item(), cand)

    assert probe is not None and probe["file_id"] == 0 and probe["mbit"] > 0
    assert all(off + MB <= 800_000_000 for _, off in provider.reads)


async def test_probe_offsets_clamped_within_chosen_file():
    """HMAX (1,46 GiB-torrent, 3 files): offsets volgen de 1,53 GB-video."""
    provider = StubProvider(LANTERNS_FILES)
    r = StubResolver(provider, [])
    jit = _mk_jit(provider, r)
    cand = _mk_cand(HMAX_HASH, "Lanterns S01E08 2160p HMAX WEB-DL DV HDR")

    probe = await jit._probe_by_hash(_mk_item(), cand)

    assert probe is not None and probe["file_id"] == 2
    vsize = LANTERNS_FILES[HMAX_HASH][2]["size"]
    offsets = next(e["offsets"] for e in r.events if e["kind"] == "jit_probe_file_selected")
    assert offsets[:1] == [0] and offsets[-1] == min(vsize // 2, vsize - MB)
    assert max(off for _, off in provider.reads) + MB <= vsize


async def test_all_probe_samples_failing_reports_reason():
    class DeadProvider(StubProvider):
        async def read_range(self, url, off, n):
            raise RuntimeError("upstream HTTP 503")

    provider = DeadProvider(LANTERNS_FILES)
    r = StubResolver(provider, [])
    jit = _mk_jit(provider, r)
    cand = _mk_cand(FLUX_HASH, "Lanterns S01E08 FLUX")

    probe = await jit._probe_by_hash(_mk_item(), cand)

    assert probe is None
    err = next(e for e in r.events if e["kind"] == "jit_candidate_probe_error")
    assert "offset" in err["error"] and err["file_id"] == 2


async def test_partial_probe_failure_degrades_instead_of_discarding():
    class FlakyProvider(StubProvider):
        async def read_range(self, url, off, n):
            if off != 0:
                raise RuntimeError("upstream HTTP 416 (range beyond EOF)")
            return b"\x00" * n

    provider = FlakyProvider(LANTERNS_FILES)
    r = StubResolver(provider, [])
    jit = _mk_jit(provider, r)
    cand = _mk_cand(FLUX_HASH, "Lanterns S01E08 FLUX")

    probe = await jit._probe_by_hash(_mk_item(), cand)

    assert probe is not None and probe["mbit"] > 0
    assert "jit_probe_sample_degraded" in r.event_kinds()


# ------------------------------------------------- Lanterns-end-to-end (fix)
async def test_lanterns_e08_top3_probeable_no_false_no_equivalent():
    """REPRO: de top-3 van de stall (FLUX/HMAX/Kitsune) moet probebaar zijn;
    jit_no_equivalent_source mag niet meer door fileIdx-mismatch ontstaan."""
    provider = StubProvider(LANTERNS_FILES)
    r = StubResolver(provider, [_mk_current()])
    jit = _mk_jit(provider, r)
    names = {FLUX_HASH: "Lanterns S01E08 Dirt and Stars 2160p AMZN WEB-DL DDP5 1 Atmos DV HDR H 265-FLUX",
             HMAX_HASH: "Lanterns S01E08 Dirt and Stars 2160p HMAX WEB-DL DDP5 1 Atmos DV HDR H 265-FLUX",
             KITSUNE_HASH: "Lanterns S01E08 Dirt and Stars 2160p AMZN WEB-DL DDP5 1 Atmos DV HDR10Plus H 265-Kitsune"}
    r.candidates = [_mk_cand(h, names[h]) for h in (FLUX_HASH, HMAX_HASH, KITSUNE_HASH)]
    # gate-simulatie: availability-batch heeft deze hashes als cached geregistreerd
    r.caches.checkcached.put("batch:test",
                             {c.info_hash: [] for c in r.candidates})

    cur = _mk_current()
    cur.is_delivery_bad = lambda: False
    decision = JitDecision(MARGINAL, 27.5, 0.5, 37.5, severity="MARGINAL")

    switched = await jit._search_and_switch(_mk_item(), cur, 37.5, decision,
                                            background=True)

    kinds = r.event_kinds()
    assert "jit_no_equivalent_source" not in kinds
    assert kinds.count("jit_candidate_probe") == 3
    selected = {e["hash"][:12]: (e["file_id"], e["file_size"])
                for e in r.events if e["kind"] == "jit_probe_file_selected"}
    assert all(fid == 2 for fid, _ in selected.values())     # altijd de video
    assert decision.switched is True and r.activated is not None


# --------------------------------------------------------- resolve-flow intact
def test_torbox_pick_file_real_implementation_unchanged():
    """De resolve/validatie-flow (pick_file) gedraagt zich ongewijzigd:
    S/E-match wint, sidecars worden overgeslagen, grootste video als fallback."""
    s = Settings(db_path="/tmp/unused.db")
    provider = TorboxProvider(s)
    torrent = SimpleNamespace(files={
        0: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p ....nfo", "size": 1467},
        1: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p ....srr", "size": 127},
        2: {"name": UINDEX + "Lanterns S01E08 Dirt and Stars 2160p AMZN WEB-DL DV HDR H 265-FLUX.mkv",
            "size": 6550605678},
    })
    hint = "Lanterns.S01E08.Dirt.and.Stars.2160p.AMZN.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX.mkv"
    fid, meta = provider.pick_file(torrent, hint)
    assert fid == 2 and meta["size"] == 6550605678
    # zonder S/E-hint: grootste videofile
    fid2, _ = provider.pick_file(torrent, None)
    assert fid2 == 2


def test_probe_source_never_uses_scraper_fileidx():
    """Structuur-guard: _probe_by_hash mag de scraper-file_index niet meer
    direct als provider-file-id gebruiken (requirement 4)."""
    src = inspect.getsource(JitController._probe_by_hash)
    assert "file_index" not in src


def test_media_ext_backstop_rejects_sidecars():
    assert _looks_like_media_file("www.UIndex.org    -    Lanterns.S01E08.mkv")
    assert not _looks_like_media_file("www.UIndex.org    -    release.nfo")
    assert not _looks_like_media_file("release.srr")
    assert not _looks_like_media_file("")
