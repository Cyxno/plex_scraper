"""Regressie: gemigreerde items + single-active-invariant.

Bewezen incident (Pirates 2011, 2026-10-10 13:05–13:09 CEST): de JIT vond
een goede kandidaat (FraMeSToR REMUX, median 45,2 mbit) maar aborteerde de
switch op rating_key_unresolved — gemigreerd READY-item zonder
part_path/-cache, en de fallback probeerde de .ids-uuid te matchen die nooit
het Plex-part-filename is. Daarnaast bleven na een crash/race twee bronnen
op state='active' staan.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.plex_revalidation import PlexRevalidator          # noqa: E402
from plex_scraper.resolver.store import Store                                # noqa: E402
from plex_scraper.common.domain import models as m                           # noqa: E402

PLEX_PATH = ".ids/4/7/5/6/1/47561234-abcd-abcd-abcd-47561234abcd"   # uuid: nooit een Plex-filename
FRAleanor = "Pirates.of.the.Caribbean.On.Stranger.Tides.2011.UHD.BluRay.2160p.TrueHD.Atmos.7.1.DV.HEVC.HYBRID.REMUX-FraMeSToR.mkv"
WEAK = "Pirates of the Caribbean- On Stranger Tides 2011 UHD BluRay 2160p DV HEVC TrueHD Atmos 7.1 x265-E.mkv"


class _ReleaseStore:
    def __init__(self, sources):
        self._sources = sources

    async def list_sources(self, item_id):
        return self._sources


def _mk_reval(plex, sources, **cfg):
    r = PlexRevalidator(SimpleNamespace(store=_ReleaseStore(sources)), plex,
                        SimpleNamespace(
                            plex_revalidation_enabled=True,
                            plex_revalidation_lookup_retries=cfg.pop(
                                "lookup_retries", 2),
                            plex_revalidation_lookup_wait_s=cfg.pop(
                                "lookup_wait", 0.0)))
    return r


def _src(name, gen=3, active=True, sid=None):
    return m.Source(id=sid or ("src-" + name[:8]), media_item_id="i1",
                    generation=gen, provider="torbox", info_hash="h" * 40,
                    torrent_name=name, size=26_546_639_478, cached=True,
                    score=90.0, file_name=name,
                    state="active" if active else "candidate")


def _item():
    return SimpleNamespace(id="i1", plex_path=PLEX_PATH, title="On Stranger Tides",
                           season=None, episode=None, generation=3,
                           duration_s=None)


class ReleaseDBPlex:
    """Read-only DB-simulatie: matcht alléén échte release-basenames."""
    def __init__(self, parts: dict[str, int], target_map=None):
        self.parts = parts
        self.target_map = target_map

    async def find_rating_key_via_db(self, part_file):
        base = os.path.basename(part_file)
        if base.startswith(".ids"):
            return None                       # uuid is nooit een Plex-filename
        return self.parts.get(base)

    async def find_rating_key_by_path(self, plex_path, exact_path=None):
        return None                            # HTTP-fallback faalt hier

    async def find_rating_key_by_target(self, plex_path):
        return self.target_map.get(plex_path) if self.target_map else None

    async def analyze_item(self, rk):
        return {"analyzed": rk, "status": 200}

    async def get_media_info(self, rk):
        return {"ok": True, "container": "mkv", "video_codec": "hevc",
                "height": 2160, "width": 3840, "duration_s": None,
                "size": None, "audio_codec": None, "streams": []}


def test_01_migrated_item_release_filename_via_db():
    """(1) gemigreerd item, geen part_path, geen cache: actieve bron-release-
    filename → Plex DB → correct ratingKey."""
    plex = ReleaseDBPlex({WEAK: 19365})
    r = _mk_reval(plex, [_src(WEAK, gen=3, active=True)])
    rk, how = asyncio.run(r.resolve_rating_key(_item()))
    assert rk == 19365 and how == "db_release_name"


def test_02_candidate_filename_wijkt_af_zelfde_logisch_item():
    """(2) kandidaat-filename wijkt af van de actieve: beide kunnen het
   zelfde logische Plex-item resolen (actief eerst, daarna kandidaat)."""
    plex = ReleaseDBPlex({FRAleanor: 19365, WEAK: 19365})
    r = _mk_reval(plex, [_src(WEAK, gen=3, active=True),
                         _src(FRAleanor, gen=5, active=False)])
    rk, how = asyncio.run(r.resolve_rating_key(_item()))
    assert rk == 19365 and how == "db_release_name"
    # en omgekeerd: alleen de kandidaat-filename bestaat in Plex
    plex2 = ReleaseDBPlex({FRAleanor: 19365})
    r2 = _mk_reval(plex2, [_src(WEAK, gen=3, active=True),
                           _src(FRAleanor, gen=5, active=False)])
    rk2, _ = asyncio.run(r2.resolve_rating_key(_item()))
    assert rk2 == 19365


def test_03_ids_uuid_nooit_plex_filename():
    """(3) een .ids-uuid-pad mag nooit als Plex-filename-identiteit gelden —
    de resolver matcht uitsluitend op release-basenames."""
    seen = []

    class Spy(ReleaseDBPlex):
        async def find_rating_key_via_db(self, part_file):
            seen.append(part_file)
            return await super().find_rating_key_via_db(part_file)

    plex = Spy({WEAK: 19365})
    r = _mk_reval(plex, [_src(WEAK, gen=3, active=True)])
    rk, _ = asyncio.run(r.resolve_rating_key(_item()))
    assert rk == 19365
    assert all(not s.startswith(".ids") for s in seen)
    assert PLEX_PATH not in seen


def test_04_movie_leaf_ratingkey():
    """(4) film: leaf-ratingKey (de film zelf), niet een parent/show-id."""
    plex = ReleaseDBPlex({WEAK: 19365})
    r = _mk_reval(plex, [_src(WEAK, gen=3, active=True)])
    item = _item()
    item.kind = "movie"
    res = asyncio.run(r._revalidate(item))
    assert res["rating_key"] == 19365 and res["coherent"] is True


def test_05_episode_leaf_niet_show_ratingkey():
    """(5) episode: leaf-ratingKey (aflevering), nooit de show-ratingKey —
    de DB-lookup joint media_parts→media_items (leaf-level)."""
    parts = {"MobLand S02E04 Blank Curtain 2160p ATV WEB-DL DDP5 1 Atmos DV HDR H 265-RAWR.mkv": 10103}
    plex = ReleaseDBPlex(parts)
    r = _mk_reval(plex, [_src("MobLand S02E04 Blank Curtain 2160p ATV WEB-DL DDP5 1 Atmos DV HDR H 265-RAWR.mkv",
                              active=True)])
    item = SimpleNamespace(id="i2", plex_path=".ids/7/1/c/b/9/71cb9332",
                           title="Blank Curtain", season=2, episode=4,
                           generation=1, duration_s=None)
    rk, _ = asyncio.run(r.resolve_rating_key(item))
    assert rk == 10103                     # niet de show-rk (6837)


def test_06_special_chars_path():
    """(6) spaties/haakjes/leestekens in de releasenaam blijven 1-op-1
    bewaard (urlencode gebeurt pas in de exec-laag)."""
    name = ("Pirates of the Caribbean - On Stranger Tides (2011)/"
            "Pirates.of.the.Caribbean.On.Stranger.Tides.2011.UHD.BluRay."
            "2160p.TrueHD.Atmos.7.1.DV.HEVC.HYBRID.REMUX-FraMeSToR.mkv")
    seen = []

    class Spy(ReleaseDBPlex):
        async def find_rating_key_via_db(self, part_file):
            seen.append(part_file)
            return self.parts.get(os.path.basename(part_file))

    plex = Spy({os.path.basename(name): 19365})
    r = _mk_reval(plex, [_src(name, active=True)])
    rk, _ = asyncio.run(r.resolve_rating_key(_item()))
    assert rk == 19365
    import os as _os
    assert seen[0] == _os.path.basename(name)   # basename ongewijzigd doorgegeven


def test_07_restart_lege_cache_werkt_via_db():
    """(7) core-restart: _last_rk/_part_paths leeg — resolutie loopt via de
    release-filename in de read-only DB."""
    plex = ReleaseDBPlex({WEAK: 19365})
    r = _mk_reval(plex, [_src(WEAK, gen=3, active=True)])
    assert r._last_rk == {} and r._part_paths == {}
    rk, _ = asyncio.run(r.resolve_rating_key(_item()))
    assert rk == 19365


def test_08_blijvend_onbekend_typed_deferred():
    """(8) nergens bekend → typed deferred, geen uitzondering, geen actie."""
    plex = ReleaseDBPlex({})
    r = _mk_reval(plex, [_src(WEAK, gen=3, active=True)], lookup_retries=1)
    res = asyncio.run(r._revalidate(_item()))
    assert res["coherent"] is False
    assert res["error"] == "rating_key_unresolved" and res["deferred"] is True


# ------------------------------------------- single-active-invariant
@pytest.fixture
def store(tmp_path):
    return Store(str(tmp_path / "inv.db"))


def _mk_source_row(item_id, name, gen=1):
    return m.Source(id=m.new_id(), media_item_id=item_id, generation=gen,
                    provider="torbox", info_hash=m.new_id()[:40].replace("-", "a"),
                    torrent_name=name, size=10**9, cached=True, score=90.0,
                    file_id=0)


async def _setup_item(store, item_id="it1"):
    item = m.MediaItem(id=item_id, kind="movie", title="T",
                       plex_path=".ids/x/" + item_id)
    await store.create_item(item)
    return item


def test_09_successful_switch_exact_een_active(tmp_path):
    """(9) switch → precies één actieve rij."""
    async def go():
        s = Store(str(tmp_path / "db.db"))
        await _setup_item(s)
        a, b = _mk_source_row("it1", "a", 1), _mk_source_row("it1", "b", 2)
        await s.upsert_source(a)
        await s.upsert_source(b)
        await s.activate_source("it1", b.id)
        return a.id, b.id, [x.id for x in await s.list_sources("it1")
                            if x.state == m.SourceState.ACTIVE.value]
    aid, bid, act = asyncio.run(go())
    assert act == [bid]


def test_10_aborted_switch_houdt_origineel_enkel(tmp_path):
    """(10) abort raakt de store niet → origineel blijft de enige actieve."""
    async def go():
        s = Store(str(tmp_path / "db.db"))
        await _setup_item(s)
        a, b = _mk_source_row("it1", "a", 1), _mk_source_row("it1", "b", 2)
        await s.upsert_source(a)
        await s.upsert_source(b)
        await s.activate_source("it1", a.id)
        # abort: geen activate-call voor b
        return [x.id for x in await s.list_sources("it1")
                if x.state == m.SourceState.ACTIVE.value]
    act = asyncio.run(go())
    assert len(act) == 1                        # origineel blijft de enige


def test_11_commit_exception_geen_dual_active(tmp_path):
    """(11) crash tussen update en invariant-txn → reconcile-on-read herstelt
    één autoritatieve rij (hoogste generation wint)."""
    async def go():
        s = Store(str(tmp_path / "db.db"))
        await _setup_item(s)
        a, b = _mk_source_row("it1", "a", 1), _mk_source_row("it1", "b", 2)
        await s.upsert_source(a)
        await s.upsert_source(b)
        # gesimuleerde crash-state: twee actieve rijen
        for x in (a, b):
            x.state = m.SourceState.ACTIVE.value
            await s.upsert_source(x)
        n1 = [x.id for x in await s.list_sources("it1")
              if x.state == m.SourceState.ACTIVE.value]
        keep = await s.reconcile_active("it1")
        n2 = [x.id for x in await s.list_sources("it1")
              if x.state == m.SourceState.ACTIVE.value]
        return n1, keep, n2
    before, keep, after = asyncio.run(go())
    assert len(before) == 2                       # dual-active bestond
    assert after == [keep]                        # precies één actief
    assert keep == before[-1]                     # hoogste generation (b) wint


def test_12_snelle_opeenvolgende_switches_enkel_latest(tmp_path):
    """(12) drie activaties snel na elkaar → precies de laatste actief."""
    async def go():
        s = Store(str(tmp_path / "db.db"))
        await _setup_item(s)
        rows = [_mk_source_row("it1", f"r{i}", i + 1) for i in range(3)]
        for x in rows:
            await s.upsert_source(x)
        for x in rows:
            await s.activate_source("it1", x.id)
        act = [x.id for x in await s.list_sources("it1")
               if x.state == m.SourceState.ACTIVE.value]
        return act, rows[-1].id
    act, last = asyncio.run(go())
    assert act == [last]


def test_13_reconcile_dual_active_deterministisch(tmp_path):
    """(13) pre-existing dual-active → deterministisch één actief; history
    blijft bewaard als retired."""
    async def go():
        s = Store(str(tmp_path / "db.db"))
        await _setup_item(s)
        a = _mk_source_row("it1", "old", 3)       # lagere generation
        b = _mk_source_row("it1", "new", 5)       # laatste commit
        for x in (a, b):
            x.state = m.SourceState.ACTIVE.value
            await s.upsert_source(x)
        keep = await s.reconcile_active("it1")
        rows = await s.list_sources("it1")
        act = [x.id for x in rows if x.state == m.SourceState.ACTIVE.value]
        ret = [x.id for x in rows if x.state == "retired"]
        return keep, act, ret, (a.id, b.id)
    keep, act, retired, (aid, bid) = asyncio.run(go())
    assert act == [keep] == [bid]                 # hoogste generation wint
    assert set(retired) == {aid}                  # history bewaard
