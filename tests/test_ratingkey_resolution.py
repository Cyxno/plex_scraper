"""RatingKey-resolutie regressietests (FASE 2, incident 99× revalidatie-faal).

Twee bewezen root causes:
  1. find_rating_key_by_path haalde /library/sections/N/all zonder
     Accept: application/json → XML → JSONDecodeError → stilletjes None;
  2. lookup matched op de .ids-uuid-basename terwijl Plex parts de
     symlink-releasenaam dragen — kon nooit matchen.

Contract nu: voorkeursvolgorde bekende ratingKey → exact part-pad →
suffix → bounded retry → typed deferred; nooit source-rollback, nooit
library-scan, nooit playback-onderbreking.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.plex_revalidation import (                      # noqa: E402
    PlexRevalidator, QUEUED, SUCCEEDED, FAILED)

PLEX_PATH = ".ids/7/1/c/b/9/71cb9332-5ee6-80bd-f58f-154b8761f9ca"
PART_PATH = ("/symlinks/TV Shows/MobLand (2025)/Season 2/"
             "MobLand S02E04 Blank Curtain 2160p ATV WEB-DL DDP5 1 Atmos DV HDR H 265-RAWR.mkv")


class _Store:
    def __init__(self, sources):
        self._sources = sources

    async def list_sources(self, item_id):
        return self._sources


def _mk_reval(plex, sources=(), **settings):
    resolver = SimpleNamespace(store=_Store(list(sources)))
    s = SimpleNamespace(
        plex_revalidation_enabled=True,
        plex_revalidation_lookup_retries=settings.pop(
            "lookup_retries", 3),
        plex_revalidation_lookup_wait_s=settings.pop("lookup_wait", 0.0),
        plex_revalidation_cooldown_s=0.0)
    return PlexRevalidator(resolver, plex, s)


def _item():
    return SimpleNamespace(id="i1", plex_path=PLEX_PATH, title="Blank Curtain",
                           season=2, episode=4, generation=2, duration_s=None)


class FakePlex:
    """Faal-volgorde-gestuurd: elke lookup-call consumeert één antwoord."""
    def __init__(self, answers, calls_log=None):
        self.answers = list(answers)
        self.calls = calls_log if calls_log is not None else []

    async def find_rating_key_by_path(self, plex_path, exact_path=None):
        self.calls.append((os.path.basename(plex_path), exact_path))
        a = self.answers.pop(0) if self.answers else None
        return a

    async def analyze_item(self, rk):
        return {"analyzed": rk, "status": 200}

    async def get_media_info(self, rk):
        return {"ok": True, "container": "mkv", "video_codec": "hevc",
                "height": 2160, "duration_s": None,
                "size": 8444260319,           # Plex ziet nog de gen2-size
                "audio_codec": None, "streams": []}


def _source():
    from plex_scraper.common.domain import models as m
    return m.Source(id="s1", media_item_id="i1", generation=2,
                    provider="torbox", info_hash="h" * 40,
                    torrent_name="MobLand.S02E04.2160p-RAWR", size=8444260319,
                    cached=True, score=90.0, file_id=0, state="active")


def test_01_known_ratingkey_reused():
    """Bekende ratingKey uit eerdere succesvolle verificatie wint — er zijn
    dan géén lookup-calls meer nodig."""
    plex = FakePlex([10103])
    r = _mk_reval(plex, [_source()])
    item = _item()
    r._last_rk[item.id] = 10103
    rk, how = asyncio.run(r.resolve_rating_key(item))
    assert rk == 10103 and how == "cached"
    assert plex.calls == []


def test_02_exact_path_lookup_success():
    """Exact part-pad (bij delivery geregistreerd) wordt als eerste gevraagd."""
    plex = FakePlex([10103])
    r = _mk_reval(plex, [_source()])
    r.register_part_path("i1", PART_PATH)
    item = _item()
    rk, how = asyncio.run(r.resolve_rating_key(item))
    assert rk == 10103 and how == "exact_path"
    assert plex.calls[0][1] == PART_PATH


def test_03_special_character_path_passed_unchanged():
    """Pad met spaties/haakjes gaat 1-op-1 naar de exact-match lookup
    (geen dubbele URL-encoding — de exec-laag doet urlencode)."""
    plex = FakePlex([None, 10103])
    r = _mk_reval(plex, [_source()])
    r.register_part_path("i1", PART_PATH)
    rk, how = asyncio.run(r.resolve_rating_key(_item()))
    assert rk == 10103
    passed_exact = plex.calls[0][1]
    assert "MobLand (2025)" in passed_exact and "Season 2" in passed_exact
    assert passed_exact == PART_PATH


def test_04_transient_miss_retry_success():
    """Tijdelijke miss (bijv. Plex index-lag) → bounded retry → succes."""
    plex = FakePlex([None, None, 10103])       # exact faalt 1×, suffix daarna
    r = _mk_reval(plex, [_source()], lookup_retries=3, lookup_wait=0.01)
    r.register_part_path("i1", PART_PATH)
    rk, how = asyncio.run(r.resolve_rating_key(_item()))
    assert rk == 10103 and how == "exact_path"   # 3e poging: exact match
    assert len(plex.calls) == 3


def test_05_permanent_miss_typed_deferred():
    """Permanent onbekend → typed deferred-resultaat, geen uitzondering,
    geen impliciete bron-actie."""
    plex = FakePlex([])
    r = _mk_reval(plex, [_source()], lookup_retries=2, lookup_wait=0.0)
    res = asyncio.run(r._revalidate(_item()))
    assert res["coherent"] is False
    assert res["error"] == "rating_key_unresolved"
    assert res["deferred"] is True
    assert res["lookup"] == "unresolved"


def test_06_full_revalidation_succeeds_via_exact_path():
    """End-to-end revalidatie: ratingKey gevonden → analyze → coherent True;
    de gevonden rk wordt gecached voor de volgende keer."""
    plex = FakePlex([10103])
    r = _mk_reval(plex, [_source()])
    r.register_part_path("i1", PART_PATH)
    item = _item()
    res = asyncio.run(r._revalidate(item))
    assert res["coherent"] is True and res["rating_key"] == 10103
    assert r._last_rk[item.id] == 10103


def test_07_source_swap_gen2_naar_gen3_zelfde_plex_item():
    """Material-swap gen2→gen3: het Plex-item (en ratingKey) blijft gelijk —
    de gecachte ratingKey blijft geldig en de revalidatie verifieert tegen
    de nieuwe actieve bron."""
    from plex_scraper.common.domain import models as m
    src3 = _source()
    src3.generation = 3
    src3.size = 52_436_176_471
    plex = FakePlex([10103, 10103])   # 1× direct resolve + 1× in _revalidate
    r = _mk_reval(plex, [src3])
    r.register_part_path("i1", PART_PATH)
    item = _item()
    item.generation = 3
    rk, how = asyncio.run(r.resolve_rating_key(item))
    assert rk == 10103 and how == "exact_path"
    res = asyncio.run(r._revalidate(item))
    assert res["rating_key"] == 10103
    # size-mismatch (nieuwe bron 52 GB vs oude Plex-metadata) → coherent False
    # mét rating_key aanwezig: de mismatch is een metadata-feit, geen lookup-faal
    assert res["coherent"] is False
    assert any(m.startswith("size") for m in res.get("mismatches", []))


def test_08_revalidation_never_rolls_back_source_or_scans():
    """Structurele guard: de revalidator triggert geen library-scan en kent
    geen bron-rollback (state-mutaties op bronnen/item bestaan niet)."""
    import inspect
    src = inspect.getsource(PlexRevalidator)
    assert "scan_section" not in src
    assert "SOURCE_FAILED" not in src
    assert "delivery_bad" not in src
    assert "retire_active" not in src


def test_09_playback_not_interrupted_by_revalidation():
    """Revalidatie-revalidatie mag geen sessies sluiten: de release-path
    bestaat niet in de revalidator (alleen de engine reconnect na commit)."""
    import inspect
    src = inspect.getsource(PlexRevalidator)
    assert "release(" not in src
    assert "sessions" not in src.replace("media_streams", "")
