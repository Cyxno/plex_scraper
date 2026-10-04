"""Legacy identity data-hygiëne: ID-laagscheiding, corrections, stale states.

Dekt de audit-scenario's: item-ID vs show-ID, authoritative-only correcties,
idempotency, persistence, duplicaten/stale, 0-vs-unreadable candidates,
PLEX_ORPHAN vs provider-no-match, ingest-hygiëne, geen secrets in rapporten.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

import pytest

from plex_scraper.common.domain import models as m
from plex_scraper.maintenance import identity_hygiene as hy


def _episode_item(**over):
    base = {"id": "e1", "kind": "episode", "series": "Game of Thrones",
            "season": 1, "episode": 1,
            "imdb_id": "tt0944947", "tmdb_id": 1399, "tvdb_id": 121361,
            "show_imdb_id": "tt0944947", "show_tmdb_id": 1399, "show_tvdb_id": 121361,
            "status": "READY", "generation": 1}
    base.update(over)
    return base


def _plex_ep(imdb="tt1480055", tmdb=63056, tvdb=3254641):
    return {"imdb": imdb, "tmdb": tmdb, "tvdb": tvdb}


# --- 1/2: item-ID vs show-ID ----------------------------------------------

def test_episode_item_ids_distinct_from_show_ids_is_valid():
    item = _episode_item(imdb_id="tt1480055", tmdb_id=63056, tvdb_id=3254641)
    assert hy.classify_item_ids(item, _plex_ep()) == hy.ITEM_ID_VALID


def test_item_id_equals_show_id_flagged():
    assert hy.classify_item_ids(_episode_item(), _plex_ep()) == hy.ITEM_ID_EQUALS_SHOW_ID


def test_item_id_missing():
    item = _episode_item(imdb_id=None, tmdb_id=None, tvdb_id=None)
    assert hy.classify_item_ids(item, _plex_ep()) == hy.ITEM_ID_MISSING


def test_item_id_conflict_needs_authoritative_guid():
    item = _episode_item(imdb_id="tt9999999", tmdb_id=777, tvdb_id=888)
    assert hy.classify_item_ids(item, _plex_ep()) == hy.ITEM_ID_CONFLICT
    # zonder authoritative guid: nooit CONFLICT claimen — UNKNOWN
    assert hy.classify_item_ids(item, None) == hy.UNKNOWN


# --- 3/4/5: corrections uitsluitend uit authoritative metadata -------------

def test_correction_from_plex_episode_guids():
    fix = hy.build_correction(_episode_item(), _plex_ep())
    assert fix == {"imdb_id": "tt1480055", "tmdb_id": 63056, "tvdb_id": 3254641}


def test_tautulli_fallback_when_plex_has_no_guids():
    """Plex zonder episode-guids -> geen correctie; tweede bron levert ze wél."""
    item = _episode_item()
    assert hy.build_correction(item, {"ratingKey": "1"}) == {}          # geen gok
    tautulli = _plex_ep()
    fix = hy.build_correction(item, {**tautulli, "source": "tautulli"})
    assert fix["imdb_id"] == "tt1480055"


def test_unresolved_item_id_never_guessed():
    """Geen authoritative episode-GUID -> lege correctie (legacy veld blijft staan)."""
    assert hy.build_correction(_episode_item(), None) == {}
    assert hy.build_correction(_episode_item(), {}) == {}


def test_correction_never_nulls_fields():
    """Authoritative bron mist tvdb -> veld wordt NIET op NULL gezet."""
    fix = hy.build_correction(_episode_item(), {"imdb": "tt1480055", "tmdb": 63056})
    assert "tvdb_id" not in fix and set(fix) <= {"imdb_id", "tmdb_id", "tvdb_id"}


# --- 6/17/18: metadata-only, geen status/source-mutatie --------------------

def test_cleanup_is_metadata_only():
    """Correctie bevat uitsluitend de 3 item-ID velden — nooit status/generation."""
    fix = hy.build_correction(_episode_item(), _plex_ep())
    assert set(fix) == {"imdb_id", "tmdb_id", "tvdb_id"}
    assert not ({"status", "generation", "desired", "plex_path", "show_imdb_id"} & set(fix))


def test_audit_during_identity_pass_touches_no_status():
    """(17) classificatie verandert niets — pure functie, geen side-effects."""
    item = _episode_item()
    before = dict(item)
    hy.classify_item_ids(item, _plex_ep())
    assert item == before


# --- 7/14/15: search-key semantics -----------------------------------------

def test_search_key_uses_show_imdb_not_item_id():
    """(7) series-search blijft show_imdb_id:S:E — item-IDs zijn irrelevant."""
    it = m.MediaItem(id="e1", kind="episode", title="Winter Is Coming",
                     plex_path=".ids/x", series="Game of Thrones",
                     season=1, episode=1, imdb_id="tt1480055", tvdb_id=3254641,
                     show_imdb_id="tt0944947")
    sk = it.search_key()
    assert sk == {"kind": "episode", "imdb_id": "tt0944947",
                  "series": "Game of Thrones", "season": 1, "episode": 1}


def test_movie_identity_path_uses_item_imdb():
    """(14) films: item-level imdb_id is de search-identity."""
    it = m.MediaItem(id="m1", kind="movie", title="Jackass: The Movie",
                     plex_path=".ids/y", imdb_id="tt0322802", tmdb_id=9012)
    assert it.search_key() == {"kind": "movie", "imdb_id": "tt0322802",
                               "title": "Jackass: The Movie", "year": None}


# --- 8/9: idempotency + persistence -----------------------------------------

def test_second_audit_run_is_noop():
    """(8) na correctie produceren dezelfde guids een lege fix (idempotent)."""
    item = _episode_item()
    first = hy.build_correction(item, _plex_ep())
    corrected = {**item, **first}
    assert hy.build_correction(corrected, _plex_ep()) == {}


async def test_correction_persists_across_store_reopen(tmp_path):
    """(9) PATCH-pad: update_item+heropenen van de store behoudt de ID-lagen."""
    from plex_scraper.resolver.store import Store
    db = str(tmp_path / "state.db")
    store = Store(db)
    await store.create_item(m.MediaItem(id="e1", kind="episode", title="t",
                                     plex_path=".ids/x", series="S", season=1,
                                     episode=1, imdb_id="tt0944947",
                                     show_imdb_id="tt0944947"))
    item = await store.get_item("e1")
    item.imdb_id, item.tmdb_id, item.tvdb_id = "tt1480055", 63056, 3254641
    await store.update_item(item)
    reopened = Store(db)                       # "restart"
    fresh = await reopened.get_item("e1")
    assert (fresh.imdb_id, fresh.tmdb_id, fresh.tvdb_id) == ("tt1480055", 63056, 3254641)
    assert fresh.show_imdb_id == "tt0944947"   # show-laag onaangeroerd


# --- 10/11/12: duplicaten + stale -------------------------------------------

def test_duplicate_episode_detection():
    """(10) MobLand-patroon: dubbele (series,S,E)-registratie wordt gevonden."""
    items = [_episode_item(id="a"),
             _episode_item(id="b", series="game of thrones"),
             _episode_item(id="c", season=2)]
    dups = hy.find_duplicate_episodes(items)
    assert dups == {("game of thrones", 1, 1): ["a", "b"]}


def test_stale_duplicate_classification():
    """(11) stale rij: gen 0, geen sources, naast READY-canonical."""
    stale = {"id": "103c", "status": "NO_SOURCE", "generation": 0, "has_sources": False}
    canonical = {"id": "40ee", "status": "READY", "generation": 1}
    decision = hy.decide_stale_duplicate(stale, canonical)
    assert decision["retire"] is True and len(decision["reasons"]) == 3


def test_ready_canonical_wins_over_stale_duplicate():
    """(12) zonder gezond canonical-item géén retirement."""
    stale = {"id": "x", "status": "NO_SOURCE", "generation": 0, "has_sources": False}
    assert hy.decide_stale_duplicate(stale, {"status": "READY", "generation": 0})["retire"] is False
    assert hy.decide_stale_duplicate(stale, {"status": "NO_SOURCE", "generation": 1})["retire"] is False
    # stale rij mét sources wordt nooit weggegooid
    assert hy.decide_stale_duplicate(
        {**stale, "has_sources": True}, {"status": "READY", "generation": 1})["retire"] is False


# --- 13/16: 0-vs-unreadable + orphan -----------------------------------------

def test_zero_candidates_vs_unreadable_distinct():
    """(13) PROVIDER_NO_MATCH (0) is niet hetzelfde als NO_USABLE_CANDIDATE (>0)."""
    assert hy.classify_no_source(0) == hy.PROVIDER_NO_MATCH
    assert hy.classify_no_source(62, validation_failures=3) == hy.NO_USABLE_CANDIDATE
    assert hy.classify_no_source(9, budget_exhausted=True) == hy.NO_USABLE_CANDIDATE
    assert hy.classify_no_source(5, identity_rejections=2) == hy.NO_USABLE_CANDIDATE
    assert hy.classify_no_source(0, provider_error=True) == hy.BACKEND_UNAVAILABLE


def test_plex_orphan_separate_from_provider_no_match():
    """(16) dode symlink/pad -> PLEX_ORPHAN, ook als de provider zou matchen."""
    assert hy.classify_no_source(0, path_readable=False) == hy.PLEX_ORPHAN
    assert hy.classify_no_source(14, path_readable=False) == hy.PLEX_ORPHAN
    assert hy.classify_no_source(14) == hy.NO_USABLE_CANDIDATE


# --- 19: ingest-hygiëne --------------------------------------------------------

def test_ingest_keeps_id_layers_separate():
    """(19) registratie met aparte lagen roundtript zonder kopiëren."""
    it = m.MediaItem(id="n1", kind="episode", title="t", plex_path=".ids/z",
                     series="S", season=1, episode=1,
                     imdb_id="tt1480055", tmdb_id=63056, tvdb_id=3254641,
                     show_imdb_id="tt0944947", show_tmdb_id=1399, show_tvdb_id=121361)
    sk = it.search_key()
    assert sk["imdb_id"] == "tt0944947"            # show-laag in de key
    assert it.imdb_id == "tt1480055"               # item-laag onaangetast
    # ontbrekende item-laag wordt NIET uit de show-laag gevuld
    bare = m.MediaItem(id="n2", kind="episode", title="t", plex_path=".ids/z",
                       series="S", season=1, episode=1, show_imdb_id="tt0944947")
    assert bare.imdb_id is None
    assert bare.search_key()["imdb_id"] == "tt0944947"


# --- 20: geen secrets in rapporten ---------------------------------------------

def test_report_allowlist_strips_secrets():
    """(20) rapporten dragen alleen audit-tellingen — geen tokens/env."""
    raw = {"total_episodes": 1684, "api_token": "supersecret", "plex_url": "http://x",
           "item_ids_valid": 1675, "password": "haha"}
    out = hy.audit_report(raw)
    assert out == {"total_episodes": 1684, "item_ids_valid": 1675}


# --- store-regressie: sources.id IntegrityError (Jackass 3D-crash) ------------

def _src(**over):
    base = dict(id="s1", media_item_id="m1", generation=1, provider="torbox",
                info_hash="aa" * 20, torrent_name="Jackass Complete Collection",
                file_id=0, file_name="movie1.mkv", size=100, state="candidate")
    base.update(over)
    return m.Source(**base)


async def test_upsert_reused_id_with_other_file_does_not_crash(tmp_path):
    """_similar_source hergebruikt id bij info_hash-match; andere file_id botste
    op de PK (UNIQUE constraint failed: sources.id). Moet in-place updaten."""
    from plex_scraper.resolver.store import Store
    store = Store(str(tmp_path / "state.db"))
    await store.create_item(m.MediaItem(id="m1", kind="movie", title="t", plex_path=".ids/x"))
    await store.upsert_source(_src())
    # zelfde id, andere file (multi-file torrent, tweede kandidaat)
    out = await store.upsert_source(_src(file_id=5, file_name="movie2.mkv"))
    assert out.file_id == 5 and out.file_name == "movie2.mkv"
    rows = await store.list_sources("m1")
    assert len(rows) == 1                      # geen crash, geen duplicaat-rij
