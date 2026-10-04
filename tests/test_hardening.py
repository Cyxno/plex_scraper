"""NO_USABLE_CANDIDATE hardening: pre-add gates, fileIdx sanity, budget,
transient-vs-invalid onderscheid, reproduceerbare reject-traces."""
from __future__ import annotations

import dataclasses
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.common.config import Settings
from plex_scraper.common.domain import models as m
from plex_scraper.scraper.providers.torbox import TorboxProvider

from conftest import cand, got_item, got_key, make_engine


def _prod_sizes(settings):
    """Productie-minima (conftest zet ze op 0 voor de oude fixtures)."""
    return dataclasses.replace(settings, min_media_movie_mb=268,
                               min_media_episode_mb=64)


async def _events(engine, item_id):
    return [e for e in await engine.store.recent_events(500)
            if e.get("media_item_id") == item_id]


# --- C1/C2: fileIdx sanity + pre-add size gate ------------------------------

async def test_sample_fileidx_falls_back_to_pick_file(settings, scorer, got_item):
    """fileIdx=0 van een pack dat naar een sample wijst -> pick_file kiest het
    echte bestand (multi-file selectie robuust)."""
    s = _prod_sizes(settings)
    results = [cand("pack1", "Game.of.Thrones.S01E01.Complete.Pack.1080p-GROUP",
                    size=2_000_000_000, file_index=0)]
    engine, provider, _scraper = make_engine(s, {}, {got_key(): results}, scorer)
    provider.specs["pack1"] = {"cached": True, "size": 200_000_000, "files": {
        0: {"name": "sample.mkv", "size": 5_000_000},
        1: {"name": "Game.of.Thrones.S01E01.1080p.mkv", "size": 200_000_000},
    }}
    item = await engine.register_item(dict(got_item))
    assert item.status == m.ItemStatus.READY.value
    src = await engine._active_source(item.id)
    assert src.file_name == "Game.of.Thrones.S01E01.1080p.mkv"
    kinds = [e["kind"] for e in await _events(engine, item.id)]
    assert "candidate_file_choice_rejected" in kinds


async def test_pregate_tiny_torrent_never_costs_add_budget(settings, scorer, got_item):
    """10MB-torrent vóór de provider-add weggefilterd -> het enige add-budget
    gaat naar de geldige candidate (regressie: budget=1 verspild aan junk)."""
    s = dataclasses.replace(_prod_sizes(settings), max_provider_adds_per_resolve=1)
    results = [
        cand("tiny1", "Game.of.Thrones.S01E01.1080p.WEB-DL.x264-TINY",
             size=10_000_000),
        cand("good1", "Game.of.Thrones.S01E01.1080p.WEB-DL.DDP5.1.H.264-GRP",
             size=1_600_000_000),
    ]
    engine, provider, _scraper = make_engine(s, {}, {got_key(): results}, scorer)
    provider.specs["good1"] = {"size": 1_600_000_000}
    item = await engine.register_item(dict(got_item))
    assert item.status == m.ItemStatus.READY.value
    src = await engine._active_source(item.id)
    assert src.info_hash == "good1"
    kinds = [e["kind"] for e in await _events(engine, item.id)]
    assert "candidate_pre_gate_rejected" in kinds
    assert "resolution_skip_uncached" not in kinds


async def test_pregate_movie_floor_uses_kind_minimum(settings, scorer):
    """Film van 100MB valt onder het film-minimum (268MB) -> pre-gate."""
    item = {"kind": "movie", "title": "Some Movie 2024", "year": 2024,
            "imdb_id": "tt123", "plex_path": "Movies/Some Movie (2024).mkv"}
    s = _prod_sizes(settings)
    results = [cand("epsmall", "Some.Movie.2024.1080p.WEB-DL.x264-GRP",
                    size=100_000_000)]
    engine, _p, _s = make_engine(s, {}, {"movie:tt123": results}, scorer)
    reg = await engine.register_item(dict(item))
    assert reg.status == m.ItemStatus.NO_SOURCE.value
    ev = [e for e in await _events(engine, reg.id)
          if e["kind"] == "candidate_pre_gate_rejected"]
    assert ev and "below min for movie" in ev[0]["reason"]


# --- C4: budget --------------------------------------------------------------

async def test_budget_allows_three_uncached_adds(settings, scorer, got_item):
    """Default-budget 3: drie uncached adds probeert, de vierde wordt geskipt
    mét budget-event (geen stil opgeven)."""
    s = _prod_sizes(settings)
    results = [
        cand(f"bad{i}", f"Game.of.Thrones.S01E01.1080p.WEB-DL.x264-GRP{i}",
             size=2_000_000_000) for i in range(4)
    ]
    engine, provider, _scraper = make_engine(s, {}, {got_key(): results}, scorer)
    for i in range(4):
        provider.specs[f"bad{i}"] = {"validate_fails": True}
    item = await engine.register_item(dict(got_item))
    assert item.status == m.ItemStatus.NO_SOURCE.value
    kinds = [e["kind"] for e in await _events(engine, item.id)]
    assert kinds.count("candidate_failed") == 3        # 3 adds geprobeerd
    skips = [e for e in await _events(engine, item.id)
             if e["kind"] == "resolution_skip_uncached"]
    assert len(skips) == 1 and skips[0]["reason"] == \
        "provider_add_budget_exhausted"


async def test_identity_never_consumes_budget(settings, scorer, got_item):
    """Identity-gated candidates mogen het add-budget niet opmaken: daarna
    moet een uncached candidate nog steeds geprobeerd worden."""
    s = dataclasses.replace(_prod_sizes(settings), max_provider_adds_per_resolve=1)
    results = [
        cand("idnoise", "Totally.Different.Show.S09E09.1080p-GRP", size=2_000_000_000),
        cand("real1", "Game.of.Thrones.S01E01.1080p.WEB-DL.x264-GRP",
             size=2_000_000_000),
    ]
    engine, provider, _scraper = make_engine(s, {}, {got_key(): results}, scorer)
    provider.specs["real1"] = {"cached": False, "size": 2_000_000_000}
    item = await engine.register_item(dict(got_item))
    assert item.status == m.ItemStatus.READY.value
    assert (await engine._active_source(item.id)).info_hash == "real1"


# --- C5: reproduceerbare reject-trace ----------------------------------------

async def test_reject_summary_event_present(settings, scorer, got_item):
    """Elke gefaalde resolve laat één samenvattings-event achter met de
    afbreuk-redenen (candidate -> reden staat in de individuele events)."""
    s = _prod_sizes(settings)
    results = [
        cand("idnoise", "Totally.Different.Show.S09E09.1080p-GRP", size=2_000_000_000),
        cand("fail1", "Game.of.Thrones.S01E01.1080p.WEB-DL.x264-GRP",
             size=2_000_000_000),
    ]
    engine, provider, _scraper = make_engine(s, {}, {got_key(): results}, scorer)
    provider.specs["fail1"] = {"validate_fails": True}
    item = await engine.register_item(dict(got_item))
    assert item.status == m.ItemStatus.NO_SOURCE.value
    summary = [e for e in await _events(engine, item.id)
               if e["kind"] == "resolution_reject_summary"]
    assert summary, "reject-summary ontbreekt"
    rej = summary[0]["rejects"]
    assert rej.get("identity_gate") == 1
    assert rej.get("transient_not_ready") == 1  # validate_fails -> NotReadyError
    assert summary[0]["candidate_count"] == 2
    assert summary[0]["provider_adds"] == 1


async def test_transient_not_ready_vs_sanity_invalid_classified(settings, scorer,
                                                                got_item):
    """'not ready' (transient) en 'too small' (definitief) krijgen verschillende
    reject-soorten in de trace; beide blijven temporary-bad (TTL), nooit
    permanent."""
    s = _prod_sizes(settings)
    results = [
        cand("notready", "Game.of.Thrones.S01E01.1080p.WEB-DL.x264-GRP",
             size=2_000_000_000),
        cand("toosmall", "Game.of.Thrones.S01E01.720p.HDTV.x264-GRP",
             size=2_000_000_000),
    ]
    engine, provider, _scraper = make_engine(s, {}, {got_key(): results}, scorer)
    provider.specs["notready"] = {"validate_failures_left": 1, "size": 2_000_000_000}
    provider.specs["toosmall"] = {"files": {0: {"name": "sample.mkv",
                                                "size": 3_000_000}}}
    item = await engine.register_item(dict(got_item))
    assert item.status == m.ItemStatus.NO_SOURCE.value
    fails = [e for e in await _events(engine, item.id)
             if e["kind"] == "candidate_failed"]
    kinds = {e["reject_kind"] for e in fails}
    assert kinds == {"transient_not_ready", "sanity_invalid"}
    for e in fails:
        assert e["bad_until"] > 0        # temporary, met TTL


# --- C3: createtorrent 400 transient retry ------------------------------------

class _FakeClient:
    """Voldoende voor TorboxProvider zonder netwerk."""


def _torbox(responses):
    """TorboxProvider met _request vervangen door een script."""
    tb = TorboxProvider(Settings(torbox_api_token="x"), client=_FakeClient())
    calls = {"n": 0}

    async def fake_request(method, path, **kw):
        i = min(calls["n"], len(responses) - 1)
        calls["n"] += 1
        r = responses[i]
        if isinstance(r, Exception):
            raise r
        return r

    tb._request = fake_request
    tb.calls = calls
    return tb


async def test_createtorrent_400_retries_once_then_succeeds():
    from plex_scraper.scraper.providers.base import ProviderError
    tb = _torbox([
        ProviderError("torbox /torrents/createtorrent: HTTP 400: "
                      '{"success":false,"error":"DIFF_ISSUE"}'),
        {"data": {"torrent_id": 42, "hash": "ab" * 20, "cached": True}},
        {"data": {"torrent_id": 42, "hash": "ab" * 20, "cached": True,
                  "download_finished": True,
                  "files": [{"id": 0, "name": "movie.mkv", "size": 1_500_000_000}]}},
    ])
    torrent = await tb._add_magnet("ab" * 20, "Movie.2024.1080p-GROUP")
    assert torrent["torrent_id"] == 42 and tb.calls["n"] == 3  # 400 + retry + poll


async def test_createtorrent_persistent_400_stays_candidate_failure():
    from plex_scraper.scraper.providers.base import ProviderError
    tb = _torbox([
        ProviderError("torbox /torrents/createtorrent: HTTP 400: bad"),
        ProviderError("torbox /torrents/createtorrent: HTTP 400: bad"),
    ])
    try:
        await tb._add_magnet("cd" * 20, "Movie.2024.1080p-GROUP")
        raise AssertionError("persistente 400 moet raisen")
    except ProviderError as exc:
        assert "HTTP 400" in str(exc)
    assert tb.calls["n"] == 2                     # begrensd: precies 1 retry


async def test_createtorrent_non_400_not_retried():
    from plex_scraper.scraper.providers.base import ProviderError
    tb = _torbox([
        ProviderError("torbox /torrents/createtorrent: HTTP 422: nope"),
    ])
    try:
        await tb._add_magnet("ee" * 20, "Movie.2024.1080p-GROUP")
        raise AssertionError("422 moet raisen")
    except ProviderError:
        pass
    assert tb.calls["n"] == 1
