"""Cockpit-backend regressie: reject-classificatie, ops view-models,
maintenance-run persistence, identity-subreasons."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from fastapi.testclient import TestClient

from plex_scraper.common.domain import models as m
from plex_scraper.resolver.api.app import create_app
from plex_scraper.resolver.engine import Resolver
from plex_scraper.resolver.selfheal import identity_gate

from conftest import cand, got_item, got_key, make_engine


# --- A2/A3: identity subreasons ----------------------------------------------

def test_identity_subreason_wrong_show():
    ok, _why, sub = identity_gate("Winter Is Coming", "Game of Thrones", 1, 1,
                                  "Totally.Different.Show.S01E01.1080p-GRP")
    assert not ok and sub == "identity_wrong_show"


def test_identity_subreason_pack_missing_episode():
    ok, _why, sub = identity_gate("X", "S.W.A.T.", 4, 1,
                                  "S.W.A.T. Season 4 Complete 1080p-GRP")
    assert not ok and sub == "identity_pack_missing_episode"


def test_identity_punctuated_sxxeyy_not_false_negative():
    """S04.E01 (punctuatie) mag niet als vals-negatief afvallen (compact)."""
    ok, _why, sub = identity_gate("3 Seventeen Year Olds", "S.W.A.T.", 4, 1,
                                  "S.W.A.T.2017.S04.E01.1080p.WEB-GRP")
    assert ok and sub == "identity_ok"


def test_identity_movie_subreasons():
    ok, _w, sub = identity_gate("Some Film", None, None, None,
                                "Other.Movie.2024.1080p-GRP")
    assert not ok and sub == "identity_wrong_movie"


# --- A4/A5: faal-classificatie + TTL -----------------------------------------

def test_failure_classification_and_ttl():
    from plex_scraper.scraper.providers.base import NotReadyError, ProviderError
    cls = Resolver._classify_candidate_failure
    assert cls("release too small for movie (0MB)", None) == \
        ("file_too_small", "PERMANENT_BAD")
    assert cls("torrent has no files", None) == ("torrent_no_files", "PERMANENT_BAD")
    assert cls("torbox x: HTTP 400: bad", None) == ("provider_400", "TRANSIENT_PROVIDER")
    assert cls("torrent ab not ready after 10 polls",
               NotReadyError("")) == ("torrent_not_ready", "RETRYABLE_NOT_READY")
    assert cls("first byte probe empty", ProviderError("")) == \
        ("first_byte_empty", "TRANSIENT_PROVIDER")
    assert cls("range probe empty", ProviderError("")) == \
        ("range_failed", "TRANSIENT_PROVIDER")


async def test_permanent_bad_gets_max_ttl(settings, scorer, got_item):
    """file_too_small valt direct in de maximale TTL, transient kort."""
    import dataclasses
    s = dataclasses.replace(settings, min_media_episode_mb=64,
                            cache_bad_ttl=60.0, cache_bad_ttl_max=3600.0)
    results = [cand("small1", "Game.of.Thrones.S01E01.720p.x264-GRP",
                    size=2_000_000_000)]
    engine, _p, _sc = make_engine(s, {}, {got_key(): results}, scorer)
    engine.provider.specs["small1"] = {"files": {0: {"name": "sample.mkv",
                                                     "size": 3_000_000}}}
    item = await engine.register_item(dict(got_item))
    srcs = await engine.store.list_sources(item.id)
    assert srcs and srcs[0].bad_until - srcs[0].last_verified > 0 \
        or srcs[0].bad_until > 0


# --- R/M: ops endpoints + jobs-persistence -----------------------------------

def _client(settings, scorer, got_item):
    engine, provider, _scraper = make_engine(
        settings, {}, {got_key(): [cand("got2160dv",
                                        "Game.of.Thrones.S01E01.2160p.DV.REMUX-GRP",
                                        size=8_000_000_000)]}, scorer)
    app = create_app(engine, settings)
    return TestClient(app), engine


async def test_ops_endpoints_shape(settings, scorer, got_item):
    client, engine = _client(settings, scorer, got_item)
    await engine.register_item(dict(got_item))

    r = client.get("/api/dashboard")
    assert r.status_code == 200
    d = r.json()
    assert d["library"]["ready"] == 1 and d["health"] in ("HEALTHY", "ATTENTION")

    r = client.get("/api/issues")
    assert r.status_code == 200 and r.json()["total"] == 0

    r = client.get("/api/jobs")
    assert r.status_code == 200 and "jobs" in r.json()

    iid = (await engine.store.list_items())[0].id
    r = client.get(f"/api/media/{iid}/trace")
    assert r.status_code == 200
    t = r.json()
    assert t["item"]["show_imdb_id"] == "tt0944947"
    assert t["last_resolve"]["candidates"]
    assert any(c["result"] == "SELECTED" for c in t["last_resolve"]["candidates"])


async def test_issue_card_with_reject_summary(settings, scorer, got_item):
    """NO_SOURCE-item krijgt semantische kaart met reject-redenen + retry."""
    import dataclasses
    s = dataclasses.replace(settings, min_media_episode_mb=64)
    results = [cand("tiny1", "Game.of.Thrones.S01E01.1080p.x264-TINY",
                    size=10_000_000)]
    engine, _p, _sc = make_engine(s, {}, {got_key(): results}, scorer)
    app = create_app(engine, s)
    client = TestClient(app)
    item = await engine.register_item(dict(got_item))
    assert item.status == "NO_SOURCE"

    d = client.get("/api/issues").json()
    assert d["total"] == 1
    card = d["issues"][0]
    assert card["classification"] in ("NO_USABLE_CANDIDATE", "PROVIDER_NO_MATCH")
    assert any(r["key"] == "pre_gate_size" for r in card["rejects"])


async def test_job_runs_persist_and_list(settings):
    from plex_scraper.resolver.store import Store
    store = Store(str(settings.db_path))
    rid = await store.job_start("health_sweeper", progress_total=10)
    await store.job_progress(rid, processed=5, recovered=1, current_item="X")
    await store.job_finish(rid, "SUCCESS", checks=10)
    runs = await store.job_runs("health_sweeper")
    assert runs[0]["id"] == rid and runs[0]["status"] == "SUCCESS"
    assert runs[0]["processed"] == 5 and runs[0]["recovered"] == 1
    assert runs[0]["summary_json"].get("checks") == 10
