"""FASE 17: HTTP API — health/status/media/session flow/debug gating."""
import pytest
from fastapi.testclient import TestClient

from plex_scraper.api.app import create_app

from conftest import got_key, got_results, make_engine

CAND_BYTES = 1 << 20  # mock default size


@pytest.fixture
def client(settings, scorer, got_item):
    engine, provider, _scraper = make_engine(
        settings, {}, {got_key(): got_results()}, scorer)
    app = create_app(engine, settings)
    return TestClient(app), engine, provider, got_item


def test_health(client):
    http, *_ = client
    r = http.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_register_and_status_flow(client):
    http, engine, provider, item = client
    r = http.post("/media", json=item)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "READY"
    assert body["generation"] == 1
    item_id = body["id"]

    listing = http.get("/media").json()
    assert listing[0]["size"] == CAND_BYTES

    one = http.get(f"/media/{item_id}").json()
    assert one["sources"], "sources listed"
    top = one["sources"][0]
    assert top["state"] == "active"
    assert top["score_breakdown"]["lines"], "transparent breakdown present"

    status = http.get("/status").json()
    assert status["items"]["total"] == 1
    assert status["items"]["by_status"].get("READY") == 1
    assert status["resolutions"] >= 1
    assert "rss_kb" in status["resources"]


def test_open_stream_release(client):
    http, engine, provider, item = client
    item_id = http.post("/media", json=item).json()["id"]
    opened = http.post(f"/media/{item_id}/open").json()
    handle = opened["handle"]
    assert opened["generation"] == 1
    expected = provider.content("got2160dv")

    r = http.get(f"/stream/{handle}", params={"offset": 0, "length": 1024})
    assert r.status_code == 200
    assert r.content == expected[:1024]

    r = http.get(f"/stream/{handle}", params={"offset": 4096, "length": 512})
    assert r.content == expected[4096:4608]

    assert http.delete(f"/open/{handle}").status_code == 200
    r = http.get(f"/stream/{handle}", params={"offset": 0, "length": 10})
    assert r.status_code in (404, 502)


def test_debug_endpoints_gated(settings, scorer, got_item):
    import dataclasses
    settings.debug = False
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    http = TestClient(create_app(engine, settings))
    item_id = http.post("/media", json=got_item).json()["id"]
    assert http.post(f"/debug/media/{item_id}/fail-current").status_code == 404

    settings2 = dataclasses.replace(settings, debug=True, db_path=settings.db_path + "2")
    engine2, _p2, _s2 = make_engine(settings2, {}, {got_key(): got_results()}, scorer)
    http2 = TestClient(create_app(engine2, settings2))
    body = http2.post("/media", json=got_item).json()
    assert http2.post(f"/debug/media/{body['id']}/fail-current").status_code == 200
    assert http2.get(f"/media/{body['id']}").json()["status"] == "SOURCE_FAILED"


def test_session_open_unresolved_returns_503(settings, scorer):
    from plex_scraper.resolver.caches import CacheSet
    from plex_scraper.providers.mock import MockProvider
    from plex_scraper.scrapers.mock import MockScraper
    from plex_scraper.resolver.store import Store
    from plex_scraper.resolver.engine import Resolver
    engine = Resolver(settings, Store(settings.db_path), MockProvider(), [MockScraper({})],
                      scorer, CacheSet(600, 600, 600))
    http = TestClient(create_app(engine, settings))
    body = http.post("/media", json={
        "kind": "movie", "title": "X", "imdb_id": "tt0000001",
        "plex_path": "Movies/X/X.mkv"}).json()
    assert body["status"] == "NO_SOURCE"
    r = http.post(f"/media/{body['id']}/open")
    assert r.status_code == 503


def test_forced_resolve_and_patch(settings, scorer, got_item):
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    http = TestClient(create_app(engine, settings))
    item_id = http.post("/media", json=got_item).json()["id"]
    r = http.post(f"/media/{item_id}/resolve")
    assert r.status_code == 200 and r.json()["resolved"]["state"] == "active"
    r = http.patch(f"/media/{item_id}", json={"desired": {"resolution": "1080p"}})
    assert r.json()["desired"] == {"resolution": "1080p"}
