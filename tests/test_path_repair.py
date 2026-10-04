"""PLEX_ORPHAN detectie + repair-policy tests (FASE 39-selectie)."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
from plex_scraper.repair.path_repair import (
    classify_part, episode_match_ok, movie_identity, episode_identity,
    new_resolver_path, registration_payload, atomic_symlink_swap)


def test_01_valid_resolver_symlink_no_repair():
    r = classify_part("/symlinks/Movies/x.mkv", "/mnt/remote/nzbdav/.ids/a/b")
    assert r["orphan"] is False and r["resolver_rel"].startswith(".ids/")


def test_02_broken_symlink_orphan():
    r = classify_part("/symlinks/x.mkv", None)
    assert r["orphan"] and r["reason"] == "BROKEN_SYMLINK"


def test_04_legacy_decyparr_target_orphan():
    r = classify_part("/symlinks/x.mkv", "/mnt/debrid/decypharr/__all__/f.mkv")
    assert r["orphan"] and r["reason"] == "LEGACY_PATH" and r["legacy"]


def test_06_series_identity_requires_season_episode():
    assert episode_identity("MobLand", None, 2) is None
    assert episode_identity("MobLand", 2, 2) == {
        "kind": "episode", "series": "MobLand", "season": 2, "episode": 2}


def test_07_movie_identity():
    assert movie_identity("Foo", 2020)["title"] == "Foo"
    assert movie_identity(None, 2020) is None


def test_13_registration_payload_new_ids_route():
    p = registration_payload({"kind": "movie", "title": "Foo", "year": 2020}, 5400000)
    assert p["plex_path"].startswith(".ids/")
    assert p["duration_s"] == 5400.0
    assert p["plex_path"] != registration_payload(
        {"kind": "movie", "title": "Foo"}, 1)["plex_path"]


def test_14_atomic_symlink_swap_preserves_pathname(tmp_path):
    link = tmp_path / "part.mkv"
    os.symlink("/mnt/debrid/old/target.mkv", link)
    new_t = "/mnt/remote/nzbdav/.ids/1/2/3/4/5/abc"
    atomic_symlink_swap(str(link), new_t)
    assert os.readlink(link) == new_t          # zelfde pathname, nieuwe target
    assert not os.path.lexists(str(link) + ".repair-tmp")


def test_27_wrong_episode_rejected():
    assert episode_match_ok("Show.S02E03.1080p-GRP", 2, 2) is False
    assert episode_match_ok("Show.S01E02.1080p-GRP", 2, 2) is False


def test_28_correct_episode_and_season_pack():
    assert episode_match_ok("Show.S02E02.1080p-GRP", 2, 2) is True
    assert episode_match_ok("Show.S02.Complete.Pack", 2, 2) is False  # pack zonder Eyy-file


def test_19_new_route_never_legacy():
    for _ in range(50):
        assert new_resolver_path().startswith(".ids/")


def test_hierarchy_extraction():
    from plex_scraper.repair.path_repair import extract_episode_identity
    ep = {"title": "Song 2", "index": 2, "parent_id": 10}
    season = {"title": "Season 2", "index": 2, "parent_id": 20}
    show = {"title": "MobLand", "year": 2025}
    ident = extract_episode_identity(ep, season, show)
    assert ident == {"kind": "episode", "series": "MobLand", "season": 2,
                     "episode": 2, "episode_title": "Song 2", "year": 2025}


def test_hierarchy_incomplete_no_identity():
    from plex_scraper.repair.path_repair import extract_episode_identity
    assert extract_episode_identity(None, {"index": 2}, {"title": "X"}) is None
    assert extract_episode_identity({"index": None}, {"index": 2},
                                    {"title": "X"}) is None


def test_parse_guids():
    from plex_scraper.repair.path_repair import parse_guids
    g = parse_guids(["imdb://tt43338257", "tmdb://7492638", "tvdb://11542639"])
    assert g == {"imdb_id": "tt43338257", "tmdb_id": "7492638", "tvdb_id": "11542639"}
    assert parse_guids([])["imdb_id"] is None


def test_series_search_key_uses_show_imdb():
    from plex_scraper.common.domain.models import MediaItem
    it = MediaItem(id="e", kind="episode", title="Song 2", plex_path="p.mkv",
                   series="MobLand", season=2, episode=2,
                   imdb_id="tt43338257", show_imdb_id="tt Show".replace(" ", "") or None)
    it.show_imdb_id = "tt111"
    key = it.search_key()
    assert key["imdb_id"] == "tt111"                # show-imdb heeft voorrang


def test_series_missing_show_imdb_flagged():
    from plex_scraper.common.domain.models import MediaItem
    it = MediaItem(id="e", kind="episode", title="Song 2", plex_path="p.mkv",
                   series="MobLand", season=2, episode=2,
                   imdb_id="tt43338257")
    key = it.search_key()
    assert key["show_imdb_missing"] is True     # FASE 11-guard signaleert


def test_series_fallback_to_episode_imdb_until_backfill():
    """FASE 12-veilig: zonder show-imdb valt search terug op episode-imdb
    (legacy gedrag) i.p.v. NO_SOURCE — tot show-backfill voltooid is."""
    from plex_scraper.common.domain.models import MediaItem
    it = MediaItem(id="e", kind="episode", title="X", plex_path="p.mkv",
                   series="MobLand", season=2, episode=2, imdb_id="tt43338257")
    assert it.search_key()["imdb_id"] == "tt43338257"
