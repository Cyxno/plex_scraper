import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
from plex_scraper.maintenance.show_backfill import (
    parse_show_guids, group_by_show, apply_show_ids)


def test_parse_show_guids():
    g = parse_show_guids(["imdb://tt0944947", "tmdb://1399", "tvdb://121361"])
    assert g == {"show_imdb_id": "tt0944947", "show_tmdb_id": "1399",
                 "show_tvdb_id": "121361"}
    assert parse_show_guids([])["show_imdb_id"] is None   # SHOW_GUID_UNAVAILABLE


def test_per_show_grouping_one_lookup_many_episodes():
    """(1) één show-lookup enriches vele episodes; (F-per-show cache)."""
    eps = [{"show_rating_key": "S1", "id": i} for i in range(50)]
    groups = group_by_show(eps)
    assert list(groups) == ["S1"] and len(groups["S1"]) == 50


def test_backfill_metadata_only_and_idempotent():
    """(A6/A14) alléén show_* velden; tweede run = 0 wijzigingen."""
    eps = [{"id": 1, "show_rating_key": "S1", "show_imdb_id": None,
            "show_tmdb_id": None, "show_tvdb_id": None, "status": "READY",
            "generation": 3}]
    ids = {"show_imdb_id": "tt1", "show_tmdb_id": "2", "show_tvdb_id": "3"}
    n = apply_show_ids(eps, ids)
    assert n == 1
    assert eps[0]["show_imdb_id"] == "tt1"
    assert eps[0]["status"] == "READY" and eps[0]["generation"] == 3  # onaangetast
    n2 = apply_show_ids(eps, ids)
    assert n2 == 0                                   # idempotent
