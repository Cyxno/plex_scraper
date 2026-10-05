"""Discovery-engine: normalisatie + matching-semantiek (discovery ≠ authority)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.maintenance.legacy_discovery import (
    extract_se, extract_year, match_episode, match_movie, norm_title)


def test_norm_strips_release_noise():
    assert norm_title("Man.Of.Steel.2013.PROPER.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT") \
        == "man of steel"
    assert norm_title("The.Holiday.2006.2160p.4K.WEB.x265.10bit.AAC5.1-[YTS.MX]") == "the holiday"


def test_year_and_se_extraction():
    assert extract_year("Movie.2026.2160p-G") == 2026
    assert extract_se("Show.S02E03.FLUX.mkv") == (2, 3)
    assert extract_se("geen marker") is None


def test_exact_unique_movie_match():
    plex = {"man of steel": ("Man of Steel", 2013, "g1"),
            "other film": ("Other", 2020, "g2")}
    conf, key, _ = match_movie(plex, "Man.Of.Steel.2013.PROPER.2160p.REMUX", 2013)
    assert conf == "EXACT" and key == "man of steel"


def test_ambiguous_blocked():
    plex = {"vengeance": ("Vengeance", 2026, "g1"),
            "vengeance a story": ("Vengeance: A Story", 2026, "g2")}
    conf, k, hits = match_movie(plex, "Vengeance.2026.PROPER.1080p-G", 2026)
    assert conf == "AMBIGUOUS" and len(hits) == 2   # nooit EXACT bij meerdere


def test_no_match_is_honest():
    conf, key, _ = match_movie({"other": ("Other", 2019, "g")}, "Totally.Different.2021.REMUX", 2021)
    assert conf == "NO_MATCH" and key is None


def test_tv_show_unique_match():
    shows = {"dark matter": ("Dark Matter", 2024, "g"),
             "something else": ("X", 2020, "g2")}
    conf, key, _ = match_episode(shows, "Dark Matter", 2, 3)  # serienaam uit mapnaam
    assert conf == "EXACT" and key == "dark matter"
    conf2, k2, _ = match_episode(shows, "Onbekende Show", 1, 1)
    assert conf2 == "NO_MATCH" and k2 is None
