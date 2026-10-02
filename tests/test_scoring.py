"""FASE 17 unit tests: scoring, exclusions, ranking."""
from plex_scraper.common.scoring.release_parser import parse_release, parse_size_bytes

from conftest import cand, got_key, got_results


def test_parse_release_full():
    p = parse_release("Movie.2024.2160p.DOLBY.VISION.REMUX.TrueHD.7.1.Atmos.HEVC.GRP")
    assert p.resolution == "2160p"
    assert p.video == "dolby_vision"
    assert p.audio == "truehd_atmos"
    assert p.release_type == "remux"
    assert p.language == "english"
    assert not p.flags


def test_parse_release_foreign_and_excluded():
    p = parse_release("Film.2023.1080p.WEB-DL.DDP5.1.H.264.MULTI.FRENCH")
    assert p.language == "foreign"
    p2 = parse_release("Film.2023.1080p.HDCAM.x264.HC")
    assert {"cam", "hardcoded_subs"} <= p2.flags


def test_parse_size():
    assert parse_size_bytes("Movie 54.33 GB x264") == int(54.33 * (1 << 30))
    assert parse_size_bytes("no size here") is None


def test_scoring_breakdown_is_transparent(scorer):
    b = scorer.score("Movie.2024.2160p.DOLBY.VISION.WEB-DL.TrueHD.7.1.Atmos-GRP",
                     cached=True, seeders=50, size=10_000_000_000)
    labels = [line.label for line in b.lines]
    assert any("2160p" in l for l in labels)
    assert any("dolby_vision" in l for l in labels)
    assert any("truehd_atmos" in l for l in labels)
    assert any("cached/ready" in l for l in labels)
    assert any("WEB-DL" in l or "web-dl" in l.lower() for l in labels)
    # position #1 across the board + language 15 + cached 18 + seeders 5
    assert b.total == 30 + 15 + 10 + 15 + 10 + 18 + 5


def test_scoring_positional_decay(scorer):
    b2160 = scorer.score("A.2160p.SDR.x264")
    b1080 = scorer.score("A.1080p.SDR.x264")
    b720 = scorer.score("A.720p.SDR.x264")
    assert b2160.total > b1080.total > b720.total


def test_exclusions_reject_with_reason(scorer):
    for name, flag in [("A.3D.1080p.BluRay.x264", "3d"),
                       ("A.1080p.CAM.x264", "cam"),
                       ("A.1080p.TELESYNC.x264", "telesync"),
                       ("A.1080p.HC.SDR.x264", "hardcoded_subs")]:
        b = scorer.score(name)
        assert b.rejected, name
        assert any(flag in r for r in b.rejects), (name, b.rejects)


def test_size_limit_reject(scorer):
    b = scorer.score("A.2160p.SDR.x264", size=200 * (1 << 30))
    assert b.rejected and any("max 100GB" in r for r in b.rejects)


async def test_ranking_prefers_higher_score(settings, scorer, got_item):
    from types import SimpleNamespace
    from conftest import make_engine, got_key

    engine, _provider, _scraper = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    ranked = await engine._rank_candidates(SimpleNamespace(id="x", generation=0), got_results())
    names = [c.torrent_name for c, _ in ranked]
    assert names[0].startswith("Game.of.Thrones.S01E01.2160p.DOLBY")
    assert not any("CAM" in n for n in names)
    assert not any(".3D." in n for n in names)
