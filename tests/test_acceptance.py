"""THE PoC acceptance test (FASE 12/17):

SOURCE A: score highest -> validation FAIL
SOURCE B: score 2nd     -> validation FAIL
SOURCE C: score 3rd     -> validation PASS
RESULT:   selected C, playback bytes valid, logical path unchanged.
          After C is force-failed and recovers, A serves as generation N+1
          behind the SAME path.
"""
from plex_scraper.domain import models as m
from plex_scraper.providers.mock import synthetic_bytes

from conftest import cand, make_engine

KEY = "movie:tt0137523"

ITEM = {
    "kind": "movie", "title": "The Matrix", "year": 1999, "imdb_id": "tt0137523",
    "plex_path": "Movies/The Matrix (1999)/The Matrix (1999).mkv",
}

RESULTS = [
    cand("sourceA", "The.Matrix.1999.2160p.DOLBY.VISION.REMUX.TrueHD.7.1.Atmos.HEVC-GRP",
         size=4_000_000),
    cand("sourceB", "The.Matrix.1999.2160p.HDR10.WEB-DL.DDP.5.1.HEVC-GRP", size=3_000_000),
    cand("sourceC", "The.Matrix.1999.1080p.WEB-DL.DDP5.1.H.264-GRP", size=2_000_000),
]

SPECS = {
    "sourceA": {"size": 4_000_000, "validate_failures_left": 1},   # fails once, then works
    "sourceB": {"size": 3_000_000, "validate_failures_left": 1},
    "sourceC": {"size": 2_000_000, "cached": True},
}


async def test_acceptance_score98_fail_score95_fail_score88_pass(settings, scorer):
    engine, provider, _s = make_engine(settings, SPECS, {KEY: RESULTS}, scorer)

    # -- ranking proof: A > B > C --------------------------------------
    class _Item:
        id = "x"
    ranked = await engine._rank_candidates(_Item(), RESULTS)
    assert [c.info_hash for c, _ in ranked] == ["sourceA", "sourceB", "sourceC"]

    # -- bootstrap walks A(fail) -> B(fail) -> C(pass) ------------------
    item = await engine.register_item(dict(ITEM))
    src = await engine._active_source(item.id)
    assert src.info_hash == "sourceC"
    assert item.generation == 1
    assert item.status == m.ItemStatus.READY.value

    # -- playback bytes valid -------------------------------------------
    ctx = await engine.open_handle(item.id)
    expected_c = synthetic_bytes("sourceC", 2_000_000)
    assert await engine.read(ctx.session.handle, 0, 4096) == expected_c[:4096]
    assert await engine.read(ctx.session.handle, 1_000_000, 2048) == expected_c[1_000_000:1_002_048]
    await engine.release(ctx.session.handle)

    # -- C dies -> next playback uses next working source (A recovered) --
    assert await engine.fail_current(item.id)
    # A and B carry a temporary-bad TTL from their bootstrap failures; wait
    # for it to expire (backoff expiry is what allows retrying them)
    import asyncio
    await asyncio.sleep(settings.cache_bad_ttl + 0.05)
    ctx2 = await engine.open_handle(item.id)
    assert ctx2.source.info_hash == "sourceA"
    assert ctx2.session.generation == 2

    expected_a = synthetic_bytes("sourceA", 4_000_000)
    assert await engine.read(ctx2.session.handle, 0, 4096) == expected_a[:4096]

    # -- logical path unchanged ------------------------------------------
    item_after = await engine.store.get_item(item.id)
    assert item_after.plex_path == ITEM["plex_path"]
    assert item_after.generation == 2
    assert item_after.status == m.ItemStatus.READY.value

    # events tell the whole story
    events = await engine.store.recent_events(20)
    kinds = [e["kind"] for e in events]
    assert "candidate_failed" in kinds
    assert "resolution_succeeded" in kinds
    assert "source_failed" in kinds
