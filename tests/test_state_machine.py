"""FASE 17: state machine — generations, fallback, temporary bad, no-source."""
import asyncio

import pytest

from plex_scraper.common.domain import models as m

from conftest import cand, got_key, got_results, make_engine


async def _register_got(engine, got_item):
    return await engine.register_item(dict(got_item))


async def test_bootstrap_resolves_best_candidate(settings, scorer, got_item):
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await _register_got(engine, got_item)
    assert item.status == m.ItemStatus.READY.value
    assert item.generation == 1
    src = await engine._active_source(item.id)
    assert src.info_hash == "got2160dv"
    # transparent breakdown stored with the source
    assert src.score_json["lines"]
    assert src.score > 50


async def test_fail_current_then_next_candidate(settings, scorer, got_item):
    import dataclasses
    settings = dataclasses.replace(settings, cache_bad_ttl=600, cache_bad_ttl_max=3600)
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await _register_got(engine, got_item)
    first = await engine._active_source(item.id)
    assert first.info_hash == "got2160dv"

    # injection: current source dies
    assert await engine.fail_current(item.id)
    item = await engine.store.get_item(item.id)   # fresh copy from the store
    assert item.status == m.ItemStatus.SOURCE_FAILED.value

    # next playback resolves the next candidate, same item, new generation
    ctx = await engine.open_handle(item.id)
    assert ctx.source.info_hash == "got1080"
    assert ctx.session.generation == 2
    item = await engine.store.get_item(item.id)
    assert item.generation == 2
    # bad candidate is marked failed with TTL
    bad = await engine.store.get_source(first.id)
    assert bad.state == m.SourceState.FAILED.value
    assert bad.bad_until > 0


async def test_one_broken_torrent_never_breaks_item(settings, scorer, got_item):
    """Crucial rule: a broken torrent is skipped, never MEDIA_ITEM_BROKEN."""
    specs = {h: {"cached": True, "validate_fails": True}
             for h in ("got2160dv", "got1080", "got720", "gotsd")}
    engine, _p, _s = make_engine(settings, specs, {got_key(): got_results()}, scorer)
    item = await _register_got(engine, got_item)
    assert item.status == m.ItemStatus.NO_SOURCE.value
    got = await engine.resolve_item(item, reason="retry")
    assert got is None
    item = await engine.store.get_item(item.id)
    assert item.status == m.ItemStatus.NO_SOURCE.value
    # every valid candidate carries its failure count + bad_until
    for s in await engine.store.list_sources(item.id):
        assert s.failure_count >= 1
        assert s.bad_until > 0


async def test_all_validations_fail_first_then_recover(settings, scorer, got_item):
    specs = {"got2160dv": {"cached": True, "validate_failures_left": 1},
             "got1080": {"cached": True, "validate_failures_left": 1},
             "got720": {"cached": True, "validate_failures_left": 1},
             "gotsd": {"cached": True, "validate_failures_left": 1}}
    engine, _p, _s = make_engine(settings, specs, {got_key(): got_results()}, scorer)
    item = await _register_got(engine, got_item)
    # everything failed during bootstrap -> NO_SOURCE
    item = await engine.store.get_item(item.id)
    assert item.status == m.ItemStatus.NO_SOURCE.value
    # bad TTL passes -> next resolution succeeds with best candidate
    await asyncio.sleep(settings.cache_bad_ttl + 0.05)
    src = await engine.resolve_item(item, reason="retry")
    assert src is not None
    assert src.info_hash == "got2160dv"
    assert item.status == m.ItemStatus.READY.value


async def test_bad_until_skips_candidates(settings, scorer, got_item):
    import dataclasses
    settings = dataclasses.replace(settings, cache_bad_ttl=600)
    engine, _p, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await _register_got(engine, got_item)
    top = await engine._active_source(item.id)
    await engine.fail_current(item.id)

    # ranking keeps order; the bad-TTL skip happens at validation time
    ranked = await engine._rank_candidates(item, got_results())
    top_cand = next(c for c, _ in ranked if c.info_hash == top.info_hash)
    assert await engine._validate_candidate(item, top_cand) is None
    other = next(c for c, _ in ranked if c.info_hash != top.info_hash)
    assert await engine._validate_candidate(item, other) is not None


async def test_candidate_cache_ttl(settings, scorer, got_item):
    import dataclasses
    settings = dataclasses.replace(settings, cache_candidates_ttl=1.0)
    import asyncio
    calls = {"n": 0}

    class CountingScraper:
        name = "counting"

        async def search(self, item_key):
            calls["n"] += 1
            return got_results()

    from plex_scraper.scraper.providers.mock import MockProvider
    from plex_scraper.resolver.caches import CacheSet
    from plex_scraper.resolver.engine import Resolver
    from plex_scraper.resolver.store import Store
    engine = Resolver(settings, Store(settings.db_path), MockProvider(), [CountingScraper()],
                      scorer, CacheSet(1.0, 600, 600))
    item = await engine.register_item(dict(got_item))
    n1 = calls["n"]
    await engine.resolve_item(item, reason="again")          # served from cache
    assert calls["n"] == n1
    await asyncio.sleep(1.1)
    await engine.resolve_item(item, reason="after-ttl")
    assert calls["n"] == n1 + 1
