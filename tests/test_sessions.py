"""FASE 17: session pinning + concurrent sessions + bytes correctness."""
from plex_scraper.domain import models as m
from plex_scraper.providers.mock import synthetic_bytes

from conftest import got_key, got_results, make_engine


async def test_open_pins_generation_and_serves_bytes(settings, scorer, got_item):
    engine, provider, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await engine.register_item(dict(got_item))
    ctx = await engine.open_handle(item.id)
    expected = provider.content("got2160dv")
    data = await engine.read(ctx.session.handle, 0, 4096)
    assert data == expected[:4096]
    # seek far out
    tail = await engine.read(ctx.session.handle, len(expected) - 100, 100)
    assert tail == expected[-100:]


async def test_mid_session_failure_does_not_switch_pinned_source(settings, scorer, got_item):
    """FASE 7: during one playback NO source switch. Failure => EIO upstream."""
    engine, provider, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await engine.register_item(dict(got_item))
    ctx = await engine.open_handle(item.id)
    handle = ctx.session.handle
    gen_before = ctx.session.generation

    from plex_scraper.providers.base import ProviderError
    async def explode(source_id, offset, length):
        raise ProviderError("upstream gone")
    engine.upstream_read = explode
    try:
        await engine.read(handle, 0, 1024)
        raised = False
    except ProviderError:
        raised = True
    assert raised
    # session still pinned to the same generation (no silent switch)
    ctx2 = engine.get_session(handle)
    assert ctx2.session.generation == gen_before


async def test_next_playback_uses_new_generation(settings, scorer, got_item):
    import dataclasses
    settings = dataclasses.replace(settings, cache_bad_ttl=600, cache_bad_ttl_max=3600)
    engine, provider, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await engine.register_item(dict(got_item))
    old_ctx = await engine.open_handle(item.id)
    old_handle = old_ctx.session.handle

    await engine.fail_current(item.id)
    new_ctx = await engine.open_handle(item.id)

    # old handle still pinned to old (now failed) source; new handle has gen+1
    assert new_ctx.session.generation == old_ctx.session.generation + 1
    expected_old = provider.content("got2160dv")
    expected_new = provider.content("got1080")
    assert await engine.read(old_handle, 0, 2048) == expected_old[:2048]
    assert await engine.read(new_ctx.session.handle, 0, 2048) == expected_new[:2048]
    await engine.release(old_handle)
    assert engine.get_session.__self__  # still alive
    try:
        engine.get_session(old_handle)
        assert False, "released handle must be gone"
    except KeyError:
        pass


async def test_concurrent_sessions_isolated(settings, scorer, got_item):
    """Two handles on one item pin independently; state never mixes."""
    engine, provider, _s = make_engine(settings, {}, {got_key(): got_results()}, scorer)
    item = await engine.register_item(dict(got_item))
    ctx_a = await engine.open_handle(item.id)
    ctx_b = await engine.open_handle(item.id)
    assert ctx_a.session.handle != ctx_b.session.handle
    expected = provider.content("got2160dv")
    a = await engine.read(ctx_a.session.handle, 0, 1000)
    b = await engine.read(ctx_b.session.handle, 5000, 1000)
    assert a == expected[:1000]
    assert b == expected[5000:6000]
    # interleaved random reads
    import asyncio as _aio
    results = await _aio.gather(*[
        engine.read(ctx_a.session.handle, off, 512) for off in range(0, 100000, 4096)])
    for i, chunk in enumerate(results):
        assert chunk == expected[i * 4096:i * 4096 + 512]


async def test_reader_buffer_and_seek(settings, scorer, got_item):
    import dataclasses
    settings = dataclasses.replace(settings, stream_readahead_bytes=65536)
    engine, provider, _s = make_engine(
        settings, {"got2160dv": {"cached": True, "size": 4 << 20}},
        {got_key(): got_results()}, scorer)
    item = await engine.register_item(dict(got_item))
    ctx = await engine.open_handle(item.id)
    reader = ctx.reader
    expected = provider.content("got2160dv")
    d1 = await reader.read(0, 1000)
    assert d1 == expected[:1000]
    # inside buffered window: no new upstream call
    calls = {"n": 0}
    real = engine.upstream_read

    async def counting(source_id, offset, length):
        calls["n"] += 1
        return await real(source_id, offset, length)
    engine.upstream_read = counting
    await reader.read(500, 100)
    assert calls["n"] == 0
    await reader.read(1_000_000, 100)          # outside window -> new GET
    assert calls["n"] == 1
    assert await reader.read(1_000_000, 100) == expected[1_000_000:1_000_100]


async def test_synthetic_bytes_deterministic():
    assert synthetic_bytes("abc", 1000) == synthetic_bytes("abc", 1000)
    assert synthetic_bytes("abc", 1000)[:100] != synthetic_bytes("abd", 1000)[:100]
