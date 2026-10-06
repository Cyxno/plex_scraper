"""Ingest-hardening Phase 44 tests 1-10: provider-foutsemantiek + circuit.

Harde regel: HTTP 429 / 5xx / timeout leiden NOOIT tot NO_SOURCE.
"""
from __future__ import annotations

import time

import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from conftest import cand, got_item, got_key, make_engine  # noqa: E402
from plex_scraper.scraper.provider_circuit import (  # noqa: E402
    CircuitBreakerScraper,
    ProviderCircuit,
)
from plex_scraper.scraper.provider_errors import (  # noqa: E402
    ProviderBackendUnavailable,
    ProviderRateLimited,
    ProviderSearchError,
    parse_retry_after,
)
from plex_scraper.common.domain import models as m  # noqa: E402


class FailingScraper:
    """Duck-typed scraper die een gegeven fout altijd gooit."""

    name = "failing"

    def __init__(self, exc_factory):
        self.exc_factory = exc_factory
        self.calls = 0

    async def search(self, item_key):
        self.calls += 1
        raise self.exc_factory()


class ZeroScraper:
    name = "zero"

    def __init__(self):
        self.calls = 0

    async def search(self, item_key):
        self.calls += 1
        return []  # 200 met nul streams — ECHT no-match


class OnceScraper:
    """Geeft de eerste n calls een fout, daarna een geldige candidate."""

    name = "once"

    def __init__(self, n_fail, exc_factory, results):
        self.n_fail = n_fail
        self.exc_factory = exc_factory
        self.results = results
        self.calls = 0

    async def search(self, item_key):
        self.calls += 1
        if self.calls <= self.n_fail:
            raise self.exc_factory()
        return self.results


# ------------------------------------------------- Phase 44 test 1 (harde regel)
@pytest.mark.asyncio
async def test_429_never_becomes_no_source(settings, scorer, got_item):
    """HTTP 429 → PROVIDER_WAIT, status NO_SOURCE is onbereikbaar."""
    engine, provider, _ = make_engine(
        settings, {"got2160dv": {"size": 1 << 20}}, {"episode:tt0944947:1:1": [
            cand("got2160dv", "Game.of.Thrones.S01E01.2160p.WEB-DL.H.264-GRP",
                 size=3_000_000_000)]},
        scorer)
    engine.scrapers = [FailingScraper(
        lambda: ProviderRateLimited("failing", "429", retry_after_s=60.0))]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "PROVIDER_WAIT"
    assert fresh.status != "NO_SOURCE"
    # géén negative cache: candidate-cache heeft GEEN lege lijst opgeslagen
    assert engine.caches.candidates.get(got_key()) is None


@pytest.mark.asyncio
async def test_429_rate_limited_after_previous_no_source_does_not_stick(
        settings, scorer, got_item):
    """Een 429 ná een eerdere mislukte resolve zet het item terug naar
    PROVIDER_WAIT — het oude NO_SOURCE-verdacht blijft niet staan."""
    engine, provider, _ = make_engine(
        settings, {}, {"episode:tt0944947:1:1": []}, scorer)
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "NO_SOURCE"       # echte zero-result = terecht
    engine.scrapers = [FailingScraper(
        lambda: ProviderRateLimited("failing", "429", retry_after_s=None))]
    await engine.resolve_item(fresh, reason="forced")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "PROVIDER_WAIT"


# ------------------------------------------------- Phase 44 test 2 (Retry-After)
def test_retry_after_seconds():
    assert parse_retry_after("120") == 120.0
    assert parse_retry_after("0") == 0.0


def test_retry_after_http_date():
    import email.utils
    future = time.time() + 300
    hdr = email.utils.formatdate(future, usegmt=True)
    val = parse_retry_after(hdr)
    assert 200 <= val <= 300


def test_retry_after_garbage_is_none():
    assert parse_retry_after("soon") is None
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None


# ------------------------------------------------- Phase 44 test 3 (backoff)
def test_circuit_backoff_steps_without_header():
    c = ProviderCircuit(jitter_frac=0.0)
    for expected in (60.0, 120.0, 300.0, 600.0, 1800.0, 1800.0):
        c.report_rate_limited("torrentio", None)
        snap = c.snapshot()["scrapers"]["torrentio"]
        delay = snap["retry_in_s"]
        assert expected - 1 <= delay <= expected + 1, (expected, delay)
        # forceer de cooldown weg voor de volgende trede
        c._circuits["torrentio"].unavailable_until = time.time() - 0.01


def test_circuit_honors_retry_after_header():
    c = ProviderCircuit(jitter_frac=0.0)
    c.report_rate_limited("torrentio", retry_after_s=900.0)
    snap = c.snapshot()["scrapers"]["torrentio"]
    assert 890 <= snap["retry_in_s"] <= 900
    assert snap["retry_source"] == "retry-after-header"


# ------------------------------------------------- Phase 44 test 4/6/7 (circuit)
@pytest.mark.asyncio
async def test_circuit_opens_and_defers_without_http(settings, scorer, got_item):
    inner = FailingScraper(
        lambda: ProviderRateLimited("failing", "429", retry_after_s=600.0))
    circuit = ProviderCircuit(jitter_frac=0.0)
    wrapped = CircuitBreakerScraper(inner, circuit)
    engine, provider, _ = make_engine(settings, {}, {}, scorer)
    engine.scrapers = [wrapped]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    calls_after_first = inner.calls
    assert calls_after_first == 1
    # circuit open: alle verdere resolves defer't zonder één HTTP-call
    for _ in range(3):
        await engine.resolve_item(item, reason="retry")
    assert inner.calls == calls_after_first
    assert circuit.snapshot()["overall"] == "RATE_LIMITED"
    snap = circuit.snapshot()["scrapers"]["failing"]
    assert snap["retry_in_s"] > 0
    assert circuit.metrics["requests_deferred"] >= 3
    # nooit als NO_SOURCE geëindigd
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "PROVIDER_WAIT"


@pytest.mark.asyncio
async def test_circuit_half_open_probe_recovers(settings, scorer, got_item):
    """Na de cooldown gaat één probe door; slaagt hij → HEALTHY."""
    results = [cand("got2160dv", "Game.of.Thrones.S01E01.2160p.WEB-DL.H.264-GRP",
                    size=3_000_000_000)]
    scraper = OnceScraper(1, lambda: ProviderRateLimited(
        "once", "429", retry_after_s=0.05), results)
    circuit = ProviderCircuit(jitter_frac=0.0)
    engine, provider, _ = make_engine(settings, {"got2160dv": {"size": 1 << 20}}, {}, scorer)
    engine.scrapers = [CircuitBreakerScraper(scraper, circuit)]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    assert circuit.snapshot()["overall"] == "RATE_LIMITED"
    await asyncio_sleep(0.06)
    assert circuit.available("once")
    await engine.resolve_item(item, reason="retry-after-cooldown")
    assert circuit.snapshot()["overall"] == "HEALTHY"
    assert circuit.metrics["circuit_recovered"] == 1
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "READY"


@pytest.mark.asyncio
async def test_circuit_half_open_probe_reopens_on_429(settings, scorer, got_item):
    scraper = OnceScraper(10 ** 9, lambda: ProviderRateLimited(
        "once", "429", retry_after_s=0.05), [])
    circuit = ProviderCircuit(jitter_frac=0.0)
    engine, provider, _ = make_engine(settings, {}, {}, scorer)
    engine.scrapers = [CircuitBreakerScraper(scraper, circuit)]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    assert circuit.snapshot()["overall"] == "RATE_LIMITED"
    opened_1 = circuit.metrics["circuit_opened"]
    await asyncio_sleep(0.06)
    await engine.resolve_item(item, reason="probe")
    snap = circuit.snapshot()
    assert snap["overall"] == "RATE_LIMITED"          # opnieuw open
    assert circuit.metrics["circuit_opened"] == opened_1  # geen nieuwe 'open'
    assert snap["scrapers"]["once"]["consecutive_failures"] == 2


async def asyncio_sleep(s):
    import asyncio
    await asyncio.sleep(s)


# ------------------------------------------------- Phase 44 test 8 (true zero)
@pytest.mark.asyncio
async def test_200_zero_streams_is_true_no_source(settings, scorer, got_item):
    """200 + nul streams (normaal afgeronde zoekactie) → wél NO_SOURCE."""
    engine, provider, _ = make_engine(settings, {}, {}, scorer)
    engine.scrapers = [ZeroScraper()]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "NO_SOURCE"


# --------------------------------------------- Phase 44 test 9/10 (5xx/timeout)
@pytest.mark.asyncio
async def test_5xx_is_transient_not_no_source(settings, scorer, got_item):
    engine, provider, _ = make_engine(settings, {}, {}, scorer)
    engine.scrapers = [FailingScraper(
        lambda: ProviderBackendUnavailable("failing", "HTTP 502"))]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "PROVIDER_WAIT"
    assert fresh.status != "NO_SOURCE"


@pytest.mark.asyncio
async def test_timeout_is_transient_not_no_source(settings, scorer, got_item):
    engine, provider, _ = make_engine(settings, {}, {}, scorer)
    engine.scrapers = [FailingScraper(
        lambda: ProviderBackendUnavailable("failing", "timeout reading stream"))]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "PROVIDER_WAIT"


@pytest.mark.asyncio
async def test_partial_result_wins_over_provider_error(settings, scorer, got_item):
    """Eén scraper 429, een andere levert geldige candidates → gewoon READY."""
    engine, provider, _ = make_engine(
        settings, {"got2160dv": {"size": 1 << 20}}, {}, scorer)
    results = [cand("got2160dv", "Game.of.Thrones.S01E01.2160p.WEB-DL.H.264-GRP",
                    size=3_000_000_000)]
    engine.scrapers = [
        FailingScraper(lambda: ProviderRateLimited("failing", "429", retry_after_s=60)),
        OnceScraper(0, lambda: None, results),
    ]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "READY"


@pytest.mark.asyncio
async def test_sweeper_skips_provider_wait_without_backoff_penalty(
        settings, scorer, got_item):
    """PROVIDER_WAIT-items: sweeper-handling telt géén no-source-failure op."""
    from plex_scraper.resolver.health import HealthSweeper
    engine, provider, _ = make_engine(settings, {}, {}, scorer)
    engine.scrapers = [FailingScraper(
        lambda: ProviderRateLimited("failing", "429", retry_after_s=120))]
    item = m.MediaItem(**got_item, id=m.new_id())
    await engine.store.create_item(item)
    await engine.resolve_item(item, reason="bootstrap")
    fresh = await engine.store.get_item(item.id)
    assert fresh.status == "PROVIDER_WAIT"
    sweeper = HealthSweeper(engine, db_path=str(settings.db_path) + ".sw")
    out = await sweeper.handle_no_source(fresh)
    assert out.get("skipped") == "provider_wait"
    assert sweeper.no_source_retry._fail_count.get(fresh.plex_path) is None
