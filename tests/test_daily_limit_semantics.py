"""Regressie 2026-10-09: DAILY_LIMIT semantisch gescheiden van RATE_LIMITED.

Bewezen productie-bug: TorBox stuurt bij de daglimiet-429
(DAILY_BANDWIDTH_LIMIT_EXCEEDED) een korte Retry-After (~61s) mee. De
report_429-logica honoreerde Retry-After altijd, dus de 3600s-daglimiet-
cooldown werd weggenoteerd → 429-probe (~60s cyclus) → opnieuw 429 →
opnieuw blackout, tot de provider-day-reset.

Deze suite bewijst: DAILY_LIMIT opent géén 60s retry-loop, RATE_LIMITED
behoudt de gewone Retry-After-semantiek, subsystemen veroorzaken nul extra
requestdl-calls tijdens DAILY_LIMIT, PROVIDER_WAIT brandt geen attempts, en
recovery werkt zodra expliciet/reset-safe opnieuw geprobed mag worden.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from plex_scraper.common.config import Settings  # noqa: E402
from plex_scraper.scraper.provider_availability import (  # noqa: E402
    DAILY_LIMIT,
    HEALTHY,
    RATE_LIMITED,
    ProviderAvailability,
    ProviderBlackout,
)
from plex_scraper.scraper.providers.torbox import TorboxProvider  # noqa: E402

DAILY_BODY = ('{"success":false,"error":"DAILY_BANDWIDTH_LIMIT_EXCEEDED",'
              '"message":"You have exceeded your daily bandwidth limit.",'
              '"data":{}}')


def fresh_availability() -> ProviderAvailability:
    av = ProviderAvailability("torbox")
    av.events: list[tuple] = []
    av.set_sink(lambda kind, **fields: av.events.append((kind, fields)))
    return av


def make_torbox(av: ProviderAvailability, status: int = 429,
                body: str = DAILY_BODY, headers: dict | None = None,
                success_data="https://cdn.example/file.mkv"):
    """TorboxProvider met een tellende MockTransport (telt per pad)."""
    counter = {"n": 0, "paths": {}}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        counter["paths"][request.url.path] = \
            counter["paths"].get(request.url.path, 0) + 1
        if status == 200:
            return httpx.Response(200, json={"success": True,
                                             "data": success_data})
        return httpx.Response(status, text=body, headers=headers or {"Retry-After": "61"})

    settings = Settings(torbox_api_token="test-token")
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                               follow_redirects=False)
    return TorboxProvider(settings, client=client, availability=av), counter


# 1) DAILY_LIMIT opent geen 60s retry-loop
async def test_daily_limit_ignores_short_retry_after():
    av = fresh_availability()
    av.report_429(retry_after_s=61.0, detail=f"torbox /torrents/requestdl: HTTP 429: {DAILY_BODY}",
                  endpoint_class="requestdl")
    assert av.state == DAILY_LIMIT
    # de ~61s Retry-After mag de daglimiet-cooldown NIET bepalen
    assert av.cooldown_until > time.time() + 3 * 3600
    # na de "61s"-expiry is de provider NOG STEEDS dicht — geen probe-loop
    av.cooldown_until -= 120.0                   # voorbij de korte Retry-After
    assert av.blocked()
    with pytest.raises(ProviderBlackout):
        av.check("requestdl")
    # tweede 429 (de oude loop) opent geen nieuwe korte cyclus
    av.report_429(retry_after_s=61.0, detail=DAILY_BODY,
                  endpoint_class="requestdl")
    assert av.cooldown_until > time.time() + 3 * 3600


# 2) RATE_LIMITED behoudt gewone Retry-After-semantiek
async def test_rate_limited_keeps_retry_after_semantics():
    av = fresh_availability()
    av.report_429(retry_after_s=61.0, detail="torbox /torrents/requestdl: HTTP 429: rate limit",
                  endpoint_class="requestdl")
    assert av.state == RATE_LIMITED
    assert av.cooldown_until == pytest.approx(time.time() + 61, abs=5)
    av.cooldown_until = time.time() - 1          # expiry → half-open probe
    av.check("requestdl")                        # één probe gaat door
    with pytest.raises(ProviderBlackout):
        av.check("requestdl")                    # rest deferred (single-flight)


# 3) meerdere subsystemen tijdens DAILY_LIMIT → 0 extra requestdl-calls
async def test_daily_limit_multiple_subsystems_zero_requestdl_calls():
    av = fresh_availability()
    provider, counter = make_torbox(av)
    # eerste call (resolve-validatie) krijgt de daily-429
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(123, 0)
    assert next((v for k, v in counter["paths"].items() if k.endswith("requestdl")), 0) == 1
    n_before = counter["n"]
    # andere subsystemen (JIT-probe, sweeper, ingest, VFS-meta) komen langs
    for _ in range(5):
        with pytest.raises(ProviderBlackout):
            await provider.get_stream_url(456, 0)
        with pytest.raises(ProviderBlackout):
            await provider.ensure_torrent("deadbeef", "name.mkv")
    # voorbij de kortst denkbare (~61s) Retry-After: nog steeds dicht
    av.cooldown_until -= 120.0
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(789, 0)
    assert counter["n"] == n_before              # 0 extra HTTP, dus 0 requestdl
    assert next((v for k, v in counter["paths"].items() if k.endswith("requestdl")), 0) == 1


# 4) PROVIDER_WAIT-items branden geen attempts tijdens DAILY_LIMIT
async def test_provider_wait_burns_no_attempts_during_daily_limit(tmp_path, settings):
    from plex_scraper.ingest.bridge import IngestBridge
    from plex_scraper.ingest.models import IngestJob, JobState
    av = fresh_availability()
    from test_provider_blackout import make_engine
    resolver = make_engine(settings, av)
    bridge = IngestBridge(resolver, resolver.s)
    job = IngestJob(source="sonarr", arr_item_id="9:9", kind="episode",
                    dedupe_key="tv:daily:S01E01", title="daily test",
                    season=1, episode=1)
    await resolver.store.upsert_job(job)
    job.attempts = 5
    await resolver.store.update_job(job, {"attempts"})
    av.report_429(retry_after_s=61.0, detail=DAILY_BODY,
                  endpoint_class="requestdl")
    await bridge.process_job(job)
    fresh = await resolver.store.get_job(job.id)
    assert fresh.status == JobState.PROVIDER_WAIT.value
    assert fresh.attempts == 0                   # geen attempt-burn
    assert fresh.next_attempt_at > time.time() + 3 * 3600   # géén 60s-retry


# 5) recovery zodra state expliciet/reset-safe opnieuw geprobed mag worden
async def test_recovery_after_reset_safe_reprobe():
    av = fresh_availability()
    provider, counter = make_torbox(av, status=200,
                                    body='{"success":true,"data":{}}')
    av.report_429(retry_after_s=61.0, detail=DAILY_BODY,
                  endpoint_class="requestdl")
    assert av.state == DAILY_LIMIT
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(123, 0)    #cooldown actief → nul HTTP
    assert counter["n"] == 0
    # expliciete reset-safe reprobe (b.v. na provider-day-reset gesignaleerd
    # of door de lange cooldown zelf): cooldown verlopen → één probe
    av.cooldown_until = time.time() - 1
    av._probe_inflight_until = 0.0
    assert await provider.get_stream_url(123, 0) is not None
    assert av.state == HEALTHY
    assert not av.blocked()
    kinds = [k for k, _ in av.events]
    assert kinds.count("provider_blackout_recovered") == 1
