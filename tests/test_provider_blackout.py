"""Regressie: provider-blackout hardening (incident 2026-10-08).

Root cause (bewezen door de playback-incident-audit): TorBox provider-wide
429 op /torrents/requestdl. De 429-semantiek was call-local: elke subsystem
hamerde独立的 door, candidates brandden, JIT rapporteerde misleidende
no_equivalent_source en items konden vals NO_SOURCE worden.

Deze suite bewijst: 429 → centrale cooldown → geen retry-storm → geen
NO_SOURCE → correcte UI-state → automatische recovery.
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from conftest import PREFS, cand, got_key, got_results  # noqa: E402
from plex_scraper.common.config import Settings  # noqa: E402
from plex_scraper.common.scoring.engine import Scorer  # noqa: E402
from plex_scraper.resolver.caches import CacheSet  # noqa: E402
from plex_scraper.resolver.engine import Resolver  # noqa: E402
from plex_scraper.resolver.health import HealthSweeper  # noqa: E402
from plex_scraper.resolver.jit import JitConfig, JitController  # noqa: E402
from plex_scraper.resolver.store import Store  # noqa: E402
from plex_scraper.scraper.provider_availability import (  # noqa: E402
    DAILY_LIMIT,
    HEALTHY,
    RATE_LIMITED,
    ProviderAvailability,
    ProviderBlackout,
    classify_429,
)
from plex_scraper.scraper.providers.mock import MockProvider  # noqa: E402
from plex_scraper.scraper.providers.torbox import TorboxProvider  # noqa: E402
from plex_scraper.scraper.scrapers.mock import MockScraper  # noqa: E402


# ----------------------------------------------------------------- helpers
def fresh_availability() -> ProviderAvailability:
    av = ProviderAvailability("torbox")
    av.events: list[tuple] = []
    av.set_sink(lambda kind, **fields: av.events.append((kind, fields)))
    return av


def make_torbox(av: ProviderAvailability, status: int = 429,
                body: str = "rate limit exceeded",
                headers: dict | None = None):
    """TorboxProvider met een tellende MockTransport."""
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        if status == 200:
            return httpx.Response(200, json={"success": True, "data": {}})
        return httpx.Response(status, text=body, headers=headers or {})

    settings = Settings(torbox_api_token="test-token")
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                               follow_redirects=False)
    provider = TorboxProvider(settings, client=client, availability=av)
    return provider, counter


class GateProvider:
    """Faithful mirror van de gewired TorboxProvider: availability-gate vóór
    elke API-call, success/backend-error terugrapporteren; read_range (CDN)
    blijft ongegated. Wikkelt MockProvider."""

    name = "torbox"

    def __init__(self, inner: MockProvider, avail: ProviderAvailability):
        self._inner = inner
        self._avail = avail

    async def _gated(self, endpoint: str, fn, *a, **k):
        self._avail.check(endpoint)          # raise ProviderBlackout
        try:
            out = await fn(*a, **k)
        except ProviderBlackout:
            raise
        except Exception as exc:             # noqa: BLE001
            self._avail.report_backend_error(repr(exc)[:120],
                                             endpoint_class=endpoint)
            raise
        self._avail.report_success()
        return out

    async def availability(self, hashes):
        return await self._gated("checkcached", self._inner.availability, hashes)

    async def ensure_torrent(self, info_hash, torrent_name):
        return await self._gated("mylist", self._inner.ensure_torrent,
                                 info_hash, torrent_name)

    async def get_stream_url(self, torrent_id, file_id):
        return await self._gated("requestdl", self._inner.get_stream_url,
                                 torrent_id, file_id)

    async def read_range(self, url, start, length):
        return await self._inner.read_range(url, start, length)   # NIET gegate

    def pick_file(self, torrent, hint=None):
        return self._inner.pick_file(torrent, hint)


def make_engine(settings: Settings, avail: ProviderAvailability,
                specs: dict | None = None) -> Resolver:
    import dataclasses
    settings = dataclasses.replace(settings, cache_bad_ttl=0.05,
                                   cache_bad_ttl_max=0.5)
    store = Store(settings.db_path)
    caches = CacheSet(candidates_ttl=settings.cache_candidates_ttl,
                      checkcached_ttl=settings.cache_checkcached_ttl,
                      link_ttl=settings.cache_link_ttl)
    provider = GateProvider(MockProvider(specs or {}), avail)
    resolver = Resolver(settings, store, provider, [MockScraper({got_key(): got_results()})],
                        Scorer.from_yaml(str(PREFS)), caches,
                        availability=avail)
    return resolver


def blackout(av: ProviderAvailability, retry_in: float = 300.0) -> None:
    """Zet de gedeelde availability in een actieve RATE_LIMITED-blackout."""
    av.report_429(retry_after_s=retry_in, detail="torbox requestdl: HTTP 429",
                  endpoint_class="requestdl")


def got_item_dict() -> dict:
    """Game of Thrones S01E01 (spiegel van de got_item-fixture in conftest)."""
    return {
        "kind": "episode", "title": "Winter Is Coming", "series": "Game of Thrones",
        "show_imdb_id": "tt0944947",
        "season": 1, "episode": 1, "imdb_id": "tt0944947",
        "plex_path": "TV/Game of Thrones/Season 01/Game of Thrones - S01E01.mkv",
    }


async def register_item(resolver: Resolver) -> object:
    return await resolver.register_item(got_item_dict())


# ------------------------------------------------------ 1) 429 → centrale state
async def test_requestdl_429_sets_global_rate_limited(tmp_path):
    av = fresh_availability()
    provider, counter = make_torbox(av, status=429, body="rate limit exceeded",
                                    headers={"Retry-After": "90"})
    with pytest.raises(ProviderBlackout) as exc:
        await provider.get_stream_url(123, 0)
    assert av.state == RATE_LIMITED
    assert av.blocked()
    assert exc.value.cooldown_until == pytest.approx(time.time() + 90, abs=5)
    snap = av.snapshot()
    assert snap["http_status"] == 429
    assert snap["last_429_at"] is not None
    assert snap["cooldown_until"] > time.time()


async def test_daily_limit_body_classified_as_daily(tmp_path):
    av = fresh_availability()
    provider, _ = make_torbox(av, status=429,
                              body="Daily bandwidth limit reached")
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(123, 0)
    assert av.state == DAILY_LIMIT
    assert classify_429("daily bandwidth") == DAILY_LIMIT
    assert classify_429("too many requests") == RATE_LIMITED


# --------------------------- 2) tweede call tijdens cooldown → nul HTTP
async def test_second_call_during_cooldown_makes_no_http_call(tmp_path):
    av = fresh_availability()
    provider, counter = make_torbox(av, status=429)
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(123, 0)
    n_after_first = counter["n"]
    assert n_after_first == 1
    # tweede "subsystem" (andere call-site, zelfde availability) blijft dicht
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(456, 0)
    with pytest.raises(ProviderBlackout):
        await provider.ensure_torrent("deadbeef", "name.mkv")
    assert counter["n"] == n_after_first          # nul extra HTTP
    assert av.metrics["requests_blocked"] >= 2


# ------------------------- 3) blackout schrijft NOOIT NO_SOURCE
async def test_blackout_never_writes_no_source(tmp_path, settings):
    av = fresh_availability()
    resolver = make_engine(settings, av)
    item = await register_item(resolver)          # gezonde eerste resolve
    assert item.status == "READY"

    blackout(av)
    resolver.caches.candidates.invalidate(
        f"{item.kind}:{item.imdb_id}:{item.season}:{item.episode}")
    resolver.caches.links._data.clear()
    src = await resolver.resolve_item(item, reason="repair_during_blackout")
    fresh = await resolver.store.get_item(item.id)
    # actieve bron behouden: READY blijft READY (repair deferred), NOOIT
    # NO_SOURCE en ook niet PROVIDER_WAIT (dat zou de volgende open breken)
    assert fresh.status == "READY"
    assert src is not None
    # geen candidate-burn: actieve bron niet op bad gezet
    sources = await resolver.store.list_sources(item.id)
    assert any(s.state == "active" for s in sources)
    assert all(not s.is_bad() for s in sources)
    assert all(s.failure_count == 0 for s in sources)


async def test_blackout_without_active_source_goes_provider_wait(
        tmp_path, settings):
    av = fresh_availability()
    resolver = make_engine(settings, av)
    item = await register_item(resolver)
    assert await resolver.fail_current(item.id)   # actieve bron sterft
    blackout(av)
    resolver.caches.candidates.invalidate(
        f"{item.kind}:{item.imdb_id}:{item.season}:{item.episode}")
    await resolver.resolve_item(item, reason="repair_during_blackout")
    fresh = await resolver.store.get_item(item.id)
    assert fresh.status == "PROVIDER_WAIT"        # nooit NO_SOURCE
    evs = await resolver.store.recent_events(30)
    assert any(e["kind"] == "resolution_deferred_provider" for e in evs)


# --------------------- 4) JIT defer, geen no_equivalent_source
async def test_jit_deferred_not_no_equivalent(tmp_path, settings):
    av = fresh_availability()
    resolver = make_engine(settings, av)
    item = await register_item(resolver)
    source = await resolver._active_source(item.id)
    blackout(av)
    decision = SimpleNamespace(measured_mbit=5.0, note="", switched=False,
                               switched_to=None)
    ok = await resolver.jit._search_and_switch(item, source, 25.0, decision,
                                               background=False)
    assert ok is False
    assert "blackout" in decision.note.lower()
    assert resolver.jit.metrics.get("jit_no_equivalent_source", 0) == 0
    assert resolver.jit.metrics.get("jit_deferred_provider_unavailable", 0) == 1
    evs = await resolver.store.recent_events(30)
    assert any(e["kind"] == "jit_deferred_provider_unavailable" for e in evs)


async def test_jit_preflight_deferred_during_blackout(tmp_path, settings):
    av = fresh_availability()
    resolver = make_engine(settings, av)
    item = await register_item(resolver)
    source = await resolver._active_source(item.id)
    blackout(av)
    decision = await resolver.jit.preflight_async(item, source, 60.0)
    assert decision.note == "provider blackout — jit deferred"
    # playback loopt door: geen switch, geen fout
    assert decision.switched is False


# ------------------- 5) ingest → PROVIDER_WAIT zonder attempt-burn
async def test_ingest_provider_wait_without_attempt_burn(tmp_path, settings):
    from plex_scraper.ingest.bridge import IngestBridge
    from plex_scraper.ingest.models import IngestJob, JobState
    av = fresh_availability()
    resolver = make_engine(settings, av)
    bridge = IngestBridge(resolver, resolver.s)
    job = IngestJob(source="sonarr", arr_item_id="1:2", kind="episode",
                    dedupe_key="tv:test:S01E01", title="test", season=1,
                    episode=1)
    await resolver.store.upsert_job(job)
    job.attempts = 3
    await resolver.store.update_job(job, {"attempts"})
    blackout(av, retry_in=120.0)
    await bridge.process_job(job)
    fresh = await resolver.store.get_job(job.id)
    assert fresh.status == JobState.PROVIDER_WAIT.value
    assert "blackout" in (fresh.provider_block or "").lower()
    assert fresh.attempts == 0                    # géén attempt-burn
    assert fresh.next_attempt_at == pytest.approx(time.time() + 120, abs=30)

    # na cooldown keert de job terug in de pipeline (geen dode PROVIDER_WAIT-
    # staat); de attempts-reset van de blackout-weg zelf is al bewezen
    av.report_success()
    await bridge.process_job(fresh)
    fresh = await resolver.store.get_job(job.id)
    assert fresh.status != JobState.PROVIDER_WAIT.value


# ------------------------------------------- 6) sweeper pauzeert volledig
async def test_sweeper_paused_during_blackout(tmp_path, settings):
    av = fresh_availability()
    resolver = make_engine(settings, av)
    sweeper = HealthSweeper(resolver, db_path=str(tmp_path / "hs.sqlite"),
                            items_per_hour=10000)
    item = await register_item(resolver)
    blackout(av)
    checked = {"n": 0}

    async def _fail_check(*a, **k):
        checked["n"] += 1
        raise AssertionError("sweeper check_source tijdens blackout")

    sweeper.check_source = _fail_check
    sweeper.playback_active_count = lambda: asyncio.sleep(0, result=0)
    await sweeper.sweep()
    assert checked["n"] == 0
    assert av.metrics["requests_blocked"] == 0    # nul provider-pogingen


# ------------------------- 7) bestaande actieve stream blijft doorlopen
async def test_active_stream_keeps_working_during_blackout(tmp_path):
    av = fresh_availability()
    provider, counter = make_torbox(av, status=429)
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(123, 0)     # blackout actief
    # CDN-readpad: mockt een 206 via een tweede provider-mock
    counter2 = {"n": 0}

    def cdn_handler(request: httpx.Request) -> httpx.Response:
        counter2["n"] += 1
        return httpx.Response(206, content=b"x" * 64)

    settings = Settings(torbox_api_token="t")
    cdn = TorboxProvider(settings, client=httpx.AsyncClient(
        transport=httpx.MockTransport(cdn_handler), follow_redirects=False),
        availability=av)
    data = await cdn.read_range("https://cdn.example/file", 0, 64)
    assert data == b"x" * 64                      # stream loopt door
    assert counter2["n"] == 1


# --------------------------------- 8) cooldown-expiry → één half-open probe
async def test_half_open_single_flight_probe(tmp_path, settings):
    av = fresh_availability()
    resolver = make_engine(settings, av)
    item = await register_item(resolver)
    blackout(av, retry_in=0.05)
    await asyncio.sleep(0.06)                     # cooldown verlopen

    # eerstvolgende call = probe (mag HTTP maken); aanroepende concurrenrien
    # tijdens de probe-window blijven deferred
    granted = 0
    for _ in range(5):
        try:
            av.check("requestdl", role="probe-test")
            granted += 1
        except ProviderBlackout:
            pass
    assert granted == 1                           # single-flight
    snap = av.snapshot()
    assert snap["half_open_probe_inflight"] is True
    assert av.metrics["probes"] == 1


# ------------------------------- 9) probe-succes → HEALTHY + recovery-event
async def test_probe_success_recovers_and_emits_event(tmp_path, settings):
    av = fresh_availability()
    resolver = make_engine(settings, av)
    item = await register_item(resolver)
    blackout(av, retry_in=0.05)
    await asyncio.sleep(0.06)

    resolver.caches.candidates.invalidate(
        f"{item.kind}:{item.imdb_id}:{item.season}:{item.episode}")
    resolver.caches.links._data.clear()
    src = await resolver.resolve_item(item, reason="recovery_probe")
    assert av.state == HEALTHY
    assert av.snapshot()["blocked"] is False
    fresh = await resolver.store.get_item(item.id)
    assert fresh.status == "READY"
    # lifecycle-events (via de resolver-sink → persistente store)
    evs = await resolver.store.recent_events(40)
    kinds = [e["kind"] for e in evs]
    assert kinds.count("provider_blackout_recovered") == 1
    assert kinds.count("provider_blackout_started") == 1


# ------------------------------- 10) probe-429 → cooldown verlengd
async def test_probe_429_extends_cooldown(tmp_path, settings):
    av = fresh_availability()
    provider, counter = make_torbox(av, status=429)
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(1, 0)
    first_cooldown = av.cooldown_until
    # forceer expiry → probe mag door → 429 opnieuw
    av.cooldown_until = time.time() - 0.01
    with pytest.raises(ProviderBlackout):
        await provider.get_stream_url(2, 0)
    assert counter["n"] == 2                      # probe is écht de wire op
    assert av.state == RATE_LIMITED
    assert av.cooldown_until > first_cooldown     # verlengd
    kinds = [k for k, _ in av.events]
    assert "provider_blackout_extended" in kinds
    assert av.snapshot()["blocked"] is True


# ---------------------------------- 11) cockpit toont state + timestamps
def test_cockpit_exposes_availability_state(tmp_path, settings):
    from fastapi.testclient import TestClient

    from plex_scraper.resolver.api.app import create_app
    av = fresh_availability()
    resolver = make_engine(settings, av)
    client = TestClient(create_app(resolver, resolver.s))

    # HEALTHY-baseline
    r = client.get("/api/providers/health").json()
    assert r["availability"]["state"] == HEALTHY
    assert r["availability"]["daily_usage"] is None       # unknown, nooit schatting

    blackout(av, retry_in=600.0)
    r = client.get("/api/providers/health").json()
    avl = r["availability"]
    assert avl["state"] == RATE_LIMITED
    assert avl["blocked"] is True
    assert avl["cooldown_until"] > time.time()
    assert avl["last_429_at"] is not None
    assert avl["retry_in_s"] > 0
    # dashboard draagt de availability expliciet
    d = client.get("/api/dashboard").json()
    assert d["provider_availability"]["state"] == RATE_LIMITED


# ------------------------------------------- 12) event-dedupe, geen storm
async def test_event_dedupe_prevents_storms(tmp_path, settings):
    av = fresh_availability()
    for _ in range(6):                            # 6 harde 429s achter elkaar
        av.report_429(retry_after_s=30.0, detail="torbox requestdl: HTTP 429",
                      endpoint_class="requestdl")
    kinds = [k for k, _ in av.events]
    assert kinds.count("provider_blackout_started") == 1   # één start
    assert "provider_blackout_extended" not in kinds       # kleine verlenging: stil
    # blocked-events worden gededupee'd (≥30s interval)
    for _ in range(4):
        with pytest.raises(ProviderBlackout):
            av.check("requestdl", role="storm-test")
    assert kinds.count("provider_request_blocked_by_cooldown") <= 1
    # herstel → precies één recovery-event, geen verdere extensions
    av.report_success()
    kinds = [k for k, _ in av.events]
    assert kinds.count("provider_blackout_recovered") == 1
    av.report_success()
    assert kinds.count("provider_blackout_recovered") == 1


async def test_meaningful_extension_emits_extended_event():
    av = fresh_availability()
    av.report_429(retry_after_s=60.0, detail="429", endpoint_class="requestdl")
    av.cooldown_until = time.time() + 60          # laat eerste cooldown bijna verlopen
    av.report_429(retry_after_s=1800.0, detail="429 again",
                  endpoint_class="requestdl")
    kinds = [k for k, _ in av.events]
    assert kinds.count("provider_blackout_extended") == 1
    assert av.cooldown_until > time.time() + 1700


async def test_stale_provider_wait_item_resumes_after_heal(tmp_path, settings):
    """Incident-backlog (46u PROVIDER_WAIT): zodra de provider weer gezond
    is, moet een stale PROVIDER_WAIT-item een verse resolve krijgen en niet
    eeuwig opnieuw als PROVIDER_WAIT gemarkeerd worden."""
    from plex_scraper.ingest.bridge import IngestBridge
    from plex_scraper.ingest.models import IngestJob, JobState
    av = fresh_availability()
    resolver = make_engine(settings, av)
    item = await register_item(resolver)
    assert await resolver.fail_current(item.id)
    blackout(av)
    await resolver.resolve_item(item, reason="blackout")
    assert (await resolver.store.get_item(item.id)).status == "PROVIDER_WAIT"

    av.report_success()                          # provider hersteld
    bridge = IngestBridge(resolver, resolver.s)
    job = IngestJob(source="sonarr", arr_item_id="1:2", kind="episode",
                    dedupe_key="tv:tt0944947:S01E01", title="test", season=1,
                    episode=1, show_imdb_id="tt0944947")
    await resolver.store.upsert_job(job)
    await bridge.process_job(job)
    item_after = await resolver.store.get_item(item.id)
    fresh_job = await resolver.store.get_job(job.id)
    assert item_after.status == "READY"          # verse resolve is gelukt
    assert fresh_job.status != JobState.PROVIDER_WAIT.value
