"""Regressie: Diagnostics-semantiek (semantiek-audit 2026-10-08).

Diagnostics = uitsluitend LIVE operationele health. Historische snapshots
(coverage, legacy audit) horen in Inventory en mogen:
  * nooit in Diagnostics verschijnen;
  * nooit dashboard overall-health beïnvloeden;
  * nooit een fictieve "recount pending"-status tonen.
"Last sweep" toont de laatste GELDIGE run — een oude INTERRUPTED run mag een
nieuwere SUCCESS niet verbergen.
"""
from __future__ import annotations

import dataclasses
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from conftest import PREFS  # noqa: E402
from plex_scraper.common.config import Settings  # noqa: E402
from plex_scraper.common.scoring.engine import Scorer  # noqa: E402
from plex_scraper.resolver.api import ops as ops_mod  # noqa: E402
from plex_scraper.resolver.api.app import create_app  # noqa: E402
from plex_scraper.resolver.caches import CacheSet  # noqa: E402
from plex_scraper.resolver.engine import Resolver  # noqa: E402
from plex_scraper.resolver.store import Store  # noqa: E402
from plex_scraper.scraper.provider_availability import (  # noqa: E402
    ProviderAvailability)
from plex_scraper.scraper.providers.mock import MockProvider  # noqa: E402
from plex_scraper.scraper.scrapers.mock import MockScraper  # noqa: E402

from test_provider_blackout import (  # noqa: E402
    GateProvider, got_item_dict, got_key, got_results)


def make_engine(settings: Settings,
                avail: ProviderAvailability | None = None) -> Resolver:
    settings = dataclasses.replace(settings, cache_bad_ttl=0.05,
                                   cache_bad_ttl_max=0.5)
    avail = avail or ProviderAvailability("torbox")
    store = Store(settings.db_path)
    caches = CacheSet(candidates_ttl=settings.cache_candidates_ttl,
                      checkcached_ttl=settings.cache_checkcached_ttl,
                      link_ttl=settings.cache_link_ttl)
    provider = GateProvider(MockProvider({}), avail)
    return Resolver(settings, store, provider,
                    [MockScraper({got_key(): got_results()})],
                    Scorer.from_yaml(str(PREFS)), caches, availability=avail)


def write_coverage(path: Path, *, age_s: float, managed: int = 1955,
                   total: int = 2022, extra: dict | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"timestamp": time.time() - age_s,
               "managed": managed, "logical_total": total}
    payload.update(extra or {})
    path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture
async def env(tmp_path, settings, monkeypatch):
    resolver = make_engine(settings)
    cov = tmp_path / "coverage" / "latest.json"
    monkeypatch.setattr(ops_mod, "COVERAGE_PATH", str(cov))
    from fastapi.testclient import TestClient
    tc = TestClient(create_app(resolver, resolver.s))
    return tc, resolver, cov


# ---------------- stale snapshots beïnvloeden health niet
async def test_stale_coverage_does_not_affect_health(env):
    tc, _resolver, cov = env
    write_coverage(cov, age_s=44 * 3600)                 # 44h oud
    d = tc.get("/api/dashboard").json()
    assert d["health"] == "HEALTHY"                      # stale ≠ health
    assert d["coverage"]["age_valid"] is True
    assert d["coverage"]["age_s"] > 43 * 3600


async def test_stale_legacy_audit_artifact_does_not_affect_health(env):
    """De coverage-snapshot IS het legacy-audit-artefact — ook 100h oud en
    met legacy_dead-velden mag het health nooit degraderen."""
    tc, _resolver, cov = env
    write_coverage(cov, age_s=100 * 3600, managed=100,
                   extra={"legacy_dead": 900})
    d = tc.get("/api/dashboard").json()
    assert d["health"] == "HEALTHY"
    assert d["coverage"]["age_valid"] is True            # gelabelde snapshot


async def test_dashboard_payload_has_no_historical_health_signal(env):
    """Geen kanaal waarmee stale/legacy data de health kleurt: geen
    legacy-audit health-veld, coverage alléén als gelabelde snapshot."""
    tc, _resolver, cov = env
    write_coverage(cov, age_s=44 * 3600)
    d = tc.get("/api/dashboard").json()
    assert "legacy_audit_health" not in d
    assert {"age_s", "age_valid"} <= set(d["coverage"] or {})


async def test_real_issues_still_visible_despite_stale_snapshots(env):
    """Control: echte live-signalen (NO_SOURCE) kleuren wél — de stale
    snapshots waren niet de rem; de health-logica werkt gewoon."""
    tc, resolver, cov = env
    write_coverage(cov, age_s=44 * 3600)
    item = await resolver.register_item(got_item_dict())
    assert item.status == "READY"
    assert await resolver.fail_current(item.id)
    item.status = "NO_SOURCE"                    # harde no-source-verdict
    await resolver.store.update_runtime(item)
    d = tc.get("/api/dashboard").json()
    assert d["health"] in ("ATTENTION", "DEGRADED")


# ---------------- recount pending alléén bij echte job
async def test_recount_pending_only_with_real_running_job(env):
    tc, resolver, cov = env
    write_coverage(cov, age_s=3600)
    d = tc.get("/api/dashboard").json()
    assert d["now"]["jobs_running"] == []                # geen fictieve jobs

    run_id = await resolver.store.job_start("library_audit",
                                            progress_total=2022)
    d = tc.get("/api/dashboard").json()
    assert any(j["job_type"] == "library_audit"
               for j in d["now"]["jobs_running"])

    await resolver.store.job_finish(run_id, "SUCCESS")
    d = tc.get("/api/dashboard").json()
    assert d["now"]["jobs_running"] == []


# ---------------- last sweep: laatste GELDIGE run
async def test_last_runs_prefers_newest_valid_sweep(env):
    tc, resolver, _cov = env
    old = await resolver.store.job_start("health_sweeper")
    await resolver.store.job_finish(old, "INTERRUPTED", processed=3)
    other = await resolver.store.job_start("library_audit")
    await resolver.store.job_finish(other, "SUCCESS")
    new = await resolver.store.job_start("health_sweeper")
    await resolver.store.job_finish(new, "SUCCESS", processed=42)

    d = tc.get("/api/dashboard").json()
    runs = d["sweeper"]["last_runs"]
    assert runs and runs[0]["status"] == "SUCCESS"       # nieuwste sweeper-run
    assert all(r["id"] != other for r in runs)           # geen mix van types
    assert {r["id"] for r in runs} >= {old, new}
