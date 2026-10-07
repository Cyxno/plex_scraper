"""Regressie: cockpit UI-data-semantiek (audit 2026-10-07).

- Active Now volgt de store: job_progress schrijft progress_current wél
  (de oude signature kraakte op die kwarg, de sweeper slikte de TypeError
  stil en de cockpit bleef eeuwig '0 / 50 · 0%' tonen);
- Ready + Issues + Pending = Total (pending is het complement);
- stale coverage / legacy audit degraderen live health niet;
- SUCCESS/DEFERRED/INTERRUPTED job-semantiek + startup-reconcile;
- dashboard en run history delen dezelfde canonical run-id.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

import pytest
from fastapi.testclient import TestClient

from plex_scraper.resolver.api import ops as ops_module
from plex_scraper.resolver.api.app import create_app
from plex_scraper.resolver.health import HealthSweeper
from plex_scraper.resolver.store import Store

from conftest import cand, got_item, got_key, make_engine  # noqa: E402


def _client_engine(settings, scorer):
    """READY-item (8GB-kandidaat, default size-gates — kleine mock-content
    zou bij een verhoogde size-gate anders NO_SOURCE worden)."""
    engine, _p, _s = make_engine(
        settings, {}, {got_key(): [cand("got2160dv",
                                        "Game.of.Thrones.S01E01.2160p.DV.REMUX-GRP",
                                        size=8_000_000_000)]}, scorer)
    app = create_app(engine, settings)
    return TestClient(app), engine


# ------------------------------------------------- Active Now volgt store
async def test_job_progress_writes_progress_current(settings):
    """DE regressie achter 'Active Now: 0 / 50 · 0%' — progress_current moest
    een geaccepteerde kwarg zijn; hiervoor crashte de update onzichtbaar."""
    store = Store(settings.db_path)
    run_id = await store.job_start("health_sweeper", progress_total=50)
    await store.job_progress(run_id, processed=37, changed=0, recovered=0,
                             current_item="Show — S01E02", progress_current=37)
    row = next(r for r in await store.job_runs(limit=5) if r["id"] == run_id)
    assert row["progress_current"] == 37
    assert row["processed"] == 37
    assert row["status"] == "RUNNING" and row["progress_total"] == 50


async def test_dashboard_active_job_follows_store(settings, scorer, got_item):
    client, engine = _client_engine(settings, scorer)
    await engine.register_item(dict(got_item))
    store = engine.store

    run_id = await store.job_start("health_sweeper", progress_total=50)
    await store.job_progress(run_id, processed=37, progress_current=37,
                             current_item="Breaking Bad — S02E05")

    d = client.get("/api/dashboard").json()
    running = d["now"]["jobs_running"]
    assert len(running) == 1
    j = running[0]
    assert j["id"] == run_id and j["job_type"] == "health_sweeper"
    assert j["progress_current"] == 37 and j["progress_total"] == 50
    assert j["processed"] == 37
    assert isinstance(j["started_at"], (int, float)) and j["started_at"] > 0

    # idle: geen RUNNING-row → lege jobs_running (UI toont compacte idle-regel,
    # nooit een stale 0/50-kaart)
    await store.job_finish(run_id, "SUCCESS", processed=50, recovered=0,
                           progress_current=50)
    d = client.get("/api/dashboard").json()
    assert d["now"]["jobs_running"] == []


async def test_active_card_and_history_share_canonical_run_id(settings, scorer, got_item):
    client, engine = _client_engine(settings, scorer)
    await engine.register_item(dict(got_item))
    store = engine.store
    run_id = await store.job_start("health_sweeper", progress_total=50)
    d = client.get("/api/dashboard").json()
    jobs = client.get("/api/jobs").json()
    assert jobs["history"][0]["id"] == d["now"]["jobs_running"][0]["id"] == run_id


# -------------------------------------------- Ready + Issues + Pending = Total
async def test_ready_issues_pending_sum_to_total(settings, scorer, got_item):
    """Eén engine, drie items: 1 READY, 1 NO_SOURCE, 1 PROVIDER_WAIT
    (als complement = Pending). Som moet exact total zijn."""
    engine, _p, _s = make_engine(
        settings, {}, {got_key(): [cand("got2160dv",
                                        "Game.of.Thrones.S01E01.2160p.DV.REMUX-GRP",
                                        size=8_000_000_000)]}, scorer)
    client = TestClient(create_app(engine, settings))
    await engine.register_item(dict(got_item))                    # READY

    def variant(season):
        item = dict(got_item)
        item.update(season=season, episode=1,
                    plex_path=f"TV/Game of Thrones/Season 0{season}/"
                              f"Game of Thrones - S0{season}E01.mkv")
        return item

    await engine.register_item(variant(2))                        # geen results → NO_SOURCE
    wait_id = (await engine.register_item(variant(3))).id
    await engine.store.run(
        lambda c: c.execute("UPDATE media_items SET status='PROVIDER_WAIT' "
                            "WHERE id=?", (wait_id,)))

    lib = client.get("/api/dashboard").json()["library"]
    assert lib["ready"] == 1 and lib["no_source"] == 1
    assert lib["provider_wait"] == 1 and lib["pending"] == 1
    assert lib["ready"] + lib["no_source"] + lib["pending"] == lib["total"] == 3


# --------------------------------- stale coverage / legacy audit vs. health
def _write_coverage(path, *, ts, legacy_dead=123):
    path.write_text(json.dumps({
        "timestamp": ts, "managed": 1955, "logical_total": 2022,
        "coverage_pct": 96.7, "legacy_dead": legacy_dead, "legacy_working": 0}))


async def test_stale_coverage_does_not_degrade_health(settings, scorer, got_item,
                                                      tmp_path, monkeypatch):
    client, engine = _client_engine(settings, scorer)
    await engine.register_item(dict(got_item))                    # 1 READY, 0 issues
    cov = tmp_path / "latest.json"
    _write_coverage(cov, ts=time.time() - 20 * 3600)
    monkeypatch.setattr(ops_module, "COVERAGE_PATH", str(cov))

    d = client.get("/api/dashboard").json()
    assert d["health"] == "HEALTHY"          # legacy_dead=123 degradeert niet meer
    assert d["coverage"]["age_s"] == pytest.approx(20 * 3600, abs=10)
    assert d["coverage"]["age_valid"] is True
    assert d["coverage"]["legacy_dead"] == 123          # blijft als snapshot-data


async def test_coverage_ms_timestamp_parsed(settings, scorer, got_item,
                                            tmp_path, monkeypatch):
    client, engine = _client_engine(settings, scorer)
    await engine.register_item(dict(got_item))
    cov = tmp_path / "latest.json"
    _write_coverage(cov, ts=(time.time() - 120) * 1000)           # milliseconden
    monkeypatch.setattr(ops_module, "COVERAGE_PATH", str(cov))

    d = client.get("/api/dashboard").json()
    assert d["coverage"]["age_s"] == pytest.approx(120, abs=10)
    assert d["coverage"]["age_valid"] is True


async def test_coverage_invalid_timestamp_no_absurd_age(settings, scorer, got_item,
                                                        tmp_path, monkeypatch):
    client, engine = _client_engine(settings, scorer)
    await engine.register_item(dict(got_item))
    cov = tmp_path / "latest.json"
    _write_coverage(cov, ts="not-a-timestamp")
    monkeypatch.setattr(ops_module, "COVERAGE_PATH", str(cov))

    d = client.get("/api/dashboard").json()
    assert d["coverage"]["age_s"] is None               # UI: 'timestamp unavailable'
    assert d["coverage"]["age_valid"] is False
    assert d["health"] == "HEALTHY"


async def test_legacy_audit_snapshot_does_not_degrade_health(settings, scorer, got_item,
                                                             tmp_path, monkeypatch):
    """Coverage + legacy audit zijn dezelfde puntmoment-snapshot; geen van
    beiden mag DEGRADED/HEALTHY_WITH_LEGACY_GAPS opleveren."""
    client, engine = _client_engine(settings, scorer)
    await engine.register_item(dict(got_item))
    cov = tmp_path / "latest.json"
    _write_coverage(cov, ts=time.time() - 72 * 3600, legacy_dead=999)
    monkeypatch.setattr(ops_module, "COVERAGE_PATH", str(cov))

    d = client.get("/api/dashboard").json()
    assert d["health"] not in ("DEGRADED", "HEALTHY_WITH_LEGACY_GAPS")
    assert d["health"] == "HEALTHY"


# ------------------------------------------------- job-state-semantiek
async def test_job_state_semantics_success_deferred_interrupted(settings):
    store = Store(settings.db_path)
    r1 = await store.job_start("health_sweeper", progress_total=10)
    await store.job_finish(r1, "SUCCESS", processed=10, recovered=1,
                           progress_current=10)
    r2 = await store.job_start("health_sweeper", progress_total=10)
    await store.job_finish(r2, "DEFERRED", reason="playback_active", processed=3)
    r3 = await store.job_start("health_sweeper", progress_total=10)
    # proces 'crasht' — r3 blijft RUNNING met finished_at NULL

    store2 = Store(settings.db_path)   # nieuwe start: reconcile RUNNING→INTERRUPTED
    rows = {r["id"]: r for r in
            await store2.job_runs(job_type="health_sweeper", limit=10)}
    assert rows[r1]["status"] == "SUCCESS" and rows[r1]["processed"] == 10
    assert rows[r1]["finished_at"] > rows[r1]["started_at"]
    assert rows[r2]["status"] == "DEFERRED" and rows[r2]["processed"] == 3
    assert rows[r3]["status"] == "INTERRUPTED"


async def test_sweep_cycles_cannot_overlap(tmp_path):
    """Scheduler start nooit een tweede sweep terwijl de vorige actief is
    (asyncio-lock serialiseert backgroundloop en handmatige checks)."""
    async def empty_items():
        return []

    fake_resolver = type("R", (), {})()
    fake_resolver.store = type("S", (), {})()
    fake_resolver.store.list_items = staticmethod(empty_items)
    sweeper = HealthSweeper(fake_resolver, str(tmp_path / "health.db"),
                            items_per_hour=10, playback_pause=False)
    events: list[str] = []

    async def slow_locked():
        events.append("in")
        await asyncio.sleep(0.05)
        events.append("out")

    sweeper._sweep_locked = slow_locked
    await asyncio.gather(sweeper.sweep(), sweeper.sweep())
    assert events == ["in", "out", "in", "out"]


# -------------------------------------------------- run history durations
async def test_run_history_exposes_duration_fields(settings, scorer, got_item):
    client, engine = _client_engine(settings, scorer)
    await engine.register_item(dict(got_item))
    store = engine.store
    run_id = await store.job_start("health_sweeper", progress_total=50)
    await store.job_finish(run_id, "SUCCESS", processed=49, recovered=0,
                           progress_current=49)
    time.sleep(0.01)
    hist = client.get("/api/jobs").json()["history"]
    row = next(h for h in hist if h["id"] == run_id)
    assert row["started_at"] > 0 and row["finished_at"] >= row["started_at"]
    assert row["processed"] == 49 and row["recovered"] == 0
    assert row["status"] == "SUCCESS"
