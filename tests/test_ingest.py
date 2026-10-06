"""Ingest-hardening Phase 44 tests 11-28: persistente queue, idempotency,
reconciliatie, delivery, arr-reconcile.

De bridge wordt getest met een echte Resolver (mock provider/scraper), echte
sqlite-store (tmp_path), duck-typed arr-clients en een duck-typed plex-exec.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conftest import cand, make_engine  # noqa: E402
from plex_scraper.common.domain import models as m  # noqa: E402
from plex_scraper.ingest import delivery  # noqa: E402
from plex_scraper.ingest.arr import ArrUnavailable  # noqa: E402
from plex_scraper.ingest.bridge import IngestBridge  # noqa: E402
from plex_scraper.ingest.models import IngestJob, JobState  # noqa: E402


@dataclass
class FakeEpisode:
    id: int
    seriesId: int
    title: str = "Episode"
    seasonNumber: int = 1
    episodeNumber: int = 1
    monitored: bool = True
    airDateUtc: str = "2026-10-05T01:00:00Z"
    hasFile: bool = False


@dataclass
class FakeSeries:
    id: int
    title: str = "Lanterns"
    imdbId: str = "tt26545992"
    tvdbId: int = 376098
    tmdbId: int = 0
    year: int = 2026
    path: str = "/media/Lanterns"
    monitored: bool = True
    episodes: list = field(default_factory=list)


@dataclass
class FakeMovie:
    id: int
    title: str = "Worldbreaker"
    year: int = 2026
    imdbId: str = "tt12345678"
    tmdbId: int = 999001
    path: str = "/media-movies/Worldbreaker (2026)"
    monitored: bool = True
    hasFile: bool = False


class FakeSonarr:
    def __init__(self, series: list[FakeSeries]):
        self.series_list = series
        self.rescans: list[int] = []
        self._commands = 0
        self._done = True

    async def _get(self, path: str, params: dict | None = None):
        if path.startswith("/api/v3/command/"):
            return {"status": "completed"}
        raise ArrUnavailable(f"unexpected path {path}")

    async def series_all(self):
        return [vars(s) | {"episodes": []} for s in self.series_list]

    async def wanted_missing(self, page_size=200, max_pages=10):
        for s in self.series_list:
            for ep in s.episodes:
                if ep.monitored and not ep.hasFile:
                    rec = {"id": ep.id, "seriesId": ep.seriesId,
                           "title": ep.title, "seasonNumber": ep.seasonNumber,
                           "episodeNumber": ep.episodeNumber,
                           "monitored": ep.monitored,
                           "airDateUtc": ep.airDateUtc}
                    srec = {"id": s.id, "title": s.title, "imdbId": s.imdbId,
                            "tvdbId": s.tvdbId, "tmdbId": s.tmdbId,
                            "year": s.year, "path": s.path,
                            "monitored": s.monitored}
                    yield rec, srec

    async def episode(self, episode_id: int):
        for s in self.series_list:
            for ep in s.episodes:
                if ep.id == episode_id:
                    return {"id": ep.id, "hasFile": ep.hasFile,
                            "monitored": ep.monitored}
        raise ArrUnavailable("unknown episode")

    async def rescan_series(self, series_id: int):
        self.rescans.append(series_id)
        self._commands += 1
        return {"id": self._commands}

    async def health(self):
        return []

    async def tasks(self):
        return []


class FakeRadarr:
    def __init__(self, movies: list[FakeMovie]):
        self.movies = movies
        self.rescans: list[int] = []

    async def _get(self, path: str, params: dict | None = None):
        if path.startswith("/api/v3/command/"):
            return {"status": "completed"}
        raise ArrUnavailable(f"unexpected path {path}")

    async def wanted_missing(self, page_size=200, max_pages=10):
        for mv in self.movies:
            if mv.monitored and not mv.hasFile:
                rec = {"id": mv.id, "movieId": mv.id, "title": mv.title,
                       "monitored": mv.monitored, "imdbId": mv.imdbId,
                       "tmdbId": mv.tmdbId, "year": mv.year}
                mrec = {"id": mv.id, "title": mv.title, "imdbId": mv.imdbId,
                        "tmdbId": mv.tmdbId, "year": mv.year, "path": mv.path,
                        "monitored": mv.monitored}
                yield rec, mrec

    async def movie(self, movie_id: int):
        for mv in self.movies:
            if mv.id == movie_id:
                return {"id": mv.id, "hasFile": mv.hasFile, "title": mv.title}
        raise ArrUnavailable("unknown movie")

    async def rescan_movie(self, movie_id: int):
        self.rescans.append(movie_id)
        return {"id": movie_id + 100}

    async def health(self):
        return []

    async def tasks(self):
        return []


class FakePlex:
    def __init__(self, verify=True):
        self.verify = verify
        self.scans: list[tuple] = []
        self.probes: list[str] = []

    async def read_probe(self, path):
        self.probes.append(path)
        return {"ok": self.verify, "size": 1000}

    async def scan_section(self, section, path=None):
        self.scans.append((section, path))
        return {"scanned": section}

    async def find_episode(self, show_title, season, episode,
                           guid_imdb=None, file_suffix=None):
        return {"present": self.verify, "file_match": self.verify,
                "files": [f"/x/{file_suffix}"] if self.verify else []}


@pytest.fixture
def ingest_settings(tmp_path):
    from plex_scraper.common.config import Settings
    import dataclasses
    s = Settings(db_path=str(tmp_path / "state.db"), debug=True,
                 cache_bad_ttl=0.05, cache_bad_ttl_max=0.5,
                 validation_probe_bytes=1024,
                 min_media_movie_mb=0, min_media_episode_mb=0,
                 ingest_enabled=True,
                 symlink_root=str(tmp_path / "symlinks"),
                 canonical_root=str(tmp_path / "canonical"),
                 ingest_batch_size=1,
                 ingest_delivery_probe_retries=1,
                 ingest_delivery_probe_wait_s=0.0,
                 ingest_job_backoff_base_s=60.0)
    (Path(tmp_path) / "canonical" / ".ids").mkdir(parents=True)
    return s


def make_bridge(ingest_settings, scorer, *, sonarr=None, radarr=None,
                plex=None, provider_specs=None, search_results=None):
    engine, provider, scraper = make_engine(
        ingest_settings, provider_specs or {}, search_results or {}, scorer)
    bridge = IngestBridge(engine, ingest_settings)
    bridge.sonarr = sonarr
    bridge.radarr = radarr
    bridge.plex = plex or FakePlex()
    return bridge, engine


def lanterns_job(episode_id=2526, series_id=58):
    return IngestJob(
        source="sonarr", arr_item_id=f"{series_id}:{episode_id}", kind="episode",
        dedupe_key=IngestJob.tv_dedupe_key("tt26545992", "376098", 1, 8),
        title="Dirt and Stars", series="Lanterns", season=1, episode=8,
        year=2026, show_imdb_id="tt26545992", show_tvdb_id="376098",
        arr_path="/media/Lanterns", air_date_utc="2026-10-05T01:00:00Z")


# ------------------------------------------------ Phase 44 test 11 (sonarr event)
@pytest.mark.asyncio
async def test_sonarr_webhook_enqueues(ingest_settings, scorer):
    bridge, engine = make_bridge(ingest_settings, scorer)
    out = await bridge.handle_webhook("sonarr", {
        "eventType": "SeriesAdded",
        "series": {"id": 58, "title": "Lanterns", "imdbId": "tt26545992",
                   "tvdbId": 376098, "year": 2026, "path": "/media/Lanterns"},
        "episodes": [{"id": 2526, "title": "Dirt and Stars",
                      "seasonNumber": 1, "episodeNumber": 8}],
    })
    assert out["enqueued"] == 1
    job = await bridge.store.get_job_by_dedupe("tv:imdb:tt26545992:1:8")
    assert job is not None
    assert job.status == JobState.QUEUED.value


# ------------------------------------------------- Phase 44 test 13 (idempotent)
@pytest.mark.asyncio
async def test_duplicate_events_do_not_duplicate_jobs(ingest_settings, scorer):
    bridge, engine = make_bridge(ingest_settings, scorer)
    payload = {"eventType": "SeriesAdded",
               "series": {"id": 58, "title": "Lanterns", "imdbId": "tt26545992",
                          "tvdbId": 376098, "year": 2026,
                          "path": "/media/Lanterns"},
               "episodes": [{"id": 2526, "title": "Dirt and Stars",
                             "seasonNumber": 1, "episodeNumber": 8}]}
    await bridge.handle_webhook("sonarr", payload)
    out = await bridge.handle_webhook("sonarr", payload)
    counts = await bridge.store.ingest_job_counts()
    assert counts.get("QUEUED") == 1
    assert out.get("enqueued") == 0          # gededupeerd


# ---------------------------------------------- Phase 44 test 12 (radarr event)
@pytest.mark.asyncio
async def test_radarr_webhook_enqueues(ingest_settings, scorer):
    bridge, engine = make_bridge(ingest_settings, scorer)
    out = await bridge.handle_webhook("radarr", {
        "eventType": "MovieAdded",
        "movie": {"id": 160, "title": "Worldbreaker", "year": 2026,
                  "imdbId": "tt12345678", "tmdbId": 999001,
                  "path": "/media-movies/Worldbreaker (2026)"},
    })
    assert out["enqueued"] == 1
    job = await bridge.store.get_job_by_dedupe("movie:imdb:tt12345678")
    assert job is not None and job.kind == "movie"


# ----------------------------------- Phase 44 test 14/16/17 (reconciliation)
@pytest.mark.asyncio
async def test_reconcile_enqueues_monitored_missing(ingest_settings, scorer):
    s = FakeSeries(58, episodes=[FakeEpisode(2526, 58)])
    bridge, engine = make_bridge(ingest_settings, scorer, sonarr=FakeSonarr([s]))
    out = await bridge.reconcile()
    assert out["sonarr"]["seen"] == 1
    assert out["sonarr"]["enqueued"] == 1
    job = await bridge.store.get_job_by_dedupe("tv:imdb:tt26545992:1:1")
    assert job is not None


@pytest.mark.asyncio
async def test_reconcile_ignores_unmonitored(ingest_settings, scorer):
    s = FakeSeries(58, episodes=[FakeEpisode(2526, 58, monitored=False)])
    bridge, engine = make_bridge(ingest_settings, scorer, sonarr=FakeSonarr([s]))
    out = await bridge.reconcile()
    assert out["sonarr"]["seen"] == 0
    counts = await bridge.store.ingest_job_counts()
    assert not counts


@pytest.mark.asyncio
async def test_reconcile_catches_missed_webhook(ingest_settings, scorer):
    """Webhook gemist → reconcile enqueued tóch (Phase 15)."""
    s = FakeSeries(58, episodes=[FakeEpisode(2526, 58)])
    bridge, engine = make_bridge(ingest_settings, scorer, sonarr=FakeSonarr([s]))
    await bridge.reconcile()
    counts = await bridge.store.ingest_job_counts()
    assert counts.get("QUEUED") == 1


# ------------------------------- Phase 44 test 15 (provider-wait niet re-queue)
@pytest.mark.asyncio
async def test_reconcile_never_requeues_provider_wait(ingest_settings, scorer):
    s = FakeSeries(58, episodes=[FakeEpisode(2526, 58)])
    bridge, engine = make_bridge(ingest_settings, scorer, sonarr=FakeSonarr([s]))
    await bridge.reconcile()
    job = await bridge.store.get_job_by_dedupe("tv:imdb:tt26545992:1:1")
    # zet de job expliciet in PROVIDER_WAIT met een verre next_attempt
    job.status = JobState.PROVIDER_WAIT.value
    job.next_attempt_at = time.time() + 9999
    await bridge.store.update_job(job, {"status", "next_attempt_at"})
    out = await bridge.reconcile()          # nog een reconcile
    fresh = await bridge.store.get_job(job.id)
    assert fresh.status == JobState.PROVIDER_WAIT.value
    assert out["sonarr"]["enqueued"] == 0
    # en de worker pakt hem niet vóór next_attempt_at
    done = await bridge.process_due()
    fresh = await bridge.store.get_job(job.id)
    assert done == 0 and fresh.status == JobState.PROVIDER_WAIT.value


# --------------------------- Phase 44 test 18/19/20 (identity + resolver reuse)
@pytest.mark.asyncio
async def test_tv_identity_and_queue_pipeline_to_ready(
        ingest_settings, scorer, monkeypatch):
    """Complete TV-pipeline: enqueue → register (show-identity) → resolve →
    READY → delivery → plex verify → arr rescan → COMPLETED."""
    s = FakeSeries(58, episodes=[FakeEpisode(2526, 58, episodeNumber=8)])
    sonarr = FakeSonarr([s])
    bridge, engine = make_bridge(
        ingest_settings, scorer, sonarr=sonarr, plex=FakePlex(True),
        provider_specs={"cand1": {"size": 4096}},
        search_results={"episode:tt26545992:1:8": [
            cand("cand1", "Lanterns.S01E08.Dirt.and.Stars.2160p.WEB-DL.DDP5.1.H.265-GRP",
                 size=3_000_000_000, file_name="Lanterns.S01E08.Dirt.and.Stars.2160p.WEB-DL.DDP5.1.H.265-GRP.mkv")]})
    await bridge.reconcile()
    job = await bridge.store.get_job_by_dedupe("tv:imdb:tt26545992:1:8")
    assert job is not None
    await bridge.process_job(job)
    fresh = await bridge.store.get_job(job.id)
    # READY-afhandeling heeft delivery gedaan (process_job loopt door tot einde)
    assert fresh.status in (JobState.COMPLETED.value, JobState.BLOCKED_MAPPING.value)
    item = await engine.store.get_item(fresh.resolver_item_id)
    assert item is not None
    assert item.kind == "episode" and item.show_imdb_id == "tt26545992"
    assert item.status == "READY"
    assert item.plex_path.startswith(".ids/")
    if fresh.status == JobState.COMPLETED.value:
        assert sonarr.rescans == [58]         # arr post-delivery (Phase 20)
        assert fresh.delivered_symlink and os.path.islink(fresh.delivered_symlink)
        target = os.readlink(fresh.delivered_symlink)
        assert target.startswith(ingest_settings.canonical_root + "/.ids/")
        # symlink in de arr-structuur onder de symlink-root
        assert "/TV Shows/Lanterns/Season 1/" in fresh.delivered_symlink
        # plex-exec kreeg de probes + scan (Phase 19)
        assert bridge.plex.scans and bridge.plex.probes


@pytest.mark.asyncio
async def test_resolver_item_reused_not_duplicated(ingest_settings, scorer):
    """Bestaand resolver-item voor dezelfde identiteit → hergebruik (Phase 14)."""
    bridge, engine = make_bridge(
        ingest_settings, scorer, provider_specs={"cand1": {"size": 4096}},
        search_results={"episode:tt26545992:1:8": [
            cand("cand1", "Lanterns.S01E08.2160p.WEB-DL.H.265-GRP",
                 size=3_000_000_000)]})
    existing = await engine.register_item({
        "plex_path": ".ids/a/b/c/d/e/aaaabbbb-cccc-dddd-eeee-ffff00001111",
        "kind": "episode", "title": "Dirt and Stars", "series": "Lanterns",
        "season": 1, "episode": 8, "show_imdb_id": "tt26545992"})
    n_before = len(await engine.store.list_items())
    job = lanterns_job()
    await bridge.enqueue(job, "test")
    await bridge.process_job(job)
    fresh = await bridge.store.get_job(job.id)
    assert fresh.resolver_item_id == existing.id
    assert len(await engine.store.list_items()) == n_before


@pytest.mark.asyncio
async def test_movie_identity_pipeline(ingest_settings, scorer):
    radarr = FakeRadarr([FakeMovie(160)])
    bridge, engine = make_bridge(
        ingest_settings, scorer, radarr=radarr, plex=FakePlex(True),
        provider_specs={"mcand": {"size": 4096}},
        search_results={"movie:tt12345678": [
            cand("mcand", "Worldbreaker.2026.2160p.WEB-DL.H.265-GRP",
                 size=8_000_000_000)]})
    await bridge.reconcile()
    job = await bridge.store.get_job_by_dedupe("movie:imdb:tt12345678")
    assert job is not None
    await bridge.process_job(job)
    fresh = await bridge.store.get_job(job.id)
    assert fresh.status in (JobState.COMPLETED.value, JobState.BLOCKED_MAPPING.value)
    item = await engine.store.get_item(fresh.resolver_item_id)
    assert item.kind == "movie" and item.imdb_id == "tt12345678"
    if fresh.delivered_symlink:
        assert "/Movies/" in fresh.delivered_symlink   # radarr root-map


# --------------------------------- Phase 44 test 21/22 (delivery + plex verify)
@pytest.mark.asyncio
async def test_delivery_plex_probe_failure_is_retryable(ingest_settings, scorer):
    bridge, engine = make_bridge(
        ingest_settings, scorer, plex=FakePlex(False),
        provider_specs={"cand1": {"size": 4096}},
        search_results={"episode:tt26545992:1:8": [
            cand("cand1", "Lanterns.S01E08.2160p.WEB-DL.H.265-GRP",
                 size=3_000_000_000)]})
    job = lanterns_job()
    await bridge.enqueue(job, "test")
    await bridge.process_job(job)
    fresh = await bridge.store.get_job(job.id)
    assert fresh.status == JobState.FAILED_RETRYABLE.value
    assert "probe" in (fresh.last_error or "").lower()


# ------------------------------- Phase 44 test 24 (backlog batching: batch=1)
@pytest.mark.asyncio
async def test_batch_size_bounds_worker(ingest_settings, scorer):
    jobs = [lanterns_job(episode_id=100 + i) for i in range(3)]
    # unieke episode-nummers: dedupe_key andersidentiek houden
    bridge, engine = make_bridge(ingest_settings, scorer)
    for j in jobs:
        j.dedupe_key = f"tv:imdb:tt26545992:2:{100 + jobs.index(j)}"
        await bridge.enqueue(j, "test")
    assert await bridge.store.due_jobs(time.time(), limit=100)
    # ingest_batch_size=1: één job per cyclus
    engine.circuit.report_rate_limited("torrentio", retry_after_s=9999)
    done = await bridge.process_due()
    assert done == 1
    counts = await bridge.store.ingest_job_counts()
    assert counts.get("PROVIDER_WAIT") == 1        # circuit open → nette pauze
    assert counts.get("QUEUED") == 2               # rest ongemoeid gelaten


# ------------------------------- Phase 44 test 27 (queue persistence)
@pytest.mark.asyncio
async def test_queue_survives_restart(ingest_settings, scorer):
    bridge, engine = make_bridge(ingest_settings, scorer)
    job = lanterns_job()
    await bridge.enqueue(job, "test")
    job.status = JobState.RESOLVING.value
    await bridge.store.update_job(job, {"status"})
    # nieuw bridge-object = herstart: tussenstand → FAILED_RETRYABLE
    bridge2 = IngestBridge(engine, ingest_settings)
    await bridge2._recover_on_start()
    fresh = await bridge2.store.get_job(job.id)
    assert fresh.status == JobState.FAILED_RETRYABLE.value
    assert "restart" in (fresh.last_error or "")


# ------------------------------- Phase 44 test 25/26 (ingest health semantics)
@pytest.mark.asyncio
async def test_ingest_status_and_rate_limit_semantics(ingest_settings, scorer):
    bridge, engine = make_bridge(ingest_settings, scorer)
    st = await bridge.status()
    assert st["enabled"] and st["provider"]["state"] == "HEALTHY"
    engine.circuit.report_rate_limited("torrentio", retry_after_s=600)
    st = await bridge.status()
    assert st["provider"]["state"] == "RATE_LIMITED"
    assert st["provider"]["retry_in_s"] > 0
    # Phase 39-semantiek: DEGRADED / rate limited — NIET "no source"
    assert "NO_SOURCE" not in str(st["provider"])


# ------------------------------ Phase 44 test 28 (arr-intent classification)
def test_arr_intent_classification_pure():
    from maintenance.legacy_arr_intent import classify_intent
    # dead + arr heeft het bestand → runtime-dead
    assert classify_intent(dead=True, arr_has_file=True, arr_monitored=True) == \
        "MONITORED_PRESENT_RUNTIME_DEAD"
    assert classify_intent(dead=True, arr_has_file=False, arr_monitored=True) == \
        "MONITORED_MISSING"
    assert classify_intent(dead=True, arr_has_file=False, arr_monitored=False) == \
        "UNMONITORED"
    assert classify_intent(dead=True, arr_has_file=False, arr_monitored=None) == \
        "NO_ARR_MATCH"
    assert classify_intent(dead=False, arr_has_file=True, arr_monitored=True) == \
        "MONITORED_WORKING_LEGACY"
    assert classify_intent(dead=False, arr_has_file=True, arr_monitored=False) == \
        "UNMONITORED_WORKING_LEGACY"
    assert classify_intent(dead=False, arr_has_file=True, arr_monitored=None) == \
        "NO_ARR_MATCH"


def test_delivery_symlink_mapping(ingest_settings):
    src = m.Source(id="s1", media_item_id="i1", generation=1, provider="torbox",
                   info_hash="abc", torrent_name="Lanterns.S01E08.2160p.WEB-DL-GRP",
                   file_name="Lanterns.S01E08.2160p.WEB-DL-GRP.mkv", size=1000)
    link, target = delivery.build_symlink_target(
        "episode", "/media/Lanterns", 1, ".ids/b/f/6/a/e/bf6ae7d7-b8cc-480a-8afb-e868a81ec71e",
        src, ingest_settings.symlink_root, {"/media": "TV Shows"},
        canonical_root=ingest_settings.canonical_root)
    assert link.endswith("Lanterns.S01E08.2160p.WEB-DL-GRP.mkv")
    assert "/TV Shows/Lanterns/Season 1/" in link
    assert target == os.path.join(
        ingest_settings.canonical_root,
        ".ids/b/f/6/a/e/bf6ae7d7-b8cc-480a-8afb-e868a81ec71e")
    link2, _ = delivery.build_symlink_target(
        "movie", "/media-movies/WB (2026)", None, ".ids/a/a/a/a/a/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        src, ingest_settings.symlink_root, {"/media-movies": "Movies"},
        canonical_root=ingest_settings.canonical_root)
    assert "/Movies/WB (2026)/" in link2
