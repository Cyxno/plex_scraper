"""Regressie: gerichte Plex-metadata-revalidatie na bron-generatiewissel.

Bewijslast (incident 2026-10-09, Dark Matter S02E03): een stabiel Plex-pad
kan na een generatiewissel naar nieuwe bytes wijzen terwijl Plex stale
media_streams behoudt → ongeldige transcoder-mapping ("Invalid decoder
type"). Deze suite dekt het trigger-contract, de state machine, dedupe/
latest-generation-wins, verificatie en playback-guard.
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.common.domain import models as m
from plex_scraper.resolver.plex_revalidation import (
    FAILED, QUEUED, RUNNING, SUCCEEDED,
    COHERENT, REVALIDATING, STALE,
    PlexRevalidator, material_change,
)


class Settings:
    plex_revalidation_enabled = True
    plex_revalidation_cooldown_s = 120.0
    plex_revalidation_attempts = 3
    plex_revalidation_size_rel_tol = 0.02
    plex_revalidation_min_size_delta_mb = 50.0
    plex_revalidation_duration_rel_tol = 0.05
    plex_revalidation_open_wait_s = 0.2


class FakeSource(m.Source):
    @classmethod
    def make(cls, size=500 * 10**6, resolution="1080p", codec="h264",
             audio="ddp", hdr="sdr", generation=1, state="active"):
        return cls(id="s" + str(size) + resolution + codec + str(generation),
                   media_item_id="item1", generation=generation,
                   provider="torbox", info_hash="h" * 40,
                   torrent_name="t", size=size, resolution=resolution,
                   codec=codec, audio=audio, hdr=hdr, state=state)


class FakeItem(m.MediaItem):
    def __init__(self, generation=2):
        super().__init__(id="item1", kind="episode", title="E3",
                         plex_path="/x/file.mkv", series="Show",
                         season=2, episode=3, generation=generation,
                         status="READY", duration_s=2580.0)


class FakeResolver:
    def __init__(self, item):
        self.store = FakeStore(item)


class FakeStore:
    def __init__(self, item):
        self.item = item

    async def get_item(self, item_id):
        return self.item

    async def list_sources(self, item_id):
        return [self.item._active]


class FakePlex:
    """Recordt calls; opgelopen latency/mislukking per test instelbaar."""

    def __init__(self, info=None, fail_analyze=0, no_rating_key=False):
        self.info = info or self._default_info()
        self.analyze_calls = 0
        self.fail_analyze = fail_analyze
        self.no_rating_key = no_rating_key
        self.lookups = 0

    @staticmethod
    def _default_info():
        return {"ok": True, "container": "mkv", "video_codec": "hevc",
                "audio_codec": "eac3", "width": 1920, "height": 1080,
                "duration_s": 2580.0, "size": 937 * 10**6,
                "streams": [{"index": 0, "type": "1", "codec": "hevc"},
                            {"index": 1, "type": "2", "codec": "eac3"}]}

    async def find_rating_key_by_path(self, path):
        self.lookups += 1
        return None if self.no_rating_key else 9555

    async def analyze_item(self, rk):
        self.analyze_calls += 1
        if self.analyze_calls <= self.fail_analyze:
            return {"error": "plex unavailable", "exit_code": 1}
        return {"status": 200}

    async def get_media_info(self, rk):
        return dict(self.info)



def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

def make_reval(item, plex, settings=None, active=None):
    res = FakeResolver(item)
    item._active = active or FakeSource.make(size=937 * 10**6)
    r = PlexRevalidator(res, plex, settings or Settings())
    return r, res, plex


# ------------------------------------------------------- trigger-contract

def test_material_change_size():
    old = FakeSource.make(size=500 * 10**6)
    new = FakeSource.make(size=937 * 10**6, resolution="1080p")
    reasons = material_change(old, new)
    assert any(r.startswith("size") for r in reasons)


def test_no_material_change_identical_characteristics():
    old = FakeSource.make()
    new = FakeSource.make(state="candidate")     # zelfde size/res/audio/hdr
    assert material_change(old, new) == []


def test_material_change_audio_and_resolution():
    old = FakeSource.make(audio="aac", resolution="1080p")
    new = FakeSource.make(audio="ddp", resolution="2160p")
    reasons = material_change(old, new)
    assert any("audio" in r for r in reasons)
    assert any("resolution" in r for r in reasons)


def test_small_size_delta_not_material():
    old = FakeSource.make(size=500 * 10**6)
    new = FakeSource.make(size=505 * 10**6)      # 1% < 2% tol, < 50 MB-drempel
    assert material_change(old, new) == []


def test_hdr_change_is_material():
    old = FakeSource.make(hdr="sdr")
    new = FakeSource.make(hdr="dolby_vision")
    assert any("hdr" in r for r in material_change(old, new))


# --------------------------------------------------------- state machine

def test_swap_queues_revalidation_and_runs_to_success():
    item = FakeItem(generation=2)
    plex = FakePlex()
    r, _, _ = make_reval(item, plex)
    old = FakeSource.make(codec="h264", generation=1)
    reasons = r.on_swap(item, old, item._active)
    assert reasons, "h264→hevc-genwissel met andere size moet triggeren"
    job = r._jobs[item.id]
    assert job.state == QUEUED
    _run(r._process(job))
    assert job.state == SUCCEEDED
    assert plex.analyze_calls == 1
    assert job.last_result["coherent"] is True
    assert job.last_result["rating_key"] == 9555


def test_no_queue_without_material_change():
    item = FakeItem()
    plex = FakePlex()
    active = FakeSource.make(size=937 * 10**6)
    r, _, _ = make_reval(item, plex, active=active)
    old = FakeSource.make(size=937 * 10**6)
    assert r.on_swap(item, old, item._active) == []
    assert item.id not in r._jobs


def test_revalidation_failure_flags_stale_no_false_success():
    item = FakeItem()
    plex = FakePlex(info={  # Plex blijft h264 melden → mismatch → FAILED
        "ok": True, "container": "mkv", "video_codec": "h264",
        "audio_codec": "eac3", "width": 1280, "height": 720,
        "duration_s": 2580.0, "size": 500 * 10**6, "streams": []})
    r, _, _ = make_reval(item, plex)
    old = FakeSource.make(codec="h264")
    r.on_swap(item, old, item._active)
    job = r._jobs[item.id]
    for _ in range(Settings.plex_revalidation_attempts):
        _run(r._process(job))
    assert job.state == FAILED
    assert job.last_result["mismatches"], "harde mismatch mag nooit succes zijn"
    assert r.snapshot(item.id)["state"] == STALE


def test_plex_unavailable_retries_bounded_then_flags():
    item = FakeItem()
    plex = FakePlex(fail_analyze=99)
    r, _, _ = make_reval(item, plex)
    old = FakeSource.make()
    r.on_swap(item, old, item._active)
    job = r._jobs[item.id]
    for _ in range(Settings.plex_revalidation_attempts):
        _run(r._process(job))
    assert job.state == FAILED
    assert job.attempts == Settings.plex_revalidation_attempts
    assert r.snapshot(item.id)["state"] == STALE


def test_rating_key_not_found_is_failure_not_success():
    item = FakeItem()
    plex = FakePlex(no_rating_key=True)
    r, _, _ = make_reval(item, plex)
    r.on_swap(item, FakeSource.make(), item._active)
    job = r._jobs[item.id]
    for _ in range(Settings.plex_revalidation_attempts):
        _run(r._process(job))
    assert job.state == FAILED
    assert job.last_result["error"] == "rating_key_unresolved"
    assert job.last_result.get("deferred") is True


# ------------------------------------------------------- dedupe/throttle

def test_rapid_gen_swaps_dedupe_latest_generation_wins():
    item = FakeItem(generation=1)
    plex = FakePlex()
    r, _, _ = make_reval(item, plex)
    gen1 = FakeSource.make(size=400 * 10**6, generation=1)
    gen2 = FakeSource.make(size=600 * 10**6, generation=2)
    gen3 = FakeSource.make(size=900 * 10**6, generation=3)
    r.on_swap(item, gen1, gen2)
    r.on_swap(item, gen2, gen3)
    jobs = [j for j in r._jobs.values()]
    assert len(jobs) == 1, "gen1→gen2→gen3 moet coalescen tot één job"
    assert jobs[0].target_generation == 3
    assert r.metrics["deduped"] == 1


def test_stale_generation_job_follows_item_generation():
    item = FakeItem(generation=3)
    plex = FakePlex()
    r, _, _ = make_reval(item, plex)
    r.on_swap(item, FakeSource.make(generation=1), item._active)
    job = r._jobs[item.id]
    _run(r._process(job))
    assert job.state == SUCCEEDED


def test_cooldown_skips_second_revalidation():
    item = FakeItem()
    plex = FakePlex()
    r, _, _ = make_reval(item, plex)
    old, new = FakeSource.make(size=100 * 10**6), FakeSource.make(size=800 * 10**6)
    r.on_swap(item, old, new)
    job = r._jobs[item.id]
    _run(r._process(job))
    r.on_swap(item, old, new)                    # binnen cooldown
    assert r.metrics["cooldown_skipped"] == 1
    assert plex.analyze_calls == 1


# ----------------------------------------------------- playback-guard

def test_playback_guard_waits_for_running_job():
    item = FakeItem()
    plex = FakePlex()
    r, _, _ = make_reval(item, plex)
    r.on_swap(item, FakeSource.make(), item._active)
    job = r._jobs[item.id]
    job.state = RUNNING

    async def finish_soon():
        await asyncio.sleep(0.05)
        job.state = SUCCEEDED

    async def scenario():
        waiter = asyncio.ensure_future(r.wait_for_coherent(item.id, 2.0))
        asyncio.ensure_future(finish_soon())
        return await waiter

    out = _run(scenario())
    assert out["waited"] is True
    assert out["state"] == SUCCEEDED


def test_playback_guard_times_out_without_job():
    item = FakeItem()
    plex = FakePlex()
    r, _, _ = make_reval(item, plex)
    out = _run(
        r.wait_for_coherent(item.id, 0.1))
    assert out["waited"] is False


# ----------------------------------------------------- observability

def test_snapshot_states():
    item = FakeItem()
    plex = FakePlex()
    r, _, _ = make_reval(item, plex)
    assert r.snapshot(item.id)["state"] == COHERENT
    r.on_swap(item, FakeSource.make(), item._active)
    assert r.snapshot(item.id)["state"] == REVALIDATING
    job = r._jobs[item.id]
    _run(r._process(job))
    assert r.snapshot(item.id)["state"] == COHERENT
