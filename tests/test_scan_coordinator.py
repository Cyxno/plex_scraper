"""PlexScanCoordinator-regressietests (incident MobLand S02E04, 2026-10-09).

Bewezen keten: ingest OK, item READY, canonical .ids compleet, symlink OK,
plex-container kan lezen — maar de library-scan fire-and-forget faalde stil
(docker-exec {"error": ...} is géén exception) en Plex kende de episode niet.
"""
import asyncio
import inspect
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.ingest.scan_coordinator import (                         # noqa: E402
    PlexScanCoordinator, QUEUED, TRIGGERED, SUCCEEDED, FAILED)


class FakePlex:
    def __init__(self, fail_times=0, fail_result=None):
        self.calls: list[tuple] = []
        self.fail_times = fail_times
        self.fail_result = fail_result or {"error": "unparsable plex-exec output"}

    async def scan_section(self, section, path=None):
        self.calls.append((section, path))
        if self.fail_times > 0:
            self.fail_times -= 1
            return dict(self.fail_result)
        return {"scanned": section, "status": 200}


class Recorder:
    def __init__(self):
        self.events: list[tuple] = []

    def __call__(self, kind, **kw):
        self.events.append((kind, kw))

    def kinds(self):
        return [k for k, _e in self.events]


def _coord(plex, **kw):
    rec = Recorder()
    c = PlexScanCoordinator(plex, store_event=rec, **kw)
    return c, rec


async def _wait_flush(coord, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        t, w = coord._timer, coord._worker
        if t is not None:
            try:
                await t
            except asyncio.CancelledError:
                pass
            continue
        if w is not None:
            try:
                await w
            except asyncio.CancelledError:
                pass
            break
        await asyncio.sleep(0.005)


def _run(coord, coro_factory, timeout=5.0):
    """request(s) + flush in ÉÉN event loop (een timer-task overleeft geen
    loop-sluiting)."""

    async def go():
        await coro_factory()
        await _wait_flush(coord, timeout)
    asyncio.run(go())


def _mklink(tmp_path, name="link.mkv"):
    link = tmp_path / name
    link.write_text("x")
    return str(link)


def test_01_tv_ingest_tv_section_refresh(tmp_path):
    """(1) TV-ingest → TV-section, targeted path-scan."""
    plex = FakePlex()
    c, rec = _coord(plex, debounce_s=0.01)
    link = _mklink(tmp_path)

    async def go():
        await c.request(section=2, path="/symlinks/TV Shows/X/Season 2",
                        item_id="i1", kind="episode", link_path=link)
    _run(c, go)
    assert plex.calls == [(2, "/symlinks/TV Shows/X/Season 2")]
    assert rec.kinds()[:3] == [QUEUED, TRIGGERED, SUCCEEDED]


def test_02_movie_ingest_movie_section_refresh(tmp_path):
    """(2) Movie-ingest → movie-section."""
    plex = FakePlex()
    c, _rec = _coord(plex, debounce_s=0.01)
    link = _mklink(tmp_path)

    async def go():
        await c.request(section=1, path="/symlinks/Movies/X",
                        item_id="i2", kind="movie", link_path=link)
    _run(c, go)
    assert plex.calls[0][0] == 1


def test_03_verkeerde_section_nooit_getriggerd(tmp_path):
    """(3) episode scant nooit de movie-section."""
    plex = FakePlex()
    c, _rec = _coord(plex, debounce_s=0.01)
    link = _mklink(tmp_path)

    async def go():
        await c.request(section=2, path="/symlinks/TV Shows/X/Season 1",
                        item_id="i3", kind="episode", link_path=link)
    _run(c, go)
    assert plex.calls and all(s == 2 for s, _p in plex.calls)


def test_04_meerdere_ingests_één_gededupliceerde_refresh(tmp_path):
    """(4) 3 TV-ingests binnen het debounce-venster → precies één refresh."""
    plex = FakePlex()
    c, rec = _coord(plex, debounce_s=0.05)

    async def go():
        for i in range(3):
            link = _mklink(tmp_path, f"e{i}.mkv")
            await c.request(section=2, path=f"/symlinks/TV Shows/S{i}",
                            item_id=f"i{i}", kind="episode", link_path=link)
    _run(c, go)
    assert len(plex.calls) == 1
    assert c.metrics["queued"] == 3 and c.metrics["triggered"] == 1
    assert plex.calls[0][1] is None            # batch → section-scope scan


def test_05_plex_offline_bounded_retry_geen_exception(tmp_path):
    """(5) Plex faalt → bounded retries, geen raise, failed-event."""
    plex = FakePlex(fail_times=99)
    c, rec = _coord(plex, debounce_s=0.01, retry_max=3, retry_backoff_s=0.01)
    link = _mklink(tmp_path)

    async def go():
        await c.request(section=2, path="/symlinks/TV Shows/X",
                        item_id="i4", kind="episode", link_path=link)
    _run(c, go)
    assert len(plex.calls) == 3                # bounded
    assert FAILED in rec.kinds() and SUCCEEDED not in rec.kinds()


def test_06_symlink_ontbreekt_geen_refresh(tmp_path):
    """(6) geen symlink → skipped, géén scan gepland."""
    plex = FakePlex()
    c, rec = _coord(plex, debounce_s=0.01)

    async def go():
        ok = await c.request(section=2, path="/symlinks/TV Shows/X",
                             item_id="i5", kind="episode",
                             link_path=str(tmp_path / "ontbreekt.mkv"))
        assert ok is False
    _run(c, go)
    assert "plex_scan_skipped" in rec.kinds()
    assert plex.calls == []


def test_07_refresh_raakt_item_status_niet():
    """(7) de coordinator muteert géén item-status — READY blijft READY,
    faal-token zoals NO_SOURCE komt er niet in voor (structurele guard)."""
    src = inspect.getsource(PlexScanCoordinator)
    assert "ItemStatus" not in src
    assert "NO_SOURCE" not in src
    assert "update_runtime" not in src


def test_08_refresh_failure_alleen_observability(tmp_path):
    """(8) scan-faal → plex_scan_failed-event met error; flush werpt niet."""
    plex = FakePlex(fail_times=99)
    c, rec = _coord(plex, debounce_s=0.01, retry_max=1, retry_backoff_s=0.01)
    link = _mklink(tmp_path)

    async def go():
        await c.request(section=1, path="/symlinks/Movies/Y",
                        item_id="i6", kind="movie", link_path=link)
    _run(c, go)                                # geen raise
    failed = [e for k, e in rec.events if k == FAILED][0]
    assert failed["error"] and failed["attempts"] == 1


def test_09_exec_error_dict_telt_als_faal():
    """De stil-faal-vorm uit het incident: _exec geeft {"error": ...} zonder
    status → de coordinator mag dat nóóit als succes zien."""
    plex = FakePlex(fail_times=1,
                    fail_result={"error": "unparsable plex-exec output: HTTP 401",
                                 "exit_code": 1})
    c, rec = _coord(plex, debounce_s=0.01, retry_max=2, retry_backoff_s=0.01)

    async def go():
        await c.request(section=2, path="/symlinks/TV Shows/Z")
    _run(c, go)
    assert len(plex.calls) == 2                # 1e poging faalde stil → retry
    assert SUCCEEDED in rec.kinds()


def test_10_scan_section_script_parseert_combined_argv():
    """Regressie (MobLand/SWAT stil-faal): _exec geeft 'section|path' als ÉÉN
    argv-waarde — het exec-script moet die zelf splitsen, geen argv[2]
    verwachten (die bestaat niet → IndexError → nooit een scan)."""
    import inspect
    from plex_scraper.ingest.plex_client import PlexExecClient
    src = inspect.getsource(PlexExecClient.scan_section)
    assert "split('|', 1)" in src
    assert "sys.argv[2]" not in src


class LookupPlex(FakePlex):
    def __init__(self, rating_key):
        super().__init__()
        self.rating_key = rating_key

    async def find_rating_key_via_db(self, part_file):
        return self.rating_key


def test_11_ratingkey_gevonden_na_scan_succes(tmp_path):
    """Natural-import observability: na plex_scan_succeeded wordt het
    Plex-item opgezocht en als plex_scan_ratingkey_found gerapporteerd."""
    plex = LookupPlex(10103)
    c, rec = _coord(plex, debounce_s=0.01)
    link = _mklink(tmp_path)

    async def go():
        await c.request(section=2, path="/symlinks/TV Shows/X/Season 2",
                        item_id="i9", kind="episode", link_path=link,
                        part_file="/symlinks/TV Shows/X/Season 2/e.mkv")
    _run(c, go)
    ev = [e for k, e in rec.events if k == "plex_scan_ratingkey_found"]
    assert ev and ev[0]["rating_key"] == 10103
    assert "Blank" not in (ev[0].get("error") or "")


def test_12_ratingkey_lookup_faal_is_alleen_observability(tmp_path):
    """Lookup-faal → event met error, scan blijft geslaagd, geen raise."""
    class Broken(LookupPlex):
        async def find_rating_key_via_db(self, part_file):
            raise RuntimeError("db locked")

    plex = Broken(10103)
    c, rec = _coord(plex, debounce_s=0.01)
    link = _mklink(tmp_path, "y.mkv")

    async def go():
        await c.request(section=2, path="/symlinks/TV Shows/Y",
                        item_id="i10", kind="episode", link_path=link,
                        part_file="/symlinks/TV Shows/Y/y.mkv")
    _run(c, go)
    assert SUCCEEDED in rec.kinds()
    ev = [e for k, e in rec.events if k == "plex_scan_ratingkey_found"][0]
    assert ev["rating_key"] is None and "db locked" in ev["error"]
