"""Tests voor de diagnostics GUI polish-pass (Fase 1-5)."""
import json
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def _row(rk="1234", status="DONE", title="Test Item", err=None, fc=None,
         rid="r-1", symlink="/tmp/niet-bestaand.mkv", attempts=1,
         plex_path="Movies/Test/Test.mkv", gen=1, gp=None, season=None,
         episode=None, year=None, kind="movie", **extra):
    row = {"rk": rk, "plex_path_rel": plex_path, "kind": kind, "title": title,
           "symlink_host": symlink, "status": status, "attempts": attempts,
           "last_error": err, "fail_class": fc, "resolver_item_id": rid,
           "generation": gen, "gp": gp, "season": season, "episode": episode,
           "year": year}
    row.update(extra)
    return row


@pytest.fixture
def web_env(tmp_path):
    """Maakt test-DB + TestClient met onbereikbare resolver (unit-isolated)."""
    path = str(tmp_path / "state.sqlite")

    def _m(rows):
        c = sqlite3.connect(path)
        c.execute("CREATE TABLE IF NOT EXISTS queue (rk TEXT, plex_path_rel TEXT, "
                  "kind TEXT, title TEXT, gp TEXT, season INT, episode INT, "
                  "year INT, imdb TEXT, tmdb INT, tvdb INT, confidence TEXT, "
                  "symlink_host TEXT, watched_count TEXT, watched_offset TEXT, "
                  "status TEXT, attempts INT, last_error TEXT, fail_class TEXT, "
                  "resolver_item_id TEXT, resolution TEXT, size_gb REAL, "
                  "symlink_ok INT, verified INT, generation INT, desired TEXT, "
                  "created_at REAL, updated_at REAL)")
        c.execute("DELETE FROM queue")
        for row in rows:
            c.execute("INSERT INTO queue VALUES (" + ",".join(["?"] * 28) + ")",
                      tuple(row.get(k) for k in (
                          "rk", "plex_path_rel", "kind", "title", "gp",
                          "season", "episode", "year", "imdb", "tmdb", "tvdb",
                          "confidence", "symlink_host", "watched_count",
                          "watched_offset", "status", "attempts", "last_error",
                          "fail_class", "resolver_item_id", "resolution",
                          "size_gb", "symlink_ok", "verified", "generation",
                          "desired", "created_at", "updated_at")))
        c.commit()
        c.close()
        from plex_scraper.common.config import Settings
        from plex_scraper.web.app import create_web_app
        settings = Settings(resolver_url="http://resolver.invalid:1",
                            mig_db_path=path)
        return TestClient(create_web_app(settings))

    return _m


# ------------------------------------------------- failure-classificatie
class TestFailureClassification:
    def test_no_source(self, web_env):
        cl = web_env([_row(rk="100", err="bestaand item NO_SOURCE",
                           fc="NO_SOURCE", status="FAILED_RETRYABLE",
                           gp="TestShow", season=1, episode=1,
                           plex_path="TV/TestShow/S01/TestShow.S01E01.mkv")])
        d = cl.get("/api/summary").json()
        assert "NO_SOURCE" in d["failure_groups"], d.get("failure_groups")

    def test_backend_400(self, web_env):
        cl = web_env([_row(rk="101", err="torbox HTTP 400: BOZO",
                           fc="CANDIDATE", status="FAILED_RETRYABLE",
                           gp="TestShow", season=1, episode=1,
                           plex_path="TV/TestShow/S01/TestShow.S01E01.mkv")])
        d = cl.get("/api/summary").json()
        assert any("400" in k or "BACKEND" in k
                   for k in d["failure_groups"]), d["failure_groups"]

    def test_plex_edition_conflict(self, web_env):
        cl = web_env([_row(rk="102", err="edition conflict stale part",
                           fc="PLEX_VERIFY", status="FAILED_FINAL",
                           gp="TestShow", season=1, episode=1,
                           plex_path="TV/TestShow/S01/TestShow.S01E01.mkv")])
        d = cl.get("/api/summary").json()
        cats = list(d["failure_groups"])
        assert any("EDITION" in k or "CONFLICT" in k for k in cats), cats

    def test_transient_timeout(self, web_env):
        cl = web_env([_row(rk="103", err="TimeoutError timed out",
                           fc="PLEX_VERIFY", status="FAILED_FINAL",
                           gp="TestShow", season=1, episode=1,
                           plex_path="TV/TestShow/S01/TestShow.S01E01.mkv")])
        d = cl.get("/api/summary").json()
        assert any("TIMEOUT" in k or "TRANSIENT" in k
                   for k in d["failure_groups"]), d["failure_groups"]


# ------------------------------------------------- trace first-fail
class TestTraceFirstFailure:
    def test_first_fail_marked(self, web_env, tmp_path):
        import os
        target = str(tmp_path / "empty-dir")
        os.makedirs(target, exist_ok=True)
        link = str(tmp_path / "link.mkv")
        os.symlink(os.path.join(target, "niets.mkv"), link)
        cl = web_env([_row(rk="999", status="READY", rid="r-9", symlink=link)])
        d = cl.get("/api/trace/999").json()
        assert d.get("first_fail") == "resolver API", d.get("first_fail")
        steps = [s["step"] for s in d.get("steps", [])]
        assert "symlink" in steps and "VFS/debrid target" in steps

    def test_trace_unknown_rk(self, web_env):
        cl = web_env([_row()])
        d = cl.get("/api/trace/9999").json()
        assert "error" in d or d.get("first_fail")


# ------------------------------------------------- safe actions
class TestSafeActions:
    def test_readtest_unknown_rk(self, web_env):
        cl = web_env([_row(rk="1")])
        r = cl.post("/api/action/readtest/9999").json()
        assert r["result"] == "fout"

    def test_resolve_unknown_rk(self, web_env):
        cl = web_env([_row(rk="1", rid=None)])
        r = cl.post("/api/action/resolve/9999").json()
        assert "error" in r


# ------------------------------------------------- GUI pages
class TestGUIPages:
    def test_summary_page_html(self, web_env):
        cl = web_env([_row()])
        r = cl.get("/")
        assert r.status_code == 200
        assert "plex_scraper cockpit" in r.text
        assert "--bg" in r.text

    def test_summary_migration_history(self, web_env):
        cl = web_env([_row(status="DONE"), _row(rk="2", status="NO_SOURCE")])
        d = cl.get("/api/summary").json()
        assert d["migration_history"].get("DONE") == 1
        assert d["migration_history"].get("NO_SOURCE") == 1

    def test_verify_presentation(self, web_env):
        cl = web_env([_row(status="DONE", verified=1)])
        d = cl.get("/api/summary").json()
        assert "resolver_health" in d or "migration_history" in d
