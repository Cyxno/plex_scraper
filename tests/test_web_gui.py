"""Tests voor de diagnostics GUI polish-pass (Fase 1-5)."""
import os
import sys
import time

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.dirname(__file__))


def _web_client(monkeypatch, tmp_db_rows, resolver_json):
    """Bouw web-app met test-db (sqlite-snapshot) + gemoctte resolver."""
    from plex_scraper.web.app import create_web_app
    from plex_scraper.common.config import Settings

    dbp = os.path.join(str(pytest.importorskip("pytest").config.rootdir), "tmp") \
        if False else None
    # db direct maken
    import sqlite3
    path = os.environ.get("WEB_TEST_DB")
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE IF NOT EXISTS queue (rk TEXT, plex_path_rel TEXT, "
              "kind TEXT, title TEXT, gp TEXT, season INT, episode INT, year INT, "
              "imdb TEXT, tmdb INT, tvdb INT, confidence TEXT, symlink_host TEXT, "
              "watched_count TEXT, watched_offset TEXT, status TEXT, attempts INT, "
              "last_error TEXT, fail_class TEXT, resolver_item_id TEXT, "
              "resolution TEXT, size_gb REAL, symlink_ok INT, verified INT, "
              "generation INT, desired TEXT, created_at REAL, updated_at REAL)")
    c.execute("DELETE FROM queue")
    for row in tmp_db_rows:
        c.execute("INSERT INTO queue VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  tuple(row.get(k) for k in (
                      "rk", "plex_path_rel", "kind", "title", "gp", "season",
                      "episode", "year", "imdb", "tmdb", "tvdb", "confidence",
                      "symlink_host", "watched_count", "watched_offset",
                      "status", "attempts", "last_error", "fail_class",
                      "resolver_item_id", "resolution", "size_gb", "symlink_ok",
                      "verified", "generation", "desired", "created_at",
                      "updated_at")))
    c.commit()
    c.close()

    settings = Settings(resolver_url="http://resolver.test:1",
                        mig_db_path=path)
    app = create_web_app(settings)
    return TestClient(app)


@pytest.fixture
def web_env(tmp_path, monkeypatch):
    import sqlite3
    path = str(tmp_path / "state.sqlite")

    def make(rows):
        c = sqlite3.connect(path)
        c.execute("CREATE TABLE IF NOT EXISTS queue (rk TEXT, plex_path_rel TEXT, "
                  "kind TEXT, title TEXT, gp TEXT, season INT, episode INT, year INT, "
                  "imdb TEXT, tmdb INT, tvdb INT, confidence TEXT, symlink_host TEXT, "
                  "watched_count TEXT, watched_offset TEXT, status TEXT, attempts INT, "
                  "last_error TEXT, fail_class TEXT, resolver_item_id TEXT, "
                  "resolution TEXT, size_gb REAL, symlink_ok INT, verified INT, "
                  "generation INT, desired TEXT, created_at REAL, updated_at REAL)")
        c.execute("DELETE FROM queue")
        for row in rows:
            c.execute("INSERT INTO queue VALUES (" + ",".join(["?"] * 28) + ")",
                      tuple(row.get(k) for k in (
                          "rk", "plex_path_rel", "kind", "title", "gp", "season",
                          "episode", "year", "imdb", "tmdb", "tvdb", "confidence",
                          "symlink_host", "watched_count", "watched_offset",
                          "status", "attempts", "last_error", "fail_class",
                          "resolver_item_id", "resolution", "size_gb", "symlink_ok",
                          "verified", "generation", "desired", "created_at",
                          "updated_at")))
        c.commit()
        c.close()

    def _m(rows):
        make(rows)
        from plex_scraper.common.config import Settings
        from plex_scraper.web.app import create_web_app
        settings = Settings(resolver_url="http://resolver.invalid:1",
                            mig_db_path=path)
        from fastapi.testclient import TestClient
        return TestClient(create_web_app(settings))

    return _m


def _row(rk="1234", status="DONE", title="Test Item", err=None, fc=None,
         rid="r-1", symlink="/tmp/niet-bestaand.mkv", attempts=1,
         plex_path="Movies/Test/Test.mkv", gen=1, **extra):
    row = {"rk": rk, "plex_path_rel": plex_path, "kind": "movie", "title": title,
           "symlink_host": symlink, "status": status, "attempts": attempts,
           "last_error": err, "fail_class": fc, "resolver_item_id": rid,
           "generation": gen}
    row.update(extra)
    return row


class TestFailureClassification:
    def test_no_source(self, web_env):
        cl = web_env([_row(err="bestaand item NO_SOURCE", fc="NO_SOURCE",
                           status="FAILED_RETRYABLE")])
        d = cl.get("/api/summary").json()
        assert "NO_SOURCE" in d["failure_groups"], d.get("failure_groups")

    def test_backend_400(self, web_env):
        cl = web_env([_row(err="torbox /torrents/createtorrent: HTTP 400: BOZO",
                           fc="CANDIDATE", status="FAILED_RETRYABLE")])
        d = cl.get("/api/summary").json()
        assert any("400" in k for k in d["failure_groups"])

    def test_plex_edition_conflict(self, web_env):
        cl = web_env([_row(err="geen werkende part (MDE kan dode editie geprefereerd)",
                           fc="PLEX_VERIFY", status="FAILED_FINAL")])
        d = cl.get("/api/summary").json()
        cats = list(d["failure_groups"])
        assert any("EDITION" in k or "dode part" in k for k in cats), cats

    def test_transient_timeout(self, web_env):
        cl = web_env([_row(err="TimeoutError('timed out')", fc="PLEX_VERIFY",
                           status="FAILED_FINAL")])
        d = cl.get("/api/summary").json()
        assert any("TRANSIENT" in k for k in d["failure_groups"])


class TestTraceFirstFailure:
    def test_first_fail_marked(self, web_env, tmp_path):
        """Eerste FAIL moet de eerste falende ketenstap zijn."""
        import os
        target = str(tmp_path / "target-dir-empty")
        os.makedirs(target, exist_ok=True)
        link = str(tmp_path / "link.mkv")
        os.symlink(os.path.join(target, "niets.mkv"), link)  # dode symlink
        cl = web_env([_row(rk="999", status="READY", rid="r-9", symlink=link)])
        d = cl.get("/api/trace/999").json()
        # resolver is onbereikbaar in de test -> 'resolver API' is terecht de
        # eerste FAIL; de lokale keten (symlink/VFS) moet nog wel doorlopen
        assert d.get("first_fail") == "resolver API", d.get("first_fail")
        steps = [s["step"] for s in d.get("steps", [])]
        assert "symlink" in steps and "VFS/debrid target" in steps
        vf = next(s for s in d["steps"] if s["step"] == "VFS/debrid target")
        assert vf["ok"] is False  # dode symlink

    def test_trace_unknown_rk(self, web_env):
        cl = web_env([_row()])
        d = cl.get("/api/trace/9999").json()
        # resolver onbereikbaar in test -> error-stap; geen 500
        assert "error" in d or d.get("first_fail")


import json  # noqa: E402


class TestSafeActions:
    def test_readtest_unknown_rk(self, web_env):
        cl = web_env([_row(rk="1")])
        r = cl.post("/api/action/readtest/9999").json()
        assert r["result"] == "fout"

    def test_resolve_unknown_rk(self, web_env):
        cl = web_env([_row(rk="1", rid=None)])
        r = cl.post("/api/action/resolve/9999").json()
        assert "error" in r


class TestGUIPages:
    def test_summary_page_html(self, web_env):
        cl = web_env([_row()])
        r = cl.get("/")
        assert r.status_code == 200
        assert "plex_scraper diagnostics" in r.text
        assert "--bg" in r.text  # dark mode css

    def test_summary_api_queue(self, web_env):
        cl = web_env([_row(status="DONE"), _row(rk="2", status="NO_SOURCE")])
        d = cl.get("/api/summary").json()
        assert d["queue"].get("DONE") == 1
        assert d["queue"].get("NO_SOURCE") == 1

    def test_verify_presentation(self, web_env):
        cl = web_env([_row(status="DONE", verified=1)])
        d = cl.get("/api/summary").json()
        assert "worker_verify" in d
