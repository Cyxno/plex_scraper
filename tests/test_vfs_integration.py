"""FASE 17 integration tests against a REAL FUSE mount.

Requires /dev/fuse + fusermount3; skips gracefully elsewhere (CI installs
fuse3, so these run in containers and on this Unraid host).

Proves: virtual file exists with honest size, sequential + random reads,
concurrent handles, source fail -> next candidate, stable logical path.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import threading
import time

import httpx
import pytest
import uvicorn

from plex_scraper.api.app import create_app
from plex_scraper.providers.mock import synthetic_bytes
from plex_scraper.vfs.fs import mount_main

from conftest import make_engine

FUSE_OK = os.path.exists("/dev/fuse") and shutil.which("fusermount3") is not None

ITEM = {
    "kind": "episode", "title": "Winter Is Coming", "series": "Game of Thrones",
    "season": 1, "episode": 1, "imdb_id": "tt0944947",
    "plex_path": "TV/Game of Thrones/Season 01/Game of Thrones - S01E01.mkv",
}
RESULTS_KEY = "episode:tt0944947:1:1"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _HttpServer:
    def __init__(self, app):
        self.port = _free_port()
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port,
                                log_level="error", access_log=False)
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        for _ in range(100):
            try:
                httpx.get(f"http://127.0.0.1:{self.port}/health", timeout=1)
                return
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("uvicorn did not start")

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=10)


class _Mount:
    def __init__(self, mountpoint: str, url: str):
        self.mountpoint = mountpoint
        self.thread = threading.Thread(
            target=lambda: mount_main(mountpoint, url, allow_other=False),
            daemon=True)
        self.thread.start()
        for _ in range(100):
            if os.path.ismount(mountpoint):
                return
            time.sleep(0.05)
        raise RuntimeError("mount did not appear")

    def stop(self):
        subprocess.run(["fusermount3", "-u", self.mountpoint],
                       check=False, capture_output=True)
        self.thread.join(timeout=10)


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    if not FUSE_OK:
        pytest.skip("no /dev/fuse or fusermount3 available")
    tmp = tmp_path_factory.mktemp("vfs")
    from plex_scraper.config import Settings
    specs = {"got2160dv": {"cached": True, "size": 8 << 20},
             "got1080": {"cached": True, "size": 6 << 20},
             "got720": {"cached": True, "size": 4 << 20}}
    settings = Settings(db_path=str(tmp / "state.db"), debug=True,
                        cache_bad_ttl=3600, stream_readahead_bytes=1 << 20,
                        min_media_movie_mb=0, min_media_episode_mb=0)
    from conftest import got_results
    engine, provider, _ = make_engine(settings, specs, {RESULTS_KEY: got_results()})
    http = _HttpServer(create_app(engine, settings))
    url = f"http://127.0.0.1:{http.port}"

    base = str(tmp / "mnt")
    os.makedirs(base)
    r = httpx.post(f"{url}/media", json=ITEM, timeout=60)
    assert r.status_code == 201, r.text
    item_id = r.json()["id"]

    mount = _Mount(base, url)
    yield {"mount": mount, "http": http, "url": url, "item_id": item_id,
           "engine": engine, "provider": provider, "settings": settings}
    mount.stop()
    http.stop()


def test_virtual_file_exists_with_honest_size(stack):
    path = stack["mount"].mountpoint + "/TV/Game of Thrones/Season 01/Game of Thrones - S01E01.mkv"
    assert os.path.exists(path)
    st = os.stat(path)
    assert st.st_size == 8 << 20


def test_sequential_and_random_reads(stack):
    path = stack["mount"].mountpoint + "/TV/Game of Thrones/Season 01/Game of Thrones - S01E01.mkv"
    expected = synthetic_bytes("got2160dv", 8 << 20)
    with open(path, "rb") as fh:
        assert fh.read(65536) == expected[:65536]
        fh.seek(5 << 20)
        assert fh.read(4096) == expected[5 << 20:(5 << 20) + 4096]
        fh.seek(-64, os.SEEK_END)
        assert fh.read(64) == expected[-64:]


def test_concurrent_read_handles(stack):
    path = stack["mount"].mountpoint + "/TV/Game of Thrones/Season 01/Game of Thrones - S01E01.mkv"
    expected = synthetic_bytes("got2160dv", 8 << 20)
    offsets = [0, 1 << 20, 3 << 20, 7 << 20]
    with open(path, "rb") as a, open(path, "rb") as b, open(path, "rb") as c:
        for off in offsets:
            a.seek(off)
            b.seek(off + 512)
            c.seek(off + 1024)
            assert a.read(512) == expected[off:off + 512]
            assert b.read(512) == expected[off + 512:off + 1024]
            assert c.read(512) == expected[off + 1024:off + 1536]


def test_source_fail_fallback_stable_path(stack):
    """THE test: open+pin, kill source, old handle keeps old bytes, new open
    gets the next generation behind the SAME path."""
    mount, engine, item_id = stack["mount"], stack["engine"], stack["item_id"]
    path = mount.mountpoint + "/TV/Game of Thrones/Season 01/Game of Thrones - S01E01.mkv"
    expected_old = synthetic_bytes("got2160dv", 8 << 20)

    # pin a session on generation 1 and prove its bytes
    fh_old = os.open(path, os.O_RDONLY)
    assert os.pread(fh_old, 1024, 4096) == expected_old[4096:5120]

    # injection: the active source dies
    assert httpx.post(f"{stack['url']}/debug/media/{item_id}/fail-current", timeout=30).status_code == 200

    # pinned handle still serves the OLD generation (no mid-playback switch)
    assert os.pread(fh_old, 1024, 8192) == expected_old[8192:9216]

    # NEW open -> next candidate, SAME path, new generation
    with open(path, "rb") as fh:
        data = fh.read(2048)
    expected_new = synthetic_bytes("got1080", 6 << 20)
    assert data == expected_new[:2048]
    item = httpx.get(f"{stack['url']}/media/{item_id}", timeout=30).json()
    assert item["generation"] == 2
    assert item["plex_path"] == ITEM["plex_path"]

    os.close(fh_old)

