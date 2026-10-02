"""TorBox adapter (FASE 4). Live API facts this implements (verified against
https://api-docs.torbox.app + OpenAPI on 2026-09-30):

  * Bearer auth; requestdl also accepts legacy `token` query param.
  * GET /torrents/checkcached?hash=h1,h2&format=list&list_files=true  (batch)
  * GET /torrents/mylist (client-side hash filter; list cache ~600s)
  * POST /torrents/createtorrent (magnet; 60/h uncached -> add at most one
    per resolution, only when nothing is cached)
  * GET /torrents/requestdl?torrent_id&file_id (valid ~3h; 307 permalink mode
    available but we refresh ourselves so we stay generation-safe)
  * Limits: 300 req/min global; we self-pace ~4 req/s + 1 req/s on requestdl.

Range requests against CDN links are de-facto standard (rclone/zurg depend on
them) but not documented; the resolver's validator probes 206 per source.
"""
from __future__ import annotations

import asyncio
import re
import time

import httpx

from plex_scraper.common.log import event
from .base import DebridProvider, LinkExpiredError, NotReadyError, ProviderError, ProviderTorrent

VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov", ".mpg", ".webm")


class TokenBucket:
    """Tiny async pacer: max `rate` events per `per_seconds`, burst 1."""

    def __init__(self, rate: float, per_seconds: float = 1.0):
        self.min_interval = per_seconds / rate
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            delay = self._last + self.min_interval - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._last = time.monotonic()


class TorboxProvider(DebridProvider):
    name = "torbox"

    def __init__(self, settings, client: httpx.AsyncClient | None = None):
        self.s = settings
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(settings.torbox_timeout_read, connect=settings.torbox_timeout_connect),
            follow_redirects=False,
        )
        self._global = TokenBucket(4.0)                 # ~240/min < 300/min cap
        self._requestdl = TokenBucket(1.0)              # community-reported safe pace
        self._mylist_ttl_until = 0.0
        self._mylist_cache: dict[str, dict] = {}        # hash -> torrent dict

    # ------------------------------------------------------------------ api
    async def _request(self, method: str, path: str, *, params: dict | None = None,
                       data: dict | None = None, retry: int = 0) -> dict:
        await self._global.wait()
        try:
            resp = await self._client.request(
                method,
                f"{self.s.torbox_base_url}{path}",
                params=params,
                data=data,
                headers={"Authorization": f"Bearer {self.s.torbox_api_token}"},
            )
        except httpx.TimeoutException as exc:
            raise ProviderError(f"torbox timeout on {path}: {exc!r}") from exc
        except httpx.HTTPError as exc:
            raise ProviderError(f"torbox network error on {path}: {exc!r}") from exc

        if resp.status_code == 429 or resp.status_code >= 500:
            if retry < self.s.torbox_max_retries:
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else 2.0 * (retry + 1)
                event("torbox_retry", path=path, status=resp.status_code, delay=delay, attempt=retry + 1)
                await asyncio.sleep(delay)
                return await self._request(method, path, params=params, data=data, retry=retry + 1)
            raise ProviderError(f"torbox {path} failed after retries: HTTP {resp.status_code}")

        if resp.status_code >= 400:
            raise ProviderError(f"torbox {path}: HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            body = resp.json()
        except ValueError as exc:
            raise ProviderError(f"torbox {path}: non-JSON response") from exc
        if not body.get("success", False) and path not in ("/torrents/checkcached",):
            raise ProviderError(f"torbox {path}: {body.get('detail') or body.get('error')}")
        return body

    # ------------------------------------------------------- DebridProvider
    async def availability(self, info_hashes: list[str]) -> dict[str, list[dict]]:
        if not info_hashes:
            return {}
        out: dict[str, list[dict]] = {}
        for i in range(0, len(info_hashes), 100):        # API accepts ~100/call
            batch = info_hashes[i:i + 100]
            body = await self._request(
                "GET", "/torrents/checkcached",
                params={"hash": ",".join(batch), "format": "list", "list_files": "true"},
            )
            for entry in body.get("data") or []:
                h = (entry.get("hash") or "").lower()
                out[h] = entry.get("files") or []
        return out

    async def ensure_torrent(self, info_hash: str, torrent_name: str) -> ProviderTorrent:
        torrent = await self._find_in_mylist(info_hash)
        if torrent is None:
            torrent = await self._add_magnet(info_hash, torrent_name)
        files = {int(f["id"]): {"name": f.get("name", ""), "size": int(f.get("size") or 0)}
                 for f in torrent.get("files") or []}
        ready = bool(torrent.get("download_finished")) or torrent.get("download_state") in ("cached", "completed")
        return ProviderTorrent(
            provider=self.name,
            torrent_id=int(torrent["id"]),
            info_hash=(torrent.get("hash") or info_hash).lower(),
            name=torrent.get("name") or torrent_name,
            cached=bool(torrent.get("cached")),
            ready=ready,
            files=files,
        )

    async def get_stream_url(self, torrent_id: int, file_id: int) -> str:
        await self._requestdl.wait()
        body = await self._request(
            "GET", "/torrents/requestdl",
            params={"torrent_id": torrent_id, "file_id": file_id,
                    "token": self.s.torbox_api_token, "redirect": "false"},
        )
        url = body.get("data")
        if not url:
            raise ProviderError("requestdl returned no url")
        return url

    async def read_range(self, url: str, start: int, length: int) -> bytes:
        end = start + length - 1
        for attempt in range(self.s.torbox_max_retries):
            try:
                async with self._client.stream(
                    "GET", url, headers={"Range": f"bytes={start}-{end}"}
                ) as resp:
                    if resp.status_code in (403, 410):
                        raise LinkExpiredError(f"stream link expired (HTTP {resp.status_code})")
                    if resp.status_code not in (200, 206):
                        if resp.status_code == 429 or resp.status_code >= 500:
                            await asyncio.sleep(1.0 + attempt)
                            continue
                        raise ProviderError(f"upstream HTTP {resp.status_code}")
                    chunks: list[bytes] = []
                    received = 0
                    async for chunk in resp.aiter_bytes(65536):
                        chunks.append(chunk)
                        received += len(chunk)
                        if received >= length:
                            break
                    return b"".join(chunks)[:length]
            except LinkExpiredError:
                raise
            except httpx.TimeoutException as exc:
                if attempt == self.s.torbox_max_retries - 1:
                    raise ProviderError(f"upstream read timeout: {exc!r}") from exc
                await asyncio.sleep(1.0 + attempt)
            except httpx.HTTPError as exc:
                if attempt == self.s.torbox_max_retries - 1:
                    raise ProviderError(f"upstream read error: {exc!r}") from exc
                await asyncio.sleep(1.0 + attempt)
        raise ProviderError("upstream read failed")

    # -------------------------------------------------------------- helpers
    async def _find_in_mylist(self, info_hash: str) -> dict | None:
        if time.monotonic() > self._mylist_ttl_until:
            body = await self._request("GET", "/torrents/mylist",
                                       params={"limit": 1000, "offset": 0, "bypass_cache": "true"})
            items = body.get("data") or []
            self._mylist_cache = {(t.get("hash") or "").lower(): t for t in items if t.get("hash")}
            self._mylist_ttl_until = time.monotonic() + 30.0
        return self._mylist_cache.get(info_hash)

    async def _add_magnet(self, info_hash: str, torrent_name: str) -> dict:
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={torrent_name}"
        body = await self._request("POST", "/torrents/createtorrent",
                                   data={"magnet": magnet, "seed": 3, "allow_zip": "false"})
        created = body.get("data") or {}
        torrent_id = created.get("torrent_id") or created.get("id")
        for attempt in range(self.s.torrent_ready_max_polls):
            await asyncio.sleep(self.s.torrent_ready_poll_interval)
            detail = await self._request("GET", "/torrents/mylist", params={"id": torrent_id})
            torrent = detail.get("data") or {}
            if torrent.get("download_finished") or torrent.get("cached"):
                return torrent
        raise NotReadyError(
            f"torrent {info_hash} not ready after {self.s.torrent_ready_max_polls} polls")

    _SE_EP = re.compile(r"S\d{1,2}E\d{1,3}", re.I)
    MIN_MEDIA_BYTES = 20 << 20

    def pick_file(self, torrent: ProviderTorrent, file_name_hint: str | None = None) -> tuple[int, dict] | None:
        """Choose the right file inside a torrent.

        Packs (season bundles) carry many files and a scraper's fileIdx=0 can
        point at an .nfo. Order: SxxEyy match on the hint, then hint substring,
        then largest video file; never accept a file below MIN_MEDIA_BYTES
        while bigger video files exist.
        """
        if not torrent.files:
            return None
        videos = [(fid, meta) for fid, meta in sorted(torrent.files.items())
                  if meta["name"].lower().endswith(VIDEO_EXTS)
                  and meta["size"] >= self.MIN_MEDIA_BYTES]
        pool = videos or list(torrent.files.items())
        if file_name_hint:
            hint = file_name_hint.lower()
            se = self._SE_EP.search(hint)
            if se:
                for fid, meta in pool:
                    if se.group(0).lower() in meta["name"].lower():
                        return fid, meta
            for fid, meta in pool:
                if hint in meta["name"].lower() or meta["name"].lower().endswith(hint):
                    return fid, meta
        if videos:
            return max(videos, key=lambda kv: kv[1]["size"])
        return max(pool, key=lambda kv: kv[1]["size"])
