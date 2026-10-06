"""Sonarr/Radarr v3 API-clients (Phases 12-13, 17, 20).

Arr-expressies van wanted-state worden HIER gelezen; mutaties alleen via
ondersteunde command-endpoints (RescanSeries/RescanMovie + RefreshMovie).
Nooit directe arr-DB-writes.

Fouten: ArrUnavailable (transient) — een arr die niet antwoordt is nooit een
reden om een job te laten falen met een permanente state.
"""
from __future__ import annotations

import asyncio
import os

import httpx


class ArrUnavailable(Exception):
    """Arr tijdelijk onbereikbaar / API-fout — altijd retryable."""


def _read_secret_file(path: str) -> str:
    if not path:
        return ""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        return ""


class _ArrClient:
    def __init__(self, name: str, base_url: str, api_key: str,
                 timeout: float = 30.0):
        self.name = name
        self.base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base, timeout=timeout,
            headers={"X-Api-Key": api_key,
                     "User-Agent": "plex-scraper-ingest/1.0"})

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict | None = None):
        try:
            r = await self._client.get(path, params=params or {})
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise ArrUnavailable(f"{self.name} transport: {exc!r}"[:160]) from exc
        if r.status_code >= 500:
            raise ArrUnavailable(f"{self.name} HTTP {r.status_code}")
        if r.status_code >= 400:
            raise ArrUnavailable(
                f"{self.name} HTTP {r.status_code}: {r.text[:120]}")
        return r.json()

    async def _post(self, path: str, payload: dict):
        try:
            r = await self._client.post(path, json=payload)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise ArrUnavailable(f"{self.name} transport: {exc!r}"[:160]) from exc
        if r.status_code >= 500:
            raise ArrUnavailable(f"{self.name} HTTP {r.status_code}")
        if r.status_code >= 400:
            raise ArrUnavailable(
                f"{self.name} HTTP {r.status_code}: {r.text[:120]}")
        return r.json()

    async def health(self) -> list[dict]:
        return await self._get("/health")

    async def status_ok(self) -> bool:
        await self._get("/system/status")
        return True

    async def tasks(self) -> list[dict]:
        return await self._get("/system/task")


class SonarrClient(_ArrClient):
    async def series_all(self) -> list[dict]:
        return await self._get("/api/v3/series")

    async def series(self, series_id: int) -> dict:
        return await self._get(f"/api/v3/series/{series_id}")

    async def wanted_missing(self, page_size: int = 200, max_pages: int = 10):
        """Monitored wanted/missing episodes, nieuwste air-date eerst.
        Yields (episode_record, series_record)."""
        series_map = {s["id"]: s for s in await self.series_all()}
        for page in range(1, max_pages + 1):
            data = await self._get(
                "/api/v3/wanted/missing",
                params={"monitored": "true", "sortKey": "airDateUtc",
                        "sortDir": "descending", "page": page,
                        "pageSize": page_size})
            for rec in data.get("records") or []:
                yield rec, series_map.get(rec.get("seriesId"))
            total = data.get("totalRecords") or 0
            if page * page_size >= total:
                break

    async def episode(self, episode_id: int) -> dict:
        return await self._get(f"/api/v3/episode/{episode_id}")

    async def rescan_series(self, series_id: int) -> dict:
        return await self._post(
            "/api/v3/command", {"name": "RescanSeries", "seriesId": series_id})

    async def episode_search(self, episode_ids: list[int]) -> dict:
        return await self._post(
            "/api/v3/command",
            {"name": "EpisodeSearch", "episodeIds": episode_ids})

    async def download_clients(self) -> list[dict]:
        return await self._get("/api/v3/downloadclient")

    async def root_folders(self) -> list[dict]:
        return await self._get("/api/v3/rootfolder")


class RadarrClient(_ArrClient):
    async def movies_all(self) -> list[dict]:
        return await self._get("/api/v3/movie")

    async def movie(self, movie_id: int) -> dict:
        return await self._get(f"/api/v3/movie/{movie_id}")

    async def wanted_missing(self, page_size: int = 200, max_pages: int = 10):
        movies_map = {mv["id"]: mv for mv in await self.movies_all()}
        for page in range(1, max_pages + 1):
            data = await self._get(
                "/api/v3/wanted/missing",
                params={"monitored": "true", "sortKey": "inCinemas",
                        "sortDir": "descending", "page": page,
                        "pageSize": page_size})
            for rec in data.get("records") or []:
                yield rec, movies_map.get(rec.get("movieId"))
            total = data.get("totalRecords") or 0
            if page * page_size >= total:
                break

    async def rescan_movie(self, movie_id: int) -> dict:
        return await self._post(
            "/api/v3/command", {"name": "RescanMovie", "movieId": movie_id})

    async def refresh_movie(self, movie_id: int) -> dict:
        return await self._post(
            "/api/v3/command", {"name": "RefreshMovie", "movieId": movie_id})

    async def download_clients(self) -> list[dict]:
        return await self._get("/api/v3/downloadclient")


def build_clients_from_env(settings) -> tuple[SonarrClient | None,
                                              RadarrClient | None]:
    """Factory vanuit Settings (env-gedreven; sleutels mogen via secret-file)."""
    sonarr = None
    radarr = None
    key = getattr(settings, "sonarr_api_key", "")
    if getattr(settings, "sonarr_enabled", False):
        key = key or _read_secret_file(
            os.environ.get("SONARR_API_KEY_FILE", ""))
        if key:
            sonarr = SonarrClient("sonarr", settings.sonarr_url, key)
    key = getattr(settings, "radarr_api_key", "")
    if getattr(settings, "radarr_enabled", False):
        key = key or _read_secret_file(
            os.environ.get("RADARR_API_KEY_FILE", ""))
        if key:
            radarr = RadarrClient("radarr", settings.radarr_url, key)
    return sonarr, radarr


async def wait_command_done(client, command: dict, *,
                            timeout_s: float = 120.0,
                            poll_s: float = 4.0) -> bool:
    """Wacht tot een gestart arr-command klaar is (begrensd)."""
    cid = command.get("id")
    if not cid:
        return False
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(poll_s)
        try:
            cur = await client._get(f"/api/v3/command/{cid}")
        except ArrUnavailable:
            return False
        if (cur.get("status") or "").lower() in ("completed", "failed"):
            return (cur.get("status") or "").lower() == "completed"
    return False
