"""Torrentio adapter (FASE 5). Live-verified format (2026-09-30):

  GET {base}/stream/movie/tt0111161.json
  GET {base}/stream/series/tt0944947:1:1.json   (series-level id + s:e)

Response: {"streams": [{name, title, infoHash, fileIdx?, behaviorHints.filename}]}
- fileIdx may be absent -> None (largest video file chosen at validation).
- Query WITHOUT debrid config so results carry infoHash instead of proxy urls.
- Pace ~1 req/s; each lookup is internally expensive for the instance.
"""
from __future__ import annotations

import asyncio
import re

import httpx

from ..provider_errors import (
    ProviderBackendUnavailable,
    ProviderRateLimited,
    parse_retry_after,
)
from .base import Scraper, TorrentCandidate

_SEEDERS = re.compile(r"👤\s*(\d+)")
_SIZE = re.compile(r"💾\s*([\d.]+)\s*(GB|MB|TB)", re.I)


class TorrentioScraper(Scraper):
    name = "torrentio"

    # Torrentio's public instances sit behind Cloudflare: the default
    # python-httpx UA gets a 403, an addon-client UA is the honest identity.
    DEFAULT_UA = "Stremio/5.0 (plex-scraper; https://github.com/Cyxno/plex_scraper)"

    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None,
                 timeout: float = 20.0, user_agent: str | None = None):
        self.base = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(
            timeout=timeout, headers={"User-Agent": user_agent or self.DEFAULT_UA})
        self._pace = asyncio.Lock()
        self._last_request = 0.0

    async def search(self, item_key: dict) -> list[TorrentCandidate]:
        imdb = item_key.get("imdb_id")
        if not imdb:
            return []
        if item_key.get("kind") == "episode":
            path = f"/stream/series/{imdb}:{int(item_key['season'])}:{int(item_key['episode'])}.json"
        else:
            path = f"/stream/movie/{imdb}.json"
        url = f"{self.base}{path}"

        async with self._pace:                       # ~1 req/s self-pacing
            delay = self._last_request + 1.1 - asyncio.get_event_loop().time()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                resp = await self._client.get(url)
            except httpx.TimeoutException as exc:
                # transient — NOOIT een no-match (ingest-hardening Phase 2)
                raise ProviderBackendUnavailable(
                    self.name, f"timeout: {exc!r}"[:160]) from exc
            except httpx.TransportError as exc:
                raise ProviderBackendUnavailable(
                    self.name, f"transport error: {exc!r}"[:160]) from exc
            self._last_request = asyncio.get_event_loop().time()
        if resp.status_code == 429:
            # 429 = provider capacity, niet "geen bron". Retry-After eerlijk
            # tonen (seconden óf HTTP-date).
            retry_after = parse_retry_after(resp.headers.get("Retry-After"))
            raise ProviderRateLimited(
                self.name, "429 too many requests", retry_after_s=retry_after)
        if resp.status_code >= 500:
            raise ProviderBackendUnavailable(
                self.name, f"HTTP {resp.status_code} from torrentio")
        try:
            resp.raise_for_status()
            payload = resp.json()
        except httpx.HTTPStatusError as exc:
            raise ProviderBackendUnavailable(
                self.name, f"HTTP {exc.response.status_code}") from exc
        except ValueError as exc:                    # json decode / foutpagina
            raise ProviderBackendUnavailable(
                self.name, f"unparsable response: {exc!r}"[:160]) from exc

        out: list[TorrentCandidate] = []
        for stream in payload.get("streams") or []:
            info_hash = (stream.get("infoHash") or "").lower()
            if not info_hash:
                continue                              # proxied/debrid result
            title = stream.get("title") or ""
            release_name = title.split("\n", 1)[0].strip()
            seeders = None
            m = _SEEDERS.search(title)
            if m:
                seeders = int(m.group(1))
            size = None
            m = _SIZE.search(title)
            if m:
                factor = {"GB": 1 << 30, "MB": 1 << 20, "TB": 1 << 40}[m.group(2).upper()]
                size = int(float(m.group(1)) * factor)
            behavior = stream.get("behaviorHints") or {}
            file_idx = stream.get("fileIdx")
            out.append(TorrentCandidate(
                info_hash=info_hash,
                torrent_name=release_name or behavior.get("filename") or info_hash,
                size=size,
                file_name=behavior.get("filename"),
                file_index=int(file_idx) if file_idx is not None else None,
                seeders=seeders,
                scraper=self.name,
            ))
        return out
