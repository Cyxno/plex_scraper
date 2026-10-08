"""Synthetische provider-blackout-simulatie (FASE 8, incident 2026-10-08).

Bewijst de keten 429 → centrale cooldown → geen retry-storm → recovery
TEGEN EEN LOKALE STUB — er wordt nul verkeer naar de echte TorBox-API
gestuurd. Draait standalone of in de test-image:

    docker run --rm -v $PWD:/app -w /app plex-scraper-test \
        python scripts/blackout_sim.py

Scenario:
  fase 1  stub antwoordt 429 + Retry-After op ALLES   → blackout RATE_LIMITED
  fase 2  20 parallelle "subsystemen" vragen URLs     → precies 1 HTTP-request
          is de wire op geweest; 19 keer fail-closed
  fase 3  cooldown verloopt; stub gaat 200 geven      → half-open probe,
          probe slaagt → HEALTHY + provider_blackout_recovered
  fase 4  stream links (CDN-pad) werken ook tijdens fase 1-2 door
"""
from __future__ import annotations

import asyncio
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, "src")

import httpx  # noqa: E402

from plex_scraper.common.config import Settings  # noqa: E402
from plex_scraper.scraper.provider_availability import (  # noqa: E402
    HEALTHY, RATE_LIMITED, ProviderAvailability, ProviderBlackout)
from plex_scraper.scraper.providers.torbox import TorboxProvider  # noqa: E402


class StubState:
    def __init__(self):
        self.lock = threading.Lock()
        self.api_requests = 0        # /torrents/* (de provider-API)
        self.cdn_requests = 0        # CDN-stream requests
        self.return_429 = True       # fase-schakelaar


STATE = StubState()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):                  # stil houden
        pass

    def do_GET(self):
        if self.path.startswith("/cdn/"):
            with STATE.lock:
                STATE.cdn_requests += 1
            body = b"\x1a\x45\xdf\xa3" + b"\x00" * 60
            self.send_response(206)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        with STATE.lock:
            STATE.api_requests += 1
            four29 = STATE.return_429
        if four29:
            self.send_response(429)
            self.send_header("Retry-After", "120")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        payload = b'{"success": true, "data": "https://stub.cdn/file"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        self.do_GET()


async def main() -> int:
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    av = ProviderAvailability("torbox")
    av.set_sink(lambda kind, **fields: print(
        f"    event: {kind} state={fields.get('state', '')} "
        f"retry_in_s={fields.get('retry_after_s', '')}"))
    settings = Settings(torbox_api_token="sim-token",
                        torbox_base_url=f"http://127.0.0.1:{port}",
                        torbox_max_retries=3)
    provider = TorboxProvider(settings, client=httpx.AsyncClient(
        timeout=httpx.Timeout(5.0), follow_redirects=False), availability=av)

    print("== fase 1: provider-wide 429 (stub, Retry-After: 2) ==")
    try:
        await provider.get_stream_url(1, 0)
        print("    FAIL: geen blackout gegooid")
        return 1
    except ProviderBlackout as exc:
        print(f"    OK: ProviderBlackout ({exc.state}), "
              f"api_requests={STATE.api_requests}")

    print("== fase 2: 20 parallelle subsystem-calls tijdens cooldown ==")
    results = await asyncio.gather(
        *[provider.get_stream_url(i, 0) for i in range(20)],
        return_exceptions=True)
    blocked = sum(1 for r in results if isinstance(r, ProviderBlackout))
    print(f"    OK: {blocked}/20 fail-closed, api_requests={STATE.api_requests} "
          f"(geen storm: nul nieuwe HTTP tijdens cooldown)")
    assert blocked == 20, "fail-closed verwacht"
    assert STATE.api_requests == 1, "retry-storm gedetecteerd"

    print("== fase 2b: CDN-stream (reeds geldige link) blijft werken ==")
    data = await provider.read_range(f"http://127.0.0.1:{port}/cdn/f", 0, 64)
    assert len(data) == 64
    print(f"    OK: 64B gelezen, cdn_requests={STATE.cdn_requests}, "
          f"api ongewijzigd={STATE.api_requests}")

    print("== fase 3: cooldown verloopt → half-open probe → 200 ==")
    with STATE.lock:
        STATE.return_429 = False
    av.cooldown_until = time.time() - 0.01       # versnel de cooldown (stub)
    url = await provider.get_stream_url(99, 0)
    print(f"    OK: probe geslaagd, url={url[:40]}…, state={av.state}")
    assert av.state == HEALTHY
    snap = av.snapshot()
    assert snap["last_success_at"] is not None
    print(f"    OK: recovery-events aanwezig, metrics={snap['metrics']}")

    server.shutdown()
    print("\nBLACKOUT_SIMULATIE: PASS — 429 → centrale cooldown → geen storm "
          "→ CDN blijft → half-open probe → HEALTHY")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
