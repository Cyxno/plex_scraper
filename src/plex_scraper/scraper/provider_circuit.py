"""Provider circuit breaker (ingest-hardening 2026-10-06, Phases 5-8).

Eén circuit per scraper-naam. States:

  HEALTHY       requests gaan normaal door
  RATE_LIMITED  429 gezien — alle searches deferred tot `unavailable_until`
  DEGRADED      5xx/timeout — korte bounded backoff, wel doorproberen na afloop
  UNAVAILABLE   herhaalde backend-storingen — langere backoff

Half-open: na `unavailable_until` gaat de EERSTVolgende request als probe door.
Slaagt hij → HEALTHY (recovered). Faalt hij → circuit opnieuw open met de
volgende backoff-trede. Geen agressieve loops: deferred requests kosten nul
provider-requests (acquire raise't meteen).

Backoff-schema (Phase 4): Retry-After wordt altijd gehonoreerd; zonder header
bounded exponentieel met jitter 1m → 2m → 5m → 10m → cap 30m (rate-limit)
en 30s → 1m → 2m → 5m → cap 10m (backend).
"""
from __future__ import annotations

import random
import threading
import time

from .provider_errors import (
    ProviderBackendUnavailable,
    ProviderRateLimited,
    ProviderSearchError,
)

RATE_LIMIT_STEPS_S = (60.0, 120.0, 300.0, 600.0, 1800.0)
BACKEND_STEPS_S = (30.0, 60.0, 120.0, 300.0, 600.0)
BACKEND_UNAVAILABLE_AFTER = 4          # opeenvolgende backend-fouten → UNAVAILABLE


class _ScraperCircuit:
    def __init__(self, name: str):
        self.name = name
        self.state = "HEALTHY"
        self.unavailable_until = 0.0
        self.consecutive_failures = 0
        self.last_error = ""
        self.retry_after_source = ""      # "retry-after-header" | "backoff" | ""
        self.half_open_probe = False


class ProviderCircuit:
    """Provider-breed overkoepelend circuit — per scraper een eigen state."""

    def __init__(self, jitter_frac: float = 0.2):
        self._circuits: dict[str, _ScraperCircuit] = {}
        self._lock = threading.Lock()
        self._jitter = jitter_frac
        self.metrics = {
            "rate_limit_events": 0, "backend_error_events": 0,
            "circuit_opened": 0, "half_open_probes": 0,
            "circuit_recovered": 0, "requests_deferred": 0,
        }
        self._change_log: list[dict] = []   # lifecycle-events, begrensd
        self._event_cursor = 0

    # ------------------------------------------------------------- internals
    def _circuit(self, name: str) -> _ScraperCircuit:
        with self._lock:
            if name not in self._circuits:
                self._circuits[name] = _ScraperCircuit(name)
            return self._circuits[name]

    def _record_change(self, c: _ScraperCircuit, event: str, **fields) -> None:
        entry = {"ts": time.time(), "event": event, "scraper": c.name,
                 "state": c.state, **fields}
        self._change_log.append(entry)
        if len(self._change_log) > 200:
            del self._change_log[:100]

    def _step_delay(self, c: _ScraperCircuit, steps) -> float:
        idx = min(c.consecutive_failures - 1, len(steps) - 1)
        base = steps[max(idx, 0)]
        jitter = base * self._jitter * (random.random() * 2 - 1)
        return max(base + jitter, 5.0)

    # ------------------------------------------------------------ interface
    def acquire(self, scraper: str) -> None:
        """Gate vóór elke provider-search. Raise ProviderSearchError (met
        retry-hint) als het circuit open is — nul HTTP-kosten."""
        c = self._circuit(scraper)
        if c.state == "HEALTHY":
            return
        now = time.time()
        if now >= c.unavailable_until:
            c.half_open_probe = True
            with self._lock:
                self.metrics["half_open_probes"] += 1
            self._record_change(c, "provider_circuit_half_open")
            return
        with self._lock:
            self.metrics["requests_deferred"] += 1
        retry_in = c.unavailable_until - now
        if c.state == "RATE_LIMITED":
            raise ProviderRateLimited(
                scraper, f"circuit open (rate limited), retry in {retry_in:.0f}s",
                retry_after_s=retry_in)
        raise ProviderBackendUnavailable(
            scraper, f"circuit open ({c.state.lower()}), retry in {retry_in:.0f}s",
            retry_after_s=retry_in)

    def report_rate_limited(self, scraper: str, retry_after_s: float | None,
                            detail: str = "") -> None:
        c = self._circuit(scraper)
        c.consecutive_failures += 1
        c.last_error = detail or "429 rate limited"
        if retry_after_s is not None and retry_after_s > 0:
            delay = retry_after_s
            c.retry_after_source = "retry-after-header"
        else:
            delay = self._step_delay(c, RATE_LIMIT_STEPS_S)
            c.retry_after_source = "backoff"
        was_open = c.state != "HEALTHY"
        c.state = "RATE_LIMITED"
        c.unavailable_until = time.time() + delay
        c.half_open_probe = False
        with self._lock:
            self.metrics["rate_limit_events"] += 1
            if not was_open:
                self.metrics["circuit_opened"] += 1
        self._record_change(c, "provider_circuit_open", retry_in_s=round(delay, 1),
                            retry_source=c.retry_after_source,
                            consecutive=c.consecutive_failures)

    def report_backend_error(self, scraper: str, detail: str = "") -> None:
        c = self._circuit(scraper)
        c.consecutive_failures += 1
        c.last_error = detail or "backend error"
        delay = self._step_delay(c, BACKEND_STEPS_S)
        new_state = ("UNAVAILABLE" if c.consecutive_failures >= BACKEND_UNAVAILABLE_AFTER
                     else "DEGRADED")
        was_open = c.state != "HEALTHY"
        c.state = new_state
        c.unavailable_until = time.time() + delay
        c.retry_after_source = "backoff"
        c.half_open_probe = False
        with self._lock:
            self.metrics["backend_error_events"] += 1
            if not was_open:
                self.metrics["circuit_opened"] += 1
        self._record_change(c, "provider_circuit_open", retry_in_s=round(delay, 1),
                            consecutive=c.consecutive_failures)

    def report_success(self, scraper: str) -> None:
        c = self._circuit(scraper)
        if c.state != "HEALTHY":
            with self._lock:
                self.metrics["circuit_recovered"] += 1
            self._record_change(c, "provider_circuit_recovered",
                                was_state=c.state)
        c.state = "HEALTHY"
        c.unavailable_until = 0.0
        c.consecutive_failures = 0
        c.last_error = ""
        c.retry_after_source = ""
        c.half_open_probe = False

    def available(self, scraper: str) -> bool:
        """Zonder zoekactie: mag er (mogelijk als half-open probe) gezocht?"""
        c = self._circuit(scraper)
        return c.state == "HEALTHY" or time.time() >= c.unavailable_until

    def blocked(self) -> list[str]:
        """Scraper-namen waarvan het circuit op dit moment open staat."""
        now = time.time()
        with self._lock:
            return [c.name for c in self._circuits.values()
                    if c.state != "HEALTHY" and now < c.unavailable_until]

    def take_change_events(self) -> list[dict]:
        """Lifecycle-events sinds vorige aanroep (voor persistente events)."""
        with self._lock:
            out = self._change_log[self._event_cursor:]
            self._event_cursor = len(self._change_log)
        return out

    def snapshot(self) -> dict:
        now = time.time()
        with self._lock:
            circuits = {}
            for name, c in self._circuits.items():
                circuits[name] = {
                    "state": c.state,
                    "retry_in_s": (round(c.unavailable_until - now, 1)
                                   if now < c.unavailable_until else 0),
                    "unavailable_until": c.unavailable_until,
                    "consecutive_failures": c.consecutive_failures,
                    "last_error": c.last_error[:160],
                    "retry_source": c.retry_after_source,
                    "half_open_probe": c.half_open_probe,
                }
            overall = "HEALTHY"
            for c in self._circuits.values():
                if c.state != "HEALTHY" and now < c.unavailable_until:
                    overall = c.state
                    break
            return {"overall": overall, "scrapers": circuits,
                    "metrics": dict(self.metrics),
                    "change_log": list(self._change_log[-20:])}


class CircuitBreakerScraper:
    """Wrapt een Scraper: gate → delegate → outcome rapporteren.

    Implementeert de Scraper-interface duck-typed (name + search) zodat de
    resolver-era lijst ongewijzigd blijft qua contract.
    """

    def __init__(self, inner, circuit: ProviderCircuit):
        self.inner = inner
        self.circuit = circuit
        self.name = inner.name

    async def search(self, item_key: dict):
        self.circuit.acquire(self.name)          # raise = defer, geen HTTP
        try:
            out = await self.inner.search(item_key)
        except ProviderRateLimited as exc:
            self.circuit.report_rate_limited(
                self.name, exc.retry_after_s, repr(exc)[:160])
            raise
        except ProviderBackendUnavailable as exc:
            self.circuit.report_backend_error(self.name, repr(exc)[:160])
            raise
        except ProviderSearchError as exc:
            self.circuit.report_backend_error(self.name, repr(exc)[:160])
            raise
        except Exception:
            # echte scraper-bug: geen circuit-reactie, gewone exceptie blijft
            raise
        self.circuit.report_success(self.name)
        return out
