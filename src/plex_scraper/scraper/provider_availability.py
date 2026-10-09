"""Globale provider-availability (incident 2026-10: TorBox requestdl-429).

Eén autoritatieve provider-state voor de hele stack binnen het
resolver-proces. De audit (2026-10-08) bewees dat de 429-semantiek tot dan
toe call-local was: elke consument (resolve-validatie, JIT-probe, sweeper,
ingest, VFS-readpad) hield zijn eigen in-call retries en niemand legde een
provider-wide cooldown vast — een provider-blackout mondde uit in
candidate-burn, misleidende jit_no_equivalent_source, dubbele requestdl's en
VFS/Plex-alerts zonder provider-attributie.

States:
  HEALTHY       requests gaan normaal door
  RATE_LIMITED  harde 429 — cooldown (Retry-After of bounded backoff),
                na expiry één half-open probe
  DAILY_LIMIT   daglimiet — lange cooldown (meerdere uren, Retry-After
                alleen als langer); géén 60s-probe-loop tot de day-reset
  DEGRADED      5xx/timeout-streak — korte bounded backoff
  UNAVAILABLE   aanhoudende backend-storingen — langere backoff

Semantiek:
  * fail closed voor provider-API-calls (`check()` raise't ProviderBlackout,
    nul HTTP-kosten), NIET voor lokale metadata/stat-paden;
  * CDN-leespad (`read_range` op reeds geldige stream-links) wordt bewust
    NIET gegate — bestaande streams blijven doorlopen tijdens een blackout;
  * half-open: na cooldown-expiry gaat precies ÉÉN request als probe door
    (single-flight); slaagt die → HEALTHY + recovery-event, faalt die →
    cooldown verlengd;
  * een blackout is NOOIT een media-no-match: consumers vertalen hem naar
    PROVIDER_WAIT / jit_deferred / sweep-pauze, nooit naar NO_SOURCE.

Events (gededupeerd, geen storm):
  provider_blackout_started                éénmalig per blackout
  provider_blackout_extended               alleen bij betekenisvolle verlenging
  provider_blackout_recovered              éénmalig per blackout
  provider_request_blocked_by_cooldown     ≥30s tussen opeenvolgende
"""
from __future__ import annotations

import asyncio
import threading
import time

from plex_scraper.common.log import event
from .providers.base import ProviderError

HEALTHY = "HEALTHY"
RATE_LIMITED = "RATE_LIMITED"
DAILY_LIMIT = "DAILY_LIMIT"
DEGRADED = "DEGRADED"
UNAVAILABLE = "UNAVAILABLE"

BLOCKED_STATES = frozenset({RATE_LIMITED, DAILY_LIMIT, DEGRADED, UNAVAILABLE})

RATE_LIMIT_STEPS_S = (60.0, 120.0, 300.0, 600.0, 1800.0)
BACKEND_STEPS_S = (30.0, 60.0, 120.0, 300.0, 600.0)
BACKEND_UNAVAILABLE_AFTER = 4          # opeenvolgende backend-fouten → UNAVAILABLE
PROBE_WINDOW_S = 120.0                 # single-flight probe mag max zo lang duren
BLOCKED_EVENT_MIN_INTERVAL_S = 30.0    # dedupe blocked-events
EXTEND_MIN_GAIN_S = 60.0               # verlenging telt pas vanaf deze groei


class ProviderBlackout(ProviderError):
    """Provider-API is in cooldown — request is NIET gestuurd (fail closed).

    Subclass van ProviderError zodat bestaande except-clauses blijven
    werken; nieuwe code kan hem apart vangen om PROVIDER_WAIT/defer-semantiek
    te geven in plaats van candidate-failure.
    """

    kind = "PROVIDER_BLACKOUT"

    def __init__(self, provider: str, state: str, *,
                 endpoint_class: str = "", role: str = "",
                 retry_after_s: float | None = None,
                 cooldown_until: float = 0.0,
                 error_code: str = "", detail: str = ""):
        retry_in = max(cooldown_until - time.time(), 0.0)
        super().__init__(
            f"{provider} unavailable ({state.lower()}), retry in {retry_in:.0f}s"
            + (f" [{error_code}]" if error_code else ""))
        self.provider = provider
        self.state = state
        self.endpoint_class = endpoint_class
        self.role = role
        self.retry_after_s = retry_after_s
        self.cooldown_until = cooldown_until
        self.error_code = error_code
        self.detail = detail


def classify_429(detail: str) -> str:
    """429-body → DAILY_LIMIT of RATE_LIMITED. TorBox vermeldt de daglimiet
    in de response-detail; zonder dat bewijs is het een gewone rate-limit."""
    low = (detail or "").lower()
    if "daily" in low or "bandwidth" in low:
        return DAILY_LIMIT
    return RATE_LIMITED


class ProviderAvailability:
    """Thread-safe, proces-globale provider-state (zie moduledocstring)."""

    def __init__(self, provider: str = "torbox", *,
                 rate_limit_steps: tuple = RATE_LIMIT_STEPS_S,
                 backend_steps: tuple = BACKEND_STEPS_S,
                 daily_default_cooldown_s: float = 4 * 3600.0,
                 daily_max_cooldown_s: float = 6 * 3600.0):
        self.provider = provider
        self._rate_steps = rate_limit_steps
        self._backend_steps = backend_steps
        self._daily_default_s = daily_default_cooldown_s
        self._daily_max_s = daily_max_cooldown_s

        self._lock = threading.Lock()
        self.state = HEALTHY
        self.cooldown_until = 0.0
        self.reason = ""
        self.error_code = ""
        self.http_status = None
        self.endpoint_class = ""
        self.consecutive_backend_errors = 0
        self.consecutive_429s = 0
        self.last_success_at = 0.0
        self.last_429_at = 0.0
        self.blackout_started_at = 0.0
        self._probe_inflight_until = 0.0
        self._extensions = 0
        self._last_blocked_event_at = 0.0
        # Optioneel: betrouwbare daily-usage-opvraag bestaat (nog) niet —
        # None betekent "unknown" voor de UI, nooit een schatting.
        self.daily_usage: dict | None = None
        self.metrics = {
            "blackouts": 0, "rate_limit_events": 0, "backend_error_events": 0,
            "requests_blocked": 0, "probes": 0, "probe_successes": 0,
            "probe_failures": 0, "recoveries": 0, "extensions": 0,
        }
        self._event_sink = None                # async store bridge (resolver)
        self._last_state_event = ""            # dedupe van lifecycle-events

    # ------------------------------------------------------------- sink
    def set_sink(self, sink) -> None:
        """Persistente event-bridge: sync callback (kind, **fields) — de
        resolver hangt hier een store.add_event-task aan."""
        self._event_sink = sink

    def _emit(self, kind: str, **fields) -> None:
        event(kind, provider=self.provider, **fields)
        if self._event_sink is not None:
            try:
                self._event_sink(kind, **fields)
            except Exception:                       # noqa: BLE001
                pass

    # ------------------------------------------------------------ queries
    def blocked(self) -> bool:
        """True zolang een cooldown actief is (fail-closed venster)."""
        return self.state in BLOCKED_STATES and time.time() < self.cooldown_until

    def retry_in_s(self) -> float:
        return max(self.cooldown_until - time.time(), 0.0) \
            if self.state in BLOCKED_STATES else 0.0

    # ------------------------------------------------------------- gate
    def check(self, endpoint_class: str = "", role: str = "") -> None:
        """Gate vóór elke provider-API-call. Raise ProviderBlackout (nul
        HTTP-kosten) tijdens cooldown; na expiry gaat precies één call als
        half-open probe door (single-flight), de rest blijft deferred."""
        with self._lock:
            now = time.time()
            if self.state == HEALTHY:
                return
            if now < self.cooldown_until:
                self.metrics["requests_blocked"] += 1
                if now - self._last_blocked_event_at >= BLOCKED_EVENT_MIN_INTERVAL_S:
                    self._last_blocked_event_at = now
                    self._emit("provider_request_blocked_by_cooldown",
                               endpoint_class=endpoint_class, role=role,
                               http_status=self.http_status,
                               error_code=self.error_code,
                               cooldown_until=self.cooldown_until,
                               retry_in_s=round(self.cooldown_until - now, 1),
                               reason=self.reason[:160])
                raise ProviderBlackout(
                    self.provider, self.state, endpoint_class=endpoint_class,
                    role=role, cooldown_until=self.cooldown_until,
                    error_code=self.error_code, detail=self.reason)
            if self._probe_inflight_until > now:
                # half-open probe loopt al (andere caller) — deferred
                self.metrics["requests_blocked"] += 1
                raise ProviderBlackout(
                    self.provider, self.state, endpoint_class=endpoint_class,
                    role=role, cooldown_until=self.cooldown_until,
                    error_code=self.error_code, detail="probe in flight")
            # half-open: dit request is dé probe
            self._probe_inflight_until = now + PROBE_WINDOW_S
            self.metrics["probes"] += 1

    # ----------------------------------------------------------- reports
    def report_429(self, retry_after_s: float | None = None, *,
                   error_code: str = "", detail: str = "",
                   endpoint_class: str = "") -> None:
        """Harde 429 gezien: cooldown centraal vastleggen. Retry-After wordt
        altijd gehonoreerd; zonder header bounded backoff per stap."""
        with self._lock:
            now = time.time()
            self.last_429_at = now
            self.consecutive_429s += 1
            self.consecutive_backend_errors = 0
            self.http_status = 429
            self.endpoint_class = endpoint_class or self.endpoint_class
            self.reason = detail or "429 rate limited"
            self.error_code = error_code or classify_429(detail)

            new_state = RATE_LIMITED if self.error_code != DAILY_LIMIT \
                else DAILY_LIMIT
            if new_state == DAILY_LIMIT:
                # 2026-10-09: DAILY_LIMIT is geen gewone rate-limit. TorBox
                # stuurt bij de daglimiet-429 een korte Retry-After (~60s)
                # mee; die overschreef de daglimiet-cooldown en produceerde
                # een 429-probe-loop tot de provider-day-reset. Retry-After
                # telt hier alleen als hij LANGER is dan de daily-default;
                # de daily-reset-tijd is onbetrouwbaar bekend, dus een
                # veilige meerurige minimum-cooldown.
                delay = max(self._daily_default_s, retry_after_s or 0.0)
                delay = min(delay, self._daily_max_s)
            elif retry_after_s is not None and retry_after_s > 0:
                delay = retry_after_s
            else:
                step = self._rate_steps[
                    min(self.consecutive_429s - 1, len(self._rate_steps) - 1)]
                delay = step

            self.metrics["rate_limit_events"] += 1
            self._transition_locked(new_state, now + delay, now)

    def report_backend_error(self, detail: str = "",
                             endpoint_class: str = "") -> None:
        """5xx/timeout-streak: korte bounded backoff, herhaald → UNAVAILABLE."""
        with self._lock:
            now = time.time()
            self.consecutive_backend_errors += 1
            self.consecutive_429s = 0
            self.http_status = None
            self.endpoint_class = endpoint_class or self.endpoint_class
            self.reason = detail or "backend error"
            self.error_code = ""
            step = self._backend_steps[
                min(self.consecutive_backend_errors - 1,
                    len(self._backend_steps) - 1)]
            new_state = (UNAVAILABLE if self.consecutive_backend_errors
                         >= BACKEND_UNAVAILABLE_AFTER else DEGRADED)
            self.metrics["backend_error_events"] += 1
            self._transition_locked(new_state, now + step, now)

    def report_success(self) -> None:
        """Succesvolle provider-call (incl. half-open probe) → HEALTHY."""
        with self._lock:
            now = time.time()
            self._probe_inflight_until = 0.0
            self.last_success_at = now
            self.consecutive_backend_errors = 0
            self.consecutive_429s = 0
            self.metrics["probe_successes"] += 1
            if self.state != HEALTHY:
                self.metrics["recoveries"] += 1
                self._emit("provider_blackout_recovered",
                           endpoint_class=self.endpoint_class,
                           duration_s=round(
                               now - self.blackout_started_at, 1)
                           if self.blackout_started_at else None,
                           last_error=self.reason[:160])
            self.state = HEALTHY
            self.cooldown_until = 0.0
            self.reason = ""
            self.error_code = ""
            self.http_status = None
            self.blackout_started_at = 0.0
            self._extensions = 0
            self._last_state_event = ""

    def _transition_locked(self, new_state: str, cooldown_until: float,
                           now: float) -> None:
        """Gemeenschappelijke open/extend-logica (lock moet gehouden worden).
        Geleidt de event-dedupe: started alleen vanuit HEALTHY, extended
        bij elke geloofwaardige verlenging (incl. een gefaalde half-open
        probe na expiry — dat is een voortzetting, geen nieuwe blackout),
        recovery via report_success."""
        was_open = self.state in BLOCKED_STATES     # ook na expiry: zelfde incident
        grew = cooldown_until > self.cooldown_until
        meaningful = grew and (cooldown_until - self.cooldown_until
                               >= max(EXTEND_MIN_GAIN_S,
                                      0.25 * max(self.cooldown_until - now, 0.0)))
        self.state = new_state
        self.cooldown_until = cooldown_until
        self._probe_inflight_until = 0.0        # probe heeft gefaald

        if not was_open:
            self.metrics["blackouts"] += 1
            self.blackout_started_at = now
            self._extensions = 0
            self._last_state_event = "started"
            self._emit("provider_blackout_started", state=new_state,
                       endpoint_class=self.endpoint_class,
                       http_status=self.http_status,
                       error_code=self.error_code,
                       cooldown_until=cooldown_until,
                       retry_after_s=round(
                           cooldown_until - now, 1), reason=self.reason[:160])
        elif meaningful:
            self._extensions += 1
            self.metrics["extensions"] += 1
            self._last_state_event = "extended"
            self._emit("provider_blackout_extended", state=new_state,
                       endpoint_class=self.endpoint_class,
                       http_status=self.http_status,
                       error_code=self.error_code,
                       cooldown_until=cooldown_until,
                       extensions=self._extensions, reason=self.reason[:160])
        # was_open && niet meaningful: stil verlengen binnen dezelfde
        # blackout — geen event-storm op elke 429

    # ---------------------------------------------------------- snapshot
    def snapshot(self) -> dict:
        """UI/cockpit-view: state, timestamps, cooldown, usage (unknown als
        niet betrouwbaar opvraagbaar)."""
        with self._lock:
            now = time.time()
            blocked = self.state in BLOCKED_STATES and now < self.cooldown_until
            return {
                "provider": self.provider,
                "state": self.state,
                "blocked": blocked,
                "cooldown_until": self.cooldown_until if blocked else 0.0,
                "retry_in_s": round(self.cooldown_until - now, 1)
                if blocked else 0.0,
                "reason": self.reason[:200],
                "error_code": self.error_code,
                "http_status": self.http_status,
                "endpoint_class": self.endpoint_class,
                "blackout_started_at": self.blackout_started_at or None,
                "last_429_at": self.last_429_at or None,
                "last_success_at": self.last_success_at or None,
                "consecutive_429s": self.consecutive_429s,
                "consecutive_backend_errors": self.consecutive_backend_errors,
                "extensions": self._extensions,
                "half_open_probe_inflight":
                    self._probe_inflight_until > now,
                "daily_usage": self.daily_usage,   # None → UI toont "unknown"
                "metrics": dict(self.metrics),
            }


_DEFAULT: dict[str, ProviderAvailability] = {}
_DEFAULT_LOCK = threading.Lock()


def default_availability(provider: str = "torbox") -> ProviderAvailability:
    """Proces-globale instantie. De resolver-role deelt hem over resolve,
    JIT, sweeper en ingest (alles in-process); VFS doet zelf geen provider-
    calls en is via de resolver-API automatisch meegenomen."""
    with _DEFAULT_LOCK:
        inst = _DEFAULT.get(provider)
        if inst is None:
            inst = ProviderAvailability(provider)
            _DEFAULT[provider] = inst
        return inst
