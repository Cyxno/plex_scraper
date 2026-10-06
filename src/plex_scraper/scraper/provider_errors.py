"""Typed provider-search errors (ingest-hardening 2026-10-06).

Harde semantische regel: een provider die niet antwoordt is NOOIT een
"wel antwoord, nul resultaten". NO_SOURCE / PROVIDER_NO_MATCH mag alleen
ontstaan uit een 200-antwoord dat écht nul bruikbare candidates bevat.

  429                    → ProviderRateLimited      (Retry-After wordt geparsed)
  5xx                    → ProviderBackendUnavailable
  timeout / transport    → ProviderBackendUnavailable
  json-parse-fout        → ProviderBackendUnavailable (foutpagina = geen data)
"""
from __future__ import annotations

import email.utils
import time


class ProviderSearchError(Exception):
    """Zoekactie kon niet normaal afronden — transient, nooit een no-match."""

    kind = "PROVIDER_ERROR"

    def __init__(self, scraper: str, message: str,
                 retry_after_s: float | None = None):
        super().__init__(message)
        self.scraper = scraper
        self.retry_after_s = retry_after_s


class ProviderRateLimited(ProviderSearchError):
    """HTTP 429: provider-capacity bereikt. ALTJD deze klasse, nooit NO_SOURCE."""

    kind = "PROVIDER_RATE_LIMITED"


class ProviderBackendUnavailable(ProviderSearchError):
    """5xx / timeout / onleesbaar antwoord: backend tijdelijk onbeschikbaar."""

    kind = "BACKEND_UNAVAILABLE"


def parse_retry_after(value) -> float | None:
    """`Retry-After` ondersteunt both seconden en HTTP-date (RFC 7231).
    Negatieve/foutieve waarden → None (aanroeper gebruikt eigen backoff)."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        seconds = float(raw)
        return max(seconds, 0.0)
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            import datetime as _dt
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        delta = dt.timestamp() - time.time()
    except (OSError, OverflowError, ValueError):
        return None
    return round(max(delta, 0.0), 1)
