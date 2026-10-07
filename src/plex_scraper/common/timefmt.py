"""Centrale tijd-parsing en relatieve-tijd-labels (audit 2026-10-07).

Eén bron van waarheid voor timestamp-normalisatie (epoch-seconden,
-milliseconden, ISO-strings) en leeftijdsberekening, zodat cockpits nooit
meer "ago ago" of absurd grote leeftijden (bijv. 20733d) tonen.

De cockpit-JS heeft een gespiegelde implementatie (cockpit.html, blok
UI-FMT); tests testen beide kanten op dezelfde semantiek.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone

# > dit aantal (seconden) kan alleen milliseconden zijn: 1e11 s is jaar ~5138.
_MS_EPOCH_FLOOR = 1e11
# Verder terug dan dit = meetfout/verkeerde eenheid, geen echte leeftijd.
MAX_PLAUSIBLE_AGE_S = 10 * 365.25 * 86400.0
# Kleine klok-skew (toekomstige timestamps) stillen we af in plaats van ze
# als ongeldig te verstoten.
_FUTURE_SKEW_S = 90.0


def parse_ts(value) -> float | None:
    """Normaliseer epoch-s, epoch-ms of ISO-8601 naar epoch-seconden.

    None bij afwezig/ongeldig (NaN, inf, <= 0, niet-parsbare strings).
    """
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            ts = float(value)
        else:
            s = str(value).strip()
            if not s:
                return None
            try:
                ts = float(s)
            except ValueError:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = dt.timestamp()
    except (ValueError, TypeError, OverflowError, OSError):
        return None
    if not math.isfinite(ts):
        return None
    if ts > _MS_EPOCH_FLOOR:
        ts /= 1000.0
    if ts <= 0.0:
        return None
    return ts


def age_seconds(value, now: float | None = None) -> float | None:
    """Leeftijd in seconden sinds `value`; None bij ongeldig of absurd.

    Absurd = negatief voorbij klok-skew of ouder dan MAX_PLAUSIBLE_AGE_S —
    precies de klasse waarin "20733d ago" viel.
    """
    ts = parse_ts(value)
    if ts is None:
        return None
    ref = time.time() if now is None else now
    age = ref - ts
    if age < -_FUTURE_SKEW_S or age > MAX_PLAUSIBLE_AGE_S:
        return None
    return max(0.0, age)


def humanize_age(seconds: float | None) -> str:
    """Duur-label: '35s ago' / '2 min ago' / '3h ago' / '20h ago' / '2d ago'.

    Voegt exact één keer 'ago' toe; None/ongeldig → 'timestamp unavailable'.
    """
    if seconds is None or not isinstance(seconds, (int, float)) \
            or not math.isfinite(seconds) or seconds < 0 \
            or seconds > MAX_PLAUSIBLE_AGE_S:
        return "timestamp unavailable"
    if seconds < 60:
        return f"{max(1, round(seconds))}s ago"
    if seconds < 3600:
        return f"{round(seconds / 60)} min ago"
    if seconds < 48 * 3600:
        return f"{round(seconds / 3600)}h ago"
    return f"{round(seconds / 86400)}d ago"


def rel_label(value, now: float | None = None) -> str:
    """Timestamp (elke vorm) → relatief label; nooit 'ago ago'."""
    return humanize_age(age_seconds(value, now))


def humanize_duration(seconds) -> str:
    """Run-duur: 42→'42s', 1509→'25m 9s', 5400→'1h 30m', 90000→'1d 1h'."""
    if seconds is None or not isinstance(seconds, (int, float)) \
            or not math.isfinite(seconds) or seconds < 0:
        return "–"
    s = round(seconds)
    if s < 60:
        return f"{s}s"
    m, sec = divmod(s, 60)
    if m < 60:
        return f"{m}m {sec}s" if sec else f"{m}m"
    h, mm = divmod(m, 60)
    if h < 24:
        return f"{h}h {mm}m" if mm else f"{h}h"
    d, hh = divmod(h, 24)
    return f"{d}d {hh}h" if hh else f"{d}d"
