"""Regressie: centrale tijd-parsing/formatting (audit 2026-10-07).

Root cause van "20733d ago ago": de cockpit gaf een DUUR (age_s) door aan
een formatter die een absolute epoch verwachtte en zelf al 'ago' toevoegde.
timefmt is de server-side bron van waarheid; de JS-mirror wordt apart
getest (test_cockpit_js.py).
"""
from __future__ import annotations

import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.common.timefmt import (  # noqa: E402
    age_seconds, humanize_age, humanize_duration, parse_ts, rel_label)

NOW = 1791397248.0


# -------------------------------------------------------------- parse_ts
def test_parse_ts_seconds_stay_seconds():
    assert parse_ts(NOW) == NOW
    assert parse_ts(int(NOW)) == NOW
    assert parse_ts(str(int(NOW))) == NOW            # numerieke string


def test_parse_ts_milliseconds_normalized():
    assert parse_ts(NOW * 1000) == NOW
    assert parse_ts(f"{int(NOW) * 1000}") == NOW


def test_parse_ts_iso_string():
    import datetime as dt
    iso = dt.datetime.fromtimestamp(NOW, dt.timezone.utc).isoformat()
    assert parse_ts(iso) == pytest_approx(NOW)
    assert parse_ts(iso.replace("+00:00", "Z")) == pytest_approx(NOW)


def pytest_approx(v):
    import pytest
    return pytest.approx(v, abs=0.001)


def test_parse_ts_invalid_inputs():
    for bad in (None, "", "   ", "garbage", 0, -5, 0.0, True, False,
                float("nan"), float("inf"), "not-a-timestamp 123abc"):
        assert parse_ts(bad) is None, bad


# ------------------------------------------------------------ age_seconds
def test_age_seconds_seconds_and_ms():
    assert age_seconds(NOW - 73328, now=NOW) == pytest_approx(73328)
    assert age_seconds((NOW - 73328) * 1000, now=NOW) == pytest_approx(73328)


def test_age_seconds_absurd_age_is_invalid():
    # 20733 dagen ≈ exact now-epoch — de foutieve waarde uit de bug
    assert age_seconds(0, now=NOW) is None
    assert age_seconds(60, now=NOW) is None
    assert 20733 * 86400 > 10 * 365.25 * 86400        # boven de plausiegrens
    assert age_seconds(NOW, now=NOW) == 0.0           # vers is geldig


def test_age_seconds_future_skew_tolerated():
    assert age_seconds(NOW + 30, now=NOW) == 0.0      # 30s in de toekomst: 0
    assert age_seconds(NOW + 3600, now=NOW) is None   # uur in de toekomst: ongeldig


def test_age_seconds_invalid_value():
    assert age_seconds(None, now=NOW) is None
    assert age_seconds("garbage", now=NOW) is None


# ----------------------------------------------------------- humanize_age
def test_humanize_age_formats():
    assert humanize_age(45) == "45s ago"
    assert humanize_age(125) == "2 min ago"
    assert humanize_age(3 * 3600) == "3h ago"
    assert humanize_age(20 * 3600 + 700) == "20h ago"
    assert humanize_age(2 * 86400 + 3600) == "2d ago"


def test_humanize_age_never_double_ago():
    for v in (45, 125, 73328, 2 * 86400, None, -1, 10**12):
        out = humanize_age(v)
        assert "ago" not in out or out.count("ago") == 1, out
        assert "ago ago" not in out


def test_humanize_age_invalid_is_timestamp_unavailable():
    for v in (None, -10, 20733 * 86400, float("nan")):
        assert humanize_age(v) == "timestamp unavailable"


def test_rel_label_end_to_end():
    assert rel_label(NOW - 73328, now=NOW) == "20h ago"
    assert rel_label((NOW - 90) * 1000, now=NOW) == "2 min ago"
    assert rel_label("garbage", now=NOW) == "timestamp unavailable"


# ------------------------------------------------------ humanize_duration
def test_humanize_duration_formats():
    assert humanize_duration(42) == "42s"
    assert humanize_duration(1509) == "25m 9s"        # was "1509s"
    assert humanize_duration(3600) == "1h"
    assert humanize_duration(5400) == "1h 30m"
    assert humanize_duration(90000) == "1d 1h"
    assert humanize_duration(0) == "0s"


def test_humanize_duration_invalid():
    for v in (None, -3, float("nan")):
        assert humanize_duration(v) == "–"


def test_realtime_age_computation():
    t0 = time.time()
    assert 0 <= age_seconds(t0) < 5
