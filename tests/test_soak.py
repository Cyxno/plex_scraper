"""Soak-harness: sample-shape, incident-diff, rotatie/retentie."""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.soak import _append, _prune, diff_incidents


def test_incident_diff_transitions():
    prev = {"ts": 1.0, "containers": {"plex-scraper-vfs": {"health": "healthy",
            "restarts": 0}}, "physical": {"status": "HEALTHY"},
            "resolution_crashed_delta": 0, "plex_recoveries_delta": 0}
    cur = {"ts": 2.0, "containers": {"plex-scraper-vfs": {"health": "unhealthy",
            "restarts": 0}}, "physical": {"status": "HEALTHY"},
            "resolution_crashed_delta": 0, "plex_recoveries_delta": 0}
    inc = diff_incidents(prev, cur)
    assert {"kind": "container_health_transition", "container": "plex-scraper-vfs",
            "from": "healthy", "to": "unhealthy"} in \
        [{k: i[k] for k in ("kind", "container", "from", "to")} for i in inc]


def test_incident_diff_recovery_and_crash():
    prev = {"ts": 1.0, "containers": {}, "physical": {"status": "HEALTHY"}}
    cur = {"ts": 2.0, "containers": {}, "physical": {"status": "SUSPECT"},
           "resolution_crashed_delta": 1, "plex_recoveries_delta": 1,
           "db_error": None}
    kinds = {i["kind"] for i in diff_incidents(prev, cur)}
    assert {"physical_health", "resolution_crashed",
            "plex_restart_recovery"} <= kinds


def test_rotation_and_retention(tmp_path):
    """F5: file-cap roteert; oude .old-bestanden worden gewist."""
    big = tmp_path / "samples.jsonl"
    big.write_text("x" * (8 * 1024 * 1024))
    old = tmp_path / "samples.jsonl.1.old"
    old.write_text("x")
    os.utime(old, (time.time() - 8 * 86400, time.time() - 8 * 86400))
    _append(str(big), {"ts": 1.0})
    assert os.path.getsize(big) < 1024            # geroteerd en vers
    assert not old.exists()                       # retentie gewist
    # recenter .old blijft bestaan
    keep = tmp_path / "samples.jsonl.2.old"
    keep.write_text("x")
    _append(str(big), {"ts": 2.0})
    assert keep.exists()
