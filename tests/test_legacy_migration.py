"""Legacy migration safety: swap-preconditions, classificatie, journal, queue."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.maintenance.legacy_migration import (
    Journal, classify_link, plan_queue, validate_swap_target)


def _probe_ok(path):
    return True


def _probe_fail(path):
    return False


def test_empty_target_rejected():
    ok, why = validate_swap_target("", "/mnt/vm_storage/symlinks/x.mkv")
    assert not ok and why == "EMPTY_TARGET"


def test_root_and_mount_root_rejected():
    for bad in ("/", "/mnt/remote/nzbdav"):
        ok, why = validate_swap_target(bad, "/mnt/vm_storage/symlinks/x.mkv")
        assert not ok and why == "MOUNT_ROOT_OR_ROOT"


def test_non_canonical_target_rejected():
    ok, why = validate_swap_target("/mnt/debrid/decypharr/x.mkv",
                                   "/mnt/vm_storage/symlinks/x.mkv")
    assert not ok and why == "NON_CANONICAL_TARGET"


def test_link_outside_symlink_root_rejected():
    ok, why = validate_swap_target("/mnt/remote/nzbdav/.ids/a/b/c/d/e/f", "/tmp/x.mkv")
    assert not ok and why == "LINK_OUTSIDE_SYMLINK_ROOT"


def test_unreadable_canonical_rejected(tmp_path, monkeypatch):
    import plex_scraper.maintenance.legacy_migration as lm
    monkeypatch.setattr(lm, "CANONICAL_PREFIX", str(tmp_path) + "/.ids/")
    fake = tmp_path / ".ids" / "missing.mkv"     # prefix ok, file bestaat niet:
    ok, why = validate_swap_target(str(fake), "/mnt/vm_storage/symlinks/x")
    assert not ok and why == "TARGET_NOT_FOUND"
    # leesbaar-pad maar probe faalt -> TARGET_READ_FAILED
    real = tmp_path / ".ids" / "dead.mkv"
    real.parent.mkdir(parents=True)
    real.write_bytes(b"x")
    ok, why = validate_swap_target(str(real), "/mnt/vm_storage/symlinks/x",
                                   read_probe=_probe_fail)
    assert not ok and why == "TARGET_READ_FAILED"


def test_valid_canonical_accepted(tmp_path, monkeypatch):
    import plex_scraper.maintenance.legacy_migration as lm
    monkeypatch.setattr(lm, "CANONICAL_PREFIX", str(tmp_path) + "/.ids/")
    f = tmp_path / ".ids" / "media.mkv"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x" * 1024)
    ok, why = validate_swap_target(str(f), "/mnt/vm_storage/symlinks/x.mkv",
                                   read_probe=_probe_ok)
    assert ok and why == "OK"


def test_classify_link():
    assert classify_link(True, False) == "LEGACY_WORKING_UNMANAGED"
    assert classify_link(False, False) == "LEGACY_DEAD_UNMANAGED"
    assert classify_link(True, True) == "LEGACY_RESOLVER_MANAGED"


def test_plan_queue_skips_completed():
    inv = [{"link": "/a", "readable": False, "plex_mapping": "EXACT"},
           {"link": "/b", "readable": False, "plex_mapping": "EXACT"},
           {"link": "/c", "readable": False, "plex_mapping": "AMBIGUOUS"},
           {"link": "/d", "readable": True, "plex_mapping": "EXACT"}]
    done = {"/a": {"state": "COMPLETED"}}
    q = plan_queue(inv, done)
    assert [i["link"] for i in q] == ["/b"]


def test_journal_roundtrip_and_restart(tmp_path):
    j = Journal(str(tmp_path))
    j.append("journal.jsonl", {"link": "/a", "state": "SWAPPED"})
    j.append("journal.jsonl", {"link": "/a", "state": "COMPLETED"})
    j2 = Journal(str(tmp_path))
    latest = j2.latest_by("journal.jsonl", "link")
    assert latest["/a"]["state"] == "COMPLETED"
