"""Tests voor de self-healing module: identity gate, upgrade policy, anti-flap."""
import os
import sys
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from plex_scraper.resolver.selfheal import (
    identity_gate, upgrade_policy, AntiFlapping, NoSourceRetry,
)


class TestIdentityGate:
    def test_film_title_match(self):
        ok, reason = identity_gate(
            "The Matrix", None, None, None,
            "The.Matrix.1999.2160p.BluRay.REMUX.HEVC.TrueHD-GRP")
        assert ok

    def test_film_title_mismatch(self):
        ok, reason = identity_gate(
            "The Matrix", None, None, None,
            "The.Matrix.Reloaded.2003.2160p.BluRay.x265-GRP",
            item_year=1999, candidate_year=2003)
        assert not ok, reason
        assert "mismatch" in reason

    def test_film_year_mismatch(self):
        ok, reason = identity_gate(
            "The Matrix", None, None, None,
            "The.Matrix.Reloaded.2003.2160p-GRP",
            item_year=1999, candidate_year=2003)
        assert not ok

    def test_episode_series_match(self):
        ok, reason = identity_gate(
            "Pilot", "Breaking Bad", 1, 1,
            "Breaking.Bad.S01E01.Pilot.1080p.BluRay.x264-GRP")
        assert ok

    def test_episode_wrong_series(self):
        ok, reason = identity_gate(
            "Pilot", "Breaking Bad", 1, 1,
            "Better.Call.Saul.S01E01.Pilot.1080p.BluRay.x264-GRP")
        assert not ok

    def test_episode_wrong_episode_number(self):
        ok, reason = identity_gate(
            "Cat in the Bag", "Breaking Bad", 1, 2,
            "Breaking.Bad.S01E03.1080p.BluRay.x264-GRP")
        assert not ok

    def test_episode_loose_format(self):
        ok, reason = identity_gate(
            "Pilot", "Breaking Bad", 1, 1,
            "Breaking Bad 1x01 Pilot 720p.mkv")
        assert ok

    def test_unknown_item_passes(self):
        """Onvoldoende metadata → gate slaagt (geen vals-positieve reject)."""
        ok, reason = identity_gate("", None, None, None, "Anything.1080p.mkv")
        assert ok


class TestUpgradePolicy:
    def test_no_delta_no_upgrade(self):
        ok, _ = upgrade_policy(91.0, 91.0, "1080p", "1080p", 5.0)
        assert not ok

    def test_small_delta_no_upgrade(self):
        ok, _ = upgrade_policy(91.0, 93.0, "1080p", "1080p", 5.0)
        assert not ok

    def test_large_delta_upgrade(self):
        ok, reason = upgrade_policy(91.0, 98.0, "1080p", "2160p", 5.0)
        assert ok, reason

    def test_resolution_jump_always_upgrade(self):
        ok, _ = upgrade_policy(91.0, 92.0, "1080p", "2160p", 5.0)
        assert ok

    def test_none_current_upgrade(self):
        ok, _ = upgrade_policy(None, 95.0, "1080p", "1080p", 5.0)
        assert ok


class TestAntiFlapping:
    def test_can_repair_initially(self):
        af = AntiFlapping()
        ok, _ = af.can_repair("/test/item")
        assert ok

    def test_max_repairs_per_day(self):
        af = AntiFlapping(max_repairs_per_day=2)
        af.record_repair("/test")
        af.record_repair("/test")
        ok, reason = af.can_repair("/test")
        assert not ok
        assert "max" in reason

    def test_cooldown_after_repair(self):
        af = AntiFlapping(cooldown_repair_s=3600.0)
        af.record_repair("/test")
        ok, reason = af.can_repair("/test")
        assert not ok
        assert "cooldown" in reason

    def test_blacklist_candidate(self):
        af = AntiFlapping()
        af.blacklist_candidate("abc123", 7200.0)
        assert af.is_blacklisted("abc123")
        assert not af.is_blacklisted("def456")


class TestNoSourceRetry:
    def test_exponential_backoff(self):
        ns = NoSourceRetry(base_s=60.0, max_s=3600.0)
        ns.record_failure("/test")
        assert ns.next_retry_in("/test") == 60.0
        ns.record_failure("/test")
        assert ns.next_retry_in("/test") == 120.0
        ns.record_failure("/test")
        assert ns.next_retry_in("/test") == 240.0

    def test_bounded_backoff(self):
        ns = NoSourceRetry(base_s=60.0, max_s=120.0)
        for _ in range(10):
            ns.record_failure("/test")
        assert ns.next_retry_in("/test") <= 120.0

    def test_success_resets(self):
        ns = NoSourceRetry(base_s=60.0)
        ns.record_failure("/test")
        ns.record_success("/test")
        assert ns.should_retry("/test")
