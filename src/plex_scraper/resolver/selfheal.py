"""Self-healing engine: background health sweeper, shadow repair, upgrade policy.

Shadow mode: detect broken sources, search alternatives, verify, report
WOULD SWITCH — without touching the active source.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sqlite3
import time
from dataclasses import dataclass, field

log = logging.getLogger("selfheal")


# ------------------------------------------------------------------ states
HEALTHY = "HEALTHY"
CHECKING = "CHECKING"
DEGRADED = "DEGRADED"
REPAIR_PENDING = "REPAIR_PENDING"
REPAIRING = "REPAIRING"
VERIFYING_REPAIR = "VERIFYING_REPAIR"
NO_SOURCE = "NO_SOURCE"
IDENTITY_AMBIGUOUS = "IDENTITY_AMBIGUOUS"
RECOVERED = "RECOVERED"


@dataclass
class ShadowResult:
    """Result of a shadow repair evaluation."""
    plex_path: str
    title: str
    current_hash: str | None
    current_score: float | None
    failure_type: str
    candidates_found: int = 0
    candidates_rejected: int = 0
    reject_reasons: list[str] = field(default_factory=list)
    chosen_hash: str | None = None
    chosen_score: float | None = None
    identity_pass: bool = False
    byte_read_ok: bool = False
    seek_ok: bool = False
    would_switch: bool = False
    elapsed_s: float = 0.0
    error: str | None = None


# ---------------------------------------------------------- identity gate
def identity_gate(item_title: str, item_gp: str | None,
                  item_season: int | None, item_episode: int | None,
                  candidate_name: str, item_year: int | None = None,
                  candidate_year: int | None = None) -> tuple[bool, str]:
    """Harde identity-match tussen media-item en candidate release-naam.
    Retourneert (pass, reden). Geen score — puur identity."""
    cname = candidate_name.lower().replace(".", " ").replace("_", " ")
    item_lower = (item_title or "").lower()

    # films: title moet in candidate zitten
    if not item_gp:
        words = [w for w in item_lower.split() if len(w) > 2]
        if not words:
            return True, "onvoldoende metadata voor identity check"
        missing = [w for w in words if w not in cname]
        if len(missing) > len(words) // 2:
            return False, f"title mismatch: {missing} niet in candidate"
        if item_year and candidate_year:
            if abs(int(item_year) - int(candidate_year)) > 1:
                return False, f"year mismatch: {item_year} vs {candidate_year}"
        return True, "film identity OK"

    # episodes: series name + SxxEyy
    series_words = [w for w in (item_gp or "").lower().split() if len(w) > 2]
    if not series_words:
        return True, "geen series-naam voor identity check"
    missing_series = [w for w in series_words if w not in cname]
    if len(missing_series) > len(series_words) // 2:
        return False, f"series mismatch: {item_gp} niet in candidate"
    if item_season is not None and item_episode is not None:
        se = f"s{int(item_season):02d}e{int(item_episode):02d}"
        se_loose = f"{int(item_season)}x{int(item_episode):02d}"
        if se not in cname and se_loose not in cname:
            return False, f"season/episode mismatch: {se} niet in candidate"
    return True, "episode identity OK"


# ---------------------------------------------------------- upgrade policy
def upgrade_policy(current_score: float | None, candidate_score: float,
                   current_res: str | None, candidate_res: str | None,
                   min_delta: float = 5.0) -> tuple[bool, str]:
    """Upgrade-beslissing met hysteresis en semantische regels.
    Resolutie-sprong is altijd een upgrade, ongeacht score-delta."""
    if current_score is None:
        return True, "geen huidige score"
    res_order = {"720p": 1, "1080p": 2, "2160p": 3}
    cr = res_order.get((current_res or "").lower(), 0)
    dr = res_order.get((candidate_res or "").lower(), 0)
    if dr > cr:
        return True, f"resolutie-sprong {current_res} → {candidate_res}"
    delta = candidate_score - current_score
    if delta < min_delta:
        return False, f"delta {delta:.1f} < {min_delta}"
    if delta < 3.0:
        return False, f"minieme delta {delta:.1f}"
    return True, f"score +{delta:.1f}"


# ---------------------------------------------------------- anti-flapping
class AntiFlapping:
    """Cooldowns en blacklisting tegen source A→B→A→B loops."""

    def __init__(self, min_source_age_s: float = 3600.0,
                 cooldown_repair_s: float = 3600.0,
                 max_repairs_per_day: int = 3):
        self.min_source_age_s = min_source_age_s
        self.cooldown_repair_s = cooldown_repair_s
        self.max_repairs_per_day = max_repairs_per_day
        self._history: dict[str, list[float]] = {}  # path → [timestamps]
        self._blacklist: dict[str, float] = {}  # hash → blacklist_until

    def can_repair(self, plex_path: str) -> tuple[bool, str]:
        now = time.time()
        stamps = self._history.get(plex_path, [])
        recent = [t for t in stamps if now - t < 86400]
        if len(recent) >= self.max_repairs_per_day:
            return False, f"max {self.max_repairs_per_day} repairs/dag bereikt"
        if stamps and now - max(stamps) < self.cooldown_repair_s:
            return False, "cooldown na vorige repair"
        return True, ""

    def record_repair(self, plex_path: str):
        self._history.setdefault(plex_path, []).append(time.time())

    def blacklist_candidate(self, info_hash: str, duration_s: float = 7200.0):
        self._blacklist[info_hash] = time.time() + duration_s

    def is_blacklisted(self, info_hash: str) -> bool:
        until = self._blacklist.get(info_hash, 0)
        if until and time.time() < until:
            return True
        if until:
            del self._blacklist[info_hash]
        return False


# ---------------------------------------------------------- NO_SOURCE retry
class NoSourceRetry:
    """Bounded backoff voor NO_SOURCE-items."""

    def __init__(self, base_s: float = 3600.0, max_s: float = 86400.0):
        self.base_s = base_s
        self.max_s = max_s
        self._next_retry: dict[str, float] = {}
        self._fail_count: dict[str, int] = {}

    def next_retry_in(self, plex_path: str) -> float:
        n = max(0, self._fail_count.get(plex_path, 1) - 1)
        return min(self.base_s * (2 ** n), self.max_s)

    def record_failure(self, plex_path: str):
        self._fail_count[plex_path] = self._fail_count.get(plex_path, 0) + 1
        self._next_retry[plex_path] = time.time() + self.next_retry_in(plex_path)

    def should_retry(self, plex_path: str) -> bool:
        return time.time() >= self._next_retry.get(plex_path, 0)

    def record_success(self, plex_path: str):
        self._fail_count.pop(plex_path, None)
        self._next_retry.pop(plex_path, None)
