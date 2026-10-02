"""Background health sweeper + opportunistic upgrade.

Rolling, oldest-first, rate-limited. Uses resolver SQLite for cursor state.
Shadow mode: detect + report but don't switch.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import sqlite3
import time

log = logging.getLogger("health_sweeper")


class HealthSweeper:
    """Rolling background health checker + upgrade evaluator."""

    def __init__(self, resolver, db_path: str, *,
                 items_per_hour: int = 100,
                 upgrade_enabled: bool = False,
                 upgrade_min_score_delta: float = 5.0,
                 min_source_age_s: float = 3600.0,
                 cooldown_after_repair_s: float = 3600.0,
                 cooldown_after_upgrade_s: float = 7200.0,
                 max_repairs_per_item_per_day: int = 3,
                 shadow_mode: bool = True):
        self.resolver = resolver
        self.db_path = db_path
        self.items_per_hour = items_per_hour
        self.upgrade_enabled = upgrade_enabled
        self.upgrade_min_score_delta = upgrade_min_score_delta
        self.min_source_age_s = min_source_age_s
        self.cooldown_after_repair_s = cooldown_after_repair_s
        self.cooldown_after_upgrade_s = cooldown_after_upgrade_s
        self.max_repairs_per_item_per_day = max_repairs_per_item_per_day
        self.shadow_mode = shadow_mode
        self._interval = 3600.0 / max(items_per_hour, 1)
        self._running = False
        self._task = None
        self._init_db()

    def _init_db(self):
        c = sqlite3.connect(self.db_path)
        c.executescript("""
CREATE TABLE IF NOT EXISTS health_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_checked_path TEXT, last_checked_at REAL
);
CREATE TABLE IF NOT EXISTS health_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
  plex_path TEXT, event TEXT, detail TEXT
);
CREATE TABLE IF NOT EXISTS source_history (
  plex_path TEXT, hash TEXT, score REAL, activated_at REAL,
  deactivated_at REAL, reason TEXT,
  PRIMARY KEY (plex_path, hash, activated_at)
);
CREATE TABLE IF NOT EXISTS repair_log (
  plex_path TEXT, ts REAL, old_hash TEXT, new_hash TEXT,
  old_score REAL, new_score REAL, result TEXT, duration_s REAL,
  PRIMARY KEY (plex_path, ts)
);
INSERT OR IGNORE INTO health_cursor (id, last_checked_path, last_checked_at)
  VALUES (1, NULL, 0);
""")
        c.commit()
        c.close()

    def _log_event(self, plex_path: str, event: str, detail: str = ""):
        c = sqlite3.connect(self.db_path)
        c.execute("INSERT INTO health_events (ts, plex_path, event, detail) VALUES (?,?,?,?)",
                  (time.time(), plex_path, event, detail[:200]))
        c.commit()
        c.close()
        log.info("%s %s %s", event, plex_path[:40], detail[:80])

    # ---------------------------------------------------------- rolling check
    def _next_batch(self, count: int) -> list[dict]:
        """Oldest-checked-first rolling selectie uit resolver-items."""
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        cur = c.execute("SELECT last_checked_path FROM health_cursor WHERE id=1").fetchone()
        cursor_path = cur["last_checked_path"] if cur else None
        all_items = self.resolver.store_items()
        if not all_items:
            c.close(); return []
        # sort by plex_path for stable ordering, start after cursor
        sorted_items = sorted(all_items, key=lambda m: m["plex_path"])
        if cursor_path:
            after = [m for m in sorted_items if m["plex_path"] > cursor_path]
            before = [m for m in sorted_items if m["plex_path"] <= cursor_path]
            ordered = after + before  # wrap around
        else:
            ordered = sorted_items
        return ordered[:count]

    def _update_cursor(self, last_path: str):
        c = sqlite3.connect(self.db_path)
        c.execute("UPDATE health_cursor SET last_checked_path=?, last_checked_at=? WHERE id=1",
                  (last_path, time.time()))
        c.commit()
        c.close()

    # ---------------------------------------------------------- health check
    def check_source(self, item: dict) -> dict:
        """Lightweight health check: byte-read op de actieve source."""
        result = {"plex_path": item["plex_path"], "status": item["status"],
                  "healthy": False, "repair_needed": False, "upgrade": None}
        if item["status"] != "READY":
            result["repair_needed"] = item["status"] == "NO_SOURCE"
            return result
        try:
            import urllib.request
            h = json.loads(urllib.request.urlopen(urllib.request.Request(
                f"http://127.0.0.1:8282/media/{item['id']}/open",
                method="POST"), timeout=60).read())
            handle = h["handle"]
            d1 = urllib.request.urlopen(
                f"http://127.0.0.1:8282/stream/{handle}?offset=0&length=64",
                timeout=60).read()
            d2 = urllib.request.urlopen(
                f"http://127.0.0.1:8282/stream/{handle}?offset=65536&length=64",
                timeout=60).read()
            urllib.request.urlopen(urllib.request.Request(
                f"http://127.0.0.1:8282/open/{handle}", method="DELETE"), timeout=30)
            result["healthy"] = (len(d1) == 64 and len(d2) == 64
                                 and d1[:4] == bytes.fromhex("1a45dfa3"))
        except Exception as e:
            result["error"] = repr(e)[:80]
            result["repair_needed"] = True
        return result

    # ---------------------------------------------------------- upgrade eval
    def _evaluate_upgrade(self, current_score: float, candidate_score: float) -> bool:
        return candidate_score >= current_score + self.upgrade_min_score_delta

    # ---------------------------------------------------------- sweep cycle
    async def sweep(self):
        """Eén sweep-cyclus: check een batch items."""
        batch = self._next_batch(min(self.items_per_hour, 50))
        if not batch:
            return
        log.info("sweep: %d items", len(batch))
        repairs = upgrades = checks = 0
        for item in batch:
            if not self._running:
                break
            try:
                result = self.check_source(item)
                checks += 1
                plex_path = item["plex_path"]
                if result["repair_needed"]:
                    self._log_event(plex_path, "sweep_repair_needed",
                                    item.get("status", ""))
                    if not self.shadow_mode:
                        await self._repair(item)
                        repairs += 1
                elif self.upgrade_enabled:
                    up = self._check_upgrade(item)
                    if up:
                        upgrades += 1
                        self._log_event(plex_path, "upgrade_available", up)
                self._update_cursor(plex_path)
            except Exception as e:
                log.warning("sweep item error: %s", e)
            await asyncio.sleep(self._interval)
        log.info("sweep done: %d checks, %d repairs, %d upgrades",
                 checks, repairs, upgrades)

    async def _repair(self, item: dict):
        """Trigger repair via resolver resolve endpoint."""
        try:
            import urllib.request
            urllib.request.urlopen(urllib.request.Request(
                f"http://127.0.0.1:8282/media/{item['id']}/resolve",
                method="POST"), timeout=300)
            self._log_event(item["plex_path"], "repair_triggered", "")
        except Exception as e:
            self._log_event(item["plex_path"], "repair_error", repr(e)[:80])

    def _check_upgrade(self, item: dict) -> str | None:
        """Check of er een duidelijk betere source beschikbaar is."""
        # placeholder: echte upgrade-evaluatie vereist candidate search
        # dit wordt geïmplementeerd als upgrade_enabled=true
        return None

    # ---------------------------------------------------------- run loop
    async def run(self):
        self._running = True
        log.info("health sweeper gestart: %d items/hour, shadow=%s, upgrade=%s",
                 self.items_per_hour, self.shadow_mode, self.upgrade_enabled)
        while self._running:
            try:
                await self.sweep()
            except Exception as e:
                log.error("sweep error: %s", e)
            await asyncio.sleep(self._interval * 10)

    def stop(self):
        self._running = False
