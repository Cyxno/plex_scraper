"""Background health sweeper + shadow repair + opportunistic upgrade.

Rolling, oldest-first, rate-limited cursor over the resolver item store.

Shadow mode (default): detect broken sources and evaluate alternatives
(scraper search -> rank -> identity gate) WITHOUT touching active sources
and without provider adds; every evaluation is logged as a health event
(``shadow_would_switch`` / ``shadow_no_alternative``).

Auto-repair mode (shadow_mode=false): additionally performs real repairs,
gated by AntiFlapping (cooldown + max/day) and NoSourceRetry (exponential
backoff for NO_SOURCE items).

Upgrade evaluation is report-only in every mode: when enabled it logs
``upgrade_available`` events but never switches sources.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time

from plex_scraper.resolver.selfheal import AntiFlapping, NoSourceRetry, identity_gate

log = logging.getLogger("health_sweeper")

RESOLVER_BASE = "http://127.0.0.1:8282"


class HealthSweeper:
    """Rolling background health checker + shadow repair + upgrade evaluator."""

    def __init__(self, resolver, db_path: str, *,
                 items_per_hour: int = 100,
                 upgrade_enabled: bool = False,
                 upgrade_min_score_delta: float = 5.0,
                 shadow_mode: bool = True,
                 max_repairs_per_item_per_day: int = 3,
                 cooldown_after_repair_s: float = 3600.0,
                 min_source_age_s: float = 3600.0,
                 no_source_base_s: float = 3600.0,
                 no_source_max_s: float = 86400.0,
                 fail_strikes: int = 2):
        self.resolver = resolver
        self.db_path = db_path
        self.items_per_hour = items_per_hour
        self.upgrade_enabled = upgrade_enabled
        self.upgrade_min_score_delta = upgrade_min_score_delta
        self.shadow_mode = shadow_mode
        self.antiflap = AntiFlapping(
            min_source_age_s=min_source_age_s,
            cooldown_repair_s=cooldown_after_repair_s,
            max_repairs_per_day=max_repairs_per_item_per_day)
        self.no_source_retry = NoSourceRetry(base_s=no_source_base_s,
                                             max_s=no_source_max_s)
        self.fail_strikes = max(1, fail_strikes)
        self._strikes: dict[str, int] = {}
        self._interval = 3600.0 / max(items_per_hour, 1)
        self._running = False
        self._task = None
        self._sweep_lock = asyncio.Lock()
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
CREATE INDEX IF NOT EXISTS idx_health_events_ts ON health_events(ts);
INSERT OR IGNORE INTO health_cursor (id, last_checked_path, last_checked_at)
  VALUES (1, NULL, 0);
""")
        c.commit()
        c.close()

    def _log_event(self, plex_path: str, event: str, detail: str = ""):
        c = sqlite3.connect(self.db_path)
        c.execute("INSERT INTO health_events (ts, plex_path, event, detail) VALUES (?,?,?,?)",
                  (time.time(), plex_path, event, detail[:400]))
        c.commit()
        c.close()
        log.info("%s %s %s", event, plex_path[:40], detail[:80])

    def _log_json(self, plex_path: str, event: str, payload: dict):
        self._log_event(plex_path, event, json.dumps(payload, default=str))

    # ---------------------------------------------------------- rolling check
    async def _next_batch(self, count: int) -> list:
        """Oldest-checked-first rolling selectie uit resolver-items."""
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        cur = c.execute("SELECT last_checked_path FROM health_cursor WHERE id=1").fetchone()
        c.close()
        cursor_path = cur["last_checked_path"] if cur else None
        all_items = await self.resolver.store.list_items()
        if not all_items:
            return []
        sorted_items = sorted(all_items, key=lambda m: m.plex_path)
        if cursor_path:
            after = [m for m in sorted_items if m.plex_path > cursor_path]
            before = [m for m in sorted_items if m.plex_path <= cursor_path]
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
    async def check_source(self, item) -> dict:
        """Lightweight health check: byte-read op de actieve source."""
        result = {"plex_path": item.plex_path, "status": item.status,
                  "healthy": False, "repair_needed": False, "upgrade": None}
        if item.status != "READY":
            # NO_SOURCE én SOURCE_FAILED: geen werkende actieve source.
            # RESOLVING/CANDIDATE_VALIDATION zijn in-flight en wachten we af.
            result["repair_needed"] = item.status in ("NO_SOURCE", "SOURCE_FAILED")
            return result
        try:
            import urllib.request

            def _read(url: str, timeout: float, attempts: int = 2) -> bytes:
                # één retry binnen dezelfde check: koude TorBox-starts zijn
                # traag maar werken; alleen structurele fouten (502 etc.)
                # moeten een strike opleveren
                last: Exception = RuntimeError("no attempt")
                for i in range(attempts):
                    try:
                        return urllib.request.urlopen(url, timeout=timeout).read()
                    except Exception as exc:            # noqa: BLE001
                        last = exc
                        if i + 1 < attempts:
                            time.sleep(2.0)
                raise last

            h = json.loads(urllib.request.urlopen(urllib.request.Request(
                f"{RESOLVER_BASE}/media/{item.id}/open",
                method="POST"), timeout=90).read())
            handle = h["handle"]
            d1 = _read(f"{RESOLVER_BASE}/stream/{handle}?offset=0&length=64", 120.0)
            d2 = _read(f"{RESOLVER_BASE}/stream/{handle}?offset=65536&length=64", 120.0)
            urllib.request.urlopen(urllib.request.Request(
                f"{RESOLVER_BASE}/open/{handle}", method="DELETE"), timeout=30)
            result["healthy"] = (len(d1) == 64 and len(d2) == 64
                                 and d1[:4] == bytes.fromhex("1a45dfa3"))
        except Exception as e:
            result["error"] = repr(e)[:80]
        if not result["healthy"]:
            result["repair_needed"] = True
        return result

    # ---------------------------------------------------------- shadow eval
    async def _shadow_evaluate(self, item) -> dict:
        """Zoek + rangschik + identity-gate alternatieven ZONDER te activeren.

        Gebruikt alleen scraper-zoekopdrachten en de checkcached-cache;
        geen provider adds, geen downloads, geen source switches.
        """
        out = {"candidates": 0, "identity_passed": 0, "identity_rejected": 0,
               "best": None, "current_score": None, "would_switch": False,
               "reject_reasons": []}
        try:
            candidates = await self.resolver._gather_candidates(item)
            ranked = await self.resolver._rank_candidates(item, candidates)
        except Exception as exc:
            out["error"] = repr(exc)[:120]
            return out
        out["candidates"] = len(candidates)
        current = await self.resolver._active_source(item.id)
        if current is not None:
            out["current_score"] = current.score
        best = None
        for cand, score in ranked:
            ok, why = identity_gate(item.title, item.series,
                                    item.season, item.episode,
                                    cand.torrent_name, item.year)
            if not ok:
                out["identity_rejected"] += 1
                if len(out["reject_reasons"]) < 3:
                    out["reject_reasons"].append({"name": cand.torrent_name[:60],
                                                  "reason": why})
                continue
            out["identity_passed"] += 1
            if best is None or score > best[1]:
                best = (cand, score)
        if best is None:
            return out
        out["best"] = {"hash": best[0].info_hash, "name": best[0].torrent_name[:80],
                       "score": round(best[1], 2)}
        if item.status == "NO_SOURCE":
            out["would_switch"] = True
        elif out["current_score"] is None or best[1] >= out["current_score"]:
            out["would_switch"] = True
        return out

    # ---------------------------------------------------------- repairs
    async def _repair(self, item) -> bool:
        """Echte repair via resolver resolve, met anti-flapping gate."""
        ok, why = self.antiflap.can_repair(item.plex_path)
        if not ok:
            self._log_json(item.plex_path, "repair_skipped_flapping", {"reason": why})
            return False
        try:
            import urllib.request
            urllib.request.urlopen(urllib.request.Request(
                f"{RESOLVER_BASE}/media/{item.id}/resolve",
                method="POST"), timeout=300)
            self.antiflap.record_repair(item.plex_path)
            self._log_event(item.plex_path, "repair_triggered", "")
            return True
        except Exception as e:
            self._log_json(item.plex_path, "repair_error", {"error": repr(e)[:120]})
            return False

    # ---------------------------------------------------------- sweep cycle
    async def sweep(self):
        """Eén sweep-cyclus: check een batch items.

        Geserialiseerd via lock: de achtergrondloop en check-now kunnen
        niet door elkaar heen checken (cursor wordt anders dubbel gelezen).
        """
        async with self._sweep_lock:
            await self._sweep_locked()

    async def _sweep_locked(self):
        batch = await self._next_batch(min(self.items_per_hour, 50))
        if not batch:
            return
        log.info("sweep: %d items (shadow=%s)", len(batch), self.shadow_mode)
        repairs = upgrades = checks = shadow_switches = 0
        for item in batch:
            plex_path = item.plex_path
            try:
                if item.status == "NO_SOURCE":
                    # backoff in beide modi: elke poging is een volledige
                    # scraper-zoekopdracht, dus die begrenzen we
                    if not self.no_source_retry.should_retry(plex_path):
                        self._update_cursor(plex_path)
                        await asyncio.sleep(self._interval)
                        continue
                    shadow = await self._shadow_evaluate(item)
                    if shadow.get("would_switch"):
                        self._log_json(plex_path, "no_source_would_recover", shadow)
                        self.no_source_retry.record_success(plex_path)
                        shadow_switches += 1
                    else:
                        self.no_source_retry.record_failure(plex_path)
                        self._log_json(plex_path, "no_source_backoff", {
                            "next_retry_s": round(
                                self.no_source_retry.next_retry_in(plex_path), 0),
                            "candidates": shadow.get("candidates", 0)})
                    self._update_cursor(plex_path)
                    await asyncio.sleep(self._interval)
                    continue

                result = await self.check_source(item)
                checks += 1
                if result["repair_needed"]:
                    strikes = self._strikes.get(plex_path, 0) + 1
                    self._strikes[plex_path] = strikes
                    if strikes < self.fail_strikes:
                        # debounce: één trage/failed read is nog geen bewijs
                        self._log_event(plex_path, "sweep_strike",
                                        json.dumps({"strike": strikes,
                                                    "of": self.fail_strikes,
                                                    "error": result.get("error")}))
                    else:
                        self._log_event(plex_path, "sweep_repair_needed",
                                        json.dumps({"status": item.status,
                                                    "strikes": strikes,
                                                    "error": result.get("error")}))
                        if self.shadow_mode:
                            shadow = await self._shadow_evaluate(item)
                            if shadow.get("would_switch"):
                                self._log_json(plex_path, "shadow_would_switch", shadow)
                                shadow_switches += 1
                            else:
                                self._log_json(plex_path, "shadow_no_alternative", shadow)
                        else:
                            if await self._repair(item):
                                repairs += 1
                elif self.upgrade_enabled:
                    shadow = await self._shadow_evaluate(item)
                    best = shadow.get("best")
                    if best and shadow.get("current_score") is not None \
                            and best["score"] >= shadow["current_score"] \
                            + self.upgrade_min_score_delta:
                        upgrades += 1
                        self._log_json(plex_path, "upgrade_available", shadow)
                if result.get("healthy"):
                    self.no_source_retry.record_success(plex_path)
                    self._strikes.pop(plex_path, None)
                self._update_cursor(plex_path)
            except Exception as e:
                log.warning("sweep item error: %s", e)
            await asyncio.sleep(self._interval)
        log.info("sweep done: %d checks, %d repairs, %d upgrades, "
                 "%d shadow_switches", checks, repairs, upgrades, shadow_switches)

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
