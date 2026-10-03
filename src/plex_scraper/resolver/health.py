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
                 fail_strikes: int = 2,
                 playback_pause: bool = True,
                 playback_min_mbit: float = 25.0,
                 throughput_margin: float = 1.5,
                 throughput_strikes: int = 3,
                 throughput_probe_interval_s: float = 3600.0):
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
        # DOEL 6/7/8
        self.playback_pause = playback_pause
        self.playback_min_mbit = playback_min_mbit
        self.throughput_margin = throughput_margin
        self.throughput_strikes = max(1, throughput_strikes)
        self.throughput_probe_interval_s = throughput_probe_interval_s
        self._tp_strikes: dict[str, int] = {}
        self._degraded: set[str] = set()
        self._last_probe: dict[str, float] = {}
        self._pause_logged_until = 0.0
        self._interval = 3600.0 / max(items_per_hour, 1)
        self._running = False
        self._task = None
        self._sweep_lock = asyncio.Lock()
        self._init_db()

    @property
    def degraded_throughput(self) -> set[str]:
        return set(self._degraded)

    def _required_mbit(self) -> float:
        """Vereiste doorvoer: fallback-mediabitrate × veiligheidsmarge
        (DOEL 7; per-item bitrate volgt in een latere fase)."""
        return self.playback_min_mbit * self.throughput_margin

    async def playback_active_count(self) -> int:
        """Aantal sessies met recente reads (actieve playback)."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                r = await client.get(f"{RESOLVER_BASE}/api/playback/active")
                r.raise_for_status()
                return int(r.json().get("active", 0))
        except Exception:                                 # noqa: BLE001
            return 0

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
        """Health check: EBML byte-read + seek + DOEL 7-doorvoermeting."""
        result = {"plex_path": item.plex_path, "status": item.status,
                  "healthy": False, "repair_needed": False, "upgrade": None}
        if item.status != "READY":
            # NO_SOURCE én SOURCE_FAILED: geen werkende actieve source.
            # RESOLVING/CANDIDATE_VALIDATION zijn in-flight en wachten we af.
            result["repair_needed"] = item.status in ("NO_SOURCE", "SOURCE_FAILED")
            return result
        try:
            import httpx
            # httpx ASYNC: urllib-blocking calls hier zouden de resolver-
            # eventloop bevriezen (sweeper draait in hetzelfde proces) —
            # dat manifesteerde zich als 90s-stalls van élke API-call.
            t = httpx.Timeout(90.0, read=120.0)
            async with httpx.AsyncClient(timeout=t) as client:
                h = (await client.post(
                    f"{RESOLVER_BASE}/media/{item.id}/open")).json()
                handle = h["handle"]

                async def _read(url: str) -> bytes:
                    # één retry binnen dezelfde check: koude TorBox-starts
                    # zijn traag maar werken; alleen structurele fouten
                    # (502 etc.) moeten een strike opleveren
                    last: Exception = RuntimeError("no attempt")
                    for i in range(2):
                        try:
                            r = await client.get(url)
                            r.raise_for_status()
                            return r.content
                        except Exception as exc:      # noqa: BLE001
                            last = exc
                            if i + 1 < 2:
                                await asyncio.sleep(2.0)
                    raise last

                # DOEL 7: eerste read = 1 MB, meten TTFB + doorvoer; deze
                # read dient tegelijk als EBML-check (geen extra request)
                t0 = time.monotonic()
                d1 = await _read(
                    f"{RESOLVER_BASE}/stream/{handle}?offset=0&length=1048576")
                ttfb_s = round(time.monotonic() - t0, 2)
                mbit = len(d1) * 8 / 1e6 / max(ttfb_s, 1e-6)
                d2 = await _read(
                    f"{RESOLVER_BASE}/stream/{handle}?offset=65536&length=65536")
                await client.delete(f"{RESOLVER_BASE}/open/{handle}")
            result["healthy"] = (len(d1) >= 64 and len(d2) == 65536
                                 and d1[:4] == bytes.fromhex("1a45dfa3"))
            result["ttfb_s"] = ttfb_s
            result["mbit"] = round(mbit, 1)
            result["required_mbit"] = round(self._required_mbit(), 1)
            self._throughput_observe(item.plex_path, result)
        except Exception as e:
            result["error"] = repr(e)[:80]
        if not result["healthy"]:
            result["repair_needed"] = True
        return result

    # ------------------------------------------------- DOEL 7: throughput
    def _throughput_observe(self, plex_path: str, result: dict) -> None:
        """Shadow-classificatie: pas na N opeenvolgende slechte observaties
        (strikes) ontstaat DEGRADED_THROUGHPUT; één trage meting nooit."""
        required = result.get("required_mbit", 0)
        mbit = result.get("mbit", 0.0)
        ttfb = result.get("ttfb_s", 0.0)
        bad = mbit < required or ttfb > 5.0
        if not bad:
            if plex_path in self._degraded:
                self._degraded.discard(plex_path)
                self._log_json(plex_path, "throughput_recovered",
                               {"mbit": mbit, "required_mbit": required})
            self._tp_strikes.pop(plex_path, None)
            return
        strikes = self._tp_strikes.get(plex_path, 0) + 1
        self._tp_strikes[plex_path] = strikes
        if strikes >= self.throughput_strikes and plex_path not in self._degraded:
            self._degraded.add(plex_path)
            self._log_json(plex_path, "throughput_degraded", {
                "mbit": mbit, "ttfb_s": ttfb, "required_mbit": required,
                "strikes": strikes})
        elif plex_path not in self._degraded:
            self._log_event(plex_path, "throughput_strike",
                            json.dumps({"strike": strikes,
                                        "of": self.throughput_strikes,
                                        "mbit": mbit, "ttfb_s": ttfb}))

    async def _throughput_would_switch(self, item, measured_mbit: float) -> None:
        """DOEL 8: shadow throughput-repair — zoek kandidaten, identity-gate,
        doorvoer-probe op de beste cached kandidaat, rapporteer WOULD SWITCH.
        Switcht NOOIT; alleen event-rapportage."""
        plex_path = item.plex_path
        last = self._last_probe.get(plex_path, 0.0)
        if time.time() - last < self.throughput_probe_interval_s:
            return
        self._last_probe[plex_path] = time.time()
        try:
            shadow = await self._shadow_evaluate(item)
            best = shadow.get("best")
            if not best:
                self._log_json(plex_path, "throughput_no_alternative",
                               {"identity_rejected": shadow.get("identity_rejected")})
                return
            cand_hash = best["hash"]
            src = None
            for s in await self.resolver.store.list_sources(item.id):
                if s.info_hash == cand_hash:
                    src = s
                    break
            if src is None or not src.cached:
                # alleen cached kandidaten proben: geen provider-add-druk
                self._log_json(plex_path, "throughput_probe_skipped",
                               {"reason": "best candidate not cached",
                                "hash": cand_hash[:16]})
                return
            torrent = await self.resolver.provider.ensure_torrent(
                src.info_hash, src.torrent_name)
            url = await self.resolver.provider.get_stream_url(
                torrent.torrent_id, src.file_id or 0)
            t0 = time.monotonic()
            data = await self.resolver.provider.read_range(
                url, min(int(1e8), max(0, int(src.size or 0) - 8388608)), 8388608)
            dt = max(time.monotonic() - t0, 1e-6)
            cand_mbit = len(data) * 8 / 1e6 / dt
            required = self._required_mbit()
            improvement = cand_mbit / max(measured_mbit, 0.1)
            if cand_mbit >= required and improvement >= 1.5:
                self._log_json(plex_path, "throughput_would_switch", {
                    "current_mbit": round(measured_mbit, 1),
                    "candidate_mbit": round(cand_mbit, 1),
                    "improvement": round(improvement, 2),
                    "required_mbit": round(required, 1),
                    "candidate_hash": cand_hash})
            else:
                self._log_json(plex_path, "throughput_probe_no_better", {
                    "current_mbit": round(measured_mbit, 1),
                    "candidate_mbit": round(cand_mbit, 1),
                    "required_mbit": round(required, 1)})
        except Exception as exc:                          # noqa: BLE001
            self._log_json(plex_path, "throughput_probe_error",
                           {"error": repr(exc)[:120]})

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
            import httpx
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=300.0)) as client:
                await client.post(f"{RESOLVER_BASE}/media/{item.id}/resolve")
            self.antiflap.record_repair(item.plex_path)
            self._log_event(item.plex_path, "repair_triggered", "")
            return True
        except Exception as e:
            self._log_json(item.plex_path, "repair_error", {"error": repr(e)[:120]})
            return False

    # ---------------------------------------------------------- NO_SOURCE
    async def handle_no_source(self, item) -> dict:
        """Per-item NO_SOURCE-afhandeling (deelp door sweep en check-item).

        Backoff-gated; shadow evalueert report-only, auto-mode repareert
        echt en leest het resultaat terug.
        """
        plex_path = item.plex_path
        if not self.no_source_retry.should_retry(plex_path):
            return {"skipped": "backoff",
                    "next_retry_s": round(
                        self.no_source_retry.next_retry_in(plex_path), 0)}
        if self.shadow_mode:
            shadow = await self._shadow_evaluate(item)
            if shadow.get("would_switch"):
                self._log_json(plex_path, "no_source_would_recover", shadow)
                self.no_source_retry.record_success(plex_path)
                return {"shadow": shadow}
            self.no_source_retry.record_failure(plex_path)
            payload = {"next_retry_s": round(
                self.no_source_retry.next_retry_in(plex_path), 0),
                "candidates": shadow.get("candidates", 0)}
            self._log_json(plex_path, "no_source_backoff", payload)
            return {"backoff": payload}
        # auto-mode: echte repair — resolve doet zijn eigen zoekactie;
        # uitlezen wat het resultaat is
        await self._repair(item)
        fresh = await self.resolver.store.get_item(item.id)
        if fresh is not None and fresh.status == "READY":
            self.no_source_retry.record_success(plex_path)
            self._log_event(plex_path, "no_source_recovered", "")
            return {"recovered": True}
        self.no_source_retry.record_failure(plex_path)
        payload = {"next_retry_s": round(
            self.no_source_retry.next_retry_in(plex_path), 0)}
        self._log_json(plex_path, "no_source_backoff", payload)
        return {"backoff": payload}

    # ---------------------------------------------------------- sweep cycle
    async def sweep(self):
        """Eén sweep-cyclus: check een batch items.

        Geserialiseerd via lock: de achtergrondloop en check-now kunnen
        niet door elkaar heen checken (cursor wordt anders dubbel gelezen).
        """
        async with self._sweep_lock:
            await self._sweep_locked()

    async def _sweep_locked(self):
        # DOEL 6: geen background traffic tijdens actieve playback —
        # geen health sweeps, geen repairs, geen throughput-probes
        if self.playback_pause:
            active = await self.playback_active_count()
            if active > 0:
                if time.time() >= self._pause_logged_until:
                    self._log_json("sweeper", "sweep_paused_playback",
                                   {"active_streams": active})
                    self._pause_logged_until = time.time() + 1800.0
                log.info("sweep gepauzeerd: %d actieve playback-streams", active)
                return

        # DOEL 3-watchdog: tussenstanden die vastgelopen zijn reconciliëren
        try:
            stale = await self.resolver.store.reconcile_stale(900.0)
            for s in stale:
                self._log_json("sweeper", "stale_state_reconciled", s)
        except Exception as e:                            # noqa: BLE001
            log.warning("stale reconcile fout: %s", e)

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
                    await self.handle_no_source(item)
                    self._update_cursor(plex_path)
                    await asyncio.sleep(self._interval)
                    continue

                result = await self.check_source(item)
                checks += 1
                if result["repair_needed"]:
                    # strikes debounceën alleen flaky READS (item was READY);
                    # definitive states (SOURCE_FAILED/NO_SOURCE) meten direct
                    if item.status == "READY":
                        strikes = self._strikes.get(plex_path, 0) + 1
                        self._strikes[plex_path] = strikes
                    else:
                        strikes = self.fail_strikes
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
                elif plex_path in self._degraded:
                    # DOEL 8: shadow throughput-repair (rapporteert alleen)
                    await self._throughput_would_switch(
                        item, result.get("mbit", 0.0))
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
