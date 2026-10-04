"""Runtime playback-delivery monitoring (FASE 5-9).

Per actieve sessie: rolling windows over de bytes die Toch al voor Plex
worden gelezen — geen extra provider-load, geen per-read DB-writes.

Staten (PLAYBACK_DELIVERY, onafhankelijk van source-validity):
  WARMING_UP → HEALTHY / MARGINAL / DEGRADED (met hysteresis) → RECOVERING

Drempels (FASE 7): MINIMUM_REALTIME = media_bitrate · TARGET = required
  HEALTHY : sustained ≥ target
  MARGINAL: minimum_realtime ≤ sustained < target
  DEGRADED: sustained < minimum_realtime, of ≥3 van laatste 5 windows onder
            media_bitrate, of severe stall zonder herstel

Detectie binnen ~15-30 s na warmup (FASE 9); één trage chunk is nooit een
trigger (FASE 8).
"""
from __future__ import annotations

import time
from collections import deque

WARMING_UP, HEALTHY, MARGINAL, DEGRADED, RECOVERING = (
    "WARMING_UP", "HEALTHY", "MARGINAL", "DEGRADED", "RECOVERING")


class DeliveryMonitor:
    """Lightweight per-session runtime validator. O(1) per read; events
    alleen op state-overgangen."""

    def __init__(self, plex_path: str, media_bitrate: float, target_mbit: float,
                 *, window_s: float = 5.0, warmup_s: float = 10.0,
                 degraded_ratio: float = 0.8, stall_s: float = 6.0,
                 recover_windows: int = 3):
        self.plex_path = plex_path
        self.media_bitrate = media_bitrate        # MINIMUM_REALTIME
        self.target_mbit = target_mbit            # required (bitrate × marge)
        self.window_s = window_s
        self.warmup_s = warmup_s
        self.degraded_ratio = degraded_ratio
        self.stall_s = stall_s
        self.recover_windows = recover_windows
        self.started = time.monotonic()
        self._now = time.monotonic            # injecteerbaar voor tests
        self._events_dq: deque = deque()          # (monotonic, bytes)
        self._windows: deque = deque(maxlen=8)    # laatste window-scores (mbit)
        self.state = WARMING_UP
        self.last_state_reported = WARMING_UP
        self.last_read_wait_s = 0.0
        # FASE 4-5: stall-tracking (bounded, in-memory)
        self.stall_minor_s = 3.0
        self.stall_severe_s = 5.0
        self.stall_extreme_s = 10.0
        self.stall_window_s = 120.0
        self.stall_total_ms = 12000.0
        self._stalls: deque = deque()          # (ts, wait_s) alleen ≥ minor
        self.stall_state = "OK"
        self.last_offset = -1
        self._pause_until = 0.0
        self.longest_stall_s = 0.0
        self.slow_windows = 0
        self.good_windows = 0
        self.degraded_since: float | None = None
        self.failed_over = False

    # -------------------------------------------------------------- feed
    def feed(self, nbytes: int, wait_s: float, seek: bool = False) -> None:
        now = self._now()
        self._events_dq.append((now, nbytes))
        self.last_read_wait_s = wait_s
        self.longest_stall_s = max(self.longest_stall_s, wait_s)
        # FASE 8: seek/pause geen stall — gap > 10 s = pauze/reconnect
        if seek or now < self._pause_until:
            return
        if wait_s >= self.stall_minor_s:
            self._stalls.append((now, wait_s))

    # ------------------------------------------------------- evaluatie
    def evaluate(self) -> dict:
        """Rolling windows berekenen en classificeren (throttled door de
        aanroeper: alleen bij reads, max 1× per window_s)."""
        now = self._now()
        self._evict(now)
        elapsed = now - self.started
        return self._classify(now, elapsed)

    def _evict(self, now: float) -> None:
        horizon = now - self.window_s * 8
        while self._events_dq and self._events_dq[0][0] < horizon:
            self._events_dq.popleft()

    # windows: verdeel de events in window_s-blokken
    def _classify(self, now: float, elapsed: float) -> dict:
        if not self._events_dq:
            return {"state": self.state, "stall_state": self.stall_state,
                    "rolling_mbit": 0.0, "elapsed_s": round(elapsed, 1),
                    "warming": elapsed < self.warmup_s, "stalls": 0,
                    "severe_stalls": 0, "max_stall_s": 0, "total_stalled_ms": 0,
                    "reason": "", "min_realtime": self.media_bitrate,
                    "target": self.target_mbit}
        # rolling 15 s
        horizon = now - 15.0
        b15 = sum(b for t, b in self._events_dq if t >= horizon)
        t15 = min(15.0, max(0.001, now - max(self._events_dq[0][0], horizon)))
        rolling_mbit = b15 * 8 / 1e6 / t15
        # windows over de laatste 5×window_s
        wstart = now - self.window_s * 5
        buckets: dict[int, int] = {}
        for t, b in self._events_dq:
            if t >= wstart:
                buckets[int((t - wstart) // self.window_s)] = \
                    buckets.get(int((t - wstart) // self.window_s), 0) + b
        wmbits = [v * 8 / 1e6 / self.window_s for _, v in sorted(buckets.items())]
        under_realtime = sum(1 for m in wmbits if m < self.media_bitrate)
        under_target = sum(1 for m in wmbits if m < self.target_mbit)
        self.slow_windows = under_realtime
        min_realtime = self.media_bitrate
        target = self.target_mbit

        # FASE 5/6: stall-window staat los van warmup — een 6s-stall in de
        # eerste seconden is reëel bewijs (cold-read excepted via minor-grens)
        cutoff = now - self.stall_window_s
        while self._stalls and self._stalls[0][0] < cutoff:
            self._stalls.popleft()
        severe = [w for _, w in self._stalls if w >= self.stall_severe_s]
        total_ms = int(sum(w for _, w in self._stalls) * 1000)
        if (len(severe) >= 2 or len(self._stalls) >= 3
                or total_ms >= self.stall_total_ms
                or any(w >= self.stall_extreme_s for _, w in self._stalls)):
            self.stall_state = "STALL_DEGRADED"
        elif self._stalls:
            self.stall_state = "STALL_WARNING"
        else:
            self.stall_state = "OK"
        if elapsed < self.warmup_s:
            self.state = WARMING_UP
            worst = {"DEGRADED": 3, "STALL_DEGRADED": 3, "RECOVERING": 2,
                     "MARGINAL": 1, "STALL_WARNING": 1, "HEALTHY": 0,
                     "WARMING_UP": 0, "OK": 0}
            final = self.state if worst.get(self.state, 0) >= worst.get(self.stall_state, 0) \
                else self.stall_state
            return {"state": final, "throughput_state": self.state,
                    "stall_state": self.stall_state,
                    "reason": ("STALL_DEGRADED" if self.stall_state == "STALL_DEGRADED"
                               else ""),
                    "stalls": len(self._stalls), "severe_stalls": len(severe),
                    "max_stall_s": round(max((w for _, w in self._stalls), default=0), 2),
                    "total_stalled_ms": total_ms,
                    "rolling_mbit": round(rolling_mbit, 1),
                    "elapsed_s": round(elapsed, 1), "warming": True,
                    "min_realtime": min_realtime, "target": target,
                    "slow_windows": under_realtime, "windows": len(wmbits)}

        # FASE 8: rolling evidence, geen reactie op één dip
        degraded_evidence = (
            (rolling_mbit < min_realtime * 0.9) or
            (under_realtime >= 3 and len(wmbits) >= 3) or
            (self.longest_stall_s >= self.stall_s and rolling_mbit < target))
        healthy_evidence = rolling_mbit >= target

        prev = self.state
        if degraded_evidence:
            if prev in (HEALTHY, MARGINAL, WARMING_UP):
                self.good_windows = 0
                self.state = DEGRADED
                self.degraded_since = now
            elif prev == RECOVERING:
                self.good_windows = 0
                self.state = DEGRADED
        elif healthy_evidence:
            self.good_windows += 1
            if prev == DEGRADED and self.good_windows >= self.recover_windows:
                self.state = RECOVERING
            elif prev == WARMING_UP and self.good_windows >= 1:
                self.state = HEALTHY
            elif prev in (RECOVERING, MARGINAL) and self.good_windows >= self.recover_windows:
                self.state = HEALTHY
            elif prev == WARMING_UP:
                self.state = HEALTHY
        else:  # tussen minimum_realtime en target → MARGINAL-zone
            self.good_windows = 0
            if prev in (HEALTHY, WARMING_UP):
                # hysteresis: HEALTHY zakt pas na 2 windows onder target
                if under_target >= 2:
                    self.state = MARGINAL
            elif prev == RECOVERING:
                self.state = MARGINAL
            elif prev == MARGINAL:
                if rolling_mbit < min_realtime and under_realtime >= 2:
                    self.state = DEGRADED
                    self.degraded_since = self.degraded_since or now
        prev_stall = self.stall_state
        if (len(severe) >= 2 or len(self._stalls) >= 3
                or total_ms >= self.stall_total_ms
                or any(w >= self.stall_extreme_s for _, w in self._stalls)):
            self.stall_state = "STALL_DEGRADED"
        elif self._stalls:
            self.stall_state = "STALL_WARNING"
        else:
            self.stall_state = "OK"
        if prev_stall == "STALL_DEGRADED" and self.stall_state != "STALL_DEGRADED" \
                and now - (self._stalls[-1][0] if self._stalls else 0) > self.stall_window_s / 2:
            self.stall_state = "OK"                # FASE 22: herstel pas na rust
        worst = {"DEGRADED": 3, "STALL_DEGRADED": 3, "RECOVERING": 2,
                 "MARGINAL": 1, "STALL_WARNING": 1, "HEALTHY": 0,
                 "WARMING_UP": 0, "OK": 0}
        final = self.state if worst.get(self.state, 0) >= worst.get(self.stall_state, 0) \
            else self.stall_state
        return {"state": final, "throughput_state": self.state,
                "stall_state": self.stall_state,
                "reason": ("STALL_DEGRADED" if self.stall_state == "STALL_DEGRADED"
                           else "THROUGHPUT_DEGRADED" if self.state == DEGRADED else ""),
                "stalls": len(self._stalls), "severe_stalls": len(severe),
                "max_stall_s": round(max((w for _, w in self._stalls), default=0), 2),
                "total_stalled_ms": total_ms,
                "rolling_mbit": round(rolling_mbit, 1),
                "elapsed_s": round(elapsed, 1), "warming": False,
                "min_realtime": min_realtime, "target": target,
                "slow_windows": under_realtime, "windows": len(wmbits)}

    def _tp_bad_streak(self) -> int:
        return min(2, self.slow_windows)

    def snapshot(self) -> dict:
        now = time.monotonic()
        out = self._classify(now, now - self.started)
        out.update({"plex_path": self.plex_path,
                    "longest_stall_s": round(self.longest_stall_s, 2)})
        return out
