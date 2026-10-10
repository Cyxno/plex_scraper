"""Per-session range streaming met adaptive read-ahead (the byte path).

Eén AdaptiveRangeReader per open handle:

- Windows zijn READAHEAD-gealigneerd (dedup: hetzelfde window wordt nooit
  twee keer tegelijk opgehaald — in-flight-registry per window-offset).
- Twee modes:
    * SEEK_RECOVERY_PREFETCH (recovery=True): cold open / seek / Cue-jump /
      probe-fase — tot 3 outstanding remote windows (P0 = gevraagd window,
      P1 = volgende sequentiële window, P2 = speculatief alleen bij
      sequentie-vooruitgang). Doel: aggregaat-doorvoer tijdens de probe/jump-
      fase (bewezen: 1 verbinding ≈ 17–39 Mbit, 3 parallel ≈ 103 Mbit).
    * normaal (recovery=False, two_way=True): één outstanding prefetch —
      het bewezen gedrag voor sequentiële playback.
- De-escalatie: 2 opeenvolgende volledig sequentieel geconsumeerde windows
  zonder jump/cancel → recovery uit, terug naar normaal.
- Jump/seek buiten het buffertraject: obsolete in-flight/ready windows
  worden gecanceld (geen stale bytes), recovery heractiveert.
- Session close / reconnect / material switch: engine sluit sessies —
  close() cancelt álles; oude futures kunnen nooit een nieuwe generatie
  bedienen (reader is per sessie/source gebonden).
- Adaptive fallback: herhaalde provider-fouten schakelen terug naar
  single-stream (geen runaway parallelisme).
- Metrics: plain dict met safe increments — ontbrekende keys gooien nooit.
"""
from __future__ import annotations

import asyncio
import logging

log = logging.getLogger("stream")

MAX_INFLIGHT_RECOVERY = 3
MAX_INFLIGHT_NORMAL = 1
MAX_READY_WINDOWS = 2
DEESCALATE_AFTER_WINDOWS = 2


class AdaptiveRangeReader:
    def __init__(self, engine, source_id: str, size: int, readahead: int,
                 two_way: bool = False,
                 fallback_after_errors: int = 2):
        self.engine = engine
        self.source_id = source_id          # source-bound: geen cross-source hergebruik
        self.size = size
        self.readahead = readahead
        self.two_way = two_way              # normaal: 1 outstanding prefetch
        self.recovery = False               # SEEK_RECOVERY_PREFETCH (cap 3)
        self.fallback_after_errors = fallback_after_errors
        self._buf_off = -1
        self._buf = b""
        self._inflight: dict[int, asyncio.Task] = {}   # window-offset → task
        self._ready: dict[int, bytes] = {}             # klaar, nietconsumeerde windows
        self._err_streak = 0
        self.closed = False
        # sequentie-detectie (escalatie + de-escalatie)
        self._last_end = -1
        self._seq_small = 0
        self._escalated = False
        self._seq_windows = 0               # volledig sequentieel geconsumeerde windows

    # ------------------------------------------------------------ metrics
    def _m(self, key: str, delta: int | float = 1) -> None:
        """Safe increment — metrics zijn een plain dict; een ontbrekende key
        mag nooit een KeyError/404/EIO veroorzaken (regressie 2026-10-10)."""
        try:
            m = self.engine.metrics
            m[key] = m.get(key, 0) + delta
        except Exception:                        # noqa: BLE001 — never throw
            pass

    # ------------------------------------------------------------ public
    async def read(self, offset: int, length: int) -> bytes:
        if offset >= self.size:
            return b""
        length = min(length, self.size - offset)
        if length <= 0:
            return b""
        # reads kunnen een window-grens crossingen — per aligned window
        # bedienen (byte-exact, nooit bytes over een grens heen mengen)
        out = bytearray()
        while length > 0:
            n = min(length, self.readahead - (offset % max(self.readahead, 1)))
            chunk = await self._read_window(offset, n)
            if not chunk:
                break
            out += chunk
            offset += len(chunk)
            length -= len(chunk)
        return bytes(out)

    async def _read_window(self, offset: int, length: int) -> bytes:
        wo = self._window_of(offset)
        length = min(length, self.readahead - (offset - wo))

        # 1) hit in het huidige window
        if self._buf_off <= offset < self._buf_off + len(self._buf):
            data = self._buf[offset - self._buf_off:]
            if len(data) >= length:
                self._track_sequential(offset, length)
                self._count_window_progress(offset)
                self._schedule(offset)
                return data[:length]

        # 2) klaarliggend (reeds gefetcht, nog niet geconsumeerd) window
        if wo in self._ready:
            data = self._ready.pop(wo)
            self._buf_off, self._buf = wo, data
            self._m("remote_window_reused")
            self._m("speculative_bytes_used", len(data))
            self._err_streak = 0
            self._track_sequential(offset, length)
            self._count_window_progress(offset)
            self._schedule(offset)
            return data[offset - self._buf_off:][:length]

        # 3) in-flight window: wait op de gedeelde task (dedup — dezelfde
        #    remote window wordt nooit dubbel opgehaald)
        if wo in self._inflight:
            task = self._inflight.pop(wo)
            try:
                data = await task
                self._ready.pop(wo, None)        # geen double-hold
                self._m("remote_window_reused")
                self._m("speculative_bytes_used", len(data))
            except asyncio.CancelledError:
                raise
            except Exception:
                data = None                      # fallback naar synchroon
            if data:
                self._buf_off, self._buf = wo, data
                self._err_streak = 0
                self._track_sequential(offset, length)
                self._count_window_progress(offset)
                self._schedule(offset)
                return data[offset - self._buf_off:][:length]
            # gefaald → val door naar de synchrone miss-pad hieronder

        # 4) miss: jump-detectie, obsolete werk cancelen, synchroon fetchen
        fresh_session = self._last_end == -1
        jumped = self._is_jump(offset, wo)
        self._track_sequential(offset, length, miss=True)
        self._cancel_obsolete(wo)
        if jumped or fresh_session:
            self._activate_recovery(jumped=jumped, fresh=fresh_session)
        data = await self._fetch(wo)
        self._buf_off, self._buf = wo, data
        self._count_window_progress(offset)
        self._schedule(offset)
        return data[offset - self._buf_off:][:length]

    def close(self) -> None:
        """Client stopt / reconnect / material switch: alle outstanding
        werk cancelen, buffers loslaten. Geen orphan background requests."""
        self.closed = True
        self._cancel_all_inflight()
        self._ready.clear()
        self._buf = b""
        self._buf_off = -1

    # ----------------------------------------------------------- internals
    def _window_of(self, offset: int) -> int:
        """Gealigneerd window-voetspoor: dedup voorspelbaar per offset."""
        ra = max(self.readahead, 1)
        return (offset // ra) * ra

    def _is_jump(self, offset: int, wo: int) -> bool:
        """Niet-sequentiële sprong: ver van het huidige window en niet
        bediend door ready/in-flight."""
        if self._buf_off < 0:
            return False
        return not (self._buf_off - self.readahead <= offset
                    <= self._buf_off + len(self._buf) + self.readahead) \
            and wo not in self._inflight and wo not in self._ready

    def _activate_recovery(self, jumped: bool, fresh: bool) -> None:
        if self.recovery:
            return
        self.recovery = True
        self._seq_windows = 0
        self._m("seek_recovery_activated")
        self._m("readahead_escalations")
        if jumped:
            self._m("seek_recovery_jump")

    def _deescalate(self) -> None:
        if not self.recovery:
            return
        self.recovery = False
        self._m("seek_recovery_deescalated")

    def _count_window_progress(self, offset: int) -> None:
        """De-escalatie: 2 opeenvolgende volledig sequentieel geconsumeerde
        windows zonder jump → recovery uit (normale playback)."""
        if not self.recovery:
            return
        if self._last_end >= 0 and \
                self._buf_off <= self._last_end < self._buf_off + len(self._buf):
            self._seq_windows += 1
        else:
            self._seq_windows = 0
        if self._seq_windows >= DEESCALATE_AFTER_WINDOWS:
            self._deescalate()

    def _cancel_obsolete(self, keep_wo: int) -> None:
        """Jump: in-flight/ready windows die het nieuwe demargebied niet
        kunnen bedienen onmiddellijk cancelen/verwijderen."""
        lo, hi = keep_wo - self.readahead, keep_wo + 2 * self.readahead
        for w in [w for w in self._inflight if not (lo <= w <= hi)]:
            task = self._inflight.pop(w)
            task.cancel()
            self._m("remote_window_cancelled")
            self._m("seek_recovery_cancelled")
            self._m("speculative_bytes_wasted", self._window_len(w))
        for w in [w for w in self._ready if not (lo <= w <= hi)]:
            self._m("speculative_bytes_wasted", len(self._ready.pop(w)))
        if self._inflight or self._ready:
            self._seq_windows = 0

    def _cancel_all_inflight(self) -> None:
        for w, task in list(self._inflight.items()):
            task.cancel()
            self._m("remote_window_cancelled")
            self._m("speculative_bytes_wasted", self._window_len(w))
        self._inflight.clear()

    def _window_len(self, wo: int) -> int:
        return min(self.readahead, max(0, self.size - wo))

    def _cap(self) -> int:
        return MAX_INFLIGHT_RECOVERY if self.recovery else MAX_INFLIGHT_NORMAL

    def _track_sequential(self, offset: int, length: int,
                          miss: bool = False) -> None:
        """Escalatie-guard (normale mode): kleine sequentiële reads (FUSE)
        zonder prefetch → single-prefetch na N bevestigde hits. Jumps/grote
        reads resetten de teller."""
        if (miss and self._last_end >= 0 and offset == self._last_end
                or not miss and offset == self._last_end) and \
                length <= self._escalate_max_len():
            self._seq_small += 1
        else:
            self._seq_small = 0
        self._last_end = offset + length
        if (not self.two_way and not self.recovery and not self._escalated
                and self._seq_small >= self._escalate_after()):
            self.two_way = True
            self._escalated = True
            self._m("readahead_escalations")

    def _escalate_after(self) -> int:
        s = getattr(self.engine, "s", None)
        return max(1, int(getattr(s, "stream_seq_escalate_after", 6)))

    def _escalate_max_len(self) -> int:
        s = getattr(self.engine, "s", None)
        return max(1024, int(getattr(s, "stream_seq_escalate_max_len", 131072)))

    def _schedule(self, offset: int) -> None:
        """Demand-aware scheduling: P0 = gedemandeerd window (loopt al),
        P1 = volgende sequentiële window, P2 = speculatief derde window
        alléén in recovery-mode met sequentie-vooruitgang. Cap: max 3
        outstanding in recovery, 1 normaal; ready+inflight gebonden."""
        if self.closed or not self.two_way and not self.recovery:
            return
        cap = self._cap()
        if self._buf_off < 0:
            return
        base = self._buf_off + len(self._buf)
        if base >= self.size:
            return
        next_w = base                    # _buf_off is altijd gealigneerd
        # sequentie-vooruitgang vereist voor P2 (geen blind lineair giswerk
        # tijdens jump-fases)
        allow_p2 = self.recovery and self._seq_windows >= 1
        started = 0
        for i, w in enumerate((next_w, next_w + self.readahead)):
            if w >= self.size:
                break
            if w in self._inflight or w in self._ready:
                self._m("duplicate_fetch_avoided")
                continue
            if len(self._inflight) + len(self._ready) >= cap:
                break
            if i == 1 and not allow_p2:
                break
            wl = self._window_len(w)
            self._m("remote_window_started")
            if i == 1:
                self._m("speculative_bytes_requested", wl)
            self._inflight[w] = asyncio.get_event_loop().create_task(
                self._fetch_window(w))
            started += 1
        try:
            self.engine.metrics["concurrent_windows_current"] = len(self._inflight)
            peak = self.engine.metrics.get("concurrent_windows_peak", 0)
            if len(self._inflight) > peak:
                self.engine.metrics["concurrent_windows_peak"] = len(self._inflight)
        except Exception:                        # noqa: BLE001
            pass

    async def _fetch_window(self, wo: int) -> bytes:
        try:
            data = await self._fetch(wo)
            # klaar → ready (geconsumeerd bij volgende read); cap ready
            while len(self._ready) >= MAX_READY_WINDOWS:
                old = min(self._ready)
                self._m("speculative_bytes_wasted", len(self._ready.pop(old)))
            self._ready[wo] = data
            self._m("prefetch_bytes", len(data))
            self._err_streak = 0
            return data
        except asyncio.CancelledError:
            raise
        except Exception as exc:                 # noqa: BLE001
            self._inflight.pop(wo, None)
            self._on_prefetch_error(exc)
            raise

    def _on_prefetch_error(self, exc: Exception) -> None:
        self._err_streak += 1
        self._m("prefetch_errors")
        if self._err_streak >= self.fallback_after_errors:
            # adaptive fallback — parallel schaalt niet, terug naar single
            self.recovery = False
            self.two_way = False
            self._m("adaptive_fallbacks")
            log.info("adaptive fallback naar single-stream %s: %r",
                     self.source_id[:12], exc)

    async def _fetch(self, offset: int) -> bytes:
        window = min(max(self.readahead, 1), self.size - offset)
        try:
            data = await self.engine.upstream_read(self.source_id, offset, window)
            self._err_streak = 0
            return data
        except Exception:
            self._on_prefetch_error(RuntimeError("sync fetch failed"))
            raise
