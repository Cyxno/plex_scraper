"""Per-session range streaming met adaptive read-ahead (the byte path).

Eén AdaptiveRangeReader per open handle:

- Sequentiële reads worden geserveerd uit een bounded window-buffer.
- Bij een window-miss: synchrone fetch van het gevraagde window; in 2-way
  mode wordt tegelijk het VOLGENDE window alvast opgehaald (bounded: max
  1 outstanding prefetch per reader, telt mee voor de globale upstream-
  semaphore).
- Seek buiten het prefetch-traject cancelt de prefetch (geen stale bytes);
  release/stop cancelt eveneens. Bytes worden uitsluitend geserveerd uit
  het huidige window — bytevolgorde is daarmee exact.
- Adaptive fallback: herhaalde provider-fouten tijdens 2-way schakelen de
  reader terug naar single-stream (geen runaway parallelisme).
"""
from __future__ import annotations

import asyncio
import logging

log = logging.getLogger("stream")


class AdaptiveRangeReader:
    def __init__(self, engine, source_id: str, size: int, readahead: int,
                 two_way: bool = False,
                 fallback_after_errors: int = 2):
        self.engine = engine
        self.source_id = source_id
        self.size = size
        self.readahead = readahead
        self.two_way = two_way
        self.fallback_after_errors = fallback_after_errors
        self._buf_off = -1
        self._buf = b""
        self._prefetch_off: int | None = None
        self._prefetch_task: asyncio.Task | None = None
        self._err_streak = 0
        self.closed = False
        # read-ahead-escalatie (incident Pirates 2026-10-10): een consument
        # met kleine sequentiële reads (Plex FUSE: 32 KiB) zonder read-ahead
        # levert 9–14 Mbit i.p.v. 37,5+ — de serialisatie tussen windows
        # (alleen het VOLGENDE window ophalen als two_way aan staat) wordt
        # dynamisch opgeheven zodra het patroon zichzelf bewijst
        self._last_end = -1
        self._seq_small = 0
        self._escalated = False

    # ------------------------------------------------------------ public
    async def read(self, offset: int, length: int) -> bytes:
        if offset >= self.size:
            return b""
        length = min(length, self.size - offset)
        if length <= 0:
            return b""

        # 1) hit in het huidige window
        if self._buf_off <= offset < self._buf_off + len(self._buf):
            data = self._buf[offset - self._buf_off:]
            if len(data) >= length:
                self._track_sequential(offset, length)
                self._maybe_prefetch(offset)
                return data[:length]

        # 2) hit in de lopende prefetch (volgend window is al onderweg)
        if (self._prefetch_task is not None
                and self._prefetch_off is not None
                and self._prefetch_off <= offset
                and offset < self._prefetch_off + self.readahead):
            off = self._prefetch_off
            try:
                data = await self._prefetch_task
                self.engine.metrics["prefetch_hits"] += 1
            except Exception:
                data = None                       # fallback naar synchroon
            finally:
                self._clear_prefetch()
            if data:
                self._buf_off, self._buf = off, data
                self._err_streak = 0
                self._track_sequential(offset, length)
                self._maybe_prefetch(offset)
                return data[offset - self._buf_off:][:length]

        # 3) miss: stale prefetch opruimen en synchroon fetchen
        self._track_sequential(offset, length, miss=True)
        self._clear_prefetch()
        data = await self._fetch(offset)
        self._buf_off, self._buf = offset, data
        self._maybe_prefetch(offset)
        return data[:length]

    def close(self) -> None:
        """FASE 18: client stopt → prefetch cancelen, buffers loslaten."""
        self.closed = True
        self._clear_prefetch()
        self._buf = b""
        self._buf_off = -1

    # ----------------------------------------------------------- internals
    def _track_sequential(self, offset: int, length: int,
                          miss: bool = False) -> None:
        """Escalatie-guard: kleine sequentiële reads (FUSE-consument) zonder
        read-ahead → schakel prefetch in na N bevestigde hits. Seeks/grote
        of random reads resetten de teller — geen runaway overfetch."""
        if miss:
            # miss op direct-opvolgend offset is alsnog sequentieel gedrag
            if self._last_end >= 0 and offset == self._last_end \
                    and length <= self._escalate_max_len():
                self._seq_small += 1
            else:
                self._seq_small = 0
        else:
            if (offset == self._last_end
                    and length <= self._escalate_max_len()):
                self._seq_small += 1
            else:
                self._seq_small = 0
        self._last_end = offset + length
        if (not self.two_way and not self._escalated
                and self._seq_small >= self._escalate_after()):
            self.two_way = True
            self._escalated = True
            self.engine.metrics["readahead_escalations"] += 1
            self._maybe_prefetch(offset)

    def _escalate_after(self) -> int:
        s = getattr(self.engine, "s", None)
        return max(1, int(getattr(s, "stream_seq_escalate_after", 6)))

    def _escalate_max_len(self) -> int:
        s = getattr(self.engine, "s", None)
        return max(1024, int(getattr(s, "stream_seq_escalate_max_len", 131072)))

    def _clear_prefetch(self) -> None:
        if self._prefetch_task is not None:
            self._prefetch_task.cancel()
            self._prefetch_task = None
            if self._prefetch_off is not None:
                self.engine.metrics["prefetch_cancelled_bytes"] += self.readahead
        self._prefetch_off = None

    def _maybe_prefetch(self, offset: int) -> None:
        """FASE 8: haal het volgende window alvast op (max 1 outstanding)."""
        if (not self.two_way or self.closed
                or self._prefetch_task is not None):
            return
        next_off = self._buf_off + len(self._buf)
        if next_off >= self.size:
            return
        # alleen prefetchen bij sequentieel leesgedrag; een ver seek
        # (verder dan 1 window vooruit) maakt prefetch zinloos
        if offset < self._buf_off - self.readahead or \
                offset > self._buf_off + len(self._buf) + self.readahead:
            return
        window = min(self.readahead, self.size - next_off)
        self._prefetch_off = next_off

        async def _run() -> bytes:
            try:
                data = await self._fetch(next_off)
                self.engine.metrics["prefetch_bytes"] += len(data)
                self._err_streak = 0
                return data
            except asyncio.CancelledError:
                raise
            except Exception as exc:                     # noqa: BLE001
                self._on_prefetch_error(exc)
                raise

        self._prefetch_task = asyncio.get_event_loop().create_task(_run())

    def _on_prefetch_error(self, exc: Exception) -> None:
        self._err_streak += 1
        self.engine.metrics["prefetch_errors"] += 1
        if self.two_way and self._err_streak >= self.fallback_after_errors:
            # FASE 12: adaptive fallback — 2-way presteert slechter, terug
            # naar single-stream voor deze sessie
            self.two_way = False
            self.engine.metrics["adaptive_fallbacks"] += 1
            log.info("adaptive fallback naar single-stream %s: %r",
                     self.source_id[:12], exc)

    async def _fetch(self, offset: int) -> bytes:
        window = min(max(self.readahead, 1), self.size - offset)
        try:
            data = await self.engine.upstream_read(self.source_id, offset, window)
            self._err_streak = 0
            return data
        except Exception:
            # een synchrone fetch-fout telt ook mee voor de fallback-guard
            self._on_prefetch_error(RuntimeError("sync fetch failed"))
            raise
