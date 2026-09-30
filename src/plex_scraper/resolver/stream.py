"""Per-session range streaming with bounded read-ahead (the byte path).

One RangeReader per open handle. Sequential reads are served from a bounded
buffer; a seek outside the buffer re-issues a ranged GET against the SAME
pinned source (offsets stay valid because the generation is pinned).
"""
from __future__ import annotations


class RangeReader:
    def __init__(self, engine, source_id: str, size: int, readahead: int):
        self.engine = engine
        self.source_id = source_id
        self.size = size
        self.readahead = readahead
        self._buf_off = -1
        self._buf = b""

    async def read(self, offset: int, length: int) -> bytes:
        if offset >= self.size:
            return b""
        length = min(length, self.size - offset)
        if length <= 0:
            return b""
        if self._buf_off <= offset < self._buf_off + len(self._buf):
            data = self._buf[offset - self._buf_off:]
            if len(data) >= length:
                return data[:length]
        window = min(max(self.readahead, length), self.size - offset)
        data = await self.engine.upstream_read(self.source_id, offset, window)
        self._buf_off = offset
        self._buf = data
        return data[:length]
