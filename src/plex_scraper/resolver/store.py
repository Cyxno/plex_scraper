"""SQLite persistence (WAL). Small surface, used from asyncio via to_thread."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from typing import Any, Callable

from plex_scraper.common.domain import models as m

_SCHEMA = """
CREATE TABLE IF NOT EXISTS media_items (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  plex_path TEXT NOT NULL UNIQUE,
  series TEXT, season INTEGER, episode INTEGER, year INTEGER,
  imdb_id TEXT, tmdb_id INTEGER, tvdb_id INTEGER,
  status TEXT NOT NULL DEFAULT 'NO_SOURCE',
  generation INTEGER NOT NULL DEFAULT 0,
  desired TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL, updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY,
  media_item_id TEXT NOT NULL REFERENCES media_items(id),
  generation INTEGER NOT NULL DEFAULT 0,
  provider TEXT NOT NULL,
  info_hash TEXT NOT NULL,
  torrent_name TEXT NOT NULL,
  file_id INTEGER, file_name TEXT, size INTEGER DEFAULT 0,
  resolution TEXT, codec TEXT, hdr TEXT, audio TEXT, language TEXT,
  release_type TEXT, seeders INTEGER, cached INTEGER DEFAULT 0,
  score REAL, score_json TEXT DEFAULT '{}',
  state TEXT NOT NULL DEFAULT 'candidate',
  failure_count INTEGER NOT NULL DEFAULT 0,
  bad_until REAL NOT NULL DEFAULT 0,
  last_verified REAL NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  UNIQUE(media_item_id, provider, info_hash, file_id)
);
CREATE TABLE IF NOT EXISTS sessions (
  handle TEXT PRIMARY KEY,
  media_item_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  size INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'open',
  opened_at REAL NOT NULL, closed_at REAL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  media_item_id TEXT,
  generation INTEGER,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_sources_item ON sources(media_item_id);
CREATE INDEX IF NOT EXISTS idx_events_item ON events(media_item_id);
"""


def _row_to_item(row: sqlite3.Row) -> m.MediaItem:
    return m.MediaItem(
        id=row["id"], kind=row["kind"], title=row["title"], plex_path=row["plex_path"],
        series=row["series"], season=row["season"], episode=row["episode"], year=row["year"],
        imdb_id=row["imdb_id"], tmdb_id=row["tmdb_id"], tvdb_id=row["tvdb_id"],
        status=row["status"], generation=row["generation"],
        desired=json.loads(row["desired"] or "{}"),
        duration_s=row["duration_s"] if "duration_s" in row.keys() else None,
        media_bitrate_mbit=row["media_bitrate_mbit"] if "media_bitrate_mbit" in row.keys() else None,
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


def _row_to_source(row: sqlite3.Row) -> m.Source:
    return m.Source(
        id=row["id"], media_item_id=row["media_item_id"], generation=row["generation"],
        provider=row["provider"], info_hash=row["info_hash"], torrent_name=row["torrent_name"],
        file_id=row["file_id"], file_name=row["file_name"], size=row["size"] or 0,
        resolution=row["resolution"], codec=row["codec"], hdr=row["hdr"], audio=row["audio"],
        language=row["language"], release_type=row["release_type"], seeders=row["seeders"],
        cached=bool(row["cached"]), score=row["score"],
        score_json=json.loads(row["score_json"] or "{}"),
        state=row["state"], failure_count=row["failure_count"], bad_until=row["bad_until"],
        last_verified=row["last_verified"], created_at=row["created_at"],
    )


class Store:
    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.executescript(_SCHEMA)
            # migrate: media-bitrate kolommen (adaptive-throughput fase)
            for col, ddl in (("duration_s", "ALTER TABLE media_items ADD COLUMN duration_s REAL"),
                             ("media_bitrate_mbit", "ALTER TABLE media_items ADD COLUMN media_bitrate_mbit REAL")):
                try:
                    self._conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass                                 # kolom bestaat al

    # ------------------------------------------------------------- plumbing
    async def run(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        def _work() -> Any:
            with self._lock:
                with self._conn:
                    return fn(self._conn)
        return await asyncio.to_thread(_work)

    # ------------------------------------------------------------- media_items
    async def create_item(self, item: m.MediaItem) -> m.MediaItem:
        def fn(c: sqlite3.Connection):
            c.execute(
                "INSERT INTO media_items (id,kind,title,plex_path,series,season,episode,year,"
                "imdb_id,tmdb_id,tvdb_id,status,generation,desired,created_at,updated_at,"
                "duration_s,media_bitrate_mbit) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (item.id, item.kind, item.title, item.plex_path, item.series, item.season,
                 item.episode, item.year, item.imdb_id, item.tmdb_id, item.tvdb_id,
                 item.status, item.generation, json.dumps(item.desired),
                 item.created_at, item.updated_at,
                 item.duration_s, item.media_bitrate_mbit))
        await self.run(fn)
        return item

    async def update_item(self, item: m.MediaItem) -> None:
        item.updated_at = m.now()

        def fn(c: sqlite3.Connection):
            c.execute(
                "UPDATE media_items SET status=?, generation=?, desired=?, updated_at=?, "
                "duration_s=?, media_bitrate_mbit=? WHERE id=?",
                (item.status, item.generation, json.dumps(item.desired), item.updated_at,
                 item.duration_s, item.media_bitrate_mbit, item.id))
        await self.run(fn)

    async def reconcile_stale(self, timeout_s: float) -> list[dict]:
        """DOEL 3-watchdog: items die in een tussenstaat (RESOLVING /
        CANDIDATE_VALIDATION) zijn blijven hangen, worden naar een bruikbare
        state gereconcilieerd — READY als er een actieve bron is, anders
        NO_SOURCE. Nooit permanent onzichtbaar voor de sweeper."""
        def fn(c: sqlite3.Connection):
            cutoff = m.now() - timeout_s
            rows = c.execute(
                "SELECT id, status FROM media_items "
                "WHERE status IN ('RESOLVING','CANDIDATE_VALIDATION') "
                "AND updated_at < ?", (cutoff,)).fetchall()
            out = []
            for r in rows:
                active = c.execute(
                    "SELECT COUNT(*) FROM sources "
                    "WHERE media_item_id=? AND state='active'",
                    (r["id"],)).fetchone()[0]
                new_status = "READY" if active else "NO_SOURCE"
                c.execute(
                    "UPDATE media_items SET status=?, updated_at=? WHERE id=?",
                    (new_status, m.now(), r["id"]))
                out.append({"id": r["id"], "was": r["status"], "now": new_status})
            return out
        return await self.run(fn)

    async def get_item(self, item_id: str) -> m.MediaItem | None:
        def fn(c: sqlite3.Connection):
            row = c.execute("SELECT * FROM media_items WHERE id=?", (item_id,)).fetchone()
            return _row_to_item(row) if row else None
        return await self.run(fn)

    async def list_items(self) -> list[m.MediaItem]:
        def fn(c: sqlite3.Connection):
            return [_row_to_item(r) for r in
                    c.execute("SELECT * FROM media_items ORDER BY created_at").fetchall()]
        return await self.run(fn)

    async def delete_item(self, item_id: str) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("DELETE FROM sources WHERE media_item_id=?", (item_id,))
            c.execute("DELETE FROM sessions WHERE media_item_id=?", (item_id,))
            c.execute("DELETE FROM media_items WHERE id=?", (item_id,))
        await self.run(fn)

    # --------------------------------------------------------------- sources
    async def upsert_source(self, src: m.Source) -> m.Source:
        def fn(c: sqlite3.Connection):
            c.execute(
                "INSERT INTO sources (id,media_item_id,generation,provider,info_hash,torrent_name,"
                "file_id,file_name,size,resolution,codec,hdr,audio,language,release_type,seeders,"
                "cached,score,score_json,state,failure_count,bad_until,last_verified,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(media_item_id,provider,info_hash,file_id) DO UPDATE SET "
                "file_name=excluded.file_name, size=excluded.size, cached=excluded.cached, "
                "score=excluded.score, score_json=excluded.score_json",
                (src.id, src.media_item_id, src.generation, src.provider, src.info_hash,
                 src.torrent_name, src.file_id, src.file_name, src.size, src.resolution,
                 src.codec, src.hdr, src.audio, src.language, src.release_type, src.seeders,
                 int(src.cached), src.score, json.dumps(src.score_json), src.state,
                 src.failure_count, src.bad_until, src.last_verified, src.created_at))
            row = c.execute(
                "SELECT * FROM sources WHERE media_item_id=? AND provider=? AND info_hash=? AND file_id IS ?",
                (src.media_item_id, src.provider, src.info_hash, src.file_id)).fetchone()
            return _row_to_source(row)
        return await self.run(fn)

    async def update_source(self, src: m.Source) -> None:
        def fn(c: sqlite3.Connection):
            c.execute(
                "UPDATE sources SET state=?, failure_count=?, bad_until=?, last_verified=?, "
                "generation=?, cached=?, file_id=?, file_name=?, size=? WHERE id=?",
                (src.state, src.failure_count, src.bad_until, src.last_verified, src.generation,
                 int(src.cached), src.file_id, src.file_name, src.size, src.id))
        await self.run(fn)

    async def get_source(self, source_id: str) -> m.Source | None:
        def fn(c: sqlite3.Connection):
            row = c.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone()
            return _row_to_source(row) if row else None
        return await self.run(fn)

    async def list_sources(self, item_id: str) -> list[m.Source]:
        def fn(c: sqlite3.Connection):
            return [_row_to_source(r) for r in c.execute(
                "SELECT * FROM sources WHERE media_item_id=? ORDER BY score DESC", (item_id,))]
        return await self.run(fn)

    async def retire_active(self, item_id: str, except_source_id: str | None = None) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("UPDATE sources SET state='retired' WHERE media_item_id=? AND state='active'"
                      " AND (? IS NULL OR id != ?)", (item_id, except_source_id, except_source_id))
        await self.run(fn)

    # -------------------------------------------------------------- sessions
    async def create_session(self, ses: m.Session) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
                      (ses.handle, ses.media_item_id, ses.source_id, ses.generation, ses.size,
                       ses.state, ses.opened_at, ses.closed_at))
        await self.run(fn)

    async def close_session(self, handle: str) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("UPDATE sessions SET state='closed', closed_at=? WHERE handle=?",
                      (m.now(), handle))
        await self.run(fn)

    # ---------------------------------------------------------------- events
    async def add_event(self, event_kind: str, media_item_id: str | None = None,
                        generation: int | None = None, **payload) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("INSERT INTO events (ts,media_item_id,generation,kind,payload) VALUES (?,?,?,?,?)",
                      (m.now(), media_item_id, generation, event_kind, json.dumps(payload)))
        await self.run(fn)

    async def recent_events(self, limit: int = 50) -> list[dict]:
        def fn(c: sqlite3.Connection):
            rows = c.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [{"ts": r["ts"], "media_item_id": r["media_item_id"],
                     "generation": r["generation"], "kind": r["kind"],
                     **json.loads(r["payload"])} for r in rows]
        return await self.run(fn)
