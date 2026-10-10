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
CREATE TABLE IF NOT EXISTS maintenance_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_type TEXT NOT NULL,
  started_at REAL NOT NULL,
  finished_at REAL,
  status TEXT NOT NULL DEFAULT 'RUNNING',
  processed INTEGER NOT NULL DEFAULT 0,
  changed INTEGER NOT NULL DEFAULT 0,
  recovered INTEGER NOT NULL DEFAULT 0,
  skipped INTEGER NOT NULL DEFAULT 0,
  failed INTEGER NOT NULL DEFAULT 0,
  current_item TEXT,
  progress_total INTEGER,
  progress_current INTEGER,
  summary_json TEXT NOT NULL DEFAULT '{}'
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
        show_imdb_id=row["show_imdb_id"] if "show_imdb_id" in row.keys() else None,
        show_tmdb_id=row["show_tmdb_id"] if "show_tmdb_id" in row.keys() else None,
        show_tvdb_id=row["show_tvdb_id"] if "show_tvdb_id" in row.keys() else None,
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
        delivery_bad_until=row["delivery_bad_until"] if "delivery_bad_until" in row.keys() else 0.0,
        last_verified=row["last_verified"], created_at=row["created_at"],
    )


class Store:
    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            # B: read-resilience — WAL laat readers door tijdens schrijvers
            # (een resolve mag het dashboard nooit laten hikken), plus een
            # expliciete bounded busy_timeout i.p.v. alleen Pythons default.
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(_SCHEMA)
            # S1: een startend proces heeft geen draaiende jobs — oude
            # RUNNING-rijen zijn van een dood proces en nooit RUNNING blijven.
            self._conn.execute(
                "UPDATE maintenance_runs SET status='INTERRUPTED' "
                "WHERE status='RUNNING' AND finished_at IS NULL")
            # T: retention — candidate-detail events korter dan samenvattingen;
            # maintenance-runs het langst. Eén keer per processtart, begrensd.
            self._conn.execute(
                "DELETE FROM events WHERE kind IN ('candidate_failed',"
                "'candidate_identity_rejected','candidate_pre_gate_rejected',"
                "'candidate_file_choice_rejected','resolution_skip_uncached') "
                "AND ts < ?", (m.now() - 7 * 86400,))
            self._conn.execute(
                "DELETE FROM events WHERE ts < ?", (m.now() - 30 * 86400,))
            self._conn.execute(
                "DELETE FROM maintenance_runs WHERE started_at < ?",
                (m.now() - 90 * 86400,))
            # migrate: media-bitrate kolommen (adaptive-throughput fase)
            for col, ddl in (("duration_s", "ALTER TABLE media_items ADD COLUMN duration_s REAL"),
                             ("media_bitrate_mbit", "ALTER TABLE media_items ADD COLUMN media_bitrate_mbit REAL"),
                             ("show_imdb_id", "ALTER TABLE media_items ADD COLUMN show_imdb_id TEXT"),
                             ("show_tmdb_id", "ALTER TABLE media_items ADD COLUMN show_tmdb_id TEXT"),
                             ("show_tvdb_id", "ALTER TABLE media_items ADD COLUMN show_tvdb_id TEXT"),
                             ("delivery_bad_until", "ALTER TABLE sources ADD COLUMN delivery_bad_until REAL NOT NULL DEFAULT 0")):
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
                "duration_s,media_bitrate_mbit,show_imdb_id,show_tmdb_id,show_tvdb_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (item.id, item.kind, item.title, item.plex_path, item.series, item.season,
                 item.episode, item.year, item.imdb_id, item.tmdb_id, item.tvdb_id,
                 item.status, item.generation, json.dumps(item.desired),
                 item.created_at, item.updated_at,
                 item.duration_s, item.media_bitrate_mbit,
                 item.show_imdb_id, item.show_tmdb_id, item.show_tvdb_id))
        await self.run(fn)
        return item

    _ITEM_COLS = ("status", "generation", "desired", "updated_at", "duration_s",
                  "media_bitrate_mbit", "imdb_id", "tmdb_id", "tvdb_id",
                  "show_imdb_id", "show_tmdb_id", "show_tvdb_id")

    async def update_item(self, item: m.MediaItem, fields: set[str] | None = None) -> None:
        """FASE-persistence: zonder `fields` wordt de volledige row geschreven
        (registration/enrichment). State-writers (resolve/open/activate/
        sweeper) geven expliciet hun eigenaarsvelden mee zodat een stale
        object NOOIT identity of external IDs kan overschrijven."""
        item.updated_at = m.now()
        cols = tuple(self._ITEM_COLS) if not fields else tuple(
            f for f in self._ITEM_COLS if f in fields)
        vals = [getattr(item, c) if c != "desired" else json.dumps(item.desired)
                for c in cols]

        def fn(c: sqlite3.Connection):
            c.execute(
                f"UPDATE media_items SET {', '.join(f'{c_}=?' for c_ in cols)} "
                "WHERE id=?", (*vals, item.id))
        await self.run(fn)

    RUNTIME_FIELDS = frozenset({"status", "generation", "desired", "updated_at"})

    async def update_runtime(self, item: m.MediaItem) -> None:
        """Partial-field update: alleen runtime-owned velden."""
        await self.update_item(item, fields=set(self.RUNTIME_FIELDS))

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
            params = (src.id, src.media_item_id, src.generation, src.provider, src.info_hash,
                      src.torrent_name, src.file_id, src.file_name, src.size, src.resolution,
                      src.codec, src.hdr, src.audio, src.language, src.release_type, src.seeders,
                      int(src.cached), src.score, json.dumps(src.score_json), src.state,
                      src.failure_count, src.bad_until, src.last_verified, src.created_at)
            try:
                c.execute(
                    "INSERT INTO sources (id,media_item_id,generation,provider,info_hash,torrent_name,"
                    "file_id,file_name,size,resolution,codec,hdr,audio,language,release_type,seeders,"
                    "cached,score,score_json,state,failure_count,bad_until,last_verified,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(media_item_id,provider,info_hash,file_id) DO UPDATE SET "
                    "file_name=excluded.file_name, size=excluded.size, cached=excluded.cached, "
                    "score=excluded.score, score_json=excluded.score_json, "
                    "state=excluded.state, generation=excluded.generation, "
                    "last_verified=excluded.last_verified", params)
            except sqlite3.IntegrityError:
                # Zelfde id hergebruikt via _similar_source (info_hash-match) maar met
                # een andere file_id: de conflict-target (media_item_id,provider,
                # info_hash,file_id) greep niet en de PK-botste. In-place bijwerken
                # in plaats van de resolve te crashen (historische Jackass 3D-crash).
                c.execute(
                    "UPDATE sources SET generation=?, provider=?, info_hash=?, torrent_name=?, "
                    "file_id=?, file_name=?, size=?, resolution=?, codec=?, hdr=?, audio=?, "
                    "language=?, release_type=?, seeders=?, cached=?, score=?, score_json=?, "
                    "state=?, failure_count=?, bad_until=?, last_verified=?, created_at=? "
                    "WHERE id=?",
                    params[2:] + (src.id,))
            row = c.execute(
                "SELECT * FROM sources WHERE media_item_id=? AND provider=? AND info_hash=? AND file_id IS ?",
                (src.media_item_id, src.provider, src.info_hash, src.file_id)).fetchone()
            return _row_to_source(row)
        return await self.run(fn)

    async def update_source(self, src: m.Source) -> None:
        def fn(c: sqlite3.Connection):
            c.execute(
                "UPDATE sources SET state=?, failure_count=?, bad_until=?, last_verified=?, "
                "generation=?, cached=?, file_id=?, file_name=?, size=?, delivery_bad_until=? WHERE id=?",
                (src.state, src.failure_count, src.bad_until, src.last_verified, src.generation,
                 int(src.cached), src.file_id, src.file_name, src.size, src.delivery_bad_until, src.id))
        await self.run(fn)

    async def set_delivery_bad(self, source_id: str, until: float) -> None:
        """FASE 13: bron tijdelijk als delivery-degraded markeren (NIET permanent bad)."""
        def fn(c: sqlite3.Connection):
            c.execute("UPDATE sources SET delivery_bad_until=? WHERE id=?", (until, source_id))
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

    async def activate_source(self, item_id: str, source_id: str) -> None:
        """Atomische single-active-invariant: demote alle andere actieve
        rijen en promoot deze — één transactie, dus interleaved activaties
        of een crash ertussen laten nooit twee actieve rijen achter."""
        def fn(c: sqlite3.Connection):
            c.execute("UPDATE sources SET state='retired' WHERE media_item_id=?"
                      " AND state='active' AND id!=?", (item_id, source_id))
            c.execute("UPDATE sources SET state='active' WHERE id=?"
                      " AND media_item_id=?", (source_id, item_id))
        await self.run(fn)

    async def reconcile_active(self, item_id: str) -> str | None:
        """Herstel de invariant deterministisch bij pre-existing dual-active:
        houd de actieve rij met de hoogste generation (tie: laatste
        last_verified, tie: laagste rowid) en demote de rest. Geschiedenis-
        rijen blijven bewaard als 'retired'."""
        def fn(c: sqlite3.Connection):
            rows = c.execute(
                "SELECT id FROM sources WHERE media_item_id=? AND state='active'"
                " ORDER BY generation DESC, last_verified DESC, rowid ASC",
                (item_id,)).fetchall()
            if len(rows) <= 1:
                return rows[0][0] if rows else None
            keep = rows[0][0]
            c.execute("UPDATE sources SET state='retired' WHERE media_item_id=?"
                      " AND state='active' AND id!=?", (item_id, keep))
            return keep
        return await self.run(fn)

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

    # ------------------------------------------------------ maintenance runs
    async def job_start(self, job_type: str, progress_total: int | None = None) -> int:
        def fn(c: sqlite3.Connection):
            cur = c.execute(
                "INSERT INTO maintenance_runs (job_type, started_at, status, progress_total) "
                "VALUES (?,?, 'RUNNING', ?)",
                (job_type, m.now(), progress_total))
            return cur.lastrowid
        return await self.run(fn)

    async def job_progress(self, run_id: int, *, processed: int | None = None,
                           changed: int | None = None, recovered: int | None = None,
                           skipped: int | None = None, failed: int | None = None,
                           current_item: str | None = None,
                           progress_current: int | None = None) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("""UPDATE maintenance_runs SET
                         processed=COALESCE(?,processed), changed=COALESCE(?,changed),
                         recovered=COALESCE(?,recovered), skipped=COALESCE(?,skipped),
                         failed=COALESCE(?,failed), current_item=COALESCE(?,current_item),
                         progress_current=COALESCE(?,progress_current)
                         WHERE id=?""",
                      (processed, changed, recovered, skipped, failed,
                       current_item, progress_current, run_id))
        await self.run(fn)

    async def job_finish(self, run_id: int, status: str = "SUCCESS",
                         **summary) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("""UPDATE maintenance_runs SET status=?, finished_at=?,
                         summary_json=?,
                         processed=COALESCE(?, processed),
                         changed=COALESCE(?, changed),
                         recovered=COALESCE(?, recovered),
                         failed=COALESCE(?, failed),
                         progress_current=COALESCE(?, progress_current)
                         WHERE id=?""",
                      (status, m.now(), json.dumps(summary),
                       summary.get("processed"), summary.get("changed"),
                       summary.get("recovered"), summary.get("failed"),
                       summary.get("processed"), run_id))
        await self.run(fn)

    async def job_runs(self, job_type: str | None = None,
                       limit: int = 20) -> list[dict]:
        def fn(c: sqlite3.Connection):
            if job_type:
                rows = c.execute("SELECT * FROM maintenance_runs WHERE job_type=? "
                                 "ORDER BY id DESC LIMIT ?", (job_type, limit)).fetchall()
            else:
                rows = c.execute("SELECT * FROM maintenance_runs "
                                 "ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["summary_json"] = json.loads(d.get("summary_json") or "{}")
                out.append(d)
            return out
        return await self.run(fn)

    # ------------------------------------------------------- physical health
    async def save_physical_health(self, result: dict) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("""CREATE TABLE IF NOT EXISTS physical_health (
                         id INTEGER PRIMARY KEY CHECK (id = 1),
                         status TEXT NOT NULL, checked_at REAL NOT NULL,
                         latency_s REAL, raw TEXT, last_healthy_at REAL)""")
            c.execute("""INSERT INTO physical_health (id,status,checked_at,latency_s,raw,last_healthy_at)
                         VALUES (1,?,?,?,?,?)
                         ON CONFLICT(id) DO UPDATE SET status=excluded.status,
                         checked_at=excluded.checked_at, latency_s=excluded.latency_s,
                         raw=excluded.raw,
                         last_healthy_at=COALESCE(excluded.last_healthy_at, physical_health.last_healthy_at)""",
                      (result["status"], result["checked_at"], result.get("latency_s"),
                       result.get("raw", "")[:200], result.get("last_healthy_at")))
        await self.run(fn)

    async def get_physical_health(self) -> dict | None:
        def fn(c: sqlite3.Connection):
            c.execute("""CREATE TABLE IF NOT EXISTS physical_health (
                         id INTEGER PRIMARY KEY CHECK (id = 1),
                         status TEXT NOT NULL, checked_at REAL NOT NULL,
                         latency_s REAL, raw TEXT, last_healthy_at REAL)""")
            row = c.execute("SELECT * FROM physical_health WHERE id=1").fetchone()
            return dict(row) if row else None
        return await self.run(fn)

    # -------------------------------------------------- identity conflicts
    async def set_identity_conflict(self, item_id: str, detail: dict) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("""CREATE TABLE IF NOT EXISTS identity_conflicts (
                         item_id TEXT PRIMARY KEY, detected_at REAL NOT NULL,
                         detail_json TEXT NOT NULL)""")
            c.execute("INSERT OR REPLACE INTO identity_conflicts VALUES (?,?,?)",
                      (item_id, m.now(), json.dumps(detail)))
        await self.run(fn)

    async def clear_identity_conflict(self, item_id: str) -> None:
        def fn(c: sqlite3.Connection):
            c.execute("CREATE TABLE IF NOT EXISTS identity_conflicts ("
                      "item_id TEXT PRIMARY KEY, detected_at REAL NOT NULL, "
                      "detail_json TEXT NOT NULL)")
            c.execute("DELETE FROM identity_conflicts WHERE item_id=?", (item_id,))
        await self.run(fn)

    async def get_identity_conflict(self, item_id: str) -> dict | None:
        def fn(c: sqlite3.Connection):
            c.execute("CREATE TABLE IF NOT EXISTS identity_conflicts ("
                      "item_id TEXT PRIMARY KEY, detected_at REAL NOT NULL, "
                      "detail_json TEXT NOT NULL)")
            row = c.execute("SELECT * FROM identity_conflicts WHERE item_id=?",
                            (item_id,)).fetchone()
            if not row:
                return None
            return {"detected_at": row["detected_at"],
                    **json.loads(row["detail_json"])}
        return await self.run(fn)

    async def list_identity_conflicts(self) -> list[dict]:
        def fn(c: sqlite3.Connection):
            c.execute("CREATE TABLE IF NOT EXISTS identity_conflicts ("
                      "item_id TEXT PRIMARY KEY, detected_at REAL NOT NULL, "
                      "detail_json TEXT NOT NULL)")
            return [{"item_id": r["item_id"], "detected_at": r["detected_at"],
                     **json.loads(r["detail_json"])}
                    for r in c.execute("SELECT * FROM identity_conflicts")]
        return await self.run(fn)

    # ---------------------------------------------------------- ingest queue
    @staticmethod
    def _ingest_schema(c: sqlite3.Connection) -> None:
        c.execute("""CREATE TABLE IF NOT EXISTS ingest_jobs (
          id TEXT PRIMARY KEY,
          source TEXT NOT NULL,
          arr_item_id TEXT NOT NULL,
          kind TEXT NOT NULL,
          dedupe_key TEXT NOT NULL UNIQUE,
          title TEXT NOT NULL DEFAULT '',
          series TEXT, season INTEGER, episode INTEGER, year INTEGER,
          show_imdb_id TEXT, show_tvdb_id TEXT, show_tmdb_id TEXT,
          imdb_id TEXT, tmdb_id TEXT,
          monitored INTEGER NOT NULL DEFAULT 1,
          wanted INTEGER NOT NULL DEFAULT 1,
          arr_path TEXT, air_date_utc TEXT,
          enqueue_reason TEXT NOT NULL DEFAULT 'reconcile',
          status TEXT NOT NULL DEFAULT 'QUEUED',
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at REAL NOT NULL DEFAULT 0,
          provider_block TEXT, last_error TEXT,
          resolver_item_id TEXT, delivered_symlink TEXT,
          created_at REAL NOT NULL, updated_at REAL NOT NULL,
          completed_at REAL
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ingest_status "
                  "ON ingest_jobs(status)")

    @staticmethod
    def _row_to_job(r: sqlite3.Row):
        from plex_scraper.ingest.models import IngestJob
        return IngestJob(
            source=r["source"], arr_item_id=r["arr_item_id"], kind=r["kind"],
            dedupe_key=r["dedupe_key"], title=r["title"], series=r["series"],
            season=r["season"], episode=r["episode"], year=r["year"],
            show_imdb_id=r["show_imdb_id"], show_tvdb_id=r["show_tvdb_id"],
            show_tmdb_id=r["show_tmdb_id"], imdb_id=r["imdb_id"],
            tmdb_id=r["tmdb_id"], monitored=bool(r["monitored"]),
            wanted=bool(r["wanted"]), arr_path=r["arr_path"],
            air_date_utc=r["air_date_utc"], enqueue_reason=r["enqueue_reason"],
            status=r["status"], attempts=r["attempts"],
            next_attempt_at=r["next_attempt_at"], provider_block=r["provider_block"],
            last_error=r["last_error"], resolver_item_id=r["resolver_item_id"],
            delivered_symlink=r["delivered_symlink"], id=r["id"],
            created_at=r["created_at"], updated_at=r["updated_at"],
            completed_at=r["completed_at"])

    _JOB_COLS = ("source", "arr_item_id", "kind", "dedupe_key", "title", "series",
                 "season", "episode", "year", "show_imdb_id", "show_tvdb_id",
                 "show_tmdb_id", "imdb_id", "tmdb_id", "monitored", "wanted",
                 "arr_path", "air_date_utc", "enqueue_reason", "status",
                 "attempts", "next_attempt_at", "provider_block", "last_error",
                 "resolver_item_id", "delivered_symlink", "updated_at",
                 "completed_at")

    async def upsert_job(self, job) -> tuple[str, bool]:
        """INSERT OR IGNORE op dedupe_key: webhook + reconcile + retry landen
        op dezelfde rij (Phase 14). Bestaande rij krijgt alleen monitored/
        wanted/arr_path/air_date ververst; returns (id, created)."""
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            existing = c.execute(
                "SELECT id FROM ingest_jobs WHERE dedupe_key=?",
                (job.dedupe_key,)).fetchone()
            if existing is None:
                c.execute(
                    f"INSERT INTO ingest_jobs ({', '.join(self._JOB_COLS)}, id, created_at) "
                    f"VALUES ({', '.join('?' for _ in self._JOB_COLS)}, ?, ?)",
                    (*[getattr(job, col) if col != "monitored" and col != "wanted"
                       else int(getattr(job, col)) for col in self._JOB_COLS],
                     job.id, job.created_at))
                return job.id, True
            job.id = existing["id"]
            c.execute(
                "UPDATE ingest_jobs SET monitored=?, wanted=?, arr_path=?, "
                "arr_item_id=?, air_date_utc=?, title=?, updated_at=? "
                "WHERE id=?",
                (int(job.monitored), int(job.wanted), job.arr_path,
                 job.arr_item_id, job.air_date_utc, job.title, m.now(), job.id))
            # queue-correctness: een COMPLETED job waarvan het item wéér
            # wanted+missing is (bestand weg, re-monitor) moet opnieuw kunnen
            # draaien — re-activatie naar QUEUED met verse pogingen.
            st = c.execute("SELECT status FROM ingest_jobs WHERE id=?",
                           (job.id,)).fetchone()
            if st is not None and st["status"] == "COMPLETED":
                c.execute(
                    "UPDATE ingest_jobs SET status='QUEUED', attempts=0, "
                    "completed_at=NULL, last_error=NULL, provider_block=NULL, "
                    "next_attempt_at=?, updated_at=? WHERE id=?",
                    (m.now(), m.now(), job.id))
                return job.id, True            # telt als (her)created
            return job.id, False
        return await self.run(fn)

    async def get_job(self, job_id: str):
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            row = c.execute("SELECT * FROM ingest_jobs WHERE id=?",
                            (job_id,)).fetchone()
            return self._row_to_job(row) if row else None
        return await self.run(fn)

    async def delete_job(self, job_id: str) -> bool:
        """Canary-probe-rijen en operator-annulleringen (Phase 3/32)."""
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            cur = c.execute("DELETE FROM ingest_jobs WHERE id=?", (job_id,))
            return cur.rowcount > 0
        return await self.run(fn)

    async def get_job_by_dedupe(self, dedupe_key: str):
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            row = c.execute("SELECT * FROM ingest_jobs WHERE dedupe_key=?",
                            (dedupe_key,)).fetchone()
            return self._row_to_job(row) if row else None
        return await self.run(fn)

    async def update_job(self, job, fields: set[str]) -> None:
        cols = tuple(f for f in fields if f in self._JOB_COLS or f == "completed_at")
        job.updated_at = m.now()
        vals = [int(getattr(job, col)) if col in ("monitored", "wanted")
                else getattr(job, col) for col in cols]

        def fn(c: sqlite3.Connection):
            c.execute(
                f"UPDATE ingest_jobs SET {', '.join(f'{col_}=?' for col_ in cols)} "
                "WHERE id=?", (*vals, job.id))
        await self.run(fn)

    async def list_jobs(self, status: str | None = None,
                        limit: int = 500) -> list:
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            if status:
                rows = c.execute(
                    "SELECT * FROM ingest_jobs WHERE status=? "
                    "ORDER BY created_at LIMIT ?", (status, limit)).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM ingest_jobs ORDER BY created_at LIMIT ?",
                    (limit,)).fetchall()
            return [self._row_to_job(r) for r in rows]
        return await self.run(fn)

    async def due_jobs(self, now_ts: float, limit: int = 5) -> list:
        """Worker-invoer: actieve jobs waarvan next_attempt_at verstreken is.

        Age-aware prioriteit (Phase 10): nieuw gelucht eerst, oude backlog
        als achtergrond — oud werk mag nieuwe releases niet vertragen.
          tier 0: air-date < 48u (new release)
          tier 1: air-date < 14d (recent backlog)
          tier 2: ouder/onbekend (background catch-up)
        Binnen een tier: next_attempt_at ASC (deferred schuift naar achteren —
        dat is tegelijk de fairness-round-robin, Phase 11)."""
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            rows = c.execute(
                "SELECT * FROM ingest_jobs WHERE status IN "
                "('QUEUED','IDENTITY_VERIFYING','REGISTERING','RESOLVING',"
                "'PROVIDER_WAIT','READY','DELIVERING','PLEX_REFRESH',"
                "'FAILED_RETRYABLE') "
                "AND next_attempt_at <= ? "
                "ORDER BY CASE WHEN air_date_utc IS NULL THEN 2 "
                "WHEN air_date_utc >= datetime(?, 'unixepoch', '-2 days') "
                "     THEN 0 "
                "WHEN air_date_utc >= datetime(?, 'unixepoch', '-14 days') "
                "     THEN 1 ELSE 2 END, next_attempt_at ASC LIMIT ?",
                (now_ts, now_ts, now_ts, limit)).fetchall()
            return [self._row_to_job(r) for r in rows]
        return await self.run(fn)

    async def ingest_job_counts(self) -> dict:
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            return {r["status"]: r["n"] for r in c.execute(
                "SELECT status, COUNT(*) n FROM ingest_jobs GROUP BY status")}
        return await self.run(fn)

    async def reset_stale_running_jobs(self) -> int:
        """Herstart-veiligheid: een startend worker-proces heeft geen draaiende
        jobs — RESOLVING/DELIVERING/... na crash naar FAILED_RETRYABLE."""
        def fn(c: sqlite3.Connection):
            self._ingest_schema(c)
            cur = c.execute(
                "UPDATE ingest_jobs SET status='FAILED_RETRYABLE', "
                "last_error='restart recovery', next_attempt_at=? "
                "WHERE status IN ('IDENTITY_VERIFYING','REGISTERING','RESOLVING',"
                "'DELIVERING','PLEX_REFRESH')",
                (m.now() + 30.0,))
            return cur.rowcount
        return await self.run(fn)
