"""Read-only diagnostics GUI — serve HTML templates + REST API.

Actuele health = resolver-API (leidend). Migration history = SQLite (context).
"""
from __future__ import annotations

import os
import sqlite3
import time

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..common.config import Settings

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


def create_web_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="plex_scraper diagnostics", docs_url=None, redoc_url=None)
    resolver_base = settings.resolver_url

    def _tpl(name: str) -> str:
        with open(os.path.join(_TEMPLATE_DIR, name), encoding="utf-8") as fh:
            return fh.read()

    def resolver(path: str, timeout: float = 20.0):
        import httpx
        r = httpx.get(f"{resolver_base}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _db() -> sqlite3.Connection | None:
        import shutil
        snap = "/tmp/migration-state-snapshot.sqlite"
        try:
            shutil.copy2(settings.mig_db_path, snap)
            c = sqlite3.connect(snap, timeout=30)
            c.row_factory = sqlite3.Row
            return c
        except Exception:
            return None

    def _mounts():
        m = {}
        with open("/proc/mounts") as fh:
            for line in fh:
                if "/mnt/cache/appdata/plex-scraper/vfs" in line and "fuse" in line:
                    m["primary_vfs"] = True
                if "/mnt/remote/nzbdav" in line and "fuse" in line:
                    m["rehydrate_vfs"] = True
                if "decypharr" in line and "fuse" in line:
                    m["decypharr"] = True
        return m

    def classify(last_error, fail_class):
        e = (last_error or "").lower()
        c = (fail_class or "").lower()
        if "no_source" in c:
            return {"cat": "NO_SOURCE", "transient": True,
                    "human": "No playable source found — resolver could not find a usable source."}
        if "403" in e:
            return {"cat": "BACKEND_AUTH", "transient": False,
                    "human": "TorBox rejected the request (403). Check API key or account."}
        if "429" in e:
            return {"cat": "BACKEND_RATE_LIMIT", "transient": True,
                    "human": "TorBox rate-limited. Will retry."}
        if "400" in e or "bozo" in e:
            return {"cat": "BACKEND_REJECT", "transient": True,
                    "human": "TorBox rejected this torrent."}
        if "vfs_fail" in c or "no data available" in e:
            return {"cat": "VFS_READ_FAIL", "transient": True,
                    "human": "VFS could not read from the debrid backend."}
        if "plex_verify" in c:
            if "edition" in e or "stale" in e:
                return {"cat": "PLEX_EDITION_CONFLICT", "transient": False,
                        "human": "Plex selected a stale media part during verification."}
            if "timed out" in e:
                return {"cat": "PLEX_VERIFY_TIMEOUT", "transient": True,
                        "human": "Plex verification timed out. Does NOT mean content is broken."}
            return {"cat": "PLEX_VERIFY_FAIL", "transient": True,
                    "human": "Plex verification failed."}
        if "timeout" in e:
            return {"cat": "TRANSIENT_TIMEOUT", "transient": True,
                    "human": "Temporary timeout. Marked for retry."}
        if not (last_error or c):
            return {"cat": "UNKNOWN", "transient": None, "human": "Unknown — check trace."}
        return {"cat": "OTHER", "transient": None, "human": (last_error or c)[:120]}

    def display_title(kind, title, gp=None, season=None, episode=None, year=None):
        """Centrale display-title formatter — één logica overal."""
        if kind == "episode" and gp:
            se = "S%02dE%02d" % (int(season or 0), int(episode or 0))
            if title and title.strip():
                return "%s \u2014 %s \u2014 %s" % (gp, se, title)
            return "%s \u2014 %s" % (gp, se)
        yr = " (%s)" % year if year else ""
        return "%s%s" % (title or "", yr)

    # ------------------------------------------------------------- pages
    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTMLResponse(_tpl("cockpit.html"))

    @app.get("/legacy", response_class=HTMLResponse)
    async def legacy_index():
        return HTMLResponse(_tpl("summary.html"))

    @app.get("/ui/trace/{rk}", response_class=HTMLResponse)
    async def ui_trace(rk: str):
        html = _tpl("trace.html")
        return HTMLResponse(html)

    # Cockpit-API passthrough: éénzelfde origin, resolver-view-modellen
    # (dashboard/activity/issues/jobs/trace/providers) zonder CORS-gedoe.
    @app.get("/ops/{path:path}")
    async def ops_proxy(path: str):
        import httpx
        try:
            r = httpx.get(f"{resolver_base}/api/{path}", timeout=20.0)
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception as exc:                       # resolver down
            return JSONResponse(status_code=503,
                                content={"error": f"resolver onbereikbaar: {exc!r}"[:200]})

    @app.get("/media-items")
    async def media_items_proxy():
        """Authoritative resolver-items voor de cockpit Library (B2):
        vervangt de legacy migration-DB view als identiteitsbron."""
        import httpx
        try:
            r = httpx.get(f"{resolver_base}/media", timeout=30.0)
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception as exc:
            return JSONResponse(status_code=503,
                                content={"error": f"resolver onbereikbaar: {exc!r}"[:200]})

    @app.post("/ops-post/{path:path}")
    async def ops_post_proxy(path: str, request: Request):
        import httpx
        try:
            body = await request.body()
            r = httpx.post(f"{resolver_base}/api/{path}", content=body,
                           headers={"Content-Type": "application/json"}, timeout=120.0)
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception as exc:
            return JSONResponse(status_code=503,
                                content={"error": f"resolver onbereikbaar: {exc!r}"[:200]})

    @app.get("/ops-media/{item_id}/trace")
    async def ops_trace_proxy(item_id: str):
        import httpx
        try:
            r = httpx.get(f"{resolver_base}/api/media/{item_id}/trace", timeout=20.0)
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception as exc:
            return JSONResponse(status_code=503,
                                content={"error": f"resolver onbereikbaar: {exc!r}"[:200]})

    # ------------------------------------------------------------- API
    @app.get("/health")
    async def health():
        r: dict = {}
        try:
            r["resolver"] = resolver("/health", 10)
        except Exception as e:
            r["resolver"] = {"status": "unreachable", "error": repr(e)[:80]}
        r["web"] = {"status": "ok"}
        r["mounts"] = _mounts()
        return r

    @app.get("/api/summary")
    async def summary():
        resolver_err = None
        rh: dict = {}
        items = None
        last_ok = last_err = None
        try:
            st = resolver("/status", 30)
            items = resolver("/media", 60)
            rh = {"READY": sum(1 for i in items if i["status"] == "READY"),
                  "NO_SOURCE": sum(1 for i in items if i["status"] == "NO_SOURCE"),
                  "TOTAL": len(items),
                  "sessions": st.get("sessions_open", 0),
                  "resources": st.get("resources")}
            for e in reversed(st.get("recent_events", [])):
                if e.get("kind") == "resolution_succeeded" and not last_ok:
                    last_ok = e
                if (e.get("kind") in ("resolution_failed", "candidate_failed",
                                      "upstream_read_failed") and not last_err):
                    last_err = e
        except Exception as e:
            resolver_err = repr(e)[:100]
        db = _db()
        mc: dict = {}
        fg: dict = {}
        fl: list = []
        if db:
            mc = {r["status"]: r["c"] for r in db.execute(
                "SELECT status, COUNT(*) c FROM queue GROUP BY status").fetchall()}
            for r in db.execute(
                "SELECT rk,title,gp,season,episode,year,kind,status,last_error,"
                "fail_class,attempts,updated_at "
                "FROM queue WHERE status IN ('FAILED_RETRYABLE','FAILED_FINAL') "
                "ORDER BY updated_at DESC").fetchall():
                d = dict(r)
                # cross-reference met resolver: READY = RECOVERED
                ritem = next((i for i in (items or [])
                              if i["plex_path"] == d.get("plex_path_rel")), None)
                current = ritem["status"] if ritem else "UNKNOWN"
                d["current_health"] = ("HEALTHY" if current == "READY"
                                       else "NO_SOURCE" if current == "NO_SOURCE"
                                       else "UNKNOWN")
                d["recovered"] = d["current_health"] == "HEALTHY"
                cl = classify(d["last_error"], d["fail_class"])
                d["category"] = cl["cat"]
                d["display_title"] = display_title(
                    "episode" if d.get("gp") else "movie",
                    d.get("title") or "", d.get("gp"),
                    d.get("season"), d.get("episode"), d.get("year"))
                d["human"] = cl["human"]
                d["transient"] = cl["transient"]
                if d["recovered"]:
                    recovered.append(d)
                else:
                    fl.append(d)
                entry = fg.setdefault(cl["cat"], {
                    "count": 0, "last": None, "first": None,
                    "transient": cl["transient"]})
                entry["count"] += 1
                ts = d.get("updated_at") or 0
                if not entry["last"] or ts > entry["last"]:
                    entry["last"] = ts
                if not entry["first"] or ts < entry["first"]:
                    entry["first"] = ts
        # split: actuele failures vs recovered — resolver-API is leidend
        current_failures = []
        recovered = []
        for d in fl:
            if d["recovered"]:
                recovered.append(d)
            else:
                current_failures.append(d)
        playback = None
        try:
            playback = resolver("/api/playback/active", 10)
        except Exception:
            playback = None
        selfheal = None
        try:
            sh = resolver("/api/selfheal/status", 15)
            selfheal = {
                "enabled": sh.get("enabled", False),
                "mode": ("auto-repair" if not sh.get("shadow_mode") else "shadow"),
                "upgrade": ("aan" if sh.get("upgrade_enabled") else "uit"),
                "items_per_hour": sh.get("items_per_hour"),
                "no_source_tracked": sh.get("no_source_tracked", 0),
                "playback_pause": sh.get("playback_pause", False),
                "active_playback": sh.get("active_playback", 0),
                "degraded_throughput": len(sh.get("degraded_throughput") or []),
                "health_states": sh.get("health_states") or {},
                "counts_24h": sh.get("counts_24h", {}),
                "events": [
                    {"ts": e.get("ts"), "event": e.get("event"),
                     "plex_path": (e.get("plex_path") or "")[-48:],
                     "detail": (e.get("detail") or "")[:100]}
                    for e in (sh.get("events") or [])[:8]],
            }
        except Exception:
            selfheal = None
        return {
            "resolver_health": rh,
            "resolver_error": resolver_err,
            "migration_history": mc,
            "failure_groups": fg,
            "failures": current_failures[:25],
            "recovered": recovered[:25],
            "selfheal": selfheal,
            "playback": playback,
            "mounts": _mounts(),
            "last_ok_resolve": (last_ok or None) and {
                "ts": last_ok.get("ts"), "item_id": last_ok.get("item_id"),
                "generation": last_ok.get("generation")},
            "last_error": (last_err or None) and {
                "ts": last_err.get("ts"), "kind": last_err.get("kind"),
                "detail": {k: v for k, v in last_err.items()
                           if k in ("error", "hash", "reason")}},
        }

    @app.get("/api/items")
    async def all_items():
        items = resolver("/media", 60)
        db = _db()
        qmap: dict = {}
        if db:
            try:
                for r in db.execute("SELECT * FROM queue").fetchall():
                    qmap[r["plex_path_rel"]] = dict(r)
            except Exception:
                pass
        out = []
        for m in items:
            q = qmap.get(m["plex_path"])
            ids = (q or {}).get("ids") or {}
            current = ("HEALTHY" if m["status"] == "READY"
                       else "NO_SOURCE" if m["status"] == "NO_SOURCE" else "DEGRADED")
            migration = (q or {}).get("status")
            recovered = current == "HEALTHY" and migration in (
                "FAILED_RETRYABLE", "FAILED_FINAL")
            out.append({
                "id": m["id"], "rk": (q or {}).get("rk"),
                "plex_path": m["plex_path"], "status": m["status"],
                "title": (q or {}).get("title") or m["plex_path"].split("/")[-1],
                "display_title": display_title(
                    "episode" if m["kind"] == "episode" else "movie",
                    (q or {}).get("title") or "",
                    (q or {}).get("gp"), (q or {}).get("s"),
                    (q or {}).get("e"), (q or {}).get("year")),
                "series": (q or {}).get("gp"),
                "season": (q or {}).get("s"), "episode": (q or {}).get("e"),
                "year": (q or {}).get("year"),
                "imdb_id": ids.get("imdbId"), "tmdb_id": ids.get("tmdbId"),
                "tvdb_id": ids.get("tvdbId"),
                "current_health": current, "migration_history": migration,
                "recovered": recovered,
                "generation": m.get("generation"),
                "size_gb": round((m.get("size") or 0) / (1 << 30), 2),
                "resolution": m.get("resolution"),
            })
        return out

    @app.get("/api/trace/{rk}")
    async def trace(rk: str):
        import httpx
        steps = []
        first_fail = None
        try:
            items = resolver("/media", 30)
        except Exception:
            items = None
        row = _queue_row(rk)
        item = None
        if row and row.get("resolver_item_id") and items:
            item = next((i for i in items
                         if i["id"] == row["resolver_item_id"]), None)

        def mark(step, ok, detail, warn=False):
            nonlocal first_fail
            state = "WARN" if (warn and ok) else ("PASS" if ok else "FAIL")
            if state == "FAIL" and first_fail is None:
                first_fail = step
            steps.append({"step": step, "ok": ok, "state": state,
                          "detail": detail, "ts": time.strftime("%H:%M:%S")})

        if resolver_ok(items) is False:
            mark("resolver API", False, "resolver onbereikbaar")
        if row is None:
            mark("migration-queue record", False,
                 "rk niet in migration-queue (legacy debrid-pad of onbekend)")
            return {"rk": rk, "steps": steps, "first_fail": first_fail}

        mark("media identity", True,
             display_title(
                 "episode" if row.get("gp") else "movie",
                 row.get("title") or "", row.get("gp"),
                 row.get("season"), row.get("episode"), row.get("year")))
        mark("resolver record", item is not None,
             f"id={row['resolver_item_id']} status={row['status']} "
             f"gen={row.get('generation', 0)}"
             if item else f"resolver_item_id {row['resolver_item_id']} ontbreekt")
        if item is None:
            mark("resolver status", False, "status onbekend")
            _local_chain(steps, mark, row)
            return {"rk": rk, "steps": steps, "first_fail": first_fail}

        mark("resolver status", item["status"] == "READY",
             f"status={item['status']}")

        src = None
        try:
            d = httpx.get(f"{resolver_base}/media/{item['id']}", timeout=30).json()
            src = next((s for s in d.get("sources", [])
                        if s.get("state") == "active"), None)
        except Exception:
            pass
        if src:
            mark("gekozen source", True,
                 f"hash={src['info_hash'][:12]} {src.get('resolution')} "
                 f"{round((src.get('size_gb') or 0)/(1<<30), 2)}GB "
                 f"cached={src.get('cached')} | "
                 f"{(src.get('file_name') or '')[:50]}")
        else:
            mark("gekozen source", False, "geen actieve source")

        _local_chain(steps, mark, row)

        try:
            with httpx.Client(base_url=resolver_base, timeout=120) as cl:
                t0 = time.time()
                o = cl.post(f"/media/{item['id']}/open").json()
                handle = o.get("handle")
                if handle:
                    r1 = cl.get(f"/stream/{handle}",
                                params={"offset": 0, "length": 64})
                    t1 = time.time()
                    r2 = cl.get(f"/stream/{handle}",
                                params={"offset": 65536, "length": 64})
                    mark("resolver stream (read+seek)",
                         len(r1.content) > 0 and len(r2.content) > 0,
                         f"read={len(r1.content)}B seek={len(r2.content)}B "
                         f"({int((t1-t0)*1000)}ms + {int((time.time()-t1)*1000)}ms)")
                    cl.delete(f"/open/{handle}")
                else:
                    mark("resolver stream", False, "geen handle")
        except Exception as e:
            mark("resolver stream", False, repr(e)[:80])

        current = "HEALTHY"
        if row["status"] in ("FAILED_RETRYABLE", "FAILED_FINAL"):
            current = "DEGRADED"
        if row["status"] == "NO_SOURCE":
            current = "NO_SOURCE"
        migration_history = row.get("status") or None
        # identity uit de "media identity" mark-stap
        identity = ""
        for s in steps:
            if s["step"] == "media identity":
                identity = s["detail"]
                break
        return {"rk": rk, "steps": steps, "first_fail": first_fail,
                "current_health": current,
                "migration_history": migration_history,
                "identity": identity}

    def resolver_ok(items):
        return items is not None

    def _local_chain(steps, mark, row):
        sym = row["symlink_host"]
        sym_ok = os.path.islink(sym)
        mark("symlink", sym_ok, sym if sym_ok else f"ONTBREEKT: {sym}")
        if sym_ok:
            tgt = os.readlink(sym)
            tgt_ok = os.path.exists(sym)
            mark("VFS/debrid target", tgt_ok, tgt[-65:])
            if tgt_ok:
                try:
                    t0 = time.time()
                    with open(sym, "rb") as fh:
                        head = fh.read(8)
                    ms = int((time.time() - t0) * 1000)
                    magic = (head[:4] == bytes.fromhex("1a45dfa3")
                             or head[4:8] == b"ftyp")
                    mark("byte-read", magic,
                         f"head={head.hex()[:16]} magic={magic} in {ms}ms")
                except Exception as e:
                    mark("byte-read", False, repr(e)[:80])

    @app.post("/api/action/resolve/{rk}")
    async def action_resolve(rk: str):
        import httpx
        row = _queue_row(rk)
        if row is None or not row.get("resolver_item_id"):
            return {"result": "fout", "error": "onbekend item",
                    "ts": time.strftime("%H:%M:%S")}
        try:
            async with httpx.AsyncClient(base_url=resolver_base,
                                         timeout=300) as cl:
                r = await cl.post(f"/media/{row['resolver_item_id']}/resolve")
                return {"result": "gestart" if r.status_code == 200 else "fout",
                        "http": r.status_code,
                        "ts": time.strftime("%H:%M:%S")}
        except Exception as e:
            return {"result": "fout", "error": repr(e)[:100],
                    "ts": time.strftime("%H:%M:%S")}

    @app.post("/api/action/readtest/{rk}")
    async def action_readtest(rk: str):
        row = _queue_row(rk)
        if row is None:
            return {"result": "fout", "error": "onbekend item",
                    "ts": time.strftime("%H:%M:%S")}
        sym = row["symlink_host"]
        try:
            t0 = time.time()
            with open(sym, "rb") as fh:
                head = fh.read(8)
            return {"result": "ok", "head": head.hex()[:16],
                    "magic": head[:4] == bytes.fromhex("1a45dfa3")
                             or head[4:8] == b"ftyp",
                    "ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            return {"result": "fout", "error": repr(e)[:100],
                    "ts": time.strftime("%H:%M:%S")}

    def _queue_row(rk: str):
        db = _db()
        if db is None:
            return None
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM queue WHERE rk=?", (rk,)).fetchone()
        return dict(row) if row else None

    return app
