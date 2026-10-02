"""Read-only diagnostics GUI (Fase 2). Leest resolver-API + SQLite-state en
doet live VFS/symlink checks. Enige schrijfacties: expliciete retry-acties via
de resolver-API (resolve/verify). Geen deletes, geen tweede bron van waarheid.

Polish-pass: first-failure-markering, failure-classificatie, activity-sectie,
verify-presentatie (read-only uit worker-state), compacte statuskleuren.
"""
from __future__ import annotations

import os
import sqlite3
import time

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from ..common.config import Settings


def create_web_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="plex_scraper diagnostics", docs_url=None, redoc_url=None)
    resolver_base = settings.resolver_url

    def resolver(path: str, timeout: float = 20.0):
        import httpx
        r = httpx.get(f"{resolver_base}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _db() -> sqlite3.Connection:
        # ro-bind: SQLite kan hier geen journal schrijven -> kopieer snapshot
        import shutil
        snap = '/tmp/migration-state.sqlite'
        try:
            shutil.copy2(settings.mig_db_path, snap)
        except Exception:
            pass
        c = sqlite3.connect(snap, timeout=30)
        c.row_factory = sqlite3.Row
        return c

    def _queue_row(rk: str):
        db = _db()
        row = db.execute("SELECT * FROM queue WHERE rk=?", (rk,)).fetchone()
        return dict(row) if row else None

    def _queue_stats() -> dict:
        try:
            db = _db()
            return {r["status"]: r["c"] for r in db.execute(
                "SELECT status, COUNT(*) c FROM queue GROUP BY status").fetchall()}
        except Exception:
            return {"error": "queue-db niet beschikbaar"}

    def _mounts() -> dict:
        mounts = {}
        with open("/proc/mounts") as fh:
            for line in fh:
                if "plex_scraper" in line and "vfs" in line:
                    mounts["vfs"] = True
                if "decypharr" in line:
                    mounts["decypharr"] = True
                if "/mnt/remote/nzbdav" in line.split()[1]:
                    mounts["vfs_legacy"] = True
        return mounts

    # ------------------------------------------------- failure classification
    def classify(last_error: str, fail_class: str, status: str) -> dict:
        """Alleen op bestaande evidence (error-string/state) — geen speculatie."""
        e = (last_error or "").lower()
        c = fail_class or ""
        if status == "NO_SOURCE" or "no_source" in c.lower():
            return {"cat": "NO_SOURCE", "transient": True}
        if "403" in e:
            return {"cat": "AUTH/CONFIG (backend 403)", "transient": False}
        if "429" in e:
            return {"cat": "BACKEND RATE-LIMIT (429)", "transient": True}
        if "400" in e:
            return {"cat": "BACKEND 400 (invalid magnet/add)", "transient": True}
        if "vfs_fail" in c.lower():
            return {"cat": "VFS/READ FAILURE", "transient": True}
        if "plex_verify" in c.lower():
            if "geen werkende part" in e or "dode editie" in e:
                return {"cat": "PLEX EDITION-CONFLICT (dode part geselecteerd)",
                        "transient": False}
            if "timed out" in e:
                return {"cat": "TRANSIENT RETRY (plex verify timeout)",
                        "transient": True}
            return {"cat": "PLEX VERIFY FAILURE", "transient": True}
        if "timeout" in e:
            return {"cat": "TRANSIENT RETRY (timeout)", "transient": True}
        if "bestaand item" in e:
            return {"cat": "NO_SOURCE", "transient": True}
        if not (last_error or c):
            return {"cat": "UNKNOWN", "transient": None}
        return {"cat": "OTHER", "transient": None}

    def _local_chain(steps, mark, row):
        """Lokale keten-stappen (symlink/VFS/byte-read) zonder resolver-API."""
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
                    magic = head[:4] == bytes.fromhex("1a45dfa3") or head[4:8] == b"ftyp"
                    mark("byte-read (VFS)", magic,
                         f"head={head.hex()[:16]} magic={magic} in {ms}ms")
                except Exception as e:
                    mark("byte-read (VFS)", False, repr(e)[:80])

    # ------------------------------------------------- trace (first-fail)
    def _trace(rk: str) -> dict:
        import httpx
        steps = []
        now = lambda: time.strftime("%H:%M:%S")
        items = None
        resolver_down = False
        try:
            items = resolver("/media", 30)
        except Exception as e:
            resolver_down = True
        row = _queue_row(rk)
        item = None
        if row and row.get("resolver_item_id") and items:
            item = next((i for i in items if i["id"] == row["resolver_item_id"]), None)
        first_fail = None

        def mark(step, ok, detail, warn=False, ts=None):
            nonlocal first_fail
            state = "WARN" if (warn and ok) else ("PASS" if ok else "FAIL")
            if state == "FAIL" and first_fail is None:
                first_fail = step
            steps.append({"step": step, "ok": ok, "warn": warn, "state": state,
                          "detail": detail, "ts": ts or now()})
            return ok

        if resolver_down:
            mark("resolver API", False, "resolver onbereikbaar — lokale keten-stappen volgen",
                 warn=True)
        if row is None:
            mark("migration-queue record", False,
                 f"rk {rk} niet in migration-queue (legacy debrid-item of onbekend)")
            return {"rk": rk, "steps": steps, "first_fail": first_fail,
                    "note": "item draait (mogelijk) via legacy debrid-pad"}

        mark("resolver record", item is not None,
             f"id={row['resolver_item_id']} status={row['status']} gen={row.get('generation', 0)}"
             if item else f"resolver_item_id {row['resolver_item_id']} ontbreekt in resolver")
        if item is None:
            # resolver-status onbekend, maar de lokale keten is nog te checken
            mark("resolver status", False, "status onbekend (resolver-item ontbreekt)")
            _local_chain(steps, mark, row)
            return {"rk": rk, "title": row["title"], "steps": steps,
                    "first_fail": first_fail}

        mark("resolver status", item["status"] == "READY",
             f"status={item['status']}")
        steps[-1]["ts"] = time.strftime(
            "%H:%M:%S", time.localtime(item.get("updated_at") or time.time()))

        src = None
        try:
            detail = httpx.get(f"{resolver_base}/media/{item['id']}", timeout=30).json()
            src = next((s for s in detail.get("sources", [])
                        if s.get("state") == "active"), None)
        except Exception:
            pass
        if src:
            mark("gekozen source", True,
                 f"provider={src.get('provider')} hash={src['info_hash'][:12]} "
                 f"res={src.get('resolution')} {round((src.get('size_gb') or 0), 2)}GB "
                 f"cached={src.get('cached')} fails={src.get('failure_count')} | "
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
                    r1 = cl.get(f"/stream/{handle}", params={"offset": 0, "length": 64})
                    t1 = time.time()
                    r2 = cl.get(f"/stream/{handle}", params={"offset": 65536, "length": 64})
                    seek_ms = int((time.time() - t1) * 1000)
                    mark("resolver stream (read+seek)",
                         len(r1.content) > 0 and len(r2.content) > 0,
                         f"read={len(r1.content)}B seek={len(r2.content)}B "
                         f"({int((t1-t0)*1000)}ms + {seek_ms}ms)")
                    cl.delete(f"/open/{handle}")
        except Exception as e:
            mark("resolver stream (read+seek)", False, repr(e)[:80])

        return {"rk": rk, "title": row["title"], "steps": steps,
                "first_fail": first_fail, "done": True}

    # ------------------------------------------------------------- routes
    @app.get("/health")
    async def health():
        r = {}
        try:
            r["resolver"] = resolver("/health", 10)
        except Exception as e:
            r["resolver"] = {"status": "unreachable", "error": repr(e)[:80]}
        r["web"] = {"status": "ok"}
        r["mounts"] = _mounts()
        return r

    @app.get("/api/summary")
    async def summary():
        # resolver-faaluur blokkeert niet de hele response: queue/failures/
        # mounts blijven zichtbaar (nuttig bij troubleshooting)
        try:
            st = resolver("/status", 30)
        except Exception as e:
            st = None
            resolver_err = repr(e)[:100]
        else:
            resolver_err = None
        # laatste succesvolle resolve + laatste fout uit resolver-events
        last_ok = last_err = None
        try:
            evs = st.get("recent_events", [])
            for e in evs[::-1]:
                if e.get("kind") == "resolution_succeeded" and not last_ok:
                    last_ok = e
                if e.get("kind") in ("resolution_failed", "candidate_failed",
                                     "upstream_read_failed") and not last_err:
                    last_err = e
        except Exception:
            pass
        # failures geclasificeerd
        classified = {}
        try:
            db = _db()
            rows = db.execute(
                "SELECT rk, title, status, last_error, fail_class, attempts, updated_at "
                "FROM queue WHERE status IN ('FAILED_RETRYABLE','FAILED_FINAL') "
                "ORDER BY updated_at DESC").fetchall()
            flist = []
            for r in rows:
                d = dict(r)
                cl = classify(d["last_error"], d["fail_class"], d["status"])
                d["category"] = cl["cat"]
                d["transient"] = cl["transient"]
                flist.append(d)
                key = cl["cat"]
                entry = classified.setdefault(key, {"count": 0, "last": None,
                                                    "first": None,
                                                    "transient": cl["transient"]})
                entry["count"] += 1
                ts = d.get("updated_at") or 0
                if not entry["last"] or ts > entry["last"]:
                    entry["last"] = ts
                if not entry["first"] or ts < entry["first"]:
                    entry["first"] = ts
        except Exception as e:
            flist = []
            classified = {"error": repr(e)[:100]}
        # worker verify-voorbeeld (read-only uit worker-state)
        verify_info = {}
        try:
            db = _db()
            db.row_factory = sqlite3.Row
            verify_info = {
                "done": db.execute("SELECT COUNT(*) c FROM queue WHERE status='DONE'").fetchone()["c"],
                "verified": db.execute("SELECT COUNT(*) c FROM queue WHERE verified=1").fetchone()["c"],
            }
        except Exception:
            verify_info = {"note": "worker-state niet beschikbaar"}
        return {
            "resolver_error": resolver_err,
            "items": (st or {}).get("items"),
            "sessions": (st or {}).get("sessions_open"),
            "queue": _queue_stats(),
            "mounts": _mounts(),
            "resources": (st or {}).get("resources"),
            "caches": (st or {}).get("caches"),
            "last_ok_resolve": (last_ok or None) and {
                "ts": last_ok.get("ts"), "item": last_ok.get("item_id"),
                "gen": last_ok.get("generation")},
            "last_error": (last_err or None) and {
                "ts": last_err.get("ts"), "kind": last_err.get("kind"),
                "detail": {k: v for k, v in last_err.items()
                           if k in ("error", "hash", "reason")}},
            "failure_groups": classified,
            "failures": flist[:25],
            "worker_verify": verify_info,
        }

    @app.get("/api/trace/{rk}")
    async def trace(rk: str):
        return _trace(rk)

    @app.post("/api/action/resolve/{rk}")
    async def action_resolve(rk: str):
        import httpx
        row = _queue_row(rk)
        if row is None or not row.get("resolver_item_id"):
            return {"result": "fout", "error": "onbekend item",
                    "ts": time.strftime("%H:%M:%S")}
        try:
            async with httpx.AsyncClient(base_url=resolver_base, timeout=300) as cl:
                r = await cl.post(f"/media/{row['resolver_item_id']}/resolve")
                return {"result": "gestart" if r.status_code == 200 else "fout",
                        "http": r.status_code, "ts": time.strftime("%H:%M:%S"),
                        "body": r.json() if r.status_code == 200 else r.text[:200]}
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
                    "magic": head[:4] == bytes.fromhex("1a45dfa3") or head[4:8] == b"ftyp",
                    "ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            return {"result": "fout", "error": repr(e)[:100],
                    "ts": time.strftime("%H:%M:%S")}

    # ------------------------------------------------------------- UI
    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTMLResponse(HTML(_page("summary")))

    @app.get("/ui/trace/{rk}", response_class=HTMLResponse)
    async def ui_trace(rk: str):
        return HTMLResponse(HTML(_page("trace", rk)))

    def HTML(body: str) -> str:
        return f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>plex_scraper diagnostics</title>
<style>
:root {{ --bg:#14161a; --panel:#1c1f24; --fg:#d7dae0; --dim:#8b919b;
        --green:#4caf50; --red:#ef5350; --amber:#ffb74d; --blue:#64b5f6;
        --line:#2a2e35; }}
* {{ box-sizing:border-box; }}
body {{ background:var(--bg); color:var(--fg); font:13px/1.45 ui-monospace,
       "Cascadia Mono","Segoe UI",monospace; margin:0; padding:14px; }}
h1 {{ font-size:15px; margin:0 0 10px; color:var(--blue); }}
a {{ color:var(--blue); text-decoration:none; }}
.panel {{ background:var(--panel); border:1px solid var(--line); border-radius:6px;
          padding:10px 12px; margin-bottom:10px; overflow-x:auto; }}
.panel h3 {{ margin:0 0 8px; font-size:12px; color:var(--dim);
             text-transform:uppercase; letter-spacing:.5px; }}
table {{ border-collapse:collapse; width:100%; }}
td, th {{ padding:3px 8px; text-align:left; border-bottom:1px solid var(--line);
          white-space:nowrap; }}
th {{ color:var(--dim); font-weight:500; }}
.ok {{ color:var(--green); }} .bad {{ color:var(--red); }} .warn {{ color:var(--amber); }}
.muted {{ color:var(--dim); }}
.kpi {{ display:inline-block; background:var(--bg); border:1px solid var(--line);
        border-radius:5px; padding:6px 12px; margin:2px 4px 2px 0; }}
.kpi b {{ font-size:16px; }} .kpi span {{ color:var(--dim); font-size:11px; }}
.alert {{ border:1px solid var(--amber); background:#2a2416; color:var(--amber);
          border-radius:6px; padding:8px 12px; margin-bottom:10px; }}
.alert.bad {{ border-color:var(--red); background:#2a1718; color:var(--red); }}
button {{ background:#2a2e35; color:var(--fg); border:1px solid #3a3f47;
          border-radius:4px; padding:4px 10px; cursor:pointer; }}
button:hover {{ background:#3a3f47; }}
input {{ background:var(--bg); color:var(--fg); border:1px solid #3a3f47;
         border-radius:4px; padding:4px 8px; }}
li {{ list-style:none; padding:7px 0; border-bottom:1px solid var(--line); }}
.ff {{ color:var(--red); font-weight:700; }}
@media (max-width: 700px) {{ td, th {{ white-space:normal; }} }}
</style></head><body>{body}</body></html>"""

    def _page(mode: str, rk: str = "") -> str:
        nav = ('<h1>plex_scraper diagnostics</h1>'
               '<p><a href="/">summary</a> · trace: '
               '<form style="display:inline" onsubmit="location='
               '\'./ui/trace/\'+document.getElementById(\'rk\').value;return false">'
               '<input id="rk" placeholder="ratingKey" size="12">'
               '<button>trace</button></form></p>')
        if mode == "summary":
            body = f"""{nav}
<div id="alert"></div>
<div id="kpi" class="panel"></div>
<div class="panel" id="comp"></div>
<div class="panel"><h3>activity</h3><div id="act"></div></div>
<div class="panel"><h3>failure-classificatie</h3><div id="fc"></div></div>
<div class="panel"><h3>failures (recent)</h3><div id="f"></div></div>
<script>
const esc = s => String(s??'').replace(/</g,'&lt;');
async function load() {{
  const s = await (await fetch('./api/summary')).json();
  if (s.error) {{ document.getElementById('alert').innerHTML =
    '<div class="alert bad">resolver onbereikbaar: '+esc(s.error)+'</div>'; return; }}
  const items = s.items || {{}};
  const fail = (items.NO_SOURCE||0);
  const q = s.queue || {{}};
  const alerts = [];
  if (fail > 0) alerts.push(fail+' NO_SOURCE items');
  if ((q.FAILED_RETRYABLE||0) > 0) alerts.push(q.FAILED_RETRYABLE+' retryable failures');
  if ((q.FAILED_FINAL||0) > 0) alerts.push(q.FAILED_FINAL+' final failures');
  if (!s.mounts || !s.mounts.vfs) alerts.push('VFS-mount niet actief');
  document.getElementById('alert').innerHTML = alerts.length ?
    '<div class="alert">⚠ '+alerts.join(' · ')+'</div>' : '';
  const k = (v,l,c) => '<div class="kpi"><b class="'+(c||'')+'">'+v+
                       '</b><br><span>'+l+'</span></div>';
  document.getElementById('kpi').innerHTML =
    k(items.READY||0,'ready','ok') + k(items.NO_SOURCE||0,'no_source',
      items.NO_SOURCE?'warn':'') +
    k(q.FAILED_RETRYABLE||0,'retry', q.FAILED_RETRYABLE?'warn':'ok') +
    k(q.FAILED_FINAL||0,'failed final', q.FAILED_FINAL?'bad':'ok') +
    k(s.sessions||0,'sessions') + k(items.total||0,'total');
  const lo = s.last_ok_resolve, le = s.last_error;
  const comp = [
    ['resolver', s.items ? 'ok' : 'bad', s.resources ? 'rss '+s.resources.rss_kb+'KB' : ''],
    ['scraper', 'ok', 'n.v.t. hier (aparte rol)'],
    ['vfs (testset)', s.mounts && s.mounts.vfs ? 'ok' : 'bad', 'FUSE'],
    ['vfs (legacy)', s.mounts && s.mounts.vfs_legacy ? 'ok' : 'bad', 'FUSE'],
    ['web', 'ok', 'deze pagina'],
    ['TorBox', 'unknown', 'via resolver-events'],
  ].map(r => '<tr><td>'+r[0]+'</td><td class="'+r[1]+'">'+r[1]+
             '</td><td class="muted">'+esc(r[2])+'</td></tr>').join('');
  document.getElementById('comp').innerHTML =
    '<table>'+comp+'</table><p class="muted">laatste OK resolve: '+
    (lo ? new Date(lo.ts*1000).toLocaleTimeString()+' gen '+lo.gen : '—')+
    ' · laatste fout: '+
    (le ? esc(le.kind)+' @ '+new Date(le.ts*1000).toLocaleTimeString() : '—')+
    '</p>';
  document.getElementById('act').innerHTML =
    '<table><tr><th>resolutions</th><th>gen-switches</th><th>reads</th></tr>'+
    '<tr><td>'+s.resolutions+'</td><td>'+s.generation_switches+
    '</td><td>'+((s.caches||{{}}).candidates||{{}}).entries+' cached cand.</td></tr></table>';
  const fc = s.failure_groups || {{}};
  let fcr = '';
  Object.entries(fc).forEach(([cat, v]) => {{
    if (cat === 'error') return;
    const cls = v.transient === true ? 'warn' : (v.transient === false ? 'bad' : 'muted');
    fcr += '<tr><td class="'+cls+'">'+esc(cat)+'</td><td>'+v.count+'</td><td>'+
           (v.last ? new Date(v.last*1000).toLocaleString() : '—')+'</td><td>'+
           (v.transient===true?'transient':v.transient===false?'persistent':'?')+'</td></tr>';
  }});
  document.getElementById('fc').innerHTML = fcr ?
    '<table><tr><th>categorie</th><th>aantal</th><th>laatste</th><th>aard</th></tr>'+fcr+'</table>'
    : '<span class="ok">geen failures geclasificeerd</span>';
  const fl = s.failures || [];
  let rows = '';
  fl.forEach(r => {{
    rows += '<tr><td>'+r.rk+'</td><td>'+esc(r.title||'')+'</td><td>'+r.status+
            '</td><td class="warn">'+esc(r.category||'')+'</td><td>'+r.attempts+
            '</td><td><a href="./ui/trace/'+r.rk+'">trace</a></td></tr>';
  }});
  document.getElementById('f').innerHTML = fl.length ?
    '<table><tr><th>rk</th><th>title</th><th>state</th><th>categorie</th><th>tries</th><th></th></tr>'+rows+'</table>'
    : '<span class="ok">geen failures</span>';
}}
load(); setInterval(load, 15000);
</script>"""
        else:
            body = f"""{nav}
<div id="t" class="panel"><h3>trace {rk}</h3>loading…</div>
<div class="panel" id="a"></div>
<script>
const esc = s => String(s??'').replace(/</g,'&lt;');
async function load() {{
  const t = await (await fetch('./api/trace/{rk}')).json();
  let rows = '';
  (t.steps||[]).forEach(s => {{
    const cls = s.state === 'WARN' ? 'warn' : (s.ok ? 'ok' : 'bad');
    const mark = s.state === 'FAIL' ? '<span class="ff">FIRST FAIL ▼</span>' :
                 (s.state === 'WARN' ? '<span class="warn">WARN</span>' : '');
    rows += '<li><span class="'+cls+'">'+(s.state==='PASS'?'●':'✗')+'</span> <b>'+
            esc(s.step)+'</b> '+mark+' <span class="muted">'+s.ts+'</span><br>'+
            '<span class="muted">'+esc(s.detail)+'</span></li>';
  }});
  document.getElementById('t').innerHTML = '<b>'+esc(t.title||'{rk}')+
    '</b> <span class="muted">rk {rk}</span>'+
    (t.note ? '<p class="warn">'+esc(t.note)+'</p>' : '')+
    '<ul class="steps">'+rows+'</ul>';
  document.getElementById('a').innerHTML =
    '<button onclick="act(\\'resolve\\')">retry resolve</button> '+
    '<button onclick="act(\\'readtest\\')">retry read-test</button> '+
    '<button onclick="load()">refresh trace</button>'+
    ' <span class="muted">geen delete-acties</span><div id="ares"></div>';
}}
async function act(kind) {{
  const r = await (await fetch('./api/action/'+kind+'/{rk}', {{method:'POST'}})).json();
  document.getElementById('ares').innerHTML =
    '<p class="'+(r.result==='ok'||r.result==='gestart'?'ok':'bad')+'">'+
    esc(r.result)+' @ '+esc(r.ts||'')+' '+esc(r.error||r.error||'')+'</p>';
  load();
}}
load();
</script>"""
        return body

    return app
