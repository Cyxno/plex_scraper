"""Read-only diagnostics GUI (Fase 2). Leest resolver-API + SQLite-state en
doet live VFS/symlink checks. Enige schrijfacties: expliciete retry-acties via
de resolver-API (resolve/verify). Geen deletes, geen tweede bron van waarheid.
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

    def _trace(rk: str) -> dict:
        """Playback-path trace: Plex item -> resolver -> symlink -> VFS -> backend."""
        steps = []
        import httpx
        try:
            items = resolver("/media", 30)
        except Exception as e:
            return {"error": f"resolver onbereikbaar: {e!r}", "steps": steps}
        item = next((i for i in items if i.get("plex_path", "").split("/")[-1]
                     and False), None)
        # zoek via queue-state op rk
        row = _queue_row(rk)
        if row is None:
            return {"error": f"rk {rk} niet in migration-queue", "steps": steps}
        item = next((i for i in items if i["id"] == row["resolver_item_id"]), None)
        gen = row["generation"] if "generation" in row.keys() else 0
        steps.append({"step": "resolver record", "ok": item is not None,
                      "detail": f"{row['status']} gen={gen}" if item
                      else f"resolver_item_id {row['resolver_item_id']} ontbreekt"})
        if item is None:
            return {"steps": steps}
        steps.append({"step": "resolver status", "ok": item["status"] == "READY",
                      "detail": f"{item['status']} / {item['plex_path'][:60]}"})
        # active source
        src = None
        try:
            detail = resolver(f"/media/{item['id']}", 30)
            src = next((s for s in detail.get("sources", [])
                        if s.get("state") == "active"), None)
        except Exception:
            pass
        steps.append({"step": "source", "ok": src is not None,
                      "detail": (f"{src['info_hash'][:12]} {src.get('resolution')} "
                                 f"{(src.get('size_gb') or 0):.1f}GB") if src else "geen actieve source"})
        # symlink
        sym = row["symlink_host"]
        sym_ok = os.path.islink(sym)
        steps.append({"step": "symlink", "ok": sym_ok,
                      "detail": sym if sym_ok else f"ONTBREEKT: {sym}"})
        # vfs target
        tgt_ok = False
        tgt_detail = ""
        if sym_ok:
            tgt = os.readlink(sym)
            tgt_ok = os.path.exists(sym)
            tgt_detail = tgt
            steps.append({"step": "VFS target", "ok": tgt_ok,
                          "detail": f"{tgt[-60:]} ({'resolvet' if tgt_ok else 'GEBROKEN'})"})
        # byte-read via VFS
        if tgt_ok:
            try:
                with open(sym, "rb") as fh:
                    head = fh.read(8)
                magic = head[:4] == bytes.fromhex("1a45dfa3") or head[4:8] == b"ftyp"
                steps.append({"step": "byte-read", "ok": True,
                              "detail": f"head={head.hex()[:16]} magic={magic}"})
            except Exception as e:
                steps.append({"step": "byte-read", "ok": False,
                              "detail": repr(e)[:80]})
        # plex range via resolver stream? — resolver moet sessie openen; doe
        # een open+read+release roundtrip
        try:
            import httpx
            with httpx.Client(base_url=resolver_base, timeout=120) as cl:
                o = cl.post(f"/media/{item['id']}/open").json()
                handle = o.get("handle")
                if handle:
                    r = cl.get(f"/stream/{handle}", params={"offset": 0, "length": 64})
                    r2 = cl.get(f"/stream/{handle}",
                                params={"offset": 65536, "length": 64})
                    steps.append({"step": "resolver stream", "ok": len(r.content) > 0,
                                  "detail": f"read={len(r.content)}B seek={len(r2.content)}B"})
                    cl.delete(f"/open/{handle}")
        except Exception as e:
            steps.append({"step": "resolver stream", "ok": False,
                          "detail": repr(e)[:80]})
        return {"rk": rk, "title": row["title"], "steps": steps}

    def _queue_row(rk: str):
        db = _db()
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM queue WHERE rk=?", (rk,)).fetchone()
        return dict(row) if row else None

    def _db():
        # ro-bind: SQLite kan hier geen journal schrijven -> kopieer snapshot
        import shutil
        snap = '/tmp/migration-state.sqlite'
        try:
            shutil.copy2(settings.mig_db_path, snap)
        except Exception:
            pass
        return sqlite3.connect(snap, timeout=30)

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

    def _queue_stats() -> dict:
        db = _db()
        try:
            return {r["status"]: r["c"] for r in db.execute(
                "SELECT status, COUNT(*) c FROM queue GROUP BY status").fetchall()}
        except Exception:
            return {"error": "queue-db niet beschikbaar"}

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
        try:
            st = resolver("/status", 30)
        except Exception as e:
            return {"error": repr(e)[:100]}
        return {
            "items": st.get("items"),
            "sessions": st.get("sessions_open"),
            "resolutions": st.get("resolutions"),
            "generation_switches": st.get("generation_switches"),
            "latency": {"resolve_avg_s": st.get("resolve_latency_avg_s"),
                        "request_avg_ms": st.get("request_latency_avg_ms")},
            "resources": st.get("resources"),
            "caches": st.get("caches"),
            "queue": _queue_stats(),
            "mounts": _mounts(),
        }

    @app.get("/api/failures")
    async def failures():
        db = _db()
        try:
            db.row_factory = sqlite3.Row
            rows = db.execute(
                "SELECT rk, title, status, last_error, fail_class, attempts, updated_at "
                "FROM queue WHERE status IN ('FAILED_RETRYABLE','FAILED_FINAL') "
                "ORDER BY updated_at DESC LIMIT 25").fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            return {"error": repr(e)[:100]}

    @app.get("/api/trace/{rk}")
    async def trace(rk: str):
        return _trace(rk)

    @app.post("/api/action/resolve/{rk}")
    async def action_resolve(rk: str):
        import httpx
        row = _queue_row(rk)
        if row is None or not row.get("resolver_item_id"):
            return {"error": "onbekend item"}
        try:
            async with httpx.AsyncClient(base_url=resolver_base, timeout=300) as cl:
                r = await cl.post(f"/media/{row['resolver_item_id']}/resolve")
                return {"http": r.status_code, "body": r.json() if r.status_code == 200 else r.text[:200]}
        except Exception as e:
            return {"error": repr(e)[:100]}

    @app.post("/api/action/readtest/{rk}")
    async def action_readtest(rk: str):
        row = _queue_row(rk)
        if row is None:
            return {"error": "onbekend item"}
        sym = row["symlink_host"]
        try:
            with open(sym, "rb") as fh:
                head = fh.read(8)
            return {"ok": True, "head": head.hex()[:16],
                    "magic": head[:4] == bytes.fromhex("1a45dfa3") or head[4:8] == b"ftyp"}
        except Exception as e:
            return {"ok": False, "error": repr(e)[:100]}

    # ------------------------------------------------------------- UI
    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTML(_page("summary"))

    @app.get("/ui/trace/{rk}", response_class=HTMLResponse)
    async def ui_trace(rk: str):
        return HTML(_page("trace", rk))

    def HTML(body: str) -> str:
        return f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>plex_scraper diagnostics</title>
<style>
:root {{ --bg:#14161a; --panel:#1c1f24; --fg:#d7dae0; --dim:#8b919b;
        --green:#4caf50; --red:#ef5350; --amber:#ffb74d; --blue:#64b5f6; }}
* {{ box-sizing:border-box; }}
body {{ background:var(--bg); color:var(--fg); font:13px/1.45 -apple-system,
       "Segoe UI", monospace, sans-serif; margin:0; padding:16px; }}
h1 {{ font-size:16px; margin:0 0 12px; color:var(--blue); }}
a {{ color:var(--blue); text-decoration:none; }}
.panel {{ background:var(--panel); border:1px solid #2a2e35; border-radius:6px;
          padding:12px; margin-bottom:12px; overflow-x:auto; }}
table {{ border-collapse:collapse; width:100%; }}
td, th {{ padding:3px 8px; text-align:left; border-bottom:1px solid #2a2e35;
          white-space:nowrap; }}
th {{ color:var(--dim); font-weight:500; }}
.ok {{ color:var(--green); }} .bad {{ color:var(--red); }} .warn {{ color:var(--amber); }}
.muted {{ color:var(--dim); }}
button {{ background:#2a2e35; color:var(--fg); border:1px solid #3a3f47;
          border-radius:4px; padding:4px 10px; cursor:pointer; }}
button:hover {{ background:#3a3f47; }}
input {{ background:var(--bg); color:var(--fg); border:1px solid #3a3f47;
         border-radius:4px; padding:4px 8px; }}
.steps li {{ padding:6px 0; border-bottom:1px solid #2a2e35; list-style:none; }}
ul {{ padding:0; margin:0; }}
</style></head><body>{body}</body></html>"""

    def _page(mode: str, rk: str = "") -> str:
        nav = ('<h1>plex_scraper diagnostics</h1>'
               '<p><a href="/">summary</a> · '
               'trace: <form style="display:inline" onsubmit="location='
               '\'./ui/trace/\'+document.getElementById(\'rk\').value;return false">'
               '<input id="rk" placeholder="ratingKey" size="12">'
               '<button>trace</button></form></p>')
        if mode == "summary":
            body = f"""{nav}
<div id="s" class="panel">loading…</div>
<div class="panel" id="q"></div>
<div class="panel" id="f"></div>
<script>
async function load() {{
  const s = await (await fetch('./api/summary')).json();
  const e = document.getElementById('s');
  if (s.error) {{ e.innerHTML = 'resolver onbereikbaar: '+s.error; return; }}
  const mounts = Object.entries(s.mounts||{{}}).map(([k,v])=>k+':'+(v?'<span class="ok">OK</span>':'<span class="bad">DOWN</span>')).join(' · ');
  const q = s.queue||{{}};
  e.innerHTML = '<table>' +
   '<tr><th>resolver items</th><td>'+JSON.stringify(s.items)+'</td></tr>' +
   '<tr><th>mounts</th><td>'+mounts+'</td></tr>' +
   '<tr><th>queue</th><td>'+JSON.stringify(q)+'</td></tr>' +
   '<tr><th>sessions open</th><td>'+s.sessions+'</td></tr>' +
   '<tr><th>resolutions</th><td>'+s.resolutions+' · gen-switches '+s.generation_switches+'</td></tr>' +
   '<tr><th>latency</th><td>resolve '+s.latency.resolve_avg_s+'s · req '+s.latency.request_avg_ms+'ms</td></tr>' +
   '<tr><th>resources</th><td>'+JSON.stringify(s.resources)+'</td></tr>' +
   '<tr><th>caches</th><td>'+JSON.stringify(s.caches)+'</td></tr></table>';
  document.getElementById('q').innerHTML = '<b>queue</b> <span class="muted">(retry-failed via CLI)</span> '+JSON.stringify(q);
  const f = await (await fetch('./api/failures')).json();
  let rows = '';
  (Array.isArray(f)?f:[]).forEach(r => {{
    rows += '<tr><td>'+r.rk+'</td><td>'+(r.title||'')+'</td><td>'+r.status+
            '</td><td>'+(r.fail_class||'')+'</td><td>'+(r.last_error||'').slice(0,60)+
            '</td><td><a href="./ui/trace/'+r.rk+'">trace</a></td></tr>';
  }});
  document.getElementById('f').innerHTML = '<b>failures</b><table><tr><th>rk</th><th>title</th><th>state</th><th>class</th><th>error</th><th></th></tr>'+rows+'</table>';
}}
load(); setInterval(load, 15000);
</script>"""
        else:
            body = f"""{nav}
<div id="t" class="panel">trace {rk}…</div>
<div class="panel" id="a"></div>
<script>
async function load() {{
  const t = await (await fetch('./api/trace/{rk}')).json();
  let rows = '';
  (t.steps||[]).forEach(s => {{
    const cls = s.ok ? 'ok' : 'bad';
    rows += '<li><span class="'+cls+'">'+(s.ok?'●':'✗')+'</span> <b>'+s.step+
            '</b><br><span class="muted">'+s.detail+'</span></li>';
  }});
  document.getElementById('t').innerHTML = '<b>trace '+(t.title||'{rk}')+
    '</b> <span class="muted">rk {rk}</span><ul class="steps">'+rows+'</ul>';
  document.getElementById('a').innerHTML =
    '<button onclick="act(\\'resolve\\')">retry resolve</button> '+
    '<button onclick="act(\\'readtest\\')">retry read-test</button>'+
    ' <span class="muted">geen delete-acties</span>';
}}
async function act(kind) {{
  const r = await (await fetch('./api/action/'+kind+'/{rk}', {{method:'POST'}})).json();
  document.getElementById('a').innerHTML += '<pre>'+JSON.stringify(r,null,1).slice(0,500)+'</pre>';
  load();
}}
load();
</script>"""
        return body

    return app
