"""Soak-harness: autonome reliability-metrix zonder AI/sessie-afhankelijkheid.

Elke SOAK_INTERVAL_S (default 600s) wordt één goedkope sample naar
/data/soak/samples.jsonl geschreven; afwijkingen t.o.v. de vorige sample
gaan naar incidents.jsonl. Retentie: 7 dagen + hard file-cap. Herstart-veilig
(append-only +旋转 op leeftijd/grootte). Geen provider-calls, geen load.
"""
from __future__ import annotations

import asyncio
import json
import os
import time

SOAK_INTERVAL_S = float(os.environ.get("SOAK_INTERVAL_S", "600"))
SOAK_DIR = os.environ.get("SOAK_DIR", "/data/soak")
RETENTION_S = 7 * 86400
MAX_FILE_BYTES = 8 * 1024 * 1024


def _rotate(path: str) -> None:
    if not os.path.exists(path):
        return
    if os.path.getsize(path) < MAX_FILE_BYTES:
        return
    os.replace(path, path + f".{int(time.time())}.old")
    _prune(path)


def _prune(path: str) -> None:
    """F5: verwijder rotatie-bestanden ouder dan RETENTION_S."""
    d = os.path.dirname(path)
    base = os.path.basename(path)
    now = time.time()
    try:
        for f in os.listdir(d):
            if f.startswith(base + ".") and f.endswith(".old"):
                p = os.path.join(d, f)
                if now - os.path.getmtime(p) > RETENTION_S:
                    os.unlink(p)
    except OSError:
        pass


def _append(path: str, record: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _rotate(path)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, separators=(",", ":")) + "\n")


def collect_sample(prev: dict | None, *, store, docker_fn, db_path="/data/state.db",
                   api_port=8282) -> dict:
    """Eén goedkope sample; puur-orkestratie rond lokale bronnen."""
    import http.client
    import urllib.request
    s = {"ts": time.time()}
    # containers (health, restarts, uptime, pids)
    s["containers"] = {}
    for name in ("plex-scraper-core", "plex-scraper-vfs", "plex"):
        try:
            info = docker_fn("GET", f"/containers/{name}/json?size=false")["json"]
            st = info.get("State", {})
            s["containers"][name] = {
                "running": st.get("Running"),
                "health": (st.get("Health") or {}).get("Status"),
                "restarts": info.get("RestartCount"),
                "started": info.get("State", {}).get("StartedAt", "")[:19],
                "pids": info.get("Pids", None),
            }
        except Exception as exc:                        # noqa: BLE001
            s["containers"][name] = {"error": repr(exc)[:80]}
    # fysieke keten
    try:
        row = store_sync_get_physical(db_path)
        s["physical"] = {"status": row[0], "checked_at": row[1]}
    except Exception:
        s["physical"] = {"status": "UNKNOWN"}
    # db / events / jobs
    try:
        import sqlite3
        c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        s["db_bytes"] = os.path.getsize(db_path)
        wal = db_path + "-wal"
        s["wal_bytes"] = os.path.getsize(wal) if os.path.exists(wal) else 0
        q = lambda sql: c.execute(sql).fetchone()[0]    # noqa: E731
        s["events"] = q("SELECT count(*) FROM events")
        s["maintenance_runs"] = q("SELECT count(*) FROM maintenance_runs")
        s["ready"] = q("SELECT count(*) FROM media_items WHERE status='READY'")
        s["no_source"] = q("SELECT count(*) FROM media_items WHERE status='NO_SOURCE'")
        s["sessions_open"] = q("SELECT count(*) FROM sessions WHERE state='open'")
        since = (prev or {}).get("ts", s["ts"] - SOAK_INTERVAL_S)
        s["resolution_crashed_delta"] = q(
            "SELECT count(*) FROM events WHERE kind='resolution_crashed' AND ts>?",
            (since,)) if False else _q1(c, "resolution_crashed", since)
        s["provider_4xx_delta"] = _q1(c, "candidate_failed", since, "HTTP 4")
        s["plex_recoveries_delta"] = _q1(c, "plex_restart_recovery", since)
        c.close()
    except Exception as exc:                            # noqa: BLE001
        s["db_error"] = repr(exc)[:80]
    # resources van de eigen container (cgroup, goedkoop)
    s["fds_self"] = _count_fds()
    s["ram_mib_self"] = _cgroup_ram_mib()
    # cockpit-latency (1 lokale GET)
    t0 = time.time()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", api_port, timeout=10)
        conn.request("GET", "/api/dashboard")
        conn.getresponse().read()
        s["dashboard_latency_ms"] = round((time.time() - t0) * 1000, 1)
    except Exception:
        s["dashboard_latency_ms"] = None
    return s


def _q1(c, kind: str, since: float, like: str | None = None) -> int:
    if like:
        return c.execute(
            "SELECT count(*) FROM events WHERE kind=? AND ts>? AND payload LIKE ?",
            (kind, since, f"%{like}%")).fetchone()[0]
    return c.execute("SELECT count(*) FROM events WHERE kind=? AND ts>?",
                     (kind, since)).fetchone()[0]


def _count_fds() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


def _cgroup_ram_mib() -> float:
    for p in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            return round(int(open(p).read()) / 2**20, 1)
        except (OSError, ValueError):
            continue
    return -1


def store_sync_get_physical(db_path: str):
    import sqlite3
    c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return c.execute("SELECT status, checked_at FROM physical_health WHERE id=1").fetchone() \
            or ("UNKNOWN", None)
    finally:
        c.close()


def diff_incidents(prev: dict | None, cur: dict) -> list[dict]:
    """F3: state-overgangen en foutsignalen → incident-record."""
    out = []
    if not prev:
        return out
    for name, cur_i in (cur.get("containers") or {}).items():
        prev_i = (prev.get("containers") or {}).get(name) or {}
        if prev_i.get("health") and prev_i["health"] != cur_i.get("health"):
            out.append({"ts": cur["ts"], "kind": "container_health_transition",
                        "container": name, "from": prev_i["health"],
                        "to": cur_i.get("health")})
        if prev_i.get("restarts") is not None \
                and cur_i.get("restarts") is not None \
                and cur_i["restarts"] != prev_i["restarts"]:
            out.append({"ts": cur["ts"], "kind": "container_restart",
                        "container": name})
    if cur.get("physical", {}).get("status") not in (None, "HEALTHY", "UNKNOWN") \
            and cur["physical"]["status"] != (prev.get("physical") or {}).get("status"):
        out.append({"ts": cur["ts"], "kind": "physical_health",
                    "status": cur["physical"]["status"]})
    if cur.get("resolution_crashed_delta"):
        out.append({"ts": cur["ts"], "kind": "resolution_crashed",
                    "n": cur["resolution_crashed_delta"]})
    if cur.get("plex_recoveries_delta"):
        out.append({"ts": cur["ts"], "kind": "plex_restart_recovery",
                    "n": cur["plex_recoveries_delta"]})
    if cur.get("db_error"):
        out.append({"ts": cur["ts"], "kind": "db_error",
                    "error": cur["db_error"]})
    return out


async def run(store, docker_fn) -> None:
    """F8: hergebruikt de resolver-asyncio-loop; geen externe scheduler."""
    os.makedirs(SOAK_DIR, exist_ok=True)
    _prune(os.path.join(SOAK_DIR, "samples.jsonl"))
    prev_path = os.path.join(SOAK_DIR, "samples.jsonl")
    prev = None
    if os.path.exists(prev_path):
        try:
            with open(prev_path, "rb") as fh:
                lines = fh.readlines()[-2:]
            if len(lines) >= 2:
                prev = json.loads(lines[-2])
        except Exception:
            prev = None
    while True:
        try:
            cur = await asyncio.to_thread(
                collect_sample, prev, store=store, docker_fn=docker_fn)
            for inc in diff_incidents(prev, cur):
                _append(os.path.join(SOAK_DIR, "incidents.jsonl"),
                        {**inc, "ts": cur["ts"]})
            _append(prev_path, cur)
            prev = cur
        except Exception:                               # noqa: BLE001
            pass
        await asyncio.sleep(SOAK_INTERVAL_S)
