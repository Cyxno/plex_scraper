#!/bin/bash
# F6/F7: eindrapport uit de soak-harness (draait in plex-scraper-core).
exec docker exec plex-scraper-core python3 - <<'PY'
import glob, json, os, statistics, sys

d = "/data/soak"
def load(name):
    p = os.path.join(d, name)
    if not os.path.exists(p):
        return []
    out = []
    for line in open(p):
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out

samples = load("samples.jsonl")
incidents = load("incidents.jsonl")
if not samples:
    print("GEEN SAMPLES — harness heeft niet gedraaid"); sys.exit(1)

def pct(v, p):
    v = sorted(x for x in v if x is not None)
    return round(v[min(int(len(v)*p), len(v)-1)], 2) if v else None

def cnt(key):
    return sum(1 for i in incidents if i["kind"] == key)

print(f"Duration : {round(samples[-1]['ts']-samples[0]['ts'])}s ({round((samples[-1]['ts']-samples[0]['ts'])/3600,1)}h)")
print(f"Samples  : {len(samples)} (cadans 600s)")
for k in ("ready","no_source"):
    print(f"{k:9}: start={samples[0].get(k)} end={samples[-1].get(k)} max={max(s.get(k,0) for s in samples)}")
phys = [s.get("physical",{}).get("status") for s in samples]
print(f"Physical : HEALTHY={phys.count('HEALTHY')} SUSPECT={phys.count('SUSPECT')} "
      f"FAILED={phys.count('FAILED')} other={len(phys)-phys.count('HEALTHY')-phys.count('SUSPECT')-phys.count('FAILED')}")
lat = [s.get("dashboard_latency_ms") for s in samples]
print(f"Cockpit  : p50={pct(lat,.5)}ms p95={pct(lat,.95)}ms max={pct(lat,1)}ms")
ram = [s.get("ram_mib_self") for s in samples]
fds = [s.get("fds_self") for s in samples]
print(f"Resources: RAM avg={round(statistics.mean(ram),1)}MiB max={max(ram)} | FD start={samples[0].get('fds_self')} max={max(fds)} end={samples[-1].get('fds_self')}")
print(f"DB       : {round(samples[0].get('db_bytes',0)/2**20,1)}MB -> {round(samples[-1].get('db_bytes',0)/2**20,1)}MB | WAL max={round(max(s.get('wal_bytes',0) for s in samples)/2**20,1)}MB")
print(f"Sessions : start={samples[0].get('sessions_open')} max={max(s.get('sessions_open',0) for s in samples)} end={samples[-1].get('sessions_open')}")
print(f"PIDs core: max={max((s.get('containers',{}).get('plex-scraper-core',{}) or {}).get('pids') or 0 for s in samples)}")
print(f"Providers: candidate 4xx-delta totaal={sum(s.get('provider_4xx_delta',0) for s in samples)}")
for k in ("container_health_transition","container_restart","physical_health",
          "resolution_crashed","plex_restart_recovery","db_error"):
    n = cnt(k)
    if n: print(f"INCIDENT {k}: {n}")
    if k == "container_health_transition":
        for i in incidents:
            if i["kind"] == k:
                print(f"  {round(i['ts'])}: {i.get('container')} {i.get('from')}->{i.get('to')}")

# F7: flags
flags = []
unhealthy = any((s.get("containers",{}).get("plex-scraper-vfs",{}) or {}).get("health")=="unhealthy" for s in samples)
if phys.count("FAILED"): flags.append("FAIL: physical_health FAILED")
if cnt("plex_restart_recovery") > 3: flags.append("FAIL: herhaalde plex_restart_recovery")
if cnt("db_error"): flags.append("FAIL: db locked/error")
if cnt("resolution_crashed"): flags.append("FAIL: resolution_crashed")
if any(i.get("kind")=="container_restart" and i.get("container")=="plex-scraper-core" for i in incidents):
    flags.append("WARNING: onverwachte core-restart")
if unhealthy: flags.append("FAIL: VFS unhealthy gezien")
wal = max(s.get('wal_bytes',0) for s in samples)
if wal > 2*2**30: flags.append("FAIL: runaway WAL")
if fds and max(fds) > 2*max(samples[0].get("fds_self",1),50): flags.append("WARNING: FD-groei")
print("\n== FLAGS ==")
if flags:
    print("\n".join(flags))
else:
    print("PASS — geen faal-signalen in de observatieperiode")
PY
