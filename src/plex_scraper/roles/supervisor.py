"""Mini-supervisor: start meerdere rol-processen, bewaakt en herstart ze
individueel (met backoff), stuurt SIGTERM/SIGINT graceful door.

Rol-specificatie via env ROLE_SPECS (JSON array):
  [{"role": "resolver", "env": {"RESOLVER_BIND": "0.0.0.0:8282"}}, ...]

Elk kind is `python -m plex_scraper.roles.proc <role>` met zijn eigen env.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time

ROL_PROC = [sys.executable, "-m", "plex_scraper.roles.proc"]
BACKOFF = [1, 2, 4, 8, 15, 30]
STOP_GRACE = 30


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} supervisor {msg}"
    print(line, flush=True)


def load_specs() -> list[dict]:
    raw = os.environ.get("ROLE_SPECS", "").strip()
    if raw:
        return json.loads(raw)
    # default: alle rollen uit individuele env (single-role compat)
    role = os.environ.get("ROLE", "")
    return [{"role": role, "env": {}}] if role else []


def main() -> int:
    specs = load_specs()
    if not specs:
        log("geen ROLE_SPECS/ROLE — niets te starten")
        return 2
    stopping = False

    procs: dict[str, subprocess.Popen] = {}
    fails: dict[str, int] = {}
    last_start: dict[str, float] = {}

    def spawn(spec: dict) -> None:
        role = spec["role"]
        env = dict(os.environ)
        env.update({k: str(v) for k, v in (spec.get("env") or {}).items()})
        procs[role] = subprocess.Popen(ROL_PROC + [role], env=env)
        last_start[role] = time.time()
        log(f"gestart: {role} (pid {procs[role].pid})")

    def shutdown(signum, _frame):
        nonlocal stopping
        stopping = True
        log(f"signaal {signum} — graceful shutdown")
        for p in procs.values():
            try:
                p.terminate()
            except OSError:
                pass

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    for spec in specs:
        spawn(spec)

    while procs:
        time.sleep(2)
        if stopping:
            alive = [r for r, p in procs.items() if p.poll() is None]
            if not alive:
                log("alle processen gestopt — supervisor exit")
                break
            deadline = time.time() + STOP_GRACE
            while time.time() < deadline and any(
                    p.poll() is None for p in procs.values()):
                time.sleep(1)
            for p in procs.values():
                if p.poll() is None:
                    p.kill()
            log("geforceerd afgerond na grace")
            break
        for role, spec in [(s["role"], s) for s in specs]:
            p = procs.get(role)
            if p is None or p.poll() is None:
                continue
            code = p.returncode
            log(f"proces {role} gestopt (exit {code}) — herstart met backoff")
            delay = BACKOFF[min(fails.get(role, 0), len(BACKOFF) - 1)]
            fails[role] = fails.get(role, 0) + 1
            time.sleep(delay)
            spawn(spec)
    return 0


if __name__ == "__main__":
    sys.exit(main())
