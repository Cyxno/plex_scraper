"""Physical serving-health: KAN PLEX DE BYTES LEZEN?

De incidentles (2026-10-05, PLEX_NAMESPACE_STALE_FUSE): een gezonde
VFS-container zegt niets over de consumer-namespace. Deze monitor draait
een goedkope sentinel IN de Plex-container (via docker exec) en classificeert
de faalwijze exact. Bij een ondubbelzinnige stale-FUSE-signature met een
gezonde VFS-container herstelt hij één keer gericht (Plex-restart) met
cooldown en without loop-mogelijkheid.
"""
from __future__ import annotations

import asyncio
import json
import time

DOCKER_SOCK = "/var/run/docker.sock"
PLEX_CONTAINER = "plex"
CHECK_INTERVAL_S = 60.0
RECOVERY_COOLDOWN_S = 1800.0
FAILURES_BEFORE_FAILED = 2

# Sentinel-script dat IN de plex-container draait (python3 is daar aanwezig).
# Print één regel: HEALTHY of <ERROR_CLASS> ok=<n>/<total>. Bounded via alarm.
SENTINEL = r"""
import os, signal, sys
signal.alarm(15)
def fail(cls):
    print(cls); sys.exit(1)
try:
    os.stat("/mnt/remote/nzbdav/.ids")
except OSError as e:
    fail("PLEX_NAMESPACE_STALE_FUSE" if "Transport endpoint" in str(e)
         else "MOUNT_MISSING")
ok, total = 0, 2
paths = []
p = "/symlinks/TV Shows/MobLand (2025)/Season 2"
if os.path.isdir(p):
    for f in sorted(os.listdir(p)):
        if f.endswith((".mkv", ".mp4")):
            paths.append(os.path.join(p, f)); break
movies = "/symlinks/Movies"
try:
    first_dir = sorted(os.scandir(movies), key=lambda e: e.name)[0].path
    for f in sorted(os.listdir(first_dir)):
        if f.endswith((".mkv", ".mp4")):
            paths.append(os.path.join(first_dir, f)); break
except OSError as e:
    fail("READ_ERROR")
for pth in paths:
    try:
        with open(pth, "rb") as fh:
            fh.read(65536)
        ok += 1
    except OSError as e:
        if "Transport endpoint" in str(e):
            fail("PLEX_NAMESPACE_STALE_FUSE")
print(f"HEALTHY ok={ok}/{total}")
"""

ERROR_CLASSES = ("HEALTHY", "PLEX_NAMESPACE_STALE_FUSE", "MOUNT_MISSING",
                 "PATH_MISSING", "READ_TIMEOUT", "READ_ERROR")


def classify(raw: str) -> str:
    """Exacte faal-klasse uit de sentinel-uitvoer (L2)."""
    head = (raw or "").strip().splitlines()[0] if (raw or "").strip() else ""
    for cls in ERROR_CLASSES:
        if head.startswith(cls):
            return cls
    if "Transport endpoint" in raw:
        return "PLEX_NAMESPACE_STALE_FUSE"
    return "READ_ERROR"


def decide_recovery(*, check_status: str, vfs_healthy: bool,
                    last_recovery_at: float, now: float) -> tuple[bool, str]:
    """Pure recovery-beslissing (H/H1): één poging per incident, cooldown,
    alleen bij exacte signature én gezonde VFS. Geen restart-loop mogelijk."""
    if check_status != "PLEX_NAMESPACE_STALE_FUSE":
        return False, "geen stale-FUSE-signature"
    if not vfs_healthy:
        return False, "VFS-container niet healthy — herstel heeft geen zin"
    if now - last_recovery_at < RECOVERY_COOLDOWN_S:
        return False, f"cooldown ({RECOVERY_COOLDOWN_S:.0f}s)"
    return True, "stale-FUSE + gezonde VFS: één gerichte Plex-restart"


class PhysicalHealthMonitor:
    """Periodieke consumer-chain check + begrensde auto-recovery."""

    def __init__(self, store, *, plex_container: str = PLEX_CONTAINER,
                 docker_sock: str = DOCKER_SOCK):
        self.store = store
        self.plex = plex_container
        self.sock = docker_sock
        self.consecutive_failures = 0
        self.last_recovery_at = 0.0
        self.state: dict = {"status": "UNKNOWN"}

    # ---------------------------------------------------------- docker API
    def _docker(self, method: str, path: str, body: dict | None = None) -> dict:
        import http.client
        conn = http.client.HTTPConnection("localhost", timeout=30)
        import socket as _s
        conn.sock = None
        # http.client over unix-socket: handmatige connect
        s = _s.socket(_s.AF_UNIX, _s.SOCK_STREAM)
        s.settimeout(60)
        s.connect(self.sock)
        payload = json.dumps(body or {}).encode()
        req = (f"{method} /v1.41{path} HTTP/1.1\r\nHost: docker\r\n"
               f"Content-Type: application/json\r\n"
               f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n")
        s.sendall(req.encode() + payload)
        chunks = []
        while True:
            b = s.recv(65536)
            if not b:
                break
            chunks.append(b)
        s.close()
        raw = b"".join(chunks)
        head, _, rest = raw.partition(b"\r\n\r\n")
        status = int(head.split(b" ")[1])
        # chunked framing strippen (docker streams zijn chunked)
        body_bytes = rest
        if b"Transfer-Encoding: chunked" in head:
            out, i = bytearray(), 0
            while True:
                j = rest.find(b"\r\n", i)
                if j < 0:
                    break
                n = int(rest[i:j], 16)
                if n == 0:
                    break
                out += rest[j + 2:j + 2 + n]
                i = j + 2 + n + 2
            body_bytes = bytes(out)
        if method == "POST" and "/exec" in path and "start" in path:
            # multiplexed stream: neem stdout-frames (type-byte 0)
            out, i = bytearray(), 0
            while i + 8 <= len(body_bytes):
                n = int.from_bytes(body_bytes[i + 4:i + 8], "big")
                if body_bytes[i] in (0, 1, 2):
                    out += body_bytes[i + 8:i + 8 + n]
                i += 8 + n
            return {"status": status, "output": out.decode("utf-8", "replace")}
        try:
            return {"status": status, "json": json.loads(body_bytes or b"{}")}
        except ValueError:
            return {"status": status, "json": {}}

    async def check_once(self) -> dict:
        """Eén sentinel-run in de plex-container + persistentie."""
        t0 = time.time()
        try:
            created = await asyncio.to_thread(
                self._docker, "POST",
                f"/containers/{self.plex}/exec",
                {"AttachStdout": True, "AttachStderr": True,
                 "Cmd": ["python3", "-c", SENTINEL]})
            eid = created["json"]["Id"]
            started = await asyncio.to_thread(
                self._docker, "POST", f"/exec/{eid}/start",
                {"Detach": False, "Tty": False})
            insp = await asyncio.to_thread(
                self._docker, "GET", f"/exec/{eid}/json")
            exit_code = insp["json"].get("ExitCode", 1)
            raw = started.get("output", "")
        except Exception as exc:                       # docker onbereikbaar
            status, raw = "READ_ERROR", f"docker exec failed: {exc!r}"[:120]
        else:
            status = classify(raw) if exit_code == 0 or raw else "READ_ERROR"
        latency = round(time.time() - t0, 2)
        result = {"status": status, "checked_at": t0, "latency_s": latency,
                  "raw": raw.strip()[:120]}
        await self._record(result)
        return result

    async def _record(self, result: dict) -> None:
        if result["status"] == "HEALTHY":
            self.consecutive_failures = 0
            result["last_healthy_at"] = result["checked_at"]
        else:
            self.consecutive_failures += 1
            if self.consecutive_failures < FAILURES_BEFORE_FAILED \
                    and result["status"] != "HEALTHY":
                # één blip is nog geen FAILED — markeer als SUSPECT
                result = {**result, "status": "SUSPECT",
                          "underlying": result["status"]}
        self.state = result
        await self.store.save_physical_health(result)

    async def maybe_recover(self) -> dict | None:
        """H: begrensde gerichte Plex-restart bij exacte signature."""
        if self.state.get("status") not in ("FAILED", "SUSPECT"):
            return None
        vfs = await asyncio.to_thread(
            self._docker, "GET",
            f"/containers/plex-scraper-vfs/json?size=false")
        vfs_healthy = vfs["json"].get("State", {}).get("Health", {}) \
            .get("Status") == "healthy"
        do_it, why = decide_recovery(
            check_status=self.state.get("underlying",
                                        self.state.get("status")),
            vfs_healthy=vfs_healthy,
            last_recovery_at=self.last_recovery_at, now=time.time())
        if not do_it:
            await self.store.add_event("physical_recovery_skipped",
                                       reason=why)
            return {"recovered": False, "reason": why}
        self.last_recovery_at = time.time()
        await asyncio.to_thread(self._docker, "POST",
                                f"/containers/{self.plex}/restart?t=10")
        await self.store.add_event("plex_restart_recovery",
                                   reason="PLEX_NAMESPACE_STALE_FUSE")
        await asyncio.sleep(25)
        result = await self.check_once()
        return {"recovered": result["status"] == "HEALTHY",
                "post_status": result["status"]}

    async def run(self):
        while True:
            try:
                await self.check_once()
                await self.maybe_recover()
            except Exception as exc:                   # noqa: BLE001
                from ..common.log import event
                event("physical_check_error", error=repr(exc)[:160])
            await asyncio.sleep(CHECK_INTERVAL_S)

    async def library_audit(self) -> dict:
        """E (lage frequentie): bounded path-audit in de plex-namespace."""
        script = (
            "import os,sys\n"
            "import sqlite3\n"
            "db=sqlite3.connect('file:/config/Plex Media Server/Plug-in Support/"
            "Databases/com.plexapp.plugins.library.db?mode=ro',uri=True)\n"
            "n=e=i=0\n"
            "for (f,) in db.execute('SELECT file FROM media_parts WHERE file "
            "IS NOT NULL'):\n"
            "    n+=1\n"
            "    try: os.lstat(f); e+=1\n"
            "    except OSError: i+=1\n"
            "print(f'PARTS n={n} exists={e} missing={i}')\n")
        created = await asyncio.to_thread(
            self._docker, "POST", f"/containers/{self.plex}/exec",
            {"AttachStdout": True, "Cmd": ["python3", "-c", script]})
        started = await asyncio.to_thread(
            self._docker, "POST", f"/exec/{created['json']['Id']}/start",
            {"Detach": False, "Tty": False})
        line = started.get("output", "").strip().splitlines()[-1:] or [""]
        summary = line[0]
        await self.store.add_event("library_audit", summary=summary)
        return {"summary": summary}
