"""Aggregate container-health: alle rol-processen in deze container.
Gebruikt door docker healthcheck (healthcheck.py).

F13-hardening: als HEALTH_ROLES niet expliciet is gezet, worden de rollen
afgeleid uit ROLE_SPECS (wat deze container werkelijk draait). Een vergeten
HEALTH_ROLES kan een VFS-container daardoor niet meer op het verkeerde
(HTTP-)rolenset laten checken; VFS checkt zijn FUSE-mounts als sentinel."""
from __future__ import annotations

import json
import os
import sys
import urllib.request


def _health_roles() -> list[str]:
    explicit = os.environ.get("HEALTH_ROLES")
    if explicit is not None:
        try:
            return json.loads(explicit)
        except ValueError:
            pass
    try:
        specs = json.loads(os.environ.get("ROLE_SPECS", "[]"))
        return list(dict.fromkeys(s["role"] for s in specs))
    except (ValueError, KeyError, TypeError):
        return ["resolver", "scraper", "web"]


PORTS = json.loads(os.environ.get(
    "HEALTH_PORTS", '{"resolver": 8282, "scraper": 8283, "web": 8285}'))
VFS_MOUNTS = ("/mnt/cache/appdata/plex-scraper/vfs", "/mnt/remote/nzbdav")


def main() -> int:
    roles = _health_roles()
    results, failed = {}, []
    if roles == ["vfs"]:
        # VFS: geen HTTP-rollen — health = FUSE-mountpoints statbaar (goedkope
        # sentinel, geen library-scan; business-state hoort hier niet).
        for mp in VFS_MOUNTS:
            try:
                os.statvfs(mp)
                results[mp] = "mounted"
            except OSError as e:
                results[mp] = f"FAIL {e.__class__.__name__}"
                failed.append(mp)
    else:
        for role in roles:
            port = PORTS.get(role)
            if not port:
                results[role] = "no-port"
                continue
            try:
                with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/health", timeout=5) as r:
                    results[role] = r.status
                    if r.status != 200:
                        failed.append(role)
            except Exception as e:
                results[role] = f"FAIL {e.__class__.__name__}"
                failed.append(role)
    print(json.dumps(results))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
