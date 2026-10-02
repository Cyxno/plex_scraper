"""Aggregate container-health: alle rol-processen in deze container.
Gebruikt door docker healthcheck (healthcheck.py)."""
from __future__ import annotations

import json
import os
import sys
import urllib.request

ROLES = json.loads(os.environ.get("HEALTH_ROLES", '["resolver", "scraper", "web"]'))
PORTS = json.loads(os.environ.get(
    "HEALTH_PORTS", '{"resolver": 8282, "scraper": 8283, "web": 8285}'))


def main() -> int:
    results = {}
    failed = []
    for role in ROLES:
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
