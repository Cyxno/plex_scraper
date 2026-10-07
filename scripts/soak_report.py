#!/usr/bin/env python3
"""Soak-analyse (Phase 38): vlagt anomalieën in ingest-soak.jsonl.

  python3 scripts/soak_report.py /mnt/cache/appdata/plex-scraper/soak/ingest-soak.jsonl
Exit 1 bij anomalieën (alarm-geschikt), 0 indien saai (goed zo).
"""
from __future__ import annotations
import json
import re
import sys


def pct(v):
    try:
        return float(v.rstrip("%"))
    except (ValueError, AttributeError):
        return 0.0


def mem_mb(v):
    m = re.match(r"([\d.]+)([MG]iB)", v or "")
    if not m:
        return 0.0
    return float(m.group(1)) * (1024 if m.group(2) == "GiB" else 1)


def completed(row):
    q = row.get("counts_by_state") or {}
    return sum(q.get(k, 0) for k in ("COMPLETED",))


def main(path: str) -> int:
    rows = []
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if len(rows) < 2:
        print(f"nog te weinig samples ({len(rows)}) — laat de monitor lopen")
        return 0
    flags: list[str] = []
    first, last = rows[0], rows[-1]

    # herstarts
    for name in ("core", "vfs"):
        a = (first.get(name) or "0").split()[0]
        b = (last.get(name) or "0").split()[0]
        if a != b:
            flags.append(f"HERSTART: {name} restart-count {a} -> {b}")

    # queue groeit zonder afname
    qc = completed(first)
    ql = completed(last)
    gained = ql - qc
    remaining = sum((last.get("counts_by_state") or {}).get(k, 0) for k in
                    ("QUEUED", "FAILED_RETRYABLE", "PROVIDER_WAIT"))
    print(f"samples={len(rows)}  completions {qc} -> {ql} (+{gained})  "
          f"open={remaining}")

    # provider healthy maar niets af
    healthy_rows = [r for r in rows if (r.get("provider") or "HEALTHY") == "HEALTHY"]
    if healthy_rows and completed(healthy_rows[0]) == completed(healthy_rows[-1]) \
            and len(healthy_rows) >= 6:
        flags.append("STILVAL: provider HEALTHY maar 0 completions gedurende "
                     f"{len(healthy_rows)} samples ({len(healthy_rows)*10} min)")

    # herhaalde 429-burst (provider niet HEALTHY in >30% van samples)
    degr = sum(1 for r in rows if (r.get("provider") or "HEALTHY") != "HEALTHY")
    if len(rows) >= 10 and degr / len(rows) > 0.3:
        flags.append(f"PROVIDER: {degr}/{len(rows)} samples niet HEALTHY")

    # geheugen-trend (core)
    mems = []
    for r in rows:
        m = re.search(r"plex-scraper-core=([\d.]+%);([\d.]+[MG]iB)",
                      r.get("resources") or "")
        if m:
            mems.append(mem_mb(m.group(2)))
    if len(mems) >= 6 and mems[-1] > mems[0] * 1.5:
        flags.append(f"GEHEUGEN: core {mems[0]:.0f}MB -> {mems[-1]:.0f}MB "
                     "(>1.5x over soak)")

    # canary rood
    red = sum(1 for r in rows if not r.get("canary_ok"))
    if red:
        flags.append(f"CANARY: {red}/{len(rows)} samples rood")

    # fysiek
    if any((r.get("physical") or "").find("DEGRADED") >= 0 or
           (r.get("physical") or "").find("FAILED") >= 0 for r in rows):
        flags.append("FYSIEK: physical health niet HEALTHY geweest")

    for f in flags:
        print("ANOMALIE:", f)
    print("ok" if not flags else f"{len(flags)} anomalieën")
    return 1 if flags else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else
                  "/mnt/cache/appdata/plex-scraper/soak/ingest-soak.jsonl"))
