"""Queue-invariant audit (Phase 3, post-ingest hardening).

Controleert de harde invariants over de live ingest_jobs-tabel:

  I1  dedupe_key is UNIQUE (één rij per logisch arr-item)        [schema]
  I2  actieve jobs delen nooit één resolver_item_id              [identiteit]
  I3  COMPLETED-jobs hebben een delivered_symlink óf een
      completed_by_arr-event (geen COMPLETED zonder levering)    [afgerond=af]
  I4  BLOCKED/FAILED_FINAL jobs worden nooit door due_jobs
      opgepakt (terminal = terminal)                             [spin]
  I5  geen job in een actieve state met attempts >
      ingest_job_max_attempts zonder terminal of PROVIDER_WAIT   [begrensd]
  I6  PROVIDER_WAIT heeft een toekomstige next_attempt_at óf is
      net verlopen (cooldown wordt gerespecteerd)                [cooldown]
  I7  elke non-terminal job heeft een leesbare last_error of is
      nog verse (QUEUED, attempts<=1)                            [reden]

Gebruik (in de core-container):
  python /app/maintenance/queue_invariants.py            # menselijk
  python /app/maintenance/queue_invariants.py --json     # machine
Exit-code 0 = alle invariants groen, 1 = violaties gevonden.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time

DB_PATH = "/data/state.db"
ACTIVE_STATES = ("QUEUED", "IDENTITY_VERIFYING", "REGISTERING", "RESOLVING",
                 "PROVIDER_WAIT", "READY", "DELIVERING", "PLEX_REFRESH",
                 "FAILED_RETRYABLE")
TERMINAL_STATES = ("COMPLETED", "BLOCKED_IDENTITY", "BLOCKED_NO_SOURCE",
                   "BLOCKED_MAPPING", "FAILED_FINAL")


def audit(db_path: str = DB_PATH,
          max_attempts: int = 8) -> dict:
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    now = time.time()
    violations: list[dict] = []
    stats: dict = {}

    rows = c.execute("SELECT * FROM ingest_jobs").fetchall()
    stats["total_jobs"] = len(rows)
    stats["by_status"] = {}
    for r in rows:
        stats["by_status"][r["status"]] = \
            stats["by_status"].get(r["status"], 0) + 1

    # I1: dedupe_key uniek
    seen: dict[str, str] = {}
    for r in rows:
        if r["dedupe_key"] in seen:
            violations.append({"inv": "I1", "job": r["id"],
                               "detail": f"duplicate dedupe_key {r['dedupe_key']} "
                                         f"(ook {seen[r['dedupe_key']]})"})
        seen[r["dedupe_key"]] = r["id"]

    # I2: actieve jobs delen geen resolver_item_id (tenzij COMPLETED-rijen die
    # hetzelfde item rapporteren — die zijn legaal: zelfde item, klaar)
    active_items: dict[str, str] = {}
    for r in rows:
        if r["status"] not in ACTIVE_STATES or not r["resolver_item_id"]:
            continue
        if r["resolver_item_id"] in active_items and \
                active_items[r["resolver_item_id"]] != r["id"]:
            violations.append({
                "inv": "I2", "job": r["id"],
                "detail": f"resolver_item_id {r['resolver_item_id']} ook "
                          f"actief op {active_items[r['resolver_item_id']]}"})
        active_items[r["resolver_item_id"]] = r["id"]

    # I3: COMPLETED zonder levering
    for r in rows:
        if r["status"] != "COMPLETED":
            continue
        if not r["delivered_symlink"]:
            # geen symlink = completed_by_arr (hasFile) — accepteren als er
            # ooit een completed_by_arr-event is; anders violatie
            ev = c.execute(
                "SELECT COUNT(*) n FROM events WHERE kind='ingest_job_completed_by_arr' "
                "AND payload LIKE ?",
                (f'%{r["id"]}%',)).fetchone()
            if ev["n"] == 0:
                violations.append({
                    "inv": "I3", "job": r["id"],
                    "detail": "COMPLETED zonder delivered_symlink en zonder "
                              "completed_by_arr-event"})

    # I4: BLOCKED/FAILED_FINAL worden niet opgepakt — controleer de query-
    # contracten (due_jobs sluit ze uit; hier: ze mogen geen verse
    # next_attempt in de toekomst hebben gekregen door de worker)
    for r in rows:
        if r["status"] in TERMINAL_STATES and r["status"] != "COMPLETED" \
                and r["next_attempt_at"] > now + 1:
            violations.append({
                "inv": "I4", "job": r["id"],
                "detail": f"terminal {r['status']} kreeg toekomstige "
                          f"next_attempt ({r['next_attempt_at'] - now:.0f}s)"})

    # I5: pogingen begrensd
    for r in rows:
        if r["status"] in ACTIVE_STATES and r["status"] != "PROVIDER_WAIT" \
                and r["attempts"] > max_attempts:
            violations.append({
                "inv": "I5", "job": r["id"],
                "detail": f"{r['status']} met attempts={r['attempts']} > "
                          f"{max_attempts} (moet terminal of PROVIDER_WAIT zijn)"})

    # I6: PROVIDER_WAIT cooldown
    for r in rows:
        if r["status"] == "PROVIDER_WAIT" and \
                r["next_attempt_at"] > now + 3600 + 1:
            violations.append({
                "inv": "I6", "job": r["id"],
                "detail": f"PROVIDER_WAIT-cooldown {r['next_attempt_at'] - now:.0f}s "
                          f"> 1h cap"})

    # I7: redenen aanwezig
    for r in rows:
        if r["status"] in ACTIVE_STATES and r["attempts"] > 1 \
                and not (r["last_error"] or r["provider_block"]):
            violations.append({
                "inv": "I7", "job": r["id"],
                "detail": f"{r['status']} na {r['attempts']} pogingen zonder "
                          f"leesbare reden"})
    c.close()
    return {"ok": not violations, "violations": violations, "stats": stats,
            "checked_at": now}


def main() -> int:
    as_json = "--json" in sys.argv
    out = audit(max_attempts=int(
        __import__("os").environ.get("INGEST_JOB_MAX_ATTEMPTS", "8")))
    if as_json:
        print(json.dumps(out, indent=1))
    else:
        print(f"jobs: {out['stats'].get('total_jobs')} "
              f"{out['stats'].get('by_status', {})}")
        if out["violations"]:
            for v in out["violations"][:20]:
                print(f"VIOLATIE {v['inv']} {v['job'][:8]}: {v['detail']}")
            print(f"totaal: {len(out['violations'])} violaties")
        else:
            print("alle invariants groen")
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
