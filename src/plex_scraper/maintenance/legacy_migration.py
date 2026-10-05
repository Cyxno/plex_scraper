"""Legacy coverage migration: veilige, persistente, idempotente migratie van
Plex-items die nog buiten de canonical `.ids`-architectuur vallen.

State machine (PHASE 3): DISCOVERED → CLASSIFIED → REGISTERED → RESOLVING →
SOURCE_VERIFIED → READY_FOR_SWAP → SWAPPING → SWAPPED → PLEX_VERIFYING →
COMPLETED; fouten: FAILED_RETRYABLE / FAILED_FINAL / BLOCKED_*; rollback:
ROLLBACK_REQUIRED → ROLLED_BACK. Journal: JSONL in
/data/migrations/legacy-v2/ (persistent, NIET /tmp). Herstart-safe: journal
is append-only, run() slaat COMPLETED over en herleidt SWAPPED-achtige
tussenstanden via werkelijke symlink-inspectie (PHASE 39).
"""
from __future__ import annotations

import json
import os
import time

MIG_DIR = "/data/migrations/legacy-v2"
CANONICAL_PREFIX = "/mnt/remote/nzbdav/.ids/"
SYMLINK_ROOT = "/mnt/vm_storage/symlinks"
BATCH_SIZE = 5
PROVIDER_PAUSE_S = 5.0


# ---------------------------------------------------------------- PHASE 4/P0
def validate_swap_target(target: str, link_path: str,
                         read_probe=None) -> tuple[bool, str]:
    """Harde swap-preconditions (P0): de bekende empty/root-target-foutklasse
    wordt hier technisch onmogelijk gemaakt. read_probe(optioneel pad) doet
    een bounded head+seek leescheck; ontbreekt hij, dan wordt existence via
    os.path gecontroleerd."""
    if not target or not target.strip():
        return False, "EMPTY_TARGET"
    if not target.startswith("/"):
        return False, "RELATIVE_TARGET"
    if target == "/" or target.rstrip("/") == "/mnt/remote/nzbdav":
        return False, "MOUNT_ROOT_OR_ROOT"
    if not target.startswith(CANONICAL_PREFIX):
        return False, "NON_CANONICAL_TARGET"
    if not link_path.startswith(SYMLINK_ROOT + "/"):
        return False, "LINK_OUTSIDE_SYMLINK_ROOT"
    if not os.path.exists(target):
        return False, "TARGET_NOT_FOUND"
    if read_probe is not None:
        try:
            if not read_probe(target):
                return False, "TARGET_READ_FAILED"
        except OSError as exc:
            return False, f"TARGET_READ_FAILED:{repr(exc)[:60]}"
    return True, "OK"


def rollback_allowed(old_target: str, old_readable: bool) -> bool:
    """PHASE 7: rollback alleen zinvol als de oude link ooit werkte."""
    return bool(old_target) and old_readable


# ---------------------------------------------------------------- PHASE 9/14
def classify_link(readable: bool, managed: bool) -> str:
    if managed:
        return "LEGACY_RESOLVER_MANAGED"
    return ("LEGACY_WORKING_UNMANAGED" if readable
            else "LEGACY_DEAD_UNMANAGED")


def discovery_rank(confidence: str) -> int:
    """PHASE 14: alleen EXACT migreert automatisch."""
    return {"EXACT": 0, "HIGH": 1, "AMBIGUOUS": 2}.get(confidence, 3)


class Journal:
    """PHASE 1/27: append-only JSONL journal in persistent /data."""

    def __init__(self, mig_dir: str = MIG_DIR):
        self.dir = mig_dir
        os.makedirs(self.dir, exist_ok=True)
        os.makedirs(os.path.join(self.dir, "rollback"), exist_ok=True)

    def append(self, name: str, record: dict) -> None:
        record = {"ts": time.time(), **record}
        with open(os.path.join(self.dir, name), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")

    def load(self, name: str) -> list[dict]:
        path = os.path.join(self.dir, name)
        if not os.path.exists(path):
            return []
        out = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
        return out

    def latest_by(self, name: str, key: str) -> dict[str, dict]:
        latest: dict[str, dict] = {}
        for rec in self.load(name):
            if key in rec:
                latest[rec[key]] = rec
        return latest


def plan_queue(inventory: list[dict], done: dict[str, dict]) -> list[dict]:
    """PHASE 10/40: prioriteit dead-exact → dead-discoverable; skip COMPLETED;
    idempotent bij herhaald runnen."""
    queue = []
    for item in inventory:
        if item.get("readable"):
            continue                                   # working legacy: P3+
        if item["link"] in done and done[item["link"]].get("state") in (
                "COMPLETED", "BLOCKED_NO_SOURCE", "IDENTITY_BLOCKED"):
            continue
        if item.get("plex_mapping") != "EXACT":
            continue
        queue.append(item)
    return queue
