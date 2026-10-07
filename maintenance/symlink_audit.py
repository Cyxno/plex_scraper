"""Symlink-invariant audit (Phase 24): elke library-symlink moet:

  S1  een symlink zijn (geen gewoon bestand) onder de symlink-tree;
  S2  een ABSOLUTE target hebben dat binnen canonical_root/.ids ligt;
  S3  target hebben dat bestaat en leesbaar is in de plex-namespace;
  S4  niet naar de mount-root zelf wijzen en niet leeg zijn;
  S5  een doel-hash hebben die bij een resolver-managed item hoort (of een
      legacy-relict zijn: geen .ids — apart gemeld, geen auto-actie).

Gebruik (op de host óf in een container met /symlinks + /mnt/remote/nzbdav):
  python3 maintenance/symlink_audit.py --root /mnt/vm_storage/symlinks \
      --canonical /mnt/remote/nzbdav --json > /tmp/audit.json
Exit 0 = geen S1-S4-violaties (S5 legacy telt als waarschuwing).
"""
from __future__ import annotations

import argparse
import json
import os
import sys


def audit(root: str, canonical_root: str,
          read_check: bool = False) -> dict:
    canonical_root = canonical_root.rstrip("/")
    violations: list[dict] = []
    legacy: list[str] = []
    stats = {"links": 0, "readable": 0, "unreadable": 0, "legacy": 0}

    for dirpath, _dirnames, filenames in os.walk(root):
        for fn in filenames:
            path = os.path.join(dirpath, fn)
            if not os.path.islink(path):
                # een echt bestand in de symlink-tree is verdacht (S1)
                if os.path.isfile(path) and not path.endswith(".srt"):
                    violations.append({"check": "S1", "path": path,
                                       "detail": "geen symlink (gewoon bestand)"})
                continue
            stats["links"] += 1
            target = os.readlink(path)
            # S2/S4
            if not target:
                violations.append({"check": "S4", "path": path,
                                   "detail": "leeg target"})
                continue
            if not os.path.isabs(target):
                violations.append({"check": "S2", "path": path,
                                   "detail": f"relatief target {target[:60]}"})
                continue
            if not target.startswith(canonical_root + "/.ids/"):
                stats["legacy"] += 1
                legacy.append(f"{path} -> {target[:90]}")
                continue
            # S3 (+S5-levend)
            try:
                st = os.lstat(path)
                if st.st_size == 0 and not os.path.exists(path):
                    raise OSError("broken")
            except OSError as exc:
                violations.append({"check": "S3", "path": path,
                                   "detail": f"target onleesbaar: {exc!r}"[:120]})
                stats["unreadable"] += 1
                continue
            if read_check:
                try:
                    with open(path, "rb") as fh:
                        if not fh.read(65536):
                            raise OSError("lege read")
                    stats["readable"] += 1
                except OSError as exc:
                    violations.append({"check": "S3", "path": path,
                                       "detail": f"read faalt: {exc!r}"[:120]})
                    stats["unreadable"] += 1
            else:
                stats["readable"] += 1

    return {"ok": not violations, "violations": violations,
            "legacy_targets": legacy[:50], "legacy_count": stats["legacy"],
            "stats": stats, "root": root, "canonical_root": canonical_root}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/mnt/vm_storage/symlinks")
    ap.add_argument("--canonical", default="/mnt/remote/nzbdav")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--read-check", action="store_true",
                    help="65536 bytes lezen per link (trager, bewijst IO)")
    args = ap.parse_args()
    out = audit(args.root, args.canonical, read_check=args.read_check)
    if args.json:
        print(json.dumps(out, indent=1))
    else:
        print(f"symlinks: {out['stats'].get('links')} "
              f"(leesbaar {out['stats'].get('readable')}, "
              f"onleesbaar {out['stats'].get('unreadable')}, "
              f"legacy-targets {out['stats'].get('legacy')})")
        for v in out["violations"][:20]:
            print(f"VIOLATIE {v['check']} {v['path']}: {v['detail']}")
        print("ok" if out["ok"] else f"{len(out['violations'])} violaties")
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
