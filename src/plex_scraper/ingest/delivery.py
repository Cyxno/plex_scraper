"""Delivery na READY (Phase 19): canonical `.ids` → stabiele symlink →
Plex-namespace-verificatie → Plex-scan.

Plex-container namespace is autoritatief: de leesprobes gebeuren in de
plex-container (docker-exec), niet in de resolver-container.

Symlink-naam = release-bestandsnaam van de actieve bron (Sonarr-parseerbaar).
Locatie = arr-seriepad (Sonarr-authority: /media/Lanterns → symlink-tree),
niet een gegokte pad-conventie.
"""
from __future__ import annotations

import os
import re
import time

from plex_scraper.common.domain import models as m

CANONICAL_ROOT = "/mnt/remote/nzbdav"
_UNSAFE = re.compile(r"[^A-Za-z0-9 ._\-()\[\]']+")


def parse_root_map(raw: str) -> dict[str, str]:
    """"/media=TV Shows,/media2=X" → {"/media": "TV Shows", ...}"""
    out: dict[str, str] = {}
    for part in (raw or "").split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def canonical_host_path(plex_path: str, canonical_root: str = CANONICAL_ROOT) -> str:
    if not plex_path.startswith(".ids/"):
        raise ValueError(f"non-canonical plex_path: {plex_path!r}")
    return os.path.join(canonical_root, plex_path)


def symlink_base(root: str, arr_path: str, arr_root_map: dict[str, str],
                 season: int | None) -> str:
    """arr-seriespath → symlink-map: /media/Lanterns + {/media: TV Shows} →
    <root>/TV Shows/Lanterns (Season N door de aanroeper)."""
    arr_path = (arr_path or "").rstrip("/")
    for arr_root, subtree in arr_root_map.items():
        arr_root = arr_root.rstrip("/")
        if arr_root and (arr_path == arr_root or
                         arr_path.startswith(arr_root + "/")):
            rel = arr_path[len(arr_root):].lstrip("/")
            base = os.path.join(root, subtree, rel) if rel else \
                os.path.join(root, subtree)
            if season is not None:
                return os.path.join(base, f"Season {season}")
            return base
    raise ValueError(f"arr_path {arr_path!r} valt buiten bekende arr-roots "
                     f"{list(arr_root_map)}")


def symlink_path(base: str, release_name: str) -> str:
    name = _UNSAFE.sub("_", release_name or "").strip() or "release"
    if not name.lower().endswith((".mkv", ".mp4", ".avi", ".m2ts", ".ts")):
        name += ".mkv"
    return os.path.join(base, name)


def atomic_symlink(link_path: str, target: str) -> None:
    """Atomische swap (zelfde mechaniek als repair/path_repair)."""
    os.makedirs(os.path.dirname(link_path), exist_ok=True)
    tmp = f"{link_path}.tmp-{os.getpid()}-{time.time_ns()}"
    os.symlink(target, tmp)
    os.replace(tmp, link_path)


def verify_canonical_source_ready(item: m.MediaItem, source: m.Source) -> tuple[bool, str]:
    """Resolver-side precondities vóór delivery (goedkoop, geen Plex-call):
    item READY, actieve bron, canonical plex_path, size bekend."""
    if item.status != m.ItemStatus.READY.value:
        return False, f"item status {item.status} != READY"
    if source is None or source.state != m.SourceState.ACTIVE.value:
        return False, "geen actieve bron"
    if not item.plex_path.startswith(".ids/"):
        return False, f"non-canonical plex_path {item.plex_path[:40]}"
    return True, "OK"


def release_file_name(source: m.Source) -> str:
    """Release-bestandsnaam voor de symlink: provider-file_name voorkeur,
    anders torrent-name + .mkv."""
    name = (source.file_name or "").strip()
    if name:
        return name.split("/")[-1]
    return (source.torrent_name or "release") + ".mkv"


def build_symlink_target(kind: str, arr_path: str, season: int | None,
                         plex_path: str, source: m.Source,
                         symlink_root: str,
                         arr_root_map: dict[str, str],
                         canonical_root: str = CANONICAL_ROOT) -> tuple[str, str]:
    """(link_path, target) voor de stabiele symlink.

    target = het canonical .ids-pad zoals Plex het ziet (via de rehydrate-
    FUSE-mount), dus canonical_root + plex_path. De link komt in de arr-
    seriepad-structuur onder de symlink-root.
    """
    base = symlink_base(symlink_root, arr_path or "", arr_root_map,
                        season if kind == "episode" else None)
    link = symlink_path(base, release_file_name(source))
    target = canonical_host_path(plex_path, canonical_root)
    return link, target

