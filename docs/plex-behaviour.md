# Plex behaviour with the resolver VFS — what we rely on, what we must re-verify

This document records which Plex behaviours the architecture depends on, what
is guaranteed by design, and what must be validated against a live Plex
instance (the PoC automates what it can with filesystem-level simulations;
Plex-only semantics are marked **VERIFY-LIVE**).

## What Plex does against a mounted path

Plex treats the mount like any local folder. Internally, every feature is
ordinary file I/O:

| Feature | Access pattern | Our guarantee |
|---|---|---|
| Library scan / add folder | `readdir`, `stat`, small probe `open/read` | Stable tree from item registry; no provider I/O on scan |
| Media analysis (`Analyze`) | sequential `read()` chunks, some seeks | Full-byte semantics; works on READY items |
| Thumbnail / BIF generation | heavier sequential + seek reads | Same |
| Intro / credits detection (Plex Pass) | long sequential stream reads | Same |
| Direct Play | `open` + range reads (MKV cue-tree seeks) | Pinned source, ranged GETs |
| Transcode | spawn `ffmpeg` on the path (own handle) | Independent session pin; unaffected by other sessions |
| Watched state / metadata | keyed by library + file path | Path never changes → item identity survives |

## Generation change: what changes for Plex, what doesn't

When the resolver replaces the backing source (generation N → N+1):

**Unchanged by design**
- The path (`/media/TV/.../MobLand - S02E01.mkv`) is byte-identical.
- Therefore Plex keeps the same library item, `ratingKey`, watched state,
  user metadata. No remove/re-add, no library dance.

**Changed**
- File size can differ between releases. Plex notices on the next scan
  (`MediaPart.size` updates). This is normal Plex behaviour for edited files
  and does not orphan the item.
- Streams inside the MKV can differ (codec, track order, HDR flavour).
  `Media`/`MediaPart`/`MediaStream` rows are re-derived only when Plex
  re-analyzes.

**Assumptions that must NOT be made**
- Intro/credit timing markers are encode-relative. A marker computed on
  generation N is *invalid* on generation N+1. The architecture is
  generation-aware: `sources.generation` is stored with each event, and the
  design reserves a re-analysis hook per generation switch.
  **VERIFY-LIVE:** confirm markers survive scans until re-analysis, and that
  `PUT /library/sections/{id}/analyze` (or per-item analyze via Plex API)
  re-derives them.
- Playable byte-identity is impossible across encodes; a resumed transcode
  offset from generation N is invalid on N+1. Within one session this cannot
  happen (pinning guarantees one generation per handle).

## Chicken-and-egg: bootstrap

Plex refuses to build a library item without real readable media. Bootstrap
flow (implemented):

1. Item registered → resolver resolves a bootstrap source immediately
   (candidates → score → validate → READY).
2. VFS exposes the path with the real size; Plex scans and analyzes real
   bytes.
3. The bootstrap source is *not* special afterwards: it is generation 1 and
   may be replaced like any other source.

## Deliberate v0.1 simplifications

- Re-analysis after generation switch is not automated end-to-end; the event
  log records the switch so a later phase can trigger Plex re-analysis
  exactly for affected items.
- Multi-edition libraries (same episode in multiple qualities as separate
  Plex versions) are out of scope: the resolver exposes exactly one file per
  logical item.

## VERIFY-LIVE checklist (needs a running Plex)

1. Direct Play an MKV through the mount; confirm "Direct Play" in dashboard.
2. Seek across the file (middle, end); confirm no stall/error.
3. Force transcode (lower bandwidth limit); confirm ffmpeg reads through VFS.
4. Run Analyze + Generate intros on an item; then `POST /debug/.../fail`,
   play again, confirm new generation serves and item/ratingKey unchanged.
5. Confirm watched state survives a generation switch + scan.
6. Confirm Skip Intro / Skip Credits appear and behave before/after a
   generation switch (expect: markers stale until re-analyzed).
