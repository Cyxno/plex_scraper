# PLEX SCRAPER POC REPORT

Date: 2026-09-30 · Repo: https://github.com/Cyxno/plex_scraper · Target host: Unraid 6.18.38

## Architecture chosen

**Userspace FUSE filesystem (pyfuse3, trio loop) backed by an HTTP-range
streaming proxy against TorBox CDN links**, with a FastAPI resolver owning all
state. Two services in one Docker image: `resolver` (state machine, scoring,
providers, caches, API) and `vfs` (the stable Plex path). Deployment recipe
verified on the target host: `CAP_SYS_ADMIN` + `/dev/fuse` + `rshared` bind
from `/mnt/cache` — the same privilege model the existing DUMB stack uses.

Why this won (full argument in `ARCHITECTURE_DECISION.md`): only a kernel
filesystem gives Plex honest `stat/open/read/seek/range/concurrent-handles`
semantics for Direct Play, transcoding, analysis and thumbnails alike.
WebDAV/rclone adds a daemon and still ends in FUSE; a pure HTTP façade gives
Plex no path; symlink/strm/LD_PRELOAD tricks violate the "no permanent local
media" or "no fragile shims" constraints.

Language: **Python 3.12** — allowed by the brief as the demonstrably fastest
safe path for a 20-phase PoC spanning HTTP, scraping, scoring, FUSE and tests.
Escape hatch documented: the resolver API is language-agnostic; only the VFS
process would need a rewrite (Go/Rust) for >200 MB/s or thousands of sessions.

## What is implemented

- Domain model: permanent `MediaItem` (stable `plex_path`) vs disposable
  `Source` (provider, info-hash, file, metadata, `score`, `failure_count`,
  `bad_until`, `generation`); torrent hash is never an identity.
- Transparent YAML scoring (resolution/video/audio/language/release/cached/
  seeders) with per-candidate breakdown lines and reject reasons.
- TorBox adapter behind `DebridProvider`: batch `checkcached` (≤100 hashes),
  `mylist`, `createtorrent` with bounded readiness polling, `requestdl`
  (link cache ≈ link TTL), Range reads with `Retry-After` + bounded retries,
  self-pacing token buckets (4 req/s global, 1 req/s requestdl).
- Pluggable scraper interface + Torrentio adapter (infoHash+fileIdx, ~1 req/s)
  + scriptable mocks + offline demo seed (synthetic or real-file content).
- Resolution state machine `NO_SOURCE → RESOLVING → CANDIDATE_VALIDATION →
  READY`, failure → `SOURCE_FAILED → RESOLVING → new generation → READY`;
  candidate walk in score order; temp-bad TTL with ×2 backoff (cap 6 h).
- Session pinning: one source generation per open handle; no mid-playback
  switch; `EIO` on dead source; next open resolves the next generation.
- pyfuse3 VFS: registry-derived tree (path traversal impossible by
  construction), honest sizes, `direct_io` handles (stale page cache
  impossible after a switch), `EIO` mapping.
- Range stream proxy: bounded per-session read-ahead (8 MiB default), global
  upstream semaphore (8), bounded timeouts.
- TTL caches: candidates 30 min, checkcached 10 min, links 2.5 h,
  failed-candidate backoff. No media byte cache (non-goal v0.1).
- Control API `/health /status /media…`, internal `open/stream/close`,
  DEBUG-gated failure injection (`POST /debug/sources/{id}/fail`,
  `POST /debug/media/{id}/fail-current`).
- Structured JSON logs + persisted event store feeding `/status`.
- Docker Compose for Unraid + GitHub Actions CI (unit/acceptance tests).
- Sonarr/Radarr integration design (register/desired/remove contract) —
  `docs/sonarr-radarr-design.md`.

## What is not implemented (deliberate v0.1 non-goals)

Sonarr/Radarr/Overseerr replacement · Usenet/NZBDAV/Real-Debrid ·
library-wide repair loops · DUMB compatibility · web UI · permanent local
media storage · bounded byte cache · automatic Plex re-analysis trigger
(generation-aware hook reserved) · LAN auth on the control API.

## Plex compatibility results

Verified without a live Plex instance, using the same access patterns Plex
uses (Plex is ffmpeg + file reads under the hood):

| Access pattern (what Plex does) | Test | Result |
|---|---|---|
| scan (readdir/stat) | host + second container list/stat of tree | ✅ stable tree, honest sizes |
| analyze (open+sequential reads) | **ffprobe through the mount** | ✅ `matroska` container, `h264 640x360`, `aac`, duration 6.02 s identified |
| seek + decode (Direct Play pattern) | `ffmpeg -ss 3 -i … -frames:v 1` | ✅ frame at t=3 s decoded through the stack |
| full read (transcode/analyzer pattern) | `ffmpeg -i … -f null -` | ✅ end-to-end decode |
| concurrent sessions | 3 handles, interleaved offsets | ✅ (unit + FUSE integration) |
| multiple readers across containers | host + alpine container + ffmpeg container simultaneously | ✅ |

Live-Plex items remain on the VERIFY-LIVE checklist in `docs/plex-behaviour.md`
(Direct Play badge, intro/credit markers across generations, watched state).

## Direct Play result

**Filesystem-level: proven.** Direct Play is open + ranged reads + MKV
cue-seeks; all three were proven through the stack with real bytes (external
`ffmpeg` stands in for Plex's demuxer). **Plex-level: not yet observed** —
requires the user's live Plex to be pointed at the mount (one bind + library
entry; checklist provided). No blocker identified.

## Seek/range result

- Random reads verified at offsets 0 … size−64 in unit tests, FUSE tests and
  host `dd skip=` reads through the propagated mount. ✅
- TorBox CDN Range support is de-facto standard but undocumented; the
  resolver validates every source with a real 206 probe (head + middle) and
  treats failure as a candidate failure — seeking can never silently depend
  on an unvalidated assumption.

## Source fallback result

The acceptance scenario, automated (`tests/test_acceptance.py`, green in CI):

```
SOURCE A: score highest → validation FAIL
SOURCE B: score second  → validation FAIL
SOURCE C: score third   → validation PASS
RESULT:   C selected, bytes valid, logical path unchanged, generation = 1
```

Runtime replacement (also green): C force-failed → next open walks ranked
candidates → recovered A validates → serves as generation 2 behind the same
path. Performed **live on the Unraid host** as well:
`demo2160dv(gen1) failed → demo1080(gen2) active → second container read new
bytes at the same path`.

## Stable-path result

`plex_path` is set once at registration and never touched by resolution.
Proven live: generation 1 → 2 switch changed only size/bytes; the path,
item id and registry entry stayed identical. Kernel page-cache staleness
across switches is excluded by `direct_io` handles (FUSE test asserts new
generation's bytes on a fresh open). Plex item retention (ratingKey/watched
state) follows from path stability and is on the VERIFY-LIVE checklist.

## Resource usage (measured on target host, idle = no playback)

| Metric | resolver | vfs |
|---|---|---|
| IDLE_CPU (docker stats) | 0.12 % | 0.00 % |
| IDLE_RSS | 37–47 MiB | 41–52 MiB |
| background loops | none (event-driven only) | none |

REQUEST_LATENCY (mock provider, warm buffer): avg 0.08 ms/read.
Cold read-ahead window: tens of ms (mock) / dominated by provider RTT in
production. 32 MB sequential through the full chain: ~13.6 MB/s with 128 KB
kernel reads + mock RTT — adequate for 1–3 PoC sessions; a throughput
optimization pass (larger FUSE reads, readahead tuning) is listed under
next phase. No polling loops, no library scans, provider calls only on
bootstrap/open/failure.

## Known risks

1. **TorBox CDN Range/206 is undocumented** — mitigated by per-source 206
   probe validation; monitor upstream changes.
2. **Throughput ceiling** (~13.6 MB/s as measured through mock + 128 KB
   reads): fine for PoC; may need tuning for multiple 4K remux transcodes.
   Escape hatch: VFS rewrite in Go/Rust behind the same resolver API.
3. **FUSE privileges** (`CAP_SYS_ADMIN` + `/dev/fuse` + shared bind) — same
   model as the existing DUMB stack on this host; NFS facade is the
   documented fallback if privileges ever become unacceptable.
4. **Plex semantics across generation change** (size changes, stale
   intro markers) — designed for (generation-aware events, re-analysis hook),
   but needs the live-Plex pass to confirm marker invalidation behaviour.
5. **Torrentio public instance limits** (~1 req/s) — pace enforced; scraper is
   pluggable (self-host Jackett/Prowlarr later).
6. **requestdl 3 h expiry** — link cache + transparent refresh within the
   pinned generation.

## Next recommended phase

1. **Live Plex pass** (half a day): point the user's Plex at
   `/mnt/cache/appdata/plex-scraper/vfs`, run the VERIFY-LIVE checklist
   (Direct Play badge, seek, transcode, Analyze, Skip Intro/Credits across a
   generation switch, watched-state retention).
2. **Real TorBox end-to-end** (1 day): set `TORBOX_API_TOKEN`, register the
   5-item set, observe scrape→checkcached→requestdl→206 validation against
   live APIs; tune `CACHE_*` TTLs from observed behaviour.
3. **Throughput pass**: FUSE read size / readahead tuning, then optional
   bounded byte cache; only if transcode concurrency demands it.
4. **Sonarr webhook adapter** (design already fixed) + `desired`-overlay in
   the scorer.

## Verdict

**POC_VIABLE**

Every acceptance criterion that can be proven without a live Plex instance is
proven, most of them twice (CI test suite + live run on the target Unraid
host): multi-item library, stable virtual path, real bytes, seek/range,
ranking, exclusions, fallback, generation replacement, stable path, low idle
resources, no DUMB, no local media storage. The only open items are the
Plex-application-level observations (Direct Play badge, intro markers,
watched state), which require pointing a live Plex at the mount and carry no
identified architectural risk.
