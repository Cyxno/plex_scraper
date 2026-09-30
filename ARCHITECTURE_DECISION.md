# ARCHITECTURE DECISION — Plex-first dynamic media resolver (PoC v0.1)

Date: 2026-09-30 · Status: **accepted** · Scope: PoC, TorBox-only, Plex-first

## 0. Evidence gathered before deciding (FASE 0 recon)

All architecture claims below were verified on the target host (Unraid, kernel
6.18.38) before writing code:

| # | Assumption | Test | Result |
|---|-----------|------|--------|
| E1 | Linux FUSE is available | `/dev/fuse` present, `fusermount3` on host | ✅ |
| E2 | A FUSE filesystem can be mounted **inside a Docker container** | `alpine` + `rclone mount` with `CAP_SYS_ADMIN` + `--device /dev/fuse` | ✅ |
| E3 | The container mount is visible on the **host** | bind source `/mnt/cache/...` (host marks `/mnt/cache` as *shared*), container bind with `bind-propagation=rshared` → files appear on host path | ✅ |
| E4 | A **second container** ("Plex simulation") can read the same path | `cat` + `dd skip=5000000` random read through bind → correct bytes | ✅ |
| E5 | Existing precedent on this exact host | `DUMB` container runs decypharr FUSE mount (`fuse.decypharr`, `allow_other`, `max_read=1048576`) with the same privilege model (non-privileged, `CAP_SYS_ADMIN`, `/dev/fuse`) | ✅ |
| E6 | TorBox API supports the needed flow | live-verified docs + OpenAPI: `mylist`, `checkcached` (batch ≤100 hashes), `requestdl` (GET, 3 h validity, `redirect=true` permalinks), `createtorrent` 60/h uncached, 300 req/min global | ✅ (Range/206 support on CDN links de-facto standard — used by rclone/zurg; re-verified at runtime by our validator) |
| E7 | Public scraper with structured results | Torrentio live-verified: `stream/{movie,series}/tt…[:s:e].json` → `infoHash`, `fileIdx` (may be absent → pick largest video file), ~1 req/s pacing | ✅ |

E2–E5 are the fundamental Plex/VFS assumptions the brief asked to prove first.
They hold on this host, so implementation proceeds.

## 1. Chosen technique

**A userspace FUSE filesystem, mounted inside Docker, backed by an in-process
HTTP-range streaming proxy against TorBox CDN links.**

Two cooperating processes in one image (two compose services):

```
                 ┌──────────────────────────── Docker host (Unraid) ────────────────────────────┐
                 │                                                                              │
 Sonarr/Radarr   │  ┌─────────────────┐   HTTP (localhost)   ┌──────────────────────────────┐   │
      │          │  │ plex-scraper-vfs │ ───────────────────▶ │ plex-scraper-resolver        │   │
      ▼          │  │  pyfuse3 mount   │  metadata ops +      │  FastAPI: state machine,     │   │
 logical media   │  │  /mnt/plex-scraper│  byte-range GETs    │  scoring, providers, cache,  │───┼──▶ TorBox API
      │          │  └───────┬─────────┘                      │  sessions, debug endpoints   │───┼──▶ Torrentio
      ▼          │          │ FUSE (/dev/fuse,               └──────────────────────────────┘   │
 resolver ───────┼──────────┘  CAP_SYS_ADMIN, bind rshared)                                    │
      │          │             ▼                                                               │
 stable VFS ─────┼──▶ host: /mnt/cache/appdata/plex-scraper/vfs  ←── Plex container bind (r/o) │
      │          │                                                                             │
      ▼          │                                                                             │
    Plex         └─────────────────────────────────────────────────────────────────────────────┘
```

Runtime: **Python 3.12** — `pyfuse3` (FUSE), `FastAPI`/`uvicorn` (control API +
range streaming), `httpx` (async provider calls), `PyYAML` (preferences),
`sqlite3` (persistent state). Single container image, two services.

### Why FUSE

Plex requires a real filesystem path. Only a kernel-visible filesystem gives
Plex honest POSIX semantics for `stat()`, `open()`, `read()`, `seek`/random
access, byte ranges, and multiple concurrent read handles — for **Direct Play,
transcoding (ffmpeg child process), media analysis, thumbnail/BIF generation
and intro/credits detection alike**, because all of them ultimately do ordinary
file reads through the VFS. FUSE is the battle-tested precedent on exactly this
platform: rclone/plexdrive mounts, and DUMB's decypharr mount already run this
way on this server.

### Why the other options lost

| Option | Verdict | Reason |
|---|---|---|
| B. WebDAV/rclone-style mount | ❌ | Plex cannot consume WebDAV natively; you end up running rclone mount → FUSE anyway, plus an extra daemon and hop. Our internal range-stream API already plays that role with one less moving part. |
| C. HTTP-range proxy + filesystem façade without kernel FS | ❌ as primary | Without a kernel filesystem Plex has no path at all. The only façade variant that works is a userspace **NFS server** (decypharr-style `go-nfs`): viable future alternative if FUSE privileges are ever unacceptable, but it drags in NFS file-handle state, locking and re-export complexity — much more code for the same PoC value. Our HTTP range proxy *is* kept as the byte-transport layer under FUSE. |
| D. Symlink/bind façade, `.strm`, LD_PRELOAD shims, Samba | ❌ | Symlinks need materialized files (violates "no permanent local media"); Plex has no `.strm` support; shims are fragile; SMB adds a server and UNC-path quirks. |

### Why Python (recorded per FASE 1 instruction)

- The PoC spans many subsystems (HTTP API, scraping, parsing, scoring, FUSE,
  tests). Python is demonstrably the fastest *and safest* path to a correct
  vertical slice; the brief explicitly allows this trade.
- `pyfuse3` is a mature, maintained FUSE 3 binding with asyncio support;
  read throughput with 128 KB–1 MB requests is far beyond Plex playback needs
  (decypharr ships the same `max_read=1 MiB` on this host).
- Idle cost is near zero: both processes are event loops that block when idle
  (measured in `POC_REPORT.md`).
- Rust/Go stay the right call *if* a later phase needs >200 MB/s or thousands
  of concurrent sessions; the provider/resolver API is language-agnostic.

## 2. Core design decisions

1. **Logical media item ≠ source.** `media_items` are permanent, keyed by a
   stable `plex_path` under the mount. `sources` are disposable rows carrying
   provider, info-hash, file index, technical metadata, score + score reasons,
   `failure_count`, `bad_until`, and a monotonically increasing
   `generation` per item. Torrent hash is never the identity.
2. **Stable Plex path (acceptance criterion #9).** The VFS tree is rendered
   from the item registry; switching generation changes only the bytes behind
   the same inode path.
3. **Session pinning (FASE 7).** Each FUSE `open()` obtains a *handle* from the
   resolver; the handle pins one source generation. Reads on that handle are
   served from that source until release. No mid-file source switching (file
   size/offsets would corrupt the demuxer); a read failure surfaces as `EIO`,
   and the *next* `open()` resolves a fresh generation.
4. **Validation = real bytes.** A candidate is only READY after TorBox knows
   it (`checkcached` / `download_finished`) **and** a probe read (first +
   middle range, expecting HTTP 206) succeeds through `requestdl`. Scraped
   score is never proof of playability.
5. **Lazy, event-driven resolution.** Provider calls happen on: item
   registration (bootstrap), first `open()` with no source, and failure-driven
   re-resolution. No background polling loops, no library-wide scans.
6. **Failure injection as a first-class debug tool** (`POST /debug/...`,
   gated by `DEBUG=true`): the acceptance test forces source A dead and proves
   the same Plex path keeps serving from a new generation.

## 3. Main risks

| Risk | Impact | Mitigation in PoC |
|---|---|---|
| TorBox CDN Range/206 support is undocumented | seeking/Direct Play could break | validator does a real 206 probe per source; failure → candidate marked bad, next candidate tried |
| FUSE caps required (`CAP_SYS_ADMIN` + `/dev/fuse`) | deployment friction | documented, non-privileged alternative (host rclone/NFS) listed; same model as DUMB already in use on this host |
| `createtorrent` 60/h limit for uncached content | resolution of niche items could exhaust quota | batch `checkcached` (≤100 hashes/call) first; cached candidates ranked above uncached; add-at-most-one per resolution; bounded retries + `Retry-After` |
| Plex semantics across generation change (size changes) | stale media info, intro markers | documented behaviour + generation-aware re-analysis hook (`docs/plex-behaviour.md`); no assumption that markers survive |
| pyfuse3 performance ceiling | transcode of many concurrent streams | PoC target is 1–3 concurrent sessions; measurement in report; escape hatch: rewrite VFS in Go/Rust behind same resolver API |
| requestdl links expire (~3 h) | long sessions fail | link cache with TTL + transparent refresh on 403/410 (same generation, session not switched) |

## 4. What the PoC includes (v0.1)

- Domain model + SQLite persistence (items, sources, sessions, events)
- YAML-driven transparent scoring with per-candidate breakdown and rejects
- TorBox adapter (rate-limited, bounded retries, `Retry-After`, timeouts)
- Pluggable scraper interface + Torrentio adapter (+ mock implementations)
- Resolution state machine: `NO_SOURCE → RESOLVING → CANDIDATE_VALIDATION →
  READY`, failure → `SOURCE_FAILED → RESOLVING → new generation → READY`
- Handle/session pinning with concurrency isolation
- pyfuse3 VFS with stable paths, honest sizes, random-access reads, `EIO`
- Range streaming proxy with bounded read-ahead buffer and upstream semaphore
- Metadata caches with TTLs (candidates, links, failed candidates); **no**
  media byte cache
- Control/observability API: `/health`, `/status`, `/media`, `/media/{id}`,
  `/media/{id}/sources`, structured JSON logs, resource metrics
- Failure injection endpoints (DEBUG-gated) + acceptance test
  (A fail, B fail, C pass → C serves, path unchanged)
- Test set of 5 logical items covering the required scenarios
- Docker Compose for Unraid with the verified FUSE recipe

## 5. What is deliberately NOT in v0.1

- Sonarr/Radarr/Overseerr replacement (only the registration API contract is
  designed; see `docs/sonarr-radarr-design.md`)
- Usenet / NZBDAV / Real-Debrid / Decypharr / DUMB compatibility
- Whole-library health scans, repair loops, 24/7 scraping
- Persistent local media storage, bounded byte cache
- Full web UI (API + docs only)
- Automatic Plex re-analysis trigger implementation (interface documented,
  generation-aware design reserved)
- AI/LLM logic of any kind
