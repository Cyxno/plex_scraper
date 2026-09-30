# PoC test plan

## Scope

Prove the acceptance criteria with automated tests (mock providers/scrapers,
real FUSE where available) plus a live checklist for the real TorBox/Plex path.

## Test set (FASE 11)

| # | Logical item | Scenario | IMDb |
|---|---|---|---|
| 1 | Breaking Bad S01E01 | normal working episode, many candidates | tt0903747 |
| 2 | Game of Thrones S01E01 | multi-quality candidates (720/1080/2160) | tt0944947 |
| 3 | Fight Club (1999) | older movie, many releases | tt0133093 |
| 4 | The Matrix (1999) | backing marked failed at runtime (injection target) | tt0137523 |
| 5 | Dune: Part Two (2024) | HDR/DV/audio scoring | tt15239678 |

MobLand S02E01 may be added locally as item 6 (optional; nothing depends on
it). With mock providers, the test set is synthetic — fixture torrents — so
CI never touches the network.

## Layers

### 1. Unit tests (pure, CI)
- Scoring: resolution/video/audio/language/release/cached bonuses; exact
  breakdown lines; ordering.
- Exclusions: 3D/CAM/telesync/hardcoded subs → reject with reason.
- Size limit: `max_size_gb`.
- Ranking: score desc; `bad_until` skip; failure_count backoff.
- State machine: full transition matrix incl. `SOURCE_FAILED → RESOLVING →
  READY(gen+1)`; no-candidates → `NO_SOURCE`.
- Session pinning: two handles on one item pin independently; a failure on
  one session does not touch the other; release closes cleanly.
- Caches: candidate TTL expiry, link refresh on 403, bad_until expiry.

### 2. Integration tests (filesystem; run where `/dev/fuse` exists, skip otherwise)
- virtual file exists with correct size after registration (bootstrap)
- sequential read matches fixture bytes
- random reads (`pread` at offsets) match
- multiple concurrent read handles (parallel, mixed offsets)
- source fail → next candidate serves; logical path unchanged; size updates
- EIO on dead session; subsequent open gets new generation

### 3. PoC acceptance test (automated, the FASE 12 scenario)

```
SOURCE A: score 98 → validation FAIL
SOURCE B: score 95 → validation FAIL
SOURCE C: score 88 → validation PASS
RESULT:   selected C, bytes valid, logical path unchanged, generation = 3
```

### 4. Live checklist (requires TORBOX_API_TOKEN, optional MobLand item)
- register 5 items → bootstrap READY for all
- `ffprobe` through the mount reads real stream metadata
- `dd`/`vlc`/`mpv` playback with seeks through the mount
- force-fail current source → next playback resolves new generation
- Plex live run: the checklist in `docs/plex-behaviour.md`

## Resource measurement (FASE 14)

Measured after a full test run, reported in `POC_REPORT.md`:
`IDLE_RSS`, `IDLE_CPU` (10 min idle), `REQUEST_CPU`, `REQUEST_LATENCY`
(first byte on warm session, cold resolution latency).

## Gates

- CI (GitHub Actions): unit + acceptance tests, no FUSE/network needed.
- Local Unraid run: integration suite inside the container (has `/dev/fuse`).
- A gate failure on the acceptance test = PoC failed; no feature work past it.
