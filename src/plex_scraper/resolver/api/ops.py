"""Operationele view-models voor de cockpit-UI.

Aggregeert store/events naar kant-en-klare endpoints zodat de frontend
geen rauwe event-historie hoeft te reconstrueren (DEEL R):
  GET /api/dashboard        overall health + KPI's + nu-actief + wins
  GET /api/activity         gegroepeerde operatiekaarten (filters, paging)
  GET /api/issues           semantische probleemkaarten (geen kale NO_SOURCE)
  GET /api/jobs             achtergrondjobs met status/progress/history
  GET /api/media/{id}/trace volledige candidate-trace + timeline
  GET /api/providers/health provider-statistiek + budget-gebruik
Lees-only, begrensde queries (geen volledige event-scan per load).
"""
from __future__ import annotations

import json
import os
import time

from fastapi import APIRouter
from fastapi import HTTPException as HTTPError
from pydantic import BaseModel

from plex_scraper.common.timefmt import age_seconds

# Testbaar: tests monkeypatchen dit pad i.p.v. het.filesysteem te forcen.
COVERAGE_PATH = os.environ.get("PLEX_SCRAPER_COVERAGE_PATH",
                               "/data/coverage/latest.json")


class MagnetBody(BaseModel):
    magnet: str
    reason: str | None = None


class MarkBadBody(BaseModel):
    reason: str

HUMAN_REJECTS = {
    "identity_wrong_show": ("wrong series", "Identity"),
    "identity_pack_missing_episode": ("episode not in pack", "Identity"),
    "identity_wrong_movie": ("wrong movie", "Identity"),
    "identity_wrong_year": ("wrong year", "Identity"),
    "pre_gate_size": ("too small (pre-filtered)", "Size"),
    "budget_exhausted": ("not tried (add budget used)", "Budget"),
    "file_too_small": ("file too small / mislabeled", "Validation"),
    "torrent_no_files": ("no files", "Validation"),
    "provider_400": ("TorBox 400 (temporary)", "Provider"),
    "provider_429": ("rate limited", "Provider"),
    "provider_5xx": ("TorBox server error", "Provider"),
    "torrent_not_ready": ("not ready yet (retry follows)", "Transient"),
    "first_byte_empty": ("no data at first byte", "Validation"),
    "range_failed": ("mid-file read probe failed", "Validation"),
    "backend_unavailable": ("backend unreachable", "Backend"),
    "provider_add_failed": ("provider add failed", "Provider"),
    "unknown_probe_failure": ("unknown validation error", "Other"),
    "bad_ttl": ("skipped (temporary bad)", "Retry"),
    "no_candidates": ("no candidates found", "Provider"),
}

OPERATION_KINDS = {
    "resolutions": ("resolution_started", "resolution_succeeded",
                    "resolution_failed", "resolution_reject_summary",
                    "resolution_skip_uncached", "search_identity_incomplete",
                    "candidate_identity_rejected", "candidate_failed",
                    "candidate_pre_gate_rejected", "candidate_failed",
                    "candidate_file_choice_rejected"),
    "repairs": ("sweep_repair_needed", "repair_kept_current",
                "path_repair", "stale_state_reconciled",
                "plex_restart_recovery"),
    "failovers": ("jit_rescue_switch", "source_failed", "failover",
                  "startup_failed", "candidate_grandfathered_retry"),
    "sweeper": ("sweep_strike", "sweep_paused_playback"),
    "playback": ("session_opened", "session_closed", "stall_detected",
                 "delivery_degraded", "startup_first_byte"),
    "provider": ("torbox_retry", "torbox_createtorrent_retry",
                 "upstream_http_4xx"),
    "metadata": ("item_registered", "show_id_backfilled", "identity_corrected",
                 "library_audit"),
    "errors": ("resolution_crashed", "upstream_read_failed",
               "physical_check_error", "operator_action_failed",
               "startup_failed"),
}


def create_ops_routes(app, resolver) -> APIRouter:
    router = APIRouter(prefix="/api", tags=["ops"])
    store = resolver.store
    _lkg: dict[str, tuple[float, dict]] = {}          # last-known-good cache

    async def _guarded(key: str, producer):
        """B3/B5: aggregate-endpoints zijn read-mostly failure-tolerant.
        SQLite-busy of een andere transiente fout -> laatste bekende goede
        payload met stale:true + leeftijd; nooit een lege/false-zero payload
        (C2). Eerste keer zonder cache: nette HTTP-fout."""
        try:
            data = await producer()
        except Exception as exc:                       # noqa: BLE001
            cached = _lkg.get(key)
            if cached is None:
                raise
            ts, prev = cached
            return {**prev, "generated_at": ts, "stale": True,
                    "stale_age_s": round(time.time() - ts),
                    "degraded": f"last known data ({str(exc)[:80]})"}
        _lkg[key] = (time.time(), data)
        return {**data, "generated_at": time.time(), "stale": False}

    def _since(hours: float) -> float:
        return time.time() - hours * 3600.0

    async def _events_since(ts: float, limit: int = 800) -> list[dict]:
        def fn(c):
            rows = c.execute(
                "SELECT * FROM events WHERE ts >= ? ORDER BY id DESC LIMIT ?",
                (ts, limit)).fetchall()
            return [{"id": r["id"], "ts": r["ts"], "media_item_id": r["media_item_id"],
                     "kind": r["kind"], **json.loads(r["payload"] or "{}")}
                    for r in rows]
        return await store.run(fn)

    async def _item_map(ids: set[str]) -> dict[str, dict]:
        out = {}
        for iid in ids:
            it = await store.get_item(iid) if iid else None
            if it is None:
                continue
            label = it.series and f"{it.series} S{it.season:02d}E{it.episode:02d}" \
                or it.title
            out[iid] = {"id": iid, "label": label, "kind": it.kind,
                        "status": it.status, "title": it.title,
                        "series": it.series, "season": it.season,
                        "episode": it.episode, "generation": it.generation}
        return out

    # ------------------------------------------------------------ dashboard
    async def _physical() -> dict:
        ph = await store.get_physical_health()
        return ph or {"status": "UNKNOWN", "checked_at": None}

    @router.get("/physical")
    async def physical():
        """Consumer-path health voor Netdata/metrics-scraping (DEEL G)."""
        ph = await _physical()
        return {"physical_library_health":
                1 if ph.get("status") == "HEALTHY"
                else 0 if ph.get("status") in ("FAILED", "SUSPECT") else -1,
                "status": ph.get("status"),
                "last_checked": ph.get("checked_at"),
                "plex_namespace_read_ok": ph.get("status") == "HEALTHY"}

    async def _dashboard_data():
        items = await store.list_items()
        counts: dict[str, int] = {}
        for i in items:
            counts[i.status] = counts.get(i.status, 0) + 1
        no_source = [i for i in items if i.status == "NO_SOURCE"]
        identity_incomplete = sum(
            1 for i in items if i.kind == "episode" and not i.show_imdb_id)

        # actieve playback + resolving
        cutoff = time.time() - 90.0
        streams = []
        for handle, ctx in getattr(resolver, "sessions", {}).items():
            if (ctx.last_read_at or 0) >= cutoff:
                streams.append({"item_id": ctx.session.media_item_id,
                                "read_count": ctx.session.read_count,
                                "delivery": (ctx.monitor.snapshot()
                                             if getattr(ctx, "monitor", None) else None)})
        resolving = [i for i in items if i.status in ("RESOLVING",
                                                      "CANDIDATE_VALIDATION")]

        # recent automatische herstels: NO_SOURCE -> READY blijkbaar uit events
        evs = await _events_since(_since(24), limit=1500)
        wins = []
        seen_items: set[str] = set()
        for e in evs:                                  # nieuwste eerst
            if e["kind"] != "resolution_succeeded" or e["media_item_id"] in seen_items:
                continue
            seen_items.add(e["media_item_id"])
            it = await store.get_item(e["media_item_id"] or "")
            if it is None:
                continue
            label = it.series and f"{it.series} S{it.season:02d}E{it.episode:02d}" or it.title
            wins.append({"item_id": it.id, "label": label,
                         "outcome": "READY",
                         "file": (e.get("selected_candidate") or {}).get("file"),
                         "ago_s": round(time.time() - e["ts"])})
            if len(wins) >= 5:
                break

        # provider-samenvatting uit 24h events
        provider = {"retries": 0, "transient": 0, "candidate_failures": 0}
        for e in evs:
            if e["kind"] in ("torbox_retry", "torbox_createtorrent_retry"):
                provider["retries"] += 1
            elif e["kind"] == "candidate_failed":
                provider["candidate_failures"] += 1
                if e.get("ttl_class", "").startswith("TRANSIENT"):
                    provider["transient"] += 1

        # probleem-samenvatting
        issues = []
        for i in no_source:
            last = await _last_reject_summary(i.id)
            issues.append({"item_id": i.id, "rejects": (last or {}).get("rejects")})
        health = "HEALTHY"
        if no_source:
            health = "ATTENTION"
        if identity_incomplete:
            health = "DEGRADED" if health == "HEALTHY" else health
        # F1: overall-health-precedentie — fysieke keten telt mee
        try:
            ph = await _physical()
        except Exception:
            ph = {"status": "UNKNOWN"}
        if ph.get("status") in ("FAILED",):
            health = "ERROR"
        elif ph.get("status") == "SUSPECT":
            health = "DEGRADED" if health == "HEALTHY" else health
        # P16-P19: coverage-semantiek — geen volgroen bij dead unmanaged media
        coverage = None
        try:
            with open(COVERAGE_PATH, encoding="utf-8") as fh:
                coverage = json.load(fh)
            # stale-marker: dit is een puntmoment-snapshot van een
            # bestandsscan, geen live teller (audit 2026-10-07).
            # Leeftijd via de centrale parser: verdraagt epoch-s én -ms én
            # ISO; ongeldig → age_s=None (UI toont 'timestamp unavailable',
            # nooit een absurd getal als "20733d").
            age = age_seconds(coverage.get("timestamp"))
            coverage["age_s"] = round(age) if age is not None else None
            coverage["age_valid"] = age is not None
        except Exception:
            coverage = None
        # Audit 2026-10-07: coverage/latest.json is een puntmoment-snapshot
        # waarvan de legacy_dead-telling grotendeels als meetfout (te korte
        # FUSE-read-timeouts) is achterhaald. De snapshot mag de live
        # health dus niet meer degraderen; hij gaat uitsluitend als
        # gelabelde snapshot mee in de payload (coverage-kaart cockpit).
        # Phase 36-42: ingest als first-class block + health-semantiek
        ingest_block = None
        bridge = getattr(app.state, "ingest", None)
        if bridge is not None:
            try:
                ingest_block = await bridge.status()
            except Exception:                       # noqa: BLE001
                ingest_block = None
            if ingest_block:
                # audit 2026-10-07: gebruik een eigen naam — de ingest-
                # provider-state (blocked/retry) mag de resolver-derived
                # 24h provider-stats (retries/transient/candidate_failures)
                # niet overschrijven, anders toont de cockpit er permanent
                # "–" voor.
                iprovider = ingest_block.get("provider") or {}
                if iprovider.get("blocked") and \
                        (iprovider.get("retry_in_s") or 0) > 3600:
                    health = "DEGRADED"
                elif iprovider.get("blocked") and health == "HEALTHY":
                    health = "ATTENTION"            # gewone, begrensde pauze
        jobs = await store.job_runs(limit=3)
        running = [j for j in jobs if j["status"] == "RUNNING"]

        # KPI-semantiek (audit 2026-10-07): Pending is het complement en
        # wordt server-side berekend zodat Ready + Issues + Pending altijd
        # exact = Total (rekenkundig sluitend, in één denominator).
        pending = max(0, len(items) - counts.get("READY", 0) - len(no_source))

        return {
            "health": health,
            "coverage": coverage,
            "ingest": ingest_block,
            "physical": {"status": ph.get("status"),
                         "checked_at": ph.get("checked_at"),
                         "latency_s": ph.get("latency_s"),
                         "raw": ph.get("raw"),
                         "last_healthy_at": ph.get("last_healthy_at"),
                         "detail": (ph.get("raw") or "")[:120]},
            "library": {"total": len(items), "ready": counts.get("READY", 0),
                        "no_source": len(no_source),
                        "resolving": len(resolving),
                        "provider_wait": counts.get("PROVIDER_WAIT", 0),
                        "identity_incomplete": identity_incomplete,
                        "pending": pending},
            "playback": {"active": len(streams), "streams": streams},
            "now": {"resolving": [{"item_id": i.id, "label":
                                   (i.series and
                                    f"{i.series} S{i.season:02d}E{i.episode:02d}")
                                   or i.title} for i in resolving],
                    "jobs_running": [{"id": j["id"], "job_type": j["job_type"],
                                      "current_item": j.get("current_item"),
                                      "progress_total": j.get("progress_total"),
                                      "progress_current": j.get("progress_current"),
                                      "processed": j.get("processed"),
                                      "recovered": j.get("recovered"),
                                      "started_at": j.get("started_at")}
                                     for j in running]},
            "recent_wins": wins,
            "provider": provider,
            "issues": {"no_source": len(no_source),
                       "with_rejects": sum(1 for x in issues if x["rejects"])},
            "sweeper": {"last_runs": [{"id": j["id"], "status": j["status"],
                                       "started_at": j["started_at"],
                                       "finished_at": j.get("finished_at"),
                                       "processed": j.get("processed"),
                                       "recovered": j.get("recovered")}
                                      for j in jobs if j["job_type"] == "health_sweeper"]},
        }

    @router.get("/dashboard")
    async def dashboard():
        return await _guarded("dashboard", _dashboard_data)

    async def _last_reject_summary(item_id: str) -> dict | None:
        def fn(c):
            r = c.execute("SELECT payload, ts FROM events WHERE media_item_id=? "
                          "AND kind='resolution_reject_summary' "
                          "ORDER BY id DESC LIMIT 1", (item_id,)).fetchone()
            if not r:
                return None
            d = json.loads(r["payload"] or "{}")
            d["ts"] = r["ts"]
            return d
        return await store.run(fn)

    # ------------------------------------------------------------- activity
    @router.get("/activity")
    async def activity(hours: float = 24.0, filter: str = "all",
                       limit: int = 40, offset: int = 0):
        evs = await _events_since(_since(min(hours, 168.0)), limit=2000)
        kinds = None if filter == "all" else OPERATION_KINDS.get(filter)
        if kinds:
            evs = [e for e in evs if e["kind"] in kinds]

        # groepeer resolve-events per (item, resolve-run)
        groups: list[dict] = []
        open_runs: dict[str, dict] = {}
        chronological = list(reversed(evs))            # oud -> nieuw
        for e in chronological:
            if e["kind"] == "resolution_started":
                key = e["media_item_id"] or "?"
                run = {"item_id": e["media_item_id"], "op": "RESOLVE",
                       "started": e["ts"], "events": [e], "outcome": "RUNNING"}
                open_runs[key] = run
                groups.append(run)
                continue
            if e["media_item_id"] and e["media_item_id"] in open_runs \
                    and e["kind"] not in ("session_opened", "session_closed"):
                run = open_runs[e["media_item_id"]]
                run["events"].append(e)
                if e["kind"] == "resolution_succeeded":
                    run["outcome"] = "SUCCESS"
                    open_runs.pop(e["media_item_id"], None)   # run is af
                elif e["kind"] == "resolution_failed":
                    run["outcome"] = "FAILED"
                    open_runs.pop(e["media_item_id"], None)
                continue
            groups.append({"item_id": e["media_item_id"], "op": e["kind"],
                           "started": e["ts"], "events": [e],
                           "outcome": e["kind"]})

        cards = []
        for g in groups:
            summary = _rejects_of(g["events"])
            item = (await _item_map({g["item_id"]})) if g["item_id"] else {}
            cards.append({
                "ts": g["started"], "op": g["op"],
                "item": item.get(g["item_id"]) if item else None,
                "outcome": g["outcome"],
                "duration_s": round(max((e["ts"] for e in g["events"]),
                                        default=g["started"]) - g["started"], 2),
                "candidate_count": summary.get("candidate_count"),
                "rejects": summary.get("rejects"),
                "event_count": len(g["events"]),
                "detail": _human_summary(g),
            })
        cards.sort(key=lambda c: -c["ts"])
        return {"total": len(cards), "cards": cards[offset:offset + limit]}

    def _rejects_of(events: list[dict]) -> dict:
        for e in reversed(events):
            if e["kind"] == "resolution_reject_summary":
                return {"candidate_count": e.get("candidate_count"),
                        "rejects": e.get("rejects")}
        return {}

    SESSION_HUMAN = {
        "session_opened": "Playback session opened",
        "session_closed": "Playback session closed",
        "startup_first_byte": "First byte received",
        "startup_failed": "Startup failed — rescuing",
        "source_failed": "Source failed",
        "item_registered": "Item registered",
        "sweep_repair_needed": "Repair applied by health sweep",
        "repair_kept_current": "Repair verified current source still healthy",
        "stale_state_reconciled": "Stale state reconciled",
        "plex_restart_recovery": "Plex restarted to restore consumer mount",
        "operator_action_started": "Operator action started",
        "operator_action_completed": "Operator action completed",
        "operator_action_failed": "Operator action failed",
        "library_audit": "Library audit",
        "torbox_retry": "Provider request retried",
        "torbox_createtorrent_retry": "Provider add retried",
        "physical_check_error": "Physical check error",
    }

    def _human_summary(g: dict) -> str:
        # B2/B4: nooit candidate-claims voor events die geen resolve zijn.
        kinds = [e["kind"] for e in g["events"]]
        if "resolution_succeeded" in kinds:
            sel = next((e for e in reversed(g["events"])
                        if e["kind"] == "resolution_succeeded"), None)
            f = (sel.get("selected_candidate") or {}).get("file") if sel else None
            return f"Source activated: {f}" if f else "Source activated"
        if "resolution_failed" in kinds or "resolution_reject_summary" in kinds:
            rej = _rejects_of(g["events"]).get("rejects") or {}
            if rej:
                parts = [f"{v}x {HUMAN_REJECTS.get(k, (k, ''))[0]}"
                         for k, v in rej.items() if v]
                return "No usable candidate — " + "; ".join(parts[:4])
            return "No usable candidate found"
        first = g["events"][0]["kind"]
        if first in SESSION_HUMAN:
            base = SESSION_HUMAN[first]
            if first == "session_closed":
                dur = g["events"][0].get("duration_s") or (
                    g["events"][0].get("closed_at") and None)
                reads = g["events"][0].get("read_count")
                extra = f" ({reads} reads)" if reads else ""
                return base + extra
            return base
        return first.replace("_", " ")

    # --------------------------------------------------------------- issues
    async def _issues_data():
        items = await store.list_items()
        out = []
        for i in items:
            if i.status != "NO_SOURCE":
                continue
            summ = await _last_reject_summary(i.id)
            rejects = (summ or {}).get("rejects") or {}
            # retry-state: slechtste bad_until van de bronnen
            srcs = await store.list_sources(i.id)
            next_retry = max((s.bad_until for s in srcs), default=None)
            has_transient = any(t for t in rejects if "provider" in t
                                or "not_ready" in t or "budget" in t)
            if not rejects and not srcs:
                classification = "PROVIDER_NO_MATCH"
                human = "Provider returned no candidates at all"
            elif rejects.get("no_candidates"):
                classification = "PROVIDER_NO_MATCH"
                human = "0 candidates despite complete identity"
            elif has_transient or next_retry and next_retry > time.time():
                classification = "NO_USABLE_CANDIDATE"
                human = "Candidates existed but none were usable — retry scheduled"
            else:
                classification = "NO_USABLE_CANDIDATE"
                human = "Candidates rejected on identity/validation"
            out.append({
                "item_id": i.id,
                "label": (i.series and f"{i.series} S{i.season:02d}E{i.episode:02d}")
                or i.title,
                "kind": i.kind,
                "classification": classification,
                "human": human,
                "candidate_count": (summ or {}).get("candidate_count"),
                "rejects": [{"key": k, "n": v,
                             "human": HUMAN_REJECTS.get(k, (k, ""))[0],
                             "group": HUMAN_REJECTS.get(k, ("", ""))[1]}
                            for k, v in sorted(rejects.items(), key=lambda kv: -kv[1])
                            if v],
                "last_attempt_ago_s": round(time.time() - summ["ts"]) if summ else None,
                "next_retry_in_s": round(next_retry - time.time())
                if next_retry and next_retry > time.time() else None,
            })
        out.sort(key=lambda x: (x["classification"], -(x["candidate_count"] or 0)))
        return {"total": len(out), "issues": out}

    async def _issues_data_with_conflicts():
        base = await _issues_data()
        out = []
        for con in await store.list_identity_conflicts():
            it = await store.get_item(con["item_id"])
            if it is None:
                continue
            out.append({
                "item_id": it.id,
                "label": (it.series and f"{it.series} S{it.season:02d}E{it.episode:02d}")
                         or it.title,
                "kind": it.kind,
                "classification": "IDENTITY_CONFLICT",
                "human": "Identity conflict: stored external ID does not match the "
                         "Plex-authoritative identity. Search is blocked.",
                "candidate_count": None,
                "rejects": [],
                "last_attempt_ago_s": None,
                "next_retry_in_s": None,
            })
        return {"total": base["total"] + len(out), "issues": out + base["issues"]}

    @router.get("/issues")
    async def issues():
        return await _guarded("issues", _issues_data_with_conflicts)

    # ----------------------------------------------------------------- jobs
    async def _jobs_data(limit: int = 30):
        runs = await store.job_runs(limit=limit)
        latest: dict[str, dict] = {}
        for r in runs:
            latest.setdefault(r["job_type"], r)
        return {"jobs": [{"job_type": jt, "state": r["status"],
                          "last_run": r["started_at"],
                          "duration_s": round((r.get("finished_at") or time.time())
                                              - r["started_at"], 1),
                          "processed": r.get("processed"),
                          "changed": r.get("changed"),
                          "recovered": r.get("recovered"),
                          "current_item": r.get("current_item"),
                          "progress_total": r.get("progress_total"),
                          "progress_current": r.get("progress_current"),
                          "run_id": r["id"]}
                         for jt, r in latest.items()],
                "history": runs}

    @router.get("/jobs")
    async def jobs(limit: int = 30):
        return await _guarded("jobs", lambda: _jobs_data(limit))

    @router.get("/jobs/{run_id}")
    async def job_detail(run_id: int):
        for r in await store.job_runs(limit=200):
            if r["id"] == run_id:
                return r
        raise HTTPError(404, "run not found")

    # ---------------------------------------------------------------- trace
    @router.get("/media/{item_id}/trace")
    async def trace(item_id: str):
        it = await store.get_item(item_id)
        if it is None:
            raise HTTPError(404, "item not found")

        def fn(c):
            rows = c.execute("SELECT * FROM events WHERE media_item_id=? "
                             "ORDER BY id DESC LIMIT 400", (item_id,)).fetchall()
            return [{"ts": r["ts"], "kind": r["kind"], "generation": r["generation"],
                     **json.loads(r["payload"] or "{}")} for r in rows]
        evs = await store.run(fn)
        evs.reverse()

        # laatste resolve-run bepalen
        start_idx = 0
        for idx, e in enumerate(evs):
            if e["kind"] == "resolution_started":
                start_idx = idx
        run = evs[start_idx:]

        candidates = []
        for e in run:
            if e["kind"] in ("candidate_identity_rejected", "candidate_failed",
                             "candidate_pre_gate_rejected",
                             "candidate_file_choice_rejected"):
                candidates.append({
                    "hash": e.get("hash"), "name": e.get("name"),
                    "result": e.get("subreason") or e.get("reject_kind")
                    or ("pre_gate_size" if e["kind"] == "candidate_pre_gate_rejected"
                        else "file_choice_replaced"),
                    "human": HUMAN_REJECTS.get(
                        e.get("subreason") or e.get("reject_kind"),
                        ("afgewezen", ""))[0],
                    "error": (e.get("error") or "")[:120] or None,
                    "ttl_class": e.get("ttl_class"),
                })
            elif e["kind"] == "resolution_skip_uncached":
                candidates.append({"hash": e.get("hash"), "name": e.get("name"),
                                   "result": "budget_exhausted",
                                   "human": HUMAN_REJECTS["budget_exhausted"][0]})
            elif e["kind"] == "resolution_succeeded":
                sel = e.get("selected_candidate") or {}
                candidates.append({"hash": sel.get("hash"), "name": sel.get("file"),
                                   "result": "SELECTED", "human": "gekozen"})

        summ = next((e for e in reversed(run)
                     if e["kind"] == "resolution_reject_summary"), None)
        srcs = await store.list_sources(item_id)
        active = next((s for s in srcs if s.state == "active"), None)
        return {
            "item": {"id": it.id, "kind": it.kind, "title": it.title,
                     "series": it.series, "season": it.season,
                     "episode": it.episode, "status": it.status,
                     "generation": it.generation,
                     "show_imdb_id": it.show_imdb_id,
                     "show_tmdb_id": it.show_tmdb_id,
                     "show_tvdb_id": it.show_tvdb_id,
                     "imdb_id": it.imdb_id, "tmdb_id": it.tmdb_id,
                     "tvdb_id": it.tvdb_id,
                     "plex_path": it.plex_path},
            "last_resolve": {
                "candidate_count": (summ or {}).get("candidate_count"),
                "provider_adds": (summ or {}).get("provider_adds"),
                "rejects": (summ or {}).get("rejects"),
                "candidates": candidates,
            },
            "sources": [{"id": s.id[:8], "state": s.state, "provider": s.provider,
                         "file_name": s.file_name, "size": s.size,
                         "info_hash": s.info_hash[:12],
                         "failure_count": s.failure_count,
                         "bad_until": s.bad_until,
                         "generation": s.generation} for s in srcs],
            "active_source": ({"file_name": active.file_name,
                               "size": active.size, "generation": active.generation,
                               "info_hash": active.info_hash[:12]}
                              if active else None),
            "timeline": [{"ts": e["ts"], "kind": e["kind"]} for e in evs[-60:]],
        }

    # ------------------------------------------------------------ providers
    async def _providers_data():
        evs = await _events_since(_since(24), limit=2000)
        # scraper-circuit (ingest-hardening): 429/5xx-semantiek + cooldowns
        circuit = getattr(resolver, "circuit", None)
        c_snap = circuit.snapshot() if circuit is not None else {"scrapers": {}}
        stats = {"torbox": {"name": "torbox", "status": "HEALTHY",
                            "retries": 0, "provider_400": 0, "provider_429": 0,
                            "provider_5xx": 0, "not_ready": 0,
                            "candidate_failures": 0, "transient": 0,
                            "permanent": 0, "adds_last_resolves": [],
                            "window": "24h"}}
        resolves = 0
        for e in evs:
            if e["kind"] in ("torbox_retry", "torbox_createtorrent_retry"):
                stats["torbox"]["retries"] += 1
            elif e["kind"] == "provider_search_failed":
                s = stats["torbox"]
                s["retries"] += 1
                if e.get("error_kind") == "PROVIDER_RATE_LIMITED":
                    s["provider_429"] += 1
                else:
                    s["provider_5xx"] += 1
            elif e["kind"] == "resolution_deferred_provider":
                stats["torbox"]["transient"] += 1
            elif e["kind"] == "candidate_failed":
                s = stats["torbox"]
                s["candidate_failures"] += 1
                kind = e.get("reject_kind", "")
                for key in ("provider_400", "provider_429", "provider_5xx",
                            "not_ready"):
                    if kind == key:
                        s[key] += 1
                if str(e.get("ttl_class", "")).startswith("TRANSIENT"):
                    s["transient"] += 1
                elif e.get("ttl_class") == "PERMANENT_BAD":
                    s["permanent"] += 1
            elif e["kind"] == "resolution_reject_summary":
                resolves += 1
                adds = e.get("provider_adds")
                if adds is not None:
                    stats["torbox"]["adds_last_resolves"].append(adds)
        s = stats["torbox"]
        s["avg_adds_per_resolve"] = (round(sum(s["adds_last_resolves"])
                                           / len(s["adds_last_resolves"]), 2)
                                     if s["adds_last_resolves"] else None)  # N/A, geen fake 0
        s["budget_limit"] = getattr(resolver.s, "max_provider_adds_per_resolve", 3)
        if s["provider_5xx"] > 5 or s["retries"] > 20:
            s["status"] = "DEGRADED"
        out = {"providers": list(stats.values()),
               "circuit": c_snap}
        # Phase 39-semantiek: rate-limit zichtbaar als DEGRADED, nooit "no source"
        for name, sc in (c_snap.get("scrapers") or {}).items():
            if sc.get("state") not in ("HEALTHY", None) and sc.get("retry_in_s", 0) > 0:
                out["rate_limit"] = {
                    "state": sc["state"], "scraper": name,
                    "retry_in_s": sc["retry_in_s"],
                    "retry_source": sc.get("retry_source"),
                    "human": (f"Provider rate limited ({name}) — "
                              f"pauze {int(sc['retry_in_s'] // 60)} min "
                              f"({sc.get('retry_source') or 'backoff'})"),
                }
                break
        return out

    @router.get("/providers/health")
    async def providers_health():
        return await _guarded("providers", _providers_data)

    # --------------------------------------------------------------- ingest
    @router.get("/ingest/health")
    async def ingest_health():
        """Phase 36-39: ingest als first-class status (arr-bridge, queue,
        provider-state, arr-indexerhealth via de bridge-clients)."""
        bridge = getattr(app.state, "ingest", None)
        out: dict = {"enabled": bridge is not None}
        if bridge is None:
            out["human"] = "Ingest-bridge niet actief (INGEST_ENABLED uit)"
            return out
        st = await bridge.status()
        out.update(st)
        # arr-zijde: health + RSS-cadans (best effort, nooit blocking)
        for key, client in (("sonarr", bridge.sonarr),
                            ("radarr", bridge.radarr)):
            if client is None:
                out[key]["health"] = "UNCONFIGURED"
                continue
            try:
                health = await client.health()
                tasks = await client.tasks()
                rss = next((t for t in tasks
                            if "RssSync" in (t.get("taskName") or "")), None)
                out[key]["arr_health"] = health
                out[key]["arr_health_ok"] = all(
                    h.get("type") != "error" for h in health)
                out[key]["last_rss_sync"] = (rss or {}).get("lastExecution")
            except Exception as exc:                    # noqa: BLE001
                out[key]["arr_health"] = [{"type": "unreachable",
                                           "message": repr(exc)[:120]}]
                out[key]["arr_health_ok"] = False
                out[key]["last_rss_sync"] = None
        counts = st.get("counts_by_state") or {}
        active = sum(counts.get(k, 0) for k in
                     ("QUEUED", "IDENTITY_VERIFYING", "REGISTERING", "RESOLVING",
                      "PROVIDER_WAIT", "READY", "DELIVERING", "PLEX_REFRESH",
                      "FAILED_RETRYABLE"))
        out["queue_active"] = active
        if st["provider"]["blocked"]:
            if (st["provider"].get("retry_in_s") or 0) > 3600:
                out["state"] = "DEGRADED"
                out["human"] = ("Provider rate limited voor langere tijd — "
                                "queue staat veilig gepauzeerd")
            else:
                out["state"] = "RATE_LIMITED"
                out["human"] = (f"Provider rate limited — retry in "
                                f"{int((st['provider'].get('retry_in_s') or 0) / 60)} min")
        elif st["metrics"].get("last_completed_at"):
            out["state"] = "HEALTHY"
            out["human"] = "Ingest actief en levert"
        else:
            out["state"] = "IDLE" if active == 0 else "PENDING"
            out["human"] = ("geen actieve jobs" if active == 0
                            else f"{active} job(s) wachten op verwerking")
        return out


    # ------------------------------------------------- operator actions (J)
    _action_locks: set = set()

    async def _action(item_id: str, action: str, fn):
        """D/G/K: lock per (action,item), audit-trail, gestructureerd resultaat."""
        key = f"{action}:{item_id}"
        if key in _action_locks:
            return {"action_id": key, "status": "BUSY",
                    "item_id": item_id, "message": "action already running"}
        _action_locks.add(key)
        it = await store.get_item(item_id)
        if it is None:
            _action_locks.discard(key)
            raise HTTPError(404, "item not found")
        label = (it.series and f"{it.series} S{it.season:02d}E{it.episode:02d}") or it.title
        await store.add_event("operator_action_started", item_id, action=action)
        try:
            out = await fn(it)
            out = {"action_id": key, "item_id": item_id,
                   "label": label, **out}
            await store.add_event("operator_action_completed", item_id,
                                  action=action, result=out.get("status"),
                                  message=str(out.get("message"))[:160])
            return out
        except Exception as exc:                        # noqa: BLE001
            await store.add_event("operator_action_failed", item_id,
                                  action=action, error=repr(exc)[:160])
            return {"action_id": key, "item_id": item_id, "status": "FAILED",
                    "message": repr(exc)[:160]}
        finally:
            _action_locks.discard(key)

    async def _resolve_bounded(item) -> dict:
        src = await resolver.resolve_item(item, reason="operator_action")
        if src is not None:
            return {"status": "SUCCESS",
                    "message": f"Source activated: {(src.file_name or '')[:80]}"}
        return {"status": "NO_MATCH",
                "message": "No usable candidate found (bounded search ran)"}

    @router.post("/media/{item_id}/actions/search")
    async def action_search(item_id: str):
        async def fn(it):
            out = await _resolve_bounded(it)
            return out
        return await _action(item_id, "search", fn)

    @router.post("/media/{item_id}/actions/retry")
    async def action_retry(item_id: str):
        async def fn(it):
            def clear(c):
                c.execute("UPDATE sources SET bad_until=0, delivery_bad_until=0, "
                          "failure_count=0 WHERE media_item_id=?", (it.id,))
            await store.run(clear)
            out = await _resolve_bounded(it)
            return {**out, "message": "Retry state cleared. " + out["message"]}
        return await _action(item_id, "retry", fn)

    @router.post("/media/{item_id}/actions/recheck-identity")
    async def action_recheck_identity(item_id: str):
        """G: herlaad exacte Plex-mapping (title+year discovery, GUIDs als
        authority), herbespreek de identity-decision; muteert alléén de
        conflict-flag — nooit external IDs, nooit bronnen."""
        import asyncio as _aio
        monitor = getattr(app.state, "physical", None)
        if monitor is None:
            return {"status": "FAILED", "message": "physical monitor niet actief"}

        async def fn(it):
            is_tv = it.kind == "episode"
            section = 2 if is_tv else 1
            script = (
                "import json,sys,re\n"
                "data = json.loads(sys.argv[1])\n"
                "import urllib.request\n"
                "tok = re.search(r'PlexOnlineToken=\"([^\"]+)\"', open('/config/Plex Media Server/Preferences.xml').read()).group(1)\n"
                f"req = urllib.request.Request('http://127.0.0.1:32400/library/sections/{section}/all?includeGuids=1',\n"
                "    headers={'Accept':'application/json','X-Plex-Token':tok})\n"
                "mds = json.loads(urllib.request.urlopen(req, timeout=30).read())['MediaContainer']['Metadata']\n"
                "hits = [x for x in mds if (x.get('title') or '').casefold()==data['title'].casefold()\n"
                "        and (not data.get('year') or x.get('year')==data['year'])]\n"
                "if len(hits) != 1:\n"
                "    print(json.dumps({'mapping': 'AMBIGUOUS' if len(hits)>1 else 'MISSING'}))\n"
                "    sys.exit(0)\n"
                "x = hits[0]\n"
                "g = {y['id'].split('://')[0]: y['id'].split('://')[1] for y in x.get('Guid',[]) or []}\n"
                "print(json.dumps({'mapping':'EXACT','title':x.get('title'),'imdb':g.get('imdb'),'tmdb':g.get('tmdb')}))\n"
            )
            arg = json.dumps({"title": (it.series if is_tv else it.title) or "",
                              "year": None if is_tv else it.year})
            created = await _aio.to_thread(
                monitor._docker, "POST", f"/containers/{monitor.plex}/exec",
                {"AttachStdout": True, "Cmd": ["python3", "-c", script, arg]})
            started = await _aio.to_thread(
                monitor._docker, "POST",
                f"/exec/{created['json']['Id']}/start",
                {"Detach": False, "Tty": False})
            raw = (started.get("output") or "").strip().splitlines()[-1:] or ["{}"]
            res = json.loads(raw[0] or "{}")
            if res.get("mapping") != "EXACT":
                return {"status": "INCOMPLETE",
                        "message": f"Plex mapping {res.get('mapping')} — identity onveranderd"}
            auth = {"imdb_id": res.get("imdb"), "tmdb_id": res.get("tmdb")}
            from plex_scraper.resolver.identity_guard import (
                decide_identity, IDENTITY_CONFLICT)
            own = ({"imdb_id": it.show_imdb_id, "tmdb_id": it.show_tmdb_id}
                   if is_tv else
                   {"imdb_id": it.imdb_id, "tmdb_id": it.tmdb_id})
            decision, detail = decide_identity(own, auth, own)
            if decision == IDENTITY_CONFLICT:
                await store.set_identity_conflict(it.id, detail)
                await store.add_event("identity_conflict", it.id,
                                      title=it.title, year=it.year,
                                      incoming_imdb=own.get("imdb_id"),
                                      authoritative_imdb=auth.get("imdb_id"),
                                      conflicting_fields=detail["conflicting_fields"])
                return {"status": "CONFLICT",
                        "message": "Identity conflict registered — search blocked "
                                   "until authoritative reconciliation"}
            await store.clear_identity_conflict(it.id)
            return {"status": "OK",
                    "message": f"Identity {decision} (Plex: {auth.get('imdb_id') or 'n/a'})"}
        return await _action(item_id, "recheck_identity", fn)

    @router.post("/media/{item_id}/actions/recheck")
    async def action_recheck(item_id: str):
        async def fn(it):
            active = await resolver._active_source(it.id)
            if active is None:
                return {"status": "NO_SOURCE",
                        "message": "No active source to recheck"}
            ok = await resolver._probe_readable(active)
            return {"status": "SUCCESS" if ok else "FAILED",
                    "message": ("Active source readable (probe OK)" if ok
                                else "Active source FAILED read probe — marked for retry")}
        return await _action(item_id, "recheck", fn)

    @router.post("/media/{item_id}/actions/find-alternative")
    async def action_find_alternative(item_id: str):
        async def fn(it):
            active = await resolver._active_source(it.id)
            if active is None:
                return {"status": "NO_SOURCE",
                        "message": "No active source — use Search instead"}
            # F3: huidige bron blijft actief tot vervanger bewezen is;
            # markeer alleen tijdelijk slecht, herstel bij falen.
            def mark_bad(c):
                c.execute("UPDATE sources SET bad_until=? WHERE id=?",
                          (time.time() + 3600.0, active.id))
            await store.run(mark_bad)
            src = await resolver.resolve_item(it, reason="find_alternative")
            if src is not None and src.id != active.id:
                return {"status": "SUCCESS",
                        "message": f"Alternative activated: {(src.file_name or '')[:80]}"}
            if src is not None:
                def restore(c):
                    c.execute("UPDATE sources SET bad_until=0 WHERE id=?",
                              (active.id,))
                await store.run(restore)
                return {"status": "KEPT",
                        "message": "Only the current source validated — kept"}
            def restore(c):
                c.execute("UPDATE sources SET bad_until=0 WHERE id=?",
                          (active.id,))
            await store.run(restore)
            return {"status": "KEPT",
                    "message": "No usable alternative found. Current source kept."}
        return await _action(item_id, "find_alternative", fn)

    @router.post("/media/{item_id}/actions/mark-bad")
    async def action_mark_bad(item_id: str, body: MarkBadBody):
        async def fn(it):
            active = await resolver._active_source(it.id)
            if active is None:
                return {"status": "NO_SOURCE",
                        "message": "No active source to mark bad"}
            def mark(c):
                c.execute("UPDATE sources SET bad_until=?, state='failed' WHERE id=?",
                          (time.time() + 7 * 86400.0, active.id))
            await store.run(mark)
            return {"status": "SUCCESS",
                    "message": f"Source marked bad ({body.reason}). Run Search to replace."}
        return await _action(item_id, "mark_bad", fn)

    @router.post("/media/{item_id}/actions/manual-magnet")
    async def action_manual_magnet(item_id: str, body: MagnetBody):
        import re as _re
        m = _re.search(r"btih:([0-9a-fA-F]{40})", body.magnet or "")
        if not m:
            dn = _re.search(r"dn=([^&]+)", body.magnet or "")
            return {"action_id": "magnet", "item_id": item_id,
                    "status": "REJECTED",
                    "message": "Invalid magnet: no 40-hex info_hash found"}
        info_hash = m.group(1).lower()
        dn = _re.search(r"dn=([^&]+)", body.magnet or "")
        name = (dn.group(1) if dn else info_hash)[:120]

        async def fn(it):
            from plex_scraper.scraper.scrapers.base import TorrentCandidate
            from plex_scraper.resolver.selfheal import identity_gate
            cand = TorrentCandidate(info_hash=info_hash, torrent_name=name,
                                    size=None, seeders=None,
                                    file_name=None, file_index=None)
            ok, why, _sub = identity_gate(it.title, it.series, it.season,
                                          it.episode, name, it.year)
            if not ok:
                return {"status": "REJECTED",
                        "message": f"Identity gate rejected magnet: {why}"}
            src = await resolver._validate_candidate(it, cand)
            if src is None:
                return {"status": "REJECTED",
                        "message": "Magnet failed validation (probe/sanity) — not activated"}
            previous = await resolver._active_source(it.id)
            await resolver._activate(it, src, previous, reason="operator_magnet")
            return {"status": "SUCCESS",
                    "message": f"Magnet validated and activated: {(src.file_name or name)[:80]}"}
        return await _action(item_id, "manual_magnet", fn)

    app.include_router(router)
    return router
