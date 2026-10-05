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
import time

from fastapi import APIRouter
from fastapi import HTTPException as HTTPError

HUMAN_REJECTS = {
    "identity_wrong_show": ("verkeerde serie", "Identity"),
    "identity_pack_missing_episode": ("aflevering niet in pack", "Identity"),
    "identity_wrong_movie": ("verkeerde film", "Identity"),
    "identity_wrong_year": ("verkeerd jaar", "Identity"),
    "pre_gate_size": ("te klein (vooraf afgefilterd)", "Size"),
    "budget_exhausted": ("niet geprobeerd (add-budget op)", "Budget"),
    "file_too_small": ("bestand te klein / mislabeled", "Validation"),
    "torrent_no_files": ("geen bestanden", "Validation"),
    "provider_400": ("TorBox 400 (tijdelijk)", "Provider"),
    "provider_429": ("rate limited", "Provider"),
    "provider_5xx": ("TorBox serverfout", "Provider"),
    "torrent_not_ready": ("nog niet klaar (retry volgt)", "Transient"),
    "first_byte_empty": ("geen data bij eerste byte", "Validation"),
    "range_failed": ("lees-probe midden faalt", "Validation"),
    "backend_unavailable": ("back-end onbereikbaar", "Backend"),
    "provider_add_failed": ("toevoegen aan provider faalt", "Provider"),
    "unknown_probe_failure": ("onbekende validatiefout", "Other"),
    "bad_ttl": ("overgeslagen (tijdelijk afgekeurd)", "Retry"),
    "no_candidates": ("geen candidates gevonden", "Provider"),
}

OPERATION_KINDS = {
    "resolutions": ("resolution_started", "resolution_succeeded",
                    "resolution_failed", "resolution_reject_summary",
                    "resolution_skip_uncached", "search_identity_incomplete"),
    "repairs": ("sweep_repair_needed", "repair_kept_current",
                "path_repair", "stale_state_reconciled"),
    "failovers": ("jit_rescue_switch", "source_failed", "failover",
                  "startup_failed"),
    "sweeper": ("sweep_strike", "sweep_paused_playback"),
    "playback": ("session_opened", "session_closed", "stall_detected",
                 "delivery_degraded", "startup_first_byte"),
    "provider": ("torbox_retry", "torbox_createtorrent_retry",
                 "upstream_http_4xx"),
    "metadata": ("item_registered", "show_id_backfilled", "identity_corrected"),
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
                    "degraded": f"laatste bekende data ({str(exc)[:80]})"}
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
        jobs = await store.job_runs(limit=3)
        running = [j for j in jobs if j["status"] == "RUNNING"]

        return {
            "health": health,
            "library": {"total": len(items), "ready": counts.get("READY", 0),
                        "no_source": len(no_source),
                        "resolving": len(resolving),
                        "identity_incomplete": identity_incomplete},
            "playback": {"active": len(streams), "streams": streams},
            "now": {"resolving": [{"item_id": i.id, "label":
                                   (i.series and
                                    f"{i.series} S{i.season:02d}E{i.episode:02d}")
                                   or i.title} for i in resolving],
                    "jobs_running": [{"id": j["id"], "job_type": j["job_type"],
                                      "current_item": j.get("current_item"),
                                      "progress_total": j.get("progress_total"),
                                      "progress_current": j.get("progress_current")}
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
                elif e["kind"] == "resolution_failed":
                    run["outcome"] = "FAILED"
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

    def _human_summary(g: dict) -> str:
        kinds = [e["kind"] for e in g["events"]]
        n_ok = kinds.count("resolution_succeeded")
        if n_ok:
            sel = next((e for e in reversed(g["events"])
                        if e["kind"] == "resolution_succeeded"), None)
            f = (sel.get("selected_candidate") or {}).get("file") if sel else None
            return f"bron geselecteerd: {f}" if f else "bron geselecteerd"
        rej = _rejects_of(g["events"]).get("rejects") or {}
        if not rej:
            return "geen candidates"
        parts = [f"{v}× {HUMAN_REJECTS.get(k, (k, ''))[0]}"
                 for k, v in rej.items() if v]
        return "; ".join(parts[:4])

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
                human = "Provider heeft geen enkele candidate"
            elif rejects.get("no_candidates"):
                classification = "PROVIDER_NO_MATCH"
                human = "0 candidates bij volledige identiteit"
            elif has_transient or next_retry and next_retry > time.time():
                classification = "NO_USABLE_CANDIDATE"
                human = "Candidates waren er, maar nog niet bruikbaar — retry gepland"
            else:
                classification = "NO_USABLE_CANDIDATE"
                human = "Candidates afgewezen op identiteit/validatie"
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

    @router.get("/issues")
    async def issues():
        return await _guarded("issues", _issues_data)

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
        raise HTTPError(404, "run niet gevonden")

    # ---------------------------------------------------------------- trace
    @router.get("/media/{item_id}/trace")
    async def trace(item_id: str):
        it = await store.get_item(item_id)
        if it is None:
            raise HTTPError(404, "item niet gevonden")

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
        stats = {"torbox": {"name": "torbox", "status": "HEALTHY",
                            "retries": 0, "provider_400": 0, "provider_429": 0,
                            "provider_5xx": 0, "not_ready": 0,
                            "candidate_failures": 0, "transient": 0,
                            "permanent": 0, "adds_last_resolves": []}}
        resolves = 0
        for e in evs:
            if e["kind"] in ("torbox_retry", "torbox_createtorrent_retry"):
                stats["torbox"]["retries"] += 1
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
                                     if s["adds_last_resolves"] else 0)
        s["budget_limit"] = getattr(resolver.s, "max_provider_adds_per_resolve", 3)
        if s["provider_5xx"] > 5 or s["retries"] > 20:
            s["status"] = "DEGRADED"
        return {"providers": list(stats.values())}

    @router.get("/providers/health")
    async def providers_health():
        return await _guarded("providers", _providers_data)

    app.include_router(router)
    return router
